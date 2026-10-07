package match

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
)

// EnqueueRequest is a request to enqueue a team as an indivisible unit.
type EnqueueRequest struct {
	Partition   model.PartitionID
	TeamID      model.TeamID
	Players     []model.Player
	RuleVersion int // 0 means the registry default at enqueue time
}

// QueueEntry is one team's queue status.
type QueueEntry struct {
	TeamID      model.TeamID
	Score       int
	RuleVersion int
	EnqueueSeq  int64
	EnqueueTick int64
	WaitTicks   int64
	Members     int
}

// QueueView is an immutable snapshot of a partition's queue.
type QueueView struct {
	Partition model.PartitionID
	Tick      int64
	Waiting   int
	Capacity  int
	Entries   []QueueEntry
}

// Engine runs deterministic, per-partition matching. Each partition is owned
// by a single goroutine (the background loop) or driven by manual Tick calls;
// match decisions never read maps, clocks, or random sources.
//
// Locking: the partitions map is immutable after construction. teamIndex is
// guarded by indexMu, matchSeq is atomic, and each partitionState has its own
// mutex. A partition mutex is never held while acquiring indexMu (team-index
// updates happen before or after the partition critical section), so no lock
// ordering cycle exists and partitions can never deadlock each other.
type Engine struct {
	cfg   config.Config
	rules *rule.Registry
	audit *audit.Log
	relay *relay.Manager

	indexMu   sync.Mutex
	teamIndex map[model.TeamID]model.PartitionID

	partitions map[model.PartitionID]*partitionState
	matchSeq   atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type partitionState struct {
	id    model.PartitionID
	mu    sync.Mutex
	teams map[model.TeamID]*model.Team
	seq   int64
	tick  int64

	backfills   map[string]*model.BackfillTicket
	backfillSeq int64
}

// NewEngine creates a matching engine with one state per configured partition.
func NewEngine(cfg config.Config, rules *rule.Registry, auditLog *audit.Log, relayMgr *relay.Manager) *Engine {
	e := &Engine{
		cfg:        cfg,
		rules:      rules,
		audit:      auditLog,
		relay:      relayMgr,
		partitions: make(map[model.PartitionID]*partitionState, len(cfg.Partitions)),
		teamIndex:  make(map[model.TeamID]model.PartitionID),
		stopCh:     make(chan struct{}),
	}
	for _, p := range cfg.Partitions {
		pid := model.PartitionID(p)
		e.partitions[pid] = &partitionState{
			id:        pid,
			teams:     make(map[model.TeamID]*model.Team),
			backfills: make(map[string]*model.BackfillTicket),
		}
	}
	return e
}

// Enqueue submits a team as an indivisible unit. The team is validated, its
// score merged by the rule version in effect at enqueue time, and it is
// stamped with a monotonic sequence that defines enqueue order.
func (e *Engine) Enqueue(req EnqueueRequest) (*model.Team, error) {
	if len(req.Players) == 0 {
		return nil, rejectTeam(RejectTeamEmpty, "team must have at least one member", req.TeamID)
	}
	if req.TeamID == "" {
		return nil, reject(RejectTeamEmpty, "team id must not be empty")
	}
	pstate, ok := e.partitions[req.Partition]
	if !ok {
		return nil, reject(RejectPartitionNotFound, fmt.Sprintf("partition %q does not exist", req.Partition))
	}

	ruleVersion := req.RuleVersion
	if ruleVersion == 0 {
		ruleVersion = e.rules.DefaultVersion()
	}
	ruleInst, err := e.rules.Get(ruleVersion)
	if err != nil {
		return nil, rejectTeam(RejectRuleNotFound, err.Error(), req.TeamID)
	}

	// Claim the team id atomically first; the partition queue insert happens
	// afterwards. The two locks are never held at the same time.
	e.indexMu.Lock()
	if _, exists := e.teamIndex[req.TeamID]; exists {
		e.indexMu.Unlock()
		return nil, rejectTeam(RejectDuplicateTeam, "team already enqueued or matched; duplicate submission is ignored", req.TeamID)
	}
	e.teamIndex[req.TeamID] = req.Partition
	e.indexMu.Unlock()

	pstate.mu.Lock()
	if len(pstate.teams) >= e.cfg.QueueCapacity {
		queueSize := len(pstate.teams)
		pstate.mu.Unlock()
		e.indexMu.Lock()
		delete(e.teamIndex, req.TeamID)
		e.indexMu.Unlock()
		e.audit.Append(audit.Record{
			Type:      audit.EventReject,
			Partition: req.Partition,
			TeamID:    req.TeamID,
			Reason:    string(RejectQueueFull),
			Input:     mustJSON(map[string]any{"queueSize": queueSize, "capacity": e.cfg.QueueCapacity}),
		})
		return nil, rejectTeam(RejectQueueFull, fmt.Sprintf("partition %q queue is full (%d/%d)", req.Partition, queueSize, e.cfg.QueueCapacity), req.TeamID)
	}

	players := make([]model.Player, len(req.Players))
	copy(players, req.Players)
	team := &model.Team{
		ID:          req.TeamID,
		Partition:   req.Partition,
		Members:     players,
		Score:       ruleInst.MergeScore(players),
		RuleVersion: ruleVersion,
		EnqueueSeq:  pstate.seq,
		EnqueueTick: pstate.tick,
		Status:      model.TeamWaiting,
	}
	pstate.seq++
	pstate.teams[req.TeamID] = team
	// The audit record is appended inside the partition critical section so
	// the audited enqueue order always matches EnqueueSeq assignment order
	// (the determinism-replay contract depends on this).
	e.audit.Append(audit.Record{
		Type:        audit.EventEnqueue,
		Partition:   req.Partition,
		TeamID:      req.TeamID,
		RuleVersion: ruleVersion,
		Input: mustJSON(map[string]any{
			"members":     playerIDs(players),
			"memberCount": len(players),
			"mergedScore": team.Score,
			"enqueueSeq":  team.EnqueueSeq,
			"enqueueTick": team.EnqueueTick,
		}),
		Decision: fmt.Sprintf("score merged by rule v%d; enqueueSeq=%d", ruleVersion, team.EnqueueSeq),
	})
	pstate.mu.Unlock()
	return team, nil
}

// Cancel removes a waiting team before it is matched.
func (e *Engine) Cancel(teamID model.TeamID) error {
	e.indexMu.Lock()
	pid, ok := e.teamIndex[teamID]
	e.indexMu.Unlock()
	if !ok {
		return rejectTeam(RejectTeamNotWaiting, "team is not enqueued", teamID)
	}
	pstate := e.partitions[pid]
	pstate.mu.Lock()
	team, ok := pstate.teams[teamID]
	if !ok || team.Status != model.TeamWaiting {
		pstate.mu.Unlock()
		return rejectTeam(RejectTeamNotWaiting, "team is not waiting (already matched or cancelled)", teamID)
	}
	team.Status = model.TeamCancelled
	delete(pstate.teams, teamID)
	pstate.mu.Unlock()
	e.indexMu.Lock()
	delete(e.teamIndex, teamID)
	e.indexMu.Unlock()
	e.audit.Append(audit.Record{
		Type:      audit.EventCancel,
		Partition: pid,
		TeamID:    teamID,
		Reason:    "cancelled before match",
	})
	return nil
}

// Tick advances every partition by one logical step. Partitions are processed
// in sorted order; each partition has its own mutex and (in background mode)
// its own goroutine, so a slow partition never blocks a fast one.
func (e *Engine) Tick() {
	for _, pid := range e.partitionIDs() {
		e.tickPartition(pid)
	}
}

func (e *Engine) tickPartition(pid model.PartitionID) {
	pstate := e.partitions[pid]
	removed := e.tickPartitionLocked(pstate)
	if len(removed) > 0 {
		e.indexMu.Lock()
		for _, id := range removed {
			delete(e.teamIndex, id)
		}
		e.indexMu.Unlock()
	}
}

// tickPartitionLocked advances one partition by one logical step and returns
// the ids of teams that left the queue (matched or backfilled), for deferred
// team-index cleanup outside the partition critical section.
func (e *Engine) tickPartitionLocked(pstate *partitionState) []model.TeamID {
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	pstate.tick++
	tick := pstate.tick
	removed := make([]model.TeamID, 0)

	// 1. Relay-side timeouts (retention / backfill expirations) for this
	//    partition only.
	backfillReqs := e.relay.TickPartition(pstate.id, tick)
	for _, req := range backfillReqs {
		e.openBackfill(pstate, req, tick)
	}

	// 2. Expire backfill tickets. A ticket is dropped when its session no
	//    longer waits on it (ended or already served); it times out when the
	//    wait exceeds the timeout of the rule version the session was created
	//    under — never the current default rule.
	for id, ticket := range pstate.backfills {
		view, ok := e.relay.GetSession(ticket.SessionID)
		if !ok || view.State != model.SessionActive || view.BackfillTicketID != ticket.ID {
			// Stale ticket: the session ended or moved on without it.
			delete(pstate.backfills, id)
			continue
		}
		ruleInst, err := e.rules.Get(ticket.RuleVersion)
		if err != nil {
			continue
		}
		if tick-ticket.EnqueueTick > int64(ruleInst.BackfillTimeoutTicks()) {
			reason := fmt.Sprintf("%s: no replacement within %d ticks (rule v%d)",
				relay.EndCodeBackfillTimeout, ruleInst.BackfillTimeoutTicks(), ruleInst.Version())
			if err := e.relay.EndSession(ticket.SessionID, tick, reason); err == nil {
				e.audit.Append(audit.Record{
					Type:        audit.EventBackfillTimeout,
					Partition:   pstate.id,
					SessionID:   ticket.SessionID,
					RuleVersion: ticket.RuleVersion,
					Reason:      reason,
					Input:       mustJSON(map[string]any{"ticketID": ticket.ID, "enqueueTick": ticket.EnqueueTick, "timeoutTick": tick}),
				})
			}
			delete(pstate.backfills, id)
		}
	}

	// 3. Pair backfill tickets first; they are the highest-priority demand.
	if len(pstate.backfills) > 0 {
		tickets := make([]*model.BackfillTicket, 0, len(pstate.backfills))
		for _, t := range pstate.backfills {
			tickets = append(tickets, t)
		}
		waiting := make([]*model.Team, 0, len(pstate.teams))
		for _, t := range pstate.teams {
			waiting = append(waiting, t)
		}
		for _, planned := range PlanBackfill(tickets, waiting) {
			followUps, err := e.relay.CompleteBackfill(planned.Ticket.SessionID, planned.Ticket.ID, planned.Team, tick)
			if err != nil {
				// Session may have ended concurrently; drop the ticket.
				delete(pstate.backfills, planned.Ticket.ID)
				continue
			}
			delete(pstate.teams, planned.Team.ID)
			removed = append(removed, planned.Team.ID)
			delete(pstate.backfills, planned.Ticket.ID)
			e.audit.Append(audit.Record{
				Type:        audit.EventBackfillSucceeded,
				Partition:   pstate.id,
				SessionID:   planned.Ticket.SessionID,
				TeamID:      planned.Team.ID,
				RuleVersion: planned.Team.RuleVersion,
				Input:       mustJSON(map[string]any{"ticketID": planned.Ticket.ID, "slots": planned.Ticket.Slots, "teamSize": len(planned.Team.Members)}),
				Decision:    "backfill ticket served by earliest matching team",
			})
			// Slots vacated while the ticket was pending get a fresh ticket.
			for _, req := range followUps {
				e.openBackfill(pstate, req, tick)
			}
		}
	}

	// 4. Regular deterministic matching.
	if len(pstate.teams) < e.cfg.TeamsPerMatch {
		return removed
	}
	waiting := make([]*model.Team, 0, len(pstate.teams))
	for _, t := range pstate.teams {
		waiting = append(waiting, t)
	}
	matches := PlanMatches(waiting, tick, e.rules, e.cfg.TeamsPerMatch, e.cfg.MatchBatchSize)
	for _, m := range matches {
		matchID := e.nextMatchID()
		view, err := e.relay.CreateSession(pstate.id, matchID, m.RuleVersion, m.Teams, tick)
		if err != nil {
			// At session capacity or otherwise unable: leave teams queued and
			// stop this tick's batch; they will be retried next tick.
			e.audit.Append(audit.Record{
				Type:      audit.EventReject,
				Partition: pstate.id,
				Reason:    string(RejectSessionAtCapacity),
				Input:     mustJSON(map[string]any{"matchID": matchID, "teamIDs": teamIDs(m.Teams)}),
			})
			break
		}
		for _, t := range m.Teams {
			t.Status = model.TeamMatched
			t.MatchedTick = tick
			t.SessionID = view.ID
			delete(pstate.teams, t.ID)
			removed = append(removed, t.ID)
		}
		e.audit.Append(audit.Record{
			Type:        audit.EventMatchFormed,
			Partition:   pstate.id,
			SessionID:   view.ID,
			RuleVersion: m.RuleVersion,
			Input: mustJSON(map[string]any{
				"matchID":    matchID,
				"teams":      teamIDs(m.Teams),
				"scores":     teamScores(m.Teams),
				"seqs":       teamSeqs(m.Teams),
				"waitTicks":  m.WaitTicks,
				"scoreDiff":  m.ScoreDiff,
				"anchorRule": m.RuleVersion,
			}),
			Decision: fmt.Sprintf("anchor=t[%d] paired with closest-score team; scoreDiff=%d; waitTicks=%v",
				m.Teams[0].EnqueueSeq, m.ScoreDiff, m.WaitTicks),
		})
	}
	return removed
}

// OpenBackfill queues a backfill ticket for a session that vacated slots
// outside the normal tick flow (e.g. an explicit member leave).
func (e *Engine) OpenBackfill(req relay.BackfillRequest, tick int64) {
	pstate, ok := e.partitions[req.Partition]
	if !ok {
		return
	}
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	e.openBackfill(pstate, req, tick)
}

func (e *Engine) openBackfill(pstate *partitionState, req relay.BackfillRequest, tick int64) {
	// One active ticket per session; duplicate requests (e.g. repeated ticks)
	// for the same wait are ignored.
	for _, t := range pstate.backfills {
		if t.SessionID == req.SessionID || t.ID == req.TicketID {
			return
		}
	}
	pstate.backfillSeq++
	ticket := &model.BackfillTicket{
		ID:          req.TicketID,
		SessionID:   req.SessionID,
		Partition:   pstate.id,
		Slots:       req.Slots,
		EnqueueSeq:  pstate.backfillSeq,
		EnqueueTick: tick,
		RuleVersion: req.RuleVersion,
	}
	pstate.backfills[ticket.ID] = ticket
	e.audit.Append(audit.Record{
		Type:        audit.EventBackfillStarted,
		Partition:   pstate.id,
		SessionID:   req.SessionID,
		RuleVersion: req.RuleVersion,
		Input:       mustJSON(map[string]any{"ticketID": req.TicketID, "slots": req.Slots, "enqueueTick": tick}),
		Decision:    "retention window expired under backfill-enabled rule",
	})
}

func (e *Engine) nextMatchID() string {
	return fmt.Sprintf("m-%d", e.matchSeq.Add(1))
}

// Start launches one independent goroutine per partition, ticking at
// cfg.TickInterval. A zero TickInterval is a no-op; use Tick for manual
// deterministic stepping.
func (e *Engine) Start() {
	if e.cfg.TickInterval <= 0 {
		return
	}
	for _, pid := range e.partitionIDs() {
		pid := pid
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			ticker := time.NewTicker(e.cfg.TickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					e.tickPartition(pid)
				case <-e.stopCh:
					return
				}
			}
		}()
	}
}

// Stop terminates background partition loops and waits for them to finish.
func (e *Engine) Stop() {
	select {
	case <-e.stopCh:
		// already closed
	default:
		close(e.stopCh)
	}
	e.wg.Wait()
}

// GetTeam returns the team with the given id, if any.
func (e *Engine) GetTeam(teamID model.TeamID) (*model.Team, bool) {
	e.indexMu.Lock()
	pid, ok := e.teamIndex[teamID]
	e.indexMu.Unlock()
	if !ok {
		return nil, false
	}
	pstate := e.partitions[pid]
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	t, ok := pstate.teams[teamID]
	return t, ok
}

// GetQueue returns an immutable snapshot of a partition's queue.
func (e *Engine) GetQueue(partition model.PartitionID) (QueueView, error) {
	pstate, ok := e.partitions[partition]
	if !ok {
		return QueueView{}, reject(RejectPartitionNotFound, fmt.Sprintf("partition %q does not exist", partition))
	}
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	entries := make([]QueueEntry, 0, len(pstate.teams))
	for _, t := range pstate.teams {
		entries = append(entries, QueueEntry{
			TeamID:      t.ID,
			Score:       t.Score,
			RuleVersion: t.RuleVersion,
			EnqueueSeq:  t.EnqueueSeq,
			EnqueueTick: t.EnqueueTick,
			WaitTicks:   pstate.tick - t.EnqueueTick,
			Members:     len(t.Members),
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].EnqueueSeq < entries[j].EnqueueSeq })
	return QueueView{
		Partition: partition,
		Tick:      pstate.tick,
		Waiting:   len(pstate.teams),
		Capacity:  e.cfg.QueueCapacity,
		Entries:   entries,
	}, nil
}

// ListPartitions returns the configured partition IDs in sorted order.
func (e *Engine) ListPartitions() []model.PartitionID {
	return e.partitionIDs()
}

func (e *Engine) partitionIDs() []model.PartitionID {
	// e.partitions is immutable after NewEngine, so no lock is needed.
	out := make([]model.PartitionID, 0, len(e.partitions))
	for pid := range e.partitions {
		out = append(out, pid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func playerIDs(players []model.Player) []model.PlayerID {
	out := make([]model.PlayerID, len(players))
	for i, p := range players {
		out[i] = p.ID
	}
	return out
}

func teamIDs(teams []*model.Team) []model.TeamID {
	out := make([]model.TeamID, len(teams))
	for i, t := range teams {
		out[i] = t.ID
	}
	return out
}

func teamScores(teams []*model.Team) []int {
	out := make([]int, len(teams))
	for i, t := range teams {
		out[i] = t.Score
	}
	return out
}

func teamSeqs(teams []*model.Team) []int64 {
	out := make([]int64, len(teams))
	for i, t := range teams {
		out[i] = t.EnqueueSeq
	}
	return out
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("json-error: %v", err)
	}
	return string(b)
}

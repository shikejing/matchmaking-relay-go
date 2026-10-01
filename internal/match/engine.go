package match

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
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
type Engine struct {
	cfg   config.Config
	rules *rule.Registry
	audit *audit.Log
	relay *relay.Manager

	mu         sync.Mutex
	partitions map[model.PartitionID]*partitionState
	teamIndex  map[model.TeamID]model.PartitionID
	matchSeq   int64

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

	// Lock order: engine.mu then partition.mu.
	e.mu.Lock()
	defer e.mu.Unlock()
	pstate.mu.Lock()
	defer pstate.mu.Unlock()

	if _, exists := e.teamIndex[req.TeamID]; exists {
		return nil, rejectTeam(RejectDuplicateTeam, "team already enqueued or matched; duplicate submission is ignored", req.TeamID)
	}
	if len(pstate.teams) >= e.cfg.QueueCapacity {
		e.audit.Append(audit.Record{
			Type:      audit.EventReject,
			Partition: req.Partition,
			TeamID:    req.TeamID,
			Reason:    string(RejectQueueFull),
			Input:     mustJSON(map[string]any{"queueSize": len(pstate.teams), "capacity": e.cfg.QueueCapacity}),
		})
		return nil, rejectTeam(RejectQueueFull, fmt.Sprintf("partition %q queue is full (%d/%d)", req.Partition, len(pstate.teams), e.cfg.QueueCapacity), req.TeamID)
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
	e.teamIndex[req.TeamID] = req.Partition

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
	return team, nil
}

// Cancel removes a waiting team before it is matched.
func (e *Engine) Cancel(teamID model.TeamID) error {
	e.mu.Lock()
	pid, ok := e.teamIndex[teamID]
	e.mu.Unlock()
	if !ok {
		return rejectTeam(RejectTeamNotWaiting, "team is not enqueued", teamID)
	}
	pstate := e.partitions[pid]
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	team, ok := pstate.teams[teamID]
	if !ok || team.Status != model.TeamWaiting {
		return rejectTeam(RejectTeamNotWaiting, "team is not waiting (already matched or cancelled)", teamID)
	}
	team.Status = model.TeamCancelled
	delete(pstate.teams, teamID)
	e.mu.Lock()
	delete(e.teamIndex, teamID)
	e.mu.Unlock()
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
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	pstate.tick++
	tick := pstate.tick

	// 1. Relay-side timeouts (retention / backfill expirations) for this
	//    partition only.
	backfillReqs := e.relay.TickPartition(pstate.id, tick)
	for _, req := range backfillReqs {
		e.openBackfill(pstate, req.SessionID, req.Slots, tick)
	}

	// 2. Expire backfill tickets that found no replacement in time.
	for id, ticket := range pstate.backfills {
		ruleInst, err := e.rules.Get(ticket.RuleVersion)
		if err != nil {
			continue
		}
		if tick-ticket.EnqueueTick > int64(ruleInst.BackfillTimeoutTicks()) {
			if err := e.relay.EndSession(ticket.SessionID, tick, string(RejectBackfillTimeout)); err == nil {
				e.audit.Append(audit.Record{
					Type:      audit.EventBackfillTimeout,
					Partition: pid,
					SessionID: ticket.SessionID,
					Reason:    fmt.Sprintf("no replacement within %d ticks", ruleInst.BackfillTimeoutTicks()),
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
			if err := e.relay.CompleteBackfill(planned.Ticket.SessionID, planned.Ticket.ID, planned.Team, tick); err != nil {
				// Session may have ended concurrently; drop the ticket.
				delete(pstate.backfills, planned.Ticket.ID)
				continue
			}
			e.removeWaiting(pstate, planned.Team)
			delete(pstate.backfills, planned.Ticket.ID)
			e.audit.Append(audit.Record{
				Type:        audit.EventBackfillSucceeded,
				Partition:   pid,
				SessionID:   planned.Ticket.SessionID,
				TeamID:      planned.Team.ID,
				RuleVersion: planned.Team.RuleVersion,
				Input:       mustJSON(map[string]any{"slots": planned.Ticket.Slots, "teamSize": len(planned.Team.Members)}),
				Decision:    "backfill ticket served by earliest matching team",
			})
		}
	}

	// 4. Regular deterministic matching.
	if len(pstate.teams) < e.cfg.TeamsPerMatch {
		return
	}
	waiting := make([]*model.Team, 0, len(pstate.teams))
	for _, t := range pstate.teams {
		waiting = append(waiting, t)
	}
	matches := PlanMatches(waiting, tick, e.rules, e.cfg.TeamsPerMatch, e.cfg.MatchBatchSize)
	for _, m := range matches {
		matchID := e.nextMatchID()
		view, err := e.relay.CreateSession(pid, matchID, m.RuleVersion, m.Teams, tick)
		if err != nil {
			// At session capacity or otherwise unable: leave teams queued and
			// stop this tick's batch; they will be retried next tick.
			e.audit.Append(audit.Record{
				Type:      audit.EventReject,
				Partition: pid,
				Reason:    string(RejectSessionAtCapacity),
				Input:     mustJSON(map[string]any{"matchID": matchID, "teamIDs": teamIDs(m.Teams)}),
			})
			break
		}
		for _, t := range m.Teams {
			t.Status = model.TeamMatched
			t.MatchedTick = tick
			t.SessionID = view.ID
			e.removeWaiting(pstate, t)
		}
		e.audit.Append(audit.Record{
			Type:        audit.EventMatchFormed,
			Partition:   pid,
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
}

// OpenBackfill queues a backfill ticket for a session that vacated slots
// outside the normal tick flow (e.g. an explicit member leave).
func (e *Engine) OpenBackfill(partition model.PartitionID, sessionID model.SessionID, slots int, tick int64) {
	pstate, ok := e.partitions[partition]
	if !ok {
		return
	}
	pstate.mu.Lock()
	defer pstate.mu.Unlock()
	e.openBackfill(pstate, sessionID, slots, tick)
}

func (e *Engine) openBackfill(pstate *partitionState, sessionID model.SessionID, slots int, tick int64) {
	// One active ticket per session.
	for _, t := range pstate.backfills {
		if t.SessionID == sessionID {
			return
		}
	}
	pstate.backfillSeq++
	ticket := &model.BackfillTicket{
		ID:          fmt.Sprintf("bf-%d-%d", pstate.tick, pstate.backfillSeq),
		SessionID:   sessionID,
		Partition:   pstate.id,
		Slots:       slots,
		EnqueueSeq:  pstate.backfillSeq,
		EnqueueTick: tick,
	}
	pstate.backfills[ticket.ID] = ticket
	e.audit.Append(audit.Record{
		Type:      audit.EventBackfillStarted,
		Partition: pstate.id,
		SessionID: sessionID,
		Input:     mustJSON(map[string]any{"slots": slots, "enqueueTick": tick}),
		Decision:  "retention window expired under backfill-enabled rule",
	})
}

func (e *Engine) removeWaiting(pstate *partitionState, team *model.Team) {
	delete(pstate.teams, team.ID)
	e.mu.Lock()
	delete(e.teamIndex, team.ID)
	e.mu.Unlock()
}

func (e *Engine) nextMatchID() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.matchSeq++
	return fmt.Sprintf("m-%d", e.matchSeq)
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
	e.mu.Lock()
	pid, ok := e.teamIndex[teamID]
	e.mu.Unlock()
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
	e.mu.Lock()
	defer e.mu.Unlock()
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

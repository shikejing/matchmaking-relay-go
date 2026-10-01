// Package engine wires queues, matchmaking decisions, session manager
// and audit together, and advances partitions independently so a slow
// partition never blocks a fast one while no partition is starved.
package engine

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shikejing/matchmaking-relay-go/audit"
	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/match"
	"github.com/shikejing/matchmaking-relay-go/queue"
	"github.com/shikejing/matchmaking-relay-go/rules"
	"github.com/shikejing/matchmaking-relay-go/session"
	"github.com/shikejing/matchmaking-relay-go/transport"
	"github.com/shikejing/matchmaking-relay-go/types"
)

// partition bundles one partition's independently-locked machinery.
type partition struct {
	cfg    config.PartitionConfig
	rules  rules.Rules
	queue  *queue.Queue
	mu     sync.Mutex // serializes all decisions for this partition
	wakeCh chan struct{}
}

// Engine is the top-level in-process matchmaking relay server.
type Engine struct {
	cfg      config.Config
	clk      clock.Clock
	registry *rules.Registry
	broker   *transport.Broker
	audit    *audit.Recorder
	manager  *session.Manager

	partitions map[types.PartitionID]*partition

	submitSeq atomic.Uint64
	sessionN  atomic.Uint64

	mu      sync.Mutex
	started bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// New constructs an engine, validates configuration and registers the
// built-in v1/v2 rulesets bound per partition version.
func New(cfg config.Config, clk clock.Clock) (*Engine, error) {
	if reason, detail := cfg.Validate(); reason != types.RejectNone {
		return nil, fmt.Errorf("invalid config (%s): %s", reason, detail)
	}
	if clk == nil {
		clk = clock.NewRealClock()
	}
	reg := rules.NewRegistry()

	broker := transport.NewBroker(cfg.PipeBuffer)
	rec := audit.NewRecorder(cfg.EventHistoryCap)
	e := &Engine{
		cfg:        cfg,
		clk:        clk,
		registry:   reg,
		broker:     broker,
		audit:      rec,
		manager:    session.NewManager(cfg, broker, rec),
		partitions: map[types.PartitionID]*partition{},
		stopCh:     make(chan struct{}),
	}
	for _, pc := range cfg.Partitions {
		var rs rules.Rules
		switch pc.Version {
		case "v2":
			rs = rules.NewV2(pc.TargetTeamSize, pc.ScoreWindow, pc.MaxWaitMs)
		default: // unknown versions fall back to v1 semantics
			rs = rules.NewV1(pc.TargetTeamSize, pc.ScoreWindow, pc.MaxWaitMs)
		}
		// Register the exact ruleset each partition runs so version
		// queries and Describe() reflect real limits; duplicate versions
		// across partitions share the first registration, while each
		// partition keeps using its own instance.
		if _, ok := reg.Get(pc.Version); !ok {
			_ = reg.Register(rs)
		}
		e.partitions[pc.ID] = &partition{
			cfg:    pc,
			rules:  rs,
			queue:  queue.NewQueue(pc, rs, clk),
			wakeCh: make(chan struct{}, 1),
		}
	}
	return e, nil
}

// Start launches one independent advance goroutine per partition plus a
// session-timeout loop. Each partition progresses on its own timer and
// is additionally nudged when work is submitted; a slow partition never
// blocks another. Round-robin tick order is fixed (partition id sort)
// so no partition is starved and output stays reproducible.
func (e *Engine) Start() {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return
	}
	e.started = true
	e.mu.Unlock()

	ids := make([]types.PartitionID, 0, len(e.partitions))
	for id := range e.partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	interval := e.cfg.TickIntervalMs
	if interval <= 0 {
		interval = 50
	}
	for _, id := range ids {
		e.wg.Add(1)
		go func(pid types.PartitionID) {
			defer e.wg.Done()
			ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
			defer ticker.Stop()
			ch := e.partitions[pid].wakeCh
			for {
				select {
				case <-e.stopCh:
					return
				case <-ticker.C:
					e.TickPartition(pid)
				case <-ch:
					e.TickPartition(pid)
				}
			}
		}(id)
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-e.stopCh:
				return
			case <-ticker.C:
				e.manager.Tick(e.clk.NowMs())
			}
		}
	}()
}

// Stop halts background loops (idempotent) and evicts all sessions.
func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return
	}
	e.started = false
	close(e.stopCh)
	e.mu.Unlock()
	e.wg.Wait()
	for _, snap := range e.manager.Snapshot() {
		e.manager.Remove(snap.ID, types.CloseEvicted)
	}
}

// SubmitTeam enqueues a team as an atomic unit. The globally monotonic
// sequence is assigned first, making concurrent submissions totally
// ordered and reproducible; the same team submitted concurrently or
// repeatedly yields exactly one queue entry.
func (e *Engine) SubmitTeam(pid types.PartitionID, team types.Team) types.SubmitResult {
	p, ok := e.partitions[pid]
	if !ok {
		return types.SubmitResult{Reason: types.RejectUnknownPartition, Detail: string(pid)}
	}
	// Bind the team to the partition's rule version: a team always
	// matches under the version active for its partition at submission.
	if team.Version == "" {
		team.Version = p.cfg.Version
	} else if team.Version != p.cfg.Version {
		return types.SubmitResult{Reason: types.RejectVersionIncompatible, Detail: string(team.Version)}
	}

	seq := e.submitSeq.Add(1)
	p.mu.Lock()
	res := p.queue.Submit(team, seq)
	p.mu.Unlock()

	if !res.Accepted {
		e.audit.Record(audit.Event{
			Kind:      audit.EventTeamRejected,
			AtMs:      e.clk.NowMs(),
			Partition: pid,
			Team:      team.ID,
			Version:   team.Version,
			Reason:    string(res.Reason),
			Detail:    res.Detail,
		})
		return res
	}
	e.audit.Record(audit.Event{
		Kind:      audit.EventTeamSubmitted,
		AtMs:      e.clk.NowMs(),
		Partition: pid,
		Team:      team.ID,
		Version:   team.Version,
		Detail:    fmt.Sprintf("seq=%d", seq),
	})
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
	return res
}

// TickPartition runs one matchmaking pass for a partition on demand.
// The decision and the queue->session handoff happen entirely under the
// partition lock, so teams are never half-removed and two concurrent
// ticks cannot interleave decisions (determinism at any concurrency).
func (e *Engine) TickPartition(pid types.PartitionID) []types.SessionID {
	p, ok := e.partitions[pid]
	if !ok {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	entries := p.queue.Entries()
	if len(entries) == 0 {
		return nil
	}
	now := e.clk.NowMs()
	groups := match.Decide(entries, p.cfg, p.rules, now)
	if len(groups) == 0 {
		return nil
	}

	ids := make([]types.TeamID, 0)
	for _, g := range groups {
		for _, en := range g.Teams {
			ids = append(ids, en.Team.ID)
		}
	}
	taken := p.queue.Take(ids) // atomic removal in sequence order
	_ = taken

	created := make([]types.SessionID, 0, len(groups))
	for gi := range groups {
		g := groups[gi]
		n := e.sessionN.Add(1)
		sid := types.SessionID(fmt.Sprintf("s-%s-%d", pid, n))

		tms := make([]session.TeamMembers, 0, len(g.Teams))
		detailParts := make([]string, 0, len(g.Teams))
		// Preserve the ranking's team order but it is sorted for audit.
		sort.Slice(g.Teams, func(i, j int) bool { return g.Teams[i].Team.ID < g.Teams[j].Team.ID })
		for _, en := range g.Teams {
			players := make([]types.PlayerID, 0, len(en.Team.Players))
			for _, pl := range en.Team.Players {
				players = append(players, pl.ID)
			}
			tms = append(tms, session.TeamMembers{Team: en.Team.ID, Players: players, Version: en.Team.Version})
			detailParts = append(detailParts, fmt.Sprintf("%s(score=%d,seq=%d)", en.Team.ID, en.Score, en.Sequence))
		}
		opts := session.Options{
			ID:        sid,
			Partition: pid,
			Version:   p.cfg.Version,
			Teams:     tms,
			PC:        p.cfg,
			Broker:    e.broker,
			Audit:     e.audit,
			Clock:     e.clk,
			NowMs:     now,
			Fill:      e.backfillFor(p),
		}
		_, res := e.manager.Create(opts)
		if !res.Accepted {
			// Global session cap hit: restore this and later groups'
			// teams to their exact prior queue positions atomically.
			var restore []queue.Entry
			for _, g := range groups[gi:] {
				restore = append(restore, g.Teams...)
			}
			p.queue.Restore(restore)
			e.audit.Record(audit.Event{
				Kind: audit.EventTeamRejected, AtMs: now, Partition: pid,
				Session: sid, Reason: string(res.Reason), Detail: res.Detail,
			})
			break
		}
		// One matched event per participating team so the decision
		// basis is traceable from each object; Detail is identical.
		for _, en := range g.Teams {
			e.audit.Record(audit.Event{
				Kind:      audit.EventMatched,
				AtMs:      now,
				Partition: pid,
				Team:      en.Team.ID,
				Session:   sid,
				Version:   en.Team.Version,
				Detail:    joinDetail(detailParts),
			})
		}
		created = append(created, sid)
	}
	return created
}

// backfillFor builds the FillFunc bound to a partition. It searches the
// same queue (partition lock released during session policy handling),
// so fill candidates obey current queue rules. Existing queued teams
// keep their original version; the backfilled session stays on its own
// version but accepts a same-version replacement team.
func (e *Engine) backfillFor(p *partition) session.FillFunc {
	return func(pc config.PartitionConfig, needSize int, except types.TeamID) (queue.Entry, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		entries := p.queue.Entries()
		// Deterministic: earliest sequence, exact size, same version.
		for _, en := range entries {
			if en.Team.ID == except || len(en.Team.Players) != needSize {
				continue
			}
			if en.Team.Version != pc.Version {
				continue
			}
			taken := p.queue.Take([]types.TeamID{en.Team.ID})
			if len(taken) == 1 {
				return taken[0], true
			}
		}
		return queue.Entry{}, false
	}
}

func joinDetail(parts []string) string {
	out := ""
	for i, s := range parts {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

// Sessions returns live-session snapshots in id order.
func (e *Engine) Sessions() []types.SessionSnapshot { return e.manager.Snapshot() }

// QueueSnapshot returns the current queue view of a partition.
func (e *Engine) QueueSnapshot(pid types.PartitionID) []types.QueuedTeam {
	if p, ok := e.partitions[pid]; ok {
		return p.queue.Snapshot()
	}
	return nil
}

// RuleVersions returns registered rule versions for tracing.
func (e *Engine) RuleVersions() []rules.Rules {
	vs := e.registry.Versions()
	out := make([]rules.Rules, 0, len(vs))
	for _, v := range vs {
		if rs, ok := e.registry.Get(v); ok {
			out = append(out, rs)
		}
	}
	return out
}

// PartitionRules returns the active ruleset of a partition.
func (e *Engine) PartitionRules(pid types.PartitionID) (rules.Rules, bool) {
	p, ok := e.partitions[pid]
	if !ok {
		return nil, false
	}
	return p.rules, true
}

// Audit exposes the audit recorder for history/decision queries.
func (e *Engine) Audit() *audit.Recorder { return e.audit }

// Broker exposes the in-memory broker for test clients.
func (e *Engine) Broker() *transport.Broker { return e.broker }

// Manager exposes the session manager.
func (e *Engine) Manager() *session.Manager { return e.manager }

// Dial connects a player to a session: a paired client peer is returned
// and the server half is attached atomically (Join or Reconnect).
func (e *Engine) Dial(sid types.SessionID, player types.PlayerID) (*transport.Peer, types.SubmitResult) {
	s, ok := e.manager.Get(sid)
	if !ok {
		return nil, types.SubmitResult{Reason: types.RejectUnknownSession, Detail: string(sid)}
	}
	peer, sp := e.broker.Dial(player)
	var res types.SubmitResult
	switch {
	case !s.HasPlayer(player):
		res = s.Join(player, sp)
	case s.HasEverJoined(player):
		res = s.Reconnect(player, sp)
	default:
		res = s.Join(player, sp) // pre-seeded slot, first connection
	}
	if !res.Accepted {
		peer.Disconnect()
		return nil, res
	}
	return peer, res
}

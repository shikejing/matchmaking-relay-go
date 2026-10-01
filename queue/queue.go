// Package queue holds per-partition FIFO-ordered teams.
//
// Submission de-duplication: a team already queued is rejected as
// duplicate; the same team submitted concurrently or repeatedly
// produces at most one queue entry. Players are also unique across the
// whole partition, so the same player cannot enter via two teams.
// The queue is iterated strictly by globally monotonic submission
// sequence, so behavior never depends on Go map iteration order.
package queue

import (
	"sort"
	"sync"

	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/rules"
	"github.com/shikejing/matchmaking-relay-go/types"
)

// Entry is one queued team with its merged score and ordering metadata.
type Entry struct {
	Team       types.Team
	Score      int
	EnqueuedAt int64
	Sequence   uint64
}

// View converts an entry to the externally visible view.
func (e Entry) View(partition types.PartitionID) types.QueuedTeam {
	return types.QueuedTeam{
		TeamID:     e.Team.ID,
		Partition:  partition,
		Size:       len(e.Team.Players),
		Score:      e.Score,
		EnqueuedAt: e.EnqueuedAt,
		Sequence:   e.Sequence,
		Version:    e.Team.Version,
	}
}

// Queue is a capacity-bounded, sequence-ordered set of teams.
type Queue struct {
	cfg   config.PartitionConfig
	rs    rules.Rules
	clock clock.Clock

	mu      sync.Mutex
	order   []types.TeamID
	byTeam  map[types.TeamID]Entry
	players map[types.PlayerID]types.TeamID
}

// NewQueue creates a queue bound to one partition's ruleset.
func NewQueue(cfg config.PartitionConfig, rs rules.Rules, clk clock.Clock) *Queue {
	return &Queue{
		cfg:     cfg,
		rs:      rs,
		clock:   clk,
		byTeam:  map[types.TeamID]Entry{},
		players: map[types.PlayerID]types.TeamID{},
	}
}

func reject(reason types.RejectReason, detail string) types.SubmitResult {
	return types.SubmitResult{Reason: reason, Detail: detail}
}

// Submit adds a team atomically under one lock. Validation precedes any
// state change, so a rejected submission never partially enqueues.
func (q *Queue) Submit(team types.Team, sequence uint64) types.SubmitResult {
	if len(team.Players) == 0 {
		return reject(types.RejectEmptyTeam, string(team.ID))
	}
	if len(team.Players) > q.cfg.MaxTeamSize {
		return reject(types.RejectOversizedTeam, string(team.ID))
	}
	seenPlayer := map[types.PlayerID]bool{}
	for _, p := range team.Players {
		if p.ID == "" || seenPlayer[p.ID] {
			return reject(types.RejectEmptyTeam, string(team.ID)+":invalid-player")
		}
		seenPlayer[p.ID] = true
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.byTeam[team.ID]; exists {
		return reject(types.RejectDuplicateTeam, string(team.ID))
	}
	if len(q.order) >= q.cfg.QueueCapacity {
		return reject(types.RejectQueueFull, string(q.cfg.ID))
	}
	for _, p := range team.Players {
		if _, busy := q.players[p.ID]; busy {
			return reject(types.RejectPlayerAlreadyQueued, string(p.ID))
		}
	}

	score := q.rs.MergeScore(team.Players)
	e := Entry{
		Team:       team,
		Score:      score,
		EnqueuedAt: q.clock.NowMs(),
		Sequence:   sequence,
	}
	q.byTeam[team.ID] = e
	q.order = append(q.order, team.ID)
	for _, p := range team.Players {
		q.players[p.ID] = team.ID
	}
	return types.SubmitResult{Accepted: true, Sequence: sequence}
}

// Entries returns all queued teams in submission-sequence order.
func (q *Queue) Entries() []Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.entriesLocked()
}

func (q *Queue) entriesLocked() []Entry {
	out := make([]Entry, 0, len(q.order))
	for _, id := range q.order {
		out = append(out, q.byTeam[id])
	}
	return out
}

// Take atomically removes and returns the named entries in sequence
// order. Unknown ids are skipped. Teams move queue -> session in one
// locked step, so no half-removed state is ever visible.
func (q *Queue) Take(ids []types.TeamID) []Entry {
	q.mu.Lock()
	defer q.mu.Unlock()

	wanted := map[types.TeamID]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	out := make([]Entry, 0, len(ids))
	kept := q.order[:0]
	for _, id := range q.order {
		if wanted[id] {
			if e, ok := q.byTeam[id]; ok {
				out = append(out, e)
				delete(q.byTeam, id)
				for _, p := range e.Team.Players {
					delete(q.players, p.ID)
				}
			}
			continue
		}
		kept = append(kept, id)
	}
	q.order = kept
	return out
}

// HasPlayer reports which team (if any) currently owns a player slot.
func (q *Queue) HasPlayer(p types.PlayerID) (types.TeamID, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.players[p]
	return t, ok
}

// Restore reinserts entries previously Taken when a downstream step
// (e.g. session cap) aborts a tick. Entries are reinserted at their
// original sequence position, so queue order after an aborted tick is
// exactly the order before it. Existing ids and players are skipped.
func (q *Queue) Restore(entries []Entry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Insert in one pass sorted by sequence into the existing order.
	restored := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if _, exists := q.byTeam[e.Team.ID]; exists {
			continue
		}
		busy := false
		for _, p := range e.Team.Players {
			if _, ok := q.players[p.ID]; ok {
				busy = true
				break
			}
		}
		if busy {
			continue
		}
		restored = append(restored, e)
	}
	sort.Slice(restored, func(i, j int) bool { return restored[i].Sequence < restored[j].Sequence })
	for _, e := range restored {
		pos := sort.Search(len(q.order), func(i int) bool {
			return q.byTeam[q.order[i]].Sequence > e.Sequence
		})
		q.order = append(q.order, "")
		copy(q.order[pos+1:], q.order[pos:])
		q.order[pos] = e.Team.ID
		q.byTeam[e.Team.ID] = e
		for _, p := range e.Team.Players {
			q.players[p.ID] = e.Team.ID
		}
	}
}

// HasTeam reports whether the team is currently queued.
func (q *Queue) HasTeam(id types.TeamID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.byTeam[id]
	return ok
}

// Len is the current queue length.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// Snapshot returns the external queue view in deterministic order.
func (q *Queue) Snapshot() []types.QueuedTeam {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]types.QueuedTeam, 0, len(q.order))
	for _, e := range q.entriesLocked() {
		out = append(out, e.View(q.cfg.ID))
	}
	return out
}

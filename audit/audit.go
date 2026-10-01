// Package audit records an append-only, bounded history of state
// transitions and matchmaking decisions, making every state explainable
// and every problem reproducible.
//
// Storage is bounded: a global ring plus a per-key ring, each capped at
// the configured capacity, so memory cannot grow without bound.
package audit

import (
	"sync"

	"github.com/shikejing/matchmaking-relay-go/types"
)

// EventKind enumerates audited event types.
type EventKind string

const (
	EventTeamSubmitted      EventKind = "team_submitted"
	EventTeamRejected       EventKind = "team_rejected"
	EventMatched            EventKind = "matched"
	EventSessionCreated     EventKind = "session_created"
	EventMemberJoined       EventKind = "member_joined"
	EventMemberLeft         EventKind = "member_left"
	EventMemberDisconnected EventKind = "member_disconnected"
	EventMemberReconnected  EventKind = "member_reconnected"
	EventSessionClosed      EventKind = "session_closed"
	EventBackfill           EventKind = "backfill"
)

// Event is one immutable audit record.
type Event struct {
	Seq       uint64
	Kind      EventKind
	AtMs      int64
	Partition types.PartitionID
	Team      types.TeamID
	Session   types.SessionID
	Player    types.PlayerID
	Version   types.RuleVersion
	Reason    string
	Detail    string
}

// ring is a fixed-capacity FIFO that drops the oldest entries.
type ring struct {
	buf []Event
	cap int
}

func newRing(cap int) ring { return ring{cap: cap} }

func (r *ring) push(e Event) {
	if r.cap <= 0 {
		return
	}
	if len(r.buf) < r.cap {
		r.buf = append(r.buf, e)
		return
	}
	copy(r.buf, r.buf[1:])
	r.buf[r.cap-1] = e
}

func (r *ring) slice() []Event {
	out := make([]Event, len(r.buf))
	copy(out, r.buf)
	return out
}

// Recorder stores events in bounded per-object ring buffers plus a
// global ring; growth is therefore bounded under any load.
type Recorder struct {
	mu       sync.Mutex
	capacity int
	seq      uint64
	global   ring
	teams    map[types.TeamID]*ring
	sessions map[types.SessionID]*ring
	players  map[types.PlayerID]*ring
}

// NewRecorder constructs a Recorder with the given per-key capacity.
func NewRecorder(capacity int) *Recorder {
	if capacity < 1 {
		capacity = 1
	}
	return &Recorder{
		capacity: capacity,
		global:   newRing(capacity),
		teams:    map[types.TeamID]*ring{},
		sessions: map[types.SessionID]*ring{},
		players:  map[types.PlayerID]*ring{},
	}
}

// Record appends an event to the global ring and to each referenced
// object ring, then returns the globally monotonic sequence number.
func (r *Recorder) Record(e Event) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	e.Seq = r.seq
	r.global.push(e)
	if e.Team != "" {
		r.recordInto(r.teams, string(e.Team), e)
	}
	if e.Session != "" {
		r.recordIntoSessions(r.sessions, e.Session, e)
	}
	if e.Player != "" {
		r.recordIntoPlayers(r.players, e.Player, e)
	}
	return e.Seq
}

func (r *Recorder) recordInto(m map[types.TeamID]*ring, id string, e Event) {
	k := types.TeamID(id)
	rg, ok := m[k]
	if !ok {
		rg = &ring{cap: r.capacity}
		m[k] = rg
	}
	rg.push(e)
}

func (r *Recorder) recordIntoSessions(m map[types.SessionID]*ring, id types.SessionID, e Event) {
	rg, ok := m[id]
	if !ok {
		rg = &ring{cap: r.capacity}
		m[id] = rg
	}
	rg.push(e)
}

func (r *Recorder) recordIntoPlayers(m map[types.PlayerID]*ring, id types.PlayerID, e Event) {
	rg, ok := m[id]
	if !ok {
		rg = &ring{cap: r.capacity}
		m[id] = rg
	}
	rg.push(e)
}

// TeamHistory returns events related to a team, oldest first.
func (r *Recorder) TeamHistory(id types.TeamID) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rg, ok := r.teams[id]; ok {
		return rg.slice()
	}
	return nil
}

// SessionHistory returns the full transition history of a session.
func (r *Recorder) SessionHistory(id types.SessionID) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rg, ok := r.sessions[id]; ok {
		return rg.slice()
	}
	return nil
}

// PlayerHistory returns events related to a player.
func (r *Recorder) PlayerHistory(id types.PlayerID) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rg, ok := r.players[id]; ok {
		return rg.slice()
	}
	return nil
}

// GlobalEvents returns up to limit most recent global events in
// sequence order (oldest first within the window).
func (r *Recorder) GlobalEvents(limit int) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := r.global.slice()
	if limit > 0 && len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all
}

package session

import (
	"sort"
	"sync"

	"github.com/shikejing/matchmaking-relay-go/audit"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/transport"
	"github.com/shikejing/matchmaking-relay-go/types"
)

// Manager owns all live sessions and enforces the global session cap.
// Each read returns per-session point-in-time consistent snapshots.
type Manager struct {
	cfg    config.Config
	broker *transport.Broker
	audit  *audit.Recorder

	mu       sync.Mutex
	sessions map[types.SessionID]*Session
}

// NewManager constructs a Manager.
func NewManager(cfg config.Config, broker *transport.Broker, rec *audit.Recorder) *Manager {
	return &Manager{
		cfg:      cfg,
		broker:   broker,
		audit:    rec,
		sessions: map[types.SessionID]*Session{},
	}
}

// Create registers a session if below the global cap and its id is
// unused; otherwise returns a distinguishable rejection result.
func (m *Manager) Create(opts Options) (*Session, types.SubmitResult) {
	if opts.ID == "" {
		return nil, types.SubmitResult{Reason: types.RejectInvalidConfig, Detail: "empty session id"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.sessions[opts.ID]; exists {
		return nil, types.SubmitResult{Reason: types.RejectDuplicateTeam, Detail: string(opts.ID)}
	}
	if len(m.sessions) >= m.cfg.MaxSessions {
		return nil, types.SubmitResult{Reason: types.RejectTooManySessions, Detail: string(opts.Partition)}
	}
	s := NewSession(opts)
	m.sessions[opts.ID] = s
	if m.audit != nil {
		m.audit.Record(audit.Event{
			Kind:      audit.EventSessionCreated,
			AtMs:      opts.NowMs,
			Partition: opts.Partition,
			Session:   opts.ID,
			Version:   opts.Version,
		})
	}
	return s, types.SubmitResult{Accepted: true}
}

// Get returns the live session.
func (m *Manager) Get(id types.SessionID) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	return s, ok
}

// Remove evicts a session from the manager. The session itself is
// closed first so no externally visible session exists without a close.
func (m *Manager) Remove(id types.SessionID, reason types.CloseReason) {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	if ok {
		s.Close(reason)
	}
}

// ReapClosed removes and returns ids of sessions already in a terminal
// state; the closed snapshot remains auditable via history.
func (m *Manager) ReapClosed() []types.SessionID {
	m.mu.Lock()
	var dead []types.SessionID
	for id, s := range m.sessions {
		if s.Done() {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
	sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })
	return dead
}

// Tick advances every live session; sessions that finish as a result
// are evicted within the same pass. Partitions/sessions are ticked in
// id order; each session transitions atomically on its own lock.
func (m *Manager) Tick(nowMs int64) {
	m.mu.Lock()
	ids := make([]types.SessionID, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		m.mu.Lock()
		s, ok := m.sessions[id]
		m.mu.Unlock()
		if !ok {
			continue
		}
		s.Tick(nowMs)
		if s.Done() {
			m.Remove(id, types.CloseEvicted)
		}
	}
}

// Snapshot returns snapshots of all live sessions in session-id order.
// Each snapshot is one atomic state+membership view.
func (m *Manager) Snapshot() []types.SessionSnapshot {
	m.mu.Lock()
	ids := make([]types.SessionID, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]types.SessionSnapshot, 0, len(ids))
	for _, id := range ids {
		m.mu.Lock()
		s := m.sessions[id]
		m.mu.Unlock()
		if s == nil {
			continue
		}
		out = append(out, s.Snapshot())
	}
	return out
}

// ActiveCount is the number of registered (non-reaped) sessions.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

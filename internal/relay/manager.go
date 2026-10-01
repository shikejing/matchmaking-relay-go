package relay

import (
	"fmt"
	"sort"
	"sync"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
)

// Reject codes used by the relay package (mirrors match.RejectCode semantics).
const (
	RejectSessionAtCapacity      = "SESSION_AT_CAPACITY"
	RejectSessionNotFound        = "SESSION_NOT_FOUND"
	RejectSessionEnded           = "SESSION_ENDED"
	RejectMemberNotFound         = "MEMBER_NOT_FOUND"
	RejectMemberNotDisconnected  = "MEMBER_NOT_DISCONNECTED"
	RejectMemberAlreadyJoined    = "MEMBER_ALREADY_JOINED"
	RejectMemberAlreadyLeft      = "MEMBER_ALREADY_LEFT"
	RejectClientSlow             = "CLIENT_SLOW"
	RejectBackfillNotRequested   = "BACKFILL_NOT_REQUESTED"
	RejectReconnectWindowExpired = "RECONNECT_WINDOW_EXPIRED"
)

// BackfillRequest is raised by the relay tick when a disconnected member's
// retention window expires under a backfill-enabled rule.
type BackfillRequest struct {
	SessionID model.SessionID
	Partition model.PartitionID
	Slots     int
}

// Manager owns all relay sessions and enforces their lifecycle transitions.
type Manager struct {
	cfg   config.Config
	rules *rule.Registry
	audit *audit.Log

	mu          sync.RWMutex
	sessions    map[model.SessionID]*model.Session
	byPartition map[model.PartitionID][]model.SessionID
	activeCount int
	nextSession int64

	pipes map[model.SessionID]map[model.PlayerID]chan model.Message
}

// NewManager creates a relay manager.
func NewManager(cfg config.Config, rules *rule.Registry, auditLog *audit.Log) *Manager {
	return &Manager{
		cfg:         cfg,
		rules:       rules,
		audit:       auditLog,
		sessions:    make(map[model.SessionID]*model.Session),
		byPartition: make(map[model.PartitionID][]model.SessionID),
		pipes:       make(map[model.SessionID]map[model.PlayerID]chan model.Message),
	}
}

// CreateSession establishes a session for a formed match. It returns
// ErrSessionAtCapacity when the global active-session limit is reached; the
// caller (engine) then leaves teams queued instead of forcing creation.
func (m *Manager) CreateSession(partition model.PartitionID, matchID string, ruleVersion int, teams []*model.Team, tick int64) (*SessionView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeCount >= m.cfg.MaxConcurrentSessions {
		m.audit.Append(audit.Record{
			Type:      audit.EventReject,
			Partition: partition,
			Reason:    RejectSessionAtCapacity,
			Input:     fmt.Sprintf(`{"matchID":%q,"active":%d,"limit":%d}`, matchID, m.activeCount, m.cfg.MaxConcurrentSessions),
		})
		return nil, fmt.Errorf("%s: active sessions %d at limit %d", RejectSessionAtCapacity, m.activeCount, m.cfg.MaxConcurrentSessions)
	}

	m.nextSession++
	sessionID := model.SessionID(fmt.Sprintf("s-%d", m.nextSession))

	members := make([]model.Member, 0)
	memberIDs := make([]model.PlayerID, 0)
	for _, team := range teams {
		for _, p := range team.Members {
			members = append(members, model.Member{
				PlayerID:   p.ID,
				TeamID:     team.ID,
				State:      model.MemberJoined,
				JoinedTick: tick,
			})
			memberIDs = append(memberIDs, p.ID)
		}
	}

	session := &model.Session{
		ID:          sessionID,
		Partition:   partition,
		RuleVersion: ruleVersion,
		State:       model.SessionActive,
		Members:     members,
		CreatedTick: tick,
		MatchID:     matchID,
	}
	m.sessions[sessionID] = session
	m.byPartition[partition] = append(m.byPartition[partition], sessionID)
	m.pipes[sessionID] = make(map[model.PlayerID]chan model.Message)
	m.activeCount++

	m.audit.Append(audit.Record{
		Type:        audit.EventSessionCreated,
		Partition:   partition,
		SessionID:   sessionID,
		RuleVersion: ruleVersion,
		Input:       fmt.Sprintf(`{"matchID":%q,"members":%q,"ruleVersion":%d}`, matchID, memberIDs, ruleVersion),
		Decision:    "session created active with all match members joined",
	})
	return ptrView(toSessionView(session)), nil
}

// GetSession returns an immutable snapshot of a session.
func (m *Manager) GetSession(id model.SessionID) (SessionView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	if !ok {
		return SessionView{}, false
	}
	return toSessionView(s), true
}

// ListSessions returns snapshots of every session in a partition (or all
// partitions when partition is empty), in creation order.
func (m *Manager) ListSessions(partition model.PartitionID) []SessionView {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]model.SessionID, 0)
	if partition != "" {
		ids = append(ids, m.byPartition[partition]...)
	} else {
		for _, pid := range m.byPartitionOrderLocked() {
			ids = append(ids, m.byPartition[pid]...)
		}
	}
	out := make([]SessionView, 0, len(ids))
	for _, id := range ids {
		if s, ok := m.sessions[id]; ok {
			out = append(out, toSessionView(s))
		}
	}
	return out
}

// ActiveSessionCount returns the number of non-ended sessions.
func (m *Manager) ActiveSessionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeCount
}

// Disconnect marks a joined member as disconnected.
func (m *Manager) Disconnect(sessionID model.SessionID, playerID model.PlayerID, tick int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.requireActive(sessionID)
	if err != nil {
		return err
	}
	mem, err := m.requireMember(s, playerID)
	if err != nil {
		return err
	}
	if mem.State != model.MemberJoined {
		return fmt.Errorf("%s: member %s is not joined (state=%d)", RejectMemberNotDisconnected, playerID, mem.State)
	}
	mem.State = model.MemberDisconnected
	mem.DisconnectTick = tick
	m.audit.Append(audit.Record{
		Type:      audit.EventMemberDisconnected,
		SessionID: sessionID,
		PlayerID:  playerID,
		Input:     fmt.Sprintf(`{"disconnectTick":%d}`, tick),
	})
	return nil
}

// Reconnect restores a disconnected member within the retention window.
func (m *Manager) Reconnect(sessionID model.SessionID, playerID model.PlayerID, tick int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.requireActive(sessionID)
	if err != nil {
		return err
	}
	mem, err := m.requireMember(s, playerID)
	if err != nil {
		return err
	}
	if mem.State != model.MemberDisconnected {
		return fmt.Errorf("%s: member %s is not disconnected (state=%d)", RejectMemberNotDisconnected, playerID, mem.State)
	}
	ruleInst, err := m.rules.Get(s.RuleVersion)
	if err != nil {
		return err
	}
	if tick-mem.DisconnectTick > int64(ruleInst.RetentionTicks()) {
		return fmt.Errorf("%s: member %s disconnected at tick %d, retention %d expired by tick %d",
			RejectReconnectWindowExpired, playerID, mem.DisconnectTick, ruleInst.RetentionTicks(), tick)
	}
	mem.State = model.MemberJoined
	mem.DisconnectTick = 0
	m.audit.Append(audit.Record{
		Type:      audit.EventMemberReconnected,
		SessionID: sessionID,
		PlayerID:  playerID,
		Input:     fmt.Sprintf(`{"reconnectTick":%d,"disconnectTick":%d}`, tick, mem.DisconnectTick),
	})
	return nil
}

// Leave permanently removes a member. When backfill is enabled and other
// members remain, a backfill request is returned for the engine to queue;
// when no members remain the session ends.
func (m *Manager) Leave(sessionID model.SessionID, playerID model.PlayerID, tick int64) ([]BackfillRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.requireActive(sessionID)
	if err != nil {
		return nil, err
	}
	mem, err := m.requireMember(s, playerID)
	if err != nil {
		return nil, err
	}
	if mem.State == model.MemberLeft {
		return nil, fmt.Errorf("%s: member %s already left", RejectMemberAlreadyLeft, playerID)
	}
	mem.State = model.MemberLeft
	m.audit.Append(audit.Record{
		Type:      audit.EventMemberLeft,
		SessionID: sessionID,
		PlayerID:  playerID,
		Input:     fmt.Sprintf(`{"leaveTick":%d,"reason":"explicit-leave"}`, tick),
	})
	return m.afterMemberVacated(s, tick, "explicit-leave")
}

// EndSession terminates a session.
func (m *Manager) EndSession(sessionID model.SessionID, tick int64, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.endSessionLocked(sessionID, tick, reason)
}

// Tick advances session-side timeouts for every partition. It is used by
// manual (single-threaded) stepping; the background loop uses TickPartition.
func (m *Manager) Tick(tick int64) []BackfillRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]BackfillRequest, 0)
	for _, pid := range m.byPartitionOrderLocked() {
		out = append(out, m.tickPartitionLocked(pid, tick)...)
	}
	return out
}

// TickPartition advances session-side timeouts for one partition only. It
// returns backfill requests for that partition. Called by each partition's
// independent goroutine, so partitions never interfere with each other.
func (m *Manager) TickPartition(partition model.PartitionID, tick int64) []BackfillRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tickPartitionLocked(partition, tick)
}

func (m *Manager) tickPartitionLocked(partition model.PartitionID, tick int64) []BackfillRequest {
	out := make([]BackfillRequest, 0)
	for _, sid := range m.byPartition[partition] {
		s := m.sessions[sid]
		if s == nil || s.State != model.SessionActive {
			continue
		}
		ruleInst, err := m.rules.Get(s.RuleVersion)
		if err != nil {
			continue
		}
		for i := range s.Members {
			mem := &s.Members[i]
			if mem.State != model.MemberDisconnected {
				continue
			}
			if tick-mem.DisconnectTick <= int64(ruleInst.RetentionTicks()) {
				continue
			}
			if ruleInst.BackfillEnabled() {
				mem.State = model.MemberLeft
				m.audit.Append(audit.Record{
					Type:      audit.EventMemberLeft,
					SessionID: s.ID,
					PlayerID:  mem.PlayerID,
					Input:     fmt.Sprintf(`{"timeoutTick":%d,"reason":"retention-timeout-backfill"}`, tick),
				})
			} else {
				mem.State = model.MemberLeft
				m.audit.Append(audit.Record{
					Type:      audit.EventMemberLeft,
					SessionID: s.ID,
					PlayerID:  mem.PlayerID,
					Input:     fmt.Sprintf(`{"timeoutTick":%d,"reason":"retention-timeout-end"}`, tick),
				})
			}
		}
		if ruleInst.BackfillEnabled() {
			if reqs := m.openBackfillIfNeeded(s, tick); len(reqs) > 0 {
				out = append(out, reqs...)
			}
		} else {
			// Any vacated slot under a no-backfill rule ends the session.
			vacated := 0
			for _, mem := range s.Members {
				if mem.State == model.MemberLeft {
					vacated++
				}
			}
			if vacated > 0 {
				_ = m.endSessionLocked(s.ID, tick, "retention-timeout: backfill disabled by rule v"+fmt.Sprint(ruleInst.Version()))
			}
		}
	}
	return out
}

// CompleteBackfill adds a replacement team to a session that requested
// backfill.
func (m *Manager) CompleteBackfill(sessionID model.SessionID, ticketID string, team *model.Team, tick int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.requireActive(sessionID)
	if err != nil {
		return err
	}
	if s.BackfillTicketID == "" {
		return fmt.Errorf("%s: session %s has no open backfill ticket", RejectBackfillNotRequested, sessionID)
	}
	for _, p := range team.Members {
		s.Members = append(s.Members, model.Member{
			PlayerID:   p.ID,
			TeamID:     team.ID,
			State:      model.MemberJoined,
			JoinedTick: tick,
		})
		m.audit.Append(audit.Record{
			Type:        audit.EventMemberJoined,
			SessionID:   sessionID,
			PlayerID:    p.ID,
			RuleVersion: team.RuleVersion,
			Input:       fmt.Sprintf(`{"joinTick":%d,"teamID":%q,"backfillTicket":%q}`, tick, team.ID, ticketID),
		})
	}
	s.BackfillTicketID = ""
	return nil
}

// RegisterPipe attaches an outbound message channel for a member.
func (m *Manager) RegisterPipe(sessionID model.SessionID, playerID model.PlayerID, ch chan model.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pipes[sessionID]; !ok {
		m.pipes[sessionID] = make(map[model.PlayerID]chan model.Message)
	}
	m.pipes[sessionID][playerID] = ch
}

// Broadcast delivers a message to every joined member of the session. Slow
// consumers (full buffers) are audited and skipped; the relay never blocks and
// never grows memory without bound.
func (m *Manager) Broadcast(sessionID model.SessionID, from model.PlayerID, payload []byte, tick int64) error {
	m.mu.Lock()
	s, err := m.requireActive(sessionID)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	chans := make([]chan model.Message, 0)
	for _, mem := range s.Members {
		if mem.State != model.MemberJoined {
			continue
		}
		if ch, ok := m.pipes[sessionID][mem.PlayerID]; ok {
			chans = append(chans, ch)
		}
	}
	s.MessageCount++
	m.mu.Unlock()

	msg := model.Message{SessionID: sessionID, From: from, Payload: payload, Tick: tick}
	for _, ch := range chans {
		select {
		case ch <- msg:
		default:
			m.audit.Append(audit.Record{
				Type:      audit.EventReject,
				SessionID: sessionID,
				Reason:    RejectClientSlow,
				Input:     fmt.Sprintf(`{"bufferSize":%d}`, cap(ch)),
				Decision:  "message dropped for slow consumer",
			})
		}
	}
	return nil
}

// ---- internal helpers ----

func (m *Manager) requireActive(sessionID model.SessionID) (*model.Session, error) {
	s, ok := m.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%s: session %s not found", RejectSessionNotFound, sessionID)
	}
	if s.State != model.SessionActive {
		return nil, fmt.Errorf("%s: session %s is not active (state=%d)", RejectSessionEnded, sessionID, s.State)
	}
	return s, nil
}

func (m *Manager) requireMember(s *model.Session, playerID model.PlayerID) (*model.Member, error) {
	for i := range s.Members {
		if s.Members[i].PlayerID == playerID {
			return &s.Members[i], nil
		}
	}
	return nil, fmt.Errorf("%s: player %s is not in session %s", RejectMemberNotFound, playerID, s.ID)
}

func (m *Manager) afterMemberVacated(s *model.Session, tick int64, reason string) ([]BackfillRequest, error) {
	joined := 0
	for _, mem := range s.Members {
		if mem.State == model.MemberJoined {
			joined++
		}
	}
	if joined == 0 {
		_ = m.endSessionLocked(s.ID, tick, reason+": no joined members remain")
		return nil, nil
	}
	ruleInst, err := m.rules.Get(s.RuleVersion)
	if err != nil {
		return nil, err
	}
	if !ruleInst.BackfillEnabled() {
		_ = m.endSessionLocked(s.ID, tick, reason+": backfill disabled by rule")
		return nil, nil
	}
	return m.openBackfillIfNeeded(s, tick), nil
}

func (m *Manager) openBackfillIfNeeded(s *model.Session, tick int64) []BackfillRequest {
	if s.BackfillTicketID != "" {
		return nil
	}
	slots := 0
	for _, mem := range s.Members {
		if mem.State == model.MemberLeft {
			slots++
		}
	}
	if slots == 0 {
		return nil
	}
	s.BackfillTicketID = "pending"
	m.audit.Append(audit.Record{
		Type:      audit.EventBackfillStarted,
		SessionID: s.ID,
		Input:     fmt.Sprintf(`{"slots":%d,"tick":%d}`, slots, tick),
		Decision:  "vacated slot(s) under backfill-enabled rule",
	})
	return []BackfillRequest{{SessionID: s.ID, Partition: s.Partition, Slots: slots}}
}

func (m *Manager) endSessionLocked(sessionID model.SessionID, tick int64, reason string) error {
	s, ok := m.sessions[sessionID]
	if !ok {
		return fmt.Errorf("%s: session %s not found", RejectSessionNotFound, sessionID)
	}
	if s.State == model.SessionEnded {
		return fmt.Errorf("%s: session %s already ended", RejectSessionEnded, sessionID)
	}
	s.State = model.SessionEnded
	s.EndedTick = tick
	m.activeCount--
	for _, ch := range m.pipes[sessionID] {
		close(ch)
	}
	delete(m.pipes, sessionID)
	m.audit.Append(audit.Record{
		Type:        audit.EventSessionEnded,
		Partition:   s.Partition,
		SessionID:   sessionID,
		RuleVersion: s.RuleVersion,
		Input:       fmt.Sprintf(`{"endedTick":%d,"reason":%q,"messages":%d}`, tick, reason, s.MessageCount),
		Decision:    reason,
	})
	return nil
}

func (m *Manager) sessionIDsLocked() []model.SessionID {
	out := make([]model.SessionID, 0, len(m.sessions))
	for id := range m.sessions {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (m *Manager) byPartitionOrderLocked() []model.PartitionID {
	out := make([]model.PartitionID, 0, len(m.byPartition))
	for id := range m.byPartition {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func ptrView(v SessionView) *SessionView { return &v }

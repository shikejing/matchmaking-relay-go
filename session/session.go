// Package session implements the relay session lifecycle, reconnect
// window handling and in-process message fan-out.
//
// State machine:
//
//	gathering -> active                 (all members have joined once)
//	active    -> degraded               (a member disconnects)
//	degraded  -> active                 (all present again / backfilled)
//	any       -> closed                 (terminal)
//
// Reconnect window outcomes when a disconnected slot expires:
//
//	end_session: the whole session closes (disconnect_timeout);
//	hold_slot:    the slot stays until reconnect (no auto-close);
//	backfill:     the disconnected member's entire team is removed and
//	              atomically replaced by a compatible queued team; if no
//	             replacement exists, the session closes (backfill_failed).
//
// Every externally visible read returns a single self-consistent
// snapshot of state plus the full member set; no half-updated view
// exists. A reconnecting member is never double-counted: membership is
// keyed by player id and each join supersedes the prior pipe.
package session

import (
	"sort"
	"sync"

	"github.com/shikejing/matchmaking-relay-go/audit"
	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/queue"
	"github.com/shikejing/matchmaking-relay-go/transport"
	"github.com/shikejing/matchmaking-relay-go/types"
)

// FillFunc supplies one replacement team for backfill. It must return a
// queued entry whose player count equals needSize for the partition's
// current rule version, or ok=false. It is invoked without holding the
// session lock, so the queue may run its own matchmaking safely.
type FillFunc func(pc config.PartitionConfig, needSize int, exceptTeam types.TeamID) (queue.Entry, bool)

type member struct {
	id         types.PlayerID
	team       types.TeamID
	version    types.RuleVersion
	state      types.MemberState
	pipe       *transport.ServerPipe
	connected  bool
	everJoined bool  // joined at least once (drives gathering->active)
	discoAt    int64 // 0 while present
}

// Session is one relay room with an atomic membership view.
type Session struct {
	opts Options

	mu      sync.Mutex
	members map[types.PlayerID]*member
	state   types.SessionState
	closeR  types.CloseReason
	created int64
	updated int64
	closed  bool
}

// Options carries construction dependencies.
type Options struct {
	ID        types.SessionID
	Partition types.PartitionID
	Version   types.RuleVersion
	Teams     []TeamMembers
	PC        config.PartitionConfig
	Broker    *transport.Broker
	Audit     *audit.Recorder
	Clock     clock.Clock
	Fill      FillFunc
	NowMs     int64
}

// TeamMembers groups the players of one matched team.
type TeamMembers struct {
	Team    types.TeamID
	Players []types.PlayerID
	Version types.RuleVersion
}

// NewSession constructs a session in the gathering state. Membership is
// pre-seeded from the matched teams; players become present on Join.
func NewSession(opts Options) *Session {
	s := &Session{
		opts:    opts,
		members: map[types.PlayerID]*member{},
		state:   types.SessionGathering,
		created: opts.NowMs,
		updated: opts.NowMs,
	}
	for _, tm := range opts.Teams {
		v := tm.Version
		if v == "" {
			v = opts.Version
		}
		for _, p := range tm.Players {
			s.members[p] = &member{id: p, team: tm.Team, version: v, state: types.MemberDisconnected}
		}
	}
	// If the session is created with no pre-join requirement it stays
	// gathering until the first Join transitions it via recompute.
	return s
}

// ID returns the session id.
func (s *Session) ID() types.SessionID { return s.opts.ID }

func (s *Session) record(kind audit.EventKind, p types.PlayerID, t types.TeamID, reason, detail string) {
	if s.opts.Audit == nil {
		return
	}
	s.opts.Audit.Record(audit.Event{
		Kind:      kind,
		AtMs:      s.opts.Clock.NowMs(),
		Partition: s.opts.Partition,
		Team:      t,
		Session:   s.opts.ID,
		Player:    p,
		Version:   s.opts.Version,
		Reason:    reason,
		Detail:    detail,
	})
}

// Join connects a member with a freshly dialed server pipe. Joining an
// existing member replaces the old pipe and never double-counts the
// player; unknown players are rejected.
func (s *Session) Join(player types.PlayerID, pipe *transport.ServerPipe) types.SubmitResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return types.SubmitResult{Reason: types.RejectUnknownSession, Detail: string(s.opts.ID)}
	}
	m, ok := s.members[player]
	if !ok {
		return types.SubmitResult{Reason: types.RejectUnknownSession, Detail: string(player)}
	}
	m.pipe = pipe
	m.connected = true
	m.everJoined = true
	m.state = types.MemberPresent
	m.discoAt = 0
	s.updated = s.opts.Clock.NowMs()
	s.recomputeStateLocked()
	s.record(audit.EventMemberJoined, player, m.team, "", "")
	go s.pump(pipe)
	return types.SubmitResult{Accepted: true}
}

// pump forwards one member's uplink into the fan-out until its pipe
// closes or the session ends.
func (s *Session) pump(pipe *transport.ServerPipe) {
	for m := range pipe.In() {
		s.relay(m)
	}
	s.mu.Lock()
	// Only treat as disconnect if this is still the active pipe.
	if cur, ok := s.members[pipe.Player()]; ok && cur.pipe == pipe && cur.connected {
		s.mu.Unlock()
		s.Disconnect(pipe.Player())
		return
	}
	s.mu.Unlock()
}

// Leave removes a member deliberately. When the live member set becomes
// empty the session closes with reason normal.
func (s *Session) Leave(player types.PlayerID) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	m, ok := s.members[player]
	if !ok {
		s.mu.Unlock()
		return
	}
	team := m.team
	delete(s.members, player)
	s.updated = s.opts.Clock.NowMs()
	s.record(audit.EventMemberLeft, player, team, "", "")
	if len(s.members) == 0 {
		s.closeLocked(types.CloseNormal)
		s.mu.Unlock()
		return
	}
	s.recomputeStateLocked()
	s.mu.Unlock()
}

// Disconnect marks a member dropped and starts the hold window.
func (s *Session) Disconnect(player types.PlayerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	m, ok := s.members[player]
	if !ok || !m.connected {
		return
	}
	m.connected = false
	m.state = types.MemberDisconnected
	m.discoAt = s.opts.Clock.NowMs()
	m.pipe = nil
	s.updated = m.discoAt
	if s.state == types.SessionActive {
		s.state = types.SessionDegraded
	}
	s.record(audit.EventMemberDisconnected, player, m.team, "", "")
}

// Reconnect restores a dropped member within the hold window using a
// new connection. A duplicate reconnect while already present, an
// unknown player or a slot that was already removed/backfilled is
// rejected, so membership is never double-counted.
func (s *Session) Reconnect(player types.PlayerID, pipe *transport.ServerPipe) types.SubmitResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return types.SubmitResult{Reason: types.RejectUnknownSession, Detail: string(s.opts.ID)}
	}
	m, ok := s.members[player]
	if !ok {
		return types.SubmitResult{Reason: types.RejectUnknownSession, Detail: string(player)}
	}
	if m.connected {
		return types.SubmitResult{Reason: types.RejectDuplicateTeam, Detail: string(player)}
	}
	if m.state == types.MemberRemoved {
		return types.SubmitResult{Reason: types.RejectBackfillUnavailable, Detail: string(player)}
	}
	if s.opts.PC.ReconnectHoldMs > 0 && s.opts.Clock.NowMs()-m.discoAt > s.opts.PC.ReconnectHoldMs {
		return types.SubmitResult{Reason: types.RejectBackfillUnavailable, Detail: string(player) + ":window_expired"}
	}
	m.pipe = pipe
	m.connected = true
	m.everJoined = true
	m.state = types.MemberPresent
	m.discoAt = 0
	s.updated = s.opts.Clock.NowMs()
	s.recomputeStateLocked()
	s.record(audit.EventMemberReconnected, player, m.team, "", "")
	go s.pump(pipe)
	return types.SubmitResult{Accepted: true}
}

// recomputeStateLocked derives the session state from member states:
// gathering until every member has connected at least once, degraded
// while anyone is disconnected, active otherwise.
func (s *Session) recomputeStateLocked() {
	if s.closed {
		return
	}
	anyDisco := false
	for _, m := range s.members {
		if !m.connected || !m.everJoined {
			anyDisco = true
		}
	}
	if s.state == types.SessionGathering {
		// Stay in gathering until every member has joined at least once.
		for _, m := range s.members {
			if !m.everJoined {
				return
			}
		}
	}
	if anyDisco {
		s.state = types.SessionDegraded
	} else {
		s.state = types.SessionActive
	}
}

// Tick advances reconnect windows and applies the configured timeout
// policy atomically per session.
func (s *Session) Tick(nowMs int64) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var expired []types.PlayerID
	for _, m := range s.members {
		if m.everJoined && !m.connected && m.state == types.MemberDisconnected &&
			s.opts.PC.ReconnectHoldMs > 0 && nowMs-m.discoAt > s.opts.PC.ReconnectHoldMs {
			expired = append(expired, m.id)
		}
	}
	if len(expired) == 0 {
		s.mu.Unlock()
		return
	}

	switch s.opts.PC.Reconnect {
	case config.PolicyHoldSlot:
		s.mu.Unlock()
		return
	case config.PolicyEndSession:
		s.closeLocked(types.CloseDisconnectTimeout)
		s.mu.Unlock()
		return
	case config.PolicyBackfill:
		// Determine the unique affected teams and needed sizes.
		teamSet := map[types.TeamID]bool{}
		for _, p := range expired {
			teamSet[s.members[p].team] = true
		}
		teams := make([]types.TeamID, 0, len(teamSet))
		for t := range teamSet {
			teams = append(teams, t)
		}
		sort.Slice(teams, func(i, j int) bool { return teams[i] < teams[j] })
		s.mu.Unlock()

		for _, t := range teams {
			if !s.backfillTeam(t, nowMs) {
				s.mu.Lock()
				s.closeLocked(types.CloseBackfillFailed)
				s.mu.Unlock()
				return
			}
		}
	}
}

// backfillTeam removes one whole team's slots and injects a replacement
// in a single locked update.
func (s *Session) backfillTeam(old types.TeamID, nowMs int64) bool {
	var need int
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	for _, m := range s.members {
		if m.team == old {
			need++
		}
	}
	s.mu.Unlock()
	if need == 0 {
		return true
	}
	if s.opts.Fill == nil {
		return false
	}
	entry, ok := s.opts.Fill(s.opts.PC, need, old)
	if !ok || len(entry.Team.Players) != need {
		s.record(audit.EventBackfill, "", old, "unavailable", "")
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	// Remove old team members.
	for pid, m := range s.members {
		if m.team == old {
			m.state = types.MemberRemoved
			delete(s.members, pid)
		}
	}
	// Inject replacement members as disconnected pending reconnect/join.
	for _, p := range entry.Team.Players {
		s.members[p.ID] = &member{
			id:      p.ID,
			team:    entry.Team.ID,
			version: entry.Team.Version,
			state:   types.MemberDisconnected,
			discoAt: 0,
		}
	}
	s.updated = nowMs
	s.state = types.SessionGathering // awaiting replacement members to join
	s.record(audit.EventBackfill, "", entry.Team.ID, "replaced", string(old))
	return true
}

// Snapshot returns one atomic state+membership view. Members are sorted
// by player id so snapshot diffs are reproducible.
func (s *Session) Snapshot() types.SessionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := make([]types.MemberSnapshot, 0, len(s.members))
	for _, m := range s.members {
		members = append(members, types.MemberSnapshot{
			Player: m.id, Team: m.team, State: m.state, Version: m.version,
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Player < members[j].Player })
	return types.SessionSnapshot{
		ID:          s.opts.ID,
		Partition:   s.opts.Partition,
		State:       s.state,
		Version:     s.opts.Version,
		Members:     members,
		CloseReason: s.closeR,
		CreatedAt:   s.created,
		UpdatedAt:   s.updated,
	}
}

// Close terminates the session with a reason.
func (s *Session) Close(reason types.CloseReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(reason)
}

func (s *Session) closeLocked(reason types.CloseReason) {
	if s.closed {
		return
	}
	s.closed = true
	s.state = types.SessionClosed
	if reason == types.CloseNotClosed {
		reason = types.CloseNormal
	}
	s.closeR = reason
	s.updated = s.opts.Clock.NowMs()
	s.record(audit.EventSessionClosed, "", "", string(reason), "")
}

// Done reports whether the session reached a terminal state.
func (s *Session) Done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// MemberCount reports the number of members currently present.
func (s *Session) MemberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.members {
		if m.connected {
			n++
		}
	}
	return n
}

// relay fans a message out: direct message to the target, otherwise
// broadcast to every other present member. Sends are non-blocking, so a
// slow consumer cannot stall the session.
func (s *Session) relay(m transport.Message) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	targets := make([]*transport.ServerPipe, 0, len(s.members))
	for _, mem := range s.members {
		if !mem.connected || mem.pipe == nil || mem.id == m.From {
			continue
		}
		if m.To != "" && mem.id != m.To {
			continue
		}
		targets = append(targets, mem.pipe)
	}
	s.mu.Unlock()
	out := m
	for _, p := range targets {
		p.Send(out)
	}
}

// Send relays an inbound message from outside the pumps (test helper).
func (s *Session) Send(m transport.Message) { s.relay(m) }

// HasPlayer reports membership, including disconnected slots.
func (s *Session) HasPlayer(p types.PlayerID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.members[p]
	return ok
}

// HasEverJoined reports whether the player previously connected at
// least once; it distinguishes an initial Join from a Reconnect.
func (s *Session) HasEverJoined(p types.PlayerID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.members[p]; ok {
		return m.everJoined
	}
	return false
}

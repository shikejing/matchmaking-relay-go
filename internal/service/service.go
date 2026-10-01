// Package service is the facade wiring matching, relay, rules, audit, and
// loopback pipes into one coherent API.
package service

import (
	"math/rand"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/match"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
	"github.com/shikejing/matchmaking-relay-go/internal/transport"
)

// Service is the entry point for all matchmaking/relay operations.
type Service struct {
	cfg    config.Config
	rules  *rule.Registry
	audit  *audit.Log
	engine *match.Engine
	relay  *relay.Manager
	rng    *rand.Rand
}

// New creates a service from configuration.
func New(cfg config.Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	rules, err := rule.NewRegistry(rule.DefaultVersion)
	if err != nil {
		return nil, err
	}
	auditLog := audit.NewLog(cfg.AuditCapacity)
	relayMgr := relay.NewManager(cfg, rules, auditLog)
	engine := match.NewEngine(cfg, rules, auditLog, relayMgr)
	return &Service{
		cfg:    cfg,
		rules:  rules,
		audit:  auditLog,
		engine: engine,
		relay:  relayMgr,
		rng:    rand.New(rand.NewSource(cfg.Seed)),
	}, nil
}

// EnqueueTeam submits a team as an indivisible matching unit. The team is
// stamped with the rule version in effect at enqueue time.
func (s *Service) EnqueueTeam(partition model.PartitionID, teamID model.TeamID, players []model.Player, ruleVersion int) (*model.Team, error) {
	return s.engine.Enqueue(match.EnqueueRequest{
		Partition:   partition,
		TeamID:      teamID,
		Players:     players,
		RuleVersion: ruleVersion,
	})
}

// CancelTeam cancels a waiting team before it is matched.
func (s *Service) CancelTeam(teamID model.TeamID) error {
	return s.engine.Cancel(teamID)
}

// Tick advances the whole system by one logical step (deterministic).
func (s *Service) Tick() {
	s.engine.Tick()
}

// Start launches background partition loops.
func (s *Service) Start() {
	s.engine.Start()
}

// Stop terminates background loops.
func (s *Service) Stop() {
	s.engine.Stop()
}

// AttachPipe attaches a client loopback pipe to a session member.
func (s *Service) AttachPipe(sessionID model.SessionID, playerID model.PlayerID, pipe *transport.Pipe) {
	s.relay.RegisterPipe(sessionID, playerID, pipe.Inbox)
}

// SendMessage relays a message from a player to all joined session members.
func (s *Service) SendMessage(sessionID model.SessionID, from model.PlayerID, payload []byte) error {
	return s.relay.Broadcast(sessionID, from, payload, s.currentTick())
}

// Disconnect simulates a member going offline.
func (s *Service) Disconnect(sessionID model.SessionID, playerID model.PlayerID) error {
	return s.relay.Disconnect(sessionID, playerID, s.currentTick())
}

// Reconnect simulates a member coming back online within the retention window.
func (s *Service) Reconnect(sessionID model.SessionID, playerID model.PlayerID) error {
	return s.relay.Reconnect(sessionID, playerID, s.currentTick())
}

// Leave removes a member from a session; vacated slots under backfill-enabled
// rules are queued for replacement.
func (s *Service) Leave(sessionID model.SessionID, playerID model.PlayerID) error {
	reqs, err := s.relay.Leave(sessionID, playerID, s.currentTick())
	if err != nil {
		return err
	}
	for _, req := range reqs {
		s.engine.OpenBackfill(req.Partition, req.SessionID, req.Slots, s.currentTick())
	}
	return nil
}

// EndSession terminates a session.
func (s *Service) EndSession(sessionID model.SessionID, reason string) error {
	return s.relay.EndSession(sessionID, s.currentTick(), reason)
}

// QuerySession returns a session snapshot.
func (s *Service) QuerySession(sessionID model.SessionID) (relay.SessionView, bool) {
	return s.relay.GetSession(sessionID)
}

// QueryQueue returns a queue snapshot.
func (s *Service) QueryQueue(partition model.PartitionID) (match.QueueView, error) {
	return s.engine.GetQueue(partition)
}

// QueryTeam returns a team by id.
func (s *Service) QueryTeam(teamID model.TeamID) (*model.Team, bool) {
	return s.engine.GetTeam(teamID)
}

// ListSessions returns session snapshots for a partition (or all).
func (s *Service) ListSessions(partition model.PartitionID) []relay.SessionView {
	return s.relay.ListSessions(partition)
}

// ListPartitions returns partition IDs.
func (s *Service) ListPartitions() []model.PartitionID {
	return s.engine.ListPartitions()
}

// Rules returns the rule registry.
func (s *Service) Rules() *rule.Registry {
	return s.rules
}

// Audit returns the audit log.
func (s *Service) Audit() *audit.Log {
	return s.audit
}

// ActiveSessions returns the count of active sessions.
func (s *Service) ActiveSessions() int {
	return s.relay.ActiveSessionCount()
}

// RNG returns the seeded random source. It is never used by match decisions;
// tests may use it to generate reproducible input permutations.
func (s *Service) RNG() *rand.Rand {
	return s.rng
}

func (s *Service) currentTick() int64 {
	// The logical clock is owned by the engine's partitions; session-side
	// operations use the max partition tick as a stable reference.
	var maxTick int64
	for _, pid := range s.engine.ListPartitions() {
		if q, err := s.engine.GetQueue(pid); err == nil && q.Tick > maxTick {
			maxTick = q.Tick
		}
	}
	return maxTick
}

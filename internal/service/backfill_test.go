package service

import (
	"errors"
	"testing"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
)

// openV2Backfill builds a v2 session of two 1-player teams, disconnects one
// member, and advances the logical clock past the v2 retention window so a
// backfill wait opens. Returns the session id and the tick count already run.
func openV2Backfill(t *testing.T, svc *Service, partition string) model.SessionID {
	t.Helper()
	enqueueOrFail(t, svc, partition, "bf-t1", 1, 100, 2)
	enqueueOrFail(t, svc, partition, "bf-t2", 1, 100, 2)
	svc.Tick()
	sessions := svc.ListSessions(model.PartitionID(partition))
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	sid := sessions[0].ID
	if err := svc.Disconnect(sid, sessions[0].Members[0].PlayerID); err != nil {
		t.Fatal(err)
	}
	// v2 retention = 200 ticks; 201 ticks open the backfill wait.
	for i := 0; i < 201; i++ {
		svc.Tick()
	}
	got, ok := svc.QuerySession(sid)
	if !ok {
		t.Fatal("session vanished")
	}
	if got.State != model.SessionActive {
		t.Fatalf("session should be active while waiting for backfill, state=%d", got.State)
	}
	if got.BackfillTicketID == "" {
		t.Fatal("backfill wait should be pending after retention expiry")
	}
	if got.ActiveMembers != 1 {
		t.Fatalf("active members = %d, want 1", got.ActiveMembers)
	}
	return sid
}

// TestBackfillTimeoutViaTicks: with no suitable replacement in the queue, the
// backfill wait must end the session after the rule's BackfillTimeoutTicks —
// triggered purely by advancing the logical clock. Query and audit must both
// show the BACKFILL_TIMEOUT cause, and the session must not hang active.
func TestBackfillTimeoutViaTicks(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	sid := openV2Backfill(t, svc, "default")

	// v2 backfill timeout = 50 ticks; 51 more ticks must end the session.
	for i := 0; i < 51; i++ {
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.State != model.SessionEnded {
		t.Fatalf("session must end after backfill timeout, state=%d", got.State)
	}
	if got.EndReason != relay.EndReasonBackfillTimeout {
		t.Fatalf("end reason = %q, want %q", got.EndReason, relay.EndReasonBackfillTimeout)
	}
	if got.BackfillTicketID != "" {
		t.Fatalf("ended session must not show a pending backfill, got %q", got.BackfillTicketID)
	}

	// Audit must show the timeout cause distinguishably.
	recs := svc.Audit().Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventBackfillTimeout}})
	if len(recs) != 1 {
		t.Fatalf("BACKFILL_TIMEOUT audit records = %d, want 1", len(recs))
	}
	ended := svc.Audit().Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventSessionEnded}})
	if len(ended) != 1 || ended[0].Reason != relay.EndReasonBackfillTimeout {
		t.Fatalf("SESSION_ENDED records = %+v, want one with reason BACKFILL_TIMEOUT", ended)
	}

	// Repeated clock advances must not resurrect or re-end the session, and
	// must not open further backfill waits.
	for i := 0; i < 200; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionEnded || got.EndReason != relay.EndReasonBackfillTimeout {
		t.Fatalf("session mutated after termination: state=%d reason=%q", got.State, got.EndReason)
	}
	starts := svc.Audit().Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventBackfillStarted}})
	if len(starts) == 0 {
		t.Fatal("expected at least the initial BACKFILL_STARTED record")
	}
	timeouts := svc.Audit().Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventBackfillTimeout}})
	if len(timeouts) != 1 {
		t.Fatalf("duplicate timeout records = %d, want exactly 1", len(timeouts))
	}
}

// TestBackfillSuccessThenReBackfill: after a backfill succeeds, all wait
// state must be cleaned up so a later vacancy enters the backfill flow again
// (at most one pending wait at any moment).
func TestBackfillSuccessThenReBackfill(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	sid := openV2Backfill(t, svc, "default")

	// Satisfy the first wait.
	enqueueOrFail(t, svc, "default", "bf-t3", 1, 100, 2)
	svc.Tick()
	got, _ := svc.QuerySession(sid)
	if got.State != model.SessionActive || got.ActiveMembers != 2 {
		t.Fatalf("after first backfill: state=%d active=%d, want active/2", got.State, got.ActiveMembers)
	}
	if got.BackfillTicketID != "" {
		t.Fatalf("backfill wait not cleaned up after success: %q", got.BackfillTicketID)
	}

	// A new vacancy must be able to enter the backfill flow again.
	var victim model.PlayerID
	for _, m := range got.Members {
		if m.State == model.MemberJoined {
			victim = m.PlayerID
			break
		}
	}
	if err := svc.Disconnect(sid, victim); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 201; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionActive || got.BackfillTicketID == "" {
		t.Fatalf("second vacancy should reopen backfill wait: state=%d ticket=%q", got.State, got.BackfillTicketID)
	}

	// Satisfy the second wait as well; the session continues.
	enqueueOrFail(t, svc, "default", "bf-t4", 1, 100, 2)
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionActive || got.ActiveMembers != 2 {
		t.Fatalf("after second backfill: state=%d active=%d, want active/2", got.State, got.ActiveMembers)
	}

	// Exactly two waits were opened over the whole run — no duplicates.
	starts := svc.Audit().Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventBackfillStarted}})
	if len(starts) != 2 {
		t.Fatalf("BACKFILL_STARTED records = %d, want 2 (one per vacancy)", len(starts))
	}
	succ := svc.Audit().Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventBackfillSucceeded}})
	if len(succ) != 2 {
		t.Fatalf("BACKFILL_SUCCEEDED records = %d, want 2", len(succ))
	}
}

// TestEndReasonsDistinguishable: the three termination paths (backfill
// timeout, retention timeout under a no-backfill rule, explicit end) must be
// distinguishable in both the query snapshot and the audit trail.
func TestEndReasonsDistinguishable(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	// Path 1: v1 session, disconnect past retention -> RETENTION_TIMEOUT.
	enqueueOrFail(t, svc, "default", "v1-a", 1, 100, 1)
	enqueueOrFail(t, svc, "default", "v1-b", 1, 100, 1)
	svc.Tick()
	sid1 := svc.ListSessions("")[0].ID
	p1 := svc.ListSessions("")[0].Members[0].PlayerID
	if err := svc.Disconnect(sid1, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 101; i++ {
		svc.Tick()
	}
	got1, _ := svc.QuerySession(sid1)
	if got1.State != model.SessionEnded || got1.EndReason != relay.EndReasonRetentionTimeout {
		t.Fatalf("v1 session: state=%d reason=%q, want ended/%s", got1.State, got1.EndReason, relay.EndReasonRetentionTimeout)
	}

	// Path 2: v2 session, backfill wait with no replacement -> BACKFILL_TIMEOUT.
	enqueueOrFail(t, svc, "default", "v2-a", 1, 100, 2)
	enqueueOrFail(t, svc, "default", "v2-b", 1, 100, 2)
	svc.Tick()
	var sid2 model.SessionID
	for _, s := range svc.ListSessions("") {
		if s.State == model.SessionActive {
			sid2 = s.ID
		}
	}
	p2 := svc.ListSessions("")[1].Members[0].PlayerID
	// Find the active session's member (sid2 may not be index 1 after end).
	for _, s := range svc.ListSessions("") {
		if s.ID == sid2 {
			p2 = s.Members[0].PlayerID
		}
	}
	if err := svc.Disconnect(sid2, p2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 201+51; i++ {
		svc.Tick()
	}
	got2, _ := svc.QuerySession(sid2)
	if got2.State != model.SessionEnded || got2.EndReason != relay.EndReasonBackfillTimeout {
		t.Fatalf("v2 session: state=%d reason=%q, want ended/%s", got2.State, got2.EndReason, relay.EndReasonBackfillTimeout)
	}

	// Path 3: explicit end keeps its caller-supplied reason.
	enqueueOrFail(t, svc, "default", "v2-c", 1, 100, 2)
	enqueueOrFail(t, svc, "default", "v2-d", 1, 100, 2)
	svc.Tick()
	var sid3 model.SessionID
	for _, s := range svc.ListSessions("") {
		if s.State == model.SessionActive {
			sid3 = s.ID
		}
	}
	if err := svc.EndSession(sid3, "match-complete"); err != nil {
		t.Fatal(err)
	}
	got3, _ := svc.QuerySession(sid3)
	if got3.State != model.SessionEnded || got3.EndReason != "match-complete" {
		t.Fatalf("explicit end: state=%d reason=%q, want ended/match-complete", got3.State, got3.EndReason)
	}

	// All three reasons are distinct in the audit trail too.
	ended := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventSessionEnded}})
	reasons := map[string]bool{}
	for _, r := range ended {
		reasons[r.Reason] = true
	}
	for _, want := range []string{relay.EndReasonRetentionTimeout, relay.EndReasonBackfillTimeout, "match-complete"} {
		if !reasons[want] {
			t.Fatalf("audit missing SESSION_ENDED reason %q (have %v)", want, reasons)
		}
	}
}

// testRuleV3 is a backfill-enabled rule with a much shorter timeout than v2,
// used to prove that in-flight backfill waits follow the rule version stamped
// at session creation, not the current default.
type testRuleV3 struct{}

func (testRuleV3) Version() int { return 3 }
func (testRuleV3) Name() string { return "v3-test-short-backfill" }
func (testRuleV3) MergeScore(ms []model.Player) int {
	total := 0
	for _, m := range ms {
		total += m.Score
	}
	if len(ms) == 0 {
		return 0
	}
	return total / len(ms)
}
func (testRuleV3) EffectiveScore(tm *model.Team, _ int64) int { return tm.Score }
func (testRuleV3) RetentionTicks() int                        { return 10 }
func (testRuleV3) BackfillEnabled() bool                      { return true }
func (testRuleV3) BackfillTimeoutTicks() int                  { return 5 }
func (testRuleV3) Describe() string                           { return "rule v3 (test): retention=10t, backfill=on(timeout=5t)" }

// TestRuleSwitchKeepsInFlightBackfill: a session created under v3 (backfill
// timeout 5 ticks) must still time out per v3 after the default rule switches
// to v2 (timeout 50 ticks) — not earlier, not later, never hanging.
func TestRuleSwitchKeepsInFlightBackfill(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	if err := svc.Rules().Register(testRuleV3{}); err != nil {
		t.Fatal(err)
	}

	enqueueOrFail(t, svc, "default", "v3-a", 1, 100, 3)
	enqueueOrFail(t, svc, "default", "v3-b", 1, 100, 3)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	if got, _ := svc.QuerySession(sid); got.RuleVersion != 3 {
		t.Fatalf("session rule = %d, want 3", got.RuleVersion)
	}
	p1 := svc.ListSessions("")[0].Members[0].PlayerID
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	// v3 retention = 10 ticks: 11 ticks open the backfill wait.
	for i := 0; i < 11; i++ {
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.BackfillTicketID == "" {
		t.Fatal("backfill wait should be open under v3")
	}

	// Switch the default rule to v2 (timeout 50) mid-wait.
	if err := svc.Rules().SetDefault(2); err != nil {
		t.Fatal(err)
	}

	// v3 timeout is 5 ticks: after 5 ticks the session must still be active
	// (expiry fires when wait > timeout), after 6 it must have ended per v3.
	for i := 0; i < 5; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionActive {
		t.Fatalf("session ended before v3 timeout: state=%d reason=%q", got.State, got.EndReason)
	}
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionEnded || got.EndReason != relay.EndReasonBackfillTimeout {
		t.Fatalf("session must end per v3 timeout: state=%d reason=%q", got.State, got.EndReason)
	}

	// Already-formed match results are unaffected by the switch.
	recs := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventMatchFormed}})
	if len(recs) != 1 || recs[0].RuleVersion != 3 {
		t.Fatalf("match records = %+v, want one v3 match", recs)
	}
}

// TestConfigUpperBounds: capacity configuration beyond the hard upper bounds
// must be rejected at startup with a distinguishable, per-field reason.
func TestConfigUpperBounds(t *testing.T) {
	cases := []struct {
		field  string
		mutate func(*config.Config)
	}{
		{"QueueCapacity", func(c *config.Config) { c.QueueCapacity = config.MaxQueueCapacity + 1 }},
		{"MatchBatchSize", func(c *config.Config) { c.MatchBatchSize = config.MaxMatchBatchSize + 1 }},
		{"MaxConcurrentSessions", func(c *config.Config) { c.MaxConcurrentSessions = config.MaxConcurrentSessions + 1 }},
		{"AuditCapacity", func(c *config.Config) { c.AuditCapacity = config.MaxAuditCapacity + 1 }},
		{"MessageBufferSize", func(c *config.Config) { c.MessageBufferSize = config.MaxMessageBufferSize + 1 }},
	}
	for _, tc := range cases {
		cfg := config.Default()
		tc.mutate(&cfg)
		_, err := New(cfg)
		var le *config.LimitError
		if !errors.As(err, &le) {
			t.Fatalf("%s over bound: expected *config.LimitError, got %v", tc.field, err)
		}
		if le.Field != tc.field {
			t.Fatalf("LimitError.Field = %q, want %q", le.Field, tc.field)
		}
	}

	// Exactly at the bound must be accepted.
	cfg := config.Default()
	cfg.QueueCapacity = config.MaxQueueCapacity
	cfg.MatchBatchSize = config.MaxMatchBatchSize
	cfg.MaxConcurrentSessions = config.MaxConcurrentSessions
	if _, err := New(cfg); err != nil {
		t.Fatalf("config at upper bounds should be accepted: %v", err)
	}

	// Below the lower bound is still rejected, with the same typed error.
	cfg = config.Default()
	cfg.QueueCapacity = 0
	var le *config.LimitError
	if _, err := New(cfg); !errors.As(err, &le) || le.Field != "QueueCapacity" {
		t.Fatalf("zero QueueCapacity: expected LimitError for QueueCapacity, got %v", err)
	}
}

package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/match"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
)

// fastRule is a test-only rule version with tiny windows so backfill timeout
// tests run in a handful of real ticks. It is registered as v3.
type fastRule struct{}

func (fastRule) Version() int                              { return 3 }
func (fastRule) Name() string                              { return "v3-test-fast-backfill" }
func (fastRule) MergeScore(ms []model.Player) int          { return ms[0].Score }
func (fastRule) EffectiveScore(t *model.Team, _ int64) int { return t.Score }
func (fastRule) RetentionTicks() int                       { return 2 }
func (fastRule) BackfillEnabled() bool                     { return true }
func (fastRule) BackfillTimeoutTicks() int                 { return 3 }
func (fastRule) Describe() string                          { return "test rule: retention=2, backfill timeout=3" }

func registerFastRule(t *testing.T, svc *Service) {
	t.Helper()
	if err := svc.Rules().Register(fastRule{}); err != nil {
		t.Fatalf("register fast rule: %v", err)
	}
}

// backfillStartedCount counts audited backfill-ticket creations (relay side +
// engine side each append one record per opened ticket).
func backfillStartedCount(svc *Service) int {
	return len(svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventBackfillStarted}}))
}

// assertConsistentView checks the snapshot invariants that must hold at every
// observation point during a backfill wait.
func assertConsistentView(t *testing.T, svc *Service, sid model.SessionID) {
	t.Helper()
	v, ok := svc.QuerySession(sid)
	if !ok {
		t.Fatal("session not found")
	}
	switch v.State {
	case model.SessionActive:
		// Active session: a pending ticket implies an unhandled vacancy, and
		// an unhandled vacancy (under a backfill rule) implies a pending
		// ticket — never one without the other.
		vacancies := 0
		for _, m := range v.Members {
			if m.State == model.MemberLeft {
				vacancies++
			}
		}
		if (v.BackfillTicketID != "") != (vacancies > 0) {
			t.Fatalf("inconsistent view: ticket=%q vacancies=%d", v.BackfillTicketID, vacancies)
		}
		if v.EndReason != "" {
			t.Fatalf("active session must not have an end reason, got %q", v.EndReason)
		}
	case model.SessionEnded:
		if v.BackfillTicketID != "" {
			t.Fatalf("ended session must not show a pending ticket, got %q", v.BackfillTicketID)
		}
		if v.EndReason == "" {
			t.Fatal("ended session must record an end reason")
		}
	default:
		t.Fatalf("unexpected session state %d", v.State)
	}
}

// TestBackfillTimeoutRealTicks: with no suitable replacement in the queue,
// advancing the logical clock must end the session with a distinguishable
// BACKFILL_TIMEOUT reason — never hang active forever. Uses the real v2 rule
// (retention 200, backfill timeout 50) and real tick advancement only.
func TestBackfillTimeoutRealTicks(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 2)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 2)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	p1 := svc.ListSessions("")[0].Members[0].PlayerID

	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	// 201 ticks: v2 retention (200) expires, backfill wait opens.
	for i := 0; i < 201; i++ {
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.State != model.SessionActive || got.BackfillTicketID == "" {
		t.Fatalf("expected active session with pending backfill, state=%d ticket=%q", got.State, got.BackfillTicketID)
	}
	firstTicket := got.BackfillTicketID

	// Repeated ticks while waiting must not open duplicate tickets.
	for i := 0; i < 10; i++ {
		svc.Tick()
		assertConsistentView(t, svc, sid)
	}
	if n := backfillStartedCount(svc); n != 2 {
		t.Fatalf("backfill-started records = %d, want exactly 2 (one ticket, relay+engine)", n)
	}
	got, _ = svc.QuerySession(sid)
	if got.BackfillTicketID != firstTicket {
		t.Fatalf("ticket changed while waiting: %q -> %q", firstTicket, got.BackfillTicketID)
	}

	// 40 more ticks (51 total since the wait opened): v2 allows 50, so the
	// session must now be ended by backfill timeout.
	for i := 0; i < 41; i++ {
		svc.Tick()
	}
	got, ok := svc.QuerySession(sid)
	if !ok {
		t.Fatal("session disappeared from query")
	}
	if got.State != model.SessionEnded {
		t.Fatalf("session must end after backfill timeout, state=%d", got.State)
	}
	if !strings.HasPrefix(got.EndReason, relay.EndCodeBackfillTimeout) {
		t.Fatalf("end reason = %q, want prefix %q", got.EndReason, relay.EndCodeBackfillTimeout)
	}
	if got.BackfillTicketID != "" {
		t.Fatalf("pending ticket must be cleared on timeout, got %q", got.BackfillTicketID)
	}

	// Audit must show the timeout distinctly from other end causes.
	timeouts := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventBackfillTimeout}, SessionID: sid})
	if len(timeouts) != 1 {
		t.Fatalf("BACKFILL_TIMEOUT audit records = %d, want 1", len(timeouts))
	}
	ends := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventSessionEnded}, SessionID: sid})
	if len(ends) != 1 || !strings.HasPrefix(ends[0].Reason, relay.EndCodeBackfillTimeout) {
		t.Fatalf("SESSION_ENDED reason = %+v, want BACKFILL_TIMEOUT", ends)
	}

	// Further ticks must not resurrect or re-end the session.
	for i := 0; i < 5; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionEnded {
		t.Fatalf("ended session changed state: %d", got.State)
	}
	if n := len(svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventSessionEnded}, SessionID: sid})); n != 1 {
		t.Fatalf("session ended %d times, want exactly 1", n)
	}
}

// TestBackfillSuccessThenNewVacancy: after a backfill is served, all wait
// state is cleaned up; a later disconnect starts a fresh backfill cycle.
func TestBackfillSuccessThenNewVacancy(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	registerFastRule(t, svc)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 3)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 3)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID

	// First vacancy -> backfill served by t3.
	p1 := svc.ListSessions("")[0].Members[0].PlayerID
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ { // fast rule retention = 2
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.BackfillTicketID == "" {
		t.Fatal("backfill wait should be pending")
	}
	enqueueOrFail(t, svc, "default", "t3", 1, 100, 3)
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.ActiveMembers != 2 || got.BackfillTicketID != "" {
		t.Fatalf("after backfill: active=%d ticket=%q, want 2 and empty", got.ActiveMembers, got.BackfillTicketID)
	}

	// Ticks after a served wait must not open spurious tickets.
	before := backfillStartedCount(svc)
	for i := 0; i < 5; i++ {
		svc.Tick()
		assertConsistentView(t, svc, sid)
	}
	if n := backfillStartedCount(svc); n != before {
		t.Fatalf("spurious backfill tickets after success: %d -> %d", before, n)
	}

	// A new disconnect starts a fresh backfill cycle with a new ticket.
	var p2 model.PlayerID
	for _, m := range svc.ListSessions("")[0].Members {
		if m.State == model.MemberJoined {
			p2 = m.PlayerID
		}
	}
	if err := svc.Disconnect(sid, p2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionActive || got.BackfillTicketID == "" {
		t.Fatalf("second vacancy should open a new wait, state=%d ticket=%q", got.State, got.BackfillTicketID)
	}
	enqueueOrFail(t, svc, "default", "t4", 1, 100, 3)
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.ActiveMembers != 2 || got.BackfillTicketID != "" {
		t.Fatalf("second backfill failed: active=%d ticket=%q", got.ActiveMembers, got.BackfillTicketID)
	}
	succ := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventBackfillSucceeded}, SessionID: sid})
	if len(succ) != 2 {
		t.Fatalf("BACKFILL_SUCCEEDED records = %d, want 2", len(succ))
	}
}

// TestBackfillTimeoutAfterRuleSwitch: a session created under v2 keeps v2's
// backfill timeout even after the default rule switches to v1 (which has no
// backfill at all). It must neither end early nor hang forever.
func TestBackfillTimeoutAfterRuleSwitch(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 2)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 2)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	p1 := svc.ListSessions("")[0].Members[0].PlayerID
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 201; i++ { // v2 retention expires, wait opens
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.BackfillTicketID == "" {
		t.Fatal("backfill wait should be pending before rule switch")
	}

	// Switch the default rule mid-wait. v1 would end immediately (timeout 0)
	// if the session wrongly followed the current default.
	if err := svc.Rules().SetDefault(1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		svc.Tick()
		assertConsistentView(t, svc, sid)
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionActive {
		t.Fatalf("session ended early after rule switch (must follow v2 timeout), reason=%q", got.EndReason)
	}
	if got.RuleVersion != 2 {
		t.Fatalf("session rule version = %d, want 2", got.RuleVersion)
	}

	// v2 allows 50 ticks from the wait opening; 201+10 done, 41 more end it.
	for i := 0; i < 41; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionEnded {
		t.Fatalf("session must end by v2 backfill timeout, state=%d", got.State)
	}
	if !strings.HasPrefix(got.EndReason, relay.EndCodeBackfillTimeout) {
		t.Fatalf("end reason = %q, want BACKFILL_TIMEOUT", got.EndReason)
	}
}

// TestBackfillPartitionsConcurrent: one partition waiting for / timing out a
// backfill must not block or steal from another partition advancing in
// parallel. Runs the real background loops (one goroutine per partition).
func TestBackfillPartitionsConcurrent(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 32
	cfg.TickInterval = 5 * time.Millisecond
	cfg.Partitions = []string{"pa", "pb"}
	svc := newTestService(t, cfg)
	registerFastRule(t, svc)
	svc.Start()
	defer svc.Stop()

	// pa: a v3 session whose member disconnects and never gets a replacement.
	enqueueOrFail(t, svc, "pa", "pa-1", 1, 100, 3)
	enqueueOrFail(t, svc, "pa", "pa-2", 1, 100, 3)
	// pb: pairs of teams that keep matching while pa waits.
	enqueueOrFail(t, svc, "pb", "pb-1", 1, 100, 3)
	enqueueOrFail(t, svc, "pb", "pb-2", 1, 100, 3)

	deadline := time.Now().Add(3 * time.Second)
	var paSID model.SessionID
	for time.Now().Before(deadline) {
		if ss := svc.ListSessions("pa"); len(ss) == 1 {
			paSID = ss[0].ID
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if paSID == "" {
		t.Fatal("pa session not formed")
	}
	p1 := svc.ListSessions("pa")[0].Members[0].PlayerID
	if err := svc.Disconnect(paSID, p1); err != nil {
		t.Fatal(err)
	}

	// pa's session must end by backfill timeout (fast rule: 2+3 ticks);
	// pb must have matched on its own in the meantime.
	endBy := time.Now().Add(3 * time.Second)
	for time.Now().Before(endBy) {
		got, _ := svc.QuerySession(paSID)
		pbDone := len(svc.ListSessions("pb")) == 1
		if got.State == model.SessionEnded && pbDone {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := svc.QuerySession(paSID)
	if got.State != model.SessionEnded {
		t.Fatalf("pa session did not end by backfill timeout, state=%d", got.State)
	}
	if !strings.HasPrefix(got.EndReason, relay.EndCodeBackfillTimeout) {
		t.Fatalf("pa end reason = %q, want BACKFILL_TIMEOUT", got.EndReason)
	}
	pbSessions := svc.ListSessions("pb")
	if len(pbSessions) != 1 {
		t.Fatalf("pb sessions = %d, want 1 (pb must keep matching while pa waits)", len(pbSessions))
	}
	for _, m := range pbSessions[0].Members {
		if !strings.HasPrefix(string(m.TeamID), "pb-") {
			t.Fatalf("pb session consumed foreign team %s", m.TeamID)
		}
	}
	// pa's wait never consumed pb's queued teams: pa's session members are
	// all pa teams.
	for _, m := range got.Members {
		if !strings.HasPrefix(string(m.TeamID), "pa-") {
			t.Fatalf("pa session contains foreign team %s", m.TeamID)
		}
	}
}

// TestConfigUpperBounds: capacity configs above their upper bounds are
// rejected at startup with a distinguishable *config.LimitError.
func TestConfigUpperBounds(t *testing.T) {
	cases := []struct {
		field  string
		mutate func(*config.Config)
	}{
		{"QueueCapacity", func(c *config.Config) { c.QueueCapacity = config.QueueCapacityMax + 1 }},
		{"MatchBatchSize", func(c *config.Config) { c.MatchBatchSize = config.MatchBatchSizeMax + 1 }},
		{"MaxConcurrentSessions", func(c *config.Config) { c.MaxConcurrentSessions = config.ConcurrentSessionsMax + 1 }},
		{"AuditCapacity", func(c *config.Config) { c.AuditCapacity = config.AuditCapacityMax + 1 }},
		{"MessageBufferSize", func(c *config.Config) { c.MessageBufferSize = config.MessageBufferSizeMax + 1 }},
	}
	for _, tc := range cases {
		cfg := config.Default()
		tc.mutate(&cfg)
		_, err := New(cfg)
		var le *config.LimitError
		if !errors.As(err, &le) {
			t.Fatalf("%s over max: expected *config.LimitError, got %v", tc.field, err)
		}
		if le.Field != tc.field {
			t.Fatalf("LimitError.Field = %q, want %q", le.Field, tc.field)
		}
	}
	// Boundary values exactly at the max are accepted.
	cfg := config.Default()
	cfg.QueueCapacity = config.QueueCapacityMax
	cfg.MatchBatchSize = config.MatchBatchSizeMax
	cfg.MaxConcurrentSessions = config.ConcurrentSessionsMax
	if _, err := New(cfg); err != nil {
		t.Fatalf("boundary values should be accepted: %v", err)
	}
}

// TestRuntimeLimitRejectionsIsolated: runtime rejections at configured limits
// stay distinguishable and do not affect subsequent normal requests.
func TestRuntimeLimitRejectionsIsolated(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 1
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 0)
	_, err := svc.EnqueueTeam("default", "t2", players("t2", 1, 100), 0)
	var me *match.Error
	if !errors.As(err, &me) || me.Code != match.RejectQueueFull {
		t.Fatalf("expected QUEUE_FULL, got %v", err)
	}
	// A normal request after the rejection still works.
	if err := svc.CancelTeam("t1"); err != nil {
		t.Fatalf("cancel after rejection: %v", err)
	}
	if _, err := svc.EnqueueTeam("default", "t3", players("t3", 1, 100), 0); err != nil {
		t.Fatalf("enqueue after rejection: %v", err)
	}
}

// fastRule2 is a test-only rule (v4) with retention 1 and a generous backfill
// timeout, used to script multi-vacancy scenarios without racing the timeout.
type fastRule2 struct{}

func (fastRule2) Version() int                              { return 4 }
func (fastRule2) Name() string                              { return "v4-test-long-wait" }
func (fastRule2) MergeScore(ms []model.Player) int          { return ms[0].Score }
func (fastRule2) EffectiveScore(t *model.Team, _ int64) int { return t.Score }
func (fastRule2) RetentionTicks() int                       { return 1 }
func (fastRule2) BackfillEnabled() bool                     { return true }
func (fastRule2) BackfillTimeoutTicks() int                 { return 20 }
func (fastRule2) Describe() string                          { return "test rule: retention=1, backfill timeout=20" }

// TestBackfillFollowUpForConcurrentVacancies: a second slot vacating while a
// backfill wait is pending does not open a parallel ticket (one pending wait
// per session); once the pending ticket is served, a follow-up ticket covers
// the remaining vacancy automatically.
func TestBackfillFollowUpForConcurrentVacancies(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	if err := svc.Rules().Register(fastRule2{}); err != nil {
		t.Fatal(err)
	}

	enqueueOrFail(t, svc, "default", "t1", 2, 100, 4)
	enqueueOrFail(t, svc, "default", "t2", 2, 100, 4)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	members := svc.ListSessions("")[0].Members

	// p1a disconnects and times out -> ticket A (1 slot).
	if err := svc.Disconnect(sid, members[0].PlayerID); err != nil {
		t.Fatal(err)
	}
	svc.Tick()
	svc.Tick()
	got, _ := svc.QuerySession(sid)
	if got.BackfillTicketID == "" {
		t.Fatal("ticket A should be pending")
	}
	ticketA := got.BackfillTicketID

	// p1b disconnects and times out while ticket A is still pending: no
	// second ticket may be opened.
	if err := svc.Disconnect(sid, members[1].PlayerID); err != nil {
		t.Fatal(err)
	}
	svc.Tick()
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.BackfillTicketID != ticketA {
		t.Fatalf("ticket changed while pending: %q -> %q (parallel wait opened?)", ticketA, got.BackfillTicketID)
	}
	vacancies := 0
	for _, m := range got.Members {
		if m.State == model.MemberLeft {
			vacancies++
		}
	}
	if vacancies != 2 {
		t.Fatalf("vacancies = %d, want 2", vacancies)
	}

	// Serve ticket A with a 1-player team: a follow-up ticket for the
	// remaining vacancy opens automatically.
	enqueueOrFail(t, svc, "default", "t3", 1, 100, 4)
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.BackfillTicketID == "" || got.BackfillTicketID == ticketA {
		t.Fatalf("follow-up ticket should be pending and differ from ticket A, got %q", got.BackfillTicketID)
	}
	if got.ActiveMembers != 3 {
		t.Fatalf("active after first backfill = %d, want 3", got.ActiveMembers)
	}
	assertConsistentView(t, svc, sid)

	// Serve the follow-up ticket: session is full again, no pending wait.
	enqueueOrFail(t, svc, "default", "t4", 1, 100, 4)
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.ActiveMembers != 4 || got.BackfillTicketID != "" {
		t.Fatalf("after follow-up: active=%d ticket=%q, want 4 and empty", got.ActiveMembers, got.BackfillTicketID)
	}
	succ := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventBackfillSucceeded}, SessionID: sid})
	if len(succ) != 2 {
		t.Fatalf("BACKFILL_SUCCEEDED records = %d, want 2", len(succ))
	}
}

// TestBackfillTicketDroppedWhenSessionEnds: a ticket whose session ends by
// other means is cleaned up; later teams are never offered to the dead
// session and stay queued.
func TestBackfillTicketDroppedWhenSessionEnds(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	registerFastRule(t, svc)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 3)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 3)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	p1 := svc.ListSessions("")[0].Members[0].PlayerID
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.BackfillTicketID == "" {
		t.Fatal("backfill wait should be pending")
	}

	// Session ends explicitly while the wait is pending.
	if err := svc.EndSession(sid, "operator-shutdown"); err != nil {
		t.Fatal(err)
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionEnded || got.BackfillTicketID != "" {
		t.Fatalf("ended session must not show a pending wait: state=%d ticket=%q", got.State, got.BackfillTicketID)
	}

	// A later 1-player team must not be consumed by the dead session's
	// stale ticket; it stays queued (no partner for a regular match).
	enqueueOrFail(t, svc, "default", "t3", 1, 100, 3)
	for i := 0; i < 10; i++ {
		svc.Tick()
	}
	q, _ := svc.QueryQueue("default")
	if q.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1 (t3 must not fill a dead session)", q.Waiting)
	}
	if n := len(svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventBackfillTimeout}, SessionID: sid})); n != 0 {
		t.Fatalf("stale ticket produced %d timeout records, want 0", n)
	}
}

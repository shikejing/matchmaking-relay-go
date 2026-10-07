package relay

import (
	"fmt"
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
)

func newTestRelay(t *testing.T, cfg config.Config) *Manager {
	t.Helper()
	regs, err := rule.NewRegistry(rule.DefaultVersion)
	if err != nil {
		t.Fatal(err)
	}
	return NewManager(cfg, regs, audit.NewLog(cfg.AuditCapacity))
}

func mkTeam(id string, ruleVersion int, playerIDs ...string) *model.Team {
	players := make([]model.Player, len(playerIDs))
	for i, p := range playerIDs {
		players[i] = model.Player{ID: model.PlayerID(p), Score: 100}
	}
	return &model.Team{
		ID:          model.TeamID(id),
		Partition:   "default",
		Members:     players,
		Score:       100,
		RuleVersion: ruleVersion,
		Status:      model.TeamMatched,
	}
}

func createSession(t *testing.T, m *Manager, ruleVersion int, tick int64, teams ...*model.Team) SessionView {
	t.Helper()
	view, err := m.CreateSession("default", "match-1", ruleVersion, teams, tick)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return *view
}

func TestSessionLifecycle(t *testing.T) {
	cfg := config.Default()
	m := newTestRelay(t, cfg)
	// Use v2 so a member leave does not end the session (v1 has no backfill
	// and ends the session when a member leaves).
	t1 := mkTeam("t1", 2, "p1", "p2")
	t2 := mkTeam("t2", 2, "p3", "p4")
	view := createSession(t, m, 2, 1, t1, t2)

	if view.State != model.SessionActive {
		t.Fatalf("state = %d, want active", view.State)
	}
	if len(view.Members) != 4 || view.ActiveMembers != 4 {
		t.Fatalf("members = %d active = %d, want 4/4", len(view.Members), view.ActiveMembers)
	}

	// Disconnect p1.
	if err := m.Disconnect(view.ID, "p1", 2); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetSession(view.ID)
	if got.ActiveMembers != 3 {
		t.Fatalf("active after disconnect = %d, want 3", got.ActiveMembers)
	}
	// Reconnect p1.
	if err := m.Reconnect(view.ID, "p1", 3); err != nil {
		t.Fatal(err)
	}
	got, _ = m.GetSession(view.ID)
	if got.ActiveMembers != 4 {
		t.Fatalf("active after reconnect = %d, want 4", got.ActiveMembers)
	}
	// Reconnect again is rejected (not disconnected).
	if err := m.Reconnect(view.ID, "p1", 4); err == nil {
		t.Fatal("reconnect of joined member should fail")
	}
	// Leave p2.
	if _, err := m.Leave(view.ID, "p2", 5); err != nil {
		t.Fatal(err)
	}
	got, _ = m.GetSession(view.ID)
	if got.ActiveMembers != 3 {
		t.Fatalf("active after leave = %d, want 3", got.ActiveMembers)
	}
	if got.State != model.SessionActive {
		t.Fatalf("v2 session must stay active after leave (backfill enabled)")
	}
	// End session.
	if err := m.EndSession(view.ID, 6, "test-end"); err != nil {
		t.Fatal(err)
	}
	got, _ = m.GetSession(view.ID)
	if got.State != model.SessionEnded {
		t.Fatalf("state = %d, want ended", got.State)
	}
	// Any mutation after end is rejected.
	if err := m.Disconnect(view.ID, "p1", 7); err == nil {
		t.Fatal("disconnect after end should fail")
	}
	if err := m.EndSession(view.ID, 8, "again"); err == nil {
		t.Fatal("double end should fail")
	}
}

func TestReconnectWindowV1(t *testing.T) {
	cfg := config.Default()
	m := newTestRelay(t, cfg)
	view := createSession(t, m, 1, 1, mkTeam("t1", 1, "p1"), mkTeam("t2", 1, "p2"))

	if err := m.Disconnect(view.ID, "p1", 10); err != nil {
		t.Fatal(err)
	}
	// Within retention (100 ticks): reconnect ok.
	if err := m.Reconnect(view.ID, "p1", 50); err != nil {
		t.Fatalf("reconnect within window: %v", err)
	}
	// Disconnect again and miss the window.
	if err := m.Disconnect(view.ID, "p1", 60); err != nil {
		t.Fatal(err)
	}
	if err := m.Reconnect(view.ID, "p1", 200); err == nil {
		t.Fatal("reconnect after window expiry should fail")
	}
	// Tick past retention: v1 ends the session (no backfill).
	reqs := m.Tick(60 + 101)
	if len(reqs) != 0 {
		t.Fatalf("v1 should not request backfill, got %d requests", len(reqs))
	}
	got, _ := m.GetSession(view.ID)
	if got.State != model.SessionEnded {
		t.Fatalf("v1 session should end after retention timeout, state=%d", got.State)
	}
}

func TestBackfillV2(t *testing.T) {
	cfg := config.Default()
	m := newTestRelay(t, cfg)
	view := createSession(t, m, 2, 1, mkTeam("t1", 2, "p1"), mkTeam("t2", 2, "p2"))

	if err := m.Disconnect(view.ID, "p1", 10); err != nil {
		t.Fatal(err)
	}
	// Tick past v2 retention (200): backfill requested, session stays active.
	reqs := m.Tick(10 + 201)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 backfill request, got %d", len(reqs))
	}
	if reqs[0].Slots != 1 {
		t.Fatalf("slots = %d, want 1", reqs[0].Slots)
	}
	got, _ := m.GetSession(view.ID)
	if got.State != model.SessionActive {
		t.Fatalf("session should remain active during backfill, state=%d", got.State)
	}
	if got.ActiveMembers != 1 {
		t.Fatalf("active = %d, want 1 (p2)", got.ActiveMembers)
	}
	// Complete backfill with a replacement team.
	repl := mkTeam("t3", 2, "p3")
	if _, err := m.CompleteBackfill(view.ID, "bf-1", repl, 300); err != nil {
		t.Fatalf("complete backfill: %v", err)
	}
	got, _ = m.GetSession(view.ID)
	if got.ActiveMembers != 2 {
		t.Fatalf("active after backfill = %d, want 2", got.ActiveMembers)
	}
	// Members includes the vacated (Left) member plus the two joined ones.
	if len(got.Members) != 3 {
		t.Fatalf("members = %d, want 3 (1 left + 2 joined)", len(got.Members))
	}
	found := false
	for _, mem := range got.Members {
		if mem.PlayerID == "p3" && mem.State == model.MemberJoined {
			found = true
		}
	}
	if !found {
		t.Fatal("replacement p3 not joined after backfill")
	}
	// Backfill ticket is consumed; second completion fails.
	if _, err := m.CompleteBackfill(view.ID, "bf-2", mkTeam("t4", 2, "p4"), 301); err == nil {
		t.Fatal("completing backfill without open ticket should fail")
	}
}

// TestEndSessionClearsBackfillWait: ending a session that has a pending
// backfill must produce a self-consistent terminal snapshot: ended state, an
// end reason, and no dangling wait marker.
func TestEndSessionClearsBackfillWait(t *testing.T) {
	cfg := config.Default()
	m := newTestRelay(t, cfg)
	view := createSession(t, m, 2, 1, mkTeam("t1", 2, "p1"), mkTeam("t2", 2, "p2"))
	if err := m.Disconnect(view.ID, "p1", 10); err != nil {
		t.Fatal(err)
	}
	reqs := m.Tick(10 + 201) // opens backfill
	if len(reqs) != 1 {
		t.Fatalf("expected 1 backfill request, got %d", len(reqs))
	}
	got, _ := m.GetSession(view.ID)
	if got.BackfillTicketID == "" {
		t.Fatal("backfill wait marker should be visible while waiting")
	}
	if err := m.EndSession(view.ID, 10+202, "test-end"); err != nil {
		t.Fatal(err)
	}
	got, _ = m.GetSession(view.ID)
	if got.State != model.SessionEnded {
		t.Fatalf("state = %d, want ended", got.State)
	}
	if got.BackfillTicketID != "" {
		t.Fatalf("ended session must not show a pending backfill, got %q", got.BackfillTicketID)
	}
	if got.EndReason != "test-end" {
		t.Fatalf("end reason = %q, want %q", got.EndReason, "test-end")
	}
}

func TestSessionCapacity(t *testing.T) {
	cfg := config.Default()
	cfg.MaxConcurrentSessions = 2
	m := newTestRelay(t, cfg)
	for i := 0; i < 2; i++ {
		createSession(t, m, 1, int64(i), mkTeam(fmt.Sprintf("a%d", i), 1, "p1"), mkTeam(fmt.Sprintf("b%d", i), 1, "p2"))
	}
	_, err := m.CreateSession("default", "m3", 1, []*model.Team{mkTeam("c", 1, "p1"), mkTeam("d", 1, "p2")}, 3)
	if err == nil {
		t.Fatal("third session should be rejected at capacity")
	}
	if m.ActiveSessionCount() != 2 {
		t.Fatalf("active = %d, want 2", m.ActiveSessionCount())
	}
}

func TestSnapshotIsConsistent(t *testing.T) {
	cfg := config.Default()
	m := newTestRelay(t, cfg)
	view := createSession(t, m, 1, 1, mkTeam("t1", 1, "p1", "p2"), mkTeam("t2", 1, "p3", "p4"))
	// Snapshot must be a copy: mutating the returned view must not affect the
	// manager's state.
	got, _ := m.GetSession(view.ID)
	got.Members[0].PlayerID = "hacked"
	again, _ := m.GetSession(view.ID)
	if again.Members[0].PlayerID == "hacked" {
		t.Fatal("snapshot aliases internal state")
	}
}

func TestBroadcastDelivery(t *testing.T) {
	cfg := config.Default()
	cfg.MessageBufferSize = 4
	m := newTestRelay(t, cfg)
	view := createSession(t, m, 1, 1, mkTeam("t1", 1, "p1"), mkTeam("t2", 1, "p2"))
	ch1 := make(chan model.Message, 4)
	ch2 := make(chan model.Message, 4)
	m.RegisterPipe(view.ID, "p1", ch1)
	m.RegisterPipe(view.ID, "p2", ch2)

	if err := m.Broadcast(view.ID, "p1", []byte("hello"), 2); err != nil {
		t.Fatal(err)
	}
	msg := <-ch1
	if string(msg.Payload) != "hello" || msg.From != "p1" {
		t.Fatalf("bad message: %+v", msg)
	}
	<-ch2

	// Slow consumer: fill ch1's buffer, then broadcast; must not block.
	for i := 0; i < 4; i++ {
		ch1 <- model.Message{}
	}
	done := make(chan struct{})
	go func() {
		_ = m.Broadcast(view.ID, "p2", []byte("drop"), 3)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on slow consumer")
	}
}

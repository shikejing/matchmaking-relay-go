package match

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
)

// TestBackfillTimeoutViaTicks: with no suitable replacement queued, a backfill
// wait must end the session after the rule's timeout — driven purely by
// logical clock advancement, never hanging in the active state.
func TestBackfillTimeoutViaTicks(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	e := newTestEngine(t, cfg)

	r2 := makeTeam("t1", 100, 1)
	r2.RuleVersion = 2
	r3 := makeTeam("t2", 100, 1)
	r3.RuleVersion = 2
	if _, err := e.Enqueue(r2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Enqueue(r3); err != nil {
		t.Fatal(err)
	}
	e.Tick()
	sessions := e.relay.ListSessions("default")
	if len(sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(sessions))
	}
	sid := sessions[0].ID

	q, _ := e.GetQueue("default")
	if err := e.relay.Disconnect(sid, sessions[0].Members[0].PlayerID, q.Tick); err != nil {
		t.Fatal(err)
	}
	// v2 retention = 200 ticks.
	for i := 0; i < 201; i++ {
		e.Tick()
	}
	view, _ := e.relay.GetSession(sid)
	if view.State != model.SessionActive || view.BackfillTicketID == "" {
		t.Fatalf("backfill wait should be pending: state=%d ticket=%q", view.State, view.BackfillTicketID)
	}
	// v2 backfill timeout = 50 ticks.
	for i := 0; i < 51; i++ {
		e.Tick()
	}
	view, _ = e.relay.GetSession(sid)
	if view.State != model.SessionEnded {
		t.Fatalf("session must end after backfill timeout, state=%d", view.State)
	}
	if view.EndReason != relay.EndReasonBackfillTimeout {
		t.Fatalf("end reason = %q, want %q", view.EndReason, relay.EndReasonBackfillTimeout)
	}
	recs := e.audit.Query(audit.Filter{SessionID: sid, Types: []audit.EventType{audit.EventBackfillTimeout}})
	if len(recs) != 1 {
		t.Fatalf("BACKFILL_TIMEOUT records = %d, want 1", len(recs))
	}
	// The partition state must not retain the expired ticket: enqueueing a
	// matching team now must NOT backfill the ended session.
	if _, err := e.Enqueue(makeTeam("late", 100, 1)); err != nil {
		t.Fatal(err)
	}
	e.Tick()
	view, _ = e.relay.GetSession(sid)
	if view.State != model.SessionEnded || view.ActiveMembers != 1 {
		t.Fatalf("ended session changed: state=%d active=%d", view.State, view.ActiveMembers)
	}
}

// TestBackfillParallelPartitions: partitions advancing concurrently (each on
// its own goroutine, as the background loop does) must not deadlock, must not
// block each other, and a backfill wait in one partition must never consume
// teams queued in another.
func TestBackfillParallelPartitions(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 32
	e := newTestEngine(t, cfg) // partitions: default, pA, pB

	// pA: a v2 session whose member will disconnect and wait for a backfill
	// that never comes.
	mk := func(id, partition string, rule int) EnqueueRequest {
		req := makeTeam(id, 100, 1)
		req.Partition = model.PartitionID(partition)
		req.RuleVersion = rule
		return req
	}
	if _, err := e.Enqueue(mk("pa-1", "pA", 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Enqueue(mk("pa-2", "pA", 2)); err != nil {
		t.Fatal(err)
	}
	// pB: teams that must keep matching while pA waits.
	for i := 0; i < 4; i++ {
		if _, err := e.Enqueue(mk(fmt.Sprintf("pb-%d", i), "pB", 2)); err != nil {
			t.Fatal(err)
		}
	}

	e.tickPartition("pA")
	e.tickPartition("pB")
	sessionsA := e.relay.ListSessions("pA")
	if len(sessionsA) != 1 {
		t.Fatalf("pA sessions = %d, want 1", len(sessionsA))
	}
	sidA := sessionsA[0].ID
	qA, _ := e.GetQueue("pA")
	if err := e.relay.Disconnect(sidA, sessionsA[0].Members[0].PlayerID, qA.Tick); err != nil {
		t.Fatal(err)
	}

	// Advance both partitions concurrently, well past retention+timeout.
	const ticks = 300
	var wg sync.WaitGroup
	wg.Add(2)
	for _, pid := range []model.PartitionID{"pA", "pB"} {
		pid := pid
		go func() {
			defer wg.Done()
			for i := 0; i < ticks; i++ {
				e.tickPartition(pid)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: concurrent partition ticks did not finish")
	}

	// pA: the backfill wait timed out and ended the session.
	viewA, _ := e.relay.GetSession(sidA)
	if viewA.State != model.SessionEnded || viewA.EndReason != relay.EndReasonBackfillTimeout {
		t.Fatalf("pA session: state=%d reason=%q, want ended/%s", viewA.State, viewA.EndReason, relay.EndReasonBackfillTimeout)
	}
	// pB: its teams formed their own sessions; none were stolen by pA.
	sessionsB := e.relay.ListSessions("pB")
	if len(sessionsB) != 2 {
		t.Fatalf("pB sessions = %d, want 2", len(sessionsB))
	}
	for _, s := range sessionsB {
		for _, m := range s.Members {
			if m.TeamID != "pb-0" && m.TeamID != "pb-1" && m.TeamID != "pb-2" && m.TeamID != "pb-3" {
				t.Fatalf("pB session contains foreign team %s", m.TeamID)
			}
		}
	}
	qB, _ := e.GetQueue("pB")
	if qB.Waiting != 0 {
		t.Fatalf("pB waiting = %d, want 0", qB.Waiting)
	}
	// pA's session members are untouched (no cross-partition backfill).
	for _, m := range viewA.Members {
		if m.TeamID != "pa-1" && m.TeamID != "pa-2" {
			t.Fatalf("pA session contains foreign team %s", m.TeamID)
		}
	}
}

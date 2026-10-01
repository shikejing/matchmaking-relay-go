package service

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/shikejing/matchmaking-relay-go/internal/model"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/match"
)

// TestConcurrentDuplicateTeam: many goroutines submit the SAME team; exactly
// one must succeed and the team must never be matched twice.
func TestConcurrentDuplicateTeam(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 100
	svc := newTestService(t, cfg)

	var wg sync.WaitGroup
	var ok, dup int64
	var mu sync.Mutex
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.EnqueueTeam("default", "same-team", players("same-team", 1, 100), 0)
			mu.Lock()
			if err == nil {
				ok++
			} else {
				var me *match.Error
				if !errors.As(err, &me) || me.Code != match.RejectDuplicateTeam {
					t.Errorf("unexpected error: %v", err)
				}
				dup++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ok != 1 {
		t.Fatalf("successful enqueues = %d, want exactly 1", ok)
	}
	if dup != 31 {
		t.Fatalf("duplicate rejections = %d, want 31", dup)
	}
}

// TestNoStarvation: an early low-score team must be matched even when many
// higher-score teams arrive later (wait-time priority).
func TestNoStarvation(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 100
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "early", 1, 100, 0)
	// Advance a tick so "early" has waited.
	svc.Tick()
	for i := 0; i < 10; i++ {
		enqueueOrFail(t, svc, "default", fmt.Sprintf("late%d", i), 1, 1000, 0)
	}
	svc.Tick()
	// The early team must have been matched (it is the anchor); matched teams
	// leave the bounded queue and live on in their session.
	found := false
	for _, s := range svc.ListSessions("") {
		for _, m := range s.Members {
			if m.TeamID == "early" {
				found = true
				if s.State != model.SessionActive {
					t.Fatalf("early team's session state = %d, want active", s.State)
				}
			}
		}
	}
	if !found {
		t.Fatal("early team was not matched (starved by later high-score teams)")
	}
}

// TestQueueCapUnderBurst: a burst beyond capacity must be rejected, never
// panic or grow the queue beyond its bound.
func TestQueueCapUnderBurst(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 8
	svc := newTestService(t, cfg)

	var wg sync.WaitGroup
	var rejected int64
	var mu sync.Mutex
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.EnqueueTeam("default", model.TeamID(fmt.Sprintf("b%d", i)), players(fmt.Sprintf("b%d", i), 1, 100), 0)
			if err != nil {
				var me *match.Error
				if errors.As(err, &me) && me.Code == match.RejectQueueFull {
					mu.Lock()
					rejected++
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	q, _ := svc.QueryQueue("default")
	if q.Waiting > cfg.QueueCapacity {
		t.Fatalf("queue grew beyond capacity: %d > %d", q.Waiting, cfg.QueueCapacity)
	}
	if rejected == 0 {
		t.Fatal("expected QUEUE_FULL rejections under burst")
	}
}

// TestAuditRingBounded: the audit log must retain at most AuditCapacity
// records even under a long run.
func TestAuditRingBounded(t *testing.T) {
	cfg := config.Default()
	cfg.AuditCapacity = 16
	cfg.QueueCapacity = 100
	svc := newTestService(t, cfg)
	for i := 0; i < 100; i++ {
		enqueueOrFail(t, svc, "default", fmt.Sprintf("a%d", i), 1, 100, 0)
	}
	if got := svc.Audit().Len(); got > cfg.AuditCapacity {
		t.Fatalf("audit records = %d, bound %d", got, cfg.AuditCapacity)
	}
	// Records must remain queryable in order.
	recs := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventEnqueue}})
	if len(recs) == 0 {
		t.Fatal("no enqueue records retained")
	}
	for i := 1; i < len(recs); i++ {
		if recs[i-1].Seq >= recs[i].Seq {
			t.Fatal("audit records out of order")
		}
	}
}

// TestConfigValidation: impossible limits are rejected at startup.
func TestConfigValidation(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 0
	if _, err := New(cfg); err == nil {
		t.Fatal("zero queue capacity should be rejected")
	}
	cfg = config.Default()
	cfg.Partitions = []string{"p", "p"}
	if _, err := New(cfg); err == nil {
		t.Fatal("duplicate partition should be rejected")
	}
}

// TestReconnectDoesNotDoubleCount: reconnecting a member must not add a
// second member entry.
func TestReconnectDoesNotDoubleCount(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	enqueueOrFail(t, svc, "default", "t1", 1, 100, 0)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 0)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	p1 := svc.ListSessions("")[0].Members[0].PlayerID

	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	if err := svc.Reconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	view, _ := svc.QuerySession(sid)
	count := 0
	for _, m := range view.Members {
		if m.PlayerID == p1 {
			count++
			if m.State != model.MemberJoined {
				t.Fatalf("member state = %d, want joined", m.State)
			}
		}
	}
	if count != 1 {
		t.Fatalf("member %s appears %d times, want 1", p1, count)
	}
}

// TestBackfillDoesNotCrossPartitions: a backfill request in partition A must
// never consume teams queued in partition B.
func TestBackfillDoesNotCrossPartitions(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	cfg.Partitions = []string{"pA", "pB"}
	svc := newTestService(t, cfg)

	// v2 session in pA.
	enqueueOrFail(t, svc, "pA", "a1", 1, 100, 2)
	enqueueOrFail(t, svc, "pA", "a2", 1, 100, 2)
	svc.Tick()
	sid := svc.ListSessions("pA")[0].ID
	p1 := svc.ListSessions("pA")[0].Members[0].PlayerID

	// pB has its own waiting teams.
	enqueueOrFail(t, svc, "pB", "b1", 1, 100, 2)
	enqueueOrFail(t, svc, "pB", "b2", 1, 100, 2)

	// Disconnect pA member past v2 retention.
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 201; i++ {
		svc.Tick()
	}
	// b1/b2 must have formed their own match in pB, never consumed by pA.
	qB, _ := svc.QueryQueue("pB")
	if qB.Waiting != 0 {
		t.Fatalf("pB waiting = %d, want 0 (b1/b2 matched in pB)", qB.Waiting)
	}
	pBSessions := svc.ListSessions("pB")
	if len(pBSessions) != 1 {
		t.Fatalf("pB sessions = %d, want 1", len(pBSessions))
	}
	for _, m := range pBSessions[0].Members {
		if m.TeamID != "b1" && m.TeamID != "b2" {
			t.Fatalf("pB session contains foreign team %s", m.TeamID)
		}
	}
	// pA session still active with a backfill ticket open.
	got, _ := svc.QuerySession(sid)
	if got.State != model.SessionActive {
		t.Fatalf("pA session state = %d, want active", got.State)
	}
}

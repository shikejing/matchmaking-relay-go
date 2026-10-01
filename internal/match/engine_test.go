package match

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
)

func newTestEngine(t *testing.T, cfg config.Config) *Engine {
	t.Helper()
	cfg.Partitions = []string{"default", "pA", "pB"}
	regs, err := rule.NewRegistry(rule.DefaultVersion)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	auditLog := audit.NewLog(cfg.AuditCapacity)
	relayMgr := relay.NewManager(cfg, regs, auditLog)
	return NewEngine(cfg, regs, auditLog, relayMgr)
}

func makeTeam(id string, score int, members int) EnqueueRequest {
	players := make([]model.Player, members)
	for i := range players {
		players[i] = model.Player{ID: model.PlayerID(fmt.Sprintf("%s-p%d", id, i)), Score: score}
	}
	return EnqueueRequest{
		Partition: "default",
		TeamID:    model.TeamID(id),
		Players:   players,
	}
}

func TestEnqueueDuplicateRejected(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	e := newTestEngine(t, cfg)
	req := makeTeam("t1", 100, 2)
	if _, err := e.Enqueue(req); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	_, err := e.Enqueue(req)
	if err == nil {
		t.Fatal("duplicate enqueue should be rejected")
	}
	me, ok := err.(*Error)
	if !ok || me.Code != RejectDuplicateTeam {
		t.Fatalf("expected DUPLICATE_TEAM, got %v", err)
	}
}

func TestEnqueueQueueFull(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 2
	e := newTestEngine(t, cfg)
	for i := 0; i < 2; i++ {
		if _, err := e.Enqueue(makeTeam(fmt.Sprintf("t%d", i), 100, 1)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	_, err := e.Enqueue(makeTeam("t2", 100, 1))
	if err == nil {
		t.Fatal("third enqueue should be rejected")
	}
	me, ok := err.(*Error)
	if !ok || me.Code != RejectQueueFull {
		t.Fatalf("expected QUEUE_FULL, got %v", err)
	}
}

func TestEnqueueEmptyTeamRejected(t *testing.T) {
	cfg := config.Default()
	e := newTestEngine(t, cfg)
	_, err := e.Enqueue(EnqueueRequest{Partition: "default", TeamID: "empty", Players: nil})
	if err == nil {
		t.Fatal("empty team should be rejected")
	}
}

func TestMatchFormedAndTeamIntegrity(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	e := newTestEngine(t, cfg)
	for i := 0; i < 4; i++ {
		if _, err := e.Enqueue(makeTeam(fmt.Sprintf("t%d", i), 100+i*10, 2)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	e.Tick()
	// 4 teams / 2 per match = 2 sessions.
	relayMgr := e.relay
	sessions := relayMgr.ListSessions("")
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}
	for _, s := range sessions {
		if s.State != model.SessionActive {
			t.Fatalf("session %s state = %d, want active", s.ID, s.State)
		}
		if len(s.Members) != 4 {
			t.Fatalf("session %s has %d members, want 4 (2 teams of 2)", s.ID, len(s.Members))
		}
		// Members must come in intact teams: check team grouping.
		teams := map[model.TeamID]int{}
		for _, m := range s.Members {
			teams[m.TeamID]++
		}
		if len(teams) != 2 {
			t.Fatalf("session %s has %d distinct teams, want 2", s.ID, len(teams))
		}
		for tid, n := range teams {
			if n != 2 {
				t.Fatalf("team %s split: %d members in session", tid, n)
			}
		}
	}
	q, _ := e.GetQueue("default")
	if q.Waiting != 0 {
		t.Fatalf("queue should be empty, got %d waiting", q.Waiting)
	}
}

func TestCancelWaitingTeam(t *testing.T) {
	cfg := config.Default()
	e := newTestEngine(t, cfg)
	if _, err := e.Enqueue(makeTeam("t1", 100, 1)); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel("t1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := e.Cancel("t1"); err == nil {
		t.Fatal("double cancel should fail")
	}
	// After cancel, the team can be re-enqueued (idempotency window passed).
	if _, err := e.Enqueue(makeTeam("t1", 100, 1)); err != nil {
		t.Fatalf("re-enqueue after cancel: %v", err)
	}
}

func TestPartitionIsolation(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	e := newTestEngine(t, cfg)
	// 2 teams in pA, 1 team in pB.
	for i, pid := range []string{"pA", "pA"} {
		req := makeTeam(fmt.Sprintf("%s-t%d", pid, i), 100, 1)
		req.Partition = model.PartitionID(pid)
		if _, err := e.Enqueue(req); err != nil {
			t.Fatal(err)
		}
	}
	req := makeTeam("pB-t0", 100, 1)
	req.Partition = "pB"
	if _, err := e.Enqueue(req); err != nil {
		t.Fatal(err)
	}
	e.Tick()
	sessions := e.relay.ListSessions("")
	if len(sessions) != 1 {
		t.Fatalf("expected exactly 1 session (pA), got %d", len(sessions))
	}
	if sessions[0].Partition != "pA" {
		t.Fatalf("session in partition %s, want pA", sessions[0].Partition)
	}
	qB, _ := e.GetQueue("pB")
	if qB.Waiting != 1 {
		t.Fatalf("pB should still have 1 waiting team, got %d", qB.Waiting)
	}
}

// grouping returns a canonical representation of session->teams grouping.
func grouping(e *Engine) [][]model.TeamID {
	sessions := e.relay.ListSessions("")
	out := make([][]model.TeamID, 0, len(sessions))
	for _, s := range sessions {
		teams := map[model.TeamID]bool{}
		for _, m := range s.Members {
			teams[m.TeamID] = true
		}
		ids := make([]model.TeamID, 0, len(teams))
		for id := range teams {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out = append(out, ids)
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i]) != len(out[j]) {
			return len(out[i]) < len(out[j])
		}
		for k := range out[i] {
			if out[i][k] != out[j][k] {
				return out[i][k] < out[j][k]
			}
		}
		return false
	})
	return out
}

// enqueueOrder returns team ids sorted by their assigned enqueue sequence.
func enqueueOrder(e *Engine) []model.TeamID {
	// Collect from audit ENQUEUE records (seq order).
	recs := e.audit.Query(audit.Filter{Types: []audit.EventType{audit.EventEnqueue}})
	out := make([]model.TeamID, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.TeamID)
	}
	return out
}

func TestDeterminismUnderConcurrency(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 200
	cfg.MatchBatchSize = 20
	cfg.MaxConcurrentSessions = 100

	buildTeams := func() []EnqueueRequest {
		reqs := make([]EnqueueRequest, 0, 24)
		for i := 0; i < 24; i++ {
			req := makeTeam(fmt.Sprintf("team-%02d", i), 100+(i%5)*7, 1+i%3)
			reqs = append(reqs, req)
		}
		return reqs
	}

	// runConcurrent enqueues with many goroutines, then ticks until empty.
	runConcurrent := func() ([][]model.TeamID, []model.TeamID) {
		e := newTestEngine(t, cfg)
		reqs := buildTeams()
		var wg sync.WaitGroup
		for _, req := range reqs {
			req := req
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := e.Enqueue(req); err != nil {
					t.Errorf("enqueue %s: %v", req.TeamID, err)
				}
			}()
		}
		wg.Wait()
		order := enqueueOrder(e)
		for i := 0; i < 20; i++ {
			e.Tick()
		}
		return grouping(e), order
	}

	// replay enqueues in the given order sequentially, then ticks.
	replay := func(order []model.TeamID) [][]model.TeamID {
		e := newTestEngine(t, cfg)
		reqs := buildTeams()
		byID := make(map[model.TeamID]EnqueueRequest, len(reqs))
		for _, r := range reqs {
			byID[r.TeamID] = r
		}
		for _, id := range order {
			if _, err := e.Enqueue(byID[id]); err != nil {
				t.Fatalf("replay enqueue %s: %v", id, err)
			}
		}
		for i := 0; i < 20; i++ {
			e.Tick()
		}
		return grouping(e)
	}

	g1, o1 := runConcurrent()
	g2, o2 := runConcurrent()
	// The enqueue order under concurrency is the order of atomic sequence
	// assignment; it may differ between runs. The required invariant is:
	// given the SAME enqueue order, the concurrent run and a sequential
	// replay produce逐条一致 (item-for-item identical) matches, no matter
	// the concurrency degree.
	if rg := replay(o1); !equalGrouping(g1, rg) {
		t.Fatalf("concurrent run != sequential replay of same order:\n%v\nvs\n%v", g1, rg)
	}
	if rg := replay(o2); !equalGrouping(g2, rg) {
		t.Fatalf("concurrent run != sequential replay of same order:\n%v\nvs\n%v", g2, rg)
	}
	// A third run under GOMAXPROCS=1 must also match its own replay.
	prev := runtime.GOMAXPROCS(1)
	g3, o3 := runConcurrent()
	runtime.GOMAXPROCS(prev)
	if rg := replay(o3); !equalGrouping(g3, rg) {
		t.Fatalf("concurrent run (GOMAXPROCS=1) != sequential replay:\n%v\nvs\n%v", g3, rg)
	}
}

func equalGrouping(a, b [][]model.TeamID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func TestNoDeadlockUnderLoad(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 50
	cfg.MaxConcurrentSessions = 50
	e := newTestEngine(t, cfg)
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			req := makeTeam(fmt.Sprintf("load-%d", i), 100+i, 1)
			wg.Add(1)
			go func(r EnqueueRequest) {
				defer wg.Done()
				_, _ = e.Enqueue(r)
			}(req)
		}
		wg.Wait()
		for i := 0; i < 30; i++ {
			e.Tick()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock detected under concurrent load")
	}
}

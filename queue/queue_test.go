package queue

import (
	"sync"
	"testing"

	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/rules"
	"github.com/shikejing/matchmaking-relay-go/types"
)

func testQueue(cap int) *Queue {
	pc := config.PartitionConfig{
		ID: "p", QueueCapacity: cap, MaxTeamSize: 5, TargetTeamSize: 4,
		Version: "v1",
	}
	return NewQueue(pc, rules.NewV1(4, 100, 1000), clock.NewFakeClock(100))
}

func team(id string, pids ...string) types.Team {
	ps := make([]types.Player, len(pids))
	for i, p := range pids {
		ps[i] = types.Player{ID: types.PlayerID(p), Score: 10}
	}
	return types.Team{ID: types.TeamID(id), Players: ps, Version: "v1"}
}

func TestSubmitDedup(t *testing.T) {
	q := testQueue(10)
	if r := q.Submit(team("t1", "a", "b"), 1); !r.Accepted {
		t.Fatalf("first submit rejected: %v", r)
	}
	r := q.Submit(team("t1", "a", "b"), 2)
	if r.Accepted || r.Reason != types.RejectDuplicateTeam {
		t.Fatalf("duplicate team must be rejected distinctly, got %v", r)
	}
	// Same player in another team is rejected too.
	r = q.Submit(team("t2", "a"), 3)
	if r.Accepted || r.Reason != types.RejectPlayerAlreadyQueued {
		t.Fatalf("busy player must be rejected, got %v", r)
	}
	if q.Len() != 1 {
		t.Fatalf("queue len = %d, want 1", q.Len())
	}
}

func TestSubmitCapacity(t *testing.T) {
	q := testQueue(1)
	if r := q.Submit(team("t1", "a"), 1); !r.Accepted {
		t.Fatal(r)
	}
	r := q.Submit(team("t2", "b"), 2)
	if r.Accepted || r.Reason != types.RejectQueueFull {
		t.Fatalf("full queue must return queue_full, got %v", r)
	}
}

func TestConcurrentDuplicateOnce(t *testing.T) {
	q := testQueue(100)
	var wg sync.WaitGroup
	var accepted int32
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := q.Submit(team("dup", "x", "y"), 7)
			if r.Accepted {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("concurrent duplicate accepted %d times, want 1", accepted)
	}
	if q.Len() != 1 {
		t.Fatalf("len = %d, want 1", q.Len())
	}
}

func TestTakeAndRestore(t *testing.T) {
	q := testQueue(10)
	q.Submit(team("t1", "a"), 1)
	q.Submit(team("t2", "b"), 2)
	q.Submit(team("t3", "c"), 3)
	taken := q.Take([]types.TeamID{"t2", "t1"})
	if len(taken) != 2 || taken[0].Team.ID != "t1" || taken[1].Team.ID != "t2" {
		t.Fatalf("take must preserve sequence order, got %v", taken)
	}
	if q.Len() != 1 || q.Entries()[0].Team.ID != "t3" {
		t.Fatalf("remaining queue wrong: %v", q.Snapshot())
	}
	q.Restore(taken)
	got := q.Entries()
	if len(got) != 3 || got[0].Team.ID != "t1" || got[1].Team.ID != "t2" || got[2].Team.ID != "t3" {
		t.Fatalf("restore order wrong: %v", got)
	}
}

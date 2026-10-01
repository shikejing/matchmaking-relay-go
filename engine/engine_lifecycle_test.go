package engine

import (
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/clock"
)

// Background loops must advance partitions, be stoppable without
// deadlock, and be idempotent on Start/Stop.
func TestStartStopLoops(t *testing.T) {
	c := cfgTwoPartitions("v1", "v1")
	c.TickIntervalMs = 10
	e, _ := New(c, clock.NewFakeClock(0))

	e.SubmitTeam("A", team("z1", 10, "aa1", "aa2"))
	e.SubmitTeam("A", team("z2", 10, "aa3", "aa4"))

	e.Start()
	e.Start() // idempotent
	deadline := time.Now().Add(2 * time.Second)
	var matched bool
	for time.Now().Before(deadline) {
		if e.Manager().ActiveCount() == 1 {
			matched = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !matched {
		t.Fatal("background tick did not create session")
	}
	e.Stop()
	e.Stop() // idempotent
	// Sessions are evicted on stop.
	if e.Manager().ActiveCount() != 0 {
		t.Fatalf("sessions remain after stop: %d", e.Manager().ActiveCount())
	}
}

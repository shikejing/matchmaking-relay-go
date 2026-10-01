package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/types"
)

// End-to-end: team submit -> match -> dial & relay -> disconnect ->
// reconnect within the window -> session continues.
func TestEndToEndDisconnectReconnect(t *testing.T) {
	fc := clock.NewFakeClock(0)
	e, _ := New(cfgTwoPartitions("v1", "v1"), fc)
	e.SubmitTeam("A", team("k1", 10, "r1", "r2"))
	e.SubmitTeam("A", team("k2", 10, "r3", "r4"))
	sids := e.TickPartition("A")
	if len(sids) != 1 {
		t.Fatalf("want session, got %v", sids)
	}
	sid := sids[0]
	peers := map[types.PlayerID]*transportPeer{}
	// helper repeated Dial
	dial := func(p types.PlayerID) *transportPeer {
		peer, res := e.Dial(sid, p)
		if !res.Accepted {
			t.Fatalf("dial %s: %v", p, res)
		}
		return &transportPeer{peer}
	}
	for _, p := range []types.PlayerID{"r1", "r2", "r3", "r4"} {
		peers[p] = dial(p)
	}
	if s := e.Sessions()[0].State; s != types.SessionActive {
		t.Fatalf("state %s, want active", s)
	}
	// r1 broadcasts; r2 receives.
	peers["r1"].send(types.PlayerID("r1"), "", []byte("ping"))
	select {
	case m := <-peers["r2"].Recv():
		if string(m.Payload) != "ping" {
			t.Fatalf("relay payload %q", m.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not deliver")
	}

	// Simulate r1's network drop.
	peers["r1"].Disconnect()
	// Pump notices pipe close asynchronously; allow it briefly.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if e.Sessions()[0].State == types.SessionDegraded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if s := e.Sessions()[0].State; s != types.SessionDegraded {
		t.Fatalf("state = %s, want degraded", s)
	}

	// Reconnect before window (hold=100ms fake time, no fake time moved).
	peers["r1"] = dial("r1")
	if s := e.Sessions()[0].State; s != types.SessionActive {
		t.Fatalf("state after reconnect = %s, want active", s)
	}
	// Membership not duplicated.
	n := 0
	for _, m := range e.Sessions()[0].Members {
		if m.Player == "r1" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("r1 counted %d times", n)
	}

	// Window expiry ends the session deterministically.
	peers["r4"].Disconnect()
	time.Sleep(20 * time.Millisecond)
	fc.Advance(101)
	e.Manager().Tick(fc.NowMs())
	e.Manager().ReapClosed()
	if e.Manager().ActiveCount() != 0 {
		t.Fatalf("session should be reaped after window, count=%d", e.Manager().ActiveCount())
	}
}

// High-load hammer: many concurrent submitters, tickers and dials must
// not deadlock and queue/session counts must stay within configured caps.
func TestHighLoadBoundedNoDeadlock(t *testing.T) {
	c := cfgTwoPartitions("v1", "v1")
	c.Partitions[0].QueueCapacity = 50
	c.Partitions[1].QueueCapacity = 50
	c.MaxSessions = 20
	c.Partitions[0].MaxBatch = 4
	c.Partitions[1].MaxBatch = 4
	c.PipeBuffer = 2
	e, _ := New(c, clock.NewFakeClock(0))

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(3)
			go func(i int) {
				defer wg.Done()
				pid := types.PartitionID("A")
				if i%2 == 0 {
					pid = "B"
				}
				tm := team(
					"t"+itoa(i),
					10+(i%5),
					"p"+itoa(i)+"a", "p"+itoa(i)+"b",
				)
				e.SubmitTeam(pid, tm)
			}(i)
			go func() { defer wg.Done(); e.TickPartition("A") }()
			go func() { defer wg.Done(); e.TickPartition("B") }()
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock or starvation under high load")
	}
	if l := len(e.QueueSnapshot("A")); l > 50 {
		t.Fatalf("A queue unbounded: %d", l)
	}
	if l := len(e.QueueSnapshot("B")); l > 50 {
		t.Fatalf("B queue unbounded: %d", l)
	}
	if n := e.Manager().ActiveCount(); n > 20 {
		t.Fatalf("sessions exceeded cap: %d", n)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

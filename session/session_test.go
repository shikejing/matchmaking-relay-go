package session

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/audit"
	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/queue"
	"github.com/shikejing/matchmaking-relay-go/transport"
	"github.com/shikejing/matchmaking-relay-go/types"
)

func optsFor(policy config.ReconnectPolicy, hold int64) Options {
	pc := config.PartitionConfig{
		ID: "p", Version: "v1", TargetTeamSize: 4, MaxTeamSize: 5,
		Reconnect: policy, ReconnectHoldMs: hold, QueueCapacity: 100,
	}
	return Options{
		ID: "s1", Partition: "p", Version: "v1", PC: pc,
		Broker: transport.NewBroker(8), Audit: audit.NewRecorder(64),
		Clock: clock.NewFakeClock(1000), NowMs: 1000,
		Teams: []TeamMembers{
			{Team: "t1", Players: []types.PlayerID{"a", "b"}},
			{Team: "t2", Players: []types.PlayerID{"c", "d"}},
		},
	}
}

func join(t *testing.T, s *Session, broker *transport.Broker, p types.PlayerID) *transport.Peer {
	t.Helper()
	peer, sp := broker.Dial(p)
	if r := s.Join(p, sp); !r.Accepted {
		t.Fatalf("join %s: %v", p, r)
	}
	return peer
}

func TestLifecycleActiveThenReconnect(t *testing.T) {
	o := optsFor(config.PolicyEndSession, 100)
	s := NewSession(o)
	if snap := s.Snapshot(); snap.State != types.SessionGathering || len(snap.Members) != 4 {
		t.Fatalf("initial snapshot wrong: %+v", snap)
	}
	for _, p := range []types.PlayerID{"a", "b", "c", "d"} {
		join(t, s, o.Broker, p)
	}
	if snap := s.Snapshot(); snap.State != types.SessionActive {
		t.Fatalf("state = %s, want active", snap.State)
	}

	s.Disconnect("b")
	if snap := s.Snapshot(); snap.State != types.SessionDegraded {
		t.Fatalf("state = %s, want degraded", snap.State)
	}
	// b stays disconnected but not counted present.
	if s.MemberCount() != 3 {
		t.Fatalf("present = %d, want 3", s.MemberCount())
	}
	// Duplicate reconnect attempts via two dials must not double-count.
	_, sp1 := o.Broker.Dial("b")
	if r := s.Reconnect("b", sp1); !r.Accepted {
		t.Fatalf("reconnect: %v", r)
	}
	_, sp2 := o.Broker.Dial("b")
	if r := s.Reconnect("b", sp2); r.Accepted {
		t.Fatal("second reconnect while present must be rejected")
	}
	snap := s.Snapshot()
	if snap.State != types.SessionActive || len(snap.Members) != 4 {
		t.Fatalf("after reconnect snapshot wrong: %+v", snap)
	}
	count := 0
	for _, m := range snap.Members {
		if m.Player == "b" && m.State == types.MemberPresent {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("member b counted %d times, want 1", count)
	}
}

func TestEndSessionPolicy(t *testing.T) {
	o := optsFor(config.PolicyEndSession, 100)
	s := NewSession(o)
	for _, p := range []types.PlayerID{"a", "b", "c", "d"} {
		join(t, s, o.Broker, p)
	}
	s.Disconnect("c")
	o.Clock.(*clock.FakeClock).Advance(101)
	s.Tick(o.Clock.NowMs())
	snap := s.Snapshot()
	if snap.State != types.SessionClosed || snap.CloseReason != types.CloseDisconnectTimeout {
		t.Fatalf("want closed/disconnect_timeout, got %s/%s", snap.State, snap.CloseReason)
	}
	if !s.Done() {
		t.Fatal("session must be done")
	}
	// Reconnect into closed session rejected.
	_, sp := o.Broker.Dial("c")
	if r := s.Reconnect("c", sp); r.Accepted {
		t.Fatal("reconnect after close must fail")
	}
}

func TestReconnectWithinWindow(t *testing.T) {
	o := optsFor(config.PolicyEndSession, 100)
	s := NewSession(o)
	for _, p := range []types.PlayerID{"a", "b", "c", "d"} {
		join(t, s, o.Broker, p)
	}
	s.Disconnect("d")
	o.Clock.(*clock.FakeClock).Advance(50)
	_, sp := o.Broker.Dial("d")
	if r := s.Reconnect("d", sp); !r.Accepted {
		t.Fatalf("reconnect inside window: %v", r)
	}
	o.Clock.(*clock.FakeClock).Advance(60)
	s.Tick(o.Clock.NowMs())
	if s.Snapshot().State != types.SessionActive {
		t.Fatalf("session should survive, got %s", s.Snapshot().State)
	}
}

func TestBackfillPolicy(t *testing.T) {
	o := optsFor(config.PolicyBackfill, 100)
	var fill queue.Entry
	var have bool
	o.Fill = func(pc config.PartitionConfig, needSize int, except types.TeamID) (queue.Entry, bool) {
		if !have || except != "t1" || needSize != 2 {
			return queue.Entry{}, false
		}
		return fill, true
	}
	s := NewSession(o)
	for _, p := range []types.PlayerID{"a", "b", "c", "d"} {
		join(t, s, o.Broker, p)
	}
	// Disconnect the whole t1 team.
	s.Disconnect("a")
	s.Disconnect("b")

	// No fill available -> close with backfill_failed.
	o.Clock.(*clock.FakeClock).Advance(101)
	s.Tick(o.Clock.NowMs())
	if !s.Done() {
		t.Fatal("session should close when backfill unavailable")
	}
	if r := s.Snapshot().CloseReason; r != types.CloseBackfillFailed {
		t.Fatalf("reason = %s, want backfill_failed", r)
	}

	// Fresh session with an available fill.
	o2 := o
	o2.ID = "s2"
	o2.Clock = clock.NewFakeClock(2000)
	o2.NowMs = 2000
	have = true
	fill = queue.Entry{
		Team: types.Team{ID: "t3", Version: "v1", Players: []types.Player{
			{ID: "e"}, {ID: "f"},
		}},
	}
	s2 := NewSession(o2)
	for _, p := range []types.PlayerID{"a", "b", "c", "d"} {
		join(t, s2, o2.Broker, p)
	}
	s2.Disconnect("a")
	s2.Disconnect("b")
	o2.Clock.(*clock.FakeClock).Advance(101)
	s2.Tick(o2.Clock.NowMs())
	snap := s2.Snapshot()
	if snap.State != types.SessionGathering {
		t.Fatalf("after backfill want gathering for replacement joins, got %s", snap.State)
	}
	ids := map[types.PlayerID]bool{}
	for _, m := range snap.Members {
		ids[m.Player] = true
	}
	if !ids["e"] || !ids["f"] || ids["a"] || ids["b"] {
		t.Fatalf("membership after backfill wrong: %v", ids)
	}
	// Old removed players can't re-enter.
	if s2.HasPlayer("a") {
		t.Fatal("removed player must no longer be a member")
	}
}

func TestBroadcastRelay(t *testing.T) {
	o := optsFor(config.PolicyHoldSlot, 0)
	s := NewSession(o)
	peers := map[types.PlayerID]*transport.Peer{}
	for _, p := range []types.PlayerID{"a", "b", "c", "d"} {
		peers[p] = join(t, s, o.Broker, p)
	}
	s.Send(transport.Message{From: "a", Payload: []byte("hi")})
	for _, p := range []types.PlayerID{"b", "c", "d"} {
		select {
		case m := <-peers[p].Recv():
			if string(m.Payload) != "hi" || m.From != "a" {
				t.Fatalf("unexpected relay msg: %+v", m)
			}
		default:
			t.Fatalf("peer %s did not receive broadcast", p)
		}
	}
	select {
	case m := <-peers["a"].Recv():
		t.Fatalf("sender must not receive own message: %+v", m)
	default:
	}
}

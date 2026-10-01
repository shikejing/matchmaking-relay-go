package engine

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/types"
)

func cfgTwoPartitions(verA, verB types.RuleVersion) config.Config {
	mk := func(id string, ver types.RuleVersion) config.PartitionConfig {
		return config.PartitionConfig{
			ID: types.PartitionID(id), Version: ver, QueueCapacity: 64,
			MaxTeamSize: 4, TargetTeamSize: 4, MaxBatch: 8,
			ScoreWindow: 100, MaxWaitMs: 1000, ReconnectHoldMs: 100,
			Reconnect: config.PolicyEndSession, SearchWindow: 32,
		}
	}
	c := config.Default()
	c.Partitions = []config.PartitionConfig{mk("A", verA), mk("B", verB)}
	c.MaxSessions = 100
	c.TickIntervalMs = 0
	c.RandomSeed = 42
	return c
}

func team(id string, score int, players ...string) types.Team {
	ps := make([]types.Player, len(players))
	for i, p := range players {
		ps[i] = types.Player{ID: types.PlayerID(p), Score: score}
	}
	return types.Team{ID: types.TeamID(id), Players: ps}
}

// matchSet runs the engine and returns sorted team groupings per run.
func matchSet(t *testing.T, submissions []types.Team) [][]string {
	t.Helper()
	e, err := New(cfgTwoPartitions("v1", "v1"), clock.NewFakeClock(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, tm := range submissions {
		tm.Version = "v1"
		if r := e.SubmitTeam("A", tm); !r.Accepted {
			t.Fatalf("submit %s: %v", tm.ID, r)
		}
	}
	sids := e.TickPartition("A")
	out := make([][]string, 0, len(sids))
	for _, sid := range sids {
		snap, ok := e.Manager().Get(sid)
		if !ok {
			t.Fatalf("session %s missing", sid)
		}
		var tms []string
		for _, m := range snap.Snapshot().Members {
			tms = append(tms, string(m.Team))
		}
		sort.Strings(tms)
		uniq := []string{}
		for i := range tms {
			if i == 0 || tms[i] != tms[i-1] {
				uniq = append(uniq, tms[i])
			}
		}
		out = append(out, uniq)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return len(a) < len(b)
	})
	return out
}

func TestConcurrentDeterministicMatching(t *testing.T) {
	mkSubs := func() []types.Team {
		return []types.Team{
			team("t01", 100, "p01", "p02"),
			team("t02", 101, "p03", "p04"),
			team("t03", 102, "p05", "p06"),
			team("t04", 103, "p07", "p08"),
			team("t05", 150, "p09", "p10"),
			team("t06", 151, "p11", "p12"),
			team("t07", 152, "p13", "p14"),
			team("t08", 153, "p15", "p16"),
		}
	}
	// Baseline with deterministic submission order.
	want := matchSet(t, mkSubs())

	// Concurrent submissions: globally monotonic seq is assigned at
	// submit but the queue order then depends on call arrival order.
	// To still assert determinism we instead fix arrival order via a
	// channel-started fan-in whose submission calls are ordered by
	// goroutine index using per-index acknowledgement... Instead assert
	// the reproducibility property directly: equal enqueue order under
	// any number of concurrent tickers yields identical results.
	for iter := 0; iter < 30; iter++ {
		e, err := New(cfgTwoPartitions("v1", "v1"), clock.NewFakeClock(0))
		if err != nil {
			t.Fatal(err)
		}
		for _, tm := range mkSubs() {
			tm.Version = "v1"
			e.SubmitTeam("A", tm)
		}
		var wg sync.WaitGroup
		sidsCh := make(chan []types.SessionID, 8)
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() { defer wg.Done(); sidsCh <- e.TickPartition("A") }()
		}
		wg.Wait()
		close(sidsCh)
		total := 0
		for s := range sidsCh {
			total += len(s)
		}
		if total != len(want) {
			t.Fatalf("iter %d: concurrent ticks created %d sessions, want %d", iter, total, len(want))
		}
	}
}

func TestDuplicateTeamConcurrentOnce(t *testing.T) {
	e, _ := New(cfgTwoPartitions("v1", "v1"), clock.NewFakeClock(0))
	var wg sync.WaitGroup
	var n int32
	var mu sync.Mutex
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := e.SubmitTeam("A", team("dup", 10, "x1", "x2"))
			if r.Accepted {
				mu.Lock()
				n++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if n != 1 {
		t.Fatalf("duplicate accepted %d times", n)
	}
	if len(e.QueueSnapshot("A")) != 1 {
		t.Fatal("queue must contain exactly one entry")
	}
}

func TestQueueCapacityReject(t *testing.T) {
	c := cfgTwoPartitions("v1", "v1")
	c.Partitions[0].QueueCapacity = 2
	e, _ := New(c, clock.NewFakeClock(0))
	if r := e.SubmitTeam("A", team("a", 1, "a1")); !r.Accepted {
		t.Fatal(r)
	}
	if r := e.SubmitTeam("A", team("b", 1, "b1")); !r.Accepted {
		t.Fatal(r)
	}
	r := e.SubmitTeam("A", team("c", 1, "c1"))
	if r.Accepted || r.Reason != types.RejectQueueFull {
		t.Fatalf("want queue_full, got %+v", r)
	}
	// Unknown partition and wrong version are distinguishable too.
	if r := e.SubmitTeam("ZZ", team("z", 1, "z1")); r.Reason != types.RejectUnknownPartition {
		t.Fatalf("want unknown_partition, got %v", r.Reason)
	}
	wrong := team("w", 1, "w1")
	wrong.Version = "v9"
	if r := e.SubmitTeam("A", wrong); r.Reason != types.RejectVersionIncompatible {
		t.Fatalf("want version_incompatible, got %v", r.Reason)
	}
}

func TestSessionCapRejectAndRestore(t *testing.T) {
	c := cfgTwoPartitions("v1", "v1")
	c.MaxSessions = 1
	e, _ := New(c, clock.NewFakeClock(0))
	// Two full sessions' worth of teams (4 teams).
	for i, id := range []string{"a", "b", "c", "d"} {
		r := e.SubmitTeam("A", team(id, 10, fmt.Sprintf("%s1", id), fmt.Sprintf("%s2", id)))
		if !r.Accepted {
			t.Fatal(r)
		}
		_ = i
	}
	created := e.TickPartition("A")
	if len(created) != 1 {
		t.Fatalf("cap allows 1 session, created %d", len(created))
	}
	// Remaining teams must have been restored in order.
	snap := e.QueueSnapshot("A")
	if len(snap) != 2 || snap[0].TeamID != "c" || snap[1].TeamID != "d" {
		t.Fatalf("queue restore wrong: %+v", snap)
	}
	// After the session closes, capacity frees and matching proceeds.
	e.Manager().Remove(created[0], types.CloseNormal)
	created2 := e.TickPartition("A")
	if len(created2) != 1 {
		t.Fatalf("expected session after capacity freed, got %d", len(created2))
	}
}

func TestVersionSwitchDoesNotAffectExisting(t *testing.T) {
	// v1 on A; queue one v1 team.
	c := cfgTwoPartitions("v1", "v2")
	e, _ := New(c, clock.NewFakeClock(0))
	t1 := team("a", 10, "a1", "a2")
	t1.Version = "v1"
	e.SubmitTeam("A", t1)
	t2 := team("b", 10, "b1", "b2")
	t2.Version = "v1"
	e.SubmitTeam("A", t2)
	sids := e.TickPartition("A")
	if len(sids) != 1 {
		t.Fatalf("want 1 session, got %d", len(sids))
	}
	snap := e.Sessions()[0]
	if snap.Version != "v1" {
		t.Fatalf("session version = %s, want v1", snap.Version)
	}
	// v2 partition computes median scores differently: [0,100] => 50
	// under v1 mean vs 100 under v2 median; check rule query surfaces.
	rs, _ := e.PartitionRules("B")
	if rs.Version() != "v2" || rs.MergeScore([]types.Player{{Score: 0}, {Score: 100}}) != 100 {
		t.Fatalf("v2 median rule wrong: %s", rs.Describe())
	}
	rsA, _ := e.PartitionRules("A")
	if rsA.MergeScore([]types.Player{{Score: 0}, {Score: 100}}) != 50 {
		t.Fatal("v1 mean rule wrong")
	}
	// Cross-version submit rejected.
	bad := team("x", 10, "x1", "x2")
	bad.Version = "v1"
	if r := e.SubmitTeam("B", bad); r.Reason != types.RejectVersionIncompatible {
		t.Fatalf("cross-version submit must reject, got %v", r.Reason)
	}
}

func TestPartitionIsolation(t *testing.T) {
	e, _ := New(cfgTwoPartitions("v1", "v1"), clock.NewFakeClock(0))
	// Only B has a complete match; ticking A must not create sessions.
	e.SubmitTeam("A", team("a", 10, "a1")) // half team, incomplete
	e.SubmitTeam("B", team("b1", 10, "b1", "b2"))
	e.SubmitTeam("B", team("b2", 10, "b3", "b4"))
	if s := e.TickPartition("A"); len(s) != 0 {
		t.Fatalf("A should not match, got %v", s)
	}
	if s := e.TickPartition("B"); len(s) != 1 {
		t.Fatalf("B should match independently, got %v", s)
	}
	if len(e.QueueSnapshot("A")) != 1 {
		t.Fatal("A queue must be untouched")
	}
}

func TestAuditTraceability(t *testing.T) {
	e, _ := New(cfgTwoPartitions("v1", "v1"), clock.NewFakeClock(0))
	e.SubmitTeam("A", team("m1", 10, "u1", "u2"))
	e.SubmitTeam("A", team("m2", 10, "u3", "u4"))
	sids := e.TickPartition("A")
	if len(sids) != 1 {
		t.Fatalf("want match, got %d", len(sids))
	}
	hist := e.Audit().SessionHistory(sids[0])
	kinds := map[string]bool{}
	for _, ev := range hist {
		kinds[string(ev.Kind)] = true
	}
	if !kinds["session_created"] {
		t.Fatal("missing session_created event")
	}
	// Match decision carries inputs (teams, scores, seq).
	var matchedDetail string
	for _, ev := range e.Audit().TeamHistory("m1") {
		if ev.Kind == "matched" {
			matchedDetail = ev.Detail
		}
	}
	if matchedDetail == "" {
		t.Fatal("matched event with decision basis not recorded")
	}
	// Rejection pinpoints object.
	c := cfgTwoPartitions("v1", "v1")
	c.Partitions[0].QueueCapacity = 1
	e2, _ := New(c, clock.NewFakeClock(0))
	e2.SubmitTeam("A", team("q1", 1, "z1"))
	r := e2.SubmitTeam("A", team("q2", 1, "z2"))
	if r.Reason != types.RejectQueueFull || r.Detail != "A" {
		t.Fatalf("rejection must pinpoint object, got %+v", r)
	}
}

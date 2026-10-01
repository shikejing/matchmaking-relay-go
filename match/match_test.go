package match

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/queue"
	"github.com/shikejing/matchmaking-relay-go/rules"
	"github.com/shikejing/matchmaking-relay-go/types"
)

func pcFor() config.PartitionConfig {
	return config.PartitionConfig{
		ID: "p", Version: "v1", QueueCapacity: 100, MaxTeamSize: 10,
		TargetTeamSize: 4, MaxBatch: 10, ScoreWindow: 100,
		MaxWaitMs: 1000, SearchWindow: 64,
	}
}

func entry(seq uint64, t0 int64, id string, size, score int) queue.Entry {
	players := make([]types.Player, size)
	for i := range players {
		players[i] = types.Player{ID: types.PlayerID(id + "-p"), Score: score}
	}
	// give distinct player ids
	for i := range players {
		players[i].ID = types.PlayerID(id + "-p" + string(rune('a'+i)))
	}
	return queue.Entry{
		Team:       types.Team{ID: types.TeamID(id), Players: players, Version: "v1"},
		Score:      score,
		EnqueuedAt: t0,
		Sequence:   seq,
	}
}

func idsOf(groups []Group) [][]string {
	out := make([][]string, len(groups))
	for i, g := range groups {
		for _, e := range g.Teams {
			out[i] = append(out[i], string(e.Team.ID))
		}
	}
	return out
}

// Two teams of two should form exactly one session when scores are close.
func TestDecideBasic(t *testing.T) {
	pc := pcFor()
	rs := rules.NewV1(pc.TargetTeamSize, pc.ScoreWindow, pc.MaxWaitMs)
	entries := []queue.Entry{
		entry(1, 0, "t1", 2, 100),
		entry(2, 1, "t2", 2, 110),
	}
	g := Decide(entries, pc, rs, 10)
	if len(g) != 1 {
		t.Fatalf("want 1 group, got %d", len(g))
	}
	got := idsOf(g)[0]
	if got[0] != "t1" || got[1] != "t2" {
		t.Fatalf("unexpected teams: %v", got)
	}
}

// Score barrier: distant scores must not match before MaxWaitMs.
func TestDecideScoreBarrier(t *testing.T) {
	pc := pcFor()
	rs := rules.NewV1(pc.TargetTeamSize, pc.ScoreWindow, pc.MaxWaitMs)
	entries := []queue.Entry{
		entry(1, 0, "t1", 2, 100),
		entry(2, 0, "t2", 2, 500),
	}
	if g := Decide(entries, pc, rs, 500); len(g) != 0 {
		t.Fatalf("want no match behind barrier, got %v", idsOf(g))
	}
	// After MaxWaitMs the soft barrier relaxes and they match.
	if g := Decide(entries, pc, rs, 1000); len(g) != 1 {
		t.Fatalf("want match after patience, got %v", idsOf(g))
	}
}

// Determinism: shuffling identical inputs into the same sequence order
// must always return identical group sets; run many times to catch
// map-order dependence.
func TestDecideDeterministic(t *testing.T) {
	pc := pcFor()
	rs := rules.NewV1(pc.TargetTeamSize, pc.ScoreWindow, pc.MaxWaitMs)
	mk := func() []queue.Entry {
		es := []queue.Entry{
			entry(1, 0, "a", 2, 100),
			entry(2, 0, "b", 2, 120),
			entry(3, 5, "c", 2, 140),
			entry(4, 5, "d", 2, 160),
			entry(5, 10, "e", 1, 100),
			entry(6, 10, "f", 3, 100),
		}
		return es
	}
	want := idsOf(Decide(mk(), pc, rs, 50))
	for i := 0; i < 200; i++ {
		got := idsOf(Decide(mk(), pc, rs, 50))
		if len(got) != len(want) {
			t.Fatalf("iter %d: group count %d != %d", i, len(got), len(want))
		}
		for j := range want {
			if len(got[j]) != len(want[j]) {
				t.Fatalf("iter %d group %d: %v != %v", i, j, got[j], want[j])
			}
			for k := range want[j] {
				if got[j][k] != want[j][k] {
					t.Fatalf("iter %d nondeterministic: %v != %v", i, got, want)
				}
			}
		}
	}
}

// Patience wins over proximity: the oldest candidate matches first even
// if a younger, closer pairing exists.
func TestDecidePatiencePriority(t *testing.T) {
	pc := pcFor()
	pc.ScoreWindow = 1000 // everything feasible
	rs := rules.NewV1(pc.TargetTeamSize, pc.ScoreWindow, pc.MaxWaitMs)
	entries := []queue.Entry{
		entry(1, 0, "old1", 2, 100), // age at tick: 900
		entry(2, 800, "mid1", 2, 100),
		entry(3, 800, "mid2", 2, 101), // closer to mid1
		entry(4, 850, "young", 2, 100),
	}
	g := Decide(entries, pc, rs, 900)
	if len(g) == 0 {
		t.Fatal("expected at least one group")
	}
	first := idsOf(g)[0]
	set := map[string]bool{}
	for _, x := range first {
		set[x] = true
	}
	if !set["old1"] || !set["mid1"] {
		t.Fatalf("oldest candidate must rank first, got %v", first)
	}
}

package rules

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/types"
)

func players(scores ...int) []types.Player {
	out := make([]types.Player, len(scores))
	for i, s := range scores {
		out[i] = types.Player{ID: types.PlayerID(string(rune('a' + i))), Score: s}
	}
	return out
}

func TestV1Mean(t *testing.T) {
	v := NewV1(4, 10, 100)
	if got := v.MergeScore(players(10, 20, 30, 40)); got != 25 {
		t.Fatalf("mean = %d, want 25", got)
	}
	if got := v.MergeScore(players(1, 2)); got != 2 {
		t.Fatalf("rounded mean = %d, want 2", got)
	}
}

func TestV2Median(t *testing.T) {
	v := NewV2(4, 10, 100)
	if got := v.MergeScore(players(40, 10, 30, 20)); got != 30 {
		t.Fatalf("upper-middle median = %d, want 30", got)
	}
	if got := v.MergeScore(players(5, 1, 3)); got != 3 {
		t.Fatalf("median = %d, want 3", got)
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(NewV1(2, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(NewV1(2, 0, 0)); err == nil {
		t.Fatal("duplicate version must error")
	}
	if err := r.Register(NewV2(2, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if vs := r.Versions(); len(vs) != 2 || vs[0] != "v1" || vs[1] != "v2" {
		t.Fatalf("versions order wrong: %v", vs)
	}
	if _, ok := r.Get("v9"); ok {
		t.Fatal("missing version must return false")
	}
}

package rule

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

func TestMergeScore(t *testing.T) {
	members := []model.Player{
		{ID: "a", Score: 100},
		{ID: "b", Score: 200},
	}
	v1 := NewV1()
	if got := v1.MergeScore(members); got != 300 {
		t.Fatalf("v1 merge = %d, want 300 (sum)", got)
	}
	v2 := NewV2()
	if got := v2.MergeScore(members); got != 150 {
		t.Fatalf("v2 merge = %d, want 150 (avg)", got)
	}
}

func TestV2EffectiveScoreWaitWeight(t *testing.T) {
	v2 := NewV2()
	team := &model.Team{Score: 100, EnqueueTick: 0}
	if got := v2.EffectiveScore(team, 10); got != 120 {
		t.Fatalf("v2 effective at tick 10 = %d, want 120", got)
	}
	v1 := NewV1()
	if got := v1.EffectiveScore(team, 10); got != 100 {
		t.Fatalf("v1 effective = %d, want 100 (no wait bonus)", got)
	}
}

func TestRegistry(t *testing.T) {
	regs, err := NewRegistry(DefaultVersion)
	if err != nil {
		t.Fatal(err)
	}
	if regs.DefaultVersion() != 1 {
		t.Fatalf("default = %d, want 1", regs.DefaultVersion())
	}
	if err := regs.SetDefault(2); err != nil {
		t.Fatal(err)
	}
	if regs.DefaultVersion() != 2 {
		t.Fatalf("default after switch = %d, want 2", regs.DefaultVersion())
	}
	if err := regs.SetDefault(99); err == nil {
		t.Fatal("switch to unregistered version should fail")
	}
	versions := regs.Versions()
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("versions = %v, want [1 2]", versions)
	}
	r, err := regs.Get(2)
	if err != nil || r.Version() != 2 {
		t.Fatalf("get v2: %v", err)
	}
	if _, err := regs.Get(99); err == nil {
		t.Fatal("get unknown version should fail")
	}
	if err := regs.Register(NewV1()); err == nil {
		t.Fatal("duplicate registration should fail")
	}
}

func TestRuleDescriptions(t *testing.T) {
	regs, err := NewRegistry(DefaultVersion)
	if err != nil {
		t.Fatal(err)
	}
	descs := regs.DescribeAll()
	if len(descs) != 2 {
		t.Fatalf("descriptions = %v", descs)
	}
	for _, d := range descs {
		if d == "" {
			t.Fatal("empty rule description")
		}
	}
}

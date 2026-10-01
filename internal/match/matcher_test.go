package match

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
)

func mkTeam(id string, score, tick, seq int64, members int) *model.Team {
	players := make([]model.Player, members)
	for i := range players {
		players[i] = model.Player{ID: model.PlayerID(id + "-p" + itoa(i)), Score: int(score)}
	}
	return &model.Team{
		ID:          model.TeamID(id),
		Partition:   "p",
		Members:     players,
		Score:       int(score),
		RuleVersion: rule.DefaultVersion,
		EnqueueTick: tick,
		EnqueueSeq:  seq,
		Status:      model.TeamWaiting,
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

func teamIDsOf(matches []PlannedMatch) [][]model.TeamID {
	out := make([][]model.TeamID, len(matches))
	for i, m := range matches {
		ids := make([]model.TeamID, len(m.Teams))
		for j, t := range m.Teams {
			ids[j] = t.ID
		}
		out[i] = ids
	}
	return out
}

func TestPlanMatchesWaitTimePriority(t *testing.T) {
	// Teams enqueued at different ticks: the oldest must be the anchor and
	// matches must be formed oldest-first.
	teams := []*model.Team{
		mkTeam("new", 100, 10, 3, 1),
		mkTeam("old", 100, 1, 0, 1),
		mkTeam("mid", 100, 5, 1, 1),
		mkTeam("old2", 100, 2, 2, 1),
	}
	regs := newRegistry(t)
	matches := PlanMatches(teams, 10, regs, 2, 10)
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}
	got := teamIDsOf(matches)
	// Match 1: anchor old (tick 1) pairs with closest-score remaining.
	// All scores equal -> tie broken by canonical order: old2 (tick 2).
	if got[0][0] != "old" || got[0][1] != "old2" {
		t.Fatalf("first match = %v, want [old old2]", got[0])
	}
	if got[1][0] != "mid" || got[1][1] != "new" {
		t.Fatalf("second match = %v, want [mid new]", got[1])
	}
}

func TestPlanMatchesScoreProximity(t *testing.T) {
	// Anchor score 100; partners 90 (diff 10) and 150 (diff 50).
	// The 90-score team must be chosen even though it was enqueued later.
	teams := []*model.Team{
		mkTeam("anchor", 100, 1, 0, 1),
		mkTeam("far", 150, 1, 1, 1),
		mkTeam("close", 90, 2, 2, 1),
	}
	regs := newRegistry(t)
	matches := PlanMatches(teams, 3, regs, 2, 10)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	got := teamIDsOf(matches)
	if got[0][0] != "anchor" || got[0][1] != "close" {
		t.Fatalf("match = %v, want [anchor close]", got[0])
	}
	if matches[0].ScoreDiff != 10 {
		t.Fatalf("scoreDiff = %d, want 10", matches[0].ScoreDiff)
	}
}

func TestPlanMatchesTieBrokenBySeq(t *testing.T) {
	// Equal scores: the anchor is the earliest team; among equal-score
	// partners the earlier enqueue seq wins the tie.
	teams := []*model.Team{
		mkTeam("anchor", 100, 0, 5, 1),
		mkTeam("b", 100, 1, 3, 1),
		mkTeam("a", 100, 1, 2, 1),
	}
	regs := newRegistry(t)
	matches := PlanMatches(teams, 2, regs, 2, 10)
	got := teamIDsOf(matches)
	if got[0][0] != "anchor" {
		t.Fatalf("anchor = %s, want anchor (earliest enqueue)", got[0][0])
	}
	if got[0][1] != "a" {
		t.Fatalf("partner tie broken with %s, want a (lowest seq)", got[0][1])
	}
}

func TestPlanMatchesBatchLimit(t *testing.T) {
	teams := []*model.Team{
		mkTeam("a", 100, 1, 0, 1),
		mkTeam("b", 100, 1, 1, 1),
		mkTeam("c", 100, 1, 2, 1),
		mkTeam("d", 100, 1, 3, 1),
	}
	regs := newRegistry(t)
	matches := PlanMatches(teams, 1, regs, 2, 1)
	if len(matches) != 1 {
		t.Fatalf("batch limit: got %d matches, want 1", len(matches))
	}
}

func TestPlanMatchesDeterminism(t *testing.T) {
	// Shuffle the input slice; the result must be identical because the
	// planner sorts canonically and never iterates maps.
	teams := []*model.Team{
		mkTeam("a", 100, 1, 0, 1),
		mkTeam("b", 120, 1, 1, 1),
		mkTeam("c", 90, 2, 2, 1),
		mkTeam("d", 110, 2, 3, 1),
		mkTeam("e", 105, 3, 4, 1),
		mkTeam("f", 95, 3, 5, 1),
	}
	regs := newRegistry(t)
	want := teamIDsOf(PlanMatches(teams, 4, regs, 2, 10))
	for _, perm := range permutations(teams) {
		got := teamIDsOf(PlanMatches(perm, 4, regs, 2, 10))
		if !equalMatches(want, got) {
			t.Fatalf("non-deterministic result: %v vs %v", want, got)
		}
	}
}

func TestPlanMatchesV2WaitWeight(t *testing.T) {
	// Under v2, effective score = score + wait*2. A team with a slightly
	// higher raw score but much longer wait becomes the anchor and pairs
	// differently.
	regs := newRegistry(t)
	// anchor enqueued at tick 0 with score 100; partner enqueued at tick 9
	// with score 110. At tick 10: anchor eff = 100+10*2=120; partner eff =
	// 110+1*2=112. The anchor pairs with the only partner anyway; the point
	// is wait weight changes which team is closest.
	teams := []*model.Team{
		mkTeam("anchor", 100, 0, 0, 1),
		mkTeam("p1", 110, 9, 1, 1),
		mkTeam("p2", 200, 9, 2, 1),
	}
	// Force v2 on all teams.
	for _, tm := range teams {
		tm.RuleVersion = 2
	}
	matches := PlanMatches(teams, 10, regs, 2, 10)
	got := teamIDsOf(matches)
	// anchor eff=120; p1 eff=112 (diff 8); p2 eff=202 (diff 82) -> p1.
	if got[0][1] != "p1" {
		t.Fatalf("v2 pairing = %v, want p1", got[0])
	}
}

func newRegistry(t *testing.T) *rule.Registry {
	t.Helper()
	regs, err := rule.NewRegistry(rule.DefaultVersion)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return regs
}

func permutations(teams []*model.Team) [][]*model.Team {
	var out [][]*model.Team
	var gen func(start int)
	gen = func(start int) {
		if start == len(teams) {
			cp := make([]*model.Team, len(teams))
			copy(cp, teams)
			out = append(out, cp)
			return
		}
		for i := start; i < len(teams); i++ {
			teams[start], teams[i] = teams[i], teams[start]
			gen(start + 1)
			teams[start], teams[i] = teams[i], teams[start]
		}
	}
	gen(0)
	return out
}

func equalMatches(a, b [][]model.TeamID) bool {
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

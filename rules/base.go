package rules

import (
	"math"
	"sort"

	"github.com/shikejing/matchmaking-relay-go/types"
)

// baseRules holds fields shared by versioned rulesets.
type baseRules struct {
	version     types.RuleVersion
	targetSize  int
	scoreWindow int
	maxWaitMs   int64
	describe    string
}

func (b *baseRules) Version() types.RuleVersion { return b.version }
func (b *baseRules) Describe() string           { return b.describe }

// lookup indexes inputs by team id via a linear scan over the
// sequence-ordered input slice (never a map iteration).
func lookup(in MatchInputs, id types.TeamID) (types.QueuedTeam, bool) {
	for _, t := range in.Teams {
		if t.TeamID == id {
			return t, true
		}
	}
	return types.QueuedTeam{}, false
}

// candidateStats computes (oldest age, score spread, validity).
func (b *baseRules) candidateStats(in MatchInputs, c Candidate) (wait int64, spread int, ok bool) {
	if len(c.Teams) == 0 {
		return 0, 0, false
	}
	var totalSize int
	minScore, maxScore := math.MaxInt, math.MinInt
	for _, id := range c.Teams {
		t, found := lookup(in, id)
		if !found {
			return 0, 0, false
		}
		totalSize += t.Size
		age := in.NowMs - t.EnqueuedAt
		if age > wait {
			wait = age
		}
		if t.Score < minScore {
			minScore = t.Score
		}
		if t.Score > maxScore {
			maxScore = t.Score
		}
	}
	if totalSize != b.targetSize {
		return wait, maxScore - minScore, false
	}
	return wait, maxScore - minScore, true
}

// Feasible: exact target size plus the soft score barrier.
func (b *baseRules) Feasible(in MatchInputs, c Candidate) bool {
	wait, spread, ok := b.candidateStats(in, c)
	if !ok {
		return false
	}
	if wait >= b.maxWaitMs {
		return true // barrier relaxed for patient candidates
	}
	return spread <= b.scoreWindow
}

func (b *baseRules) rank(in MatchInputs, candidates []Candidate) []ScoredCandidate {
	out := make([]ScoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		wait, spread, ok := b.candidateStats(in, c)
		if !ok || !b.Feasible(in, c) {
			continue
		}
		out = append(out, ScoredCandidate{Candidate: c, WaitMs: wait, ScoreSpread: spread})
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, z := out[i], out[j]
		if a.WaitMs != z.WaitMs {
			return a.WaitMs > z.WaitMs // oldest first
		}
		if a.ScoreSpread != z.ScoreSpread {
			return a.ScoreSpread < z.ScoreSpread // closest first
		}
		return tupleLess(a.Candidate.Teams, z.Candidate.Teams)
	})
	return out
}

// tupleLess compares two team-id tuples lexicographically after sorting
// each tuple, so the tie-break does not depend on enumeration order.
func tupleLess(a, z []types.TeamID) bool {
	as := append([]types.TeamID(nil), a...)
	zs := append([]types.TeamID(nil), z...)
	sort.Slice(as, func(i, j int) bool { return as[i] < as[j] })
	sort.Slice(zs, func(i, j int) bool { return zs[i] < zs[j] })
	for k := 0; k < len(as) && k < len(zs); k++ {
		if as[k] != zs[k] {
			return as[k] < zs[k]
		}
	}
	return len(as) < len(zs)
}

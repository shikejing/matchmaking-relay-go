package rules

import (
	"sort"

	"github.com/shikejing/matchmaking-relay-go/types"
)

// V1 is the first ruleset version.
//
// Score merge: the team score is the rounded arithmetic mean of member
// scores (sum / n, half rounded to nearest).
// Matching: exact target size, score window soft barrier, ranking by
// oldest-wait then score spread then lexicographic team tuple.
type V1 struct {
	baseRules
}

// NewV1 builds the version 1 ruleset from partition limits.
func NewV1(targetSize, scoreWindow int, maxWaitMs int64) *V1 {
	return &V1{baseRules: baseRules{
		version:     "v1",
		targetSize:  targetSize,
		scoreWindow: scoreWindow,
		maxWaitMs:   maxWaitMs,
		describe: "v1: team score = rounded mean of member scores; " +
			"match exact target size; rank by oldest wait, then score " +
			"spread, then team-id tuple; score window relaxed after MaxWaitMs",
	}}
}

// MergeScore returns the rounded mean of member scores.
func (v *V1) MergeScore(players []types.Player) int {
	if len(players) == 0 {
		return 0
	}
	sum := 0
	for _, p := range players {
		sum += p.Score
	}
	// round half away from zero; scores are non-negative by convention.
	return (sum + len(players)/2) / len(players)
}

// Rank applies the v1 deterministic ordering.
func (v *V1) Rank(in MatchInputs, candidates []Candidate) []ScoredCandidate {
	return v.rank(in, candidates)
}

// V2 is the second ruleset version.
//
// Score merge: the team score is the median of member scores (upper
// middle for even counts). Matching ordering is unchanged from v1 so
// that behavior differences are limited to the documented score merge.
type V2 struct {
	baseRules
}

// NewV2 builds the version 2 ruleset from partition limits.
func NewV2(targetSize, scoreWindow int, maxWaitMs int64) *V2 {
	return &V2{baseRules: baseRules{
		version:     "v2",
		targetSize:  targetSize,
		scoreWindow: scoreWindow,
		maxWaitMs:   maxWaitMs,
		describe: "v2: team score = median of member scores (upper middle); " +
			"matching order identical to v1",
	}}
}

// MergeScore returns the median member score.
func (v *V2) MergeScore(players []types.Player) int {
	n := len(players)
	if n == 0 {
		return 0
	}
	s := make([]int, n)
	for i, p := range players {
		s[i] = p.Score
	}
	sort.Ints(s)
	return s[n/2] // upper middle for even n, middle for odd n
}

// Rank applies the same deterministic ordering as v1.
func (v *V2) Rank(in MatchInputs, candidates []Candidate) []ScoredCandidate {
	return v.rank(in, candidates)
}

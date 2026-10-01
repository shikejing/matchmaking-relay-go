// Package rules defines versioned matchmaking/relay rule sets.
//
// A team or session always keeps the ruleset version it entered with:
// rule upgrades never alter already produced matches or live sessions.
package rules

import (
	"github.com/shikejing/matchmaking-relay-go/types"
)

// MatchInputs is the immutable decision input recorded for audit.
type MatchInputs struct {
	Partition types.PartitionID
	NowMs     int64
	Teams     []types.QueuedTeam
}

// Candidate is a proposed set of teams forming one session.
type Candidate struct {
	Teams []types.TeamID
}

// ScoredCandidate attaches the ranking values to a Candidate.
type ScoredCandidate struct {
	Candidate Candidate
	// WaitMs is the age of the oldest team in the candidate
	// (max enqueue age): the patience key.
	WaitMs int64
	// ScoreSpread is max team score - min team score (proximity key).
	ScoreSpread int
}

// Rules is one immutable version of the matchmaking rules.
//
// Ranking contract (see docs/matchmaking.md):
//  1. candidates whose oldest team has waited longest rank first
//     (patience prevents starvation);
//  2. on equal patience, smaller team-score spread ranks first
//     (score proximity);
//  3. final ties break on the lexicographically ordered tuple of team
//     ids — never on Go map/set iteration;
//  4. the score window is a soft barrier: it is ignored for a candidate
//     once that candidate's oldest team reaches MaxWaitMs.
//
// Consequently a fixed input order yields bit-identical output on every
// run; rule versions hold no runtime randomness (the configured seed is
// reserved for future, still deterministic, tie-breaks).
type Rules interface {
	Version() types.RuleVersion
	// MergeScore combines member scores into one team score.
	MergeScore(players []types.Player) int
	// Rank orders feasible candidates from best to worst deterministically.
	Rank(in MatchInputs, candidates []Candidate) []ScoredCandidate
	// Feasible reports whether the candidate is allowed at now.
	Feasible(in MatchInputs, c Candidate) bool
	// Describe returns human-readable rule semantics for version queries.
	Describe() string
}

package match

import (
	"sort"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/rule"
)

// PlannedMatch is one deterministic match decision.
type PlannedMatch struct {
	// Teams in the match, anchor first (the longest-waiting team).
	Teams []*model.Team
	// RuleVersion governs the match: it is the anchor team's enqueue-time rule.
	RuleVersion int
	// WaitTicks[i] is how long Teams[i] had waited at decision time.
	WaitTicks []int64
	// ScoreDiff is the absolute effective-score difference of the pairing.
	ScoreDiff int
}

// sortTeams establishes the canonical deterministic order: earlier enqueue
// tick first, then the atomic enqueue sequence. No map iteration, no randomness.
func sortTeams(teams []*model.Team) {
	sort.SliceStable(teams, func(i, j int) bool {
		if teams[i].EnqueueTick != teams[j].EnqueueTick {
			return teams[i].EnqueueTick < teams[j].EnqueueTick
		}
		return teams[i].EnqueueSeq < teams[j].EnqueueSeq
	})
}

// PlanMatches deterministically forms matches from a slice of waiting teams.
//
// Decision rule (documented in README):
//  1. Candidates are ordered by (enqueue tick, enqueue seq) — wait time is the
//     first-class priority; the longest-waiting team is the anchor.
//  2. The anchor is paired with the remaining team whose effective score is
//     closest to the anchor's (absolute difference).
//  3. Ties (equal score difference) are broken by the canonical order, i.e.
//     the earlier enqueue sequence.
//  4. At most batchSize matches are planned; teamsPerMatch teams per match.
//
// The match is governed by the anchor team's enqueue-time rule version: both
// teams' effective scores are computed under that rule. The function is pure:
// it never reads maps, clocks, or random sources, so its result is a pure
// function of (teams, tick, rules, limits).
func PlanMatches(teams []*model.Team, tick int64, rules *rule.Registry, teamsPerMatch, batchSize int) []PlannedMatch {
	if len(teams) < teamsPerMatch || batchSize <= 0 {
		return nil
	}
	ordered := make([]*model.Team, len(teams))
	copy(ordered, teams)
	sortTeams(ordered)

	matches := make([]PlannedMatch, 0, batchSize)
	for len(ordered) >= teamsPerMatch && len(matches) < batchSize {
		anchor := ordered[0]
		anchorRule, err := rules.Get(anchor.RuleVersion)
		if err != nil {
			// A stamped version is always registered; skip if not.
			break
		}
		anchorEff := anchorRule.EffectiveScore(anchor, tick)

		bestIdx := 1
		bestDiff := abs(anchorRule.EffectiveScore(ordered[1], tick) - anchorEff)
		for i := 2; i < len(ordered); i++ {
			diff := abs(anchorRule.EffectiveScore(ordered[i], tick) - anchorEff)
			if diff < bestDiff {
				bestDiff = diff
				bestIdx = i
			}
		}

		matchTeams := make([]*model.Team, 0, teamsPerMatch)
		matchTeams = append(matchTeams, anchor, ordered[bestIdx])
		usedIdx := map[int]bool{0: true, bestIdx: true}
		for i := 1; i < len(ordered) && len(matchTeams) < teamsPerMatch; i++ {
			if usedIdx[i] {
				continue
			}
			matchTeams = append(matchTeams, ordered[i])
			usedIdx[i] = true
		}

		waitTicks := make([]int64, len(matchTeams))
		for i, t := range matchTeams {
			waitTicks[i] = tick - t.EnqueueTick
		}
		matches = append(matches, PlannedMatch{
			Teams:       matchTeams,
			RuleVersion: anchor.RuleVersion,
			WaitTicks:   waitTicks,
			ScoreDiff:   bestDiff,
		})

		// Remove anchor and chosen partners, keep the rest in canonical order.
		next := make([]*model.Team, 0, len(ordered)-len(matchTeams))
		for i, t := range ordered {
			if !usedIdx[i] {
				next = append(next, t)
			}
		}
		ordered = next
	}
	return matches
}

// PlannedBackfill is one deterministic backfill decision.
type PlannedBackfill struct {
	Ticket *model.BackfillTicket
	Team   *model.Team
}

// PlanBackfill pairs backfill tickets with waiting teams of the exact slot
// count. Tickets are served in (enqueue tick, enqueue seq) order; for each
// ticket the earliest matching team is chosen. Pure and deterministic.
func PlanBackfill(tickets []*model.BackfillTicket, teams []*model.Team) []PlannedBackfill {
	if len(tickets) == 0 || len(teams) == 0 {
		return nil
	}
	orderedTickets := make([]*model.BackfillTicket, len(tickets))
	copy(orderedTickets, tickets)
	sort.SliceStable(orderedTickets, func(i, j int) bool {
		if orderedTickets[i].EnqueueTick != orderedTickets[j].EnqueueTick {
			return orderedTickets[i].EnqueueTick < orderedTickets[j].EnqueueTick
		}
		return orderedTickets[i].EnqueueSeq < orderedTickets[j].EnqueueSeq
	})

	orderedTeams := make([]*model.Team, len(teams))
	copy(orderedTeams, teams)
	sortTeams(orderedTeams)

	usedTeam := make([]bool, len(orderedTeams))
	out := make([]PlannedBackfill, 0, len(orderedTickets))
	for _, ticket := range orderedTickets {
		for ti, t := range orderedTeams {
			if usedTeam[ti] {
				continue
			}
			if len(t.Members) != ticket.Slots {
				continue
			}
			usedTeam[ti] = true
			out = append(out, PlannedBackfill{Ticket: ticket, Team: t})
			break
		}
	}
	return out
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

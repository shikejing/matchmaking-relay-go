// Package match implements the deterministic matchmaking decision.
//
// Determinism contract (see docs/matchmaking.md):
//
//  1. The input slice is taken strictly in submission-sequence (FIFO)
//     order; no map or set is ever iterated for ordering.
//  2. Anchors are processed in that order. An anchor that can never fit
//     (team larger than target) is skipped.
//  3. For each anchor, completions are enumerated by an index-order DFS
//     over later teams within SearchWindow; enumeration order is fixed
//     and independent of Go runtime randomization. Search effort is
//     bounded (SearchWindow, MaxCandidatesPerAnchor,
//     MaxSearchNodesPerTick) and truncation is deterministic.
//  4. Feasible candidates are ranked by the versioned ruleset:
//     oldest max wait (patience), then score spread (proximity), then
//     lexicographic team-id tuple. The best candidate is chosen, its
//     teams removed, and the process continues with the next free
//     anchor.
//  5. At most MaxBatch groups are produced per tick.
//
// Therefore, with fixed config (incl. RandomSeed) and equal enqueue
// order, every run at every concurrency level yields identical groups.
package match

import (
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/queue"
	"github.com/shikejing/matchmaking-relay-go/rules"
	"github.com/shikejing/matchmaking-relay-go/types"
)

// Group is one decided match: the exact set of teams forming a session.
type Group struct {
	Teams []queue.Entry
}

// enumeration state for one anchor's DFS.
type enumerator struct {
	entries    []queue.Entry
	anchor     int
	end        int // exclusive upper index (search window end)
	target     int
	candidates []rules.Candidate
	maxCands   int
	nodeBudget int
}

func (en *enumerator) dfs(start int, chosen []int, size int) {
	if len(en.candidates) >= en.maxCands || en.nodeBudget <= 0 {
		return
	}
	en.nodeBudget--
	if size == en.target {
		ids := make([]types.TeamID, 0, len(chosen))
		for _, ix := range chosen {
			ids = append(ids, en.entries[ix].Team.ID)
		}
		en.candidates = append(en.candidates, rules.Candidate{Teams: ids})
		return
	}
	for j := start; j < en.end; j++ {
		ns := size + len(en.entries[j].Team.Players)
		if ns > en.target {
			continue // too big now; later teams only get added, so skip
		}
		en.dfs(j+1, append(chosen, j), ns)
		if len(en.candidates) >= en.maxCands || en.nodeBudget <= 0 {
			return
		}
	}
}

// Decide produces zero or more disjoint groups for one tick of one
// partition.
func Decide(entries []queue.Entry, pc config.PartitionConfig, rs rules.Rules, nowMs int64) []Group {
	window := pc.SearchWindow
	if window <= 0 || window > config.HardSearchWindow {
		window = config.HardSearchWindow
	}
	maxBatch := pc.MaxBatch
	if maxBatch <= 0 {
		return nil
	}

	used := make([]bool, len(entries))
	groups := make([]Group, 0)
	nodeBudget := config.MaxSearchNodesPerTick

	for anchor := 0; anchor < len(entries) && len(groups) < maxBatch; anchor++ {
		if used[anchor] {
			continue
		}
		anchorSize := len(entries[anchor].Team.Players)
		if anchorSize > pc.TargetTeamSize {
			continue // can never be completed; leave queued
		}

		end := anchor + 1 + window
		if end > len(entries) {
			end = len(entries)
		}
		en := &enumerator{
			entries:    entries,
			anchor:     anchor,
			end:        end,
			target:     pc.TargetTeamSize,
			maxCands:   config.MaxCandidatesPerAnchor,
			nodeBudget: nodeBudget,
		}
		en.dfs(anchor+1, []int{anchor}, anchorSize)
		nodeBudget = en.nodeBudget
		if len(en.candidates) == 0 {
			continue
		}

		// Rank only candidates consisting entirely of unused teams.
		views := make([]types.QueuedTeam, 0, end-anchor)
		for i := anchor; i < end; i++ {
			views = append(views, entries[i].View(pc.ID))
		}
		in := rules.MatchInputs{Partition: pc.ID, NowMs: nowMs, Teams: views}
		ranked := rs.Rank(in, en.candidates)

		var picked []types.TeamID
		for _, sc := range ranked {
			if allUnused(sc.Candidate.Teams, entries, used) {
				picked = sc.Candidate.Teams
				break
			}
		}
		if picked == nil {
			continue
		}

		byID := map[types.TeamID]queue.Entry{}
		for i := anchor; i < end; i++ {
			byID[entries[i].Team.ID] = entries[i]
		}
		g := Group{Teams: make([]queue.Entry, 0, len(picked))}
		for _, id := range picked {
			e := byID[id]
			g.Teams = append(g.Teams, e)
			for i := anchor; i < end; i++ {
				if entries[i].Team.ID == id {
					used[i] = true
					break
				}
			}
		}
		groups = append(groups, g)
	}
	return groups
}

func allUnused(ids []types.TeamID, entries []queue.Entry, used []bool) bool {
	for _, id := range ids {
		found := false
		for i := range entries {
			if entries[i].Team.ID == id {
				if used[i] {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

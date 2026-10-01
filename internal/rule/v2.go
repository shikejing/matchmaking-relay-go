package rule

import (
	"fmt"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// V2 is the updated rule version.
//   - Team score: average of member scores (rounded down).
//   - Pairing: effective score = average + waitTicks*2, so long-waiting teams
//     are prioritized; score proximity still decides among similarly aged teams.
//   - Disconnect retention: 200 ticks.
//   - Timeout policy: backfill the slot from the queue; if no replacement is
//     found within 50 ticks, end the session.
type V2 struct{}

// NewV2 returns a V2 rule.
func NewV2() *V2 { return &V2{} }

func (v *V2) Version() int { return 2 }

func (v *V2) Name() string { return "v2-wait-weighted-backfill" }

func (v *V2) MergeScore(members []model.Player) int {
	if len(members) == 0 {
		return 0
	}
	total := 0
	for _, m := range members {
		total += m.Score
	}
	return total / len(members)
}

func (v *V2) EffectiveScore(team *model.Team, tick int64) int {
	waited := tick - team.EnqueueTick
	if waited < 0 {
		waited = 0
	}
	return team.Score + int(waited)*2
}

func (v *V2) RetentionTicks() int { return 200 }

func (v *V2) BackfillEnabled() bool { return true }

func (v *V2) BackfillTimeoutTicks() int { return 50 }

func (v *V2) Describe() string {
	return fmt.Sprintf("rule v2 (%s): merge=avg, pairing=wait-weighted(w*2)+closest, retention=%dt, backfill=on(timeout=%dt)",
		v.Name(), v.RetentionTicks(), v.BackfillTimeoutTicks())
}

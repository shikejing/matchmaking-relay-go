package rule

import (
	"fmt"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// V1 is the original rule version.
//   - Team score: sum of member scores.
//   - Pairing: closest merged score, no wait-time bonus.
//   - Disconnect retention: 100 ticks.
//   - Timeout policy: end the session (no backfill).
type V1 struct{}

// NewV1 returns a V1 rule.
func NewV1() *V1 { return &V1{} }

func (v *V1) Version() int { return 1 }

func (v *V1) Name() string { return "v1-original" }

func (v *V1) MergeScore(members []model.Player) int {
	total := 0
	for _, m := range members {
		total += m.Score
	}
	return total
}

func (v *V1) EffectiveScore(team *model.Team, tick int64) int {
	return team.Score
}

func (v *V1) RetentionTicks() int { return 100 }

func (v *V1) BackfillEnabled() bool { return false }

func (v *V1) BackfillTimeoutTicks() int { return 0 }

func (v *V1) Describe() string {
	return fmt.Sprintf("rule v1 (%s): merge=sum, pairing=closest-score, retention=%dt, backfill=off",
		v.Name(), v.RetentionTicks())
}

// Package relay manages relay session lifecycles and message delivery.
package relay

import (
	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// MemberView is an immutable snapshot of a session member.
type MemberView struct {
	PlayerID       model.PlayerID
	TeamID         model.TeamID
	State          model.MemberState
	JoinedTick     int64
	DisconnectTick int64
}

// SessionView is an immutable, self-consistent snapshot of a session and its
// member set. External observers always read a copy taken under the manager
// lock; they never see a half-updated session.
type SessionView struct {
	ID               model.SessionID
	Partition        model.PartitionID
	RuleVersion      int
	State            model.SessionState
	Members          []MemberView
	CreatedTick      int64
	EndedTick        int64
	MatchID          string
	Backfill         bool
	BackfillTicketID string
	MessageCount     int64
	ActiveMembers    int
}

func toMemberView(m model.Member) MemberView {
	return MemberView{
		PlayerID:       m.PlayerID,
		TeamID:         m.TeamID,
		State:          m.State,
		JoinedTick:     m.JoinedTick,
		DisconnectTick: m.DisconnectTick,
	}
}

func toSessionView(s *model.Session) SessionView {
	members := make([]MemberView, len(s.Members))
	active := 0
	for i, m := range s.Members {
		members[i] = toMemberView(m)
		if m.State == model.MemberJoined {
			active++
		}
	}
	return SessionView{
		ID:               s.ID,
		Partition:        s.Partition,
		RuleVersion:      s.RuleVersion,
		State:            s.State,
		Members:          members,
		CreatedTick:      s.CreatedTick,
		EndedTick:        s.EndedTick,
		MatchID:          s.MatchID,
		Backfill:         s.Backfill,
		BackfillTicketID: s.BackfillTicketID,
		MessageCount:     s.MessageCount,
		ActiveMembers:    active,
	}
}

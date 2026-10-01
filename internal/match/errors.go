package match

import (
	"fmt"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// RejectCode is a machine-readable rejection reason. Callers can switch on it
// to distinguish the outcome (e.g. queue full vs. duplicate team).
type RejectCode string

const (
	RejectQueueFull              RejectCode = "QUEUE_FULL"
	RejectDuplicateTeam          RejectCode = "DUPLICATE_TEAM"
	RejectTeamEmpty              RejectCode = "TEAM_EMPTY"
	RejectRuleNotFound           RejectCode = "RULE_NOT_FOUND"
	RejectPartitionNotFound      RejectCode = "PARTITION_NOT_FOUND"
	RejectTeamNotWaiting         RejectCode = "TEAM_NOT_WAITING"
	RejectSessionAtCapacity      RejectCode = "SESSION_AT_CAPACITY"
	RejectSessionNotFound        RejectCode = "SESSION_NOT_FOUND"
	RejectSessionEnded           RejectCode = "SESSION_ENDED"
	RejectMemberNotFound         RejectCode = "MEMBER_NOT_FOUND"
	RejectMemberNotDisconnected  RejectCode = "MEMBER_NOT_DISCONNECTED"
	RejectMemberAlreadyLeft      RejectCode = "MEMBER_ALREADY_LEFT"
	RejectInvalidTransition      RejectCode = "INVALID_TRANSITION"
	RejectClientSlow             RejectCode = "CLIENT_SLOW"
	RejectBackfillTimeout        RejectCode = "BACKFILL_TIMEOUT"
	RejectReconnectWindowExpired RejectCode = "RECONNECT_WINDOW_EXPIRED"
)

// Error is a typed, locatable rejection. It carries the specific object
// (team/session/member) the rejection pertains to.
type Error struct {
	Code      RejectCode
	Message   string
	TeamID    model.TeamID
	SessionID model.SessionID
	PlayerID  model.PlayerID
}

func (e *Error) Error() string {
	loc := ""
	if e.TeamID != "" {
		loc += fmt.Sprintf(" team=%s", e.TeamID)
	}
	if e.SessionID != "" {
		loc += fmt.Sprintf(" session=%s", e.SessionID)
	}
	if e.PlayerID != "" {
		loc += fmt.Sprintf(" player=%s", e.PlayerID)
	}
	return fmt.Sprintf("%s%s: %s", e.Code, loc, e.Message)
}

func reject(code RejectCode, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

func rejectTeam(code RejectCode, msg string, team model.TeamID) *Error {
	return &Error{Code: code, Message: msg, TeamID: team}
}

func rejectSession(code RejectCode, msg string, session model.SessionID) *Error {
	return &Error{Code: code, Message: msg, SessionID: session}
}

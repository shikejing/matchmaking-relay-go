// Package types defines shared value types for the matchmaking relay server.
package types

// ID aliases.
type (
	// PlayerID is the unique identifier of a player.
	PlayerID string
	// TeamID is the unique identifier of a team. A team is the atomic
	// unit of matchmaking and is never split or re-formed across teams.
	TeamID string
	// SessionID identifies a relay session.
	SessionID string
	// PartitionID identifies an independent matchmaking partition/queue.
	PartitionID string
	// RuleVersion tags the ruleset a queue entry or session belongs to.
	RuleVersion string
)

// Player is one member of a team.
type Player struct {
	ID    PlayerID
	Score int
	// Meta holds optional opaque player attributes reserved for rules.
	Meta map[string]string
}

// Team is the atomic unit submitted to a queue.
type Team struct {
	ID      TeamID
	Players []Player
	// Version is the ruleset version this team submits under.
	Version RuleVersion
}

// CombinedScore returns the team score produced by the active ruleset;
// zero until attached to a scored queue entry.
func (t *Team) Size() int { return len(t.Players) }

// RejectReason is a distinguishable reason for a rejected submission
// or rejected session allocation.
type RejectReason string

const (
	RejectNone                RejectReason = ""
	RejectQueueFull           RejectReason = "queue_full"
	RejectTooManySessions     RejectReason = "too_many_sessions"
	RejectDuplicateTeam       RejectReason = "duplicate_team"
	RejectPlayerAlreadyQueued RejectReason = "player_already_queued"
	RejectEmptyTeam           RejectReason = "empty_team"
	RejectOversizedTeam       RejectReason = "oversized_team"
	RejectInvalidConfig       RejectReason = "invalid_config"
	RejectUnknownPartition    RejectReason = "unknown_partition"
	RejectUnknownSession      RejectReason = "unknown_session"
	RejectVersionIncompatible RejectReason = "version_incompatible"
	RejectBackfillUnavailable RejectReason = "backfill_unavailable"
)

// MemberState is the state of one member inside a relay session.
type MemberState string

const (
	MemberPresent      MemberState = "present"
	MemberDisconnected MemberState = "disconnected"
	MemberRemoved      MemberState = "removed"
)

// SessionState is the externally visible state of a relay session.
type SessionState string

const (
	// SessionGathering: session created, waiting for initial members to join.
	SessionGathering SessionState = "gathering"
	// SessionActive: full member set connected; relay runs.
	SessionActive SessionState = "active"
	// SessionDegraded: one or more members disconnected within the hold window.
	SessionDegraded SessionState = "degraded"
	// SessionClosed: terminal state.
	SessionClosed SessionState = "closed"
)

// CloseReason explains why a session entered the closed state.
type CloseReason string

const (
	CloseNotClosed         CloseReason = ""
	CloseNormal            CloseReason = "normal"
	CloseDisconnectTimeout CloseReason = "disconnect_timeout"
	CloseBackfillFailed    CloseReason = "backfill_failed"
	CloseEvicted           CloseReason = "evicted"
)

// SubmitResult is the outcome of submitting a team.
type SubmitResult struct {
	Accepted bool
	// Sequence is the globally monotonic submission number when accepted.
	Sequence uint64
	Reason   RejectReason
	// Detail names the offending object (team/player/session id).
	Detail string
}

// MemberSnapshot is one immutable member entry in a SessionSnapshot.
type MemberSnapshot struct {
	Player  PlayerID
	Team    TeamID
	State   MemberState
	Version RuleVersion
}

// SessionSnapshot is one self-consistent view of a relay session:
// state and the full member set are read atomically together.
type SessionSnapshot struct {
	ID          SessionID
	Partition   PartitionID
	State       SessionState
	Version     RuleVersion
	Members     []MemberSnapshot
	CloseReason CloseReason
	CreatedAt   int64
	UpdatedAt   int64
}

// QueuedTeam is one queue entry as observed from outside the queue.
type QueuedTeam struct {
	TeamID     TeamID
	Partition  PartitionID
	Size       int
	Score      int
	EnqueuedAt int64
	Sequence   uint64
	Version    RuleVersion
}

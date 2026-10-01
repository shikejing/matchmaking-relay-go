// Package model defines the shared data types for matchmaking and relay.
package model

// PlayerID identifies a player within a team.
type PlayerID string

// TeamID identifies a team. A team is the indivisible matching unit.
type TeamID string

// SessionID identifies a relay session.
type SessionID string

// PartitionID identifies an isolated matching partition (queue).
type PartitionID string

// Player is a member of a team with a skill score.
type Player struct {
	ID    PlayerID
	Score int
}

// TeamStatus is the lifecycle status of a team in a queue.
type TeamStatus int

const (
	// TeamWaiting is queued and waiting to be matched.
	TeamWaiting TeamStatus = iota
	// TeamMatched has been placed into a session.
	TeamMatched
	// TeamCancelled was cancelled before matching.
	TeamCancelled
)

// Team is the indivisible matching unit. It is enqueued as a whole and can
// never be split or re-formed across teams.
type Team struct {
	ID          TeamID
	Partition   PartitionID
	Members     []Player
	Score       int   // merged score snapshot, produced by the rule version at enqueue time
	RuleVersion int   // rule version that merged the score and governs this team
	EnqueueSeq  int64 // per-partition monotonic sequence, assigned atomically at enqueue
	EnqueueTick int64 // logical tick at enqueue time
	Status      TeamStatus
	MatchedTick int64     // logical tick when matched
	SessionID   SessionID // session the team was placed into, if matched
}

// MemberState is the connection state of a session member.
type MemberState int

const (
	// MemberJoined is connected and in session.
	MemberJoined MemberState = iota
	// MemberDisconnected is offline within the retention window.
	MemberDisconnected
	// MemberLeft has permanently left the session.
	MemberLeft
)

// Member is a player participating in a relay session.
type Member struct {
	PlayerID       PlayerID
	TeamID         TeamID
	State          MemberState
	JoinedTick     int64
	DisconnectTick int64 // tick of last disconnect, 0 if never disconnected
}

// SessionState is the lifecycle state of a relay session.
type SessionState int

const (
	// SessionCreated is reserved for future use; sessions are created Active.
	SessionCreated SessionState = iota
	// SessionActive has members and relays messages.
	SessionActive
	// SessionEnded is closed; no further transitions are allowed.
	SessionEnded
)

// Session is a relay session. All mutations happen under the relay manager
// lock; external reads go through SessionView snapshots.
type Session struct {
	ID               SessionID
	Partition        PartitionID
	RuleVersion      int
	State            SessionState
	Members          []Member
	CreatedTick      int64
	EndedTick        int64
	MatchID          string
	Backfill         bool   // true if the session was created via backfill
	BackfillTicketID string // active backfill ticket, if any
	MessageCount     int64
}

// BackfillTicket requests a replacement team for a vacated session slot.
type BackfillTicket struct {
	ID          string
	SessionID   SessionID
	Partition   PartitionID
	Slots       int
	EnqueueSeq  int64
	EnqueueTick int64
	RuleVersion int
}

// Message is a relayed message delivered to session members.
type Message struct {
	SessionID SessionID
	From      PlayerID
	Payload   []byte
	Tick      int64
}

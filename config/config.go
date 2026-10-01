// Package config defines tunable limits for the whole server.
// Every capacity has a hard upper bound; over-capacity requests receive
// a distinguishable rejection result instead of unbounded growth.
package config

import "github.com/shikejing/matchmaking-relay-go/types"

// Hard upper bounds. Deployments may only configure values at or below
// these limits; they exist to guarantee bounded memory under any load.
const (
	HardQueueCapacity = 100_000
	HardMaxTeamSize   = 100
	HardMaxBatch      = 1_000
	HardMaxSessions   = 100_000
	HardEventHistory  = 100_000
	HardPartitions    = 1_024
	HardPipeBuffer    = 10_000
	HardSearchWindow  = 256
)

// Per-tick search effort caps inside one partition. They bound CPU even
// with a full queue; truncation is deterministic (sequence-order DFS).
const (
	MaxCandidatesPerAnchor = 5_000
	MaxSearchNodesPerTick  = 200_000
)

// ReconnectPolicy decides what happens when the hold window expires.
type ReconnectPolicy string

const (
	// PolicyEndSession closes the session when a disconnect window expires.
	PolicyEndSession ReconnectPolicy = "end_session"
	// PolicyHoldSlot keeps the slot forever (no backfill, no auto-close).
	PolicyHoldSlot ReconnectPolicy = "hold_slot"
	// PolicyBackfill replaces the whole affected team from the queue.
	PolicyBackfill ReconnectPolicy = "backfill"
)

// PartitionConfig configures one matchmaking partition.
type PartitionConfig struct {
	ID              types.PartitionID
	Version         types.RuleVersion
	QueueCapacity   int   // max teams queued (hard cap)
	MaxTeamSize     int   // hard cap on members per submitted team
	TargetTeamSize  int   // total players per matched session
	MaxBatch        int   // max sessions created per matchmaking tick
	ScoreWindow     int   // max acceptable score distance, relaxed after MaxWaitMs
	MaxWaitMs       int64 // patience threshold for the soft score barrier
	ReconnectHoldMs int64 // window in which a member may reconnect
	Reconnect       ReconnectPolicy
	// SearchWindow bounds how many later queued teams may combine with
	// an anchor team during one tick; <=0 falls back to HardSearchWindow.
	SearchWindow int
}

// Config is the full server configuration.
type Config struct {
	Partitions      []PartitionConfig
	MaxSessions     int   // global hard cap on concurrent live sessions
	EventHistoryCap int   // per-object audit history ring size
	RandomSeed      int64 // makes rule-level randomness reproducible
	TickIntervalMs  int64 // auto-tick interval; 0 disables background ticks
	PipeBuffer      int   // in-memory per-peer channel buffer
}

// Default returns a usable baseline configuration with one partition.
func Default() Config {
	return Config{
		Partitions: []PartitionConfig{{
			ID:              "default",
			Version:         "v1",
			QueueCapacity:   1024,
			MaxTeamSize:     5,
			TargetTeamSize:  10,
			MaxBatch:        16,
			ScoreWindow:     200,
			MaxWaitMs:       30_000,
			ReconnectHoldMs: 10_000,
			Reconnect:       PolicyEndSession,
			SearchWindow:    64,
		}},
		MaxSessions:     4096,
		EventHistoryCap: 1024,
		RandomSeed:      1,
		TickIntervalMs:  50,
		PipeBuffer:      64,
	}
}

// Validate checks limits and returns a typed reject reason plus detail.
// RejectNone means valid.
func (c Config) Validate() (types.RejectReason, string) {
	if len(c.Partitions) == 0 {
		return types.RejectInvalidConfig, "no partitions"
	}
	if len(c.Partitions) > HardPartitions {
		return types.RejectInvalidConfig, "too many partitions"
	}
	if c.MaxSessions <= 0 || c.MaxSessions > HardMaxSessions {
		return types.RejectInvalidConfig, "MaxSessions out of bounds"
	}
	if c.EventHistoryCap <= 0 || c.EventHistoryCap > HardEventHistory {
		return types.RejectInvalidConfig, "EventHistoryCap out of bounds"
	}
	if c.PipeBuffer < 0 || c.PipeBuffer > HardPipeBuffer {
		return types.RejectInvalidConfig, "PipeBuffer out of bounds"
	}
	seen := map[types.PartitionID]bool{}
	for _, pc := range c.Partitions {
		if pc.ID == "" {
			return types.RejectInvalidConfig, "empty partition id"
		}
		if seen[pc.ID] {
			return types.RejectInvalidConfig, "duplicate partition " + string(pc.ID)
		}
		seen[pc.ID] = true
		switch {
		case pc.QueueCapacity <= 0 || pc.QueueCapacity > HardQueueCapacity:
			return types.RejectInvalidConfig, "QueueCapacity out of bounds: " + string(pc.ID)
		case pc.MaxTeamSize <= 0 || pc.MaxTeamSize > HardMaxTeamSize:
			return types.RejectInvalidConfig, "MaxTeamSize out of bounds: " + string(pc.ID)
		case pc.TargetTeamSize <= 0 || pc.TargetTeamSize > pc.MaxTeamSize*HardMaxBatch:
			return types.RejectInvalidConfig, "TargetTeamSize out of bounds: " + string(pc.ID)
		case pc.MaxBatch <= 0 || pc.MaxBatch > HardMaxBatch:
			return types.RejectInvalidConfig, "MaxBatch out of bounds: " + string(pc.ID)
		case pc.ScoreWindow < 0:
			return types.RejectInvalidConfig, "negative ScoreWindow: " + string(pc.ID)
		case pc.SearchWindow < 0 || pc.SearchWindow > HardSearchWindow:
			return types.RejectInvalidConfig, "SearchWindow out of bounds: " + string(pc.ID)
		case pc.MaxWaitMs < 0 || pc.ReconnectHoldMs < 0:
			return types.RejectInvalidConfig, "negative time window: " + string(pc.ID)
		}
		switch pc.Reconnect {
		case PolicyEndSession, PolicyHoldSlot, PolicyBackfill:
		default:
			return types.RejectInvalidConfig, "unknown reconnect policy: " + string(pc.ID)
		}
	}
	return types.RejectNone, ""
}

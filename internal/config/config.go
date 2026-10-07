// Package config holds all tunable limits of the matchmaking/relay service.
package config

import (
	"fmt"
	"time"
)

// Upper bounds for capacity-related settings. Every capacity knob has both a
// lower and an upper bound: values outside the range are rejected at startup
// with a *LimitError, so a misconfigured deployment fails fast instead of
// growing without bound at runtime.
const (
	// QueueCapacityMax is the largest allowed per-partition waiting queue.
	QueueCapacityMax = 8192
	// MatchBatchSizeMax is the largest allowed per-tick match batch.
	MatchBatchSizeMax = 256
	// ConcurrentSessionsMax is the largest allowed global active-session cap.
	ConcurrentSessionsMax = 4096
	// AuditCapacityMax is the largest allowed audit ring buffer.
	AuditCapacityMax = 1 << 20
	// MessageBufferSizeMax is the largest allowed per-member pipe buffer.
	MessageBufferSizeMax = 4096
)

// LimitError reports a capacity configuration value outside its allowed
// range. The Field name makes the rejected setting machine-distinguishable.
type LimitError struct {
	Field string // configuration field name, e.g. "QueueCapacity"
	Value int    // the rejected value
	Min   int    // smallest allowed value (inclusive)
	Max   int    // largest allowed value (inclusive)
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("config: %s=%d out of allowed range [%d, %d]", e.Field, e.Value, e.Min, e.Max)
}

func checkRange(field string, value, min, max int) error {
	if value < min || value > max {
		return &LimitError{Field: field, Value: value, Min: min, Max: max}
	}
	return nil
}

// Config groups the tunable limits. All limits are enforced and have upper
// bounds so the system cannot grow without bound under high load.
type Config struct {
	// Seed is the deterministic seed recorded for reproducibility. The match
	// decision path itself never consumes randomness; the seed is part of the
	// reproducibility contract and is recorded in every audit trail.
	Seed int64

	// TickInterval drives the background match loop. Zero means manual
	// stepping only (Service.Tick), which is what deterministic tests use.
	TickInterval time.Duration

	// QueueCapacity is the max number of waiting teams per partition.
	// Enqueue beyond this is rejected with ErrQueueFull.
	QueueCapacity int

	// MatchBatchSize is the max number of matches a single partition may form
	// per tick. Bounds burst work and prevents a hot partition from starving
	// others.
	MatchBatchSize int

	// MaxConcurrentSessions is the global cap on simultaneously active
	// sessions. While at capacity, partitions stop forming matches (teams
	// stay queued) instead of overshooting.
	MaxConcurrentSessions int

	// TeamsPerMatch is the number of teams in one regular match.
	TeamsPerMatch int

	// AuditCapacity is the max number of audit records retained in the
	// in-memory ring buffer.
	AuditCapacity int

	// MessageBufferSize is the per-member outbound pipe buffer size.
	MessageBufferSize int

	// Partitions is the set of partition IDs created at startup.
	Partitions []string
}

// Default returns a configuration with safe, documented defaults.
func Default() Config {
	return Config{
		Seed:                  1,
		TickInterval:          0,
		QueueCapacity:         64,
		MatchBatchSize:        8,
		MaxConcurrentSessions: 32,
		TeamsPerMatch:         2,
		AuditCapacity:         4096,
		MessageBufferSize:     64,
		Partitions:            []string{"default"},
	}
}

// Validate checks the configuration for impossible limits. Capacity fields
// must be within [1, *Max]; violations return a *LimitError identifying the
// exact field, so callers can distinguish which limit was exceeded.
func (c Config) Validate() error {
	if err := checkRange("QueueCapacity", c.QueueCapacity, 1, QueueCapacityMax); err != nil {
		return err
	}
	if err := checkRange("MatchBatchSize", c.MatchBatchSize, 1, MatchBatchSizeMax); err != nil {
		return err
	}
	if err := checkRange("MaxConcurrentSessions", c.MaxConcurrentSessions, 1, ConcurrentSessionsMax); err != nil {
		return err
	}
	if err := checkRange("AuditCapacity", c.AuditCapacity, 1, AuditCapacityMax); err != nil {
		return err
	}
	if err := checkRange("MessageBufferSize", c.MessageBufferSize, 1, MessageBufferSizeMax); err != nil {
		return err
	}
	if c.TeamsPerMatch < 2 {
		return fmt.Errorf("config: TeamsPerMatch must be >= 2, got %d", c.TeamsPerMatch)
	}
	if len(c.Partitions) == 0 {
		return fmt.Errorf("config: at least one partition is required")
	}
	seen := make(map[string]struct{}, len(c.Partitions))
	for _, p := range c.Partitions {
		if p == "" {
			return fmt.Errorf("config: partition id must not be empty")
		}
		if _, ok := seen[p]; ok {
			return fmt.Errorf("config: duplicate partition id %q", p)
		}
		seen[p] = struct{}{}
	}
	return nil
}

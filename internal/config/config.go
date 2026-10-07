// Package config holds all tunable limits of the matchmaking/relay service.
package config

import (
	"fmt"
	"time"
)

// Hard upper bounds for capacity-related configuration. Every capacity has
// both a lower bound (validated in Validate) and one of these upper bounds,
// so a misconfigured deployment is rejected at startup instead of growing
// without bound at runtime.
const (
	// MaxQueueCapacity bounds Config.QueueCapacity (waiting teams per partition).
	MaxQueueCapacity = 4096
	// MaxMatchBatchSize bounds Config.MatchBatchSize (matches per partition per tick).
	MaxMatchBatchSize = 256
	// MaxConcurrentSessions bounds Config.MaxConcurrentSessions (global active sessions).
	MaxConcurrentSessions = 4096
	// MaxAuditCapacity bounds Config.AuditCapacity (retained audit records).
	MaxAuditCapacity = 1 << 20
	// MaxMessageBufferSize bounds Config.MessageBufferSize (per-member pipe buffer).
	MaxMessageBufferSize = 4096
)

// LimitError reports a configuration limit violation. It is distinguishable
// per field and per direction (below minimum vs. above maximum), so startup
// failures can be matched programmatically with errors.As.
type LimitError struct {
	Field string // configuration field name, e.g. "QueueCapacity"
	Value int    // the rejected value
	Min   int    // smallest allowed value (inclusive)
	Max   int    // largest allowed value (inclusive)
}

func (e *LimitError) Error() string {
	if e.Value < e.Min {
		return fmt.Sprintf("config: %s=%d is below the minimum %d", e.Field, e.Value, e.Min)
	}
	return fmt.Sprintf("config: %s=%d exceeds the maximum %d", e.Field, e.Value, e.Max)
}

// checkLimit returns a *LimitError unless min <= v <= max.
func checkLimit(field string, v, min, max int) error {
	if v < min || v > max {
		return &LimitError{Field: field, Value: v, Min: min, Max: max}
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

// Validate checks the configuration for impossible limits. Every capacity
// field is checked against both its lower and its hard upper bound; violations
// are returned as *LimitError so the offending field is distinguishable.
func (c Config) Validate() error {
	if err := checkLimit("QueueCapacity", c.QueueCapacity, 1, MaxQueueCapacity); err != nil {
		return err
	}
	if err := checkLimit("MatchBatchSize", c.MatchBatchSize, 1, MaxMatchBatchSize); err != nil {
		return err
	}
	if err := checkLimit("MaxConcurrentSessions", c.MaxConcurrentSessions, 1, MaxConcurrentSessions); err != nil {
		return err
	}
	if err := checkLimit("AuditCapacity", c.AuditCapacity, 1, MaxAuditCapacity); err != nil {
		return err
	}
	if err := checkLimit("MessageBufferSize", c.MessageBufferSize, 1, MaxMessageBufferSize); err != nil {
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

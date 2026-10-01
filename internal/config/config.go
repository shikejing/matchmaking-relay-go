// Package config holds all tunable limits of the matchmaking/relay service.
package config

import (
	"fmt"
	"time"
)

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

// Validate checks the configuration for impossible limits.
func (c Config) Validate() error {
	if c.QueueCapacity <= 0 {
		return fmt.Errorf("config: QueueCapacity must be > 0, got %d", c.QueueCapacity)
	}
	if c.MatchBatchSize <= 0 {
		return fmt.Errorf("config: MatchBatchSize must be > 0, got %d", c.MatchBatchSize)
	}
	if c.MaxConcurrentSessions <= 0 {
		return fmt.Errorf("config: MaxConcurrentSessions must be > 0, got %d", c.MaxConcurrentSessions)
	}
	if c.TeamsPerMatch < 2 {
		return fmt.Errorf("config: TeamsPerMatch must be >= 2, got %d", c.TeamsPerMatch)
	}
	if c.AuditCapacity <= 0 {
		return fmt.Errorf("config: AuditCapacity must be > 0, got %d", c.AuditCapacity)
	}
	if c.MessageBufferSize <= 0 {
		return fmt.Errorf("config: MessageBufferSize must be > 0, got %d", c.MessageBufferSize)
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

package config

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/types"
)

func TestDefaultValid(t *testing.T) {
	if r, d := Default().Validate(); r != types.RejectNone {
		t.Fatalf("default config invalid: %s %s", r, d)
	}
}

func TestValidateBounds(t *testing.T) {
	bad := Default()
	bad.MaxSessions = HardMaxSessions + 1
	if r, _ := bad.Validate(); r != types.RejectInvalidConfig {
		t.Fatalf("over-hardcap sessions: %s", r)
	}
	bad = Default()
	bad.Partitions[0].QueueCapacity = -1
	if r, _ := bad.Validate(); r != types.RejectInvalidConfig {
		t.Fatalf("negative queue: %s", r)
	}
	bad = Default()
	bad.Partitions[0].Reconnect = "nope"
	if r, _ := bad.Validate(); r != types.RejectInvalidConfig {
		t.Fatalf("bad policy: %s", r)
	}
	bad = Default()
	bad.Partitions = append(bad.Partitions, bad.Partitions[0])
	if r, _ := bad.Validate(); r != types.RejectInvalidConfig {
		t.Fatalf("dup partition: %s", r)
	}
}

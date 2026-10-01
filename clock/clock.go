// Package clock provides an injectable time source so scheduling is
// deterministic in tests.
package clock

import (
	"sync/atomic"
	"time"
)

// Clock is the minimal time source used across the system.
type Clock interface {
	// NowMs returns the current time in milliseconds.
	NowMs() int64
}

// RealClock reads the actual system monotonic wall clock.
type RealClock struct{}

func NewRealClock() *RealClock { return &RealClock{} }

func (RealClock) NowMs() int64 { return time.Now().UnixMilli() }

// FakeClock is a manually advanced clock for deterministic tests.
type FakeClock struct {
	ms atomic.Int64
}

func NewFakeClock(startMs int64) *FakeClock {
	f := &FakeClock{}
	f.ms.Store(startMs)
	return f
}

func (f *FakeClock) NowMs() int64 { return f.ms.Load() }

// Advance moves the fake clock forward by d milliseconds.
// It panics on negative d to catch test mistakes early.
func (f *FakeClock) Advance(d int64) {
	if d < 0 {
		panic("clock: negative advance")
	}
	f.ms.Add(d)
}

// Set jumps the clock to an absolute millisecond timestamp.
func (f *FakeClock) Set(ms int64) { f.ms.Store(ms) }

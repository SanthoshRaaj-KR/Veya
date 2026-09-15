// Package clock provides the implementations of core.Clock.
//
// Two of them, and the second is the point. System reads the wall clock.
// Virtual advances only when a test tells it to, which is what lets a test
// about a 24-hour durable timer finish in a millisecond and produce the same
// result every time.
//
// No code above the composition root calls time.Now. Time enters the runtime
// here or not at all.
package clock

import (
	"sync"
	"time"
)

// System reads the host clock. Used by every binary in cmd/.
type System struct{}

func (System) Now() time.Time { return time.Now() }

// Virtual is a clock that only moves when told. Used by tests and, from
// Layer 7, by the deterministic simulation harness.
//
// Safe for concurrent use: the runtime reads time from several goroutines,
// and a test that advances it while they read must not race.
type Virtual struct {
	mu  sync.Mutex
	now time.Time
}

// NewVirtual returns a Virtual clock started at t.
func NewVirtual(t time.Time) *Virtual { return &Virtual{now: t} }

func (v *Virtual) Now() time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.now
}

// Advance moves the clock forward by d.
func (v *Virtual) Advance(d time.Duration) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.now = v.now.Add(d)
}

// Set moves the clock to an absolute time.
func (v *Virtual) Set(t time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.now = t
}

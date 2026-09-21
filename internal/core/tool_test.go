package core_test

import (
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// TestZeroRetryPolicyIsImmediateRetry. Every run committed before this type
// existed retried immediately; the zero value has to keep reading that way,
// or a policy nobody configured would change behaviour underneath it.
func TestZeroRetryPolicyIsImmediateRetry(t *testing.T) {
	var p core.RetryPolicy
	for attempt := 1; attempt <= 5; attempt++ {
		if got := p.Backoff(attempt); got != 0 {
			t.Fatalf("Backoff(%d) = %s, want 0 for the zero policy", attempt, got)
		}
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	p := core.RetryPolicy{
		InitialBackoff: time.Second,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2,
	}

	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second, // would be 16s uncapped
		10 * time.Second,
	}
	for i, w := range want {
		attempt := i + 1
		if got := p.Backoff(attempt); got != w {
			t.Fatalf("Backoff(%d) = %s, want %s", attempt, got, w)
		}
	}
}

func TestMultiplierBelowOneIsReadAsOne(t *testing.T) {
	// A policy that set a backoff but forgot to make it grow gets a fixed
	// delay, not a curve that shrinks toward zero and not an error.
	p := core.RetryPolicy{InitialBackoff: 5 * time.Second, Multiplier: 0.5}
	for attempt := 1; attempt <= 4; attempt++ {
		if got := p.Backoff(attempt); got != 5*time.Second {
			t.Fatalf("Backoff(%d) = %s, want a fixed 5s", attempt, got)
		}
	}
}

func TestJitterStaysWithinBounds(t *testing.T) {
	p := core.RetryPolicy{InitialBackoff: 10 * time.Second, Jitter: 0.5}
	floor := 5 * time.Second // delay * (1 - jitter)
	for i := 0; i < 200; i++ {
		got := p.Backoff(1)
		if got < floor || got > 10*time.Second {
			t.Fatalf("Backoff(1) = %s, want within [%s, 10s]", got, floor)
		}
	}
}

func TestFullJitterCanReachZero(t *testing.T) {
	p := core.RetryPolicy{InitialBackoff: 10 * time.Second, Jitter: 1}
	sawSmall := false
	for i := 0; i < 200; i++ {
		if p.Backoff(1) < time.Second {
			sawSmall = true
			break
		}
	}
	if !sawSmall {
		t.Fatal("full jitter (1.0) never produced a small delay in 200 draws")
	}
}

func TestBackoffBeforeTheFirstAttemptIsZero(t *testing.T) {
	p := core.RetryPolicy{InitialBackoff: time.Second}
	if got := p.Backoff(0); got != 0 {
		t.Fatalf("Backoff(0) = %s, want 0", got)
	}
}

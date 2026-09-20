package main

import (
	"strings"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// TestAnIndefiniteParkIsNeverPrintedAsADate.
//
// core.Indefinite is a real timestamp, so the obvious rendering prints
// "until 9999-12-31 23:59:59". An operator reading that concludes something is
// corrupt, and starts debugging a run that is doing exactly what it was told:
// waiting for a person.
func TestAnIndefiniteParkIsNeverPrintedAsADate(t *testing.T) {
	got := describeWait(core.Wait{
		Kind: core.WaitTimer, StepID: core.Step(3), Until: core.Indefinite,
	}, time.Now())

	if strings.Contains(got, "9999") {
		t.Fatalf("describeWait printed the sentinel as a date: %q", got)
	}
	if !strings.Contains(got, "no scheduled wake-up") {
		t.Fatalf("describeWait = %q, want it to say there is no scheduled wake-up", got)
	}
	if !strings.Contains(got, "S3") {
		t.Fatalf("describeWait = %q, want it to name the step", got)
	}
}

// TestAnOverdueParkReadsAsImminentNotBroken. A park in the past means ready.
// Presenting it as an error would send an operator looking for a fault at the
// one moment when the run is about to proceed on its own.
func TestAnOverdueParkReadsAsImminentNotBroken(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	got := describeWait(core.Wait{
		Kind: core.WaitTimer, StepID: core.Step(1), Until: now.Add(-90 * time.Second),
	}, now)

	if !strings.Contains(got, "next scan") {
		t.Fatalf("describeWait = %q, want it to say the next scan will pick it up", got)
	}
	for _, bad := range []string{"error", "stuck", "overdue by"} {
		if strings.Contains(strings.ToLower(got), bad) {
			t.Fatalf("describeWait = %q; an overdue park is ready, not a fault", got)
		}
	}
}

func TestAFuturePrintsHowLongIsLeft(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	got := describeWait(core.Wait{
		Kind: core.WaitTimer, StepID: core.Step(2), Until: now.Add(18*time.Hour + 32*time.Minute),
	}, now)

	if !strings.Contains(got, "in 18h32m") {
		t.Fatalf("describeWait = %q, want it to say how long is left", got)
	}
	if !strings.Contains(got, "2026-09-21 06:32:00") {
		t.Fatalf("describeWait = %q, want the absolute instant too", got)
	}
}

func TestRoughlyDropsNoiseAPersonCannotUse(t *testing.T) {
	cases := map[time.Duration]string{
		42 * time.Second: "42s",
		18*time.Hour + 32*time.Minute + 14*time.Second: "18h32m",
		72 * time.Hour: "3d0s",
	}
	for d, want := range cases {
		if got := roughly(d); !strings.HasPrefix(got, want) {
			t.Errorf("roughly(%s) = %q, want it to start with %q", d, got, want)
		}
	}
}

// TestASignalWaitNamesTheSignal. "waiting for SIGNAL" tells an operator
// nothing the parked status did not already say. The name is the thing they
// can act on -- it is the argument to `veya signal`.
func TestASignalWaitNamesTheSignal(t *testing.T) {
	got := describeWait(core.Wait{
		Kind: core.WaitSignal, StepID: core.Step(2),
		Signal: "approval", Until: core.Indefinite,
	}, time.Now())

	if !strings.Contains(got, `"approval"`) {
		t.Fatalf("describeWait = %q, want it to name the signal", got)
	}
	if strings.Contains(got, "SIGNAL") {
		t.Fatalf("describeWait = %q, want the name rather than the kind", got)
	}
}

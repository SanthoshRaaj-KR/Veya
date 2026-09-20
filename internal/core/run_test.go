package core_test

import (
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// TestAPastWakeUpIsReadyNotLate is the regression test for the failure mode
// durable timers exist to prevent.
//
// If an overdue park read as anything other than "ready", a runtime that was
// down over a wake-up would come back and never pick the run up again: every
// timer that expired during the outage would be lost, silently, with the run
// still RUNNING and nothing in flight.
func TestAPastWakeUpIsReadyNotLate(t *testing.T) {
	now := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)

	past := now.Add(-48 * time.Hour)
	overdue := core.Run{AvailableAt: &past}
	if overdue.IsWaiting(now) {
		t.Fatal("a run whose wake-up passed two days ago is still waiting; " +
			"an outage would lose every timer that expired during it")
	}

	future := now.Add(time.Minute)
	sleeping := core.Run{AvailableAt: &future}
	if !sleeping.IsWaiting(now) {
		t.Fatal("a run parked a minute from now is not waiting")
	}

	forever := core.Indefinite
	parked := core.Run{AvailableAt: &forever}
	if !parked.IsWaiting(now) {
		t.Fatal("a run parked indefinitely is not waiting")
	}
}

// TestAnUnparkedRunIsReady. Nil is the value every run that predates the
// column carries, so it has to mean ready rather than "never looked at".
func TestAnUnparkedRunIsReady(t *testing.T) {
	if (core.Run{}).IsWaiting(time.Now()) {
		t.Fatal("a run with no park reads as waiting; every pre-existing run has nil here")
	}
}

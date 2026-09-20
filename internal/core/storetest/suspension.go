package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// suspensionContracts covers runs.available_at: a run that is RUNNING with
// nothing in flight and is nonetheless not owed a decision yet.
func suspensionContracts() []contract {
	return []contract{
		{"AvailableAtRoundTrips", testAvailableAtRoundTrips},
		{"AdvancingClearsThePark", testAdvancingClearsThePark},
	}
}

// testAvailableAtRoundTrips. The column is the whole suspension mechanism, so
// an adapter that dropped the value, or returned it in the wrong zone, would
// turn "sleep until Tuesday" into "sleep until some other time" — and the
// difference would first be visible as a run that woke early in production.
func testAvailableAtRoundTrips(t *testing.T, s core.Store) {
	id := core.RunID("run-parked")
	mustCreateRun(t, s, id)

	if got := mustGetRun(t, s, id); got.AvailableAt != nil {
		t.Fatalf("a new run has AvailableAt = %v, want nil; nil is what ready means", got.AvailableAt)
	}

	wake := time.Date(2026, time.September, 22, 9, 30, 0, 0, time.UTC)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{
			Status:      core.RunRunning,
			AvailableAt: &wake,
		})
	})

	got := mustGetRun(t, s, id)
	if got.AvailableAt == nil {
		t.Fatal("AvailableAt was not persisted")
	}
	if !got.AvailableAt.Equal(wake) {
		t.Fatalf("AvailableAt = %s, want %s", got.AvailableAt.UTC(), wake)
	}
	if !got.IsWaiting(wake.Add(-time.Second)) {
		t.Fatal("a run parked a second from now does not read as waiting")
	}
	if got.IsWaiting(wake.Add(time.Second)) {
		t.Fatal("a run whose wake-up has passed still reads as waiting; " +
			"a past park means ready, not late")
	}

	// Indefinite has to survive the round trip too. It is a real timestamp far
	// beyond any schedule, not a magic NULL, so an adapter that clamped or
	// truncated it would quietly wake every signal wait in the system.
	forever := core.Indefinite
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 1, core.RunState{
			Status:      core.RunRunning,
			AvailableAt: &forever,
		})
	})

	got = mustGetRun(t, s, id)
	if got.AvailableAt == nil || !core.IsIndefinite(*got.AvailableAt) {
		t.Fatalf("AvailableAt = %v, want an indefinite park", got.AvailableAt)
	}
}

// testAdvancingClearsThePark. Nil in RunState means "not parked", and every
// caller that is not suspending leaves it nil — so advancement clears the
// column without anyone having to remember to.
//
// The alternative, carrying the old value forward unless told otherwise, would
// mean a run that woke up, did some work and dispatched a task was still
// marked as waiting, and the scan would refuse to touch it after the task
// finished. That is a permanently stuck run, produced by a default.
func testAdvancingClearsThePark(t *testing.T, s core.Store) {
	id := core.RunID("run-unparked")
	mustCreateRun(t, s, id)

	wake := time.Date(2026, time.September, 22, 9, 30, 0, 0, time.UTC)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{
			Status:      core.RunRunning,
			AvailableAt: &wake,
		})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 1, core.RunState{Status: core.RunRunning})
	})

	if got := mustGetRun(t, s, id); got.AvailableAt != nil {
		t.Fatalf("AvailableAt = %v after an ordinary advance, want nil", got.AvailableAt)
	}
}

package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// farFuture is the "now" a scan is given when the test is not about parking.
// It is past every instant these contracts set except core.Indefinite, so a
// run only fails to appear if something other than a park excluded it.
var farFuture = time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC)

// suspensionContracts covers runs.available_at: a run that is RUNNING with
// nothing in flight and is nonetheless not owed a decision yet.
func suspensionContracts() []contract {
	return []contract{
		{"AvailableAtRoundTrips", testAvailableAtRoundTrips},
		{"AdvancingClearsThePark", testAdvancingClearsThePark},
		{"TheScanLeavesAWaitingRunAlone", testScanLeavesAWaitingRunAlone},
		{"AnExpiredParkIsPickedUpNormally", testExpiredParkIsPickedUpNormally},
		{"NextWakeUpFindsTheEarliestPark", testNextWakeUpFindsTheEarliestPark},
		{"NextWakeUpIgnoresFinishedRuns", testNextWakeUpIgnoresFinishedRuns},
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

// testScanLeavesAWaitingRunAlone. Without the predicate a sleeping run is
// RUNNING with nothing in flight, which is exactly what this query returns —
// so the recovery loop would advance it on every scan and the run would never
// actually sleep.
func testScanLeavesAWaitingRunAlone(t *testing.T, s core.Store) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)

	ready := core.RunID("run-ready")
	sleeping := core.RunID("run-sleeping")
	parked := core.RunID("run-parked-forever")
	for _, id := range []core.RunID{ready, sleeping, parked} {
		mustCreateRun(t, s, id)
	}

	tomorrow := now.Add(24 * time.Hour)
	forever := core.Indefinite
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, sleeping, 0, core.RunState{
			Status: core.RunRunning, AvailableAt: &tomorrow,
		})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, parked, 0, core.RunState{
			Status: core.RunRunning, AvailableAt: &forever,
		})
	})

	got, err := s.RunsAwaitingAdvance(ctx, now, 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(got) != 1 || got[0] != ready {
		t.Fatalf("RunsAwaitingAdvance = %v, want only %s; a parked run must not be swept up", got, ready)
	}
}

// testExpiredParkIsPickedUpNormally is the other half, and the one that
// matters after an outage.
//
// A run whose wake-up passed while the runtime was down has to come back
// through the ordinary scan with no catch-up pass and no special case. If an
// overdue park needed separate handling, every timer that expired during the
// outage would be waiting for a code path nobody wrote.
func testExpiredParkIsPickedUpNormally(t *testing.T, s core.Store) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)

	id := core.RunID("run-overdue")
	mustCreateRun(t, s, id)

	// Parked to wake three days ago: the runtime was down over the weekend.
	overdue := now.Add(-72 * time.Hour)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{
			Status: core.RunRunning, AvailableAt: &overdue,
		})
	})

	got, err := s.RunsAwaitingAdvance(ctx, now, 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(got) != 1 || got[0] != id {
		t.Fatalf("RunsAwaitingAdvance = %v, want %s; a park in the past means ready, not late", got, id)
	}

	// And the instant itself is the boundary: exactly now is ready.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 1, core.RunState{
			Status: core.RunRunning, AvailableAt: &now,
		})
	})
	got, err = s.RunsAwaitingAdvance(ctx, now, 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(got) != 1 {
		t.Fatal("a run parked at exactly now was not picked up; the comparison must be inclusive")
	}
}

// testNextWakeUpFindsTheEarliestPark. The recovery loop uses this to shorten
// its next sleep, so a wrong answer is a late wake-up rather than a lost one —
// but "earliest" has to mean earliest, or a run parked for a second waits out
// the full interval behind one parked for a week.
func testNextWakeUpFindsTheEarliestPark(t *testing.T, s core.Store) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)

	if _, ok, err := s.NextWakeUp(ctx, now); err != nil || ok {
		t.Fatalf("NextWakeUp on an empty store = (ok %v, err %v), want no wake-up", ok, err)
	}

	soon := now.Add(time.Second)
	later := now.Add(7 * 24 * time.Hour)
	for id, at := range map[core.RunID]time.Time{
		"run-later": later,
		"run-soon":  soon,
	} {
		mustCreateRun(t, s, id)
		at := at
		mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
			return tx.AdvanceRun(ctx, id, 0, core.RunState{
				Status: core.RunRunning, AvailableAt: &at,
			})
		})
	}

	got, ok, err := s.NextWakeUp(ctx, now)
	if err != nil || !ok {
		t.Fatalf("NextWakeUp = (ok %v, err %v), want a wake-up", ok, err)
	}
	if !got.Equal(soon) {
		t.Fatalf("NextWakeUp = %s, want the earliest park %s", got, soon)
	}

	// Strictly after. A park that is already due is not a future wake-up: it
	// is work for this scan, and returning it would have the loop wake
	// immediately to rediscover what it just failed to advance.
	if _, ok, err := s.NextWakeUp(ctx, later); err != nil || ok {
		t.Fatalf("NextWakeUp past every park = (ok %v, err %v), want none", ok, err)
	}
	if _, ok, _ := s.NextWakeUp(ctx, soon); !ok {
		t.Fatal("NextWakeUp at the first park found nothing; the later one is still ahead")
	}
}

// testNextWakeUpIgnoresFinishedRuns. A terminal run's available_at is never
// cleared by anything, so a query that did not filter on status would keep
// waking the loop for runs that ended weeks ago.
func testNextWakeUpIgnoresFinishedRuns(t *testing.T, s core.Store) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)
	wake := now.Add(time.Hour)

	id := core.RunID("run-done-but-parked")
	mustCreateRun(t, s, id)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{
			Status: core.RunRunning, AvailableAt: &wake,
		})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 1, core.RunState{
			Status: core.RunCompleted, AvailableAt: &wake,
		})
	})

	if _, ok, err := s.NextWakeUp(ctx, now); err != nil || ok {
		t.Fatalf("NextWakeUp = (ok %v, err %v); a finished run must not wake the loop", ok, err)
	}
}

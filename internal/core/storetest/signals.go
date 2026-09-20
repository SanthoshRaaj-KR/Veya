package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// signalContracts covers the signals table: something that happened outside
// the system, recorded against a run whether or not anything is waiting.
func signalContracts() []contract {
	return []contract{
		{"SignalsRoundTrip", testSignalsRoundTrip},
		{"ASignalIsDedupedBySenderID", testSignalDedup},
		{"SignalsComeBackInArrivalOrder", testSignalOrder},
		{"ReleaseRunClearsTheParkWithoutAdvancing", testReleaseRun},
		{"RecordingAndReleasingCommitTogether", testSignalAndReleaseAreAtomic},
	}
}

func testSignalsRoundTrip(t *testing.T, s core.Store) {
	ctx := context.Background()
	id := core.RunID("run-signalled")
	mustCreateRun(t, s, id)

	// A read with nothing stored is empty, not an error. A wait on a signal
	// that has not arrived is the normal case, not an exceptional one.
	got, err := s.Signals(ctx, id, "approval")
	if err != nil {
		t.Fatalf("Signals before anything arrived: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d signals, want none", len(got))
	}

	payload := json.RawMessage(`{"approved_by":"ops"}`)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.RecordSignal(ctx, core.Signal{
			RunID: id, ID: "cb-1", Name: "approval", Payload: payload,
		})
	})

	got, err = s.Signals(ctx, id, "approval")
	if err != nil {
		t.Fatalf("Signals: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if got[0].ID != "cb-1" || got[0].Name != "approval" {
		t.Fatalf("got %+v", got[0])
	}
	if got[0].CreatedAt.IsZero() {
		t.Fatal("a stored signal has no CreatedAt; the store must stamp it from the clock")
	}
	assertJSONEqual(t, "payload", got[0].Payload, payload)

	// Names do not bleed. A run waiting for "approval" must not be woken by
	// "cancellation" landing on the same run.
	other, err := s.Signals(ctx, id, "cancellation")
	if err != nil {
		t.Fatalf("Signals: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("a signal named approval answered a read for cancellation: %+v", other)
	}
}

// testSignalDedup is the at-least-once guard.
//
// The sender retries a callback it is unsure landed. If the second arrival
// were recorded, a run waiting for one approval would find two, and the second
// would satisfy the next wait for the same name — approving something nobody
// approved.
func testSignalDedup(t *testing.T, s core.Store) {
	ctx := context.Background()
	id := core.RunID("run-retried-callback")
	mustCreateRun(t, s, id)

	record := func() error {
		return s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
			return tx.RecordSignal(ctx, core.Signal{
				RunID: id, ID: "cb-1", Name: "approval",
				Payload: json.RawMessage(`{"n":1}`),
			})
		})
	}

	if err := record(); err != nil {
		t.Fatalf("first arrival: %v", err)
	}
	if err := record(); !errors.Is(err, core.ErrSignalExists) {
		t.Fatalf("second arrival under the same id = %v, want ErrSignalExists", err)
	}

	got, err := s.Signals(ctx, id, "approval")
	if err != nil {
		t.Fatalf("Signals: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d signals after a retried callback, want 1", len(got))
	}
}

// testSignalOrder. Several signals may share a name on one run, and a wait
// consumes them in arrival order. An order that varies between reads would
// have a run consume them one way on the first pass and another on replay.
func testSignalOrder(t *testing.T, s core.Store) {
	ctx := context.Background()
	id := core.RunID("run-many-signals")
	mustCreateRun(t, s, id)

	for _, sid := range []core.SignalID{"c", "a", "b"} {
		sid := sid
		mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
			return tx.RecordSignal(ctx, core.Signal{
				RunID: id, ID: sid, Name: "tick",
			})
		})
	}

	for attempt := 0; attempt < 3; attempt++ {
		got, err := s.Signals(ctx, id, "tick")
		if err != nil {
			t.Fatalf("Signals: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d signals, want 3", len(got))
		}
		want := []core.SignalID{"c", "a", "b"}
		for i, w := range want {
			if got[i].ID != w {
				t.Fatalf("read %d: signal %d is %s, want %s (arrival order)",
					attempt, i, got[i].ID, w)
			}
		}
	}
}

// testReleaseRun. Releasing says "this run is worth looking at again" and
// nothing more. It must not bump the version: the sender of a signal is not a
// decider, and a version bump would make it lose a race with one, or make one
// lose a race with it.
func testReleaseRun(t *testing.T, s core.Store) {
	ctx := context.Background()
	id := core.RunID("run-released")
	mustCreateRun(t, s, id)

	forever := core.Indefinite
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{
			Status: core.RunRunning, AvailableAt: &forever,
		})
	})

	before := mustGetRun(t, s, id)
	if before.AvailableAt == nil {
		t.Fatal("the run did not park")
	}

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.ReleaseRun(ctx, id)
	})

	after := mustGetRun(t, s, id)
	if after.AvailableAt != nil {
		t.Fatalf("AvailableAt = %v after release, want nil", after.AvailableAt)
	}
	if after.Version != before.Version {
		t.Fatalf("version went %d -> %d; releasing is not advancing",
			before.Version, after.Version)
	}
	if after.Status != core.RunRunning {
		t.Fatalf("status = %s after release, want RUNNING", after.Status)
	}

	// The park it cleared is gone, so the ordinary scan sees the run.
	awaiting, err := s.RunsAwaitingAdvance(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(awaiting) != 1 || awaiting[0] != id {
		t.Fatalf("RunsAwaitingAdvance = %v, want the released run", awaiting)
	}

	if err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ReleaseRun(ctx, "run-that-does-not-exist")
	}); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("releasing an unknown run = %v, want ErrNotFound", err)
	}
}

// testSignalAndReleaseAreAtomic. Arrival records the signal and un-parks the
// run, and those commit together or not at all. Split, a crash between them
// leaves a signal nobody will act on attached to a run that is still waiting —
// which is the failure storing the signal was supposed to prevent,
// reintroduced one layer down.
func testSignalAndReleaseAreAtomic(t *testing.T, s core.Store) {
	ctx := context.Background()
	id := core.RunID("run-atomic-signal")
	mustCreateRun(t, s, id)

	forever := core.Indefinite
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{
			Status: core.RunRunning, AvailableAt: &forever,
		})
	})

	// A transaction that records and then fails leaves neither behind.
	boom := errors.New("the process died here")
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.RecordSignal(ctx, core.Signal{
			RunID: id, ID: "cb-doomed", Name: "approval",
		}); err != nil {
			return err
		}
		if err := tx.ReleaseRun(ctx, id); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("RunInTx = %v, want the caller's error", err)
	}

	got, err := s.Signals(ctx, id, "approval")
	if err != nil {
		t.Fatalf("Signals: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("a rolled-back signal survived: %+v", got)
	}
	if run := mustGetRun(t, s, id); run.AvailableAt == nil {
		t.Fatal("a rolled-back release un-parked the run anyway")
	}

	// And the successful version does both.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		if err := tx.RecordSignal(ctx, core.Signal{
			RunID: id, ID: "cb-real", Name: "approval",
		}); err != nil {
			return err
		}
		return tx.ReleaseRun(ctx, id)
	})

	got, err = s.Signals(ctx, id, "approval")
	if err != nil {
		t.Fatalf("Signals: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if run := mustGetRun(t, s, id); run.AvailableAt != nil {
		t.Fatalf("AvailableAt = %v; the signal landed but the run is still parked",
			run.AvailableAt)
	}
}

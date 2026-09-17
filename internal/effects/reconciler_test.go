package effects_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/effects"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
	"github.com/SanthoshRaaj-KR/Veya/internal/tool"
)

// The background reconciler exists for effects nothing else will ever revisit:
// the task dead-lettered, the run failed, and a row is left saying "we do not
// know". Without this sweep, that becomes "we never found out".

// TestReconcilerSettlesAnAbandonedEffect is the case it was built for.
func TestReconcilerSettlesAnAbandonedEffect(t *testing.T) {
	f := newFixture(t)
	f.tools.Effectful("charge", core.ClassQueryable, time.Hour, failingHandler,
		core.ReconcilerFunc(func(context.Context, core.Effect) (core.Resolution, error) {
			return core.Resolution{
				Kind:        core.ResolvedCommitted,
				Response:    json.RawMessage(`{"reference":"ch_7"}`),
				ExternalRef: "ch_7",
				Detail:      "found by idempotency key",
			}, nil
		}))

	key := f.seedEffect(t, "charge", core.ClassQueryable, core.EffectUnknown)
	f.clock.Advance(2 * time.Minute) // older than StaleAfter

	if n := f.reconcile(t); n != 1 {
		t.Fatalf("settled %d effects, want 1", n)
	}

	got := f.effect(t, key)
	if got.Status != core.EffectCommitted {
		t.Fatalf("status = %s, want COMMITTED", got.Status)
	}
	if got.ExternalRef != "ch_7" {
		t.Fatalf("external ref = %q, want ch_7", got.ExternalRef)
	}
}

// TestReconcilerRecordsADefiniteNonEvent covers the other answer. "It never
// happened" is just as much a resolution as "it did", and recording it is what
// turns an open question into a closed one.
func TestReconcilerRecordsADefiniteNonEvent(t *testing.T) {
	f := newFixture(t)
	f.tools.Effectful("charge", core.ClassQueryable, time.Hour, failingHandler,
		core.ReconcilerFunc(func(context.Context, core.Effect) (core.Resolution, error) {
			return core.Resolution{Kind: core.ResolvedNotExecuted, Detail: "no record"}, nil
		}))

	key := f.seedEffect(t, "charge", core.ClassQueryable, core.EffectUnknown)
	f.clock.Advance(2 * time.Minute)

	if n := f.reconcile(t); n != 1 {
		t.Fatalf("settled %d effects, want 1", n)
	}
	if got := f.effect(t, key); got.Status != core.EffectFailed {
		t.Fatalf("status = %s, want FAILED", got.Status)
	}
}

// TestReconcilerNeverPerformsTheAction is the constraint that separates this
// loop from the executor.
//
// An IDEMPOTENT_BY_KEY effect is normally settled by re-sending, which is safe
// because the provider deduplicates. Here there is no live task waiting for
// the result, so a send would be a real external action taken on behalf of
// work nobody is waiting for. The effect stays unresolved and visible instead.
func TestReconcilerNeverPerformsTheAction(t *testing.T) {
	f := newFixture(t)

	var calls atomic.Int32
	f.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		func(context.Context, core.ToolCall) ([]byte, error) {
			calls.Add(1)
			return json.RawMessage(`{"reference":"ch_9"}`), nil
		}, nil) // no reconciler: it can act, but it cannot be asked

	key := f.seedEffect(t, "charge", core.ClassIdempotentByKey, core.EffectUnknown)
	f.clock.Advance(2 * time.Minute)

	if n := f.reconcile(t); n != 0 {
		t.Fatalf("settled %d effects; without a way to ask, nothing can be settled here", n)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("the provider was called %d times by the background sweep; "+
			"it resolves knowledge and must never perform actions", got)
	}
	if got := f.effect(t, key); got.Status != core.EffectUnknown {
		t.Fatalf("status = %s, want UNKNOWN: it is genuinely still unknown", got.Status)
	}
}

// TestReconcilerRespectsProviderKeyRetention covers the window in which an
// answer can be trusted. Past it, "not found" means the provider forgot, and
// acting on that is the duplicate this design exists to prevent.
func TestReconcilerRespectsProviderKeyRetention(t *testing.T) {
	f := newFixture(t)

	var queries atomic.Int32
	f.tools.Effectful("charge", core.ClassQueryable, time.Minute, failingHandler,
		core.ReconcilerFunc(func(context.Context, core.Effect) (core.Resolution, error) {
			queries.Add(1)
			return core.Resolution{Kind: core.ResolvedNotExecuted}, nil
		}))

	key := f.seedEffect(t, "charge", core.ClassQueryable, core.EffectUnknown)
	f.clock.Advance(time.Hour) // far past the provider's one-minute retention

	if n := f.reconcile(t); n != 0 {
		t.Fatalf("settled %d effects past the key TTL, want 0", n)
	}
	if got := queries.Load(); got != 0 {
		t.Fatalf("reconciler consulted %d times past the key TTL; its answer "+
			"cannot be trusted there", got)
	}
	if got := f.effect(t, key); got.Status != core.EffectUnknown {
		t.Fatalf("status = %s, want UNKNOWN", got.Status)
	}
}

// TestReconcilerIgnoresFreshEffects keeps the sweep away from live work. An
// effect marked RUNNING a moment ago is a request in flight, and asking about
// it would get "no" right up until the moment it becomes "yes".
func TestReconcilerIgnoresFreshEffects(t *testing.T) {
	f := newFixture(t)

	var queries atomic.Int32
	f.tools.Effectful("charge", core.ClassQueryable, time.Hour, failingHandler,
		core.ReconcilerFunc(func(context.Context, core.Effect) (core.Resolution, error) {
			queries.Add(1)
			return core.Resolution{Kind: core.ResolvedCommitted}, nil
		}))

	f.seedEffect(t, "charge", core.ClassQueryable, core.EffectRunning)
	// No clock advance: the effect was touched a moment ago.

	if n := f.reconcile(t); n != 0 {
		t.Fatalf("settled %d fresh effects, want 0", n)
	}
	if got := queries.Load(); got != 0 {
		t.Fatalf("the provider was asked about %d in-flight requests", got)
	}
}

// TestReconcilerMarksAbandonedRunningAsUnknown checks the bookkeeping. A
// RUNNING row with no live owner is asserting something false, and the ledger
// should say what is true even when it cannot say what happened.
func TestReconcilerMarksAbandonedRunningAsUnknown(t *testing.T) {
	f := newFixture(t)
	f.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		failingHandler, nil)

	key := f.seedEffect(t, "charge", core.ClassIdempotentByKey, core.EffectRunning)
	f.clock.Advance(2 * time.Minute)

	if _, err := f.reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if got := f.effect(t, key); got.Status != core.EffectUnknown {
		t.Fatalf("status = %s, want UNKNOWN: nobody is working on it any more", got.Status)
	}
}

// --- fixture --------------------------------------------------------------

type fixture struct {
	clock      *clock.Virtual
	store      core.Store
	tools      *tool.Registry
	reconciler *effects.Reconciler
	seq        int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	clk := clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New(clk)
	tools := tool.New()

	reconciler, err := effects.NewReconciler(effects.ReconcilerConfig{
		Store:      store,
		Tools:      tools,
		Clock:      clk,
		StaleAfter: time.Minute,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	f := &fixture{clock: clk, store: store, tools: tools, reconciler: reconciler}
	t.Cleanup(func() { _ = store.Close() })
	return f
}

// seedEffect creates a run, a task, and an effect already in the given state —
// the wreckage a crashed worker leaves behind.
func (f *fixture) seedEffect(t *testing.T, toolName string, class core.EffectClass, status core.EffectStatus) core.IdempotencyKey {
	t.Helper()

	f.seq++
	runID := core.RunID("run-" + toolName)
	taskID := core.TaskID("task-" + toolName)
	key := core.NewIdempotencyKey(runID, core.Step(f.seq), 1)

	err := f.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateRun(ctx, core.Run{
			ID: runID, AgentName: "fixture", AgentVersion: "v1", Status: core.RunRunning,
		}); err != nil && !errors.Is(err, core.ErrConflict) {
			return err
		}
		if err := tx.CreateTask(ctx, core.Task{
			ID: taskID, RunID: runID, StepID: core.Step(f.seq), Type: toolName,
			Payload: json.RawMessage(`{}`), Status: core.TaskPending, MaxAttempts: 3,
		}); err != nil && !errors.Is(err, core.ErrTaskExists) {
			return err
		}
		if err := tx.ReserveEffect(ctx, core.Effect{
			ID: core.EffectID("eff-" + toolName), TaskID: taskID, RunID: runID,
			Type: toolName, Class: class, Key: key, Status: core.EffectPending,
		}); err != nil {
			return err
		}
		if err := tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{}); err != nil {
			return err
		}
		if status == core.EffectRunning {
			return nil
		}
		return tx.TransitionEffect(ctx, key, core.EffectRunning, status, core.EffectOutcome{})
	})
	if err != nil {
		t.Fatalf("seed effect: %v", err)
	}
	return key
}

func (f *fixture) reconcile(t *testing.T) int {
	t.Helper()
	n, err := f.reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	return n
}

func (f *fixture) effect(t *testing.T, key core.IdempotencyKey) core.Effect {
	t.Helper()
	e, err := f.store.GetEffect(context.Background(), key)
	if err != nil {
		t.Fatalf("GetEffect: %v", err)
	}
	return e
}

func failingHandler(context.Context, core.ToolCall) ([]byte, error) {
	return nil, errors.New("this handler must not be called by the background sweep")
}

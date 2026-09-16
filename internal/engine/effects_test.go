package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/decider"
)

// Layer 2's reason for existing, end to end.
//
// Layer 1 proved a task executes once when everything behaves. These prove the
// external action happens once when things do not: duplicate delivery, an
// answer that never arrives, a provider that cannot be asked.

// TestSideEffectHappensOnceUnderDuplicateDelivery is the headline.
//
// Fifty deliveries of one task across four workers, and the provider is called
// once. Layer 1 achieved this through the claim transition alone; here the
// ledger is what carries it, which matters because the claim can be bypassed
// by any failure that leaves a task reclaimable while a request is in flight.
func TestSideEffectHappensOnceUnderDuplicateDelivery(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "send_email", Payload: json.RawMessage(`{"to":"ana@example.com"}`)},
	))

	var calls atomic.Int32
	release := make(chan struct{})
	h.tools.Effectful("send_email", core.ClassIdempotentByKey, 0,
		func(ctx context.Context, _ []byte) ([]byte, error) {
			calls.Add(1)
			select {
			case <-release: // hold the provider call open so redeliveries land mid-flight
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return json.RawMessage(`{"reference":"msg_98374"}`), nil
		}, nil)

	h.startWorkers(t, 4)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	taskID := h.awaitTask(t, runID)

	for i := 0; i < 50; i++ {
		if err := h.dispatcher.Publish(context.Background(), taskID); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	close(release)

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider called %d times despite 51 deliveries; want exactly 1", got)
	}

	effects := h.effects(t, runID)
	if len(effects) != 1 {
		t.Fatalf("got %d ledger rows, want 1: one logical action is one row", len(effects))
	}
	if effects[0].Status != core.EffectCommitted {
		t.Fatalf("effect status = %s, want COMMITTED", effects[0].Status)
	}
	if effects[0].ExternalRef != "msg_98374" {
		t.Fatalf("external ref = %q, want the provider's own identifier", effects[0].ExternalRef)
	}
}

// TestPureReadsBypassTheLedger keeps the fast path honest. A read that happens
// twice costs latency, not correctness, so it must not pay for two extra
// transactions per call.
func TestPureReadsBypassTheLedger(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "lookup", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("lookup") // registered via Func, so ClassNone
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run := h.awaitTerminal(t, runID); run.Status != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED", run.Status)
	}

	if effects := h.effects(t, runID); len(effects) != 0 {
		t.Fatalf("a NONE-class tool wrote %d ledger rows, want 0", len(effects))
	}
}

// TestAmbiguousFailureIsUnknownThenResent covers the case the project is named
// for: a request goes out and no answer comes back.
//
// The effect must land in UNKNOWN, never FAILED, and the recovery must be a
// re-send under the same key rather than a fresh attempt — which is only safe
// because the tool declared that its provider deduplicates.
func TestAmbiguousFailureIsUnknownThenResent(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "charge", Payload: json.RawMessage(`{"amount":500}`)},
	))

	var calls atomic.Int32
	h.tools.Effectful("charge", core.ClassIdempotentByKey, 0,
		func(context.Context, []byte) ([]byte, error) {
			if calls.Add(1) == 1 {
				// The shape of a lost response: no information either way.
				return nil, errors.New("read tcp: connection reset by peer")
			}
			return json.RawMessage(`{"reference":"ch_1"}`), nil
		}, nil)
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}

	// The ledger must have admitted uncertainty rather than claiming failure.
	types := typesOf(h.history(t, runID))
	if !contains(types, core.EventEffectUnknown) {
		t.Fatalf("history = %v; an unexplained error must record EFFECT_UNKNOWN", types)
	}
	if contains(types, core.EventEffectFailed) {
		t.Fatalf("history = %v; an unexplained error must not be recorded as a definite failure", types)
	}

	effects := h.effects(t, runID)
	if len(effects) != 1 {
		t.Fatalf("got %d ledger rows, want 1: a re-send reuses the key", len(effects))
	}
	if effects[0].Status != core.EffectCommitted {
		t.Fatalf("effect status = %s, want COMMITTED after the re-send", effects[0].Status)
	}
}

// TestNotExecutedIsADefiniteFailure is the other half. A tool that can prove
// nothing was sent says so, and the runtime is then allowed to treat a retry
// as a first attempt.
func TestNotExecutedIsADefiniteFailure(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "charge", Payload: json.RawMessage(`{}`)},
	))

	var calls atomic.Int32
	h.tools.Effectful("charge", core.ClassIdempotentByKey, 0,
		func(context.Context, []byte) ([]byte, error) {
			if calls.Add(1) == 1 {
				return nil, core.NotExecuted(errors.New("amount is missing"))
			}
			return json.RawMessage(`{"reference":"ch_2"}`), nil
		}, nil)
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run := h.awaitTerminal(t, runID); run.Status != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED", run.Status)
	}

	types := typesOf(h.history(t, runID))
	if !contains(types, core.EventEffectFailed) {
		t.Fatalf("history = %v; a proven non-event must record EFFECT_FAILED", types)
	}
	if contains(types, core.EventEffectUnknown) {
		t.Fatalf("history = %v; a proven non-event must not be recorded as ambiguous", types)
	}
}

// TestQueryableEffectIsResolvedByAsking proves the middle class works: the
// provider will not deduplicate, but it will answer, and answering is enough.
//
// The provider is called once. The second attempt does not re-send — it asks,
// learns the action already happened, and records that.
func TestQueryableEffectIsResolvedByAsking(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "send_invoice", Payload: json.RawMessage(`{"customer":7}`)},
	))

	var (
		calls   atomic.Int32
		queries atomic.Int32
	)
	reconciler := core.ReconcilerFunc(func(_ context.Context, e core.Effect) (core.Resolution, error) {
		queries.Add(1)
		// The provider did receive it; only the response was lost.
		return core.Resolution{
			Kind:        core.ResolvedCommitted,
			Response:    json.RawMessage(`{"reference":"inv_1"}`),
			ExternalRef: "inv_1",
			Detail:      "found by idempotency key",
		}, nil
	})

	h.tools.Effectful("send_invoice", core.ClassQueryable, 0,
		func(context.Context, []byte) ([]byte, error) {
			calls.Add(1)
			return nil, errors.New("timeout waiting for response")
		}, reconciler)
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider called %d times; a QUERYABLE effect is resolved by asking, not re-sending", got)
	}
	if got := queries.Load(); got == 0 {
		t.Fatal("the reconciler was never consulted")
	}

	types := typesOf(h.history(t, runID))
	if !contains(types, core.EventEffectReconciled) {
		t.Fatalf("history = %v; resolving by lookup must record EFFECT_RECONCILED", types)
	}

	effects := h.effects(t, runID)
	if effects[0].Status != core.EffectCommitted || effects[0].ExternalRef != "inv_1" {
		t.Fatalf("effect = %s ref %q, want COMMITTED ref inv_1",
			effects[0].Status, effects[0].ExternalRef)
	}
}

// TestUnreconcilableEffectEscalates is the honest failure.
//
// A provider that neither deduplicates nor answers cannot be made exactly-once
// by any runtime. The requirement is not that Veya solve it — it cannot — but
// that it refuse to guess, stop, and leave a record a person can act on.
func TestUnreconcilableEffectEscalates(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "fire_and_forget", Payload: json.RawMessage(`{}`)},
	))

	var calls atomic.Int32
	h.tools.Effectful("fire_and_forget", core.ClassUnreconcilable, 0,
		func(context.Context, []byte) ([]byte, error) {
			calls.Add(1)
			return nil, errors.New("no response")
		}, nil)
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED", run.Status)
	}
	if !strings.Contains(run.LastError, "human resolution") {
		t.Fatalf("run error = %q; it must say a person has to decide", run.LastError)
	}

	// The critical assertion: it stopped rather than trying again. Retrying an
	// unresolvable ambiguous action is the coin flip this design exists to
	// refuse.
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider called %d times; an unresolvable outcome must never be retried", got)
	}

	types := typesOf(h.history(t, runID))
	if !contains(types, core.EventEffectEscalated) {
		t.Fatalf("history = %v; escalation must be recorded so it is visible", types)
	}

	// And the ledger keeps saying "we do not know", because we do not.
	effects := h.effects(t, runID)
	if effects[0].Status != core.EffectUnknown {
		t.Fatalf("effect status = %s, want UNKNOWN: the outcome is genuinely unresolved",
			effects[0].Status)
	}
}

// TestExpiredProviderKeyEscalates covers the limit that lives in someone
// else's system. Past the provider's key retention, a lookup that says "not
// found" means they forgot, not that nothing happened — so reconciliation
// stops rather than concluding it is safe to re-send.
func TestExpiredProviderKeyEscalates(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "charge", Payload: json.RawMessage(`{}`)},
	))

	var queries atomic.Int32
	reconciler := core.ReconcilerFunc(func(context.Context, core.Effect) (core.Resolution, error) {
		queries.Add(1)
		return core.Resolution{Kind: core.ResolvedNotExecuted}, nil
	})

	// The provider honours keys for a minute. The call itself takes an hour of
	// wall time — a stalled request, a queue backed up — so by the time anyone
	// reconciles, the key is long forgotten.
	h.tools.Effectful("charge", core.ClassQueryable, time.Minute,
		func(context.Context, []byte) ([]byte, error) {
			h.clock.Advance(time.Hour)
			return nil, errors.New("timeout")
		}, reconciler)
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED", run.Status)
	}
	if got := queries.Load(); got != 0 {
		t.Fatalf("reconciler consulted %d times past the key TTL; its answer "+
			"cannot be trusted there and must not be acted on", got)
	}
	if !contains(typesOf(h.history(t, runID)), core.EventEffectEscalated) {
		t.Fatal("an expired provider key must escalate, not resolve")
	}
}

// --- helpers --------------------------------------------------------------

// awaitTask waits for the run's first task to exist and returns its id.
func (h *harness) awaitTask(t *testing.T, runID core.RunID) core.TaskID {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tasks, err := h.engine.Tasks(context.Background(), runID)
		if err != nil {
			t.Fatalf("Tasks: %v", err)
		}
		if len(tasks) > 0 {
			return tasks[0].ID
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("run %s never produced a task", runID)
	return ""
}

func (h *harness) effects(t *testing.T, runID core.RunID) []core.Effect {
	t.Helper()
	effects, err := h.engine.Effects(context.Background(), runID)
	if err != nil {
		t.Fatalf("Effects: %v", err)
	}
	return effects
}

func contains(types []core.EventType, want core.EventType) bool {
	for _, got := range types {
		if got == want {
			return true
		}
	}
	return false
}

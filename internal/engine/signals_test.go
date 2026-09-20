package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// waitingAgent parks on a named signal and then completes, reporting what it
// saw. It is the shape every test in this file drives.
func waitingAgent(name string, deadline time.Time) deciderFunc {
	return func(_ core.Run, history []core.Event) (core.Decision, error) {
		if hasEvent(history, core.EventSignalReceived) {
			return core.Decision{
				Kind:   core.DecideComplete,
				Output: json.RawMessage(`{"got":"signal"}`),
			}, nil
		}
		if hasEvent(history, core.EventSignalWaitTimedOut) {
			return core.Decision{Kind: core.DecideFail, Error: "nobody approved it"}, nil
		}
		return core.Decision{
			Kind:   core.DecideWaitForSignal,
			StepID: core.Step(1),
			Signal: core.SignalWait{Name: name, Deadline: deadline},
		}, nil
	}
}

// TestASignalWakesAWaitingRun is the ordinary case: the run gets there first.
func TestASignalWakesAWaitingRun(t *testing.T) {
	h := newHarness(t, waitingAgent("approval", time.Time{}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// Parked with no scheduled wake-up. A deadline nobody asked for would be
	// worse than waiting: the run would resume on its own and report a
	// timeout that never happened.
	run := h.run(t, runID)
	if run.AvailableAt == nil || !core.IsIndefinite(*run.AvailableAt) {
		t.Fatalf("AvailableAt = %v, want an indefinite park", run.AvailableAt)
	}

	// Time passing changes nothing. Only an arrival ends this wait.
	h.clock.Advance(365 * 24 * time.Hour)
	h.runtime.ScanOnce(ctx)
	if got := h.run(t, runID).Status; got != core.RunRunning {
		t.Fatalf("status = %s after a year with no signal, want RUNNING", got)
	}

	if err := h.engine.Signal(ctx, core.Signal{
		RunID: runID, ID: "cb-1", Name: "approval",
		Payload: json.RawMessage(`{"by":"ops"}`),
	}); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	run = h.run(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventSignalWaitStarted,
		core.EventSignalReceived,
		core.EventRunCompleted,
	})
}

// TestASignalUnderAnotherNameLeavesTheRunWaiting.
//
// Delivery un-parks the run so that it can look, and looking is what decides.
// A release is not a satisfaction: if the run resumed on any arrival, a
// "cancellation" callback would satisfy a wait for "approval", and the body
// would carry on as though it had been approved.
func TestASignalUnderAnotherNameLeavesTheRunWaiting(t *testing.T) {
	h := newHarness(t, waitingAgent("approval", time.Time{}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if err := h.engine.Signal(ctx, core.Signal{
		RunID: runID, ID: "cb-other", Name: "cancellation",
	}); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	run := h.run(t, runID)
	if run.Status != core.RunRunning {
		t.Fatalf("status = %s; a signal under another name satisfied the wait", run.Status)
	}
	// And it is parked again, or the scan would pick it up on every pass to
	// ask a decider that can only answer "still waiting".
	if run.AvailableAt == nil {
		t.Fatal("the run was left un-parked with its wait still open")
	}
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventSignalWaitStarted,
	})

	// The right one still works afterwards.
	if err := h.engine.Signal(ctx, core.Signal{
		RunID: runID, ID: "cb-right", Name: "approval",
	}); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	if got := h.run(t, runID).Status; got != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED", got)
	}
}

// TestARetriedDeliveryApprovesOnce. Senders retry. The second arrival under
// the same id is the same signal, and must not become a second approval.
//
// The body waits again on a name nothing will ever send, so the run is still
// RUNNING for the retries -- otherwise the first delivery finishes the run and
// the later ones are refused for a reason that has nothing to do with dedup.
func TestARetriedDeliveryApprovesOnce(t *testing.T) {
	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		step, name := core.Step(1), "approval"
		if hasEvent(history, core.EventSignalReceived) {
			step, name = core.Step(2), "never"
		}
		return core.Decision{
			Kind:   core.DecideWaitForSignal,
			StepID: step,
			Signal: core.SignalWait{Name: name},
		}, nil
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	sig := core.Signal{RunID: runID, ID: "cb-1", Name: "approval"}
	for i := 0; i < 3; i++ {
		if err := h.engine.Signal(ctx, sig); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}

	if got := countEvents(h.history(t, runID), core.EventSignalReceived); got != 1 {
		t.Fatalf("three deliveries of one signal produced %d SIGNAL_RECEIVED events, want 1", got)
	}
	if got := h.run(t, runID).Status; got != core.RunRunning {
		t.Fatalf("status = %s, want RUNNING on the second wait", got)
	}
}

// TestADeadlineEndsAWaitAsAFact.
//
// The body cannot look at a clock, so it cannot work out for itself that its
// deadline passed. If the engine merely resumed it, the body would replay,
// find no signal, and park again -- forever. The timeout has to be recorded.
func TestADeadlineEndsAWaitAsAFact(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	deadline := start.Add(48 * time.Hour)

	h := newHarness(t, waitingAgent("approval", deadline))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.run(t, runID)
	if run.AvailableAt == nil || !run.AvailableAt.Equal(deadline) {
		t.Fatalf("AvailableAt = %v, want the deadline %s", run.AvailableAt, deadline)
	}

	h.clock.Advance(47 * time.Hour)
	h.runtime.ScanOnce(ctx)
	if got := h.run(t, runID).Status; got != core.RunRunning {
		t.Fatalf("status = %s an hour before the deadline, want RUNNING", got)
	}

	h.clock.Advance(2 * time.Hour)
	h.runtime.ScanOnce(ctx)

	run = h.run(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED after the deadline", run.Status)
	}
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventSignalWaitStarted,
		core.EventSignalWaitTimedOut,
		core.EventRunFailed,
	})
}

// TestASignalForAFinishedRunIsRefused. Storing it would tell the sender its
// callback landed somewhere that mattered, when nothing will ever read it.
func TestASignalForAFinishedRunIsRefused(t *testing.T) {
	h := newHarness(t, deciderFunc(func(core.Run, []core.Event) (core.Decision, error) {
		return core.Decision{Kind: core.DecideComplete}, nil
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if got := h.run(t, runID).Status; got != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED", got)
	}

	err = h.engine.Signal(ctx, core.Signal{RunID: runID, ID: "cb-late", Name: "approval"})
	if !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("signalling a finished run = %v, want a refusal", err)
	}
}

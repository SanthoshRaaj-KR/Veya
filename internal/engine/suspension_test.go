package engine_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/inproc"
)

// TestSleepParksARunInsteadOfAdvancingIt is the headline of the timer work.
//
// The run goes to sleep for a day, and for that whole day it is RUNNING with
// nothing in flight — which before this layer was the definition of a run owed
// a decision. The recovery scan has to leave it alone, and the decider has to
// not be asked again, or "sleep" means "spin".
func TestSleepParksARunInsteadOfAdvancingIt(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wake := start.Add(24 * time.Hour)

	var decisions atomic.Int64
	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		decisions.Add(1)
		if !hasEvent(history, core.EventTimerFired) {
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(1), WakeAt: wake}, nil
		}
		return core.Decision{Kind: core.DecideComplete, Output: json.RawMessage(`{"slept":true}`)}, nil
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// Parked: still RUNNING, with the wake instant on the row.
	run := h.run(t, runID)
	if run.Status != core.RunRunning {
		t.Fatalf("status = %s, want RUNNING; a waiting run is still running", run.Status)
	}
	if run.AvailableAt == nil || !run.AvailableAt.Equal(wake) {
		t.Fatalf("AvailableAt = %v, want %s", run.AvailableAt, wake)
	}
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTimerSet,
	})

	// The scan must not pick it up, and poking it directly must not reach the
	// decider. Both are the same bug seen from two directions: a sleeping run
	// that is asked what happens next answers "sleep", forever, as fast as it
	// can be asked.
	before := decisions.Load()
	awaiting, err := h.store.RunsAwaitingAdvance(ctx, h.clock.Now(), 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(awaiting) != 0 {
		t.Fatalf("a sleeping run is owed a decision: %v", awaiting)
	}
	for i := 0; i < 3; i++ {
		if err := h.engine.Advance(ctx, runID); err != nil {
			t.Fatalf("Advance while parked: %v", err)
		}
	}
	if got := decisions.Load(); got != before {
		t.Fatalf("the decider was asked %d more times while the run slept", got-before)
	}

	// A day passes.
	h.clock.Advance(24 * time.Hour)
	if err := h.engine.Advance(ctx, runID); err != nil {
		t.Fatalf("Advance after the wake-up: %v", err)
	}

	run = h.run(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	if run.AvailableAt != nil {
		t.Fatalf("AvailableAt = %v after waking, want nil", run.AvailableAt)
	}
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTimerSet,
		core.EventTimerFired,
		core.EventRunCompleted,
	})
}

// TestAWakeUpThatPassedWhileNobodyWasLookingStillFires.
//
// The scan is the timer wheel, so there is no wheel to miss a tick. A run
// whose instant went by while the process was down is simply a run the scan is
// now allowed to pick up, and it is picked up by the ordinary path with no
// catch-up pass — which is what stops an outage from swallowing every timer
// that expired during it.
func TestAWakeUpThatPassedWhileNobodyWasLookingStillFires(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wake := start.Add(time.Hour)

	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		if !hasEvent(history, core.EventTimerFired) {
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(1), WakeAt: wake}, nil
		}
		return core.Decision{Kind: core.DecideComplete}, nil
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// A week goes by with nothing running. The wake-up is long overdue.
	h.clock.Advance(7 * 24 * time.Hour)

	h.runtime.ScanOnce(ctx)

	run := h.run(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED; an overdue wake-up must be ready, not lost", run.Status)
	}
}

// TestSleepingTwiceIsTwoSeparateWaits. The second sleep must not be closed by
// the first one's TIMER_FIRED, or the run resumes immediately and the payload
// of whatever it does next was computed at the wrong time.
func TestSleepingTwiceIsTwoSeparateWaits(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first := start.Add(time.Hour)
	second := start.Add(48 * time.Hour)

	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		switch countEvents(history, core.EventTimerFired) {
		case 0:
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(1), WakeAt: first}, nil
		case 1:
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(2), WakeAt: second}, nil
		default:
			return core.Decision{Kind: core.DecideComplete}, nil
		}
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	h.clock.Advance(time.Hour)
	if err := h.engine.Advance(ctx, runID); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	// Now parked on the second sleep, which is 47 hours away.
	run := h.run(t, runID)
	if run.Status != core.RunRunning {
		t.Fatalf("status = %s, want RUNNING between the two sleeps", run.Status)
	}
	if run.AvailableAt == nil || !run.AvailableAt.Equal(second) {
		t.Fatalf("AvailableAt = %v, want the second wake %s", run.AvailableAt, second)
	}

	if err := h.engine.Advance(ctx, runID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := h.run(t, runID).Status; got != core.RunRunning {
		t.Fatalf("status = %s; the first timer's fire released the second sleep", got)
	}

	h.clock.Advance(47 * time.Hour)
	if err := h.engine.Advance(ctx, runID); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := h.run(t, runID).Status; got != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED", got)
	}

	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTimerSet, core.EventTimerFired,
		core.EventTimerSet, core.EventTimerFired,
		core.EventRunCompleted,
	})
}

func hasEvent(history []core.Event, typ core.EventType) bool {
	return countEvents(history, typ) > 0
}

func countEvents(history []core.Event, typ core.EventType) int {
	n := 0
	for _, e := range history {
		if e.Type == typ {
			n++
		}
	}
	return n
}

func (h *harness) run(t *testing.T, id core.RunID) core.Run {
	t.Helper()
	run, err := h.engine.Run(context.Background(), id)
	if err != nil {
		t.Fatalf("Run(%s): %v", id, err)
	}
	return run
}

// TestASleepingRunSurvivesARestart is the exit criterion for durable timers.
//
// The whole claim of a durable timer is that it is not a timer: nothing is
// waiting in memory, so there is nothing for a process to take with it when it
// dies. This test kills everything above the store while a run is a day into
// its sleep, builds a second runtime on top of the same data, and expects the
// run to wake on schedule under a runtime that has never heard of it.
//
// A pending wake-up held in a goroutine passes every test that does not do
// this, and fails the first deploy.
func TestASleepingRunSurvivesARestart(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wake := start.Add(24 * time.Hour)

	agent := deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		if !hasEvent(history, core.EventTimerFired) {
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(1), WakeAt: wake}, nil
		}
		return core.Decision{Kind: core.DecideComplete, Output: json.RawMessage(`{"woke":true}`)}, nil
	})

	first := newHarness(t, agent)
	first.start(t)

	ctx := context.Background()
	runID, err := first.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if got := first.run(t, runID); got.AvailableAt == nil {
		t.Fatal("the run did not park")
	}

	// The process dies. Workers, relay, recovery loop, engine: all gone. The
	// store is the database and stays.
	clk, store := first.clock, first.store
	first.crash()

	// Hours pass with nothing running, and the wake-up goes by unattended.
	clk.Advance(30 * time.Hour)

	// A new process comes up. It has never seen this run.
	second := newHarnessOn(t, agent, inproc.New(64), clk, store)
	second.start(t)

	second.runtime.ScanOnce(ctx)

	run := second.run(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED after the restart", run.Status, run.LastError)
	}
	if run.AvailableAt != nil {
		t.Fatalf("AvailableAt = %v after waking, want nil", run.AvailableAt)
	}
	assertHistory(t, second.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTimerSet,
		core.EventTimerFired,
		core.EventRunCompleted,
	})
}

// TestARestartMidSleepDoesNotWakeTheRunEarly is the other half.
//
// Recovering a run must not mean resuming it. A restarted runtime rediscovers
// every RUNNING run, and the naive recovery — advance anything that is RUNNING
// with nothing in flight — would cut short every sleep in the system on every
// deploy, which is worse than losing the timers because the runs would carry
// on as though the wait had happened.
func TestARestartMidSleepDoesNotWakeTheRunEarly(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wake := start.Add(24 * time.Hour)

	agent := deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		if !hasEvent(history, core.EventTimerFired) {
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(1), WakeAt: wake}, nil
		}
		return core.Decision{Kind: core.DecideComplete}, nil
	})

	first := newHarness(t, agent)
	first.start(t)

	ctx := context.Background()
	runID, err := first.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	clk, store := first.clock, first.store
	first.crash()

	// One hour in. Twenty-three still to go.
	clk.Advance(time.Hour)

	second := newHarnessOn(t, agent, inproc.New(64), clk, store)
	second.start(t)
	for i := 0; i < 5; i++ {
		second.runtime.ScanOnce(ctx)
	}

	run := second.run(t, runID)
	if run.Status != core.RunRunning {
		t.Fatalf("status = %s, want RUNNING; the restart cut the sleep short", run.Status)
	}
	if run.AvailableAt == nil || !run.AvailableAt.Equal(wake) {
		t.Fatalf("AvailableAt = %v, want the original wake %s", run.AvailableAt, wake)
	}
	assertHistory(t, second.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTimerSet,
	})
}

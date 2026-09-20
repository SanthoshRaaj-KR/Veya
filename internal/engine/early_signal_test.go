package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The early signal is the bug this whole mechanism exists to delete.
//
// An external system calls back faster than the run reaches its wait. A design
// that delivers signals to waiters finds no waiter, drops the callback, and the
// run then waits forever for something that already happened. It is a race, so
// it passes every test written by someone who has not thought about it, and it
// fails in production under load — usually on the day the provider gets faster.
//
// Storing on arrival removes the race rather than narrowing it: a wait is a
// read, so early and late arrival run the same code. These tests are the
// evidence, and they are deterministic rather than timing-dependent, because a
// test that only sometimes exercises the early case is a test that reports the
// bug is gone before it is.

// TestASignalThatArrivesBeforeItsWaitIsNotLost is the headline.
//
// The callback lands while step 1 is still in flight — held there on purpose,
// so this is the early ordering every time and not merely usually. By the time
// the body asks to wait, the signal is long past, and the run proceeds on the
// first pass with nothing having to wake it.
func TestASignalThatArrivesBeforeItsWaitIsNotLost(t *testing.T) {
	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		switch {
		case !hasEvent(history, core.EventTaskCompleted):
			return core.Decision{
				Kind: core.DecideCallTool, StepID: core.Step(1),
				TaskType: "prepare", Payload: json.RawMessage(`{}`),
			}, nil
		case hasEvent(history, core.EventSignalReceived):
			return core.Decision{Kind: core.DecideComplete}, nil
		default:
			return core.Decision{
				Kind: core.DecideWaitForSignal, StepID: core.Step(2),
				Signal: core.SignalWait{Name: "approval"},
			}, nil
		}
	}))

	// The tool blocks until the test lets it go, which pins the ordering: the
	// signal is recorded while the run is provably nowhere near its wait.
	running, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.tools.Func("prepare", func(ctx context.Context, _ core.ToolCall) ([]byte, error) {
		once.Do(func() { close(running) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.RawMessage(`{"ready":true}`), nil
	})
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	<-running
	if err := h.engine.Signal(ctx, core.Signal{
		RunID: runID, ID: "cb-early", Name: "approval",
		Payload: json.RawMessage(`{"by":"ops"}`),
	}); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	close(release)

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED; the early signal was lost",
			run.Status, run.LastError)
	}

	// Consumed by the wait, not merely sitting in the table.
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTaskCreated, core.EventTaskClaimed, core.EventTaskCompleted,
		core.EventSignalWaitStarted,
		core.EventSignalReceived,
		core.EventRunCompleted,
	})
}

// TestEarlyAndLateArrivalProduceTheSameHistory.
//
// The claim is not that the early case works. It is that there is no early
// case: both orderings run identical code, so the ordering that is hard to
// reproduce is exercised by every test of the other. If these two histories
// ever diverge, something has grown a delivery path, and the race is back.
func TestEarlyAndLateArrivalProduceTheSameHistory(t *testing.T) {
	// Late: the run parks first, then the callback arrives.
	late := newHarness(t, waitingAgent("approval", zeroTime))
	late.start(t)
	ctx := context.Background()

	lateRun, err := late.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if late.run(t, lateRun).AvailableAt == nil {
		t.Fatal("the late run did not park before the signal")
	}
	if err := late.engine.Signal(ctx, core.Signal{
		RunID: lateRun, ID: "cb-1", Name: "approval",
	}); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	// Early: the callback is recorded against the run before the body has ever
	// been asked what happens next, through the same store-level write a
	// caller with no engine would make.
	early := newHarness(t, waitingAgent("approval", zeroTime))
	early.start(t)

	earlyRun := early.createRunWithoutDeciding(t, "early-arrival")
	if err := early.engine.Signal(ctx, core.Signal{
		RunID: earlyRun, ID: "cb-1", Name: "approval",
	}); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	lateHistory := typesOf(late.history(t, lateRun))
	earlyHistory := typesOf(early.history(t, earlyRun))

	if fmt.Sprint(lateHistory) != fmt.Sprint(earlyHistory) {
		t.Fatalf("early arrival produced %v\nlate arrival produced  %v\n"+
			"the two orderings must be indistinguishable", earlyHistory, lateHistory)
	}
	if late.run(t, lateRun).Status != core.RunCompleted ||
		early.run(t, earlyRun).Status != core.RunCompleted {
		t.Fatal("both orderings must complete")
	}
}

// TestSeveralEarlySignalsAreConsumedOneAtATime.
//
// Three callbacks land under the same name before the run waits for any of
// them. Each wait takes exactly one, in arrival order, and no wait takes one an
// earlier wait already used — the property that stops a run approving three
// things because the provider sent three callbacks.
func TestSeveralEarlySignalsAreConsumedOneAtATime(t *testing.T) {
	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		taken := countEvents(history, core.EventSignalReceived)
		if taken >= 3 {
			return core.Decision{Kind: core.DecideComplete}, nil
		}
		return core.Decision{
			Kind: core.DecideWaitForSignal, StepID: core.Step(taken + 1),
			Signal: core.SignalWait{Name: "tick"},
		}, nil
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := h.engine.Signal(ctx, core.Signal{
			RunID: runID, ID: core.SignalID(fmt.Sprintf("cb-%d", i)), Name: "tick",
			Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("Signal %d: %v", i, err)
		}
	}

	if got := h.run(t, runID).Status; got != core.RunCompleted {
		t.Fatalf("status = %s, want COMPLETED after three signals", got)
	}

	ids := consumedSignalIDs(t, h.history(t, runID))
	want := []core.SignalID{"cb-0", "cb-1", "cb-2"}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("consumed %v, want %v; each wait takes the oldest it has not taken", ids, want)
	}
}

// TestDeliveriesRacingTheWaitAreAllAccountedFor.
//
// Ten senders, each retrying, against a run that is advancing at the same
// time, on whatever interleaving the scheduler picks. Whichever order they land
// in, each id is consumed exactly once and none is dropped: the two properties
// that a delivery-based design trades against each other and this one does not.
func TestDeliveriesRacingTheWaitAreAllAccountedFor(t *testing.T) {
	const senders = 10

	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		taken := countEvents(history, core.EventSignalReceived)
		if taken >= senders {
			return core.Decision{Kind: core.DecideComplete}, nil
		}
		return core.Decision{
			Kind: core.DecideWaitForSignal, StepID: core.Step(taken + 1),
			Signal: core.SignalWait{Name: "tick"},
		}, nil
	}))
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each sender retries, because that is what senders do.
			for try := 0; try < 2; try++ {
				_ = h.engine.Signal(ctx, core.Signal{
					RunID: runID, ID: core.SignalID(fmt.Sprintf("cb-%d", i)), Name: "tick",
				})
			}
		}(i)
	}
	wg.Wait()

	// Advancement may have been lost to a compare-and-swap at any point, which
	// is expected and is what the scan is for.
	for i := 0; i < senders+2; i++ {
		h.runtime.ScanOnce(ctx)
	}

	run := h.run(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED; %d of %d signals were consumed",
			run.Status, run.LastError,
			countEvents(h.history(t, runID), core.EventSignalReceived), senders)
	}

	seen := map[core.SignalID]int{}
	for _, id := range consumedSignalIDs(t, h.history(t, runID)) {
		seen[id]++
	}
	if len(seen) != senders {
		t.Fatalf("consumed %d distinct signals, want %d", len(seen), senders)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("signal %s was consumed %d times", id, n)
		}
	}
}

func consumedSignalIDs(t *testing.T, history []core.Event) []core.SignalID {
	t.Helper()

	var ids []core.SignalID
	for _, e := range history {
		if e.Type != core.EventSignalReceived {
			continue
		}
		var data core.SignalReceivedData
		if err := e.Decode(&data); err != nil {
			t.Fatalf("decode SIGNAL_RECEIVED at seq %d: %v", e.Seq, err)
		}
		ids = append(ids, data.SignalID)
	}
	return ids
}

// createRunWithoutDeciding writes a run the way `veya run start` does: the row
// and its RUN_STARTED event, and no decision.
//
// The CLI cannot decide -- it has no decider, and running one there would mean
// two processes advancing the same run -- so a run created this way sits until
// the runtime picks it up. It is the state a signal is most likely to arrive
// in, and there is no other way to reach it from a test without a sleep.
func (h *harness) createRunWithoutDeciding(t *testing.T, id core.RunID) core.RunID {
	t.Helper()

	err := h.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateRun(ctx, core.Run{
			ID:           id,
			AgentName:    testAgent,
			AgentVersion: "v1",
			Status:       core.RunRunning,
			Input:        json.RawMessage(`{}`),
		}); err != nil {
			return err
		}
		return core.Append(ctx, tx, id, core.EventRunStarted, "", core.RunStartedData{
			AgentName:    testAgent,
			AgentVersion: "v1",
			Input:        json.RawMessage(`{}`),
		})
	})
	if err != nil {
		t.Fatalf("create run %s: %v", id, err)
	}
	return id
}

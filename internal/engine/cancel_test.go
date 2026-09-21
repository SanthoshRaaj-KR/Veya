package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// TestDecideCancelEndsTheRunCancelled is CANCEL's headline case: a body that
// decides to stop ends the run in RunCancelled, with the reason it gave
// recorded on RUN_CANCELLED for an operator reading history later.
func TestDecideCancelEndsTheRunCancelled(t *testing.T) {
	h := newHarness(t, deciderFunc(func(core.Run, []core.Event) (core.Decision, error) {
		return core.Decision{Kind: core.DecideCancel, Reason: "the customer withdrew the request"}, nil
	}))
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCancelled {
		t.Fatalf("status = %s (%s), want CANCELLED", run.Status, run.LastError)
	}

	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventRunCancelled,
	})

	var recorded core.RunCancelledData
	for _, e := range h.history(t, runID) {
		if e.Type != core.EventRunCancelled {
			continue
		}
		if err := e.Decode(&recorded); err != nil {
			t.Fatalf("decode RUN_CANCELLED: %v", err)
		}
	}
	if recorded.Reason != "the customer withdrew the request" {
		t.Fatalf("recorded reason = %q", recorded.Reason)
	}
}

// TestCancelWithNoReasonIsLeftUnexplained. Unlike FAIL, which invents a
// reason when the client gave none, a CANCEL with no reason is recorded as
// such: cancelling for no stated reason is itself something a body can mean.
func TestCancelWithNoReasonIsLeftUnexplained(t *testing.T) {
	h := newHarness(t, deciderFunc(func(core.Run, []core.Event) (core.Decision, error) {
		return core.Decision{Kind: core.DecideCancel}, nil
	}))
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCancelled {
		t.Fatalf("status = %s, want CANCELLED", run.Status)
	}

	var recorded core.RunCancelledData
	for _, e := range h.history(t, runID) {
		if e.Type == core.EventRunCancelled {
			if err := e.Decode(&recorded); err != nil {
				t.Fatalf("decode RUN_CANCELLED: %v", err)
			}
		}
	}
	if recorded.Reason != "" {
		t.Fatalf("reason = %q, want empty", recorded.Reason)
	}
}

// TestACancelDoesNotStopAChildAlreadyInFlight.
//
// CANCEL is cooperative and forward-looking, exactly like the ANY-join case
// it shares its mechanism with (fanout_join_test.go): it prevents the *next*
// durable step, not an effect already in flight. A run cancelled the instant
// an ANY join is satisfied still has siblings running, and their completion
// still lands and is still recorded — a ledger with an orphan in it would be
// worse than a slow child. See docs/execution-model.md section 7.4.
func TestACancelDoesNotStopAChildAlreadyInFlight(t *testing.T) {
	const children = 3

	cancelOnceJoined := func(_ core.Run, history []core.Event) (core.Decision, error) {
		parent := core.Step(1)
		settled := 0
		for _, e := range history {
			if idx := indexOfChild(parent, e.StepID, children); idx >= 0 && e.Type == core.EventTaskCompleted {
				settled++
			}
		}
		if settled > 0 {
			return core.Decision{Kind: core.DecideCancel, Reason: "good enough"}, nil
		}
		calls := make([]core.Call, children)
		for i := range calls {
			calls[i] = core.Call{TaskType: "check", Payload: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))}
		}
		return core.Decision{
			Kind: core.DecideCallToolParallel, StepID: parent, Calls: calls,
			Join: core.JoinPolicy{Kind: core.JoinAny},
		}, nil
	}

	h := newHarness(t, deciderFunc(cancelOnceJoined))

	release := make(chan struct{})
	var first sync.Once
	firstDone := make(chan struct{})
	h.tools.Func("check", func(ctx context.Context, call core.ToolCall) ([]byte, error) {
		var payload struct {
			I int `json:"i"`
		}
		if err := json.Unmarshal(call.Payload, &payload); err != nil {
			return nil, err
		}
		if payload.I == 0 {
			first.Do(func() { close(firstDone) })
			return json.RawMessage(`{"i":0}`), nil
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.RawMessage(fmt.Sprintf(`{"i":%d}`, payload.I)), nil
	})
	h.startWorkers(t, 3)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	<-firstDone
	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCancelled {
		t.Fatalf("status = %s (%s), want CANCELLED on the first success", run.Status, run.LastError)
	}

	// The run is cancelled with two children still working. Let them land.
	close(release)
	h.awaitEventCount(t, runID, core.EventTaskCompleted, children)

	types := typesOf(h.history(t, runID))
	cancelledAt := -1
	for i, typ := range types {
		if typ == core.EventRunCancelled {
			cancelledAt = i
		}
	}
	if cancelledAt < 0 {
		t.Fatalf("no RUN_CANCELLED in %v", types)
	}
	late := 0
	for _, typ := range types[cancelledAt+1:] {
		if typ == core.EventTaskCompleted {
			late++
		}
	}
	if late == 0 {
		t.Fatalf("no child landed after the run was cancelled; the test did not "+
			"reach the state it exists for (%v)", types)
	}
}

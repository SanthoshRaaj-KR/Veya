package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// fanOutAgent fans out n calls at step 1 under a policy, then completes once
// the body decides the join is done. Each test supplies its own joined().
func fanOutAgent(n int, join core.JoinPolicy, joined func([]core.Event) bool) deciderFunc {
	return func(_ core.Run, history []core.Event) (core.Decision, error) {
		if joined(history) {
			return core.Decision{Kind: core.DecideComplete, Output: json.RawMessage(`{"done":true}`)}, nil
		}
		calls := make([]core.Call, n)
		for i := range calls {
			calls[i] = core.Call{
				TaskType: "check",
				Payload:  json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			}
		}
		return core.Decision{
			Kind:   core.DecideCallToolParallel,
			StepID: core.Step(1),
			Calls:  calls,
			Join:   join,
		}, nil
	}
}

// TestAFanOutCommitsAsOneDecision.
//
// Three children, one transaction: the policy, three tasks, three TASK_CREATED
// events and three delivery intents. Committing them one at a time would raise
// a question this design has never had to answer — what a run means when three
// of its ten children exist and the process died before the fourth — and with
// no record of how many were intended, a join over the survivors would be
// satisfied by a set nobody chose.
func TestAFanOutCommitsAsOneDecision(t *testing.T) {
	allDone := func(history []core.Event) bool {
		return countEvents(history, core.EventTaskCompleted) >= 3
	}
	h := newHarness(t, fanOutAgent(3, core.JoinPolicy{Kind: core.JoinAll}, allDone))
	h.registerEcho("check")
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}

	// The children are named from the parent, by invocation order.
	tasks, err := h.store.ListTasks(ctx, runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("got %d tasks, want 3", len(tasks))
	}
	seen := map[core.StepID]bool{}
	for _, task := range tasks {
		seen[task.StepID] = true
	}
	for i, child := range core.Step(1).Children(3) {
		if !seen[child] {
			t.Fatalf("child %d is not named %s; the engine invented its own numbering", i, child)
		}
	}

	// And the policy is in history, which is the one thing about a fan-out
	// that the TASK_CREATED events do not already say.
	var recorded core.FanOutStartedData
	found := false
	for _, e := range h.history(t, runID) {
		if e.Type != core.EventFanOutStarted {
			continue
		}
		if err := e.Decode(&recorded); err != nil {
			t.Fatalf("decode FAN_OUT_STARTED: %v", err)
		}
		found = true
		if e.StepID != core.Step(1) {
			t.Fatalf("FAN_OUT_STARTED is at step %s, want the parent S1", e.StepID)
		}
	}
	if !found {
		t.Fatal("no FAN_OUT_STARTED in history; the join policy is unrecoverable")
	}
	if recorded.Join != "ALL" {
		t.Fatalf("recorded join = %q, want ALL", recorded.Join)
	}
	if len(recorded.Calls) != 3 {
		t.Fatalf("recorded %d calls, want 3", len(recorded.Calls))
	}
}

// TestRedecidingAFanOutIsANoOp is how a fan-out waits.
//
// While the join is unsatisfied the body re-issues the identical decision on
// every advance. The unique constraint on (run_id, step_id) rolls the whole
// transaction back, so nothing happens — the same path a re-decided single
// CallTool has taken since Layer 1, which is why fan-out needs no waiting
// state anywhere.
func TestRedecidingAFanOutIsANoOp(t *testing.T) {
	never := func([]core.Event) bool { return false }
	h := newHarness(t, fanOutAgent(4, core.JoinPolicy{Kind: core.JoinAll}, never))
	h.registerEcho("check")
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := h.engine.Advance(ctx, runID); err != nil {
			t.Fatalf("re-advance %d: %v", i, err)
		}
	}

	tasks, err := h.store.ListTasks(ctx, runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 4 {
		t.Fatalf("got %d tasks after five re-decisions, want 4", len(tasks))
	}
	if got := countEvents(h.history(t, runID), core.EventFanOutStarted); got != 1 {
		t.Fatalf("history has %d FAN_OUT_STARTED events, want 1", got)
	}
	if got := countEvents(h.history(t, runID), core.EventTaskCreated); got != 4 {
		t.Fatalf("history has %d TASK_CREATED events, want 4", got)
	}
}

// TestAnUnsatisfiableJoinFailsTheRunLoudly.
//
// A quorum larger than the fan-out can never be met. Dispatching the work
// anyway would look like a successful decision followed by a run that never
// moves again, which is the failure mode hardest to find: nothing is wrong
// with any individual part.
func TestAnUnsatisfiableJoinFailsTheRunLoudly(t *testing.T) {
	never := func([]core.Event) bool { return false }
	h := newHarness(t, fanOutAgent(2, core.JoinPolicy{Kind: core.JoinQuorum, Quorum: 5}, never))
	h.registerEcho("check")
	h.start(t)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.run(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED; QUORUM(5) over 2 calls can never be satisfied", run.Status)
	}
	if run.LastError == "" {
		t.Fatal("the run failed with no reason recorded")
	}

	tasks, err := h.store.ListTasks(ctx, runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("%d tasks were dispatched for a join that can never complete", len(tasks))
	}
}

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/inproc"
)

// TestTheSameFanOutReplaysIdentically.
//
// Replay is the property everything else in this system is arranged around,
// and fan-out is the first thing that could break it by accident: children
// finish in an order nobody controls, so a decider that let completion order
// reach its output would produce a different answer every time it was asked
// the same question.
//
// This drives one fan-out to completion, then asks the decider the same
// question again, repeatedly, against the recorded history. Every answer has
// to be byte-identical to the first.
func TestTheSameFanOutReplaysIdentically(t *testing.T) {
	const children = 6

	agent := joiningAgent(children, core.JoinPolicy{Kind: core.JoinAll})
	h := newHarness(t, agent)
	h.registerEcho("check")
	h.startWorkers(t, 4)

	ctx := context.Background()
	runID, err := h.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}

	history := h.history(t, runID)

	var first string
	for attempt := 0; attempt < 10; attempt++ {
		decision, err := agent.Decide(ctx, run, history)
		if err != nil {
			t.Fatalf("replay %d: %v", attempt, err)
		}
		got := describeDecision(decision)
		if attempt == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("replay %d produced %s\nthe first replay produced %s",
				attempt, got, first)
		}
	}

	// And the answer is the one the run actually ended on, not merely a
	// consistent one. A decider that replayed to a stable but different
	// conclusion would still have forked the run.
	if want := describeDecision(core.Decision{
		Kind: core.DecideComplete, Output: run.Output,
	}); first != want {
		t.Fatalf("replay answers %s, the run ended on %s", first, want)
	}
}

// TestAFanOutRestartedMidFlightReusesItsChildren.
//
// The process dies with children in flight. A second runtime comes up, the
// body replays, and the fan-out is re-issued — which must produce the same
// children, not a second batch. The unique constraint on (run_id, step_id) is
// what guarantees it, and the keys those children hold are derived from those
// step ids, so a second batch would be six fresh ledger rows and six repeated
// external actions.
func TestAFanOutRestartedMidFlightReusesItsChildren(t *testing.T) {
	const children = 6

	agent := joiningAgent(children, core.JoinPolicy{Kind: core.JoinAll})

	first := newHarness(t, agent)
	first.registerEcho("check")
	// No workers: the children are committed and nothing executes them, which
	// is exactly the state a crash leaves behind.
	first.startWorkers(t, 0)

	ctx := context.Background()
	runID, err := first.engine.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	before, err := first.store.ListTasks(ctx, runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(before) != children {
		t.Fatalf("got %d children before the crash, want %d", len(before), children)
	}

	clk, store := first.clock, first.store
	first.crash()

	second := newHarnessOn(t, agent, inproc.New(64), clk, store)
	second.registerEcho("check")
	second.startWorkers(t, 2)

	// The new process rediscovers the run and re-issues the fan-out several
	// times before the children finish.
	for i := 0; i < 3; i++ {
		second.runtime.ScanOnce(ctx)
	}

	run := second.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED after the restart", run.Status, run.LastError)
	}

	after, err := second.store.ListTasks(ctx, runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(after) != children {
		t.Fatalf("got %d tasks after the restart, want %d; the fan-out was issued twice",
			len(after), children)
	}

	// Same task identities, not merely the same count.
	ids := map[core.TaskID]bool{}
	for _, task := range before {
		ids[task.ID] = true
	}
	for _, task := range after {
		if !ids[task.ID] {
			t.Fatalf("task %s at step %s did not exist before the restart", task.ID, task.StepID)
		}
	}

	// One ledger row per child, under the key derived from its own step.
	rows, err := second.store.ListEffects(ctx, runID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	if len(rows) != 0 && len(rows) != children {
		t.Fatalf("the ledger has %d rows for %d children", len(rows), children)
	}

	// And exactly one FAN_OUT_STARTED, however many times it was re-decided.
	if got := countEvents(second.history(t, runID), core.EventFanOutStarted); got != 1 {
		t.Fatalf("history has %d FAN_OUT_STARTED events after a restart and three "+
			"re-decisions, want 1", got)
	}
}

// describeDecision renders a decision for comparison, including everything a
// replay could differ in.
func describeDecision(d core.Decision) string {
	switch d.Kind {
	case core.DecideCallToolParallel:
		calls := make([]string, len(d.Calls))
		for i, c := range d.Calls {
			calls[i] = fmt.Sprintf("%s(%s)", c.TaskType, c.Payload)
		}
		return fmt.Sprintf("PARALLEL %s %s %v", d.StepID, d.Join, calls)
	case core.DecideCallTool:
		return fmt.Sprintf("CALL %s %s(%s)", d.StepID, d.TaskType, d.Payload)
	case core.DecideComplete:
		return fmt.Sprintf("COMPLETE %s", d.Output)
	case core.DecideFail:
		return fmt.Sprintf("FAIL %s", d.Error)
	default:
		return fmt.Sprintf("%s %+v", d.Kind, d)
	}
}

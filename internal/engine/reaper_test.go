package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/decider"
)

// Crash recovery, which is the part of the design that only ever runs when
// something has already gone wrong.
//
// A worker "dies" here by claiming a task and then never reporting — the same
// state a SIGKILL leaves behind, reached deterministically instead of by
// killing a process and hoping the timing lines up.

// TestReaperReclaimsAnAbandonedTask is the base case: work whose owner went
// silent goes back on the queue and finishes.
func TestReaperReclaimsAnAbandonedTask(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("work")

	runID, taskID := h.abandonedTask(t, "ghost-worker")

	// Nothing recovers it until the lease lapses.
	h.advanceClockPast(h.engine.LeaseTTL())
	reclaimed := h.reap(t)
	if reclaimed != 1 {
		t.Fatalf("reaper reclaimed %d tasks, want 1", reclaimed)
	}

	task := h.task(t, taskID)
	if task.Status != core.TaskPending {
		t.Fatalf("task status = %s, want PENDING so it can be picked up again", task.Status)
	}

	types := typesOf(h.history(t, runID))
	if !contains(types, core.EventLeaseExpired) {
		t.Fatalf("history = %v; reclaiming must be recorded", types)
	}

	// And with workers running, it completes.
	h.start(t)
	if err := h.dispatcher.Publish(context.Background(), taskID); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if run := h.awaitTerminal(t, runID); run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED after recovery", run.Status, run.LastError)
	}
}

// TestReaperFencesThePreviousOwner is the reason the reaper takes the lease
// rather than rewriting the task row.
//
// Acquiring issues a strictly higher token, so the abandoned worker — which
// may be alive and simply slow — is refused when it finally reports. Without
// that, a task could be reclaimed, re-executed, and then overwritten by a
// result from the first attempt.
func TestReaperFencesThePreviousOwner(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("work")

	runID, taskID := h.abandonedTask(t, "frozen-worker")
	stale := h.lease(t, taskID)

	h.advanceClockPast(h.engine.LeaseTTL())
	if n := h.reap(t); n != 1 {
		t.Fatalf("reaper reclaimed %d tasks, want 1", n)
	}

	// The frozen worker thaws and reports success under its old token.
	err := h.engine.CompleteTask(context.Background(), taskID, stale.Token,
		json.RawMessage(`{"stale":true}`))
	if err != nil {
		t.Fatalf("CompleteTask should absorb a fenced report, got: %v", err)
	}

	task := h.task(t, taskID)
	if task.Status != core.TaskPending {
		t.Fatalf("task status = %s; a fenced report must not complete the task", task.Status)
	}

	// Its heartbeat is refused too, which is how the worker learns to stop.
	if err := h.engine.Heartbeat(context.Background(), taskID, stale.Token); !errors.Is(err, core.ErrFenced) {
		t.Fatalf("stale heartbeat = %v, want ErrFenced", err)
	}

	_ = runID
}

// TestAbandonedEffectIsReconciledNotRerun is the scenario from README section
// 6.3, and the one the project exists for.
//
// The provider acts, then the worker dies before recording it. The ledger is
// left holding RUNNING, which is neither success nor failure. Recovery must
// find out what happened rather than assume, and here the provider can be
// asked — so the action is not repeated.
func TestAbandonedEffectIsReconciledNotRerun(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "send_money", Payload: json.RawMessage(`{"amount":500}`)},
	))

	var (
		sends   atomic.Int32
		queries atomic.Int32
	)
	h.tools.Effectful("send_money", core.ClassQueryable, time.Hour,
		func(context.Context, core.ToolCall) ([]byte, error) {
			sends.Add(1)
			return json.RawMessage(`{"reference":"pay_98374"}`), nil
		},
		core.ReconcilerFunc(func(context.Context, core.Effect) (core.Resolution, error) {
			queries.Add(1)
			// The provider did act; only the acknowledgement was lost.
			return core.Resolution{
				Kind:        core.ResolvedCommitted,
				Response:    json.RawMessage(`{"reference":"pay_98374"}`),
				ExternalRef: "pay_98374",
				Detail:      "found by idempotency key",
			}, nil
		}))

	runID, taskID := h.abandonedTask(t, "dead-worker")

	// Simulate the worst moment: the provider moved money, and the effect row
	// still says RUNNING because the worker died before writing the outcome.
	key := core.NewIdempotencyKey(runID, core.Step(1), 1)
	h.reserveRunningEffect(t, runID, taskID, key, core.ClassQueryable, "send_money")

	h.advanceClockPast(h.engine.LeaseTTL())
	if n := h.reap(t); n != 1 {
		t.Fatalf("reaper reclaimed %d tasks, want 1", n)
	}

	h.start(t)
	if err := h.dispatcher.Publish(context.Background(), taskID); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}

	// The whole point: the money moved once.
	if got := sends.Load(); got != 0 {
		t.Fatalf("the provider was called %d more times after recovery; an "+
			"action that may already have happened must never be re-sent blindly", got)
	}
	if got := queries.Load(); got == 0 {
		t.Fatal("recovery did not ask the provider what happened")
	}

	effects := h.effects(t, runID)
	if len(effects) != 1 {
		t.Fatalf("got %d ledger rows, want 1", len(effects))
	}
	if effects[0].Status != core.EffectCommitted || effects[0].ExternalRef != "pay_98374" {
		t.Fatalf("effect = %s ref %q, want COMMITTED ref pay_98374",
			effects[0].Status, effects[0].ExternalRef)
	}

	types := typesOf(h.history(t, runID))
	if !contains(types, core.EventEffectUnknown) {
		t.Fatalf("history = %v; an abandoned RUNNING effect must be recorded as UNKNOWN", types)
	}
	if !contains(types, core.EventEffectReconciled) {
		t.Fatalf("history = %v; settling it by lookup must be recorded", types)
	}
}

// TestReaperLeavesLiveWorkAlone guards the other direction. A reaper that
// reclaimed healthy tasks would interrupt work that is progressing perfectly
// well, and for a side-effecting tool that means a second attempt.
func TestReaperLeavesLiveWorkAlone(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("work")

	_, taskID := h.abandonedTask(t, "busy-worker")

	// The lease has not lapsed, so there is nothing to reclaim.
	if n := h.reap(t); n != 0 {
		t.Fatalf("reaper reclaimed %d live tasks, want 0", n)
	}
	if task := h.task(t, taskID); task.Status != core.TaskRunning {
		t.Fatalf("task status = %s, want RUNNING: a live claim must be left alone", task.Status)
	}
}

// --- helpers --------------------------------------------------------------

// abandonedTask starts a run, claims its first task as workerID, and then
// never reports — the state a killed worker leaves behind.
func (h *harness) abandonedTask(t *testing.T, workerID string) (core.RunID, core.TaskID) {
	t.Helper()
	ctx := context.Background()

	runID, err := h.engine.StartRun(ctx, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	taskID := h.awaitTask(t, runID)

	if err := h.engine.RegisterWorker(ctx, workerID, "test"); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if _, err := h.engine.ClaimTask(ctx, taskID, workerID); err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}
	return runID, taskID
}

// reserveRunningEffect puts an effect into the state a crash mid-call leaves:
// reserved, marked RUNNING, and never resolved.
func (h *harness) reserveRunningEffect(t *testing.T, runID core.RunID, taskID core.TaskID, key core.IdempotencyKey, class core.EffectClass, toolName string) {
	t.Helper()

	err := h.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.ReserveEffect(ctx, core.Effect{
			ID:     core.EffectID("effect-crashed"),
			TaskID: taskID,
			RunID:  runID,
			Type:   toolName,
			Class:  class,
			Key:    key,
			Status: core.EffectPending,
		}); err != nil {
			return err
		}
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{})
	})
	if err != nil {
		t.Fatalf("seed running effect: %v", err)
	}
}

func (h *harness) reap(t *testing.T) int {
	t.Helper()
	n, err := h.reaper.ReapOnce(context.Background())
	if err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	return n
}

func (h *harness) task(t *testing.T, id core.TaskID) core.Task {
	t.Helper()
	task, err := h.store.GetTask(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	return task
}

func (h *harness) lease(t *testing.T, id core.TaskID) core.Lease {
	t.Helper()

	var l core.Lease
	err := h.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		var err error
		l, err = tx.GetLease(ctx, id)
		return err
	})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	return l
}

package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/decider"
)

// The engine's half of the outbox: it commits the intent to deliver and stops
// there. These tests are about what is true in the store the instant after a
// commit, with nothing running — which is the state a crash leaves behind.

// TestEngineCommitsDeliveryWithTheTask is the dual write, gone.
//
// No relay, no workers. The engine creates a task and returns; if the process
// died at this exact instant, the delivery intent is already durable and any
// other process can publish it.
func TestEngineCommitsDeliveryWithTheTask(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("work")

	runID, err := h.engine.StartRun(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	tasks := h.tasks(t, runID)
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}

	pending, err := h.store.PendingDeliveries(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("got %d pending deliveries, want 1: a task must never be committed "+
			"without the intent to announce it", len(pending))
	}
	if pending[0].TaskID != tasks[0].ID {
		t.Fatalf("delivery is for %s, want %s", pending[0].TaskID, tasks[0].ID)
	}
}

// TestEngineDoesNotPublishDirectly pins where the boundary now is.
//
// If the engine still reached the dispatcher itself, everything would appear to
// work and the window would be back — visible only as a run that occasionally
// stops for no recorded reason. Asserting the dispatcher is untouched is how
// that stays a test failure rather than a production mystery.
func TestEngineDoesNotPublishDirectly(t *testing.T) {
	spy := &countingDispatcher{}
	h := newHarnessWithDispatcher(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	), spy)
	h.registerEcho("work")

	if _, err := h.engine.StartRun(context.Background(), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if got := spy.publishes; got != 0 {
		t.Fatalf("the engine published %d times; delivery belongs to the relay, and "+
			"a publish outside the transaction is the dual write this layer removed", got)
	}
}

// TestReclaimedTaskIsAnnouncedInTheSameTransaction covers the recovery path.
//
// This is the one that would hurt most if it were wrong: the reaper runs
// because something already failed, so a task it makes PENDING without
// announcing would be stranded by the very code meant to rescue it.
func TestReclaimedTaskIsAnnouncedInTheSameTransaction(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("work")

	_, taskID := h.abandonedTask(t, "ghost-worker")

	// The delivery from the original dispatch is still sitting there; drain it
	// so what is left is only what the reaper writes.
	h.drainDeliveries(t)

	h.advanceClockPast(h.engine.LeaseTTL())
	if n := h.reap(t); n != 1 {
		t.Fatalf("reaper reclaimed %d tasks, want 1", n)
	}

	pending, err := h.store.PendingDeliveries(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	if len(pending) != 1 || pending[0].TaskID != taskID {
		t.Fatalf("after a reclaim the outbox holds %v, want one delivery for %s",
			deliveryTasks(pending), taskID)
	}
}

// TestDeadLetteredTaskIsNotAnnounced is the other half. A task that has run out
// of attempts must not be handed to anyone: it would be claimed, rejected by
// its own status, and dropped — noise on top of a run that already failed.
func TestDeadLetteredTaskIsNotAnnounced(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "work", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("work")

	_, taskID := h.abandonedTask(t, "ghost-worker")
	h.drainDeliveries(t)

	// Burn the attempts: each lapse costs one, and the task has three.
	for i := 0; i < 5; i++ {
		h.advanceClockPast(h.engine.LeaseTTL())
		if n := h.reap(t); n == 0 {
			break
		}
		h.drainDeliveries(t)
		h.claimAndAbandon(t, taskID, "ghost-worker")
	}

	task := h.task(t, taskID)
	if task.Status != core.TaskDeadLetter {
		t.Fatalf("task status = %s, want DEAD_LETTER after exhausting attempts", task.Status)
	}
	pending, err := h.store.PendingDeliveries(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("a dead-lettered task was announced to %d workers; nobody should be "+
			"told to work on it", len(pending))
	}
}

// --- helpers --------------------------------------------------------------

func (h *harness) tasks(t *testing.T, runID core.RunID) []core.Task {
	t.Helper()
	out, err := h.engine.Tasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("Tasks(%s): %v", runID, err)
	}
	return out
}

// drainDeliveries marks everything currently in the outbox published, without
// publishing it. It lets a test assert on what one specific operation added,
// rather than on the accumulated history of the run.
func (h *harness) drainDeliveries(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	pending, err := h.store.PendingDeliveries(ctx, 100)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	if len(pending) == 0 {
		return
	}
	ids := make([]core.DeliveryID, len(pending))
	for i, d := range pending {
		ids[i] = d.ID
	}
	if err := h.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.MarkDelivered(ctx, ids)
	}); err != nil {
		t.Fatalf("drain deliveries: %v", err)
	}
}

// claimAndAbandon re-creates the killed-worker state on a task that is already
// back in PENDING, so a test can watch a task lapse more than once.
func (h *harness) claimAndAbandon(t *testing.T, id core.TaskID, workerID string) {
	t.Helper()
	if _, err := h.engine.ClaimTask(context.Background(), id, workerID); err != nil {
		t.Fatalf("ClaimTask(%s): %v", id, err)
	}
}

// countingDispatcher answers like a dispatcher and records that nobody used it.
type countingDispatcher struct {
	publishes int
}

func (d *countingDispatcher) Publish(context.Context, core.TaskID) error {
	d.publishes++
	return nil
}

func (d *countingDispatcher) Claim(ctx context.Context) (core.TaskID, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func (d *countingDispatcher) Close() error { return nil }

func deliveryTasks(ds []core.Delivery) []core.TaskID {
	out := make([]core.TaskID, len(ds))
	for i, d := range ds {
		out[i] = d.TaskID
	}
	return out
}

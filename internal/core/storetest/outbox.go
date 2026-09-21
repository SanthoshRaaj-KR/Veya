package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Contracts for the transactional outbox.
//
// The one that matters is DeliveryCommitsWithItsTask. Everything else here is
// bookkeeping; that test is the reason the table exists. If a store lets a
// task commit without its delivery intent, or the reverse, the runtime is back
// to a dual write and can silently strand work.

func outboxContracts() []contract {
	return []contract{
		{"EnqueuedDeliveryIsPending", testDeliveryPending},
		{"DeliveryCommitsWithItsTask", testDeliveryAtomicWithTask},
		{"MarkDeliveredClearsItFromPending", testMarkDelivered},
		{"FailedDeliveryStaysPendingAndCounts", testFailDelivery},
		{"PendingDeliveriesAreOldestFirst", testDeliveryOrder},
		{"DeliveryRequiresARealTask", testDeliveryNeedsTask},
		{"ADelayedDeliveryIsNotPendingUntilItsInstant", testDelayedDelivery},
	}
}

func testDeliveryPending(t *testing.T, s core.Store) {
	runID := core.RunID("run-outbox")
	mustCreateRun(t, s, runID)
	task := newTask("task-outbox", runID, core.Step(1))

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateTask(ctx, task); err != nil {
			return err
		}
		return tx.EnqueueDelivery(ctx, task.ID, time.Time{})
	})

	pending := mustPending(t, s, 10)
	if len(pending) != 1 {
		t.Fatalf("got %d pending deliveries, want 1", len(pending))
	}
	if pending[0].TaskID != task.ID {
		t.Fatalf("delivery is for %s, want %s", pending[0].TaskID, task.ID)
	}
	if pending[0].ID == 0 {
		t.Fatal("delivery has no ID; the store must assign one so the relay can mark it")
	}
	if pending[0].CreatedAt.IsZero() {
		t.Fatal("delivery has zero CreatedAt; the store must stamp it from the clock")
	}
	if pending[0].Attempts != 0 {
		t.Fatalf("new delivery has %d attempts, want 0", pending[0].Attempts)
	}
}

// testDeliveryAtomicWithTask is the contract the outbox exists for.
//
// A transaction that creates a task, enqueues its delivery, and then fails must
// leave neither behind. A store that kept the delivery would announce work that
// does not exist; a store that kept the task would strand it forever. Both are
// the dual write, just in different directions.
func testDeliveryAtomicWithTask(t *testing.T, s core.Store) {
	runID := core.RunID("run-outbox-atomic")
	mustCreateRun(t, s, runID)

	boom := errors.New("deliberate failure after enqueue")
	err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		task := newTask("task-doomed-delivery", runID, core.Step(1))
		if err := tx.CreateTask(ctx, task); err != nil {
			return err
		}
		if err := tx.EnqueueDelivery(ctx, task.ID, time.Time{}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("RunInTx error = %v, want the sentinel", err)
	}

	if pending := mustPending(t, s, 10); len(pending) != 0 {
		t.Fatalf("rolled-back transaction left %d deliveries behind", len(pending))
	}
	tasks, err := s.ListTasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("rolled-back transaction left %d tasks behind", len(tasks))
	}
}

func testMarkDelivered(t *testing.T, s core.Store) {
	runID := core.RunID("run-outbox-mark")
	mustCreateRun(t, s, runID)

	first := newTask("task-mark-1", runID, core.Step(1))
	second := newTask("task-mark-2", runID, core.Step(2))
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		for _, task := range []core.Task{first, second} {
			if err := tx.CreateTask(ctx, task); err != nil {
				return err
			}
			if err := tx.EnqueueDelivery(ctx, task.ID, time.Time{}); err != nil {
				return err
			}
		}
		return nil
	})

	pending := mustPending(t, s, 10)
	if len(pending) != 2 {
		t.Fatalf("got %d pending deliveries, want 2", len(pending))
	}

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.MarkDelivered(ctx, []core.DeliveryID{pending[0].ID})
	})

	left := mustPending(t, s, 10)
	if len(left) != 1 {
		t.Fatalf("got %d pending deliveries after marking one, want 1", len(left))
	}
	if left[0].ID != pending[1].ID {
		t.Fatalf("wrong delivery left pending: %d, want %d", left[0].ID, pending[1].ID)
	}

	// Marking an already-marked row is how a relay that crashed mid-batch
	// recovers, so it has to be harmless rather than an error.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.MarkDelivered(ctx, []core.DeliveryID{pending[0].ID})
	})
	// An empty batch is what the relay sends when every publish failed.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.MarkDelivered(ctx, nil)
	})
}

// testFailDelivery covers the counter an operator reads. A delivery that
// cannot be published is not abandoned — there is no attempt count at which
// stranding a task becomes acceptable — so the row stays pending and only says
// how hard it has been trying.
func testFailDelivery(t *testing.T, s core.Store) {
	runID := core.RunID("run-outbox-fail")
	mustCreateRun(t, s, runID)
	task := newTask("task-fail-delivery", runID, core.Step(1))

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateTask(ctx, task); err != nil {
			return err
		}
		return tx.EnqueueDelivery(ctx, task.ID, time.Time{})
	})

	id := mustPending(t, s, 10)[0].ID
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.FailDelivery(ctx, id, "broker unreachable")
	})

	pending := mustPending(t, s, 10)
	if len(pending) != 1 {
		t.Fatalf("got %d pending deliveries after a failure, want 1: a failed publish must not drop the task", len(pending))
	}
	if pending[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", pending[0].Attempts)
	}
	if pending[0].LastError != "broker unreachable" {
		t.Fatalf("last error = %q, want %q", pending[0].LastError, "broker unreachable")
	}

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.FailDelivery(ctx, id, "still unreachable")
	})
	if got := mustPending(t, s, 10)[0].Attempts; got != 2 {
		t.Fatalf("attempts = %d, want 2: failures accumulate", got)
	}
}

// testDeliveryOrder keeps the relay fair. Newest-first would let a busy system
// starve the task that has been waiting longest, which is the one most likely
// to be someone waiting on a stuck run.
func testDeliveryOrder(t *testing.T, s core.Store) {
	runID := core.RunID("run-outbox-order")
	mustCreateRun(t, s, runID)

	want := []core.TaskID{"task-o1", "task-o2", "task-o3"}
	for i, id := range want {
		task := newTask(id, runID, core.Step(i+1))
		mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
			if err := tx.CreateTask(ctx, task); err != nil {
				return err
			}
			return tx.EnqueueDelivery(ctx, task.ID, time.Time{})
		})
	}

	pending := mustPending(t, s, 2)
	if len(pending) != 2 {
		t.Fatalf("got %d deliveries for limit 2, want 2", len(pending))
	}
	for i, d := range pending {
		if d.TaskID != want[i] {
			t.Fatalf("delivery %d is for %s, want %s (oldest first)", i, d.TaskID, want[i])
		}
	}
}

func testDeliveryNeedsTask(t *testing.T, s core.Store) {
	err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.EnqueueDelivery(ctx, "task-that-does-not-exist", time.Time{})
	})
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("EnqueueDelivery for an absent task = %v, want ErrNotFound", err)
	}
}

// testDelayedDelivery is the reason AvailableAt exists. A retry backing off
// is not published early just because the relay happened to sweep before its
// instant, and it is published once that instant is reached -- the same "not
// before time T" contract runs.available_at already keeps for a parked run.
func testDelayedDelivery(t *testing.T, s core.Store) {
	runID := core.RunID("run-outbox-delayed")
	mustCreateRun(t, s, runID)
	task := newTask("task-delayed", runID, core.Step(1))

	readyAt := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateTask(ctx, task); err != nil {
			return err
		}
		return tx.EnqueueDelivery(ctx, task.ID, readyAt)
	})

	before := readyAt.Add(-time.Second)
	if pending := mustPendingAt(t, s, before, 10); len(pending) != 0 {
		t.Fatalf("got %d deliveries before their instant, want 0", len(pending))
	}

	after := readyAt.Add(time.Second)
	pending := mustPendingAt(t, s, after, 10)
	if len(pending) != 1 {
		t.Fatalf("got %d deliveries after their instant, want 1", len(pending))
	}
	if pending[0].TaskID != task.ID {
		t.Fatalf("delivery is for %s, want %s", pending[0].TaskID, task.ID)
	}

	// Exactly at the instant is ready, not one tick early or late — the same
	// inclusive comparison runs.available_at makes for a parked run.
	if pending := mustPendingAt(t, s, readyAt, 10); len(pending) != 1 {
		t.Fatalf("got %d deliveries exactly at their instant, want 1 (inclusive)", len(pending))
	}
}

func mustPending(t *testing.T, s core.Store, limit int) []core.Delivery {
	t.Helper()
	return mustPendingAt(t, s, farFuture, limit)
}

func mustPendingAt(t *testing.T, s core.Store, now time.Time, limit int) []core.Delivery {
	t.Helper()
	out, err := s.PendingDeliveries(context.Background(), now, limit)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	return out
}

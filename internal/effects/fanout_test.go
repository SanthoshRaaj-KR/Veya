package effects_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Several effects from one decision, through one ledger.
//
// Fan-out is the first thing in this system that puts more than one external
// action in flight for a single run at a single logical position. Nothing in
// the executor changed to allow it, which is the claim these tests check: the
// ledger's guarantee comes from a unique key per logical call, and children get
// distinct keys because they get distinct step ids. If that were not enough,
// the fix would have had to be inside the executor, and the executor is the one
// place in this codebase where a subtle change is most expensive.

// TestSiblingsGetTheirOwnLedgerRows. Ten children of one decision, ten rows,
// ten keys, each provider called once.
//
// The failure this rules out is the one a shared key produces: the second
// child reserves, finds a COMMITTED row belonging to the first, and is told
// its action already happened. It returns the first child's result and never
// calls the provider — silently, and correctly according to the ledger.
func TestSiblingsGetTheirOwnLedgerRows(t *testing.T) {
	const children = 10

	x := newExecutor(t)

	var calls atomic.Int64
	seen := &sync.Map{}
	x.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		func(_ context.Context, call core.ToolCall) ([]byte, error) {
			calls.Add(1)
			seen.Store(call.Key, call.StepID)
			return json.RawMessage(fmt.Sprintf(`{"reference":%q}`, call.Key)), nil
		}, nil)

	parent := core.Step(3)
	tasks := x.seedFanOut(t, "charge", parent, children)

	for _, task := range tasks {
		if _, err := x.executor.Execute(context.Background(), task); err != nil {
			t.Fatalf("Execute %s: %v", task.StepID, err)
		}
	}

	if got := calls.Load(); got != children {
		t.Fatalf("the provider was called %d times for %d children", got, children)
	}

	rows, err := x.store.ListEffects(context.Background(), tasks[0].RunID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	if len(rows) != children {
		t.Fatalf("the ledger has %d rows for %d children", len(rows), children)
	}

	// Every key distinct, and every key the one derived from the child's own
	// step. A collision here is not a test failure, it is a double charge.
	keys := map[core.IdempotencyKey]bool{}
	for i, child := range parent.Children(children) {
		want := core.NewIdempotencyKey(tasks[0].RunID, child, 1)
		if keys[want] {
			t.Fatalf("child %d reuses key %s", i, want)
		}
		keys[want] = true

		if _, ok := seen.Load(want); !ok {
			t.Fatalf("no call was made under key %s; child %d shared a sibling's row", want, i)
		}
	}
}

// TestASiblingRedeliveredDoesNotDisturbTheOthers.
//
// Delivery is at-least-once, so one child of a fan-out being handed out twice
// is ordinary. Its own second attempt must return the recorded result without
// calling the provider again, and the other children must be untouched —
// neither blocked by it nor re-run because of it.
func TestASiblingRedeliveredDoesNotDisturbTheOthers(t *testing.T) {
	x := newExecutor(t)

	perKey := &sync.Map{}
	x.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		func(_ context.Context, call core.ToolCall) ([]byte, error) {
			n, _ := perKey.LoadOrStore(call.Key, new(atomic.Int64))
			n.(*atomic.Int64).Add(1)
			return json.RawMessage(fmt.Sprintf(`{"reference":%q}`, call.Key)), nil
		}, nil)

	parent := core.Step(2)
	tasks := x.seedFanOut(t, "charge", parent, 3)
	ctx := context.Background()

	// The middle child is delivered three times; its siblings once each.
	for _, task := range tasks {
		attempts := 1
		if task.StepID == parent.Child(1) {
			attempts = 3
		}
		for i := 0; i < attempts; i++ {
			if _, err := x.executor.Execute(ctx, task); err != nil {
				t.Fatalf("Execute %s attempt %d: %v", task.StepID, i+1, err)
			}
		}
	}

	for i, child := range parent.Children(3) {
		key := core.NewIdempotencyKey(tasks[0].RunID, child, 1)
		n, ok := perKey.Load(key)
		if !ok {
			t.Fatalf("child %d (%s) never reached the provider", i, child)
		}
		if got := n.(*atomic.Int64).Load(); got != 1 {
			t.Fatalf("child %d was performed %d times; the ledger is meant to make "+
				"a redelivery return the recorded result", i, got)
		}
	}
}

// TestSiblingsRunningAtOnceEachActOnce is the shape fan-out actually
// executes: several workers, several children of one run, all in flight
// together.
//
// Concurrency here is across *distinct* children, which is the state the
// engine produces. Two workers executing the same child at the same instant is
// not: the task's PENDING -> RUNNING transition is conditional, so exactly one
// worker gets the task, and that exclusion is the claim's, not the ledger's.
//
// Worth saying plainly, because the ledger does not stand in for it. An
// IDEMPOTENT_BY_KEY effect found RUNNING by a second live caller is
// *deliberately* re-sent under the same key -- that is what the class means,
// and the provider is the thing that deduplicates it. Calling Execute twice
// concurrently on one task therefore can reach the provider twice, safely, and
// a test that forbade it would be testing a guarantee this layer does not make
// and does not need to.
func TestSiblingsRunningAtOnceEachActOnce(t *testing.T) {
	const children = 6

	x := newExecutor(t)

	perKey := &sync.Map{}
	x.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		func(_ context.Context, call core.ToolCall) ([]byte, error) {
			n, _ := perKey.LoadOrStore(call.Key, new(atomic.Int64))
			n.(*atomic.Int64).Add(1)
			return json.RawMessage(`{"reference":"ok"}`), nil
		}, nil)

	parent := core.Step(1)
	tasks := x.seedFanOut(t, "charge", parent, children)
	ctx := context.Background()

	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(task core.Task) {
			defer wg.Done()
			if _, err := x.executor.Execute(ctx, task); err != nil {
				t.Errorf("Execute %s: %v", task.StepID, err)
			}
		}(task)
	}
	wg.Wait()

	for i, child := range parent.Children(children) {
		key := core.NewIdempotencyKey(tasks[0].RunID, child, 1)
		n, ok := perKey.Load(key)
		if !ok {
			t.Fatalf("child %d (%s) never reached the provider; a sibling took its row", i, child)
		}
		if got := n.(*atomic.Int64).Load(); got != 1 {
			t.Fatalf("child %d was performed %d times", i, got)
		}
	}

	rows, err := x.store.ListEffects(ctx, tasks[0].RunID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	if len(rows) != children {
		t.Fatalf("the ledger has %d rows for %d concurrent children", len(rows), children)
	}
}

// seedFanOut creates a run and n sibling tasks under one parent step, named the
// way the engine names them.
func (x *executorFixture) seedFanOut(t *testing.T, toolName string, parent core.StepID, n int) []core.Task {
	t.Helper()

	runID := core.RunID("run-fanout")
	tasks := make([]core.Task, n)

	err := x.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateRun(ctx, core.Run{
			ID: runID, AgentName: "fixture", AgentVersion: "v1", Status: core.RunRunning,
		}); err != nil {
			return err
		}
		for i, step := range parent.Children(n) {
			task := core.Task{
				ID:     core.TaskID(fmt.Sprintf("task-%s", step)),
				RunID:  runID,
				StepID: step,
				Type:   toolName,
				// Deliberately identical payloads. Keys are derived from
				// position, not content, so identical children must still get
				// different keys -- and the version of this system that hashed
				// the request body would fail right here.
				Payload:     json.RawMessage(`{"amount":500}`),
				Status:      core.TaskRunning,
				MaxAttempts: 3,
			}
			tasks[i] = task

			if err := tx.CreateTask(ctx, core.Task{
				ID: task.ID, RunID: runID, StepID: step, Type: toolName,
				Payload: task.Payload, Status: core.TaskPending, MaxAttempts: 3,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed fan-out: %v", err)
	}
	return tasks
}

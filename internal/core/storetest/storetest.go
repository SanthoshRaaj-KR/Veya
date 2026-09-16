// Package storetest is the contract suite for core.Store.
//
// Every adapter runs it. That is the whole mechanism by which the in-memory
// store and the PostgreSQL store stay interchangeable: if a behaviour holds in
// one and not the other, this suite fails, and either an adapter is wrong or
// the port is leaking an implementation detail. Nothing else in the codebase
// is allowed to notice which adapter it is talking to.
//
// The tests concentrate on the conditional writes, because those are the
// concurrency barriers the design rests on:
//
//	AdvanceRun      compare-and-swap on run version — who advances the run
//	CreateTask      UNIQUE (run_id, step_id)        — idempotent task creation
//	TransitionTask  conditional on current status   — who claims the task
//	AppendEvent     PRIMARY KEY (run_id, seq)       — gapless history
//
// A store that quietly permits any of these to happen twice is broken in a way
// that would not show up until a duplicate side effect reached a customer.
//
// # JSON documents round-trip semantically, not byte for byte
//
// A store may return a document that differs textually from the one written.
// PostgreSQL JSONB normalizes whitespace, orders keys by its own rule, and
// drops duplicate keys; the in-memory adapter preserves the exact bytes. Both
// are conforming, so this suite compares documents by value.
//
// The contract is therefore: payloads are JSON documents, not byte strings.
// Nothing may depend on their exact encoding — which is another reason
// idempotency keys are derived from logical position rather than by hashing a
// request body, since the same document can have two encodings and would hash
// to two different keys.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// NewStore builds an empty store for a single test.
type NewStore func(t *testing.T) core.Store

// TickingClock returns a clock that advances one millisecond per read.
//
// Every adapter's tests use it, so that records created in sequence get
// distinct ordered timestamps without depending on wall-clock resolution — on
// Windows that is ~15ms, which is long enough for several inserts to share a
// timestamp and make ordering assertions flap.
func TickingClock() core.Clock {
	return &ticking{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

type ticking struct {
	mu  sync.Mutex
	now time.Time
}

func (c *ticking) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

// contract is one named behaviour every adapter must exhibit.
type contract struct {
	name string
	fn   func(*testing.T, core.Store)
}

// RunStoreSuite runs the full contract against one adapter.
func RunStoreSuite(t *testing.T, newStore NewStore) {
	t.Helper()

	tests := []contract{
		{"RunLifecycle", testRunLifecycle},
		{"AdvanceRunIsCompareAndSwap", testAdvanceRunCAS},
		{"AdvanceRunRejectsTerminal", testAdvanceRunTerminal},
		{"CreateTaskIsIdempotentPerStep", testCreateTaskIdempotent},
		{"ClaimIsExclusive", testClaimExclusive},
		{"AppendEventIsConditional", testAppendEventConditional},
		{"HistoryIsOrderedAndGapless", testHistoryOrdered},
		{"RollbackLeavesNothing", testRollback},
		{"MissingRecordsReportNotFound", testNotFound},
		{"PendingTasksReturnsDispatchable", testPendingTasks},
		{"RunsAwaitingAdvanceExcludesInFlight", testAwaitingAdvance},
	}
	tests = append(tests, effectContracts()...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t)
			t.Cleanup(func() { _ = s.Close() })
			tc.fn(t, s)
		})
	}
}

// --- individual contracts -------------------------------------------------

func testRunLifecycle(t *testing.T, s core.Store) {
	ctx := context.Background()
	id := core.RunID("run-lifecycle")
	mustCreateRun(t, s, id)

	got := mustGetRun(t, s, id)
	if got.Status != core.RunRunning {
		t.Fatalf("new run status = %s, want %s", got.Status, core.RunRunning)
	}
	if got.Version != 0 {
		t.Fatalf("new run version = %d, want 0", got.Version)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("new run has zero CreatedAt; the store must stamp it from the clock")
	}

	out := json.RawMessage(`{"ok":true}`)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{Status: core.RunCompleted, Output: out})
	})

	got = mustGetRun(t, s, id)
	if got.Status != core.RunCompleted {
		t.Fatalf("status = %s, want %s", got.Status, core.RunCompleted)
	}
	if got.Version != 1 {
		t.Fatalf("version = %d, want 1 after one advance", got.Version)
	}
	assertJSONEqual(t, "output", got.Output, out)
	if got.CompletedAt == nil {
		t.Fatal("terminal run must have CompletedAt set")
	}
	_ = ctx
}

// testAdvanceRunCAS is the run-forking guard. Two workers finishing sibling
// tasks both read version N and both try to advance; exactly one may win.
func testAdvanceRunCAS(t *testing.T, s core.Store) {
	id := core.RunID("run-cas")
	mustCreateRun(t, s, id)

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{Status: core.RunRunning})
	})

	err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{Status: core.RunCompleted})
	})
	if !errors.Is(err, core.ErrConflict) {
		t.Fatalf("stale advance error = %v, want ErrConflict", err)
	}

	if got := mustGetRun(t, s, id); got.Version != 1 {
		t.Fatalf("version = %d, want 1: the losing advance must not have applied", got.Version)
	}
}

func testAdvanceRunTerminal(t *testing.T, s core.Store) {
	id := core.RunID("run-terminal")
	mustCreateRun(t, s, id)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 0, core.RunState{Status: core.RunCompleted})
	})

	err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, id, 1, core.RunState{Status: core.RunFailed, LastError: "too late"})
	})
	if !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("advancing a completed run = %v, want ErrInvalidTransition", err)
	}
}

// testCreateTaskIdempotent covers UNIQUE (run_id, step_id): the same logical
// step of the same run is one task, no matter how many times it is derived.
func testCreateTaskIdempotent(t *testing.T, s core.Store) {
	runID := core.RunID("run-dup-step")
	mustCreateRun(t, s, runID)

	first := newTask("task-a", runID, core.Step(1))
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error { return tx.CreateTask(ctx, first) })

	// Different task ID, same logical position. Must be rejected.
	second := newTask("task-b", runID, core.Step(1))
	err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.CreateTask(ctx, second)
	})
	if !errors.Is(err, core.ErrTaskExists) {
		t.Fatalf("duplicate step error = %v, want ErrTaskExists", err)
	}

	tasks, err := s.ListTasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
}

// testClaimExclusive is the duplicate-delivery guard. It holds with no lease
// and no fencing token: the conditional transition alone is what makes two
// deliveries of one task safe.
func testClaimExclusive(t *testing.T, s core.Store) {
	runID := core.RunID("run-claim")
	mustCreateRun(t, s, runID)
	task := newTask("task-claim", runID, core.Step(1))
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error { return tx.CreateTask(ctx, task) })

	claim := func() error {
		return s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
			return tx.TransitionTask(ctx, task.ID, core.TaskPending, core.TaskRunning, core.TaskOutcome{})
		})
	}

	if err := claim(); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := claim(); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("second claim = %v, want ErrConflict", err)
	}

	got, err := s.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.Status != core.TaskRunning {
		t.Fatalf("status = %s, want %s", got.Status, core.TaskRunning)
	}
	if got.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1: the losing claim must not have counted", got.Attempt)
	}
}

// testAppendEventConditional covers PRIMARY KEY (run_id, seq).
func testAppendEventConditional(t *testing.T, s core.Store) {
	runID := core.RunID("run-events")
	mustCreateRun(t, s, runID)

	ev, err := core.NewEvent(runID, 1, core.EventRunStarted, "", core.RunStartedData{AgentName: "a"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error { return tx.AppendEvent(ctx, ev) })

	dup, _ := core.NewEvent(runID, 1, core.EventTaskCreated, core.Step(1), core.TaskCreatedData{TaskID: "x"})
	err = s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.AppendEvent(ctx, dup)
	})
	if !errors.Is(err, core.ErrSeqConflict) {
		t.Fatalf("duplicate seq error = %v, want ErrSeqConflict", err)
	}
}

func testHistoryOrdered(t *testing.T, s core.Store) {
	runID := core.RunID("run-history")
	mustCreateRun(t, s, runID)

	types := []core.EventType{
		core.EventRunStarted, core.EventTaskCreated, core.EventTaskClaimed,
		core.EventTaskCompleted, core.EventRunCompleted,
	}
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		for _, typ := range types {
			seq, err := tx.NextSeq(ctx, runID)
			if err != nil {
				return err
			}
			ev, err := core.NewEvent(runID, seq, typ, "", map[string]string{"t": string(typ)})
			if err != nil {
				return err
			}
			if err := tx.AppendEvent(ctx, ev); err != nil {
				return err
			}
		}
		return nil
	})

	history, err := s.History(context.Background(), runID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != len(types) {
		t.Fatalf("got %d events, want %d", len(history), len(types))
	}
	for i, ev := range history {
		if want := int64(i + 1); ev.Seq != want {
			t.Fatalf("event %d has seq %d, want %d: history must be gapless and ordered", i, ev.Seq, want)
		}
		if ev.Type != types[i] {
			t.Fatalf("event %d type = %s, want %s", i, ev.Type, types[i])
		}
	}

	// Round-trip through the versioned envelope.
	var decoded map[string]string
	if err := history[0].Decode(&decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded["t"] != string(core.EventRunStarted) {
		t.Fatalf("decoded payload = %v, want t=%s", decoded, core.EventRunStarted)
	}
}

// testRollback is why Tx exists at all. A transaction that fails part way must
// leave no trace, or the atomic commit of task + event is a fiction.
func testRollback(t *testing.T, s core.Store) {
	runID := core.RunID("run-rollback")
	mustCreateRun(t, s, runID)

	sentinel := errors.New("deliberate failure")
	err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		task := newTask("task-doomed", runID, core.Step(9))
		if err := tx.CreateTask(ctx, task); err != nil {
			return err
		}
		ev, err := core.NewEvent(runID, 1, core.EventTaskCreated, core.Step(9), core.TaskCreatedData{TaskID: task.ID})
		if err != nil {
			return err
		}
		if err := tx.AppendEvent(ctx, ev); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("RunInTx error = %v, want the sentinel", err)
	}

	tasks, err := s.ListTasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("rolled-back transaction left %d tasks behind", len(tasks))
	}
	history, err := s.History(context.Background(), runID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("rolled-back transaction left %d events behind", len(history))
	}
}

func testNotFound(t *testing.T, s core.Store) {
	ctx := context.Background()
	if _, err := s.GetRun(ctx, "absent"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("GetRun(absent) = %v, want ErrNotFound", err)
	}
	if _, err := s.GetTask(ctx, "absent"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("GetTask(absent) = %v, want ErrNotFound", err)
	}
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, "absent", 0, core.RunState{Status: core.RunCompleted})
	})
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("AdvanceRun(absent) = %v, want ErrNotFound", err)
	}
}

func testPendingTasks(t *testing.T, s core.Store) {
	runID := core.RunID("run-pending")
	mustCreateRun(t, s, runID)

	pending := newTask("task-pending", runID, core.Step(1))
	claimed := newTask("task-claimed", runID, core.Step(2))
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateTask(ctx, pending); err != nil {
			return err
		}
		if err := tx.CreateTask(ctx, claimed); err != nil {
			return err
		}
		return tx.TransitionTask(ctx, claimed.ID, core.TaskPending, core.TaskRunning, core.TaskOutcome{})
	})

	got, err := s.PendingTasks(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingTasks: %v", err)
	}
	if len(got) != 1 || got[0].ID != pending.ID {
		t.Fatalf("PendingTasks = %v, want only %s", ids(got), pending.ID)
	}
}

func testAwaitingAdvance(t *testing.T, s core.Store) {
	ctx := context.Background()
	idle := core.RunID("run-idle")
	busy := core.RunID("run-busy")
	mustCreateRun(t, s, idle)
	mustCreateRun(t, s, busy)

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.CreateTask(ctx, newTask("task-busy", busy, core.Step(1)))
	})

	got, err := s.RunsAwaitingAdvance(ctx, 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(got) != 1 || got[0] != idle {
		t.Fatalf("RunsAwaitingAdvance = %v, want only %s", got, idle)
	}

	// Completing the task makes the busy run owe a decision too.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionTask(ctx, "task-busy", core.TaskPending, core.TaskRunning, core.TaskOutcome{})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionTask(ctx, "task-busy", core.TaskRunning, core.TaskCompleted, core.TaskOutcome{})
	})

	got, err = s.RunsAwaitingAdvance(ctx, 10)
	if err != nil {
		t.Fatalf("RunsAwaitingAdvance: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d runs awaiting advance, want 2", len(got))
	}
}

// --- helpers --------------------------------------------------------------

func newTask(id core.TaskID, runID core.RunID, step core.StepID) core.Task {
	return core.Task{
		ID:          id,
		RunID:       runID,
		StepID:      step,
		Type:        "noop",
		Payload:     json.RawMessage(`{}`),
		Status:      core.TaskPending,
		MaxAttempts: 3,
	}
}

func mustCreateRun(t *testing.T, s core.Store, id core.RunID) {
	t.Helper()
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.CreateRun(ctx, core.Run{
			ID:           id,
			AgentName:    "suite",
			AgentVersion: "v1",
			Status:       core.RunRunning,
			Input:        json.RawMessage(`{}`),
		})
	})
}

func mustGetRun(t *testing.T, s core.Store, id core.RunID) core.Run {
	t.Helper()
	r, err := s.GetRun(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRun(%s): %v", id, err)
	}
	return r
}

func mustTx(t *testing.T, s core.Store, fn func(context.Context, core.Tx) error) {
	t.Helper()
	if err := s.RunInTx(context.Background(), fn); err != nil {
		t.Fatalf("RunInTx: %v", err)
	}
}

// assertJSONEqual compares two documents by value, because a conforming store
// may re-encode them. See the package comment.
func assertJSONEqual(t *testing.T, field string, got, want json.RawMessage) {
	t.Helper()

	var gotVal, wantVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("%s: stored value is not valid JSON (%s): %v", field, got, err)
	}
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("%s: expected value is not valid JSON (%s): %v", field, want, err)
	}
	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Fatalf("%s = %s, want %s (compared by value)", field, got, want)
	}
}

func ids(ts []core.Task) []core.TaskID {
	out := make([]core.TaskID, len(ts))
	for i, t := range ts {
		out[i] = t.ID
	}
	return out
}

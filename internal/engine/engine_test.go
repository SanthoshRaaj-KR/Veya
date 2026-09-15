package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/santhoshraajkr/veya/internal/core"
	"github.com/santhoshraajkr/veya/internal/core/storetest"
	"github.com/santhoshraajkr/veya/internal/decider"
	"github.com/santhoshraajkr/veya/internal/dispatch/inproc"
	"github.com/santhoshraajkr/veya/internal/engine"
	"github.com/santhoshraajkr/veya/internal/idgen"
	"github.com/santhoshraajkr/veya/internal/store/memory"
	"github.com/santhoshraajkr/veya/internal/tool"
	"github.com/santhoshraajkr/veya/internal/worker"
)

// These tests run entirely on the memory adapter with a virtual clock, so they
// need no Docker and finish in milliseconds. That is the point of having
// written the memory adapter first.

// TestRunCompletesEndToEnd is Layer 1's headline: one run, one worker, a
// sequence of tools, start to completion, with a gapless event history.
func TestRunCompletesEndToEnd(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "upper", Payload: json.RawMessage(`{"text":"hello"}`)},
		decider.Step{Tool: "exclaim", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("upper")
	h.registerEcho("exclaim")
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), "demo", "v1", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}

	// Every step ran, and history records the full story in order.
	assertHistory(t, h.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTaskCreated, core.EventTaskClaimed, core.EventTaskCompleted,
		core.EventTaskCreated, core.EventTaskClaimed, core.EventTaskCompleted,
		core.EventRunCompleted,
	})

	// Two decisions dispatched work, one finished the run: three advances.
	if run.Version != 3 {
		t.Fatalf("version = %d, want 3 (two dispatches plus the finish)", run.Version)
	}
}

// TestFailedTaskRetriesThenSucceeds covers the retry path: a tool that fails
// twice and then works must not fail the run.
func TestFailedTaskRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "flaky", Payload: json.RawMessage(`{}`)},
	))

	var calls atomic.Int32
	h.tools.Register("flaky", func(_ context.Context, _ []byte) ([]byte, error) {
		if calls.Add(1) < 3 {
			return nil, fmt.Errorf("transient failure %d", calls.Load())
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), "flaky-agent", "v1", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("tool called %d times, want 3", got)
	}

	// The step is one task across all three attempts — UNIQUE (run_id, step_id)
	// means a retry reuses the row rather than creating a second one.
	tasks, err := h.engine.Tasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1: a retry must not create a new task", len(tasks))
	}
	if tasks[0].Attempt != 3 {
		t.Fatalf("attempt = %d, want 3", tasks[0].Attempt)
	}
}

// TestExhaustedTaskFailsTheRun covers the other end: a step that never
// succeeds must fail the run loudly rather than leave it stalled forever.
func TestExhaustedTaskFailsTheRun(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "broken", Payload: json.RawMessage(`{}`)},
	))

	var calls atomic.Int32
	h.tools.Register("broken", func(_ context.Context, _ []byte) ([]byte, error) {
		calls.Add(1)
		return nil, fmt.Errorf("permanently broken")
	})
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), "broken-agent", "v1", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED", run.Status)
	}
	if run.LastError == "" {
		t.Fatal("a failed run must record why")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("tool called %d times, want 3 (the attempt limit)", got)
	}

	tasks, err := h.engine.Tasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if tasks[0].Status != core.TaskDeadLetter {
		t.Fatalf("task status = %s, want DEAD_LETTER", tasks[0].Status)
	}
}

// TestUnknownToolFailsTheRun proves a task naming a tool nobody registered is
// reported rather than silently dropped.
func TestUnknownToolFailsTheRun(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "does-not-exist", Payload: json.RawMessage(`{}`)},
	))
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), "bad-agent", "v1", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED", run.Status)
	}
}

// TestScanRecoversUndeliveredTask is the crash-recovery case.
//
// A task is committed and then the delivery is lost — exactly what happens if
// the process dies between the transaction and the Publish call. Nothing in
// memory knows the task exists. The scan re-reads the store, finds it, and the
// run completes, which is the property that makes publishing outside the
// transaction survivable until Layer 3's outbox removes the window.
func TestScanRecoversUndeliveredTask(t *testing.T) {
	h := newHarnessWithDispatcher(t,
		decider.NewStatic(decider.Step{Tool: "upper", Payload: json.RawMessage(`{}`)}),
		&droppingDispatcher{inner: inproc.New(16)},
	)
	h.registerEcho("upper")

	// Start the run with delivery broken. The task commits; nothing arrives.
	runID, err := h.engine.StartRun(context.Background(), "recovered", "v1", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	tasks, err := h.engine.Tasks(context.Background(), runID)
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Status != core.TaskPending {
		t.Fatalf("expected one PENDING task after a dropped delivery, got %v", tasks)
	}

	// Repair delivery and start working, as a restarted process would.
	h.dispatcher.(*droppingDispatcher).repair()
	h.start(t)
	h.runtime.ScanOnce(context.Background())

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED after recovery", run.Status, run.LastError)
	}
}

// TestDuplicateDeliveryExecutesOnce is the at-least-once guarantee in action:
// the same task delivered many times runs exactly once, with no lease and no
// fencing token involved.
func TestDuplicateDeliveryExecutesOnce(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "counted", Payload: json.RawMessage(`{}`)},
	))

	var calls atomic.Int32
	release := make(chan struct{})
	h.tools.Register("counted", func(_ context.Context, _ []byte) ([]byte, error) {
		calls.Add(1)
		<-release // hold the task open so redeliveries arrive mid-flight
		return json.RawMessage(`{"ok":true}`), nil
	})

	// Several workers, so duplicates land on different ones.
	h.startWorkers(t, 4)

	runID, err := h.engine.StartRun(context.Background(), "dup", "v1", nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	tasks, err := h.engine.Tasks(context.Background(), runID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("Tasks = %v, %v; want one task", tasks, err)
	}
	taskID := tasks[0].ID

	// Deliver the same task twenty more times while it is executing.
	for i := 0; i < 20; i++ {
		if err := h.dispatcher.Publish(context.Background(), taskID); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Give the redeliveries time to be claimed and rejected.
	time.Sleep(50 * time.Millisecond)
	close(release)

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("tool executed %d times, want exactly 1 despite 21 deliveries", got)
	}
}

// --- harness --------------------------------------------------------------

type harness struct {
	store      core.Store
	dispatcher core.Dispatcher
	tools      *tool.Registry
	engine     *engine.Engine
	runtime    *engine.Runtime

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newHarness(t *testing.T, d core.Decider) *harness {
	return newHarnessWithDispatcher(t, d, inproc.New(64))
}

func newHarnessWithDispatcher(t *testing.T, d core.Decider, disp core.Dispatcher) *harness {
	t.Helper()

	store := memory.New(storetest.TickingClock())
	tools := tool.New()

	eng, err := engine.New(engine.Config{
		Store:      store,
		Dispatcher: disp,
		Decider:    d,
		IDGen:      idgen.NewSequential(),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	h := &harness{
		store:      store,
		dispatcher: disp,
		tools:      tools,
		engine:     eng,
		runtime:    engine.NewRuntime(engine.RuntimeConfig{Engine: eng, Interval: 10 * time.Millisecond}),
	}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) registerEcho(name string) {
	h.tools.Register(name, func(_ context.Context, payload []byte) ([]byte, error) {
		if len(payload) == 0 {
			return json.RawMessage(`{"echoed":null}`), nil
		}
		return json.RawMessage(fmt.Sprintf(`{"echoed":%s}`, payload)), nil
	})
}

func (h *harness) start(t *testing.T)               { h.startWorkers(t, 1) }
func (h *harness) startWorkers(t *testing.T, n int) { t.Helper(); h.spawn(t, n) }

func (h *harness) spawn(t *testing.T, n int) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	for i := 0; i < n; i++ {
		w, err := worker.New(worker.Config{
			ID:         fmt.Sprintf("worker-%d", i),
			Engine:     h.engine,
			Dispatcher: h.dispatcher,
			Tools:      h.tools,
			Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err != nil {
			t.Fatalf("worker.New: %v", err)
		}
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			_ = w.Run(ctx)
		}()
	}
}

func (h *harness) stop() {
	if h.cancel != nil {
		h.cancel()
	}
	_ = h.dispatcher.Close()
	h.wg.Wait()
	_ = h.store.Close()
}

// awaitTerminal polls until the run finishes or the test times out. Polling
// rather than signalling keeps the engine free of test-only hooks.
func (h *harness) awaitTerminal(t *testing.T, id core.RunID) core.Run {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := h.engine.Run(context.Background(), id)
		if err != nil {
			t.Fatalf("Run(%s): %v", id, err)
		}
		if run.Status.IsTerminal() {
			return run
		}
		time.Sleep(2 * time.Millisecond)
	}

	run, _ := h.engine.Run(context.Background(), id)
	t.Fatalf("run %s did not finish within 5s (status %s); history: %v",
		id, run.Status, typesOf(h.history(t, id)))
	return core.Run{}
}

func (h *harness) history(t *testing.T, id core.RunID) []core.Event {
	t.Helper()
	history, err := h.engine.History(context.Background(), id)
	if err != nil {
		t.Fatalf("History(%s): %v", id, err)
	}
	return history
}

func assertHistory(t *testing.T, got []core.Event, want []core.EventType) {
	t.Helper()

	if gotTypes := typesOf(got); len(gotTypes) != len(want) {
		t.Fatalf("history = %v, want %v", gotTypes, want)
	}
	for i, ev := range got {
		if want := int64(i + 1); ev.Seq != want {
			t.Fatalf("event %d has seq %d, want %d: history must be gapless", i, ev.Seq, want)
		}
		if ev.Type != want[i] {
			t.Fatalf("history = %v, want %v", typesOf(got), want)
		}
	}
}

func typesOf(evs []core.Event) []core.EventType {
	out := make([]core.EventType, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

// droppingDispatcher swallows publishes until repaired, simulating a process
// that dies between committing a task and delivering it.
type droppingDispatcher struct {
	inner    *inproc.Dispatcher
	mu       sync.Mutex
	repaired bool
}

func (d *droppingDispatcher) repair() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.repaired = true
}

func (d *droppingDispatcher) Publish(ctx context.Context, id core.TaskID) error {
	d.mu.Lock()
	repaired := d.repaired
	d.mu.Unlock()

	if !repaired {
		return nil // silently lost, exactly as a crash would lose it
	}
	return d.inner.Publish(ctx, id)
}

func (d *droppingDispatcher) Claim(ctx context.Context) (core.TaskID, error) {
	return d.inner.Claim(ctx)
}

func (d *droppingDispatcher) Close() error { return d.inner.Close() }

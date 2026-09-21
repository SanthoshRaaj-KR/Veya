package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/decider"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/inproc"
	"github.com/SanthoshRaaj-KR/Veya/internal/effects"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/lease"
	"github.com/SanthoshRaaj-KR/Veya/internal/outbox"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
	"github.com/SanthoshRaaj-KR/Veya/internal/tool"
	"github.com/SanthoshRaaj-KR/Veya/internal/worker"
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

	runID, err := h.engine.StartRun(context.Background(), json.RawMessage(`{"x":1}`))
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

// deciderFunc adapts a function to core.Decider, so a test can state a
// decider's whole behaviour where it is used.
type deciderFunc func(core.Run, []core.Event) (core.Decision, error)

func (f deciderFunc) Decide(_ context.Context, run core.Run, history []core.Event) (core.Decision, error) {
	return f(run, history)
}

// TestADeciderThatCannotDecideFailsTheRun. A run nobody will ever advance is
// invisible, and invisible stalled work is worse than a recorded failure.
func TestADeciderThatCannotDecideFailsTheRun(t *testing.T) {
	h := newHarness(t, deciderFunc(func(core.Run, []core.Event) (core.Decision, error) {
		return core.Decision{}, errors.New("the agent body raised")
	}))
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := h.awaitTerminal(t, runID)
	if run.Status != core.RunFailed {
		t.Fatalf("status = %s, want FAILED", run.Status)
	}
	if !strings.Contains(run.LastError, "the agent body raised") {
		t.Fatalf("LastError = %q, which does not say what went wrong", run.LastError)
	}
}

// TestADeciderThatIsNotReadyLeavesTheRunAlone is the other half, and the more
// important one.
//
// "No worker has connected yet" and "the connected worker serves a different
// version" are facts about the deployment, not about the run. Failing runs for
// those would mean a runtime started thirty seconds before its workers
// destroyed every run in that window, and a rolling deploy destroyed every run
// still in flight. Both are fixed by waiting, so the run stays RUNNING and the
// recovery loop tries again.
func TestADeciderThatIsNotReadyLeavesTheRunAlone(t *testing.T) {
	var ready atomic.Bool

	h := newHarness(t, deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		if !ready.Load() {
			return core.Decision{}, fmt.Errorf("%w: no worker yet", core.ErrUnavailable)
		}
		if len(history) > 1 {
			return core.Decision{Kind: core.DecideComplete}, nil
		}
		return core.Decision{
			Kind:     core.DecideCallTool,
			StepID:   core.Step(1),
			TaskType: "upper",
			Payload:  json.RawMessage(`{}`),
		}, nil
	}))
	h.registerEcho("upper")
	h.start(t)

	// StartRun advances, and advancing cannot proceed. The error surfaces; the
	// run does not.
	runID, err := h.engine.StartRun(context.Background(), nil)
	if !errors.Is(err, core.ErrUnavailable) {
		t.Fatalf("StartRun err = %v, want ErrUnavailable", err)
	}

	run, err := h.store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != core.RunRunning {
		t.Fatalf("status = %s, want RUNNING; a deployment gap is not a run outcome", run.Status)
	}

	// The worker arrives. The recovery loop finds the run and drives it.
	ready.Store(true)
	h.runtime.ScanOnce(context.Background())

	if got := h.awaitTerminal(t, runID); got.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED once a decider was available",
			got.Status, got.LastError)
	}
}

// TestFailedTaskRetriesThenSucceeds covers the retry path: a tool that fails
// twice and then works must not fail the run.
func TestFailedTaskRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "flaky", Payload: json.RawMessage(`{}`)},
	))

	var calls atomic.Int32
	h.tools.Func("flaky", func(_ context.Context, _ core.ToolCall) ([]byte, error) {
		if calls.Add(1) < 3 {
			return nil, fmt.Errorf("transient failure %d", calls.Load())
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
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
	h.tools.Func("broken", func(_ context.Context, _ core.ToolCall) ([]byte, error) {
		calls.Add(1)
		return nil, fmt.Errorf("permanently broken")
	})
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
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

	runID, err := h.engine.StartRun(context.Background(), nil)
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
	runID, err := h.engine.StartRun(context.Background(), nil)
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
	h.tools.Func("counted", func(_ context.Context, _ core.ToolCall) ([]byte, error) {
		calls.Add(1)
		<-release // hold the task open so redeliveries arrive mid-flight
		return json.RawMessage(`{"ok":true}`), nil
	})

	// Several workers, so duplicates land on different ones.
	h.startWorkers(t, 4)

	runID, err := h.engine.StartRun(context.Background(), nil)
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

// testAgent is the agent every harness engine serves.
const testAgent = "test-agent"

type harness struct {
	clock      *clock.Virtual
	store      core.Store
	dispatcher core.Dispatcher
	tools      *tool.Registry
	engine     *engine.Engine
	executor   *effects.Executor
	relay      *outbox.Relay
	reaper     *lease.Reaper
	runtime    *engine.Runtime

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newHarness(t *testing.T, d core.Decider) *harness {
	return newHarnessWithDispatcher(t, d, inproc.New(64))
}

func newHarnessWithDispatcher(t *testing.T, d core.Decider, disp core.Dispatcher) *harness {
	t.Helper()

	// A virtual clock, frozen unless a test advances it. Leases therefore
	// never lapse by accident, and a test that wants to simulate a dead worker
	// says so explicitly rather than sleeping and hoping.
	clk := clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return newHarnessOn(t, d, disp, clk, memory.New(clk))
}

// newHarnessOn builds a runtime over a store and clock that already exist.
//
// It is what makes a restart testable: the store is the database, so a test
// that wants to simulate the process dying keeps the store, throws everything
// above it away, and builds a second runtime on top. Anything that survives
// that survived because it was durable, not because a goroutine remembered it.
func newHarnessOn(t *testing.T, d core.Decider, disp core.Dispatcher,
	clk *clock.Virtual, store core.Store) *harness {
	t.Helper()

	tools := tool.New()

	// Tasks reach the dispatcher only through the outbox relay, so the harness
	// has to run one. Its short interval is a backstop; Wake carries the
	// normal path.
	relay, err := outbox.New(outbox.Config{
		Store:      store,
		Dispatcher: disp,
		Clock:      clk,
		Interval:   5 * time.Millisecond,
		Logger:     quietLogger(),
	})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}

	eng, err := engine.New(engine.Config{
		Store:        store,
		Dispatcher:   disp,
		Decider:      d,
		IDGen:        idgen.NewSequential(),
		Clock:        clk,
		LeaseTTL:     30 * time.Second,
		Agent:        testAgent,
		AgentVersion: "v1",
		Wake:         relay.Wake,
		Logger:       quietLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	executor, err := effects.New(effects.Config{
		Store: store,
		Tools: tools,
		IDGen: idgen.NewSequential(),
		Clock: clk,
		Log:   quietLogger(),
	})
	if err != nil {
		t.Fatalf("effects.New: %v", err)
	}

	reaper, err := lease.New(lease.Config{
		Store:  store,
		Clock:  clk,
		Wake:   relay.Wake,
		Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("lease.New: %v", err)
	}

	h := &harness{
		clock:      clk,
		store:      store,
		dispatcher: disp,
		tools:      tools,
		engine:     eng,
		executor:   executor,
		relay:      relay,
		reaper:     reaper,
		runtime:    engine.NewRuntime(engine.RuntimeConfig{Engine: eng, Interval: 10 * time.Millisecond}),
	}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) registerEcho(name string) {
	h.tools.Func(name, func(_ context.Context, call core.ToolCall) ([]byte, error) {
		if len(call.Payload) == 0 {
			return json.RawMessage(`{"echoed":null}`), nil
		}
		return json.RawMessage(fmt.Sprintf(`{"echoed":%s}`, call.Payload)), nil
	})
}

func (h *harness) start(t *testing.T)               { h.startWorkers(t, 1) }
func (h *harness) startWorkers(t *testing.T, n int) { t.Helper(); h.spawn(t, n) }

func (h *harness) spawn(t *testing.T, n int) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	// Nothing reaches a worker without the relay: the engine commits delivery
	// intent and stops there.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		_ = h.relay.Run(ctx)
	}()

	for i := 0; i < n; i++ {
		w, err := worker.New(worker.Config{
			ID:                fmt.Sprintf("worker-%d", i),
			Engine:            h.engine,
			Dispatcher:        h.dispatcher,
			Executor:          h.executor,
			HeartbeatInterval: 200 * time.Millisecond,
			Logger:            quietLogger(),
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
	h.crash()
	_ = h.store.Close()
}

// crash stops everything above the store and leaves the store alone, which is
// what a process dying looks like to a database.
func (h *harness) crash() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	_ = h.dispatcher.Close()
	h.wg.Wait()
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

// TestEngineIgnoresOtherAgentsRuns is a regression test.
//
// A live run against PostgreSQL found the demo runtime advancing a run left
// behind by the contract suite — a run belonging to agent "suite" was driven
// through the meeting assistant's steps because RunsAwaitingAdvance returns
// every stalled run regardless of who owns it. Nothing failed loudly; the
// wrong work simply happened and was recorded as correct.
//
// An engine now advances only its own agent's runs.
func TestEngineIgnoresOtherAgentsRuns(t *testing.T) {
	h := newHarness(t, decider.NewStatic(
		decider.Step{Tool: "upper", Payload: json.RawMessage(`{}`)},
	))
	h.registerEcho("upper")

	// A run belonging to somebody else, in exactly the state the recovery scan
	// looks for: RUNNING with no tasks outstanding.
	foreign := core.RunID("foreign-run")
	err := h.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.CreateRun(ctx, core.Run{
			ID:           foreign,
			AgentName:    "some-other-agent",
			AgentVersion: "v9",
			Status:       core.RunRunning,
		})
	})
	if err != nil {
		t.Fatalf("create foreign run: %v", err)
	}

	h.start(t)
	h.runtime.ScanOnce(context.Background())
	time.Sleep(20 * time.Millisecond)

	run, err := h.engine.Run(context.Background(), foreign)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Version != 0 {
		t.Fatalf("foreign run advanced to version %d; an engine must not touch another agent's run", run.Version)
	}

	tasks, err := h.engine.Tasks(context.Background(), foreign)
	if err != nil {
		t.Fatalf("Tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("engine created %d tasks on another agent's run", len(tasks))
	}
}

// quietLogger keeps test output readable; failures report through t, not logs.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// advanceClockPast moves the virtual clock beyond d, so that anything with a
// deadline of d has demonstrably lapsed.
func (h *harness) advanceClockPast(d time.Duration) {
	h.clock.Advance(d + time.Second)
}

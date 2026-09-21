package engine_test

import (
	"context"
	"encoding/json"
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
)

// newHarnessWithRetry is newHarnessOn with one difference: the engine's
// RetryPolicy is the caller's, not the zero value. Kept separate from
// newHarnessOn rather than threading a parameter through its four existing
// callers, none of which care about retry timing.
func newHarnessWithRetry(t *testing.T, d core.Decider, retry core.RetryPolicy) *harness {
	t.Helper()

	clk := clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New(clk)
	disp := inproc.New(64)
	tools := tool.New()

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
		Retry:        retry,
		Agent:        testAgent,
		AgentVersion: "v1",
		Wake:         relay.Wake,
		Logger:       quietLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	executor, err := effects.New(effects.Config{
		Store: store, Tools: tools, IDGen: idgen.NewSequential(), Clock: clk, Log: quietLogger(),
	})
	if err != nil {
		t.Fatalf("effects.New: %v", err)
	}

	reaper, err := lease.New(lease.Config{Store: store, Clock: clk, Wake: relay.Wake, Logger: quietLogger()})
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

// TestARetryWithBackoffWaitsForItsInstant is the reason available_at exists
// on the outbox: a retry configured with backoff must not be redelivered
// before its instant, however many times the relay sweeps, and must be
// redelivered once the clock reaches it.
func TestARetryWithBackoffWaitsForItsInstant(t *testing.T) {
	h := newHarnessWithRetry(t, decider.NewStatic(
		decider.Step{Tool: "flaky", Payload: json.RawMessage(`{}`)},
	), core.RetryPolicy{InitialBackoff: 200 * time.Millisecond})

	var calls atomic.Int32
	h.tools.Func("flaky", func(_ context.Context, _ core.ToolCall) ([]byte, error) {
		if calls.Add(1) == 1 {
			return nil, errFlaky
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	h.start(t)

	runID, err := h.engine.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// The first attempt fails. Give the relay real wall-clock time to sweep
	// several times over the virtual clock, which has not moved: if backoff
	// were not honoured, the retry would land during this window.
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() > 1 {
			t.Fatal("the retry was redelivered before its backoff instant elapsed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Fatalf("tool called %d times before backoff elapsed, want exactly 1", calls.Load())
	}
	run, err := h.engine.Run(context.Background(), runID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Status != core.RunRunning {
		t.Fatalf("status = %s, want RUNNING: the run must still be waiting on backoff", run.Status)
	}

	// Advance the virtual clock past the backoff instant. The relay's own
	// ticker (real time) then finds the now-ready row on its next sweep.
	h.clock.Advance(300 * time.Millisecond)

	run = h.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED once backoff elapsed", run.Status, run.LastError)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("tool called %d times, want 2", got)
	}
}

var errFlaky = flakyError{}

type flakyError struct{}

func (flakyError) Error() string { return "transient failure" }

package effects_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/effects"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
	"github.com/SanthoshRaaj-KR/Veya/internal/tool"
)

// What a tool is told about the call it is making.
//
// The interesting field is Key. IDEMPOTENT_BY_KEY means "the provider
// deduplicates by idempotency key", and a tool cannot make that true unless it
// is handed the key to send — so these tests are about a class meaning what it
// says rather than about plumbing.

// TestEffectfulToolReceivesItsIdempotencyKey is the property the class rests on.
func TestEffectfulToolReceivesItsIdempotencyKey(t *testing.T) {
	x := newExecutor(t)

	var got core.ToolCall
	x.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		func(_ context.Context, call core.ToolCall) ([]byte, error) {
			got = call
			return json.RawMessage(`{"reference":"ch_1"}`), nil
		}, nil)

	task := x.seedTask(t, "charge", core.Step(1))
	if _, err := x.executor.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	want := core.NewIdempotencyKey(task.RunID, task.StepID, 1)
	if got.Key != want {
		t.Fatalf("tool received key %q, want %q; without it the tool has nothing "+
			"to send the provider and IDEMPOTENT_BY_KEY is a claim it cannot honour",
			got.Key, want)
	}
	if got.TaskID != task.ID || got.RunID != task.RunID || got.StepID != task.StepID {
		t.Fatalf("call identity = %+v, want task %s of run %s", got, task.ID, task.RunID)
	}
	if string(got.Payload) != string(task.Payload) {
		t.Fatalf("payload = %s, want %s", got.Payload, task.Payload)
	}
}

// TestTheKeyIsTheSameOnEveryAttempt is what makes the key worth sending. A key
// that changed between attempts would deduplicate nothing: the provider would
// see two different calls and do the work twice.
func TestTheKeyIsTheSameOnEveryAttempt(t *testing.T) {
	x := newExecutor(t)

	var (
		mu   sync.Mutex
		keys []core.IdempotencyKey
	)
	x.tools.Effectful("charge", core.ClassIdempotentByKey, time.Hour,
		func(_ context.Context, call core.ToolCall) ([]byte, error) {
			mu.Lock()
			keys = append(keys, call.Key)
			mu.Unlock()
			return nil, core.NotExecuted(errors.New("provider refused"))
		}, nil)

	task := x.seedTask(t, "charge", core.Step(1))
	for attempt := 1; attempt <= 3; attempt++ {
		task.Attempt = attempt
		if _, err := x.executor.Execute(context.Background(), task); err == nil {
			t.Fatal("Execute succeeded; the handler always fails")
		}
	}

	if len(keys) != 3 {
		t.Fatalf("the tool ran %d times, want 3", len(keys))
	}
	for i, k := range keys {
		if k != keys[0] {
			t.Fatalf("attempt %d used key %q, attempt 1 used %q; a key that changes "+
				"between attempts deduplicates nothing", i+1, k, keys[0])
		}
	}
}

// TestPureReadsGetNoKey keeps the fast path honest. A NONE-class tool has no
// ledger row, so there is nothing to deduplicate and no key to hand out;
// inventing one would suggest a guarantee that does not exist.
func TestPureReadsGetNoKey(t *testing.T) {
	x := newExecutor(t)

	var got core.ToolCall
	x.tools.Func("lookup", func(_ context.Context, call core.ToolCall) ([]byte, error) {
		got = call
		return json.RawMessage(`{"found":true}`), nil
	})

	task := x.seedTask(t, "lookup", core.Step(1))
	if _, err := x.executor.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got.Key != "" {
		t.Fatalf("a pure read was given key %q; it has no ledger row and nothing "+
			"to deduplicate", got.Key)
	}
	if got.TaskID != task.ID {
		t.Fatalf("task id = %q, want %q: identity is still useful for logging", got.TaskID, task.ID)
	}
}

// --- fixture --------------------------------------------------------------

type executorFixture struct {
	store    core.Store
	tools    *tool.Registry
	executor *effects.Executor
}

func newExecutor(t *testing.T) *executorFixture {
	t.Helper()

	clk := clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New(clk)
	tools := tool.New()

	x, err := effects.New(effects.Config{
		Store: store,
		Tools: tools,
		IDGen: idgen.NewSequential(),
		Clock: clk,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("effects.New: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })
	return &executorFixture{store: store, tools: tools, executor: x}
}

func (x *executorFixture) seedTask(t *testing.T, toolName string, step core.StepID) core.Task {
	t.Helper()

	runID := core.RunID("run-toolcall")
	task := core.Task{
		ID: core.TaskID("task-" + toolName), RunID: runID, StepID: step, Type: toolName,
		Payload: json.RawMessage(`{"amount":500}`), Status: core.TaskRunning, MaxAttempts: 3,
	}
	err := x.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateRun(ctx, core.Run{
			ID: runID, AgentName: "fixture", AgentVersion: "v1", Status: core.RunRunning,
		}); err != nil {
			return err
		}
		return tx.CreateTask(ctx, core.Task{
			ID: task.ID, RunID: runID, StepID: step, Type: toolName,
			Payload: task.Payload, Status: core.TaskPending, MaxAttempts: 3,
		})
	})
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}
	return task
}

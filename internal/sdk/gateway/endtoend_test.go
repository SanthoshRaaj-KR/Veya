package gateway_test

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
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/inproc"
	"github.com/SanthoshRaaj-KR/Veya/internal/effects"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/outbox"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/gateway"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
	"github.com/SanthoshRaaj-KR/Veya/internal/worker"
)

// The whole of Layer 4, assembled: a real engine, a real effect ledger, a real
// worker loop, and an agent whose body and tools live on the far side of a
// socket.
//
// Nothing here is a mock of the runtime. The only test double is the *agent*,
// which is the thing that is supposed to be replaceable.

const remoteAgent = "remote_agent"

// --- a stack whose agent is somewhere else --------------------------------

type remoteStack struct {
	store core.Store
	clock *clock.Virtual
	eng   *engine.Engine
	addr  string

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newRemoteStack(t *testing.T) *remoteStack {
	t.Helper()

	server, addr := listen(t)
	gw := server.Gateway()

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Frozen unless a test advances it, so a lease never lapses by accident
	// and a test that wants a dead worker has to say so.
	clk := clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New(clk)
	dispatcher := inproc.New(64)

	relay, err := outbox.New(outbox.Config{
		Store:      store,
		Dispatcher: dispatcher,
		Clock:      clk,
		Interval:   5 * time.Millisecond,
		Logger:     quiet,
	})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}

	// The two lines this entire layer exists to make possible: a decider and a
	// tool registry that are not in this process.
	eng, err := engine.New(engine.Config{
		Store:        store,
		Dispatcher:   dispatcher,
		Decider:      gw.Decider(remoteAgent),
		IDGen:        idgen.NewSequential(),
		Clock:        clk,
		LeaseTTL:     30 * time.Second,
		Agent:        remoteAgent,
		AgentVersion: "v1",
		Wake:         relay.Wake,
		Logger:       quiet,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	executor, err := effects.New(effects.Config{
		Store: store,
		Tools: gw.Registry(remoteAgent),
		IDGen: idgen.NewSequential(),
		Clock: clk,
		Log:   quiet,
	})
	if err != nil {
		t.Fatalf("effects.New: %v", err)
	}

	w, err := worker.New(worker.Config{
		ID:                "local-worker-0",
		Engine:            eng,
		Dispatcher:        dispatcher,
		Executor:          executor,
		HeartbeatInterval: 200 * time.Millisecond,
		Logger:            quiet,
	})
	if err != nil {
		t.Fatalf("worker.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &remoteStack{store: store, clock: clk, eng: eng, addr: addr, cancel: cancel}

	for _, loop := range []func(context.Context) error{
		relay.Run,
		w.Run,
		engine.NewRuntime(engine.RuntimeConfig{Engine: eng, Interval: 10 * time.Millisecond}).Run,
	} {
		s.wg.Add(1)
		go func(run func(context.Context) error) {
			defer s.wg.Done()
			_ = run(ctx)
		}(loop)
	}

	t.Cleanup(func() {
		cancel()
		_ = dispatcher.Close()
		s.wg.Wait()
		_ = store.Close()
	})
	return s
}

func (s *remoteStack) awaitTerminal(t *testing.T, id core.RunID) core.Run {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		run, err := s.store.GetRun(context.Background(), id)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if run.Status.IsTerminal() {
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s never reached a terminal state", id)
	return core.Run{}
}

func (s *remoteStack) history(t *testing.T, id core.RunID) []core.Event {
	t.Helper()
	history, err := s.store.History(context.Background(), id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	return history
}

// --- a remote agent, in Go, over the wire ---------------------------------

// scriptedAgent decides by walking a fixed list, the same way decider.Static
// does — except it does so on the far side of a socket, from history it was
// sent rather than from state it kept.
type scriptedAgent struct {
	steps []struct {
		tool    string
		payload string
	}
}

func (a *scriptedAgent) decide(req *pb.DecideRequest) *pb.DecideResult {
	// Position from history, never from a counter on this struct. A decider
	// that counts its own invocations returns step 1 again after a restart and
	// forks the run.
	done := 0
	for _, e := range req.GetHistory() {
		if e.GetType() == "TASK_COMPLETED" {
			done++
		}
	}

	if done < len(a.steps) {
		step := a.steps[done]
		return &pb.DecideResult{
			CallId: req.GetCallId(),
			Outcome: &pb.DecideResult_Decision{Decision: &pb.Decision{
				Kind:    pb.DecisionKind_DECISION_KIND_CALL_TOOL,
				StepId:  fmt.Sprintf("S%d", done+1),
				Tool:    step.tool,
				Payload: []byte(step.payload),
			}},
		}
	}

	return &pb.DecideResult{
		CallId: req.GetCallId(),
		Outcome: &pb.DecideResult_Decision{Decision: &pb.Decision{
			Kind:   pb.DecisionKind_DECISION_KIND_COMPLETE,
			Output: []byte(`{"done":true}`),
		}},
	}
}

// --- the headline ---------------------------------------------------------

func TestARunCompletesThroughARemoteAgent(t *testing.T) {
	s := newRemoteStack(t)

	script := &scriptedAgent{steps: []struct {
		tool    string
		payload string
	}{
		{"fetch", `{"id":"M-1042"}`},
		{"summarize", `{"style":"brief"}`},
	}}

	var fetched, summarized atomic.Int32

	dial(t, goWorkerConfig{
		addr: s.addr, agent: remoteAgent, version: "v1", workerID: "remote-1", decides: true,
		tools: []*pb.ToolDescriptor{
			{Name: "fetch", EffectClass: pb.EffectClass_EFFECT_CLASS_NONE},
			{
				Name:          "summarize",
				EffectClass:   pb.EffectClass_EFFECT_CLASS_IDEMPOTENT_BY_KEY,
				KeyTtlSeconds: int64(24 * time.Hour / time.Second),
			},
		},
		onDecide: script.decide,
		onExecute: func(req *pb.ExecuteRequest) *pb.ExecuteResult {
			switch req.GetTool() {
			case "fetch":
				fetched.Add(1)
				return ok(req, `{"notes":"Layer 4 ships."}`)
			case "summarize":
				summarized.Add(1)
				return ok(req, `{"reference":"cmpl_1","key":"`+req.GetIdempotencyKey()+`"}`)
			}
			return &pb.ExecuteResult{CallId: req.GetCallId(), Outcome: &pb.ExecuteResult_Failure{
				Failure: wire.FailureFrom(core.NotExecuted(fmt.Errorf("no tool %q", req.GetTool()))),
			}}
		},
	})

	runID, err := s.eng.StartRun(context.Background(), json.RawMessage(`{"id":"M-1042"}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := s.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}
	if fetched.Load() != 1 || summarized.Load() != 1 {
		t.Fatalf("fetch ran %d times and summarize %d; want one each",
			fetched.Load(), summarized.Load())
	}

	assertTypes(t, s.history(t, runID), []core.EventType{
		core.EventRunStarted,
		core.EventTaskCreated, core.EventTaskClaimed, core.EventTaskCompleted,
		core.EventTaskCreated, core.EventTaskClaimed,
		core.EventEffectCreated, core.EventEffectCommitted, core.EventTaskCompleted,
		core.EventRunCompleted,
	})

	// The pure read wrote no ledger row; the model call did. Exactly the
	// behaviour a Go-defined agent gets, reached through a socket.
	ledger, err := s.store.ListEffects(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("%d ledger rows, want 1 (the NONE tool writes none)", len(ledger))
	}
	if ledger[0].Status != core.EffectCommitted || ledger[0].Class != core.ClassIdempotentByKey {
		t.Fatalf("ledger row = %+v", ledger[0])
	}
	if !strings.Contains(string(ledger[0].Response), string(ledger[0].Key)) {
		t.Fatalf("the tool did not receive its key: %s", ledger[0].Response)
	}
}

// TestAnAmbiguousRemoteEffectIsReconciledNotRepeated is the project's central
// claim, performed across the wire by a worker in another process.
//
// The tool acts, and then the answer is lost. That is the case everything in
// Layer 2 exists for: an error after a request may have been transmitted is
// not evidence that it was not. The retry must not send a second message — it
// must ask.
func TestAnAmbiguousRemoteEffectIsReconciledNotRepeated(t *testing.T) {
	s := newRemoteStack(t)

	script := &scriptedAgent{steps: []struct {
		tool    string
		payload string
	}{
		{"send", `{"to":"ana@example.com"}`},
	}}

	// The provider, such as it is. It records what it sent under the caller's
	// key, which is the only reason it can be asked about it afterwards.
	var (
		mu        sync.Mutex
		sent      = map[string]string{}
		sends     atomic.Int32
		reconcile atomic.Int32
	)

	dial(t, goWorkerConfig{
		addr: s.addr, agent: remoteAgent, version: "v1", workerID: "remote-1", decides: true,
		tools: []*pb.ToolDescriptor{{
			Name:          "send",
			EffectClass:   pb.EffectClass_EFFECT_CLASS_QUERYABLE,
			KeyTtlSeconds: int64(24 * time.Hour / time.Second),
			Reconcilable:  true,
		}},
		onDecide: script.decide,
		onExecute: func(req *pb.ExecuteRequest) *pb.ExecuteResult {
			key := req.GetIdempotencyKey()

			mu.Lock()
			if _, already := sent[key]; !already {
				sends.Add(1)
				sent[key] = "msg_" + key
			}
			mu.Unlock()

			// The message went out and the connection dropped before the
			// acknowledgement came back. An ordinary error, which the wire
			// reports as UNKNOWN because that is what it is.
			if req.GetAttempt() == 1 {
				return &pb.ExecuteResult{CallId: req.GetCallId(), Outcome: &pb.ExecuteResult_Failure{
					Failure: &pb.Failure{
						Message:   "the connection dropped after the request went out",
						Type:      "TimeoutError",
						Certainty: pb.Certainty_CERTAINTY_UNKNOWN,
					},
				}}
			}
			return ok(req, `{"reference":"`+sent[key]+`"}`)
		},
		onReconcile: func(req *pb.ReconcileRequest) *pb.ReconcileResult {
			reconcile.Add(1)

			mu.Lock()
			reference, found := sent[req.GetEffect().GetIdempotencyKey()]
			mu.Unlock()

			if !found {
				return &pb.ReconcileResult{CallId: req.GetCallId(),
					Outcome: &pb.ReconcileResult_Resolution{Resolution: &pb.Resolution{
						Kind: pb.ResolutionKind_RESOLUTION_KIND_NOT_EXECUTED,
					}}}
			}
			return &pb.ReconcileResult{CallId: req.GetCallId(),
				Outcome: &pb.ReconcileResult_Resolution{Resolution: &pb.Resolution{
					Kind:        pb.ResolutionKind_RESOLUTION_KIND_COMMITTED,
					Response:    []byte(`{"reference":"` + reference + `"}`),
					ExternalRef: reference,
					Detail:      "found by idempotency key",
				}}}
		},
	})

	runID, err := s.eng.StartRun(context.Background(), nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := s.awaitTerminal(t, runID)
	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED", run.Status, run.LastError)
	}

	// The whole point, in one assertion.
	if got := sends.Load(); got != 1 {
		t.Fatalf("the provider was sent %d messages, want exactly 1", got)
	}
	if reconcile.Load() == 0 {
		t.Fatal("the outcome was resolved without asking the provider; " +
			"a retry that re-sends is the duplicate this design exists to prevent")
	}

	ledger, err := s.store.ListEffects(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("%d ledger rows, want 1", len(ledger))
	}
	if ledger[0].Status != core.EffectCommitted {
		t.Fatalf("ledger row is %s, want COMMITTED", ledger[0].Status)
	}
	if ledger[0].ExternalRef == "" {
		t.Fatal("the reconciled row has no external reference; " +
			"an operator could not tie it back to the provider's record")
	}

	// History tells the whole story: acted, lost the answer, asked, settled.
	types := typesOf(s.history(t, runID))
	for _, want := range []core.EventType{
		core.EventEffectCreated,
		core.EventEffectUnknown,
		core.EventEffectReconciled,
	} {
		if !hasType(types, want) {
			t.Fatalf("history is missing %s; it reads %v", want, types)
		}
	}
}

// TestARunPinnedToAnotherVersionIsRefusedLoudly is the exit criterion for
// agent versioning.
//
// A v2 body may call different tools in a different order, so letting it
// decide for a run pinned to v1 would fork the run at replay — silently, and
// visible later only as a duplicate or a missing step.
//
// Refused, and specifically not failed. A rolling deploy that put v2 in front
// of every in-flight v1 run must not destroy them; they are recoverable by
// rolling back or by leaving a v1 worker up, so the run stays RUNNING and the
// error names both versions.
func TestARunPinnedToAnotherVersionIsRefusedLoudly(t *testing.T) {
	s := newRemoteStack(t)

	// The engine pins v1. The only connected worker serves v2.
	dial(t, goWorkerConfig{
		addr: s.addr, agent: remoteAgent, version: "v2", workerID: "remote-v2", decides: true,
		tools: []*pb.ToolDescriptor{{Name: "send", EffectClass: pb.EffectClass_EFFECT_CLASS_NONE}},
		onDecide: func(req *pb.DecideRequest) *pb.DecideResult {
			t.Error("a v2 worker was asked to decide for a run pinned to v1")
			return &pb.DecideResult{CallId: req.GetCallId()}
		},
	})

	runID, err := s.eng.StartRun(context.Background(), nil)

	if !errors.Is(err, gateway.ErrAgentVersion) {
		t.Fatalf("StartRun err = %v, want ErrAgentVersion", err)
	}
	if !errors.Is(err, core.ErrUnavailable) {
		t.Fatalf("a version mismatch must be recoverable, not terminal: %v", err)
	}
	for _, want := range []string{"v1", "v2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error must name both versions; %q is missing from %q", want, err)
		}
	}

	run, getErr := s.store.GetRun(context.Background(), runID)
	if getErr != nil {
		t.Fatalf("GetRun: %v", getErr)
	}
	if run.Status != core.RunRunning {
		t.Fatalf("the run is %s; a deploy putting v2 in front of an in-flight v1 run "+
			"must not destroy it", run.Status)
	}

	// Nothing was recorded on the run's behalf. History holds the start and
	// nothing else: no step decided under the wrong body.
	if types := typesOf(s.history(t, runID)); len(types) != 1 || types[0] != core.EventRunStarted {
		t.Fatalf("history reads %v; a refused decision must leave no trace on the run", types)
	}
}

// TestAV1WorkerArrivingLaterDrivesTheRun. The other half: the refusal above is
// a pause, so the run has to actually resume once the right worker connects.
func TestAV1WorkerArrivingLaterDrivesTheRun(t *testing.T) {
	s := newRemoteStack(t)

	runID, err := s.eng.StartRun(context.Background(), nil)
	if !errors.Is(err, gateway.ErrNoWorker) {
		t.Fatalf("StartRun err = %v, want ErrNoWorker with nothing connected", err)
	}

	script := &scriptedAgent{steps: []struct {
		tool    string
		payload string
	}{{"send", `{"to":"ana@example.com"}`}}}

	dial(t, goWorkerConfig{
		addr: s.addr, agent: remoteAgent, version: "v1", workerID: "remote-late", decides: true,
		tools:    []*pb.ToolDescriptor{{Name: "send", EffectClass: pb.EffectClass_EFFECT_CLASS_NONE}},
		onDecide: script.decide,
		onExecute: func(req *pb.ExecuteRequest) *pb.ExecuteResult {
			return ok(req, `{"delivered":true}`)
		},
	})

	if run := s.awaitTerminal(t, runID); run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s), want COMPLETED once a v1 worker arrived",
			run.Status, run.LastError)
	}
}

// --- helpers --------------------------------------------------------------

func ok(req *pb.ExecuteRequest, body string) *pb.ExecuteResult {
	return &pb.ExecuteResult{
		CallId:  req.GetCallId(),
		Outcome: &pb.ExecuteResult_Result{Result: []byte(body)},
	}
}

func typesOf(history []core.Event) []core.EventType {
	out := make([]core.EventType, len(history))
	for i, e := range history {
		out[i] = e.Type
	}
	return out
}

func hasType(types []core.EventType, want core.EventType) bool {
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}

func assertTypes(t *testing.T, history []core.Event, want []core.EventType) {
	t.Helper()

	got := typesOf(history)
	if len(got) != len(want) {
		t.Fatalf("history has %d events, want %d:\n got %v\nwant %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d is %s, want %s:\n got %v\nwant %v", i+1, got[i], want[i], got, want)
		}
	}

	// Gapless and ordered by construction; a violation here means the
	// conditional append stopped being conditional.
	for i, e := range history {
		if e.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d; history must be gapless", i+1, e.Seq)
		}
	}
}

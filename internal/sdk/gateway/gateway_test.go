package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// These tests drive Gateway.serve directly over a channel-backed stream. No
// listener, no TCP, no gRPC server: the protocol logic is what is under test,
// and a transport in the way would only add flakiness and seconds.
//
// The end-to-end path over a real socket is covered separately, by the Go test
// worker that speaks the generated client.

const (
	testAgent   = "refund_agent"
	testVersion = "v1"
)

// --- a worker made of channels --------------------------------------------

type fakeStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	toServer chan *pb.ClientMessage
	toClient chan *pb.ServerMessage

	closeOnce sync.Once
}

func newFakeStream() *fakeStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeStream{
		ctx:      ctx,
		cancel:   cancel,
		toServer: make(chan *pb.ClientMessage, 8),
		toClient: make(chan *pb.ServerMessage, 8),
	}
}

func (f *fakeStream) Send(m *pb.ServerMessage) error {
	select {
	case f.toClient <- m:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}

func (f *fakeStream) Recv() (*pb.ClientMessage, error) {
	select {
	case m, ok := <-f.toServer:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	case <-f.ctx.Done():
		return nil, io.EOF
	}
}

func (f *fakeStream) Context() context.Context { return f.ctx }

// hangUp simulates the worker's process dying: the stream ends with no warning
// and nothing further is answered.
func (f *fakeStream) hangUp() { f.closeOnce.Do(func() { f.cancel() }) }

// worker is a test double that registers and then answers requests with a
// caller-supplied function.
type worker struct {
	t      *testing.T
	stream *fakeStream
	gw     *Gateway
	served chan error

	// stopOnce so that a test which hangs the worker up on purpose does not
	// deadlock against the cleanup that hangs it up again.
	stopOnce sync.Once
}

// answer is what a test worker does with one request. Returning nil means
// "send nothing", which is how a hung tool is simulated.
type answer func(*pb.ServerMessage) *pb.ClientMessage

func connect(t *testing.T, gw *Gateway, reg *pb.Register, reply answer) *worker {
	t.Helper()

	w := &worker{t: t, stream: newFakeStream(), gw: gw, served: make(chan error, 1)}

	go func() { w.served <- gw.serve(w.stream) }()

	w.stream.toServer <- &pb.ClientMessage{Message: &pb.ClientMessage_Register{Register: reg}}

	// The registration acknowledgement, or a refusal. A refusal shows up as
	// serve returning rather than as a message.
	select {
	case msg := <-w.stream.toClient:
		if msg.GetRegistered() == nil {
			t.Fatalf("first server message was %T, want Registered", msg.GetMessage())
		}
	case err := <-w.served:
		t.Fatalf("registration refused: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the registration acknowledgement")
	}

	if reply != nil {
		go w.answerLoop(reply)
	}
	t.Cleanup(func() { w.stop() })
	return w
}

// connectExpectingRefusal registers and requires that the gateway rejects it.
func connectExpectingRefusal(t *testing.T, gw *Gateway, reg *pb.Register) error {
	t.Helper()

	w := &worker{t: t, stream: newFakeStream(), gw: gw, served: make(chan error, 1)}
	go func() { w.served <- gw.serve(w.stream) }()
	w.stream.toServer <- &pb.ClientMessage{Message: &pb.ClientMessage_Register{Register: reg}}

	select {
	case err := <-w.served:
		if err == nil {
			t.Fatal("registration was accepted; it should have been refused")
		}
		return err
	case msg := <-w.stream.toClient:
		t.Fatalf("registration was acknowledged with %T; it should have been refused", msg.GetMessage())
		return nil
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the refusal")
		return nil
	}
}

func (w *worker) answerLoop(reply answer) {
	for {
		select {
		case msg := <-w.stream.toClient:
			if out := reply(msg); out != nil {
				select {
				case w.stream.toServer <- out:
				case <-w.stream.ctx.Done():
					return
				}
			}
		case <-w.stream.ctx.Done():
			return
		}
	}
}

func (w *worker) stop() {
	w.stopOnce.Do(func() {
		w.stream.hangUp()
		select {
		case <-w.served:
		case <-time.After(2 * time.Second):
			w.t.Error("serve did not return after the stream ended")
		}
	})
}

func newGateway() *Gateway {
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func registration(tools ...*pb.ToolDescriptor) *pb.Register {
	return &pb.Register{
		ProtocolVersion: pb.ProtocolVersion_PROTOCOL_VERSION_1,
		AgentName:       testAgent,
		AgentVersion:    testVersion,
		WorkerId:        "w-1",
		Runtime:         "test",
		Decides:         true,
		Tools:           tools,
	}
}

func queryableTool(name string) *pb.ToolDescriptor {
	return &pb.ToolDescriptor{
		Name:          name,
		EffectClass:   pb.EffectClass_EFFECT_CLASS_QUERYABLE,
		KeyTtlSeconds: int64(24 * time.Hour / time.Second),
		Reconcilable:  true,
	}
}

func testRun() core.Run {
	return core.Run{
		ID:           "run-1",
		AgentName:    testAgent,
		AgentVersion: testVersion,
		Status:       core.RunRunning,
	}
}

// --- registration ---------------------------------------------------------

func TestRegistrationEstablishesTheAgentAndItsTools(t *testing.T) {
	gw := newGateway()
	connect(t, gw, registration(queryableTool("send_invoice")), nil)

	if got := gw.Registered()[testAgent]; len(got) != 1 || got[0] != testVersion {
		t.Fatalf("Registered() = %v, want one %s", got, testVersion)
	}
	if got := gw.Registry(testAgent).Names(); len(got) != 1 || got[0] != "send_invoice" {
		t.Fatalf("Names() = %v", got)
	}
}

func TestRegistrationIsRefusedWhenItCannotBeTrusted(t *testing.T) {
	tests := []struct {
		name string
		reg  *pb.Register
		want string
	}{
		{
			"no protocol version",
			&pb.Register{AgentName: testAgent, AgentVersion: testVersion},
			"protocol version",
		},
		{
			"no agent name",
			&pb.Register{ProtocolVersion: pb.ProtocolVersion_PROTOCOL_VERSION_1, AgentVersion: "v1"},
			"no agent name",
		},
		{
			// A run pins the version it started under. A worker that declares
			// none cannot be matched against any pin, so admitting it would
			// mean deciding for runs on the basis of nothing.
			"no agent version",
			&pb.Register{ProtocolVersion: pb.ProtocolVersion_PROTOCOL_VERSION_1, AgentName: testAgent},
			"without a version",
		},
		{
			"queryable with no reconcile hook",
			registration(&pb.ToolDescriptor{
				Name:        "send_invoice",
				EffectClass: pb.EffectClass_EFFECT_CLASS_QUERYABLE,
			}),
			"UNRECONCILABLE",
		},
		{
			"unspecified effect class",
			registration(&pb.ToolDescriptor{Name: "send_invoice"}),
			"not one this build knows",
		},
		{
			"the same tool twice",
			registration(queryableTool("send_invoice"), queryableTool("send_invoice")),
			"twice",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := connectExpectingRefusal(t, newGateway(), tc.reg)
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refusal was %q, which does not mention %q", err, tc.want)
			}
		})
	}
}

// TestAFirstMessageThatIsNotRegisterIsRefused. The registration is the
// contract; a session that skipped it has declared nothing, and there is
// nothing sensible to assume on its behalf.
func TestAFirstMessageThatIsNotRegisterIsRefused(t *testing.T) {
	gw := newGateway()
	w := &worker{t: t, stream: newFakeStream(), gw: gw, served: make(chan error, 1)}
	go func() { w.served <- gw.serve(w.stream) }()

	w.stream.toServer <- &pb.ClientMessage{
		Message: &pb.ClientMessage_ExecuteResult{ExecuteResult: &pb.ExecuteResult{CallId: "x"}},
	}

	select {
	case err := <-w.served:
		if err == nil || !strings.Contains(err.Error(), "must be a Register") {
			t.Fatalf("err = %v, want a complaint about the first message", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestDisconnectionUnregistersTheWorker(t *testing.T) {
	gw := newGateway()
	w := connect(t, gw, registration(queryableTool("send_invoice")), nil)

	w.stop()

	if got := gw.Registered(); len(got) != 0 {
		t.Fatalf("Registered() = %v after the worker went away, want empty", got)
	}
	if _, err := gw.Registry(testAgent).Lookup("send_invoice"); !errors.Is(err, core.ErrToolNotFound) {
		t.Fatalf("Lookup after disconnect = %v, want ErrToolNotFound", err)
	}
}

// --- deciding -------------------------------------------------------------

func TestDecideRoundTrips(t *testing.T) {
	gw := newGateway()
	connect(t, gw, registration(queryableTool("send_invoice")), func(msg *pb.ServerMessage) *pb.ClientMessage {
		req := msg.GetDecide()
		if req == nil {
			return nil
		}
		return &pb.ClientMessage{Message: &pb.ClientMessage_DecideResult{
			DecideResult: &pb.DecideResult{
				CallId: req.GetCallId(),
				Outcome: &pb.DecideResult_Decision{Decision: &pb.Decision{
					Kind:    pb.DecisionKind_DECISION_KIND_CALL_TOOL,
					StepId:  "S1",
					Tool:    "send_invoice",
					Payload: []byte(`{"customer_id":42}`),
				}},
			},
		}}
	})

	got, err := gw.Decider(testAgent).Decide(context.Background(), testRun(), nil)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if got.Kind != core.DecideCallTool || got.TaskType != "send_invoice" || got.StepID != core.Step(1) {
		t.Fatalf("got %+v", got)
	}
}

// TestDecideSendsWholeHistory. A worker that received deltas would have to
// hold a run's position between requests, and a worker holding that position
// is a worker that restarts, loses it, and replays against a history it no
// longer agrees with.
func TestDecideSendsWholeHistory(t *testing.T) {
	gw := newGateway()

	seen := make(chan int, 1)
	connect(t, gw, registration(), func(msg *pb.ServerMessage) *pb.ClientMessage {
		req := msg.GetDecide()
		if req == nil {
			return nil
		}
		select {
		case seen <- len(req.GetHistory()):
		default:
		}
		return &pb.ClientMessage{Message: &pb.ClientMessage_DecideResult{
			DecideResult: &pb.DecideResult{
				CallId:  req.GetCallId(),
				Outcome: &pb.DecideResult_Decision{Decision: &pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_COMPLETE}},
			},
		}}
	})

	history := make([]core.Event, 5)
	for i := range history {
		ev, err := core.NewEvent("run-1", int64(i+1), core.EventTaskCompleted, core.Step(i+1),
			core.TaskCompletedData{TaskID: core.TaskID("t")})
		if err != nil {
			t.Fatalf("NewEvent: %v", err)
		}
		history[i] = ev
	}

	if _, err := gw.Decider(testAgent).Decide(context.Background(), testRun(), history); err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if got := <-seen; got != len(history) {
		t.Fatalf("the worker saw %d events, want all %d", got, len(history))
	}
}

// TestDecideRefusesAVersionThePinDoesNotMatch is the loud failure the exit
// criteria ask for. A v2 body may call different tools in a different order,
// so deciding for a v1 run would fork it at replay — silently, and visible
// later only as a duplicate or a missing step.
func TestDecideRefusesAVersionThePinDoesNotMatch(t *testing.T) {
	gw := newGateway()

	reg := registration(queryableTool("send_invoice"))
	reg.AgentVersion = "v2"
	connect(t, gw, reg, nil)

	_, err := gw.Decider(testAgent).Decide(context.Background(), testRun(), nil)

	if !errors.Is(err, ErrAgentVersion) {
		t.Fatalf("err = %v, want ErrAgentVersion", err)
	}
	for _, want := range []string{"v1", "v2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q should name both versions; %q is missing", err, want)
		}
	}
}

func TestDecideWithNoWorkerIsALivenessFailure(t *testing.T) {
	_, err := newGateway().Decider(testAgent).Decide(context.Background(), testRun(), nil)
	if !errors.Is(err, ErrNoWorker) {
		t.Fatalf("err = %v, want ErrNoWorker", err)
	}
}

// TestAToolOnlyWorkerIsNeverAskedToDecide. A process that hosts tools but has
// no agent body has nothing to say about what happens next, and asking it
// would block until the call timed out.
func TestAToolOnlyWorkerIsNeverAskedToDecide(t *testing.T) {
	gw := newGateway()

	reg := registration(queryableTool("send_invoice"))
	reg.Decides = false
	connect(t, gw, reg, nil)

	_, err := gw.Decider(testAgent).Decide(context.Background(), testRun(), nil)
	if !errors.Is(err, ErrNoWorker) {
		t.Fatalf("err = %v, want ErrNoWorker", err)
	}
	if !strings.Contains(err.Error(), "none of them decides") {
		t.Fatalf("error %q should say why", err)
	}
}

// --- executing ------------------------------------------------------------

func toolCall() core.ToolCall {
	return core.ToolCall{
		RunID:   "run-1",
		TaskID:  "task-1",
		StepID:  core.Step(1),
		Key:     core.NewIdempotencyKey("run-1", core.Step(1), 1),
		Payload: []byte(`{"customer_id":42}`),
		Attempt: 1,
	}
}

func TestExecuteCarriesTheIdempotencyKeyAndReturnsTheResult(t *testing.T) {
	gw := newGateway()

	keys := make(chan string, 1)
	connect(t, gw, registration(queryableTool("send_invoice")), func(msg *pb.ServerMessage) *pb.ClientMessage {
		req := msg.GetExecute()
		if req == nil {
			return nil
		}
		select {
		case keys <- req.GetIdempotencyKey():
		default:
		}
		return &pb.ClientMessage{Message: &pb.ClientMessage_ExecuteResult{
			ExecuteResult: &pb.ExecuteResult{
				CallId:  req.GetCallId(),
				Outcome: &pb.ExecuteResult_Result{Result: []byte(`{"reference":"inv_1"}`)},
			},
		}}
	})

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	got, err := d.Handler(context.Background(), toolCall())
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if string(got) != `{"reference":"inv_1"}` {
		t.Fatalf("result = %s", got)
	}
	// Without the key on the wire, a tool declaring IDEMPOTENT_BY_KEY or
	// QUERYABLE is making a claim it has no way to honour.
	if k := <-keys; k != "run-1:S1:E1" {
		t.Fatalf("the worker was handed key %q, want run-1:S1:E1", k)
	}
}

func TestLookupCarriesTheDeclaredClassAndTTL(t *testing.T) {
	gw := newGateway()
	connect(t, gw, registration(queryableTool("send_invoice")), nil)

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if d.Class != core.ClassQueryable {
		t.Fatalf("Class = %s, want %s", d.Class, core.ClassQueryable)
	}
	if d.KeyTTL != 24*time.Hour {
		t.Fatalf("KeyTTL = %s, want 24h", d.KeyTTL)
	}
	if d.Reconciler == nil {
		t.Fatal("a QUERYABLE tool came back with no reconciler")
	}
}

// TestAFailedToolKeepsItsCertainty. The worker's claim about whether anything
// was transmitted has to survive the gateway, or the ledger is classifying on
// invented information.
func TestAFailedToolKeepsItsCertainty(t *testing.T) {
	tests := []struct {
		name      string
		certainty pb.Certainty
		want      core.EffectStatus
	}{
		{"asserted local", pb.Certainty_CERTAINTY_NOT_EXECUTED, core.EffectFailed},
		{"ambiguous", pb.Certainty_CERTAINTY_UNKNOWN, core.EffectUnknown},
		{"unset", pb.Certainty_CERTAINTY_UNSPECIFIED, core.EffectUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gw := newGateway()
			connect(t, gw, registration(queryableTool("send_invoice")), func(msg *pb.ServerMessage) *pb.ClientMessage {
				req := msg.GetExecute()
				if req == nil {
					return nil
				}
				return &pb.ClientMessage{Message: &pb.ClientMessage_ExecuteResult{
					ExecuteResult: &pb.ExecuteResult{
						CallId: req.GetCallId(),
						Outcome: &pb.ExecuteResult_Failure{Failure: &pb.Failure{
							Message:   "billing said no",
							Type:      "BillingError",
							Certainty: tc.certainty,
						}},
					},
				}}
			})

			d, err := gw.Registry(testAgent).Lookup("send_invoice")
			if err != nil {
				t.Fatalf("Lookup: %v", err)
			}
			_, callErr := d.Handler(context.Background(), toolCall())
			if callErr == nil {
				t.Fatal("a reported failure came back as success")
			}
			if got := core.ClassifyFailure(callErr); got != tc.want {
				t.Fatalf("classified as %s, want %s", got, tc.want)
			}
			if !strings.Contains(callErr.Error(), "BillingError") {
				t.Fatalf("error %q lost the worker's exception type", callErr)
			}
		})
	}
}

// TestAWorkerDyingMidCallIsAmbiguous is the case the ledger exists for. The
// worker may have sent the invoice and died before answering, so this must not
// come back as proof that nothing happened.
func TestAWorkerDyingMidCallIsAmbiguous(t *testing.T) {
	gw := newGateway()

	w := connect(t, gw, registration(queryableTool("send_invoice")), nil)
	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, callErr := d.Handler(context.Background(), toolCall())
		done <- callErr
	}()

	// Let the request reach the worker, then kill it without answering.
	<-w.stream.toClient
	w.stream.hangUp()

	select {
	case callErr := <-done:
		if callErr == nil {
			t.Fatal("the call succeeded despite the worker dying")
		}
		if core.IsNotExecuted(callErr) {
			t.Fatal("a worker dying mid-call was read as proof the action did not happen")
		}
		if got := core.ClassifyFailure(callErr); got != core.EffectUnknown {
			t.Fatalf("classified as %s, want %s", got, core.EffectUnknown)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the call did not return after the worker disconnected; " +
			"a caller blocked on a dead stream holds a lease it cannot renew")
	}
}

// TestACancelledCallIsAmbiguousToo. The worker's lease was lost or the process
// is shutting down; either way the request may already be executing.
func TestACancelledCallIsAmbiguousToo(t *testing.T) {
	gw := newGateway()
	connect(t, gw, registration(queryableTool("send_invoice")), func(*pb.ServerMessage) *pb.ClientMessage {
		return nil // never answers
	})

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, callErr := d.Handler(ctx, toolCall())
	if callErr == nil {
		t.Fatal("a call that was never answered returned success")
	}
	if core.IsNotExecuted(callErr) {
		t.Fatal("a cancelled call was read as proof the action did not happen")
	}
}

func TestUnknownToolIsNotFound(t *testing.T) {
	gw := newGateway()
	connect(t, gw, registration(queryableTool("send_invoice")), nil)

	if _, err := gw.Registry(testAgent).Lookup("refund"); !errors.Is(err, core.ErrToolNotFound) {
		t.Fatalf("err = %v, want ErrToolNotFound", err)
	}
}

// TestWorkersDisagreeingAboutAClassIsRefused. One process treating a send as
// QUERYABLE and another as UNRECONCILABLE is the failure that is invisible
// until an outage; here it is a refusal naming both claims.
func TestWorkersDisagreeingAboutAClassIsRefused(t *testing.T) {
	gw := newGateway()

	connect(t, gw, registration(queryableTool("send_invoice")), nil)

	second := registration(&pb.ToolDescriptor{
		Name:        "send_invoice",
		EffectClass: pb.EffectClass_EFFECT_CLASS_UNRECONCILABLE,
	})
	second.WorkerId = "w-2"
	connect(t, gw, second, nil)

	_, err := gw.Registry(testAgent).Lookup("send_invoice")
	if !errors.Is(err, wire.ErrProtocol) {
		t.Fatalf("err = %v, want a protocol error", err)
	}
	for _, want := range []string{"QUERYABLE", "UNRECONCILABLE", "w-1", "w-2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q should name %q", err, want)
		}
	}
}

// --- reconciling ----------------------------------------------------------

func TestReconcileAsksTheProviderAndReturnsWhatItSaid(t *testing.T) {
	gw := newGateway()

	connect(t, gw, registration(queryableTool("send_invoice")), func(msg *pb.ServerMessage) *pb.ClientMessage {
		req := msg.GetReconcile()
		if req == nil {
			return nil
		}
		if got := req.GetEffect().GetIdempotencyKey(); got != "run-1:S1:E1" {
			t.Errorf("the hook was asked about key %q, want run-1:S1:E1", got)
		}
		return &pb.ClientMessage{Message: &pb.ClientMessage_ReconcileResult{
			ReconcileResult: &pb.ReconcileResult{
				CallId: req.GetCallId(),
				Outcome: &pb.ReconcileResult_Resolution{Resolution: &pb.Resolution{
					Kind:        pb.ResolutionKind_RESOLUTION_KIND_COMMITTED,
					ExternalRef: "inv_77",
					Detail:      "found by idempotency key",
				}},
			},
		}}
	})

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	got, err := d.Reconciler.Reconcile(context.Background(), core.Effect{
		Key:   core.NewIdempotencyKey("run-1", core.Step(1), 1),
		RunID: "run-1",
		Class: core.ClassQueryable,
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got.Kind != core.ResolvedCommitted || got.ExternalRef != "inv_77" {
		t.Fatalf("got %+v", got)
	}
}

// TestAFailedReconcilerEstablishesNothing. Returning an error leaves the
// effect UNKNOWN, which is what it is; the sweep will ask again.
func TestAFailedReconcilerEstablishesNothing(t *testing.T) {
	gw := newGateway()

	connect(t, gw, registration(queryableTool("send_invoice")), func(msg *pb.ServerMessage) *pb.ClientMessage {
		req := msg.GetReconcile()
		if req == nil {
			return nil
		}
		return &pb.ClientMessage{Message: &pb.ClientMessage_ReconcileResult{
			ReconcileResult: &pb.ReconcileResult{
				CallId:  req.GetCallId(),
				Outcome: &pb.ReconcileResult_Failure{Failure: &pb.Failure{Message: "billing is down"}},
			},
		}}
	})

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	got, err := d.Reconciler.Reconcile(context.Background(), core.Effect{Class: core.ClassQueryable})
	if err == nil {
		t.Fatal("a reconciler that could not reach the provider reported success")
	}
	if got.Kind == core.ResolvedCommitted || got.Kind == core.ResolvedNotExecuted {
		t.Fatalf("a failed reconciler returned a verdict: %s", got.Kind)
	}
}

// --- concurrency ----------------------------------------------------------

// TestCallsAreConcurrentOverOneStream. The runtime does not wait for one tool
// call before issuing the next, so a worker serving several at once must see
// its replies matched by call id rather than by arrival order.
func TestCallsAreConcurrentOverOneStream(t *testing.T) {
	gw := newGateway()

	connect(t, gw, registration(queryableTool("send_invoice")), func(msg *pb.ServerMessage) *pb.ClientMessage {
		req := msg.GetExecute()
		if req == nil {
			return nil
		}
		// Echo the key back, so a mismatched reply would be detectable rather
		// than merely late.
		return &pb.ClientMessage{Message: &pb.ClientMessage_ExecuteResult{
			ExecuteResult: &pb.ExecuteResult{
				CallId:  req.GetCallId(),
				Outcome: &pb.ExecuteResult_Result{Result: []byte(`"` + req.GetIdempotencyKey() + `"`)},
			},
		}}
	})

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	const calls = 25
	var wg sync.WaitGroup
	errs := make(chan error, calls)

	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			call := toolCall()
			call.StepID = core.Step(i + 1)
			call.Key = core.NewIdempotencyKey(call.RunID, call.StepID, 1)

			got, err := d.Handler(context.Background(), call)
			switch {
			case err != nil:
				errs <- err
			case string(got) != `"`+string(call.Key)+`"`:
				errs <- errors.New("reply was matched to the wrong call: got " + string(got) +
					", want " + string(call.Key))
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestWorkIsSpreadAcrossWorkers. Not a correctness property — one worker doing
// everything would still be correct — but a runtime that ignores every worker
// but the first is broken in a way nothing else would report.
func TestWorkIsSpreadAcrossWorkers(t *testing.T) {
	gw := newGateway()

	seen := make(chan string, 8)
	for _, id := range []string{"w-1", "w-2", "w-3"} {
		reg := registration(queryableTool("send_invoice"))
		reg.WorkerId = id
		worker := id
		connect(t, gw, reg, func(msg *pb.ServerMessage) *pb.ClientMessage {
			req := msg.GetExecute()
			if req == nil {
				return nil
			}
			seen <- worker
			return &pb.ClientMessage{Message: &pb.ClientMessage_ExecuteResult{
				ExecuteResult: &pb.ExecuteResult{
					CallId:  req.GetCallId(),
					Outcome: &pb.ExecuteResult_Result{Result: []byte(`{}`)},
				},
			}}
		})
	}

	d, err := gw.Registry(testAgent).Lookup("send_invoice")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	distinct := make(map[string]bool)
	for i := 0; i < 6; i++ {
		if _, err := d.Handler(context.Background(), toolCall()); err != nil {
			t.Fatalf("Handler: %v", err)
		}
		distinct[<-seen] = true
	}
	if len(distinct) != 3 {
		t.Fatalf("six calls reached %d of 3 workers: %v", len(distinct), distinct)
	}
}

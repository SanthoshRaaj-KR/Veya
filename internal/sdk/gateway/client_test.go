package gateway_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/gateway"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// A worker written in Go, speaking the same .proto as the Python SDK, over a
// real socket.
//
// It is the guard on the coupling risk the plan named: the Go runtime growing
// Python-shaped assumptions about payload encoding or error formats. A second
// implementation in a different language is what would catch that, and a
// second Go implementation catches it more cheaply than a second real
// language would — it shares no code with either the gateway or the SDK, only
// the contract.
//
// It is also the only test that goes over TCP. Everything in gateway_test.go
// drives serve() directly, which is right for protocol logic and wrong for
// finding out whether the service is actually registered on the server.

type goWorker struct {
	t      *testing.T
	stream grpc.BidiStreamingClient[pb.ClientMessage, pb.ServerMessage]
	conn   *grpc.ClientConn
	cancel context.CancelFunc

	ready chan struct{}
	done  chan struct{}

	sendMu sync.Mutex

	onDecide    func(*pb.DecideRequest) *pb.DecideResult
	onExecute   func(*pb.ExecuteRequest) *pb.ExecuteResult
	onReconcile func(*pb.ReconcileRequest) *pb.ReconcileResult
}

type goWorkerConfig struct {
	addr        string
	agent       string
	version     string
	workerID    string
	decides     bool
	tools       []*pb.ToolDescriptor
	onDecide    func(*pb.DecideRequest) *pb.DecideResult
	onExecute   func(*pb.ExecuteRequest) *pb.ExecuteResult
	onReconcile func(*pb.ReconcileRequest) *pb.ReconcileResult
}

// dial connects a Go worker and waits for its registration to be acknowledged.
func dial(t *testing.T, cfg goWorkerConfig) *goWorker {
	t.Helper()

	conn, err := grpc.NewClient(cfg.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", cfg.addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := pb.NewWorkerClient(conn).Session(ctx)
	if err != nil {
		cancel()
		_ = conn.Close()
		t.Fatalf("open session: %v", err)
	}

	w := &goWorker{
		t:           t,
		stream:      stream,
		conn:        conn,
		cancel:      cancel,
		ready:       make(chan struct{}),
		done:        make(chan struct{}),
		onDecide:    cfg.onDecide,
		onExecute:   cfg.onExecute,
		onReconcile: cfg.onReconcile,
	}

	if err := stream.Send(&pb.ClientMessage{Message: &pb.ClientMessage_Register{
		Register: &pb.Register{
			ProtocolVersion: pb.ProtocolVersion_PROTOCOL_VERSION_1,
			AgentName:       cfg.agent,
			AgentVersion:    cfg.version,
			WorkerId:        cfg.workerID,
			Runtime:         "go (test worker)",
			Decides:         cfg.decides,
			Tools:           cfg.tools,
		},
	}}); err != nil {
		cancel()
		_ = conn.Close()
		t.Fatalf("send registration: %v", err)
	}

	go w.serve()
	t.Cleanup(w.stop)

	select {
	case <-w.ready:
	case <-w.done:
		t.Fatal("the session ended before the registration was acknowledged")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the registration acknowledgement")
	}
	return w
}

func (w *goWorker) serve() {
	defer close(w.done)

	for {
		message, err := w.stream.Recv()
		if err != nil {
			return
		}

		switch m := message.GetMessage().(type) {
		case *pb.ServerMessage_Registered:
			close(w.ready)

		case *pb.ServerMessage_Decide:
			// Answered on a goroutine, like the Python SDK does, so that a
			// slow handler cannot stall the stream.
			go w.reply(&pb.ClientMessage{Message: &pb.ClientMessage_DecideResult{
				DecideResult: w.onDecide(m.Decide),
			}})

		case *pb.ServerMessage_Execute:
			go w.reply(&pb.ClientMessage{Message: &pb.ClientMessage_ExecuteResult{
				ExecuteResult: w.onExecute(m.Execute),
			}})

		case *pb.ServerMessage_Reconcile:
			go w.reply(&pb.ClientMessage{Message: &pb.ClientMessage_ReconcileResult{
				ReconcileResult: w.onReconcile(m.Reconcile),
			}})
		}
	}
}

func (w *goWorker) reply(message *pb.ClientMessage) {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	_ = w.stream.Send(message)
}

func (w *goWorker) stop() {
	w.cancel()
	_ = w.conn.Close()
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		w.t.Error("the Go worker's session did not end")
	}
}

// --- a served gateway -----------------------------------------------------

func listen(t *testing.T) (*gateway.Server, string) {
	t.Helper()

	gw := gateway.New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Port 0: the OS picks, and Addr reports it. A fixed port would make two
	// packages' tests unable to run at once.
	server, err := gateway.Listen("127.0.0.1:0", gw, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("gateway.Listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Run(ctx); err != nil {
			t.Errorf("gateway.Run: %v", err)
		}
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			server.Close()
		}
	})
	return server, server.Addr()
}

// --- the protocol, over a socket -----------------------------------------

func TestListenBindsAndReportsItsAddress(t *testing.T) {
	server, addr := listen(t)

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("Addr() = %q, which is not host:port: %v", addr, err)
	}
	if host != "127.0.0.1" {
		// The protocol has no authentication. A port that accepts
		// unauthenticated tool registrations must not be on a wider interface
		// unless someone asked for that on purpose.
		t.Fatalf("bound to %q, want loopback", host)
	}
	if port == "0" {
		t.Fatal("Addr() reports port 0; a test could never find the real one")
	}
	_ = server
}

func TestAPortAlreadyInUseFailsAtStartup(t *testing.T) {
	_, addr := listen(t)

	// Not a goroutine failing quietly a moment after the process claims to be
	// ready: an error the operator sees, from the call that binds.
	if _, err := gateway.Listen(addr, gateway.New(nil), nil); err == nil {
		t.Fatal("binding an address already in use succeeded")
	}
}

func TestAGoWorkerSpeaksTheSameProtocol(t *testing.T) {
	server, addr := listen(t)
	gw := server.Gateway()

	w := dial(t, goWorkerConfig{
		addr:     addr,
		agent:    "go_agent",
		version:  "v1",
		workerID: "go-1",
		decides:  true,
		tools: []*pb.ToolDescriptor{{
			Name:          "send",
			EffectClass:   pb.EffectClass_EFFECT_CLASS_QUERYABLE,
			KeyTtlSeconds: int64(24 * time.Hour / time.Second),
			Reconcilable:  true,
		}},
		onDecide: func(req *pb.DecideRequest) *pb.DecideResult {
			return &pb.DecideResult{
				CallId: req.GetCallId(),
				Outcome: &pb.DecideResult_Decision{Decision: &pb.Decision{
					Kind:    pb.DecisionKind_DECISION_KIND_CALL_TOOL,
					StepId:  "S1",
					Tool:    "send",
					Payload: []byte(`{"to":"ana@example.com"}`),
				}},
			}
		},
		onExecute: func(req *pb.ExecuteRequest) *pb.ExecuteResult {
			return &pb.ExecuteResult{
				CallId:  req.GetCallId(),
				Outcome: &pb.ExecuteResult_Result{Result: []byte(`{"reference":"` + req.GetIdempotencyKey() + `"}`)},
			}
		},
		onReconcile: func(req *pb.ReconcileRequest) *pb.ReconcileResult {
			return &pb.ReconcileResult{
				CallId: req.GetCallId(),
				Outcome: &pb.ReconcileResult_Resolution{Resolution: &pb.Resolution{
					Kind:        pb.ResolutionKind_RESOLUTION_KIND_COMMITTED,
					ExternalRef: req.GetEffect().GetIdempotencyKey(),
				}},
			}
		},
	})
	_ = w

	ctx := context.Background()
	run := core.Run{ID: "run-1", AgentName: "go_agent", AgentVersion: "v1", Status: core.RunRunning}

	decision, err := gw.Decider("go_agent").Decide(ctx, run, nil)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if decision.Kind != core.DecideCallTool || decision.TaskType != "send" {
		t.Fatalf("decision = %+v", decision)
	}

	descriptor, err := gw.Registry("go_agent").Lookup("send")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if descriptor.Class != core.ClassQueryable || descriptor.KeyTTL != 24*time.Hour {
		t.Fatalf("descriptor = %+v", descriptor)
	}

	key := core.NewIdempotencyKey("run-1", core.Step(1), 1)
	result, err := descriptor.Handler(ctx, core.ToolCall{
		RunID: "run-1", TaskID: "task-1", StepID: core.Step(1), Key: key, Attempt: 1,
		Payload: []byte(`{"to":"ana@example.com"}`),
	})
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if string(result) != `{"reference":"run-1:S1:E1"}` {
		t.Fatalf("result = %s; the key did not reach the tool intact", result)
	}

	resolution, err := descriptor.Reconciler.Reconcile(ctx, core.Effect{Key: key, Class: core.ClassQueryable})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if resolution.Kind != core.ResolvedCommitted || resolution.ExternalRef != string(key) {
		t.Fatalf("resolution = %+v", resolution)
	}
}

// TestCertaintySurvivesTheSocket. The same assertion the in-process tests
// make, over TCP, because this is the bit that must not be lost in transit.
func TestCertaintySurvivesTheSocket(t *testing.T) {
	server, addr := listen(t)

	dial(t, goWorkerConfig{
		addr: addr, agent: "go_agent", version: "v1", workerID: "go-1",
		tools: []*pb.ToolDescriptor{{
			Name:        "send",
			EffectClass: pb.EffectClass_EFFECT_CLASS_IDEMPOTENT_BY_KEY,
		}},
		onExecute: func(req *pb.ExecuteRequest) *pb.ExecuteResult {
			// wire.FailureFrom is the inverse the SDK's _failure() mirrors.
			return &pb.ExecuteResult{
				CallId:  req.GetCallId(),
				Outcome: &pb.ExecuteResult_Failure{Failure: wire.FailureFrom(core.NotExecuted(errors.New("payload would not serialize")))},
			}
		},
	})

	descriptor, err := server.Gateway().Registry("go_agent").Lookup("send")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	_, callErr := descriptor.Handler(context.Background(), core.ToolCall{Key: "k"})

	if !core.IsNotExecuted(callErr) {
		t.Fatalf("the NOT_EXECUTED assertion was lost over the wire: %v", callErr)
	}
	if got := core.ClassifyFailure(callErr); got != core.EffectFailed {
		t.Fatalf("classified as %s, want %s", got, core.EffectFailed)
	}
}

// TestARefusedRegistrationClosesTheStreamWithAReason. A worker whose
// declaration cannot be trusted has to be told what was wrong with it, in a
// status it can print, because no amount of retrying will fix it.
func TestARefusedRegistrationClosesTheStreamWithAReason(t *testing.T) {
	_, addr := listen(t)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewWorkerClient(conn).Session(context.Background())
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	// QUERYABLE with no reconcile hook: a tool that cannot be asked what it
	// did is UNRECONCILABLE, whatever it calls itself.
	if err := stream.Send(&pb.ClientMessage{Message: &pb.ClientMessage_Register{
		Register: &pb.Register{
			ProtocolVersion: pb.ProtocolVersion_PROTOCOL_VERSION_1,
			AgentName:       "go_agent",
			AgentVersion:    "v1",
			Tools: []*pb.ToolDescriptor{{
				Name:        "send",
				EffectClass: pb.EffectClass_EFFECT_CLASS_QUERYABLE,
			}},
		},
	}}); err != nil {
		t.Fatalf("send: %v", err)
	}

	_, err = stream.Recv()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Recv = %v, want a status explaining the refusal", err)
	}
	if !strings.Contains(err.Error(), "UNRECONCILABLE") {
		t.Fatalf("the refusal was %q, which does not say what the tool actually is", err)
	}
}

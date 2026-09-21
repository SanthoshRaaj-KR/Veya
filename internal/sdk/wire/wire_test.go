package wire_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// TestUnsetCertaintyIsUnknown is the most important test in this package.
//
// A worker that does not set Certainty — because it forgot, or because it was
// built against a contract revision that did not have the field — must be
// understood as saying "I do not know whether this happened". The opposite
// default would let a forgotten field silently authorise a retry of something
// that already took effect, and there is no mechanism anywhere else in the
// system that would catch it.
func TestUnsetCertaintyIsUnknown(t *testing.T) {
	err := wire.Failure(&pb.Failure{Message: "connection reset"})

	if err == nil {
		t.Fatal("a reported failure converted to a nil error")
	}
	if core.IsNotExecuted(err) {
		t.Fatal("an unset Certainty was read as NOT_EXECUTED; it must mean UNKNOWN")
	}
	if got := core.ClassifyFailure(err); got != core.EffectUnknown {
		t.Fatalf("classified as %s, want %s", got, core.EffectUnknown)
	}
}

// TestCertaintyCarriesTheAssertion covers the other half: a worker that can
// prove nothing was sent says so, and that assertion survives the wire.
func TestCertaintyCarriesTheAssertion(t *testing.T) {
	tests := []struct {
		name      string
		certainty pb.Certainty
		want      core.EffectStatus
	}{
		{"unspecified", pb.Certainty_CERTAINTY_UNSPECIFIED, core.EffectUnknown},
		{"unknown", pb.Certainty_CERTAINTY_UNKNOWN, core.EffectUnknown},
		{"not executed", pb.Certainty_CERTAINTY_NOT_EXECUTED, core.EffectFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := wire.Failure(&pb.Failure{Message: "boom", Certainty: tc.certainty})
			if got := core.ClassifyFailure(err); got != tc.want {
				t.Fatalf("classified as %s, want %s", got, tc.want)
			}
		})
	}
}

// TestFailureRoundTrips checks that a Go worker's error and the runtime's
// reading of it agree. The Go test worker in the gateway suite depends on
// this being an exact inverse.
func TestFailureRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ambiguous", errors.New("upstream timed out")},
		{"provably local", core.NotExecuted(errors.New("payload would not serialize"))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := wire.Failure(wire.FailureFrom(tc.err))

			if core.IsNotExecuted(got) != core.IsNotExecuted(tc.err) {
				t.Fatalf("NotExecuted survived as %v, want %v",
					core.IsNotExecuted(got), core.IsNotExecuted(tc.err))
			}
			if !strings.Contains(got.Error(), tc.err.Error()) {
				t.Fatalf("message %q lost the original %q", got, tc.err)
			}
		})
	}
}

// TestFailureWithNoDetailIsAProtocolError. A worker reporting a failure with
// no failure in it is broken, and saying so beats inventing a cause.
func TestFailureWithNoDetailIsAProtocolError(t *testing.T) {
	if err := wire.Failure(nil); !errors.Is(err, wire.ErrProtocol) {
		t.Fatalf("Failure(nil) = %v, want a protocol error", err)
	}
}

// TestUnknownResolutionKindIsStillUnknown. An answer this build cannot name
// must not be read as either "it happened" or "it did not". STILL_UNKNOWN
// leads to escalation, which is where an unnameable outcome belongs.
func TestUnknownResolutionKindIsStillUnknown(t *testing.T) {
	for _, r := range []*pb.Resolution{
		nil,
		{},
		{Kind: pb.ResolutionKind_RESOLUTION_KIND_UNSPECIFIED},
		{Kind: pb.ResolutionKind(999)},
	} {
		if got := wire.Resolution(r).Kind; got != core.ResolvedUnknown {
			t.Fatalf("Resolution(%v).Kind = %s, want %s", r, got, core.ResolvedUnknown)
		}
	}
}

func TestResolutionCarriesTheProvidersAnswer(t *testing.T) {
	got := wire.Resolution(&pb.Resolution{
		Kind:        pb.ResolutionKind_RESOLUTION_KIND_COMMITTED,
		Response:    []byte(`{"ok":true}`),
		ExternalRef: "msg_9001",
		Detail:      "found by idempotency key",
	})

	if got.Kind != core.ResolvedCommitted {
		t.Fatalf("Kind = %s, want %s", got.Kind, core.ResolvedCommitted)
	}
	if got.ExternalRef != "msg_9001" {
		t.Fatalf("ExternalRef = %q, want msg_9001", got.ExternalRef)
	}
	if string(got.Response) != `{"ok":true}` {
		t.Fatalf("Response = %s, want the provider's body verbatim", got.Response)
	}
}

// TestQueryableWithoutAReconcilerIsRefused pins the rule at the wire edge.
// internal/tool panics on this for Go tools; a Python tool must not be able to
// claim what a Go tool would be panicked for.
func TestQueryableWithoutAReconcilerIsRefused(t *testing.T) {
	_, err := wire.ToolDescriptor(&pb.ToolDescriptor{
		Name:        "send_summary",
		EffectClass: pb.EffectClass_EFFECT_CLASS_QUERYABLE,
	})
	if !errors.Is(err, wire.ErrProtocol) {
		t.Fatalf("err = %v, want a protocol error", err)
	}
	if !strings.Contains(err.Error(), "UNRECONCILABLE") {
		t.Fatalf("error %q should say what the tool actually is", err)
	}
}

// TestUnspecifiedEffectClassIsRefused. Defaulting would be wrong in both
// directions: NONE bypasses the ledger, UNRECONCILABLE escalates to a human
// who was never told why.
func TestUnspecifiedEffectClassIsRefused(t *testing.T) {
	if _, err := wire.EffectClassFrom(pb.EffectClass_EFFECT_CLASS_UNSPECIFIED); !errors.Is(err, wire.ErrProtocol) {
		t.Fatalf("err = %v, want a protocol error", err)
	}
}

func TestEffectClassRoundTrips(t *testing.T) {
	for _, class := range []core.EffectClass{
		core.ClassNone,
		core.ClassIdempotentByKey,
		core.ClassQueryable,
		core.ClassUnreconcilable,
	} {
		got, err := wire.EffectClassFrom(wire.EffectClassTo(class))
		if err != nil {
			t.Fatalf("%s: %v", class, err)
		}
		if got != class {
			t.Fatalf("%s round-tripped to %s", class, got)
		}
	}
}

func TestToolDescriptorCarriesKeyTTL(t *testing.T) {
	got, err := wire.ToolDescriptor(&pb.ToolDescriptor{
		Name:          "create_refund",
		EffectClass:   pb.EffectClass_EFFECT_CLASS_IDEMPOTENT_BY_KEY,
		KeyTtlSeconds: int64(24 * time.Hour / time.Second),
	})
	if err != nil {
		t.Fatalf("ToolDescriptor: %v", err)
	}
	if got.KeyTTL != 24*time.Hour {
		t.Fatalf("KeyTTL = %s, want 24h", got.KeyTTL)
	}
	if got.Handler != nil {
		t.Fatal("the wire conversion must not invent a handler; the gateway supplies it")
	}
}

func TestDecisionRequiresWhatItsKindNeeds(t *testing.T) {
	tests := []struct {
		name string
		in   *pb.Decision
		want string
	}{
		{"nil", nil, "nil decision"},
		{"unset kind", &pb.Decision{}, "not one this build knows"},
		{
			"call with no tool",
			&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_CALL_TOOL, StepId: "S1"},
			"names no tool",
		},
		{
			"call with no step",
			&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_CALL_TOOL, Tool: "send"},
			"no step id",
		},
		{
			"compensate, which this build does not implement",
			&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_COMPENSATE},
			"not one this build knows",
		},
		{
			"fan-out with no calls",
			&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL, StepId: "S1"},
			"makes no calls",
		},
		{
			"fan-out with no parent step",
			&pb.Decision{
				Kind:  pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL,
				Calls: []*pb.ToolCall{{Tool: "check"}},
			},
			"no parent step id",
		},
		{
			"fan-out with a nameless call",
			&pb.Decision{
				Kind:   pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL,
				StepId: "S1",
				Calls:  []*pb.ToolCall{{Tool: "check"}, {}},
				Join:   &pb.JoinPolicy{Kind: pb.JoinKind_JOIN_KIND_ALL},
			},
			"call 1 names no tool",
		},
		{
			"fan-out with no join policy",
			&pb.Decision{
				Kind:   pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL,
				StepId: "S1",
				Calls:  []*pb.ToolCall{{Tool: "check"}},
			},
			"no join policy",
		},
		{
			"fan-out with an unspecified join kind",
			&pb.Decision{
				Kind:   pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL,
				StepId: "S1",
				Calls:  []*pb.ToolCall{{Tool: "check"}},
				Join:   &pb.JoinPolicy{},
			},
			"not one this build knows",
		},
		{
			"quorum bigger than the fan-out",
			&pb.Decision{
				Kind:   pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL,
				StepId: "S1",
				Calls:  []*pb.ToolCall{{Tool: "check"}, {Tool: "check"}},
				Join:   &pb.JoinPolicy{Kind: pb.JoinKind_JOIN_KIND_QUORUM, Quorum: 3},
			},
			"can never be satisfied",
		},
		{
			"sleep with no instant",
			&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_SLEEP, StepId: "S1"},
			"names no instant",
		},
		{
			"wait with no signal name",
			&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_WAIT_FOR_SIGNAL, StepId: "S1"},
			"names no signal",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := wire.Decision(tc.in)
			if !errors.Is(err, wire.ErrProtocol) {
				t.Fatalf("err = %v, want a protocol error", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestDecisionConverts(t *testing.T) {
	call, err := wire.Decision(&pb.Decision{
		Kind:    pb.DecisionKind_DECISION_KIND_CALL_TOOL,
		StepId:  "S2",
		Tool:    "summarize",
		Payload: []byte(`{"style":"brief"}`),
	})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if call.Kind != core.DecideCallTool || call.StepID != core.Step(2) || call.TaskType != "summarize" {
		t.Fatalf("got %+v", call)
	}

	done, err := wire.Decision(&pb.Decision{
		Kind:   pb.DecisionKind_DECISION_KIND_COMPLETE,
		Output: []byte(`{"sent":true}`),
	})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if done.Kind != core.DecideComplete || string(done.Output) != `{"sent":true}` {
		t.Fatalf("got %+v", done)
	}
}

// TestFailWithNoReasonStillFails. Refusing a FAIL because it carried no words
// would leave the run advancing as though nothing had gone wrong.
func TestFailWithNoReasonStillFails(t *testing.T) {
	got, err := wire.Decision(&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_FAIL})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if got.Kind != core.DecideFail {
		t.Fatalf("Kind = %s, want %s", got.Kind, core.DecideFail)
	}
	if got.Error == "" {
		t.Fatal("a failure with no reason must still carry something an operator can read")
	}
}

// TestCancelCarriesItsReason. Unlike FAIL, a CANCEL with no reason is left
// unexplained rather than given an invented one: cancelling for no stated
// reason is itself something a body can mean.
func TestCancelCarriesItsReason(t *testing.T) {
	got, err := wire.Decision(&pb.Decision{
		Kind:         pb.DecisionKind_DECISION_KIND_CANCEL,
		CancelReason: "the customer withdrew the request",
	})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if got.Kind != core.DecideCancel {
		t.Fatalf("Kind = %s, want %s", got.Kind, core.DecideCancel)
	}
	if got.Reason != "the customer withdrew the request" {
		t.Fatalf("Reason = %q", got.Reason)
	}

	unexplained, err := wire.Decision(&pb.Decision{Kind: pb.DecisionKind_DECISION_KIND_CANCEL})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if unexplained.Reason != "" {
		t.Fatalf("Reason = %q, want an unexplained cancel left as such", unexplained.Reason)
	}
}

// TestEventPayloadCrossesVerbatim. The envelope is the client's to interpret,
// including its version field, so this conversion must not touch it.
func TestEventPayloadCrossesVerbatim(t *testing.T) {
	ev, err := core.NewEvent("run-1", 3, core.EventTaskCompleted, core.Step(1),
		core.TaskCompletedData{TaskID: "task-1", Result: json.RawMessage(`{"n":1}`)})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	ev.CreatedAt = time.Unix(1700000000, 0)

	got := wire.Event(ev)

	if string(got.GetPayload()) != string(ev.Payload) {
		t.Fatalf("payload was reshaped:\n got %s\nwant %s", got.GetPayload(), ev.Payload)
	}
	if got.GetSeq() != 3 || got.GetType() != string(core.EventTaskCompleted) || got.GetStepId() != "S1" {
		t.Fatalf("got %+v", got)
	}
	if got.GetCreatedAtUnixNano() != ev.CreatedAt.UnixNano() {
		t.Fatalf("CreatedAt = %d, want %d", got.GetCreatedAtUnixNano(), ev.CreatedAt.UnixNano())
	}
}

// TestZeroTimeIsZeroOnTheWire. time.Time's zero value has a large negative
// UnixNano, which would read on the other side as a date in 1754 rather than
// as "unset".
func TestZeroTimeIsZeroOnTheWire(t *testing.T) {
	if got := wire.Event(core.Event{}).GetCreatedAtUnixNano(); got != 0 {
		t.Fatalf("zero time crossed as %d, want 0", got)
	}
	if got := wire.Effect(core.Effect{}).GetCreatedAtUnixNano(); got != 0 {
		t.Fatalf("zero time crossed as %d, want 0", got)
	}
}

// TestRunDoesNotCarryItsVersion. The advancement counter is the engine's
// concurrency control; a client reasoning about it is a client about to
// serialize advancement a second time, incorrectly.
func TestRunDoesNotCarryItsVersion(t *testing.T) {
	got := wire.Run(core.Run{
		ID:           "run-7",
		AgentName:    "refund_agent",
		AgentVersion: "v1",
		Status:       core.RunRunning,
		Version:      42,
		Input:        json.RawMessage(`{"order_id":987}`),
	})

	if got.GetRunId() != "run-7" || got.GetAgentVersion() != "v1" {
		t.Fatalf("got %+v", got)
	}
	// There is no field to check; the assertion is that the generated struct
	// has none, which the compiler enforces. This documents why.
	if string(got.GetInput()) != `{"order_id":987}` {
		t.Fatalf("Input = %s", got.GetInput())
	}
}

func TestExecuteRequestCarriesTheKey(t *testing.T) {
	got := wire.ExecuteRequest("c-1", "send_summary", core.ToolCall{
		RunID:   "run-7",
		TaskID:  "task-9",
		StepID:  core.Step(3),
		Key:     core.NewIdempotencyKey("run-7", core.Step(3), 1),
		Payload: []byte(`{"channel":"email"}`),
		Attempt: 2,
	})

	if got.GetIdempotencyKey() != "run-7:S3:E1" {
		t.Fatalf("IdempotencyKey = %q, want run-7:S3:E1", got.GetIdempotencyKey())
	}
	if got.GetAttempt() != 2 || got.GetTool() != "send_summary" || got.GetCallId() != "c-1" {
		t.Fatalf("got %+v", got)
	}
}

// TestAFanOutKeepsInvocationOrder. Order in the calls list is what the
// children are named by, so a conversion that reordered them would hand the
// same logical call a different idempotency key on the next replay.
func TestAFanOutKeepsInvocationOrder(t *testing.T) {
	got, err := wire.Decision(&pb.Decision{
		Kind:   pb.DecisionKind_DECISION_KIND_CALL_TOOL_PARALLEL,
		StepId: "S3",
		Calls: []*pb.ToolCall{
			{Tool: "check_stock", Payload: []byte(`{"sku":"a"}`)},
			{Tool: "check_price", Payload: []byte(`{"sku":"b"}`)},
			{Tool: "check_stock", Payload: []byte(`{"sku":"c"}`)},
		},
		Join: &pb.JoinPolicy{Kind: pb.JoinKind_JOIN_KIND_QUORUM, Quorum: 2},
	})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}

	if got.Kind != core.DecideCallToolParallel {
		t.Fatalf("Kind = %s, want %s", got.Kind, core.DecideCallToolParallel)
	}
	if got.StepID != core.Step(3) {
		t.Fatalf("StepID = %s, want the parent step S3", got.StepID)
	}
	if got.Join.Kind != core.JoinQuorum || got.Join.Quorum != 2 {
		t.Fatalf("Join = %s, want QUORUM(2)", got.Join)
	}

	want := []string{"check_stock", "check_price", "check_stock"}
	if len(got.Calls) != len(want) {
		t.Fatalf("got %d calls, want %d", len(got.Calls), len(want))
	}
	for i, tool := range want {
		if got.Calls[i].TaskType != tool {
			t.Fatalf("call %d is %q, want %q; invocation order is what children are named by",
				i, got.Calls[i].TaskType, tool)
		}
	}
}

// TestASleepCarriesAnInstantAndAStep. Both halves are needed: the instant is
// when to wake, and the step is what history records the wait against.
func TestASleepCarriesAnInstantAndAStep(t *testing.T) {
	wake := time.Date(2026, time.September, 21, 9, 0, 0, 0, time.UTC)

	got, err := wire.Decision(&pb.Decision{
		Kind:           pb.DecisionKind_DECISION_KIND_SLEEP,
		StepId:         "S4",
		WakeAtUnixNano: wake.UnixNano(),
	})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if got.Kind != core.DecideSleep || got.StepID != core.Step(4) {
		t.Fatalf("got %+v", got)
	}
	if !got.WakeAt.Equal(wake) {
		t.Fatalf("WakeAt = %s, want %s", got.WakeAt, wake)
	}
}

// TestAnUnsetSignalDeadlineWaitsIndefinitely. Zero is honoured rather than
// refused: waiting forever for a human approval is the case this exists for,
// and a run that resumed early on a deadline nobody asked for is worse than
// one that is visibly still waiting.
func TestAnUnsetSignalDeadlineWaitsIndefinitely(t *testing.T) {
	got, err := wire.Decision(&pb.Decision{
		Kind:   pb.DecisionKind_DECISION_KIND_WAIT_FOR_SIGNAL,
		StepId: "S5",
		Signal: &pb.SignalWait{Name: "approval"},
	})
	if err != nil {
		t.Fatalf("Decision: %v", err)
	}
	if got.Kind != core.DecideWaitForSignal || got.Signal.Name != "approval" {
		t.Fatalf("got %+v", got)
	}
	if !got.Signal.Deadline.IsZero() {
		t.Fatalf("Deadline = %s, want the zero time to mean no deadline", got.Signal.Deadline)
	}
}

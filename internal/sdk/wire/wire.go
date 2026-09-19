// Package wire converts between core domain types and the worker protocol.
//
// It exists as its own package so that the translation is testable without a
// network, and so that exactly one file decides what a protocol message means.
// A conversion scattered across the gateway would eventually have two places
// that disagree about, say, what an unset Certainty implies — and that
// particular disagreement sends a second refund.
//
// # The direction that matters
//
// Converting outward (core to protobuf) is bookkeeping. Converting inward
// (protobuf to core) is trust: the bytes came from another process, in another
// language, possibly built against an older version of the contract. So every
// inward conversion validates, and every ambiguous input resolves to the
// pessimistic answer rather than the convenient one.
package wire

import (
	"errors"
	"fmt"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// ErrProtocol reports a message that does not make sense against the contract:
// a missing oneof, an unknown enum, a decision with no tool. It is never
// retried, because a client that sent it once will send it again.
var ErrProtocol = errors.New("veya: worker protocol violation")

// --- outward: core to protobuf --------------------------------------------

// Run converts a run for a DecideRequest.
//
// Version is deliberately absent. It is the engine's concurrency control and
// means nothing to a decider; sending it would invite a client to reason about
// it, and a client reasoning about the run version is a client about to
// serialize advancement a second time, incorrectly.
func Run(r core.Run) *pb.Run {
	return &pb.Run{
		RunId:        r.ID.String(),
		AgentName:    r.AgentName,
		AgentVersion: r.AgentVersion,
		Status:       string(r.Status),
		Input:        r.Input,
	}
}

// Event converts one history event.
//
// The payload crosses verbatim, as the stored envelope. A client reading
// history sees what the database holds byte for byte, so an envelope version
// the client does not understand is the client's error to raise rather than
// something this function has silently reshaped.
func Event(e core.Event) *pb.Event {
	return &pb.Event{
		RunId:             e.RunID.String(),
		Seq:               e.Seq,
		Type:              string(e.Type),
		StepId:            e.StepID.String(),
		Payload:           e.Payload,
		CreatedAtUnixNano: unixNano(e.CreatedAt),
	}
}

// Events converts a whole history, preserving order.
func Events(history []core.Event) []*pb.Event {
	out := make([]*pb.Event, len(history))
	for i, e := range history {
		out[i] = Event(e)
	}
	return out
}

// ExecuteRequest converts a tool call for the worker that will perform it.
func ExecuteRequest(callID, tool string, call core.ToolCall) *pb.ExecuteRequest {
	return &pb.ExecuteRequest{
		CallId:         callID,
		Tool:           tool,
		RunId:          call.RunID.String(),
		TaskId:         call.TaskID.String(),
		StepId:         call.StepID.String(),
		IdempotencyKey: string(call.Key),
		Payload:        call.Payload,
		Attempt:        int32(call.Attempt),
	}
}

// Effect converts a ledger row for a reconcile hook.
func Effect(e core.Effect) *pb.Effect {
	return &pb.Effect{
		EffectId:          string(e.ID),
		RunId:             e.RunID.String(),
		TaskId:            e.TaskID.String(),
		EffectType:        e.Type,
		EffectClass:       EffectClassTo(e.Class),
		IdempotencyKey:    string(e.Key),
		Status:            string(e.Status),
		ExternalRef:       e.ExternalRef,
		Request:           e.Request,
		Response:          e.Response,
		CreatedAtUnixNano: unixNano(e.CreatedAt),
	}
}

// EffectClassTo converts a class outward. An unrecognised class maps to
// UNSPECIFIED rather than panicking: this direction is describing our own
// state to someone else, and a truthful "I cannot name this" beats a crash.
func EffectClassTo(c core.EffectClass) pb.EffectClass {
	switch c {
	case core.ClassNone:
		return pb.EffectClass_EFFECT_CLASS_NONE
	case core.ClassIdempotentByKey:
		return pb.EffectClass_EFFECT_CLASS_IDEMPOTENT_BY_KEY
	case core.ClassQueryable:
		return pb.EffectClass_EFFECT_CLASS_QUERYABLE
	case core.ClassUnreconcilable:
		return pb.EffectClass_EFFECT_CLASS_UNRECONCILABLE
	default:
		return pb.EffectClass_EFFECT_CLASS_UNSPECIFIED
	}
}

// --- inward: protobuf to core ---------------------------------------------

// EffectClassFrom converts a class a client declared at registration.
//
// UNSPECIFIED is rejected rather than defaulted. A tool whose class is unknown
// cannot be run safely in either direction: assume NONE and an effect bypasses
// the ledger entirely, assume UNRECONCILABLE and every ambiguity escalates to
// a human who was never told why. Refusing the registration puts the error in
// front of the person who can fix it, at the moment they can fix it.
func EffectClassFrom(c pb.EffectClass) (core.EffectClass, error) {
	switch c {
	case pb.EffectClass_EFFECT_CLASS_NONE:
		return core.ClassNone, nil
	case pb.EffectClass_EFFECT_CLASS_IDEMPOTENT_BY_KEY:
		return core.ClassIdempotentByKey, nil
	case pb.EffectClass_EFFECT_CLASS_QUERYABLE:
		return core.ClassQueryable, nil
	case pb.EffectClass_EFFECT_CLASS_UNRECONCILABLE:
		return core.ClassUnreconcilable, nil
	default:
		return "", fmt.Errorf("%w: effect class %q is not one this build knows",
			ErrProtocol, c.String())
	}
}

// ToolDescriptor converts one registered tool. The handler and reconciler are
// left nil: they are the parts that live on the far side of the wire, and the
// gateway fills them in with functions that call back over the stream.
func ToolDescriptor(d *pb.ToolDescriptor) (core.ToolDescriptor, error) {
	if d == nil {
		return core.ToolDescriptor{}, fmt.Errorf("%w: nil tool descriptor", ErrProtocol)
	}
	if d.GetName() == "" {
		return core.ToolDescriptor{}, fmt.Errorf("%w: tool descriptor has no name", ErrProtocol)
	}

	class, err := EffectClassFrom(d.GetEffectClass())
	if err != nil {
		return core.ToolDescriptor{}, fmt.Errorf("tool %q: %w", d.GetName(), err)
	}

	// The same rule the Go registry enforces, applied at the wire edge so that
	// a Python tool cannot claim something a Go tool would be panicked for.
	// A tool that cannot be asked what it did is UNRECONCILABLE, whatever it
	// calls itself, and discovering that during an outage is the whole failure
	// this check exists to prevent.
	if class == core.ClassQueryable && !d.GetReconcilable() {
		return core.ToolDescriptor{}, fmt.Errorf(
			"%w: tool %q is QUERYABLE but registered no reconcile hook; "+
				"a tool that cannot be asked what it did is UNRECONCILABLE",
			ErrProtocol, d.GetName())
	}

	return core.ToolDescriptor{
		Name:   d.GetName(),
		Class:  class,
		KeyTTL: time.Duration(d.GetKeyTtlSeconds()) * time.Second,
	}, nil
}

// Decision converts a decider's answer, validating that the fields the kind
// requires are actually present.
//
// A CALL_TOOL with no tool name, or no step, would be committed as a task
// nobody can run. Catching it here names the client that sent it; catching it
// in the engine names the engine.
func Decision(d *pb.Decision) (core.Decision, error) {
	if d == nil {
		return core.Decision{}, fmt.Errorf("%w: nil decision", ErrProtocol)
	}

	switch d.GetKind() {
	case pb.DecisionKind_DECISION_KIND_CALL_TOOL:
		switch {
		case d.GetTool() == "":
			return core.Decision{}, fmt.Errorf("%w: CALL_TOOL names no tool", ErrProtocol)
		case d.GetStepId() == "":
			return core.Decision{}, fmt.Errorf("%w: CALL_TOOL for tool %q has no step id",
				ErrProtocol, d.GetTool())
		}
		return core.Decision{
			Kind:     core.DecideCallTool,
			StepID:   core.StepID(d.GetStepId()),
			TaskType: d.GetTool(),
			Payload:  d.GetPayload(),
		}, nil

	case pb.DecisionKind_DECISION_KIND_COMPLETE:
		return core.Decision{Kind: core.DecideComplete, Output: d.GetOutput()}, nil

	case pb.DecisionKind_DECISION_KIND_FAIL:
		// A failure with no reason is still a failure; inventing a reason
		// would be worse than admitting the client gave none.
		reason := d.GetError()
		if reason == "" {
			reason = "the agent failed the run without giving a reason"
		}
		return core.Decision{Kind: core.DecideFail, Error: reason}, nil

	default:
		return core.Decision{}, fmt.Errorf("%w: decision kind %q is not one this build knows",
			ErrProtocol, d.GetKind().String())
	}
}

// Resolution converts a reconcile hook's answer.
//
// An unrecognised kind becomes STILL_UNKNOWN rather than an error. Saying "I
// cannot tell" is a valid answer from a reconciler, and it is the answer that
// leads to escalation — which is where an outcome nobody can name belongs.
func Resolution(r *pb.Resolution) core.Resolution {
	if r == nil {
		return core.Resolution{
			Kind:   core.ResolvedUnknown,
			Detail: "the worker returned no resolution",
		}
	}

	kind := core.ResolvedUnknown
	switch r.GetKind() {
	case pb.ResolutionKind_RESOLUTION_KIND_COMMITTED:
		kind = core.ResolvedCommitted
	case pb.ResolutionKind_RESOLUTION_KIND_NOT_EXECUTED:
		kind = core.ResolvedNotExecuted
	}

	return core.Resolution{
		Kind:        kind,
		Response:    r.GetResponse(),
		ExternalRef: r.GetExternalRef(),
		Detail:      r.GetDetail(),
	}
}

// Failure converts a reported failure into an error carrying what it proves.
//
// This is the function the whole package is arranged around. core.NotExecuted
// is an assertion that nothing reached the provider, and it moves an effect to
// FAILED, which permits a clean retry. Everything else is ambiguous and
// reconciles instead.
//
// So NOT_EXECUTED is the only value that produces that assertion, and
// UNSPECIFIED does not. A client that forgot to set the field, or one built
// against a version of the contract that did not have it, gets the pessimistic
// answer: a reconcile for what may have been a validation error. That is
// sometimes wasteful and never wrong, and the opposite default is neither.
func Failure(f *pb.Failure) error {
	if f == nil {
		return fmt.Errorf("%w: the worker reported a failure with no detail", ErrProtocol)
	}

	msg := f.GetMessage()
	if msg == "" {
		msg = "the worker reported a failure with no message"
	}
	if t := f.GetType(); t != "" {
		msg = t + ": " + msg
	}

	err := errors.New(msg)
	if f.GetCertainty() == pb.Certainty_CERTAINTY_NOT_EXECUTED {
		return core.NotExecuted(err)
	}
	return err
}

// FailureFrom converts an error into a reported failure, for a worker
// implemented in Go. It is the exact inverse of Failure.
func FailureFrom(err error) *pb.Failure {
	if err == nil {
		return nil
	}
	certainty := pb.Certainty_CERTAINTY_UNKNOWN
	if core.IsNotExecuted(err) {
		certainty = pb.Certainty_CERTAINTY_NOT_EXECUTED
	}
	return &pb.Failure{Message: err.Error(), Certainty: certainty}
}

// unixNano returns 0 for the zero time rather than the large negative number
// time.Time.UnixNano gives it, so "unset" reads as unset on the other side.
func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

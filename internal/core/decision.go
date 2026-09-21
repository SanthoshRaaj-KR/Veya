package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// DecisionKind is what an agent decided to do next.
type DecisionKind string

const (
	// DecideCallTool dispatches one tool call and waits for its result.
	DecideCallTool DecisionKind = "CALL_TOOL"
	// DecideComplete ends the run successfully.
	DecideComplete DecisionKind = "COMPLETE"
	// DecideFail ends the run with an error.
	DecideFail DecisionKind = "FAIL"

	// DecideCallToolParallel dispatches several tool calls at once and joins
	// them under a policy.
	//
	// One decision carrying N calls, rather than N decisions, is the choice
	// argued in docs/execution-model.md section 3. It makes "a sleep and a
	// cancel in the same batch" unrepresentable instead of something the
	// engine has to reject at runtime, and it keeps a decision atomic: the
	// engine still commits one decision as one transaction, so there is no
	// question about what a half-applied batch would mean.
	DecideCallToolParallel DecisionKind = "CALL_TOOL_PARALLEL"

	// DecideSleep parks the run until a wall-clock instant.
	DecideSleep DecisionKind = "SLEEP"

	// DecideWaitForSignal parks the run until a named signal arrives, or
	// until its deadline passes.
	DecideWaitForSignal DecisionKind = "WAIT_FOR_SIGNAL"
)

// IsSuspension reports whether a decision parks the run rather than advancing
// it. Both kinds that do are reached with nothing in flight — see Indefinite.
func (k DecisionKind) IsSuspension() bool {
	return k == DecideSleep || k == DecideWaitForSignal
}

// Call is one tool invocation inside a parallel decision.
//
// It carries no step ID. Children are numbered by the engine as
// parent.Child(i), by position in the slice, so that StepID.Child has exactly
// one definition on the Go side rather than one per caller. Completion order
// is never involved: the idempotency key is derived from the step ID, so
// numbering by whichever child finished first would give the same logical call
// a different key on every replay.
//
// Distinct from ToolCall, which is what a tool is handed when it is actually
// performed. This is intent; that is execution.
type Call struct {
	TaskType string
	Payload  json.RawMessage
}

// JoinKind is how many children have to settle before a fan-out is joined.
type JoinKind string

const (
	// JoinAll waits for every child to reach a terminal state, successful or
	// not. The body is then handed all N outcomes and decides what they mean.
	JoinAll JoinKind = "ALL"

	// JoinAny completes as soon as one child succeeds — or when every child
	// has failed, which is the exit that gets forgotten. A join satisfiable
	// only by success hangs forever on a bad day.
	JoinAny JoinKind = "ANY"

	// JoinQuorum completes when Quorum children have succeeded, or as soon as
	// too few remain for that to be possible.
	JoinQuorum JoinKind = "QUORUM"
)

// JoinPolicy states when a fan-out is finished waiting.
//
// It never states what a partial failure means. Whether three failures out of
// ten is a disaster or a Tuesday is a question about meaning, and the engine
// does not do meaning — the outcomes go back to the body in invocation order
// and the body decides. See docs/execution-model.md section 7.3.
type JoinPolicy struct {
	Kind JoinKind

	// Quorum is how many successes JoinQuorum needs. Ignored otherwise.
	Quorum int
}

// Valid reports whether a policy makes sense for n children, and says why not
// when it does not.
//
// A quorum larger than the fan-out is the mistake worth catching here: it is
// unsatisfiable from the first instant, so it would park the run until the
// heat death of the universe with nothing in the logs to suggest why.
//
// n <= 0 is refused for every kind, not only QUORUM. A zero-call ALL or ANY
// is trivially satisfied the instant it is recorded — FanOut.Satisfied has no
// other way to read "nothing to wait for" — so PendingFanOut would report it
// as already joined and the body would get no outcomes back for a decision it
// just made. Rejecting it here keeps that a decider mistake caught at the
// decision, not a run that silently joins on an empty set.
func (p JoinPolicy) Valid(n int) error {
	if n <= 0 {
		return fmt.Errorf("join policy %s over %d calls: a fan-out needs at least one call", p.Kind, n)
	}
	switch p.Kind {
	case JoinAll, JoinAny:
		return nil
	case JoinQuorum:
		switch {
		case p.Quorum <= 0:
			return fmt.Errorf("join policy QUORUM needs a positive quorum, got %d", p.Quorum)
		case p.Quorum > n:
			return fmt.Errorf("join policy QUORUM(%d) over %d calls can never be satisfied",
				p.Quorum, n)
		}
		return nil
	default:
		return fmt.Errorf("join policy %q is not one this build knows", p.Kind)
	}
}

func (p JoinPolicy) String() string {
	if p.Kind == JoinQuorum {
		return fmt.Sprintf("QUORUM(%d)", p.Quorum)
	}
	return string(p.Kind)
}

// SignalWait is a run's request to be woken when something outside the system
// happens.
//
// Name is what the run is waiting for, and it is matched against the name a
// signal arrives under. The wait is a read rather than a delivery: a signal
// that arrived before the run got here is already stored, so early and late
// arrival run identical code and the early-signal race does not exist. See
// docs/execution-model.md section 4.
type SignalWait struct {
	Name string

	// Deadline bounds the wait. The zero value waits indefinitely, which is
	// honest for a human approval and dangerous for anything automated: a run
	// blocked on a signal nobody will ever send is invisible until somebody
	// goes looking.
	Deadline time.Time
}

// Indefinite is the instant a run parks at when it is waiting for something
// with no schedule of its own.
//
// A sentinel is needed because NULL in runs.available_at already means ready —
// it has to, since every run that predates the column has NULL and every one
// of them is ready. The alternative is a second column whose only job is to
// say which of two meanings the first column carries, and two columns
// asserting one fact is the drift this codebase avoids elsewhere.
//
// Nothing waits for this instant to arrive. A signal's arrival clears the
// column in the same transaction that records the signal, so the value exists
// only to keep the recovery scan from sweeping a run that is waiting on
// purpose. The CLI renders it as what the run is waiting for, never as a date.
var Indefinite = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)

// IsIndefinite reports whether a park has no scheduled wake-up.
func IsIndefinite(t time.Time) bool { return !t.Before(Indefinite) }

// Decision is one step of agent intent, expressed without reference to how it
// was arrived at.
type Decision struct {
	Kind DecisionKind

	// Set when Kind is DecideCallTool, and when Kind is
	// DecideCallToolParallel — where it is the *parent* step whose children
	// are numbered from it.
	StepID StepID

	// Set when Kind is DecideCallTool.
	TaskType string
	Payload  json.RawMessage

	// Set when Kind is DecideCallToolParallel. Order is invocation order and
	// is load-bearing: it is what the children are named by.
	Calls []Call
	Join  JoinPolicy

	// Set when Kind is DecideSleep.
	WakeAt time.Time

	// Set when Kind is DecideWaitForSignal.
	Signal SignalWait

	// Set when Kind is DecideComplete.
	Output json.RawMessage

	// Set when Kind is DecideFail.
	Error string
}

// Decider answers "what happens next in this run?" given durable history.
//
// This is the seam that keeps the engine free of any knowledge that a language
// model exists. Layer 1 ships a static decider that walks a fixed sequence;
// Layer 4 adds one that calls a model. Both satisfy this interface, so
// introducing the model changes no engine code.
//
// # Replay depends on this signature
//
// Decide receives history and returns the next decision. It is not asked to
// re-derive decisions already in history — those are facts, and facts are read
// back rather than recomputed. A decider that consults history and returns the
// first unrecorded step is replay-safe; one that ignores history and counts
// its own invocations is not, and will fork a run on recovery.
//
// Past decisions are replayed. Future decisions are generated.
type Decider interface {
	Decide(ctx context.Context, run Run, history []Event) (Decision, error)
}

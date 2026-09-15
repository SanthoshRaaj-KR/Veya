package core

import (
	"context"
	"encoding/json"
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
)

// Decision is one step of agent intent, expressed without reference to how it
// was arrived at.
type Decision struct {
	Kind DecisionKind

	// Set when Kind is DecideCallTool.
	StepID   StepID
	TaskType string
	Payload  json.RawMessage

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

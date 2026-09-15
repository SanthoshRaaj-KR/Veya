package core

import "fmt"

// RunID identifies one agent execution from start to terminal state.
type RunID string

// TaskID identifies one unit of work dispatched to a worker.
type TaskID string

// StepID is a run-relative logical position, not a database key.
//
// This distinction carries weight from Layer 2 onward. Idempotency keys are
// derived as run_id:step_id:effect_seq, so a step's identity must survive
// retries, reassignment, and replay. Task IDs do not survive those things;
// logical positions do.
//
// Steps are numbered by invocation order, never completion order. Under
// fan-out those differ, and only invocation order reproduces on replay.
type StepID string

// Step returns the StepID for the nth top-level step of a run, 1-indexed.
func Step(n int) StepID {
	return StepID(fmt.Sprintf("S%d", n))
}

// Child returns the StepID for the nth parallel child of a step, 0-indexed by
// invocation order.
//
// Unused until Layer 5 introduces fan-out. It lives here now so that child
// naming has exactly one definition rather than being invented at the call
// site later.
func (s StepID) Child(n int) StepID {
	return StepID(fmt.Sprintf("%s.%d", s, n))
}

func (r RunID) String() string  { return string(r) }
func (t TaskID) String() string { return string(t) }
func (s StepID) String() string { return string(s) }

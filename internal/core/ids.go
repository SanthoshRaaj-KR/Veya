package core

import (
	"fmt"
	"strconv"
	"strings"
)

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
// Invocation order, never completion order, and the reason is the idempotency
// key. The key is derived from the step ID, so numbering by whichever child
// finished first would give the same logical call a different key on every
// replay -- and the ledger's one guarantee would evaporate the first time two
// children raced.
func (s StepID) Child(n int) StepID {
	return StepID(fmt.Sprintf("%s.%d", s, n))
}

// Children returns the StepIDs for a fan-out of n calls, in invocation order.
//
// One function rather than a loop at each call site, because the naming rule
// is the thing that has to be identical in the engine, in the ledger and in
// every language that reads history back.
func (s StepID) Children(n int) []StepID {
	out := make([]StepID, n)
	for i := range out {
		out[i] = s.Child(i)
	}
	return out
}

// Parent returns the step a child was fanned out from, and whether it is a
// child at all.
//
// This is the inverse of Child and it has to stay one: history records child
// steps, and reading a fan-out back means grouping them by the step they came
// from. A parse that disagreed with the format would silently split one
// fan-out into several joins of one.
func (s StepID) Parent() (StepID, bool) {
	i := strings.LastIndex(string(s), ".")
	if i <= 0 || i == len(s)-1 {
		return "", false
	}
	// The suffix has to be a number. A step named "S1.retry" is not a child
	// of S1; it is a step somebody named oddly, and treating it as one would
	// put it in a join it has nothing to do with.
	if _, err := strconv.Atoi(string(s)[i+1:]); err != nil {
		return "", false
	}
	return StepID(string(s)[:i]), true
}

// ChildIndex returns a child's position in its fan-out, or -1.
func (s StepID) ChildIndex() int {
	i := strings.LastIndex(string(s), ".")
	if _, ok := s.Parent(); !ok {
		return -1
	}
	n, err := strconv.Atoi(string(s)[i+1:])
	if err != nil {
		return -1
	}
	return n
}

func (r RunID) String() string  { return string(r) }
func (t TaskID) String() string { return string(t) }
func (s StepID) String() string { return string(s) }

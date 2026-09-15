// Package decider holds implementations of core.Decider — the thing that
// answers "what happens next in this run?".
//
// Layer 1 ships Static, which walks a fixed sequence of tool calls. Layer 4
// adds one backed by a language model. They satisfy the same interface, which
// is what lets the engine stay ignorant that models exist.
package decider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/santhoshraajkr/veya/internal/core"
)

// Step is one tool call in a static sequence.
type Step struct {
	Tool    string
	Payload json.RawMessage
}

// Static walks a predetermined list of steps and then completes the run.
//
// It exists to prove the execution model without a model in the loop, and it
// stays in the test suite permanently afterwards: a bug that only reproduces
// with an LLM attached is nearly impossible to isolate, so the engine must
// remain exercisable by something deterministic.
type Static struct {
	steps []Step
}

// NewStatic returns a decider that performs steps in order.
func NewStatic(steps ...Step) *Static {
	return &Static{steps: steps}
}

// Decide returns the next step the run owes, derived from history.
//
// Position comes from counting completed steps in the event log, never from a
// counter on this struct. That is what makes it replay-safe: a decider that
// counts its own invocations returns step 1 again after a restart and forks
// the run. Recorded decisions are facts to be read back, not recomputed.
func (s *Static) Decide(_ context.Context, run core.Run, history []core.Event) (core.Decision, error) {
	done := completedSteps(history)

	if done < len(s.steps) {
		step := s.steps[done]
		return core.Decision{
			Kind:     core.DecideCallTool,
			StepID:   core.Step(done + 1),
			TaskType: step.Tool,
			Payload:  step.Payload,
		}, nil
	}

	output, err := lastResult(history)
	if err != nil {
		return core.Decision{}, fmt.Errorf("decide for run %s: %w", run.ID, err)
	}
	return core.Decision{Kind: core.DecideComplete, Output: output}, nil
}

// completedSteps counts tasks that finished successfully. Failed attempts are
// not counted: the step is still owed, and retrying it is the correct answer.
func completedSteps(history []core.Event) int {
	n := 0
	for _, ev := range history {
		if ev.Type == core.EventTaskCompleted {
			n++
		}
	}
	return n
}

// lastResult returns the result of the final completed task, which becomes the
// run's output.
func lastResult(history []core.Event) (json.RawMessage, error) {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Type != core.EventTaskCompleted {
			continue
		}
		var data core.TaskCompletedData
		if err := history[i].Decode(&data); err != nil {
			return nil, err
		}
		return data.Result, nil
	}
	return nil, nil // a run with no steps completes with no output
}

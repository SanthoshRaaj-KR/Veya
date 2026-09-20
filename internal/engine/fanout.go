package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// dispatchParallel commits a fan-out: N tasks, N events, N delivery intents,
// and the record of the join policy, in one transaction.
//
// # Why one transaction rather than N
//
// A fan-out is one decision, and the engine's rule is that a decision commits
// as a unit. Committing children one at a time would raise a question this
// design has never had to answer: what a run means when three of its ten
// children exist and the process died before the fourth. There would be no
// FAN_OUT_STARTED to say how many were intended, so nothing could tell a
// partial fan-out from a complete fan-out of three — and a join over the
// survivors would be satisfied by a set nobody chose.
//
// The cost is a larger transaction. Ten children is ten inserts and ten event
// appends against one run, which contends with nothing: serialization is per
// run, and this run is the one making the decision.
//
// # Re-deciding is a no-op, and that is the whole waiting mechanism
//
// While the join is unsatisfied the body suspends and re-issues the identical
// decision. CreateTask hits UNIQUE (run_id, step_id) on the first child,
// ErrTaskExists rolls the transaction back, and nothing happens — the same
// path a re-decided single CallTool has taken since Layer 1.
//
// That is why fan-out adds no waiting state anywhere. The mechanism that makes
// re-deciding one step harmless makes re-deciding N steps harmless, and it is
// a database constraint rather than a check somebody has to remember to write.
func (e *Engine) dispatchParallel(ctx context.Context, run core.Run, d core.Decision) error {
	if err := d.Join.Valid(len(d.Calls)); err != nil {
		// A quorum larger than the fan-out, or a policy this build does not
		// know. Failing the run beats dispatching work that can never be
		// joined, which would look like a successful decision and then a run
		// that never moves again.
		return e.finish(ctx, run, core.RunState{
			Status:    core.RunFailed,
			LastError: fmt.Sprintf("fan-out at step %s: %v", d.StepID, err),
		}, core.EventRunFailed, core.RunFailedData{
			Error: fmt.Sprintf("fan-out at step %s: %v", d.StepID, err),
		})
	}

	children := d.StepID.Children(len(d.Calls))
	taskIDs := make([]core.TaskID, len(d.Calls))
	tools := make([]string, len(d.Calls))
	for i := range d.Calls {
		taskIDs[i] = e.ids.NewTaskID()
		tools[i] = d.Calls[i].TaskType
	}

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		// The CAS first, so a losing caller stops before doing any work.
		if err := tx.AdvanceRun(ctx, run.ID, run.Version, core.RunState{
			Status: core.RunRunning,
			Output: run.Output,
		}); err != nil {
			return err
		}

		// The policy, before the children, so a reader of history knows what
		// the calls beneath it were for.
		if err := core.Append(ctx, tx, run.ID, core.EventFanOutStarted, d.StepID,
			core.FanOutStartedData{Calls: tools, Join: d.Join.String()}); err != nil {
			return err
		}

		for i, call := range d.Calls {
			task := core.Task{
				ID:          taskIDs[i],
				RunID:       run.ID,
				StepID:      children[i],
				Type:        call.TaskType,
				Payload:     call.Payload,
				Status:      core.TaskPending,
				MaxAttempts: defaultMaxAttempts,
			}
			if err := tx.CreateTask(ctx, task); err != nil {
				return err
			}
			if err := core.Append(ctx, tx, run.ID, core.EventTaskCreated, children[i],
				core.TaskCreatedData{
					TaskID:   taskIDs[i],
					TaskType: call.TaskType,
					Payload:  call.Payload,
				}); err != nil {
				return err
			}
			if err := tx.EnqueueDelivery(ctx, taskIDs[i]); err != nil {
				return err
			}
		}
		return nil
	})

	switch {
	case errors.Is(err, core.ErrConflict), errors.Is(err, core.ErrTaskExists):
		// Someone else advanced this run, or these children already exist.
		// Both mean the work is accounted for. Doing nothing is correct, and
		// in the second case it is also how a fan-out waits for its join.
		e.log.Debug("fan-out lost the race or is already dispatched",
			"run_id", run.ID, "step_id", d.StepID)
		return nil
	case err != nil:
		return fmt.Errorf("fan-out %s step %s: %w", run.ID, d.StepID, err)
	}

	e.log.Info("fan-out dispatched", "run_id", run.ID, "step_id", d.StepID,
		"children", len(d.Calls), "join", d.Join.String())

	// One wake for the batch. The relay drains whatever it finds, so N hints
	// for N rows committed together would be N-1 wasted wake-ups.
	e.wake()
	return nil
}

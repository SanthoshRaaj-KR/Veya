package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/santhoshraajkr/veya/internal/core"
)

// ClaimTask takes ownership of a task on behalf of a worker.
//
// This is the concurrency barrier that makes at-least-once delivery safe. Two
// workers handed the same task both call this; the conditional
// PENDING -> RUNNING transition means exactly one succeeds and the other gets
// ErrConflict and drops the delivery. It holds with no lease and no fencing
// token, which is why duplicate delivery is a property to tolerate rather than
// a problem the transport has to solve.
//
// Layer 2 adds a lease and a fencing token alongside this transition. Neither
// replaces it: the lease says who *should* work, the token rejects writes from
// a worker that has lost ownership, and this claim is what stops the work
// happening twice in the first place.
func (e *Engine) ClaimTask(ctx context.Context, id core.TaskID, workerID string) (core.Task, error) {
	var claimed core.Task

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, id)
		if err != nil {
			return err
		}
		if err := tx.TransitionTask(ctx, id, core.TaskPending, core.TaskRunning, core.TaskOutcome{}); err != nil {
			return err
		}

		// Attempt is read before the transition and incremented here to match
		// what the store recorded, so the event and the row agree.
		task.Status = core.TaskRunning
		task.Attempt++
		claimed = task

		return appendEvent(ctx, tx, task.RunID, core.EventTaskClaimed, task.StepID, core.TaskClaimedData{
			TaskID:   id,
			WorkerID: workerID,
			Attempt:  task.Attempt,
		})
	})
	if err != nil {
		return core.Task{}, fmt.Errorf("claim task %s: %w", id, err)
	}

	e.log.Debug("task claimed", "task_id", id, "worker_id", workerID,
		"attempt", claimed.Attempt, "tool", claimed.Type)
	return claimed, nil
}

// CompleteTask records a successful tool call and advances the run.
//
// The result is written to history and nowhere else. It is not a column on the
// task row: replay reads history, and a second copy would be two records
// asserting the same fact.
func (e *Engine) CompleteTask(ctx context.Context, id core.TaskID, result json.RawMessage) error {
	var runID core.RunID

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, id)
		if err != nil {
			return err
		}
		runID = task.RunID

		if err := tx.TransitionTask(ctx, id, core.TaskRunning, core.TaskCompleted, core.TaskOutcome{}); err != nil {
			return err
		}
		return appendEvent(ctx, tx, task.RunID, core.EventTaskCompleted, task.StepID, core.TaskCompletedData{
			TaskID: id,
			Result: result,
		})
	})
	if errors.Is(err, core.ErrConflict) {
		// A stale worker reporting on a task that has moved on. Rejecting it
		// is the point; from Layer 2 the fencing token catches this earlier
		// and more precisely.
		e.log.Warn("ignoring completion for a task that is no longer running", "task_id", id)
		return nil
	}
	if err != nil {
		return fmt.Errorf("complete task %s: %w", id, err)
	}

	e.log.Info("task completed", "task_id", id, "run_id", runID)
	return e.Advance(ctx, runID)
}

// FailTask records a failed tool call, then either retries it or gives up.
//
// Retries are immediate and the policy is a fixed attempt count. Backoff,
// jitter, and per-tool policy arrive in Layer 5; pretending to have them now
// would mean writing a scheduler with nothing to schedule against.
func (e *Engine) FailTask(ctx context.Context, id core.TaskID, cause string) error {
	var (
		runID   core.RunID
		retry   bool
		attempt int
	)

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, id)
		if err != nil {
			return err
		}
		runID = task.RunID
		attempt = task.Attempt
		retry = !task.Exhausted()

		next := core.TaskDeadLetter
		if retry {
			next = core.TaskPending
		}
		if err := tx.TransitionTask(ctx, id, core.TaskRunning, next, core.TaskOutcome{Error: cause}); err != nil {
			return err
		}
		return appendEvent(ctx, tx, task.RunID, core.EventTaskFailed, task.StepID, core.TaskFailedData{
			TaskID:  id,
			Error:   cause,
			Attempt: task.Attempt,
			Final:   !retry,
		})
	})
	if errors.Is(err, core.ErrConflict) {
		e.log.Warn("ignoring failure for a task that is no longer running", "task_id", id)
		return nil
	}
	if err != nil {
		return fmt.Errorf("fail task %s: %w", id, err)
	}

	if retry {
		e.log.Info("task failed, retrying", "task_id", id, "attempt", attempt, "error", cause)
		if err := e.dispatcher.Publish(ctx, id); err != nil {
			e.log.Warn("republish failed; task will be recovered by scan", "task_id", id, "error", err)
		}
		return nil
	}

	// Retries exhausted. The run cannot proceed past a step that will not
	// complete, so it fails with the cause rather than stalling silently.
	e.log.Error("task dead lettered", "task_id", id, "attempts", attempt, "error", cause)

	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("fail task %s: %w", id, err)
	}
	if run.Status.IsTerminal() {
		return nil
	}
	return e.finish(ctx, run, core.RunState{
		Status:    core.RunFailed,
		LastError: fmt.Sprintf("task %s exhausted %d attempts: %s", id, attempt, cause),
	}, core.EventRunFailed, core.RunFailedData{
		Error: fmt.Sprintf("task %s exhausted %d attempts: %s", id, attempt, cause),
	})
}

// Run and History expose read access for the CLI, so that callers do not reach
// past the engine into the store.
func (e *Engine) Run(ctx context.Context, id core.RunID) (core.Run, error) {
	return e.store.GetRun(ctx, id)
}

func (e *Engine) History(ctx context.Context, id core.RunID) ([]core.Event, error) {
	return e.store.History(ctx, id)
}

func (e *Engine) Tasks(ctx context.Context, id core.RunID) ([]core.Task, error) {
	return e.store.ListTasks(ctx, id)
}

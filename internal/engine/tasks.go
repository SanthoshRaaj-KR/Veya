package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Claim is what a worker receives when it takes a task: the work, and the
// authority to report on it.
type Claim struct {
	Task  core.Task
	Lease core.Lease
}

// ClaimTask takes ownership of a task on behalf of a worker.
//
// Two independent things happen here, and conflating them is a common way to
// get this wrong:
//
//   - The conditional PENDING -> RUNNING transition decides who *executes*.
//     Of two workers handed the same task, exactly one succeeds. This alone
//     makes at-least-once delivery safe, and it held before leases existed.
//   - The lease and its fencing token decide who may *report back*. The lease
//     can be wrong under clock skew; the token cannot, and it is what rejects
//     a frozen worker's result after its lease has been taken.
//
// Both happen in one transaction, so a task can never be RUNNING without an
// owner, or owned without being RUNNING.
func (e *Engine) ClaimTask(ctx context.Context, id core.TaskID, workerID string) (Claim, error) {
	var claim Claim

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, id)
		if err != nil {
			return err
		}
		if err := tx.TransitionTask(ctx, id, core.TaskPending, core.TaskRunning, core.TaskOutcome{}); err != nil {
			return err
		}

		now := e.clock.Now()
		lease, err := tx.AcquireLease(ctx, id, workerID, now, now.Add(e.leaseTTL))
		if err != nil {
			return err
		}

		task.Status = core.TaskRunning
		task.Attempt++
		claim = Claim{Task: task, Lease: lease}

		return core.Append(ctx, tx, task.RunID, core.EventTaskClaimed, task.StepID, core.TaskClaimedData{
			TaskID:   id,
			WorkerID: workerID,
			Attempt:  task.Attempt,
		})
	})
	if err != nil {
		return Claim{}, fmt.Errorf("claim task %s: %w", id, err)
	}

	e.log.Debug("task claimed", "task_id", id, "worker_id", workerID,
		"attempt", claim.Task.Attempt, "token", claim.Lease.Token, "tool", claim.Task.Type)
	return claim, nil
}

// Heartbeat extends a worker's lease, proving it is still alive.
//
// Fenced like every other mutation. A worker whose heartbeat is rejected has
// lost the task and must stop, rather than carry on believing it owns work
// that now belongs to someone else.
func (e *Engine) Heartbeat(ctx context.Context, id core.TaskID, token core.FencingToken) error {
	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ExtendLease(ctx, id, token, e.clock.Now().Add(e.leaseTTL))
	})
	if err != nil {
		return fmt.Errorf("heartbeat task %s: %w", id, err)
	}
	return nil
}

// CompleteTask records a successful tool call and advances the run.
//
// The result is written to history and nowhere else. It is not a column on the
// task row: replay reads history, and a second copy would be two records
// asserting the same fact.
func (e *Engine) CompleteTask(ctx context.Context, id core.TaskID, token core.FencingToken, result json.RawMessage) error {
	var runID core.RunID

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, id)
		if err != nil {
			return err
		}
		runID = task.RunID

		if err := assertToken(ctx, tx, id, token); err != nil {
			return err
		}
		if err := tx.TransitionTask(ctx, id, core.TaskRunning, core.TaskCompleted, core.TaskOutcome{}); err != nil {
			return err
		}
		if err := tx.ReleaseLease(ctx, id, token, e.clock.Now()); err != nil {
			return err
		}
		return core.Append(ctx, tx, task.RunID, core.EventTaskCompleted, task.StepID, core.TaskCompletedData{
			TaskID: id,
			Result: result,
		})
	})
	switch {
	case errors.Is(err, core.ErrFenced):
		// A worker that lost ownership while it was away. Rejecting it is the
		// entire point of the token: someone else owns this work now, and
		// their result is the one that counts.
		e.log.Warn("rejected a result from a worker that no longer owns the task",
			"task_id", id, "token", token)
		return nil
	case errors.Is(err, core.ErrConflict):
		e.log.Warn("ignoring completion for a task that is no longer running", "task_id", id)
		return nil
	case err != nil:
		return fmt.Errorf("complete task %s: %w", id, err)
	}

	e.log.Info("task completed", "task_id", id, "run_id", runID)
	return e.Advance(ctx, runID)
}

// FailTask records a failed tool call, then either retries it or gives up.
//
// Retries are immediate and the policy is a fixed attempt count. Backoff,
// jitter, and per-tool policy arrive in Layer 6; pretending to have them now
// would mean writing a scheduler with nothing to schedule against.
// An escalated outcome is never retried. Retrying would mean either calling a
// provider that may already have acted, or burning attempts until the task
// dead-letters anyway — both noise on top of a situation that already needs a
// person. It goes straight to DEAD_LETTER so the run fails visibly and the
// unresolved effect stays on the books.
func (e *Engine) FailTask(ctx context.Context, id core.TaskID, token core.FencingToken, cause error) error {
	var (
		runID   core.RunID
		stepID  core.StepID
		retry   bool
		attempt int
	)
	reason := cause.Error()
	escalated := errors.Is(cause, core.ErrEscalated)

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, id)
		if err != nil {
			return err
		}
		runID = task.RunID
		stepID = task.StepID
		attempt = task.Attempt
		retry = !task.Exhausted() && !escalated

		if err := assertToken(ctx, tx, id, token); err != nil {
			return err
		}

		next := core.TaskDeadLetter
		followUp := core.EventTaskDeadLettered
		if retry {
			next = core.TaskPending
			followUp = core.EventTaskRetryScheduled
		}
		if err := tx.TransitionTask(ctx, id, core.TaskRunning, next, core.TaskOutcome{Error: reason}); err != nil {
			return err
		}
		if err := tx.ReleaseLease(ctx, id, token, e.clock.Now()); err != nil {
			return err
		}

		if err := core.Append(ctx, tx, task.RunID, core.EventTaskFailed, task.StepID, core.TaskFailedData{
			TaskID:  id,
			Error:   reason,
			Attempt: task.Attempt,
			Final:   !retry,
		}); err != nil {
			return err
		}
		if err := core.Append(ctx, tx, task.RunID, followUp, task.StepID, core.TaskRetryScheduledData{
			TaskID:  id,
			Attempt: task.Attempt,
			Reason:  reason,
		}); err != nil {
			return err
		}
		if !retry {
			return nil
		}
		// A retry makes the task dispatchable again, so it needs delivery
		// intent exactly as its creation did. Republishing after the commit
		// instead would put back the window this layer removed — and a task
		// stranded on its second attempt is no more visible than one stranded
		// on its first.
		return tx.EnqueueDelivery(ctx, id)
	})
	switch {
	case errors.Is(err, core.ErrFenced):
		e.log.Warn("rejected a failure report from a worker that no longer owns the task",
			"task_id", id, "token", token)
		return nil
	case errors.Is(err, core.ErrConflict):
		e.log.Warn("ignoring failure for a task that is no longer running", "task_id", id)
		return nil
	case err != nil:
		return fmt.Errorf("fail task %s: %w", id, err)
	}

	if retry {
		e.log.Info("task failed, retrying", "task_id", id, "attempt", attempt, "error", reason)
		e.wake()
		return nil
	}

	// An escalated task stops the run whatever shape it is in. The effect's
	// outcome is unresolved -- the outside world may or may not have
	// changed -- and carrying on past that is how a run acts twice on
	// something a human has not yet looked at.
	if escalated {
		e.log.Error("task stopped for human resolution", "task_id", id, "reason", reason)
		return e.failRun(ctx, runID, fmt.Sprintf("task %s needs human resolution: %s", id, reason))
	}

	e.log.Error("task dead lettered", "task_id", id, "attempts", attempt, "error", reason)

	// A dead-lettered child of a fan-out is one outcome among several, and
	// what it means is the body's business, not the engine's. Three
	// failures out of ten is a disaster or a Tuesday depending on the agent
	// -- docs/execution-model.md section 7.3 -- and the failure is already
	// recorded as TASK_FAILED with Final set, which is what the join reads.
	//
	// Before fan-out this distinction did not exist: one step was in flight
	// at a time, so a step that would never complete was a run that could
	// never proceed. Failing the run was right then and is wrong now.
	if _, isChild := stepID.Parent(); isChild {
		e.log.Info("a fan-out child failed for good; the agent decides what that means",
			"run_id", runID, "task_id", id, "step_id", stepID)
		return e.Advance(ctx, runID)
	}

	// A top-level step that will not complete is a run that cannot proceed,
	// so it fails with the cause rather than stalling silently.
	return e.failRun(ctx, runID, fmt.Sprintf("task %s exhausted %d attempts: %s", id, attempt, reason))
}

// failRun moves a run to FAILED with a reason, unless it has already finished.
func (e *Engine) failRun(ctx context.Context, runID core.RunID, reason string) error {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("fail run %s: %w", runID, err)
	}
	if run.Status.IsTerminal() {
		return nil
	}
	return e.finish(ctx, run, core.RunState{
		Status:    core.RunFailed,
		LastError: reason,
	}, core.EventRunFailed, core.RunFailedData{Error: reason})
}

// assertToken rejects a write from a worker that no longer owns the task.
//
// Checked on every mutating call rather than at one chokepoint. A single
// well-placed check is the version of this that looks correct and is not:
// ownership can lapse between any two statements, so the guarantee has to be
// re-established wherever state is written, not once on the way in.
func assertToken(ctx context.Context, tx core.Tx, id core.TaskID, token core.FencingToken) error {
	lease, err := tx.GetLease(ctx, id)
	if err != nil {
		return err
	}
	if lease.Token != token {
		return fmt.Errorf("task %s is at token %d, presented %d: %w",
			id, lease.Token, token, core.ErrFenced)
	}
	return nil
}

// RegisterWorker records a worker so that leases can reference it.
func (e *Engine) RegisterWorker(ctx context.Context, id, workerType string) error {
	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.RegisterWorker(ctx, core.Worker{
			ID:     id,
			Type:   workerType,
			Status: core.WorkerActive,
		})
	})
	if err != nil {
		return fmt.Errorf("register worker %s: %w", id, err)
	}
	return nil
}

// LeaseTTL reports how long a claim is good for before it must be renewed.
func (e *Engine) LeaseTTL() time.Duration { return e.leaseTTL }

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

func (e *Engine) Effects(ctx context.Context, id core.RunID) ([]core.Effect, error) {
	return e.store.ListEffects(ctx, id)
}

// Package worker runs tool calls.
//
// The loop is small on purpose: claim, execute, report. It holds no state
// between iterations, so a worker that dies mid-task loses nothing the store
// does not already know.
//
// # What a worker deliberately does not decide
//
// It does not choose what to run next — that is the decider's job, reached
// through the engine. It does not decide whether a failure is retryable, or
// whether a task is finished with; it reports an outcome and the engine
// decides. Keeping the worker this thin is what makes it replaceable by a
// Python process speaking the same protocol in Layer 4.
//
// # Ownership
//
// A claim comes with a lease and a fencing token. The worker heartbeats for as
// long as it is executing and presents the token on every report. If a
// heartbeat is rejected the worker has lost the task, and it stops immediately
// rather than finishing work that now belongs to someone else — its report
// would be rejected anyway, and every extra second is another second in which
// the new owner may be doing the same thing.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
)

// Engine is what a worker needs from the runtime.
type Engine interface {
	RegisterWorker(ctx context.Context, id, workerType string) error
	ClaimTask(ctx context.Context, id core.TaskID, workerID string) (engine.Claim, error)
	Heartbeat(ctx context.Context, id core.TaskID, token core.FencingToken) error
	CompleteTask(ctx context.Context, id core.TaskID, token core.FencingToken, result json.RawMessage) error
	FailTask(ctx context.Context, id core.TaskID, token core.FencingToken, cause error) error
	LeaseTTL() time.Duration
}

// Executor runs one task's tool, through the effect ledger when the tool has
// external consequences.
type Executor interface {
	Execute(ctx context.Context, task core.Task) (json.RawMessage, error)
}

// Worker claims tasks and executes them.
type Worker struct {
	id        string
	engine    Engine
	dispatch  core.Dispatcher
	executor  Executor
	heartbeat time.Duration
	log       *slog.Logger
}

// Config wires a Worker. All fields except Logger and HeartbeatInterval are
// required.
type Config struct {
	ID         string
	Engine     Engine
	Dispatcher core.Dispatcher
	Executor   Executor

	// HeartbeatInterval defaults to a third of the lease TTL, so a worker has
	// to miss three consecutive beats before it is presumed dead. One missed
	// beat on a busy machine is not evidence of anything.
	HeartbeatInterval time.Duration

	Logger *slog.Logger
}

// New validates the configuration and returns a Worker.
func New(cfg Config) (*Worker, error) {
	switch {
	case cfg.ID == "":
		return nil, errors.New("worker: ID is required")
	case cfg.Engine == nil:
		return nil, errors.New("worker: Engine is required")
	case cfg.Dispatcher == nil:
		return nil, errors.New("worker: Dispatcher is required")
	case cfg.Executor == nil:
		return nil, errors.New("worker: Executor is required")
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	interval := cfg.HeartbeatInterval
	if interval <= 0 {
		interval = cfg.Engine.LeaseTTL() / 3
	}
	if interval <= 0 {
		interval = time.Second
	}

	return &Worker{
		id:        cfg.ID,
		engine:    cfg.Engine,
		dispatch:  cfg.Dispatcher,
		executor:  cfg.Executor,
		heartbeat: interval,
		log:       log.With("worker_id", cfg.ID),
	}, nil
}

// ID reports the worker's identity.
func (w *Worker) ID() string { return w.id }

// Run claims and executes tasks until ctx is cancelled or the dispatcher
// closes.
func (w *Worker) Run(ctx context.Context) error {
	// A lease references a worker row, so registration has to happen before
	// the first claim rather than lazily.
	if err := w.engine.RegisterWorker(ctx, w.id, "go"); err != nil {
		return fmt.Errorf("worker %s: register: %w", w.id, err)
	}
	w.log.Info("worker started", "heartbeat", w.heartbeat)

	for {
		id, err := w.dispatch.Claim(ctx)
		switch {
		case errors.Is(err, core.ErrDispatcherClosed):
			w.log.Info("worker stopped: dispatcher closed")
			return nil
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			w.log.Info("worker stopped: context done")
			return nil
		case err != nil:
			return fmt.Errorf("worker %s: claim: %w", w.id, err)
		}

		w.execute(ctx, id)
	}
}

// execute runs one task to an outcome. It never returns an error: a failure to
// run a task is reported to the engine, which owns what happens next.
func (w *Worker) execute(ctx context.Context, id core.TaskID) {
	claim, err := w.engine.ClaimTask(ctx, id, w.id)
	switch {
	case errors.Is(err, core.ErrConflict), errors.Is(err, core.ErrLeaseHeld):
		// Another worker got there first, or this is a redundant delivery.
		// Dropping it is the correct and expected outcome.
		w.log.Debug("task already claimed elsewhere", "task_id", id)
		return
	case errors.Is(err, core.ErrNotFound):
		w.log.Warn("delivered a task that does not exist", "task_id", id)
		return
	case err != nil:
		w.log.Error("claim failed", "task_id", id, "error", err)
		return
	}

	token := claim.Lease.Token

	beatCtx, stopBeating := context.WithCancel(ctx)
	toolCtx, lostOwnership := w.keepAlive(beatCtx, id, token)

	result, execErr := w.executor.Execute(toolCtx, claim.Task)
	stopBeating()

	if lostOwnership() {
		w.log.Warn("stopped work after losing the lease", "task_id", id, "token", token)
		return
	}

	// Report under the loop's context rather than the tool's: the tool's is
	// spent, and the outcome still has to be recorded.
	if execErr != nil {
		w.log.Warn("tool failed", "task_id", id, "tool", claim.Task.Type, "error", execErr)
		w.report(w.engine.FailTask(ctx, id, token, execErr), id)
		return
	}
	w.report(w.engine.CompleteTask(ctx, id, token, result), id)
}

// keepAlive renews the lease until ctx is cancelled.
//
// It returns the context the tool should run under, and a predicate reporting
// whether ownership was lost. A rejected heartbeat cancels the tool
// immediately, because continuing would be working on someone else's task.
func (w *Worker) keepAlive(ctx context.Context, id core.TaskID, token core.FencingToken) (context.Context, func() bool) {
	toolCtx, cancelTool := context.WithCancel(ctx)
	lost := make(chan struct{})

	go func() {
		defer cancelTool()

		ticker := time.NewTicker(w.heartbeat)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := w.engine.Heartbeat(ctx, id, token)
				switch {
				case err == nil:
				case errors.Is(err, core.ErrFenced), errors.Is(err, core.ErrNotFound):
					w.log.Warn("lost the lease while working", "task_id", id, "token", token)
					close(lost)
					return
				case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					return
				default:
					// A transient store error is not proof of anything. Keep
					// beating: the lease has not lapsed yet, and abandoning
					// healthy work over one failed renewal is its own bug.
					w.log.Warn("heartbeat failed, will retry", "task_id", id, "error", err)
				}
			}
		}
	}()

	return toolCtx, func() bool {
		select {
		case <-lost:
			return true
		default:
			return false
		}
	}
}

// report logs a failure to record an outcome.
//
// If the outcome cannot be written, the task stays RUNNING, its lease lapses,
// and the reaper recovers it. For a side-effecting tool the ledger already
// holds whatever was learned, so that recovery reconciles rather than guesses.
func (w *Worker) report(err error, id core.TaskID) {
	switch {
	case err == nil:
		return
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Shutdown, not a fault. Logging it at ERROR trains operators to
		// ignore ERROR.
		w.log.Debug("task outcome interrupted by shutdown", "task_id", id)
	default:
		w.log.Error("failed to record task outcome", "task_id", id, "error", err)
	}
}

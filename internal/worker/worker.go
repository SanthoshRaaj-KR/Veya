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
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Engine is what a worker needs from the runtime.
//
// Declared here rather than imported from the engine package so that the
// dependency points inward: the worker names the small surface it uses, and
// the engine happens to satisfy it.
type Engine interface {
	ClaimTask(ctx context.Context, id core.TaskID, workerID string) (core.Task, error)
	CompleteTask(ctx context.Context, id core.TaskID, result json.RawMessage) error
	FailTask(ctx context.Context, id core.TaskID, cause string) error
}

// Worker claims tasks and executes them.
type Worker struct {
	id         string
	engine     Engine
	dispatcher core.Dispatcher
	tools      core.ToolRegistry
	log        *slog.Logger
}

// Config wires a Worker. All fields except Logger are required.
type Config struct {
	ID         string
	Engine     Engine
	Dispatcher core.Dispatcher
	Tools      core.ToolRegistry
	Logger     *slog.Logger
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
	case cfg.Tools == nil:
		return nil, errors.New("worker: Tools is required")
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		id:         cfg.ID,
		engine:     cfg.Engine,
		dispatcher: cfg.Dispatcher,
		tools:      cfg.Tools,
		log:        log.With("worker_id", cfg.ID),
	}, nil
}

// ID reports the worker's identity.
func (w *Worker) ID() string { return w.id }

// Run claims and executes tasks until ctx is cancelled or the dispatcher
// closes.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "tools", w.tools.Names())

	for {
		id, err := w.dispatcher.Claim(ctx)
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
	task, err := w.engine.ClaimTask(ctx, id, w.id)
	if errors.Is(err, core.ErrConflict) {
		// Another worker got there first, or this is a redundant delivery.
		// Dropping it is the correct and expected outcome.
		w.log.Debug("task already claimed elsewhere", "task_id", id)
		return
	}
	if errors.Is(err, core.ErrNotFound) {
		w.log.Warn("delivered a task that does not exist", "task_id", id)
		return
	}
	if err != nil {
		w.log.Error("claim failed", "task_id", id, "error", err)
		return
	}

	descriptor, err := w.tools.Lookup(task.Type)
	if err != nil {
		// This worker cannot run this tool. That is a failure of the task, not
		// of the worker: report it and let the engine decide whether another
		// attempt could succeed.
		w.report(ctx, w.engine.FailTask(ctx, id, err.Error()), id)
		return
	}

	result, err := descriptor.Handler(ctx, task.Payload)
	if err != nil {
		w.log.Warn("tool failed", "task_id", id, "tool", task.Type, "error", err)
		w.report(ctx, w.engine.FailTask(ctx, id, err.Error()), id)
		return
	}

	w.report(ctx, w.engine.CompleteTask(ctx, id, result), id)
}

// report logs a failure to record an outcome.
//
// There is nothing else to do here, and that is worth being explicit about: if
// the outcome cannot be written, the task stays RUNNING and is recovered by
// the scan. From Layer 2 an ambiguous outcome on a side-effecting tool becomes
// an UNKNOWN effect rather than a lost update, which is the case this
// placeholder exists to grow into.
func (w *Worker) report(_ context.Context, err error, id core.TaskID) {
	switch {
	case err == nil:
		return
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Shutdown, not a fault. The task stays RUNNING and the next process
		// to scan recovers it. Logging this at ERROR trains operators to
		// ignore ERROR.
		w.log.Debug("task outcome interrupted by shutdown", "task_id", id)
	default:
		w.log.Error("failed to record task outcome", "task_id", id, "error", err)
	}
}

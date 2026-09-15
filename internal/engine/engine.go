// Package engine drives runs: it starts them, asks the decider what happens
// next, turns decisions into tasks, and records every step in history.
//
// It depends only on core. It does not know whether state lives in PostgreSQL
// or a map, whether tasks travel over a channel or NATS JetStream, or whether
// decisions come from a fixed list or a language model.
//
// # The one rule
//
// Everything that must be true together commits together. A task and the event
// recording it are one transaction, never two writes that could half-succeed.
// The runtime holds no authoritative state in memory: on restart it re-reads
// the store and continues, which is why a crash costs latency rather than
// correctness.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/santhoshraajkr/veya/internal/core"
)

// Engine advances runs for exactly one agent.
//
// The agent binding is not incidental. A decider only makes sense for the
// agent it was written for, so an engine must refuse to advance a run
// belonging to a different one — otherwise a process serving agent A will
// happily drive agent B's run through A's steps, which is silent corruption
// rather than a visible failure.
type Engine struct {
	store        core.Store
	dispatcher   core.Dispatcher
	decider      core.Decider
	ids          core.IDGen
	agent        string
	agentVersion string
	log          *slog.Logger
}

// Config wires an Engine. Every field except Logger is required.
type Config struct {
	Store      core.Store
	Dispatcher core.Dispatcher
	Decider    core.Decider
	IDGen      core.IDGen

	// Agent names the agent this engine serves, and AgentVersion is pinned
	// onto every run it starts. Runs for any other agent are left alone.
	Agent        string
	AgentVersion string

	Logger *slog.Logger
}

// New validates the configuration and returns an Engine.
func New(cfg Config) (*Engine, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("engine: Store is required")
	case cfg.Dispatcher == nil:
		return nil, errors.New("engine: Dispatcher is required")
	case cfg.Decider == nil:
		return nil, errors.New("engine: Decider is required")
	case cfg.IDGen == nil:
		return nil, errors.New("engine: IDGen is required")
	case cfg.Agent == "":
		return nil, errors.New("engine: Agent is required")
	case cfg.AgentVersion == "":
		return nil, errors.New("engine: AgentVersion is required")
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Engine{
		store:        cfg.Store,
		dispatcher:   cfg.Dispatcher,
		decider:      cfg.Decider,
		ids:          cfg.IDGen,
		agent:        cfg.Agent,
		agentVersion: cfg.AgentVersion,
		log:          log,
	}, nil
}

// Agent reports which agent this engine serves.
func (e *Engine) Agent() string { return e.agent }

// StartRun creates a run and drives it to its first decision.
func (e *Engine) StartRun(ctx context.Context, input json.RawMessage) (core.RunID, error) {
	id := e.ids.NewRunID()
	agentName, agentVersion := e.agent, e.agentVersion

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		run := core.Run{
			ID:           id,
			AgentName:    agentName,
			AgentVersion: agentVersion, // pinned here; a resumed run never changes it
			Status:       core.RunRunning,
			Input:        input,
		}
		if err := tx.CreateRun(ctx, run); err != nil {
			return err
		}
		return appendEvent(ctx, tx, id, core.EventRunStarted, "", core.RunStartedData{
			AgentName:    agentName,
			AgentVersion: agentVersion,
			Input:        input,
		})
	})
	if err != nil {
		return "", fmt.Errorf("start run: %w", err)
	}

	e.log.Info("run started", "run_id", id, "agent", agentName, "version", agentVersion)
	if err := e.Advance(ctx, id); err != nil {
		return id, fmt.Errorf("start run %s: %w", id, err)
	}
	return id, nil
}

// Advance asks the decider what the run owes next and commits that decision.
//
// It is safe to call more than once and from more than one place. Concurrent
// callers are serialized by the compare-and-swap on the run's version: one
// wins, the rest observe ErrConflict and return without acting, having
// established that the work is already done. That is what stops two workers
// finishing sibling tasks from both calling the model and forking the run.
func (e *Engine) Advance(ctx context.Context, runID core.RunID) error {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("advance %s: %w", runID, err)
	}
	if run.Status.IsTerminal() {
		return nil
	}
	// This engine's decider only knows this engine's agent. Advancing someone
	// else's run would drive it through the wrong steps and record the result
	// as though it were correct.
	if run.AgentName != e.agent {
		e.log.Debug("skipping run for another agent",
			"run_id", runID, "run_agent", run.AgentName, "this_agent", e.agent)
		return nil
	}

	history, err := e.store.History(ctx, runID)
	if err != nil {
		return fmt.Errorf("advance %s: %w", runID, err)
	}

	decision, err := e.decider.Decide(ctx, run, history)
	if err != nil {
		// A decider that cannot decide fails the run rather than leaving it
		// stuck: a run nobody will ever advance is invisible, and invisible
		// stalled work is worse than a recorded failure.
		return e.finish(ctx, run, core.RunState{
			Status:    core.RunFailed,
			LastError: fmt.Sprintf("decide: %v", err),
		}, core.EventRunFailed, core.RunFailedData{Error: err.Error()})
	}

	switch decision.Kind {
	case core.DecideCallTool:
		return e.dispatch(ctx, run, decision)

	case core.DecideComplete:
		return e.finish(ctx, run, core.RunState{
			Status: core.RunCompleted,
			Output: decision.Output,
		}, core.EventRunCompleted, core.RunCompletedData{Output: decision.Output})

	case core.DecideFail:
		return e.finish(ctx, run, core.RunState{
			Status:    core.RunFailed,
			LastError: decision.Error,
		}, core.EventRunFailed, core.RunFailedData{Error: decision.Error})

	default:
		return fmt.Errorf("advance %s: unknown decision kind %q", runID, decision.Kind)
	}
}

// dispatch commits a decision as work and hands it to the dispatcher.
func (e *Engine) dispatch(ctx context.Context, run core.Run, d core.Decision) error {
	taskID := e.ids.NewTaskID()

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		// The CAS first, so a losing caller stops before doing any work.
		if err := tx.AdvanceRun(ctx, run.ID, run.Version, core.RunState{
			Status: core.RunRunning,
			Output: run.Output,
		}); err != nil {
			return err
		}
		task := core.Task{
			ID:          taskID,
			RunID:       run.ID,
			StepID:      d.StepID,
			Type:        d.TaskType,
			Payload:     d.Payload,
			Status:      core.TaskPending,
			MaxAttempts: defaultMaxAttempts,
		}
		if err := tx.CreateTask(ctx, task); err != nil {
			return err
		}
		return appendEvent(ctx, tx, run.ID, core.EventTaskCreated, d.StepID, core.TaskCreatedData{
			TaskID:   taskID,
			TaskType: d.TaskType,
			Payload:  d.Payload,
		})
	})

	switch {
	case errors.Is(err, core.ErrConflict), errors.Is(err, core.ErrTaskExists):
		// Someone else advanced this run, or this exact step already exists.
		// Both mean the work is accounted for. Doing nothing is correct.
		e.log.Debug("advance lost the race", "run_id", run.ID, "step_id", d.StepID)
		return nil
	case err != nil:
		return fmt.Errorf("dispatch %s step %s: %w", run.ID, d.StepID, err)
	}

	e.log.Info("task created", "run_id", run.ID, "task_id", taskID,
		"step_id", d.StepID, "tool", d.TaskType)

	// Published after the commit, which leaves a window: if the process dies
	// here, the task is durable but undelivered. That is survivable because
	// PendingTasks finds it on restart — see Runtime.
	//
	// Layer 3 closes the window properly by writing an outbox row inside the
	// transaction above and having a relay publish it.
	if err := e.dispatcher.Publish(ctx, taskID); err != nil {
		e.log.Warn("publish failed; task will be recovered by scan",
			"task_id", taskID, "error", err)
	}
	return nil
}

// finish moves a run to a terminal state.
func (e *Engine) finish(ctx context.Context, run core.Run, next core.RunState, evt core.EventType, data any) error {
	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.AdvanceRun(ctx, run.ID, run.Version, next); err != nil {
			return err
		}
		return appendEvent(ctx, tx, run.ID, evt, "", data)
	})
	if errors.Is(err, core.ErrConflict) {
		return nil // another caller finished it
	}
	if err != nil {
		return fmt.Errorf("finish run %s: %w", run.ID, err)
	}

	e.log.Info("run finished", "run_id", run.ID, "status", next.Status)
	return nil
}

// appendEvent reads the next sequence and writes one event, inside tx.
//
// The sequence is read and used in the same transaction, so the conditional
// append on (run_id, seq) is a real check rather than a formality: a racing
// writer that read the same number loses on commit.
func appendEvent(ctx context.Context, tx core.Tx, runID core.RunID, typ core.EventType, step core.StepID, data any) error {
	seq, err := tx.NextSeq(ctx, runID)
	if err != nil {
		return err
	}
	ev, err := core.NewEvent(runID, seq, typ, step, data)
	if err != nil {
		return err
	}
	return tx.AppendEvent(ctx, ev)
}

// defaultMaxAttempts is how many times a task may be tried before it is dead
// lettered. Per-tool retry policy and backoff arrive in Layer 5; until then
// retries are immediate, which is honest but not yet kind to a struggling
// downstream service.
const defaultMaxAttempts = 3

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
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
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
	clock        core.Clock
	leaseTTL     time.Duration
	agent        string
	agentVersion string
	wake         func()
	log          *slog.Logger
}

// Config wires an Engine. Every field except Logger is required.
type Config struct {
	Store      core.Store
	Dispatcher core.Dispatcher
	Decider    core.Decider
	IDGen      core.IDGen
	Clock      core.Clock

	// LeaseTTL is how long a claim is good for before a worker must renew it.
	//
	// It trades recovery speed against tolerance for a slow tool: too short
	// and a healthy worker loses work it is still doing, too long and a dead
	// worker's task sits idle. Both are liveness costs, which is why an
	// imperfect value here is survivable — README section 16. Per-task-type
	// TTLs arrive in Layer 6, when an LLM call and a deployment stop deserving
	// the same timeout.
	LeaseTTL time.Duration

	// Agent names the agent this engine serves, and AgentVersion is pinned
	// onto every run it starts. Runs for any other agent are left alone.
	Agent        string
	AgentVersion string

	// Wake tells the outbox relay that something was just committed.
	//
	// It is a plain optional func rather than a port because it carries no
	// guarantee: dropping every call would cost latency and nothing else, since
	// the delivery is already committed and the relay sweeps on a timer. The
	// type is the documentation — safety went into the transaction, and this is
	// the liveness half that is allowed to be missed.
	Wake func()

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
	case cfg.Clock == nil:
		return nil, errors.New("engine: Clock is required")
	case cfg.Agent == "":
		return nil, errors.New("engine: Agent is required")
	case cfg.AgentVersion == "":
		return nil, errors.New("engine: AgentVersion is required")
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	wake := cfg.Wake
	if wake == nil {
		wake = func() {} // no relay to nudge; its ticker will find the row
	}
	return &Engine{
		store:        cfg.Store,
		dispatcher:   cfg.Dispatcher,
		decider:      cfg.Decider,
		ids:          cfg.IDGen,
		clock:        cfg.Clock,
		leaseTTL:     leaseTTL,
		agent:        cfg.Agent,
		agentVersion: cfg.AgentVersion,
		wake:         wake,
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
		return core.Append(ctx, tx, id, core.EventRunStarted, "", core.RunStartedData{
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

	// A parked run is not owed a decision, and must not be asked for one. The
	// check is here rather than only in the scan's query because Advance is
	// reachable from the CLI and from a sibling task's completion, and a
	// sleeping run asked to decide would re-derive the sleep it is already
	// serving -- harmless, but it would mean a round trip to the agent body on
	// every poke, for a run whose answer cannot have changed.
	run, history, ready, err := e.resume(ctx, run, history)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	// A fan-out whose join is not yet satisfiable has nothing new to say.
	// Every child completing would otherwise cost a full round trip to the
	// agent body, which would replay, find the join unmet, and re-issue the
	// fan-out it already issued -- correct, and N-1 times more expensive
	// than it needs to be when the body lives in another process.
	//
	// This can only skip work. The body stays the authority on what a join
	// means: when the join is satisfied, or when this build cannot read the
	// policy, the body is asked and its answer stands.
	if pending, waiting := core.PendingFanOut(history); waiting {
		succeeded, failed, settled := pending.Counts()
		e.log.Debug("fan-out is still joining",
			"run_id", run.ID, "step_id", pending.StepID, "join", pending.Join.String(),
			"succeeded", succeeded, "failed", failed, "settled", settled,
			"children", len(pending.Children))
		return nil
	}

	decision, err := e.decider.Decide(ctx, run, history)
	if errors.Is(err, core.ErrUnavailable) {
		// Not the run's fault, and not permanent: no worker has connected yet,
		// or the connected one serves a version this run is not pinned to.
		// Leaving the run RUNNING lets the recovery loop try again once
		// whatever is missing arrives. Logged at ERROR because a run that
		// cannot advance is still a problem, just not this run's problem — and
		// an operator who cannot see it has a silently stalled run, which is
		// the thing the branch below exists to avoid.
		e.log.Error("cannot advance this run yet", "run_id", run.ID,
			"agent", run.AgentName, "version", run.AgentVersion, "reason", err)
		return fmt.Errorf("advance %s: %w", runID, err)
	}
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

	case core.DecideCallToolParallel:
		return e.dispatchParallel(ctx, run, decision)

	case core.DecideSleep:
		// Nothing holds this wake-up. There is no timer goroutine and no
		// time.After owning state a restart would lose: the instant goes in
		// the database, and the recovery scan -- which already looks for runs
		// owed a decision -- is what notices it has arrived.
		return e.park(ctx, run, decision.WakeAt, core.EventTimerSet, decision.StepID,
			core.TimerSetData{WakeAt: decision.WakeAt})

	case core.DecideWaitForSignal:
		// Park at the deadline, or indefinitely when there is none. The
		// signal itself is not looked for here: resume does that on the way
		// in, and it will do it again the instant a delivery un-parks the
		// run. Checking in both places would be two chances to disagree
		// about what counts as already consumed.
		until := decision.Signal.Deadline
		if until.IsZero() {
			until = core.Indefinite
		}
		if err := e.park(ctx, run, until, core.EventSignalWaitStarted, decision.StepID,
			core.SignalWaitStartedData{
				Name:     decision.Signal.Name,
				Deadline: decision.Signal.Deadline,
			}); err != nil {
			return err
		}

		// Then look immediately, because an arrival that beat the run here is
		// already stored and nothing is going to un-park the run for it: the
		// release happens on arrival, and arrival already happened. Parking
		// and waiting to be woken would be the early-signal bug moved one
		// level up -- a run asleep forever on something that has occurred.
		//
		// Advance rather than a check here, so the stored signal is consumed
		// by exactly the code that consumes a late one. Each pass takes one
		// signal, so this ends.
		return e.Advance(ctx, run.ID)

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

// dispatch commits a decision as work, together with the intent to deliver it.
//
// The task, the event recording it, and the outbox row are one transaction.
// Publishing used to happen after the commit, which left a window where the
// task was durable and nobody would ever be told about it — a run that stops
// with nothing failed and nothing to retry. Now the only thing after the commit
// is a hint to the relay, and a hint that is lost costs latency, not work.
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
		if err := core.Append(ctx, tx, run.ID, core.EventTaskCreated, d.StepID, core.TaskCreatedData{
			TaskID:   taskID,
			TaskType: d.TaskType,
			Payload:  d.Payload,
		}); err != nil {
			return err
		}
		return tx.EnqueueDelivery(ctx, taskID)
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

	// Everything durable is already done. This only saves the relay from
	// waiting out its ticker, so it has no error to check and nothing to
	// recover: a wake that never arrives is a slower delivery, not a lost one.
	e.wake()
	return nil
}

// finish moves a run to a terminal state.
func (e *Engine) finish(ctx context.Context, run core.Run, next core.RunState, evt core.EventType, data any) error {
	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.AdvanceRun(ctx, run.ID, run.Version, next); err != nil {
			return err
		}
		return core.Append(ctx, tx, run.ID, evt, "", data)
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

// resume releases a parked run whose wait is over, and reports whether the run
// is ready to be asked for a decision.
//
// It runs before the decider on every advance, and it is what turns an event
// in the outside world into a fact in history. The agent body cannot look at a
// clock, and cannot query the signals table: an answer that changes between
// replays diverges the step after the one that read it. So the engine looks
// once, records what it found, and the body reads that record back like any
// other history.
//
// A run whose wait is still running comes back not ready, and Advance stops
// without asking anyone anything.
func (e *Engine) resume(ctx context.Context, run core.Run, history []core.Event) (core.Run, []core.Event, bool, error) {
	wait, waiting := core.PendingWait(history)
	if !waiting {
		return run, history, true, nil
	}

	evt, data, over, err := e.waitIsOver(ctx, run, history, wait)
	if err != nil {
		return run, history, false, err
	}
	if !over {
		e.log.Debug("run is still waiting",
			"run_id", run.ID, "step_id", wait.StepID, "kind", wait.Kind, "until", wait.Until)
		// Something released the run without satisfying its wait -- a signal
		// under a different name, most often. Re-park it, or the scan will
		// pick it up on every pass and ask a decider that can only answer
		// "still waiting".
		if run.AvailableAt == nil {
			if err := e.repark(ctx, run, wait); err != nil {
				return run, history, false, err
			}
		}
		return run, history, false, nil
	}

	err = e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		// Clearing the park and recording why it ended is one transaction.
		// Split, a crash between them leaves either a released run whose
		// history says it is still waiting, or a run marked waiting that
		// history says resumed -- and both are a run that never resumes.
		if err := tx.AdvanceRun(ctx, run.ID, run.Version, core.RunState{
			Status:      core.RunRunning,
			Output:      run.Output,
			AvailableAt: nil, // released
		}); err != nil {
			return err
		}
		return core.Append(ctx, tx, run.ID, evt, wait.StepID, data)
	})
	if errors.Is(err, core.ErrConflict) {
		// Another process released it first. Nothing to do, and nothing wrong:
		// it will be advanced by whoever won.
		return run, history, false, nil
	}
	if err != nil {
		return run, history, false, fmt.Errorf("resume %s: %w", run.ID, err)
	}

	e.log.Info("run resumed", "run_id", run.ID, "step_id", wait.StepID,
		"waited_for", wait.Kind, "because", evt)

	// Re-read both, because the decider is entitled to a history that includes
	// the wake-up it is about to be asked to replay past.
	run, err = e.store.GetRun(ctx, run.ID)
	if err != nil {
		return run, history, false, fmt.Errorf("resume %s: %w", run.ID, err)
	}
	history, err = e.store.History(ctx, run.ID)
	if err != nil {
		return run, history, false, fmt.Errorf("resume %s: %w", run.ID, err)
	}
	return run, history, true, nil
}

// waitIsOver decides whether a suspension has ended, and what history should
// record about how.
//
// It is the only place that reads the outside world on a waiting run's behalf,
// which is why it returns an event rather than a boolean: what ended the wait
// has to become a durable fact, or the body cannot tell on replay whether its
// approval arrived or its deadline passed.
func (e *Engine) waitIsOver(ctx context.Context, run core.Run, history []core.Event,
	wait core.Wait) (core.EventType, any, bool, error) {

	now := e.clock.Now()

	switch wait.Kind {
	case core.WaitTimer:
		if !wait.Elapsed(now) {
			return "", nil, false, nil
		}
		return core.EventTimerFired, core.TimerFiredData{WakeAt: wait.Until}, true, nil

	case core.WaitSignal:
		// The read that replaces a delivery. A signal that arrived before the
		// run got here is already stored, so this finds it on the first pass
		// and the early-signal race has nowhere to happen.
		sig, found, err := e.unconsumedSignal(ctx, run.ID, history, wait)
		if err != nil {
			return "", nil, false, err
		}
		if found {
			return core.EventSignalReceived, core.SignalReceivedData{
				Name:     sig.Name,
				SignalID: sig.ID,
				Payload:  sig.Payload,
			}, true, nil
		}
		if wait.Elapsed(now) {
			// The deadline passed with nothing to read. Recorded rather than
			// silently resumed, because the body has to be able to tell a
			// signal that arrived from one that never did -- and it cannot
			// look at a clock to work it out for itself.
			return core.EventSignalWaitTimedOut, core.SignalWaitTimedOutData{
				Name:     wait.Signal,
				Deadline: wait.Until,
			}, true, nil
		}
		return "", nil, false, nil

	default:
		// A wait kind this build does not understand keeps the run parked.
		// Visibly stuck beats resumed on a suspension nobody could read.
		e.log.Error("run is parked on a wait this build does not understand",
			"run_id", run.ID, "step_id", wait.StepID, "kind", wait.Kind)
		return "", nil, false, nil
	}
}

// park suspends a run until an instant, recording what it is waiting for.
//
// The run stays RUNNING. A waiting run is still running, and anything that can
// happen to a RUNNING run can happen to it -- which is the argument against a
// WAITING state and the reason this is one column.
func (e *Engine) park(ctx context.Context, run core.Run, until time.Time,
	evt core.EventType, step core.StepID, data any) error {

	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.AdvanceRun(ctx, run.ID, run.Version, core.RunState{
			Status:      core.RunRunning,
			Output:      run.Output,
			AvailableAt: &until,
		}); err != nil {
			return err
		}
		return core.Append(ctx, tx, run.ID, evt, step, data)
	})
	if errors.Is(err, core.ErrConflict) {
		// Someone else advanced this run. Whatever they did, this decision is
		// stale; doing nothing is correct, exactly as it is in dispatch.
		e.log.Debug("park lost the race", "run_id", run.ID, "step_id", step)
		return nil
	}
	if err != nil {
		return fmt.Errorf("park %s step %s: %w", run.ID, step, err)
	}

	e.log.Info("run parked", "run_id", run.ID, "step_id", step,
		"until", until, "indefinite", core.IsIndefinite(until))
	return nil
}

// repark puts a run back to sleep on a wait it is already serving.
//
// It records nothing. The wait is already in history; appending a second
// SIGNAL_WAIT_STARTED for the same step would make the log say the run waited
// twice, and would leave PendingWait reading a suspension that never closes
// because only one of the two ever gets a matching resolution.
//
// It exists because a release is not a satisfaction: a signal under a
// different name un-parks the run, the wait it is serving is untouched, and
// without this the scan would pick the run up on every pass forever.
func (e *Engine) repark(ctx context.Context, run core.Run, wait core.Wait) error {
	until := wait.Until
	err := e.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.AdvanceRun(ctx, run.ID, run.Version, core.RunState{
			Status:      core.RunRunning,
			Output:      run.Output,
			AvailableAt: &until,
		})
	})
	if errors.Is(err, core.ErrConflict) {
		return nil // someone else moved the run on; their view is the newer one
	}
	if err != nil {
		return fmt.Errorf("repark %s: %w", run.ID, err)
	}
	return nil
}

// DefaultLeaseTTL is how long a claim lasts without a heartbeat.
//
// Thirty seconds is short enough that a crashed worker's task is recoverable
// quickly, and long enough that an ordinary tool call finishes inside one
// lease even if the process is briefly busy.
const DefaultLeaseTTL = 30 * time.Second

// defaultMaxAttempts is how many times a task may be tried before it is dead
// lettered. Per-tool retry policy and backoff arrive in Layer 6; until then
// retries are immediate, which is honest but not yet kind to a struggling
// downstream service.
const defaultMaxAttempts = 3

// Package effects runs tool calls through the effect ledger.
//
// This is where the project's central claim is actually enforced. Everything
// else — leases, fencing, the claim transition — reduces the chance of a
// duplicate attempt. This package makes a duplicate attempt harmless, which is
// a different and stronger property, because the chance of a duplicate attempt
// can never be reduced to zero.
//
// # The ordering rule
//
// Write down what is about to happen, commit that, and only then act:
//
//  1. reserve the effect (PENDING) and COMMIT
//  2. mark it RUNNING and COMMIT
//  3. call the provider
//  4. record what happened
//
// A crash after step 1 leaves PENDING, which recovery reads as "nothing was
// sent, safe to run". A crash after step 3 leaves RUNNING, which recovery
// reads as "this may have happened, find out". Reverse the order — call first,
// record after — and a crash in between leaves no trace at all, so recovery
// sees a clean slate and runs the action a second time. There is no mechanism
// anywhere else in the system that could catch that, which is why this
// ordering is the load-bearing detail of the whole design.
package effects

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// effectSeq is the position of an effect within its step.
//
// A task performs one tool call, so there is exactly one external action per
// step and this is always 1. The concept is still threaded through the key,
// because it is what will distinguish sub-effects when the SDK lets one tool
// declare several, and retrofitting the numbering later would invalidate every
// key ever issued.
const effectSeq = 1

// Executor runs a task's tool, through the ledger when the tool has
// consequences.
type Executor struct {
	store core.Store
	tools core.ToolRegistry
	ids   core.IDGen
	clock core.Clock
	log   *slog.Logger
}

// Config wires an Executor. Every field except Logger is required.
type Config struct {
	Store core.Store
	Tools core.ToolRegistry
	IDGen core.IDGen
	Clock core.Clock
	Log   *slog.Logger
}

// New validates the configuration and returns an Executor.
func New(cfg Config) (*Executor, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("effects: Store is required")
	case cfg.Tools == nil:
		return nil, errors.New("effects: Tools is required")
	case cfg.IDGen == nil:
		return nil, errors.New("effects: IDGen is required")
	case cfg.Clock == nil:
		return nil, errors.New("effects: Clock is required")
	}

	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	return &Executor{
		store: cfg.Store,
		tools: cfg.Tools,
		ids:   cfg.IDGen,
		clock: cfg.Clock,
		log:   log,
	}, nil
}

// Execute runs one task's tool and returns its result.
func (x *Executor) Execute(ctx context.Context, task core.Task) (json.RawMessage, error) {
	descriptor, err := x.tools.Lookup(task.Type)
	if err != nil {
		return nil, err
	}

	// The fast path. A pure read that happens twice costs latency, not
	// correctness, so taxing it with two extra transactions would be paying
	// for a guarantee it does not need. It gets no key, because it has no
	// ledger row and nothing to deduplicate.
	if !descriptor.Class.HasConsequence() {
		return x.call(ctx, descriptor, callFor(task, ""))
	}

	key := core.NewIdempotencyKey(task.RunID, task.StepID, effectSeq)
	effect, err := x.reserve(ctx, task, descriptor, key)
	if err != nil {
		return nil, err
	}

	// A reservation that found an existing record decides what happens next
	// from the ledger, never from optimism.
	switch action := core.RecoveryFor(true, effect.Status, descriptor.Class); action {
	case core.RecoverComplete:
		// Already done. Return what the provider said the first time rather
		// than asking it to do the same thing again.
		x.log.Info("effect already committed; returning the recorded result",
			"key", key, "external_ref", effect.ExternalRef)
		return effect.Response, nil

	case core.RecoverExecute:
		return x.perform(ctx, task, descriptor, effect)

	case core.RecoverReconcile:
		return x.resolve(ctx, task, descriptor, effect)

	case core.RecoverEscalate:
		return nil, x.escalate(ctx, task, effect,
			fmt.Sprintf("%s is %s and %s outcomes cannot be resolved automatically",
				key, effect.Status, descriptor.Class))

	default:
		return nil, fmt.Errorf("effect %s: unhandled recovery action %q", key, action)
	}
}

// reserve records the intent to act, and commits it before anything is sent.
//
// ReserveEffect is one conditional insert. When the key is taken the existing
// row is returned instead: that is not a failure but the duplicate-suppression
// path, and the caller branches on the status it finds.
func (x *Executor) reserve(ctx context.Context, task core.Task, d core.ToolDescriptor, key core.IdempotencyKey) (core.Effect, error) {
	fresh := core.Effect{
		ID:      x.ids.NewEffectID(),
		TaskID:  task.ID,
		RunID:   task.RunID,
		Type:    task.Type,
		Class:   d.Class,
		Key:     key,
		Status:  core.EffectPending,
		Request: task.Payload,
	}

	err := x.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.ReserveEffect(ctx, fresh); err != nil {
			return err
		}
		return core.Append(ctx, tx, task.RunID, core.EventEffectCreated, task.StepID, effectData(fresh, ""))
	})

	switch {
	case err == nil:
		return fresh, nil
	case errors.Is(err, core.ErrEffectExists):
		existing, err := x.store.GetEffect(ctx, key)
		if err != nil {
			return core.Effect{}, fmt.Errorf("load existing effect %s: %w", key, err)
		}
		x.log.Debug("effect already reserved", "key", key, "status", existing.Status)
		return existing, nil
	default:
		return core.Effect{}, fmt.Errorf("reserve effect %s: %w", key, err)
	}
}

// perform moves the effect to RUNNING, commits that, then calls the provider.
func (x *Executor) perform(ctx context.Context, task core.Task, d core.ToolDescriptor, effect core.Effect) (json.RawMessage, error) {
	if err := x.transition(ctx, task, effect, effect.Status, core.EffectRunning, core.EffectOutcome{}, ""); err != nil {
		return nil, err
	}

	result, callErr := x.call(ctx, d, callFor(task, effect.Key))
	if callErr == nil {
		out := core.EffectOutcome{
			Response:    result,
			ExternalRef: externalRef(result),
		}
		if err := x.transition(ctx, task, effect, core.EffectRunning, core.EffectCommitted, out,
			core.EventEffectCommitted); err != nil {
			// The provider acted but we could not record it. Leaving the row
			// RUNNING is correct: recovery will treat it as ambiguous and
			// reconcile, which is exactly what it is.
			return nil, fmt.Errorf("record commit for %s: %w", effect.Key, err)
		}
		return result, nil
	}

	// This is the decision the whole package exists for. An error after the
	// request may have gone out is not evidence that it did not.
	outcome := core.ClassifyFailure(callErr)
	event := core.EventEffectFailed
	if outcome == core.EffectUnknown {
		event = core.EventEffectUnknown
	}
	if err := x.transition(ctx, task, effect, core.EffectRunning, outcome,
		core.EffectOutcome{Error: callErr.Error()}, event); err != nil {
		return nil, fmt.Errorf("record %s for %s: %w", outcome, effect.Key, err)
	}

	if outcome == core.EffectUnknown {
		x.log.Warn("effect outcome is unknown; it will be reconciled, not retried",
			"key", effect.Key, "tool", task.Type, "error", callErr)
		return nil, fmt.Errorf("%s: %w: %w", effect.Key, core.ErrNeedsReconciliation, callErr)
	}
	return nil, callErr
}

// call invokes the tool, converting a panic into an ambiguous outcome.
//
// A panic mid-call proves nothing about whether the request was sent, so it is
// treated the same as any other unexplained failure: unknown, not failed.
func (x *Executor) call(ctx context.Context, d core.ToolDescriptor, call core.ToolCall) (result json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("tool %s panicked: %v", d.Name, r)
		}
	}()
	return d.Handler(ctx, call)
}

// callFor describes one invocation to the tool that is about to run it.
//
// The key is the part that matters. A tool declaring IDEMPOTENT_BY_KEY is
// asserting that its provider deduplicates by key, which it cannot make true
// without being given the key — so passing it is not a convenience, it is what
// makes the class mean anything.
func callFor(task core.Task, key core.IdempotencyKey) core.ToolCall {
	return core.ToolCall{
		RunID:   task.RunID,
		TaskID:  task.ID,
		StepID:  task.StepID,
		Key:     key,
		Payload: task.Payload,
		Attempt: task.Attempt,
	}
}

// transition records an effect's new status and, optionally, an event.
func (x *Executor) transition(
	ctx context.Context,
	task core.Task,
	effect core.Effect,
	from, to core.EffectStatus,
	out core.EffectOutcome,
	event core.EventType,
) error {
	return x.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.TransitionEffect(ctx, effect.Key, from, to, out); err != nil {
			return err
		}
		if event == "" {
			return nil
		}
		updated := effect
		updated.Status = to
		updated.Response = out.Response
		updated.ExternalRef = out.ExternalRef
		updated.LastError = out.Error
		return core.Append(ctx, tx, task.RunID, event, task.StepID, effectData(updated, ""))
	})
}

// escalate records that only a human can settle this, and stops.
//
// The runtime will not guess. A tool that neither deduplicates nor answers
// questions cannot be made exactly-once by any runtime, and pretending
// otherwise would mean choosing between a possible duplicate and a possible
// silent drop on the operator's behalf.
func (x *Executor) escalate(ctx context.Context, task core.Task, effect core.Effect, reason string) error {
	err := x.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return core.Append(ctx, tx, task.RunID, core.EventEffectEscalated, task.StepID,
			effectData(effect, reason))
	})
	if err != nil {
		x.log.Error("failed to record escalation", "key", effect.Key, "error", err)
	}

	x.log.Error("effect needs human resolution", "key", effect.Key,
		"status", effect.Status, "class", effect.Class, "reason", reason)
	return fmt.Errorf("%s: %w: %s", effect.Key, core.ErrEscalated, reason)
}

func effectData(e core.Effect, detail string) core.EffectData {
	return core.EffectData{
		TaskID:      e.TaskID,
		Key:         e.Key,
		EffectType:  e.Type,
		Class:       e.Class,
		Status:      e.Status,
		ExternalRef: e.ExternalRef,
		Response:    e.Response,
		Error:       e.LastError,
		Detail:      detail,
	}
}

// externalRef pulls the provider's own identifier out of a response, if it
// offered one under a conventional name.
//
// Best effort by design. The reference is for operators and for QUERYABLE
// lookups; the idempotency key, which we control, is what correctness rests on.
func externalRef(response json.RawMessage) string {
	if len(response) == 0 {
		return ""
	}
	var fields map[string]any
	if err := json.Unmarshal(response, &fields); err != nil {
		return ""
	}
	for _, name := range []string{"reference", "external_ref", "id", "reference_id"} {
		if v, ok := fields[name].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

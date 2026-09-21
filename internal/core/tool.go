package core

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"
)

// ToolCall is everything a tool is told about the call it is making.
//
// It is a struct rather than a payload argument because of Key. A tool
// classified IDEMPOTENT_BY_KEY is asserting that its provider deduplicates by
// idempotency key — and it cannot make that true unless it is given the key to
// send. The same goes for QUERYABLE: a reconciler is asked "did the action
// under this key happen?", which only has an answer if the original call
// recorded the key with the provider.
//
// So the key travels with the call. Without it the two classes that carry the
// design's central guarantee would be declarations a tool has no way to honour.
type ToolCall struct {
	RunID  RunID
	TaskID TaskID
	StepID StepID

	// Key is the idempotency key for this call: stable across every retry,
	// reassignment and replay of this logical step, and the thing to hand the
	// provider as its Idempotency-Key header or equivalent.
	//
	// Empty for a NONE-class tool, which has no ledger row and nothing to
	// deduplicate.
	Key IdempotencyKey

	// Payload is opaque JSON. The runtime never interprets it.
	Payload json.RawMessage

	// Attempt is which try this is, starting at 1. For logging and for a tool
	// that wants to reason about its own retries; nothing about correctness
	// depends on it.
	Attempt int
}

// ToolHandler executes one tool call. Payload and return value are opaque
// JSON: the runtime never interprets either.
type ToolHandler func(ctx context.Context, call ToolCall) ([]byte, error)

// ToolDescriptor is everything the runtime needs to know about a tool.
//
// Behaviour lives on the descriptor rather than in a switch somewhere upstream.
// A `switch toolName` above this type is the coupling this design exists to
// prevent: it would put knowledge of specific tools inside the engine, and the
// engine's correctness must not depend on which tools happen to be registered.
type ToolDescriptor struct {
	Name    string
	Class   EffectClass
	Handler ToolHandler

	// Reconciler resolves an UNKNOWN effect. Required for ClassQueryable and
	// meaningless for every other class: IDEMPOTENT_BY_KEY resolves by
	// re-sending, NONE has nothing to resolve, and UNRECONCILABLE is
	// unresolvable by definition.
	Reconciler Reconciler

	// KeyTTL is how long the provider actually honours an idempotency key.
	//
	// Half of an exactly-once guarantee lives in someone else's system, under
	// someone else's retention policy. Stripe forgets keys after 24 hours; a
	// QUERYABLE effect reconciled past that window returns "not found", which
	// means the provider forgot rather than that nothing happened. Re-sending
	// on that basis is precisely the duplicate this design prevents, so past
	// KeyTTL an unresolved effect escalates instead of reconciling.
	//
	// Zero means no expiry is claimed, which is only honest for a provider
	// that documents indefinite retention.
	KeyTTL time.Duration
}

// KeyExpired reports whether an effect is older than its provider's key
// retention, measured from when the effect was created.
func (d ToolDescriptor) KeyExpired(e Effect, now time.Time) bool {
	if d.KeyTTL <= 0 {
		return false
	}
	return now.Sub(e.CreatedAt) > d.KeyTTL
}

// RetryPolicy is how many times a failed task is retried, and how long it
// waits between attempts.
//
// Engine-wide today, not per tool. KeyTTL sits on ToolDescriptor because a
// provider's key retention is a fact about that provider; a retry curve is
// not obviously the same kind of fact, and a struct is a bigger thing to make
// public on the descriptor than one duration was — see
// docs/execution-model.md section 8 and status.md section 5.2. One lease TTL
// for every task type has the identical shape and the identical open item.
//
// The zero value is today's behaviour exactly: MaxAttempts of zero is read as
// 3 by the caller, and a zero Backoff at every attempt means immediate
// retry, unchanged from before this type existed.
type RetryPolicy struct {
	// MaxAttempts is how many tries a task gets before it is dead-lettered.
	// Zero means the caller's default (3).
	MaxAttempts int

	// InitialBackoff is the delay before the second attempt. Zero means no
	// delay — retries stay immediate, which is what an unset policy must do,
	// since that is the only value every run committed before this existed
	// can be read as having chosen.
	InitialBackoff time.Duration

	// MaxBackoff caps the curve. Zero means uncapped, which only matters once
	// Multiplier makes the curve grow at all.
	MaxBackoff time.Duration

	// Multiplier grows the delay each attempt. Zero or less than 1 is read as
	// 1: a policy that set a backoff but forgot to make it grow gets a fixed
	// delay, not a curve that never advances and not one that errors.
	Multiplier float64

	// Jitter is the fraction of the computed delay to randomize away, in
	// [0,1]. A fleet of tasks that all failed together and all retry on the
	// same fixed curve hits the provider at the same instant every time;
	// jitter is what keeps a thundering herd from reconstituting itself on
	// every attempt. Values outside [0,1] are clamped.
	Jitter float64
}

// Backoff returns how long to wait before retrying, given which attempt just
// failed (1 for the first failure, matching Task.Attempt).
//
// Not required to be replay-deterministic: the decider never sees this value
// or the instant it produces, only that a retry happened, so jitter drawn
// from the package-level random source is honest rather than a determinism
// risk — unlike ctx.random() in the SDK, which a replayed agent body does
// see.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if p.InitialBackoff <= 0 || attempt < 1 {
		return 0
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}

	delay := float64(p.InitialBackoff)
	// attempt-1 doublings: attempt 1 gets InitialBackoff itself.
	for i := 1; i < attempt; i++ {
		delay *= mult
		if p.MaxBackoff > 0 && delay >= float64(p.MaxBackoff) {
			delay = float64(p.MaxBackoff)
			break
		}
	}
	if p.MaxBackoff > 0 && delay > float64(p.MaxBackoff) {
		delay = float64(p.MaxBackoff)
	}

	jitter := p.Jitter
	switch {
	case jitter <= 0:
		return time.Duration(delay)
	case jitter > 1:
		jitter = 1
	}
	// Randomize away up to `jitter` of the delay, keeping the rest as a floor
	// — a fully-jittered retry that lands at 0 is indistinguishable from no
	// backoff at all, which defeats the point of having asked for one.
	floor := delay * (1 - jitter)
	spread := delay * jitter
	return time.Duration(floor + rand.Float64()*spread)
}

// ToolRegistry resolves a task type to its descriptor.
type ToolRegistry interface {
	Lookup(name string) (ToolDescriptor, error)
	Names() []string
}

// ResolutionKind is what a reconciliation attempt established.
type ResolutionKind string

const (
	// ResolvedCommitted: the action definitely happened.
	ResolvedCommitted ResolutionKind = "COMMITTED"
	// ResolvedNotExecuted: the action definitely did not happen, so it is safe
	// to execute now.
	ResolvedNotExecuted ResolutionKind = "NOT_EXECUTED"
	// ResolvedUnknown: still cannot tell. Saying so is a valid and honest
	// answer; guessing is not.
	ResolvedUnknown ResolutionKind = "STILL_UNKNOWN"
)

// Resolution is the outcome of asking a provider what it actually did.
type Resolution struct {
	Kind        ResolutionKind
	Response    json.RawMessage
	ExternalRef string
	Detail      string
}

// Reconciler answers "did this actually happen?" for one effect.
//
// Implementations must not perform the action. A reconciler that executes
// rather than queries turns the reconciliation of an already-committed effect
// into a duplicate, which is the exact failure the ledger exists to prevent.
type Reconciler interface {
	Reconcile(ctx context.Context, e Effect) (Resolution, error)
}

// ReconcilerFunc adapts a function to Reconciler.
type ReconcilerFunc func(ctx context.Context, e Effect) (Resolution, error)

func (f ReconcilerFunc) Reconcile(ctx context.Context, e Effect) (Resolution, error) {
	return f(ctx, e)
}

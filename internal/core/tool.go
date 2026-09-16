package core

import (
	"context"
	"encoding/json"
	"time"
)

// ToolHandler executes one tool call. Payload and return value are opaque
// JSON: the runtime never interprets either.
type ToolHandler func(ctx context.Context, payload []byte) ([]byte, error)

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

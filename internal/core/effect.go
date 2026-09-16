package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// EffectID is the runtime's internal identity for an effect row.
//
// Kept separate from IdempotencyKey because the two answer different
// questions and have different owners: this one is ours, the key is the
// provider's, and providers impose their own format and length limits.
type EffectID string

// IdempotencyKey is the provider-facing identity of one logical external
// action: run_id:step_id:E<seq>, for example "R123:S2:E1".
type IdempotencyKey string

func (k IdempotencyKey) String() string { return string(k) }

// NewIdempotencyKey derives the stable identity of an external action from its
// logical position in a run.
//
// This is a pure function and the only place a key is constructed. Two
// properties matter and both come from using position rather than content:
//
//   - It survives retry, reassignment and replay. Step 2 of run R123 is step 2
//     of run R123 however many times it is attempted.
//   - It does not change when the payload does. Two logically identical
//     refunds differing only in a free-text reason hash differently and would
//     both execute; logical position has no such failure mode.
//
// seq distinguishes multiple external actions within one step, numbered by
// invocation order. Today a task performs one tool call so seq is always 1;
// the parameter exists because that changes when the SDK lets a tool declare
// sub-effects, and inventing the numbering later would break every key.
func NewIdempotencyKey(run RunID, step StepID, seq int) IdempotencyKey {
	return IdempotencyKey(fmt.Sprintf("%s:%s:E%d", run, step, seq))
}

// EffectStatus is what the runtime believes about an external action. Values
// match the effect_status enum in migration 0001.
type EffectStatus string

const (
	// EffectPending means we intend to act. Nothing has been sent.
	EffectPending EffectStatus = "PENDING"
	// EffectRunning means a request may be in flight right now.
	EffectRunning EffectStatus = "RUNNING"
	// EffectCommitted means the external world definitely changed.
	EffectCommitted EffectStatus = "COMMITTED"
	// EffectFailed means it definitely did not happen.
	EffectFailed EffectStatus = "FAILED"

	// EffectUnknown means we genuinely do not know.
	//
	// This is the state that justifies the entire design. It is not a synonym
	// for failure: a request that was transmitted and never answered may well
	// have succeeded. Treating that as failure and retrying is the single most
	// common way systems send a second email or issue a second refund.
	//
	// UNKNOWN triggers reconciliation, never a blind retry.
	EffectUnknown EffectStatus = "UNKNOWN"
)

// IsResolved reports whether the outcome is settled and needs nothing further.
func (s EffectStatus) IsResolved() bool {
	return s == EffectCommitted || s == EffectFailed
}

// CanTransitionTo reports whether s -> next is legal.
//
// The permissive-looking edges are deliberate:
//
//	FAILED  -> RUNNING   FAILED means it definitely did not happen, so
//	                     re-executing is safe. That is the whole value of
//	                     distinguishing FAILED from UNKNOWN.
//	UNKNOWN -> RUNNING   an IDEMPOTENT_BY_KEY effect is reconciled by
//	                     re-sending under the same key.
//	UNKNOWN -> UNKNOWN   a reconciliation attempt that still cannot tell must
//	                     be able to say so without lying in either direction.
func (s EffectStatus) CanTransitionTo(next EffectStatus) bool {
	switch s {
	case EffectPending:
		return next == EffectRunning || next == EffectFailed
	case EffectRunning:
		return next == EffectCommitted || next == EffectFailed || next == EffectUnknown
	case EffectFailed:
		return next == EffectRunning
	case EffectUnknown:
		return next == EffectCommitted || next == EffectFailed ||
			next == EffectRunning || next == EffectUnknown
	default: // COMMITTED is terminal, and must stay that way
		return false
	}
}

func (s EffectStatus) Valid() bool {
	switch s {
	case EffectPending, EffectRunning, EffectCommitted, EffectFailed, EffectUnknown:
		return true
	default:
		return false
	}
}

// EffectClass declares what can be done about an ambiguous outcome. It is a
// property of the external service, not of the runtime.
type EffectClass string

const (
	// ClassNone is a pure read with no external consequence. These bypass the
	// ledger entirely — a read that happens twice costs latency, not
	// correctness, and taxing every call with a ledger write would be paying
	// for a guarantee that is not needed.
	ClassNone EffectClass = "NONE"

	// ClassIdempotentByKey means the provider honours the idempotency key and
	// will not act twice on it. An UNKNOWN is resolved by re-sending.
	ClassIdempotentByKey EffectClass = "IDEMPOTENT_BY_KEY"

	// ClassQueryable means the provider can be asked what happened. An UNKNOWN
	// is resolved by looking it up.
	ClassQueryable EffectClass = "QUERYABLE"

	// ClassUnreconcilable means the provider neither deduplicates nor answers
	// questions. An UNKNOWN cannot be resolved by any runtime, so it is
	// escalated to a human and never guessed at.
	ClassUnreconcilable EffectClass = "UNRECONCILABLE"
)

// HasConsequence reports whether acting twice could be harmful.
func (c EffectClass) HasConsequence() bool { return c != ClassNone }

// CanAutoResolve reports whether an UNKNOWN of this class can be settled
// without a human.
func (c EffectClass) CanAutoResolve() bool {
	return c == ClassIdempotentByKey || c == ClassQueryable
}

func (c EffectClass) Valid() bool {
	switch c {
	case ClassNone, ClassIdempotentByKey, ClassQueryable, ClassUnreconcilable:
		return true
	default:
		return false
	}
}

// Effect is the ledger row for one external action.
type Effect struct {
	ID          EffectID
	TaskID      TaskID
	RunID       RunID
	Type        string // the tool that performs it
	Class       EffectClass
	Key         IdempotencyKey
	Status      EffectStatus
	Request     json.RawMessage
	Response    json.RawMessage
	ExternalRef string // the provider's own identifier, once known
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CommittedAt *time.Time
}

// EffectOutcome carries the detail of an effect transition.
type EffectOutcome struct {
	Response    json.RawMessage
	ExternalRef string
	Error       string
}

// --- failure classification -----------------------------------------------

// errNotExecuted marks an error as proof that nothing reached the provider.
var errNotExecuted = errors.New("veya: external action was not executed")

// NotExecuted wraps an error to assert that the external action definitely did
// not happen — the request was never transmitted.
//
// This is a claim, not a hint. It moves the effect to FAILED, which permits a
// clean retry, so a tool that wraps an ambiguous error in it has defeated the
// protection this package exists to provide. Use it only where the failure is
// provably local: a payload that would not serialize, a required argument that
// was missing, a connection that was refused before anything was sent.
func NotExecuted(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errNotExecuted, err)
}

// IsNotExecuted reports whether err carries that assertion.
func IsNotExecuted(err error) bool { return errors.Is(err, errNotExecuted) }

// ClassifyFailure decides what a failed tool call proves.
//
// The default is EffectUnknown, and that is the important decision in this
// file. An error returned after a request may have been transmitted is not
// evidence that it was not transmitted: the response could have been lost, the
// connection could have dropped after the provider committed, the deadline
// could have passed while the work succeeded.
//
// Defaulting to FAILED would be convenient and would occasionally send a
// second refund. Defaulting to UNKNOWN is sometimes pessimistic — a reconcile
// for what was really a validation error — and never wrong. Tools that can
// prove otherwise say so with NotExecuted.
func ClassifyFailure(err error) EffectStatus {
	if err == nil {
		return EffectCommitted
	}
	if IsNotExecuted(err) {
		return EffectFailed
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// The textbook ambiguous case: the request went out and the answer
		// never came back.
		return EffectUnknown
	}
	return EffectUnknown
}

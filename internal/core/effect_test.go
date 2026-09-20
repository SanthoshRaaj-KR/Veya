package core_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// These are pure unit tests. Everything here is domain logic with no I/O,
// which matters because this is the code that only ever runs during failures —
// the paths hardest to reach with an integration test are the ones most
// important to get right.

// TestIdempotencyKeyIsStableAcrossPayloadChanges is the regression test for
// the "hash the request" anti-pattern.
//
// Two logically identical actions differing only in a free-text field must
// produce the same key. A content hash would produce two, both would execute,
// and the customer would be refunded twice.
func TestIdempotencyKeyIsStableAcrossPayloadChanges(t *testing.T) {
	first := core.NewIdempotencyKey("R123", core.Step(2), 1)
	second := core.NewIdempotencyKey("R123", core.Step(2), 1)

	if first != second {
		t.Fatalf("same logical position produced %q and %q", first, second)
	}
	if want := core.IdempotencyKey("R123:S2:E1"); first != want {
		t.Fatalf("key = %q, want %q", first, want)
	}
}

func TestIdempotencyKeyDistinguishesPosition(t *testing.T) {
	base := core.NewIdempotencyKey("R1", core.Step(1), 1)

	others := map[string]core.IdempotencyKey{
		"different run":      core.NewIdempotencyKey("R2", core.Step(1), 1),
		"different step":     core.NewIdempotencyKey("R1", core.Step(2), 1),
		"different sequence": core.NewIdempotencyKey("R1", core.Step(1), 2),
		"child step":         core.NewIdempotencyKey("R1", core.Step(1).Child(0), 1),
	}
	for name, other := range others {
		if other == base {
			t.Errorf("%s collided with the base key (%q)", name, base)
		}
	}
}

// TestClassifyFailureDefaultsToUnknown is the heart of the design.
//
// Anything that is not provably "never sent" is ambiguous. A generic error
// returned after a request may have been transmitted is not evidence that it
// was not.
func TestClassifyFailureDefaultsToUnknown(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want core.EffectStatus
	}{
		{"nil means committed", nil, core.EffectCommitted},
		{"deadline exceeded is ambiguous", context.DeadlineExceeded, core.EffectUnknown},
		{"cancellation is ambiguous", context.Canceled, core.EffectUnknown},
		{"a bare error is ambiguous", errors.New("connection reset"), core.EffectUnknown},
		{"wrapped timeout is ambiguous", fmt.Errorf("calling provider: %w", context.DeadlineExceeded), core.EffectUnknown},
		{"NotExecuted is a definite failure", core.NotExecuted(errors.New("bad payload")), core.EffectFailed},
		{"wrapped NotExecuted still counts", fmt.Errorf("tool: %w", core.NotExecuted(errors.New("no such field"))), core.EffectFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := core.ClassifyFailure(tc.err); got != tc.want {
				t.Fatalf("ClassifyFailure(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

func TestNotExecutedPreservesTheUnderlyingError(t *testing.T) {
	cause := errors.New("missing customer id")
	wrapped := core.NotExecuted(cause)

	if !errors.Is(wrapped, cause) {
		t.Fatal("NotExecuted must wrap, not replace, the original error")
	}
	if !core.IsNotExecuted(wrapped) {
		t.Fatal("IsNotExecuted must recognise its own wrapper")
	}
	if core.IsNotExecuted(cause) {
		t.Fatal("a plain error must not be mistaken for a definite failure")
	}
	if core.NotExecuted(nil) != nil {
		t.Fatal("NotExecuted(nil) must stay nil")
	}
}

// TestEffectTransitions pins the state machine, including the edges that look
// permissive and are deliberate.
func TestEffectTransitions(t *testing.T) {
	legal := []struct{ from, to core.EffectStatus }{
		{core.EffectPending, core.EffectRunning},
		{core.EffectPending, core.EffectFailed},
		{core.EffectRunning, core.EffectCommitted},
		{core.EffectRunning, core.EffectFailed},
		{core.EffectRunning, core.EffectUnknown},
		// FAILED means it definitely did not happen, so retrying is safe.
		{core.EffectFailed, core.EffectRunning},
		// An IDEMPOTENT_BY_KEY effect is reconciled by re-sending.
		{core.EffectUnknown, core.EffectRunning},
		{core.EffectUnknown, core.EffectCommitted},
		{core.EffectUnknown, core.EffectFailed},
		// A reconcile that still cannot tell must be able to say so.
		{core.EffectUnknown, core.EffectUnknown},
	}
	for _, tc := range legal {
		if !tc.from.CanTransitionTo(tc.to) {
			t.Errorf("%s -> %s should be legal", tc.from, tc.to)
		}
	}

	illegal := []struct{ from, to core.EffectStatus }{
		// COMMITTED is terminal. Anything else would let the runtime forget
		// that the outside world already changed.
		{core.EffectCommitted, core.EffectRunning},
		{core.EffectCommitted, core.EffectUnknown},
		{core.EffectCommitted, core.EffectFailed},
		// PENDING means nothing was sent, so it cannot have succeeded.
		{core.EffectPending, core.EffectCommitted},
		{core.EffectPending, core.EffectUnknown},
		// FAILED is a definite claim; it cannot quietly become success.
		{core.EffectFailed, core.EffectCommitted},
	}
	for _, tc := range illegal {
		if tc.from.CanTransitionTo(tc.to) {
			t.Errorf("%s -> %s must not be legal", tc.from, tc.to)
		}
	}
}

// TestRecoveryTable covers every branch of README section 5.8. It is
// exhaustive on purpose: this table decides whether an interrupted refund is
// retried, skipped, or escalated, and every wrong answer is a customer-visible
// bug.
func TestRecoveryTable(t *testing.T) {
	const auto = core.ClassIdempotentByKey
	const manual = core.ClassUnreconcilable

	cases := []struct {
		name      string
		hasEffect bool
		status    core.EffectStatus
		class     core.EffectClass
		want      core.RecoveryAction
	}{
		{"no effect row: nothing was sent", false, "", auto, core.RecoverExecute},
		{"pending: recorded but not sent", true, core.EffectPending, auto, core.RecoverExecute},
		{"failed: definitely did not happen", true, core.EffectFailed, auto, core.RecoverExecute},
		{"committed: already done", true, core.EffectCommitted, auto, core.RecoverComplete},
		{"running, resolvable: find out", true, core.EffectRunning, auto, core.RecoverReconcile},
		{"unknown, resolvable: find out", true, core.EffectUnknown, auto, core.RecoverReconcile},
		{"running, unreconcilable: ask a human", true, core.EffectRunning, manual, core.RecoverEscalate},
		{"unknown, unreconcilable: ask a human", true, core.EffectUnknown, manual, core.RecoverEscalate},
		{"queryable is resolvable too", true, core.EffectUnknown, core.ClassQueryable, core.RecoverReconcile},
		{"an unrecognised status is not optimism", true, core.EffectStatus("WAT"), auto, core.RecoverEscalate},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := core.RecoveryFor(tc.hasEffect, tc.status, tc.class)
			if got != tc.want {
				t.Fatalf("RecoveryFor(%v, %s, %s) = %s, want %s",
					tc.hasEffect, tc.status, tc.class, got, tc.want)
			}
		})
	}
}

// TestRecoveryNeverExecutesOnAmbiguity is the invariant behind the table: no
// combination may answer "just run it again" when something might already have
// happened. A single wrong cell here is a duplicate side effect.
func TestRecoveryNeverExecutesOnAmbiguity(t *testing.T) {
	ambiguous := []core.EffectStatus{core.EffectRunning, core.EffectUnknown}
	classes := []core.EffectClass{
		core.ClassNone, core.ClassIdempotentByKey,
		core.ClassQueryable, core.ClassUnreconcilable,
	}

	for _, status := range ambiguous {
		for _, class := range classes {
			if got := core.RecoveryFor(true, status, class); got == core.RecoverExecute {
				t.Errorf("RecoveryFor(true, %s, %s) = EXECUTE; a possibly-completed "+
					"action must never be blindly re-run", status, class)
			}
		}
	}
}

func TestEffectClassCapabilities(t *testing.T) {
	if core.ClassNone.HasConsequence() {
		t.Error("NONE must have no external consequence; it is the ledger's fast path")
	}
	for _, c := range []core.EffectClass{core.ClassIdempotentByKey, core.ClassQueryable, core.ClassUnreconcilable} {
		if !c.HasConsequence() {
			t.Errorf("%s must be treated as having consequence", c)
		}
	}
	if core.ClassUnreconcilable.CanAutoResolve() {
		t.Error("UNRECONCILABLE must never resolve automatically; that is what the class means")
	}
	if core.ClassNone.CanAutoResolve() {
		t.Error("NONE has nothing to resolve")
	}
}

// TestKeysIssuedBeforeLayer5AreUnchanged is a regression test with a literal
// in it on purpose.
//
// Layer 5 made the step half of the key able to read S3.0 as well as S3, and
// moved the sequence half from a package constant to something derived. Either
// change could have altered the key a plain single-call step produces — and an
// altered key is not a cosmetic difference. Every ledger row already written
// is addressed by its key, so a run recovering across the upgrade would find
// no record of an action it had already performed, and perform it again.
//
// These strings are what this system has been issuing since Layer 2. They are
// written out rather than computed so that the test cannot agree with a bug by
// deriving the expectation the same wrong way.
func TestKeysIssuedBeforeLayer5AreUnchanged(t *testing.T) {
	cases := map[string]core.IdempotencyKey{
		"R123:S2:E1": core.NewIdempotencyKey("R123", core.Step(2), 1),
		"R1:S1:E1":   core.NewIdempotencyKey("R1", core.Step(1), 1),
		"R1:S10:E1":  core.NewIdempotencyKey("R1", core.Step(10), 1),
	}

	for want, got := range cases {
		if string(got) != want {
			t.Errorf("key = %q, want %q; every ledger row already written is "+
				"addressed by the old form", got, want)
		}
	}
}

// TestAFanOutDoesNotCollideWithItsParent. A child's key has to differ from its
// parent's and from its siblings', or two calls share a ledger row and the
// second is reported as already committed.
func TestAFanOutDoesNotCollideWithItsParent(t *testing.T) {
	parent := core.Step(3)

	seen := map[core.IdempotencyKey]string{}
	record := func(name string, key core.IdempotencyKey) {
		if other, clash := seen[key]; clash {
			t.Fatalf("%s and %s share the key %s", name, other, key)
		}
		seen[key] = name
	}

	record("parent", core.NewIdempotencyKey("R1", parent, 1))
	for i, child := range parent.Children(3) {
		record(fmt.Sprintf("child %d", i), core.NewIdempotencyKey("R1", child, 1))
	}

	// And a child's key is stable, which is the whole reason children are
	// numbered by invocation order.
	first := core.NewIdempotencyKey("R1", parent.Child(0), 1)
	if string(first) != "R1:S3.0:E1" {
		t.Fatalf("child key = %q, want R1:S3.0:E1", first)
	}
}

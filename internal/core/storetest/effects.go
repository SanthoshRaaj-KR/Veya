package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The effect ledger half of the contract.
//
// Every test here is about one question: can this store let a single logical
// external action happen twice? The answer has to be no on both adapters, for
// the same reasons, with the same errors — otherwise the claim that they are
// interchangeable is false precisely where it matters most.

func effectContracts() []contract {
	return []contract{
		{"ReserveIsExclusive", testReserveExclusive},
		{"ReserveUnderConcurrencyAdmitsOne", testReserveConcurrent},
		{"ReserveRejectsUnknownParents", testReserveUnknownParents},
		{"TransitionIsConditional", testEffectTransitionConditional},
		{"TransitionRejectsIllegalEdges", testEffectIllegalEdges},
		{"CommittedIsTerminal", testCommittedTerminal},
		{"CommitRecordsTheProvidersAnswer", testCommitRecordsResponse},
		{"UnresolvedFindsAmbiguousEffects", testUnresolvedEffects},
		{"ListEffectsIsTheAuditTrail", testListEffects},
		{"TheAuditTrailIsStableUnderSiblings", testAuditTrailStableUnderSiblings},
		{"MissingEffectReportsNotFound", testEffectNotFound},
	}
}

// testReserveExclusive is the mutual-exclusion primitive of the whole design.
//
// Note what is absent: no lease, no fencing token, no coordination of any
// kind. One unique constraint is what stops the second refund.
func testReserveExclusive(t *testing.T, s core.Store) {
	ctx := context.Background()
	runID, taskID := seedTask(t, s, "run-reserve", "task-reserve")
	key := core.NewIdempotencyKey(runID, core.Step(1), 1)

	first := newEffect("effect-1", key, runID, taskID)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error { return tx.ReserveEffect(ctx, first) })

	// A different worker, a different effect id, the same logical action.
	second := newEffect("effect-2", key, runID, taskID)
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ReserveEffect(ctx, second)
	})
	if !errors.Is(err, core.ErrEffectExists) {
		t.Fatalf("second reservation = %v, want ErrEffectExists", err)
	}

	// And the loser must be able to find out what actually happened, because
	// "already reserved" is an instruction to go and read, not to give up.
	got, err := s.GetEffect(ctx, key)
	if err != nil {
		t.Fatalf("GetEffect after conflict: %v", err)
	}
	if got.ID != first.ID {
		t.Fatalf("stored effect id = %s, want the first reservation %s", got.ID, first.ID)
	}
	if got.Status != core.EffectPending {
		t.Fatalf("status = %s, want PENDING", got.Status)
	}
}

// testReserveConcurrent runs the same reservation from many goroutines at
// once. Exactly one may win, and the rest must lose in the one way callers
// know how to handle.
func testReserveConcurrent(t *testing.T, s core.Store) {
	runID, taskID := seedTask(t, s, "run-race", "task-race")
	key := core.NewIdempotencyKey(runID, core.Step(1), 1)

	const attempts = 24
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		won        int
		lost       int
		unexpected []error
	)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := newEffect(core.EffectID(idFor(i)), key, runID, taskID)
			err := s.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
				return tx.ReserveEffect(ctx, e)
			})

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, core.ErrEffectExists):
				lost++
			default:
				unexpected = append(unexpected, err)
			}
		}(i)
	}
	wg.Wait()

	if len(unexpected) > 0 {
		t.Fatalf("unexpected errors racing to reserve: %v", unexpected)
	}
	if won != 1 {
		t.Fatalf("%d goroutines reserved the same key; exactly 1 may", won)
	}
	if lost != attempts-1 {
		t.Fatalf("%d losers, want %d", lost, attempts-1)
	}
}

func testReserveUnknownParents(t *testing.T, s core.Store) {
	ctx := context.Background()
	runID, taskID := seedTask(t, s, "run-parents", "task-parents")

	orphanRun := newEffect("e-orphan-run", "K:orphan:E1", "no-such-run", taskID)
	if err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ReserveEffect(ctx, orphanRun)
	}); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("effect on a missing run = %v, want ErrNotFound", err)
	}

	orphanTask := newEffect("e-orphan-task", "K:orphan:E2", runID, "no-such-task")
	if err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ReserveEffect(ctx, orphanTask)
	}); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("effect on a missing task = %v, want ErrNotFound", err)
	}
}

// testEffectTransitionConditional covers the case where a reconciler and a
// worker both act on one effect: only the one whose view of the current status
// is accurate may write.
func testEffectTransitionConditional(t *testing.T, s core.Store) {
	ctx := context.Background()
	key := seedEffect(t, s, "run-cond", "task-cond", core.ClassIdempotentByKey)

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{})
	})

	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{})
	})
	if !errors.Is(err, core.ErrConflict) {
		t.Fatalf("stale transition = %v, want ErrConflict", err)
	}
}

func testEffectIllegalEdges(t *testing.T, s core.Store) {
	ctx := context.Background()
	key := seedEffect(t, s, "run-edges", "task-edges", core.ClassQueryable)

	// PENDING means nothing was sent, so it cannot have succeeded.
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectCommitted, core.EffectOutcome{})
	})
	if !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("PENDING -> COMMITTED = %v, want ErrInvalidTransition", err)
	}

	// Nor can it be ambiguous: ambiguity requires having sent something.
	err = s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectUnknown, core.EffectOutcome{})
	})
	if !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("PENDING -> UNKNOWN = %v, want ErrInvalidTransition", err)
	}
}

// testCommittedTerminal guards the one edge that must never exist. If a
// committed effect could move back, the runtime could forget that the outside
// world already changed, and then do it again.
func testCommittedTerminal(t *testing.T, s core.Store) {
	ctx := context.Background()
	key := seedEffect(t, s, "run-terminal", "task-terminal", core.ClassIdempotentByKey)

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectRunning, core.EffectCommitted,
			core.EffectOutcome{ExternalRef: "pay_1"})
	})

	for _, to := range []core.EffectStatus{core.EffectRunning, core.EffectUnknown, core.EffectFailed} {
		err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
			return tx.TransitionEffect(ctx, key, core.EffectCommitted, to, core.EffectOutcome{})
		})
		if !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("COMMITTED -> %s = %v, want ErrInvalidTransition", to, err)
		}
	}
}

func testCommitRecordsResponse(t *testing.T, s core.Store) {
	ctx := context.Background()
	key := seedEffect(t, s, "run-commit", "task-commit", core.ClassIdempotentByKey)

	response := json.RawMessage(`{"reference":"pay_98374"}`)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, key, core.EffectRunning, core.EffectCommitted, core.EffectOutcome{
			Response:    response,
			ExternalRef: "pay_98374",
		})
	})

	got, err := s.GetEffect(ctx, key)
	if err != nil {
		t.Fatalf("GetEffect: %v", err)
	}
	if got.Status != core.EffectCommitted {
		t.Fatalf("status = %s, want COMMITTED", got.Status)
	}
	if got.ExternalRef != "pay_98374" {
		t.Fatalf("external ref = %q, want pay_98374", got.ExternalRef)
	}
	// The recorded answer is what a duplicate delivery gets returned instead
	// of calling the provider again, so it has to survive the round trip.
	assertJSONEqual(t, "response", got.Response, response)
	if got.CommittedAt == nil {
		t.Fatal("a committed effect must record when the world changed")
	}
}

// testUnresolvedEffects drives reconciliation. It must find exactly the two
// statuses that mean "we do not know", and must not return effects so fresh
// that they are probably still in flight.
func testUnresolvedEffects(t *testing.T, s core.Store) {
	ctx := context.Background()
	runID, taskID := seedTask(t, s, "run-unresolved", "task-unresolved")

	mk := func(n int, target core.EffectStatus) core.IdempotencyKey {
		key := core.NewIdempotencyKey(runID, core.Step(n), 1)
		mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
			return tx.ReserveEffect(ctx, newEffect(core.EffectID(idFor(n)), key, runID, taskID))
		})
		if target == core.EffectPending {
			return key
		}
		mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
			return tx.TransitionEffect(ctx, key, core.EffectPending, core.EffectRunning, core.EffectOutcome{})
		})
		if target != core.EffectRunning {
			mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
				return tx.TransitionEffect(ctx, key, core.EffectRunning, target, core.EffectOutcome{})
			})
		}
		return key
	}

	running := mk(1, core.EffectRunning)
	unknown := mk(2, core.EffectUnknown)
	mk(3, core.EffectCommitted)
	mk(4, core.EffectFailed)
	mk(5, core.EffectPending)

	// Far future cutoff: everything written so far counts as stale.
	got, err := s.UnresolvedEffects(ctx, time.Now().Add(24*time.Hour), 10)
	if err != nil {
		t.Fatalf("UnresolvedEffects: %v", err)
	}
	found := map[core.IdempotencyKey]bool{}
	for _, e := range got {
		found[e.Key] = true
	}
	if !found[running] || !found[unknown] {
		t.Fatalf("unresolved = %v; RUNNING and UNKNOWN must both be found", keysOf(got))
	}
	if len(got) != 2 {
		t.Fatalf("unresolved = %v, want exactly the RUNNING and UNKNOWN effects", keysOf(got))
	}

	// Distant past cutoff: nothing is stale enough to reconcile yet.
	got, err = s.UnresolvedEffects(ctx, time.Unix(0, 0), 10)
	if err != nil {
		t.Fatalf("UnresolvedEffects: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("unresolved = %v; effects newer than the cutoff must be left alone", keysOf(got))
	}
}

func testListEffects(t *testing.T, s core.Store) {
	ctx := context.Background()
	runID, taskID := seedTask(t, s, "run-audit", "task-audit")

	var want []core.IdempotencyKey
	for i := 1; i <= 3; i++ {
		key := core.NewIdempotencyKey(runID, core.Step(i), 1)
		want = append(want, key)
		mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
			return tx.ReserveEffect(ctx, newEffect(core.EffectID(idFor(i)), key, runID, taskID))
		})
	}

	got, err := s.ListEffects(ctx, runID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d effects, want %d", len(got), len(want))
	}
	for i, e := range got {
		if e.Key != want[i] {
			t.Fatalf("effect %d = %s, want %s (audit order is creation order)", i, e.Key, want[i])
		}
	}
}

// testAuditTrailStableUnderSiblings. Before fan-out, a run's effects were
// created one at a time and creation order was a total order. Children of one
// decision are reserved together and can share a timestamp to whatever
// resolution the clock has, so creation order alone stops deciding.
//
// The audit trail is what answers "what did this run actually do to the
// outside world?". An answer whose order changes between two reads of
// unchanged data is not an audit trail, it is a bag -- and a reader diffing
// two exports would see churn that is not there.
func testAuditTrailStableUnderSiblings(t *testing.T, s core.Store) {
	ctx := context.Background()
	runID, taskID := seedTask(t, s, "run-siblings", "task-siblings")

	// Reserved in one transaction, so nothing distinguishes them but the key.
	parent := core.Step(4)
	children := parent.Children(5)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		// Inserted back to front, so an adapter that happened to return
		// insertion order would disagree with the expectation below.
		for i := len(children) - 1; i >= 0; i-- {
			key := core.NewIdempotencyKey(runID, children[i], 1)
			if err := tx.ReserveEffect(ctx, newEffect(
				core.EffectID(idFor(i)), key, runID, taskID)); err != nil {
				return err
			}
		}
		return nil
	})

	var first []core.IdempotencyKey
	for read := 0; read < 3; read++ {
		got, err := s.ListEffects(ctx, runID)
		if err != nil {
			t.Fatalf("ListEffects: %v", err)
		}
		if len(got) != len(children) {
			t.Fatalf("got %d effects, want %d", len(got), len(children))
		}

		keys := keysOf(got)
		if read == 0 {
			first = keys
			continue
		}
		if fmt.Sprint(keys) != fmt.Sprint(first) {
			t.Fatalf("read %d returned %v, first read returned %v; "+
				"the audit trail must not reorder between reads", read, keys, first)
		}
	}

	// And the order obeys the rule a reader can rely on: creation time, then
	// key. Asserting the rule rather than a literal sequence keeps this
	// honest whatever the clock's resolution is -- on a real one, siblings
	// reserved in a single transaction routinely share a timestamp, and the
	// key is then the only thing standing between an audit trail and a bag.
	rows, err := s.ListEffects(ctx, runID)
	if err != nil {
		t.Fatalf("ListEffects: %v", err)
	}
	for i := 1; i < len(rows); i++ {
		prev, cur := rows[i-1], rows[i]
		switch {
		case cur.CreatedAt.Before(prev.CreatedAt):
			t.Fatalf("effect %d was created before effect %d but comes after it", i, i-1)
		case cur.CreatedAt.Equal(prev.CreatedAt) && cur.Key < prev.Key:
			t.Fatalf("effects %d and %d share a timestamp and are not in key order: "+
				"%s then %s", i-1, i, prev.Key, cur.Key)
		}
	}
}

func testEffectNotFound(t *testing.T, s core.Store) {
	ctx := context.Background()
	if _, err := s.GetEffect(ctx, "R0:S0:E0"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("GetEffect(absent) = %v, want ErrNotFound", err)
	}
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionEffect(ctx, "R0:S0:E0", core.EffectPending, core.EffectRunning, core.EffectOutcome{})
	})
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("TransitionEffect(absent) = %v, want ErrNotFound", err)
	}
}

// --- helpers --------------------------------------------------------------

func newEffect(id core.EffectID, key core.IdempotencyKey, runID core.RunID, taskID core.TaskID) core.Effect {
	return core.Effect{
		ID:      id,
		TaskID:  taskID,
		RunID:   runID,
		Type:    "send_email",
		Class:   core.ClassIdempotentByKey,
		Key:     key,
		Status:  core.EffectPending,
		Request: json.RawMessage(`{"to":"ana@example.com"}`),
	}
}

// seedTask creates a run with one task and returns both identifiers.
func seedTask(t *testing.T, s core.Store, runID core.RunID, taskID core.TaskID) (core.RunID, core.TaskID) {
	t.Helper()
	mustCreateRun(t, s, runID)
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.CreateTask(ctx, newTask(taskID, runID, core.Step(1)))
	})
	return runID, taskID
}

// seedEffect creates a run, a task, and one PENDING effect on it.
func seedEffect(t *testing.T, s core.Store, runID core.RunID, taskID core.TaskID, class core.EffectClass) core.IdempotencyKey {
	t.Helper()
	seedTask(t, s, runID, taskID)

	key := core.NewIdempotencyKey(runID, core.Step(1), 1)
	e := newEffect("effect-seed", key, runID, taskID)
	e.Class = class
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error { return tx.ReserveEffect(ctx, e) })
	return key
}

func keysOf(es []core.Effect) []core.IdempotencyKey {
	out := make([]core.IdempotencyKey, len(es))
	for i, e := range es {
		out[i] = e.Key
	}
	return out
}

func idFor(n int) string { return fmt.Sprintf("effect-%d", n) }

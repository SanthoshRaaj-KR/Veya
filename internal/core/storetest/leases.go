package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The ownership half of the contract.
//
// The tests are arranged around one distinction, because getting it wrong is
// how systems built on leases corrupt themselves: the lease decides who
// *should* work and is allowed to be wrong, while the fencing token decides
// whose writes *still count* and is not.

func leaseContracts() []contract {
	return []contract{
		{"LeaseIsExclusiveWhileLive", testLeaseExclusive},
		{"ExpiredLeaseCanBeTakenOver", testLeaseTakeover},
		{"FencingTokenOnlyEverIncreases", testTokenMonotonic},
		{"StaleWorkerCannotHeartbeat", testStaleHeartbeat},
		{"StaleWorkerCannotRelease", testStaleRelease},
		{"ReleaseMakesTaskReclaimableButKeepsTheToken", testReleaseKeepsToken},
		{"ExpiredLeasesExcludeFinishedTasks", testExpiredLeasesFiltering},
		{"TwoWorkersCanBelieveTheyHoldOneLease", testClockSkewIsSurvivable},
		{"MissingLeaseReportsNotFound", testLeaseNotFound},
	}
}

func testLeaseExclusive(t *testing.T, s core.Store) {
	ctx := context.Background()
	_, taskID := seedTask(t, s, "run-lease", "task-lease")
	now := time.Now()

	mustRegisterWorker(t, s, "worker-a")
	mustRegisterWorker(t, s, "worker-b")

	lease := mustAcquire(t, s, taskID, "worker-a", now, now.Add(time.Minute))
	if lease.Token != 1 {
		t.Fatalf("first token = %d, want 1", lease.Token)
	}

	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		_, err := tx.AcquireLease(ctx, taskID, "worker-b", now, now.Add(time.Minute))
		return err
	})
	if !errors.Is(err, core.ErrLeaseHeld) {
		t.Fatalf("stealing a live lease = %v, want ErrLeaseHeld", err)
	}
}

func testLeaseTakeover(t *testing.T, s core.Store) {
	_, taskID := seedTask(t, s, "run-takeover", "task-takeover")
	start := time.Now()

	mustRegisterWorker(t, s, "worker-a")
	mustRegisterWorker(t, s, "worker-b")

	first := mustAcquire(t, s, taskID, "worker-a", start, start.Add(10*time.Second))

	// Worker A goes silent and its lease lapses.
	later := start.Add(11 * time.Second)
	second := mustAcquire(t, s, taskID, "worker-b", later, later.Add(time.Minute))

	if second.WorkerID != "worker-b" {
		t.Fatalf("owner = %s, want worker-b", second.WorkerID)
	}
	if second.Token <= first.Token {
		t.Fatalf("token went %d -> %d; a takeover must issue a strictly higher one",
			first.Token, second.Token)
	}
}

// testTokenMonotonic is the property the whole fencing mechanism rests on. A
// token that could repeat would stop distinguishing a new owner from an old
// one, and a zombie worker's write would be indistinguishable from a live
// worker's.
func testTokenMonotonic(t *testing.T, s core.Store) {
	_, taskID := seedTask(t, s, "run-monotonic", "task-monotonic")
	mustRegisterWorker(t, s, "worker-a")

	at := time.Now()
	var previous core.FencingToken
	for i := 0; i < 5; i++ {
		lease := mustAcquire(t, s, taskID, "worker-a", at, at.Add(time.Second))
		if lease.Token <= previous {
			t.Fatalf("acquisition %d issued token %d after %d; tokens must strictly increase",
				i, lease.Token, previous)
		}
		previous = lease.Token
		at = at.Add(2 * time.Second) // let it lapse before reacquiring
	}
}

// testStaleHeartbeat covers the subtle case. If a heartbeat were not fenced, a
// worker that lost ownership while frozen could renew a lease it no longer
// holds, and the runtime would believe live work belonged to a dead process —
// worse than the worker simply dying, because the task would never be
// reclaimed.
func testStaleHeartbeat(t *testing.T, s core.Store) {
	ctx := context.Background()
	_, taskID := seedTask(t, s, "run-heartbeat", "task-heartbeat")
	mustRegisterWorker(t, s, "worker-a")
	mustRegisterWorker(t, s, "worker-b")

	start := time.Now()
	stale := mustAcquire(t, s, taskID, "worker-a", start, start.Add(time.Second))

	later := start.Add(2 * time.Second)
	fresh := mustAcquire(t, s, taskID, "worker-b", later, later.Add(time.Minute))

	// Worker A wakes up and tries to renew with its old token.
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ExtendLease(ctx, taskID, stale.Token, later.Add(time.Hour))
	})
	if !errors.Is(err, core.ErrFenced) {
		t.Fatalf("stale heartbeat = %v, want ErrFenced", err)
	}

	// The live owner can still renew.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.ExtendLease(ctx, taskID, fresh.Token, later.Add(time.Hour))
	})
}

func testStaleRelease(t *testing.T, s core.Store) {
	ctx := context.Background()
	_, taskID := seedTask(t, s, "run-release", "task-release")
	mustRegisterWorker(t, s, "worker-a")
	mustRegisterWorker(t, s, "worker-b")

	start := time.Now()
	stale := mustAcquire(t, s, taskID, "worker-a", start, start.Add(time.Second))
	later := start.Add(2 * time.Second)
	mustAcquire(t, s, taskID, "worker-b", later, later.Add(time.Minute))

	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ReleaseLease(ctx, taskID, stale.Token, later)
	})
	if !errors.Is(err, core.ErrFenced) {
		t.Fatalf("stale release = %v, want ErrFenced; a zombie must not be able "+
			"to hand away work it no longer owns", err)
	}
}

func testReleaseKeepsToken(t *testing.T, s core.Store) {
	_, taskID := seedTask(t, s, "run-keep", "task-keep")
	mustRegisterWorker(t, s, "worker-a")
	mustRegisterWorker(t, s, "worker-b")

	now := time.Now()
	first := mustAcquire(t, s, taskID, "worker-a", now, now.Add(time.Hour))

	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.ReleaseLease(ctx, taskID, first.Token, now)
	})

	// Released means immediately reclaimable, even though the original expiry
	// was an hour away.
	second := mustAcquire(t, s, taskID, "worker-b", now, now.Add(time.Hour))
	if second.Token <= first.Token {
		t.Fatalf("token after release went %d -> %d; releasing must not reset the counter",
			first.Token, second.Token)
	}
}

func testExpiredLeasesFiltering(t *testing.T, s core.Store) {
	ctx := context.Background()
	runID := core.RunID("run-reap")
	mustCreateRun(t, s, runID)
	mustRegisterWorker(t, s, "worker-a")

	open := core.TaskID("task-open")
	done := core.TaskID("task-done")
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateTask(ctx, newTask(open, runID, core.Step(1))); err != nil {
			return err
		}
		return tx.CreateTask(ctx, newTask(done, runID, core.Step(2)))
	})

	start := time.Now()
	mustAcquire(t, s, open, "worker-a", start, start.Add(time.Second))
	mustAcquire(t, s, done, "worker-a", start, start.Add(time.Second))

	// Finish one of them, leaving behind a released-looking lease.
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionTask(ctx, done, core.TaskPending, core.TaskRunning, core.TaskOutcome{})
	})
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.TransitionTask(ctx, done, core.TaskRunning, core.TaskCompleted, core.TaskOutcome{})
	})

	expired, err := s.ExpiredLeases(ctx, start.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("ExpiredLeases: %v", err)
	}
	if len(expired) != 1 || expired[0].TaskID != open {
		var got []core.TaskID
		for _, l := range expired {
			got = append(got, l.TaskID)
		}
		t.Fatalf("expired leases = %v, want only %s: a finished task's lease "+
			"looks expired forever and must not be reaped repeatedly", got, open)
	}
}

// testClockSkewIsSurvivable states the design's central claim about leases.
//
// Two workers really can hold what each believes is a valid lease — that is
// not a bug to fix but a property of distributed systems. What must hold is
// that only one of them can still write, and the fencing token, not the lease,
// is what makes that true.
func testClockSkewIsSurvivable(t *testing.T, s core.Store) {
	ctx := context.Background()
	_, taskID := seedTask(t, s, "run-skew", "task-skew")
	mustRegisterWorker(t, s, "frozen-worker")
	mustRegisterWorker(t, s, "replacement")

	start := time.Now()

	// Worker A acquires, then freezes. Its own clock never advances, so from
	// its point of view the lease is still perfectly valid.
	frozen := mustAcquire(t, s, taskID, "frozen-worker", start, start.Add(5*time.Second))

	// The world moves on and the lease lapses.
	later := start.Add(6 * time.Second)
	replacement := mustAcquire(t, s, taskID, "replacement", later, later.Add(time.Minute))

	// Both workers now sincerely believe they own this task. The lease alone
	// cannot tell them apart.
	if frozen.WorkerID == replacement.WorkerID {
		t.Fatal("test setup is wrong: the two owners must differ")
	}

	// The token can. Every mutation the frozen worker attempts is rejected.
	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ExtendLease(ctx, taskID, frozen.Token, later.Add(time.Hour))
	})
	if !errors.Is(err, core.ErrFenced) {
		t.Fatalf("frozen worker's heartbeat = %v, want ErrFenced", err)
	}
	err = s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ReleaseLease(ctx, taskID, frozen.Token, later)
	})
	if !errors.Is(err, core.ErrFenced) {
		t.Fatalf("frozen worker's release = %v, want ErrFenced", err)
	}

	// And the replacement's ownership is untouched by any of it.
	var current core.Lease
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		var err error
		current, err = tx.GetLease(ctx, taskID)
		return err
	})
	if current.WorkerID != "replacement" || current.Token != replacement.Token {
		t.Fatalf("ownership = %s at token %d, want replacement at %d",
			current.WorkerID, current.Token, replacement.Token)
	}
}

func testLeaseNotFound(t *testing.T, s core.Store) {
	ctx := context.Background()
	_, taskID := seedTask(t, s, "run-noleases", "task-noleases")

	err := s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		_, err := tx.GetLease(ctx, taskID)
		return err
	})
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("GetLease on an unclaimed task = %v, want ErrNotFound", err)
	}

	err = s.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		return tx.ExtendLease(ctx, taskID, 1, time.Now().Add(time.Minute))
	})
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("ExtendLease on an unclaimed task = %v, want ErrNotFound", err)
	}
}

// --- helpers --------------------------------------------------------------

func mustRegisterWorker(t *testing.T, s core.Store, id string) {
	t.Helper()
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		return tx.RegisterWorker(ctx, core.Worker{
			ID:     id,
			Type:   "suite",
			Status: core.WorkerActive,
		})
	})
}

func mustAcquire(t *testing.T, s core.Store, taskID core.TaskID, workerID string, now, expiresAt time.Time) core.Lease {
	t.Helper()

	var lease core.Lease
	mustTx(t, s, func(ctx context.Context, tx core.Tx) error {
		var err error
		lease, err = tx.AcquireLease(ctx, taskID, workerID, now, expiresAt)
		return err
	})
	return lease
}

package core

import "time"

// FencingToken is a per-task counter that only ever increases.
//
// It answers a different question from the lease that carries it. The lease
// says who *should* be working; the token says whose writes *still count*.
// Every reassignment issues a strictly higher one, and every mutating call
// presents it, so a worker that lost ownership while frozen is rejected when
// it wakes up and tries to report.
type FencingToken int64

// Lease is temporary ownership of a task.
//
// # A lease is not a lock
//
// It cannot be. If ownership never expired, one frozen worker would stall a
// task forever; because it does expire, a worker can be unreachable, lose its
// lease, and wake up still believing it holds one. For a moment two workers
// sincerely believe they own the same task, and no amount of care in this
// package prevents that — it is a property of distributed systems, not a bug
// to fix.
//
// So the lease is treated as what it is: a scheduling hint, which is a
// liveness mechanism and allowed to be occasionally wrong. Correctness comes
// from two other places that are not allowed to be wrong — the fencing token
// rejects a stale worker's writes into the runtime, and the unique constraint
// on an effect's idempotency key prevents a duplicate call going out.
type Lease struct {
	TaskID      TaskID
	WorkerID    string
	Token       FencingToken
	AcquiredAt  time.Time
	ExpiresAt   time.Time
	HeartbeatAt time.Time
}

// Expired reports whether the lease has lapsed as of now.
func (l Lease) Expired(now time.Time) bool { return !now.Before(l.ExpiresAt) }

// WorkerStatus tracks a worker's liveness. Values match the worker_status enum
// in migration 0001.
type WorkerStatus string

const (
	WorkerActive   WorkerStatus = "ACTIVE"
	WorkerDraining WorkerStatus = "DRAINING"
	WorkerDead     WorkerStatus = "DEAD"
)

// Worker is a process that executes tasks.
//
// Layer 2 registers workers only because leases reference them, and a lease
// must point at something real. Capability-based routing and liveness
// heartbeating arrive in Layer 3, when workers can live in another process and
// their absence starts to matter.
type Worker struct {
	ID            string
	Type          string
	Status        WorkerStatus
	LastHeartbeat *time.Time
	RegisteredAt  time.Time
}

// RecoveryAction is what to do with a task whose owner has gone away.
type RecoveryAction string

const (
	// RecoverExecute: nothing was sent. Run it cleanly.
	RecoverExecute RecoveryAction = "EXECUTE"
	// RecoverReconcile: something may have been sent. Find out before acting.
	RecoverReconcile RecoveryAction = "RECONCILE"
	// RecoverComplete: it already happened. Use the recorded result.
	RecoverComplete RecoveryAction = "COMPLETE"
	// RecoverEscalate: it is ambiguous and cannot be resolved automatically.
	RecoverEscalate RecoveryAction = "ESCALATE"
)

// RecoveryFor is README section 5.8's decision table as a pure function.
//
// Given what the ledger knows about an interrupted task, there is exactly one
// safe action, and none of them is a guess:
//
//	no effect row   nothing was ever sent            EXECUTE
//	PENDING         recorded but never sent          EXECUTE
//	RUNNING         may or may not have happened     RECONCILE
//	UNKNOWN         already known to be ambiguous    RECONCILE
//	COMMITTED       already done                     COMPLETE
//	FAILED          definitely did not happen        EXECUTE
//
// A class that cannot be resolved automatically turns RECONCILE into ESCALATE,
// because the honest answer for an UNRECONCILABLE tool is that a human has to
// look. Being pure makes every branch testable without a database, which
// matters for a table that is only exercised during failures.
func RecoveryFor(hasEffect bool, status EffectStatus, class EffectClass) RecoveryAction {
	if !hasEffect {
		return RecoverExecute
	}

	switch status {
	case EffectPending, EffectFailed:
		return RecoverExecute
	case EffectCommitted:
		return RecoverComplete
	case EffectRunning, EffectUnknown:
		if class.CanAutoResolve() {
			return RecoverReconcile
		}
		return RecoverEscalate
	default:
		// An unrecognised status is not something to be optimistic about.
		return RecoverEscalate
	}
}

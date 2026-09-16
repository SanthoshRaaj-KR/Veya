package core

import "errors"

// Sentinel errors. Every adapter translates its driver's errors into these,
// and every caller compares with errors.Is. No error string is matched
// anywhere in the codebase: a Postgres error code and an in-memory map miss
// have to be indistinguishable upstream, or the contract test suite is lying
// about the two adapters being interchangeable.
var (
	// ErrNotFound is returned when an identified record does not exist.
	ErrNotFound = errors.New("veya: not found")

	// ErrConflict is returned when a compare-and-swap loses: the record
	// changed between read and write. The caller re-reads and retries; it is
	// an expected outcome under concurrency, not a fault.
	ErrConflict = errors.New("veya: version conflict")

	// ErrTaskExists is returned when a task already exists for a
	// (run_id, step_id) pair, enforced by a unique constraint.
	//
	// Callers treat this as success. Task creation is idempotent by design:
	// re-deriving the same step of the same run must not produce a second
	// task, and the database is what guarantees it rather than a prior read.
	ErrTaskExists = errors.New("veya: task already exists for step")

	// ErrSeqConflict is returned when an event append collides with an
	// existing (run_id, seq). The append was conditional and lost; the caller
	// re-reads the sequence and retries.
	ErrSeqConflict = errors.New("veya: event sequence already used")

	// ErrInvalidTransition is returned when a status change is not permitted
	// by the domain state machine. It means the caller's view is stale or its
	// logic is wrong, and it is never retried blindly.
	ErrInvalidTransition = errors.New("veya: invalid status transition")

	// ErrUnknownPayloadVersion is returned when reading an event whose
	// envelope version this build does not understand.
	ErrUnknownPayloadVersion = errors.New("veya: unknown event payload version")

	// ErrToolNotFound is returned when a task names a tool no worker has
	// registered.
	ErrToolNotFound = errors.New("veya: tool not registered")

	// ErrDispatcherClosed is returned by Claim once a dispatcher is shut down
	// and no further deliveries will arrive.
	ErrDispatcherClosed = errors.New("veya: dispatcher closed")

	// ErrEffectExists is returned when an effect already exists for an
	// idempotency key, enforced by a unique constraint.
	//
	// This is the mutual-exclusion primitive of the whole design, and callers
	// must not treat it as a failure. It means "this action already has a
	// record" — the correct response is to load that record and branch on its
	// status, never to give up. Failing the task here would turn a safely
	// prevented duplicate into a stuck run.
	ErrEffectExists = errors.New("veya: effect already exists for idempotency key")

	// ErrFenced is returned when a worker presents a fencing token lower than
	// the one currently valid for a task.
	//
	// It means the worker lost ownership while it was away — almost always
	// because it froze or was partitioned long enough for its lease to expire
	// and be taken. The write is rejected and the worker must stop; someone
	// else owns this work now.
	ErrFenced = errors.New("veya: stale fencing token")

	// ErrLeaseHeld is returned when a lease cannot be acquired because a live
	// one already exists.
	ErrLeaseHeld = errors.New("veya: lease is held by another worker")

	// ErrNeedsReconciliation is returned when an effect's outcome is unknown
	// and must be resolved before the task can proceed.
	ErrNeedsReconciliation = errors.New("veya: effect outcome is unknown")

	// ErrEscalated is returned when an outcome cannot be resolved by any
	// mechanism and a human has to decide.
	ErrEscalated = errors.New("veya: effect requires human resolution")
)

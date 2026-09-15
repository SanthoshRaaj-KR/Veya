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
)

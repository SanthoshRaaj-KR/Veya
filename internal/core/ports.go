package core

import (
	"context"
	"time"
)

// Store is the authoritative record of what is true and what happened.
//
// Reads are direct. Every write goes through RunInTx, because the guarantees
// this system makes are guarantees about what commits together: a task, the
// event recording it, and (from Layer 3) the intent to deliver it are one
// transaction or they are a dual-write bug.
type Store interface {
	// RunInTx runs fn inside a transaction, committing if it returns nil and
	// rolling back otherwise. fn may be called more than once if the adapter
	// retries a serialization failure, so it must not have side effects
	// outside tx.
	RunInTx(ctx context.Context, fn func(context.Context, Tx) error) error

	GetRun(ctx context.Context, id RunID) (Run, error)
	GetTask(ctx context.Context, id TaskID) (Task, error)
	ListTasks(ctx context.Context, runID RunID) ([]Task, error)

	// History returns a run's events in sequence order.
	History(ctx context.Context, runID RunID) ([]Event, error)

	// PendingTasks returns dispatchable tasks, oldest first. It is how a
	// restarted process rediscovers work that was committed but never
	// delivered — the engine holds no authoritative state in memory.
	PendingTasks(ctx context.Context, limit int) ([]Task, error)

	// RunsAwaitingAdvance returns runs that are RUNNING with no task still in
	// flight, meaning the next decision is owed. A run started by one process
	// and advanced by another is found this way.
	RunsAwaitingAdvance(ctx context.Context, limit int) ([]RunID, error)

	// GetEffect reads one ledger row by its provider-facing key.
	GetEffect(ctx context.Context, key IdempotencyKey) (Effect, error)

	// ListEffects returns a run's effects, oldest first. This is the audit
	// trail that answers "what did this run actually do to the outside world?".
	ListEffects(ctx context.Context, runID RunID) ([]Effect, error)

	// UnresolvedEffects returns effects still RUNNING or UNKNOWN whose last
	// update predates `before`, oldest first.
	//
	// The staleness bound is what stops the reconciler racing live workers: an
	// effect marked RUNNING a moment ago is almost certainly a request in
	// flight, not an abandoned one, and asking the provider about it would be
	// both wasteful and misleading.
	UnresolvedEffects(ctx context.Context, before time.Time, limit int) ([]Effect, error)

	// PendingDeliveries returns unpublished outbox rows, oldest first. The
	// relay's input.
	//
	// A row here is a task that has been committed and not yet handed to the
	// dispatcher. The list is normally empty; a row that persists across sweeps
	// means the broker is refusing work.
	PendingDeliveries(ctx context.Context, limit int) ([]Delivery, error)

	// ExpiredLeases returns leases whose owner has gone silent, on tasks that
	// are still supposed to be worked on, oldest expiry first. The reaper's
	// input.
	//
	// Leases on finished tasks are excluded. A completed task's lease is
	// released and therefore looks expired forever; returning it would give
	// the reaper an ever-growing pile of work that is already done.
	ExpiredLeases(ctx context.Context, now time.Time, limit int) ([]Lease, error)

	Close() error
}

// Tx is the transactional write surface.
//
// It is an interface rather than a driver handle so that no concrete
// transaction type ever appears in a signature the engine can see. An adapter
// that needed to expose *sql.Tx here would be telling us the port is wrong.
type Tx interface {
	// GetTask reads a task inside the transaction. Needed so that a caller
	// which both inspects and mutates a task sees one consistent view rather
	// than reading before the transaction and acting on a stale answer.
	GetTask(ctx context.Context, id TaskID) (Task, error)

	CreateRun(ctx context.Context, r Run) error

	// AdvanceRun applies next only if the run is still at expectedVersion,
	// then increments the version. Returns ErrConflict if it is not.
	AdvanceRun(ctx context.Context, id RunID, expectedVersion int64, next RunState) error

	// CreateTask inserts a task. Returns ErrTaskExists if one already exists
	// for the (run, step) pair; callers treat that as success.
	CreateTask(ctx context.Context, t Task) error

	// TransitionTask moves a task from one status to another, applying only
	// if it is currently in from. Returns ErrConflict if it is not — which is
	// what makes claiming safe when the same task is delivered twice.
	TransitionTask(ctx context.Context, id TaskID, from, to TaskStatus, out TaskOutcome) error

	// NextSeq returns the sequence number an append should use next.
	NextSeq(ctx context.Context, runID RunID) (int64, error)

	// AppendEvent writes one event. Returns ErrSeqConflict if (run, seq) is
	// taken.
	AppendEvent(ctx context.Context, e Event) error

	// EnqueueDelivery records the intent to hand a task to a worker.
	//
	// It belongs in the same transaction as whatever made the task
	// dispatchable — its creation, a retry, a reclaim — because that is the
	// entire point. Publishing outside the transaction is the dual write this
	// closes; see outbox.go.
	//
	// Returns ErrNotFound if the task does not exist.
	EnqueueDelivery(ctx context.Context, taskID TaskID) error

	// MarkDelivered records that deliveries reached the dispatcher.
	//
	// It is deliberately after the publish, never before. The other ordering
	// would lose a delivery whenever the process died in between, and this
	// ordering only duplicates one — which the conditional claim absorbs.
	MarkDelivered(ctx context.Context, ids []DeliveryID) error

	// FailDelivery counts a failed publish and records why.
	//
	// The row stays pending: the relay does not give up on a delivery, because
	// a task nobody is told about is a stuck run and there is no attempt count
	// at which that becomes acceptable.
	FailDelivery(ctx context.Context, id DeliveryID, cause string) error

	// ReserveEffect inserts a ledger row, relying on the unique constraint on
	// the idempotency key.
	//
	// It must be one conditional insert, never a read followed by a write: the
	// gap between those two is exactly where a second worker slips through,
	// and the database constraint is the only thing that closes it. This is
	// the real mutual-exclusion primitive of the design — not the lease, which
	// can be wrong, and not the fencing token, which guards a different
	// boundary.
	//
	// Returns ErrEffectExists when the key is taken. That is not a failure: it
	// means the action already has a record, and the caller must load it and
	// branch on its status.
	ReserveEffect(ctx context.Context, e Effect) error

	// GetEffect reads a ledger row inside the transaction.
	GetEffect(ctx context.Context, key IdempotencyKey) (Effect, error)

	// TransitionEffect moves an effect between statuses, applying only if it
	// is currently in from. Returns ErrConflict if it is not, and
	// ErrInvalidTransition if the edge is not permitted.
	TransitionEffect(ctx context.Context, key IdempotencyKey, from, to EffectStatus, out EffectOutcome) error

	// RegisterWorker records a worker, or updates one already known.
	//
	// Layer 2 needs this only because a lease points at a worker and must
	// point at something real. Capability routing and liveness heartbeating
	// arrive in Layer 3, when workers can live in another process.
	RegisterWorker(ctx context.Context, w Worker) error

	// AcquireLease takes ownership of a task until expiresAt, returning a
	// fencing token strictly higher than any previously issued for that task.
	//
	// A live lease held by someone else yields ErrLeaseHeld. An expired one is
	// taken over, which is the entire point: a worker that went silent must
	// not be able to hold work hostage. The token counter survives the
	// takeover, because a token that could repeat would stop distinguishing
	// the new owner from the old one.
	AcquireLease(ctx context.Context, taskID TaskID, workerID string, now, expiresAt time.Time) (Lease, error)

	// GetLease reads current ownership. Returns ErrNotFound if the task has
	// never been claimed.
	GetLease(ctx context.Context, taskID TaskID) (Lease, error)

	// ExtendLease pushes back the expiry, for the holder of token only.
	//
	// A heartbeat is a mutation and is fenced like every other one. Without
	// that check a stale worker could renew a lease it no longer owns, and the
	// runtime would believe live work belonged to a process that is gone.
	// Returns ErrFenced when the token is not current.
	ExtendLease(ctx context.Context, taskID TaskID, token FencingToken, expiresAt time.Time) error

	// ReleaseLease gives up ownership, for the holder of token only. The row
	// survives so the fencing token keeps counting up.
	ReleaseLease(ctx context.Context, taskID TaskID, token FencingToken, now time.Time) error
}

// Dispatcher carries a task from the runtime to a worker.
//
// It carries identity, never state. A delivery says "task T may need
// attention", and the worker then reads what is actually true from the Store.
// That is what makes duplicated, delayed, and out-of-order deliveries
// uninteresting: order across tasks is irrelevant because tasks are
// independent, and order within a run comes from run version CAS.
//
// Delivery is at-least-once and always will be. Duplicate suppression belongs
// to the claim transition and to the effect ledger's unique key — never to the
// transport.
//
// Layer 3 adds acknowledgement and redelivery for the JetStream adapter. The
// in-process adapter has neither, which is honest: it cannot lose a message
// because it never leaves the process.
type Dispatcher interface {
	// Publish makes a committed task available for delivery.
	Publish(ctx context.Context, id TaskID) error

	// Claim blocks until a task is available, ctx is done, or the dispatcher
	// is closed (ErrDispatcherClosed).
	Claim(ctx context.Context) (TaskID, error)

	Close() error
}

// Clock is the only source of time above the composition root.
type Clock interface {
	Now() time.Time
}

// IDGen is the only source of identity above the composition root.
type IDGen interface {
	NewRunID() RunID
	NewTaskID() TaskID
	NewEffectID() EffectID
}

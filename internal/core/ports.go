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

	Close() error
}

// Tx is the transactional write surface.
//
// It is an interface rather than a driver handle so that no concrete
// transaction type ever appears in a signature the engine can see. An adapter
// that needed to expose *sql.Tx here would be telling us the port is wrong.
type Tx interface {
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
// to the claim transition and, from Layer 2, to the effect ledger's unique
// key — never to the transport.
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

// ToolHandler executes one tool call. Payload and return value are opaque
// JSON: the runtime never interprets either.
type ToolHandler func(ctx context.Context, payload []byte) ([]byte, error)

// ToolRegistry resolves a task type to the code that runs it.
//
// From Layer 2 this also carries the effect class, which is what decides
// whether an ambiguous outcome may be retried, must be queried, or has to be
// escalated. The engine will read that off the descriptor rather than
// switching on tool names — a switch on a tool name anywhere above this
// interface is the coupling this port exists to prevent.
type ToolRegistry interface {
	Lookup(name string) (ToolHandler, error)
	Names() []string
}

// Clock is the only source of time above the composition root.
type Clock interface {
	Now() time.Time
}

// IDGen is the only source of identity above the composition root.
type IDGen interface {
	NewRunID() RunID
	NewTaskID() TaskID
}

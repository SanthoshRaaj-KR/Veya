package core

import (
	"context"
	"encoding/json"
	"time"
)

// SignalID is the sender's identity for one delivery.
//
// It is the dedup key, and it comes from the sender rather than from us
// because only the sender knows that its second call is a retry of the first.
// At-least-once delivery is the assumption everywhere else in this system —
// the dispatcher, the outbox, the effect ledger — and the signal port does not
// get to be the one place that assumes otherwise.
type SignalID string

func (s SignalID) String() string { return string(s) }

// Signal is something that happened outside the system, recorded against a run.
//
// # It is stored on arrival, whether or not anything is waiting
//
// That is the whole design, and it exists to delete a race rather than narrow
// it. The bug everyone writes here is the early signal: an external system
// calls back faster than the run reaches its wait, the delivery finds no
// waiter, and the signal is dropped. The run then waits forever for something
// that already happened. It is a race, so it passes every test written by
// someone who has not thought about it, and it fails in production under load.
//
// So there is no "deliver to a waiting run" path at all. Arrival writes a row.
// A wait is a *read* that either finds the row and proceeds, or parks until
// one appears. Early and late arrival run identical code, which means the case
// that is hard to test is the case that is always exercised.
type Signal struct {
	RunID RunID

	// ID is unique per run. A second arrival under the same ID is the same
	// signal, and is refused with ErrSignalExists rather than recorded twice.
	ID SignalID

	// Name is what the run waits for: "approval", "payment_settled". Several
	// signals may share a name on one run — two approvals, three callbacks —
	// which is why a wait consumes one rather than matching the name and
	// moving on.
	Name string

	// Payload is opaque JSON handed to the waiting body. The runtime never
	// interprets it.
	Payload json.RawMessage

	CreatedAt time.Time
}

// SignalStore reads signals recorded against a run.
//
// Writing one is a Tx method, because arrival has to do two things together:
// record the signal, and make the run eligible to be looked at again. Split,
// a crash between them leaves a signal nobody will act on attached to a run
// that is still parked, which is the failure the storage was supposed to
// prevent, reintroduced one layer down.
type SignalStore interface {
	// Signals returns every signal recorded against a run under a name,
	// oldest first.
	//
	// It does not filter out the ones already consumed, and that is
	// deliberate: which signals a run has taken is recorded in its history, as
	// SIGNAL_RECEIVED events carrying the signal id. A `consumed` column would
	// be a second copy of that fact, free to disagree with it — and a run
	// whose history says it took a signal that the table says is unconsumed is
	// a run that takes it twice on the next replay.
	Signals(ctx context.Context, runID RunID, name string) ([]Signal, error)
}

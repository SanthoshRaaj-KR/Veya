package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// DeliverSignal records that something happened outside the system, and makes
// the run eligible to be looked at again.
//
// It is a package-level function over a Store rather than a method, because
// the two things that deliver signals have different amounts of machinery: the
// runtime has a whole engine, and `veya signal` has a database connection.
// Both need the same two writes in the same transaction, and a second
// implementation of that pair is a second chance to get the ordering wrong.
//
// # It does not care whether anything is waiting
//
// That is the entire design. There is no "find the waiter" path here, so the
// early signal — a callback that arrives before the run reaches its wait —
// takes exactly the same code as a late one. A wait is a read; see
// docs/execution-model.md section 4.
//
// # A repeat delivery is success
//
// Senders retry. ErrSignalExists means this exact delivery is already
// recorded, and reporting that as a failure would have an honest sender
// retrying forever against a constraint that is doing its job.
func DeliverSignal(ctx context.Context, store core.Store, sig core.Signal) error {
	if sig.RunID == "" || sig.ID == "" || sig.Name == "" {
		return fmt.Errorf("deliver signal: a signal needs a run, an id and a name")
	}

	run, err := store.GetRun(ctx, sig.RunID)
	if err != nil {
		return fmt.Errorf("deliver signal to %s: %w", sig.RunID, err)
	}
	if run.Status.IsTerminal() {
		// Refused rather than stored. A signal for a finished run will never
		// be read by anything, and accepting it would tell the sender its
		// callback landed somewhere that mattered.
		return fmt.Errorf("deliver signal to %s: the run is %s: %w",
			sig.RunID, run.Status, core.ErrInvalidTransition)
	}

	err = store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.RecordSignal(ctx, sig); err != nil {
			return err
		}
		// Un-parking in the same transaction is what makes the wake-up
		// transactional rather than a hint. A run waiting on a signal is
		// parked at Indefinite, so a missed wake would be a run that never
		// resumes -- not a latency cost, which is what the outbox relay's
		// Wake is allowed to be.
		return tx.ReleaseRun(ctx, sig.RunID)
	})
	switch {
	case errors.Is(err, core.ErrSignalExists):
		return nil
	case err != nil:
		return fmt.Errorf("deliver signal to %s: %w", sig.RunID, err)
	}
	return nil
}

// Signal delivers a signal and gives the run an immediate chance to act on it.
//
// The advance is liveness only. The run was un-parked by the delivery, so the
// recovery scan will reach it regardless; this just saves it a tick, which
// matters when the signal is a human clicking approve and watching.
func (e *Engine) Signal(ctx context.Context, sig core.Signal) error {
	if err := DeliverSignal(ctx, e.store, sig); err != nil {
		return err
	}
	e.log.Info("signal delivered", "run_id", sig.RunID, "signal_id", sig.ID, "name", sig.Name)

	if err := e.Advance(ctx, sig.RunID); err != nil {
		// Not the sender's problem, and not a reason to report a durable
		// delivery as failed. The scan will try again.
		e.log.Debug("signal delivered but the run did not advance yet",
			"run_id", sig.RunID, "error", err)
	}
	return nil
}

// unconsumedSignal returns the oldest stored signal under this wait's name
// that the run has not already taken.
func (e *Engine) unconsumedSignal(ctx context.Context, runID core.RunID,
	history []core.Event, wait core.Wait) (core.Signal, bool, error) {

	stored, err := e.store.Signals(ctx, runID, wait.Signal)
	if err != nil {
		return core.Signal{}, false, fmt.Errorf("read signals for %s: %w", runID, err)
	}

	taken := core.ConsumedSignals(history)
	for _, sig := range stored {
		if !taken[sig.ID] {
			return sig, true, nil
		}
	}
	return core.Signal{}, false, nil
}

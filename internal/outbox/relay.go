// Package outbox moves committed delivery intent onto the dispatcher.
//
// The engine writes a row saying "task T should be delivered" in the same
// transaction as the task itself. This is the only thing that reads those rows
// and turns them into actual deliveries.
//
// # Why the publisher is a separate loop
//
// Because publishing cannot be part of the transaction. A broker has no way to
// participate in a PostgreSQL commit, so any code that writes the task and
// publishes it in one breath has two operations that can disagree, and the
// disagreement is silent: the task exists, nobody is told, the run stops. The
// fix is not to try harder at publishing — it is to make the *intent* durable
// and let a separate loop be late instead of lossy.
//
// # What it is allowed to get wrong
//
// Everything about timing. A relay that runs slowly delays work; a relay that
// crashes after publishing and before marking the row publishes twice. Both are
// liveness costs, and both are absorbed — the first by the recovery scan, the
// second by the conditional PENDING -> RUNNING claim. What it may never do is
// mark a row published that was not, which is why the ordering below is fixed:
// publish, then mark.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Relay drains the outbox into the dispatcher.
type Relay struct {
	store      core.Store
	dispatcher core.Dispatcher
	interval   time.Duration
	batch      int
	log        *slog.Logger

	// wake is a liveness shortcut, not a channel of work. See Wake.
	wake chan struct{}
}

// Config wires a Relay. Store and Dispatcher are required.
type Config struct {
	Store      core.Store
	Dispatcher core.Dispatcher

	// Interval is the longest a committed task waits to be published if nobody
	// calls Wake. Defaults to 1s.
	//
	// It is a backstop, not the normal path: the engine wakes the relay the
	// moment it commits, so this only governs work committed by another
	// process, or a wake that was dropped because a sweep was already running.
	Interval time.Duration

	// Batch caps how many deliveries one sweep publishes. Defaults to 100.
	Batch int

	Logger *slog.Logger
}

// New validates the configuration and returns a Relay.
func New(cfg Config) (*Relay, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("outbox: Store is required")
	case cfg.Dispatcher == nil:
		return nil, errors.New("outbox: Dispatcher is required")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = time.Second
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = 100
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	return &Relay{
		store:      cfg.Store,
		dispatcher: cfg.Dispatcher,
		interval:   interval,
		batch:      batch,
		log:        log,
		wake:       make(chan struct{}, 1),
	}, nil
}

// Wake hints that there is something to publish now.
//
// It is a pure liveness optimization and correctness does not depend on it:
// the ticker finds the same rows a moment later, and a dropped wake is
// therefore not a lost delivery. That is exactly why it is a plain method with
// a one-slot buffer and a non-blocking send — it must never be able to block
// the transaction that just committed.
func (r *Relay) Wake() {
	select {
	case r.wake <- struct{}{}:
	default: // a sweep is already pending; one is enough
	}
}

// Run drains the outbox until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("outbox relay started", "interval", r.interval, "batch", r.batch)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Sweep immediately: a restart should publish whatever the process that
	// just died committed and never announced.
	r.sweep(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("outbox relay stopped")
			return ctx.Err()
		case <-ticker.C:
			r.sweep(ctx)
		case <-r.wake:
			r.sweep(ctx)
		}
	}
}

func (r *Relay) sweep(ctx context.Context) {
	published, err := r.RelayOnce(ctx)
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		r.log.Debug("relay sweep interrupted by shutdown")
		return
	case errors.Is(err, core.ErrDispatcherClosed):
		r.log.Debug("relay sweep stopped: dispatcher closed")
		return
	default:
		r.log.Error("relay sweep failed", "error", err)
		return
	}
	if published > 0 {
		r.log.Debug("published committed deliveries", "count", published)
	}
}

// RelayOnce publishes one batch and reports how many deliveries it handed off.
//
// The ordering is the whole contract of this function:
//
//  1. read unpublished rows
//  2. publish each one
//  3. mark the ones that published, count the ones that did not
//
// Marking before publishing would lose a delivery whenever the process died in
// between. Marking after only duplicates one, and the claim absorbs duplicates.
// When the two failure modes are "silently stuck" and "delivered twice", the
// design always takes the second.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	pending, err := r.store.PendingDeliveries(ctx, r.batch)
	if err != nil {
		return 0, fmt.Errorf("list pending deliveries: %w", err)
	}
	if len(pending) == 0 {
		return 0, nil
	}

	var (
		delivered []core.DeliveryID
		failed    = map[core.DeliveryID]string{}
	)
	for _, d := range pending {
		err := r.dispatcher.Publish(ctx, d.TaskID)
		switch {
		case err == nil:
			delivered = append(delivered, d.ID)

		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
			errors.Is(err, core.ErrDispatcherClosed):
			// Shutdown. Record what did publish and stop; the rows left behind
			// are still pending and the next process to sweep will find them.
			if markErr := r.mark(ctx, delivered, failed); markErr != nil {
				return 0, markErr
			}
			return len(delivered), err

		default:
			failed[d.ID] = err.Error()
			r.log.Warn("could not publish a committed task; it stays in the outbox",
				"task_id", d.TaskID, "delivery_id", d.ID, "attempts", d.Attempts+1, "error", err)
		}
	}

	if err := r.mark(ctx, delivered, failed); err != nil {
		return 0, err
	}
	return len(delivered), nil
}

// mark records the outcome of one batch in a single transaction.
//
// Successes and failures go together because they describe the same sweep, and
// splitting them would let a crash in the middle record the failures without
// the successes — which republishes work that was already published, for no
// benefit.
func (r *Relay) mark(ctx context.Context, delivered []core.DeliveryID, failed map[core.DeliveryID]string) error {
	if len(delivered) == 0 && len(failed) == 0 {
		return nil
	}
	err := r.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.MarkDelivered(ctx, delivered); err != nil {
			return err
		}
		for id, cause := range failed {
			if err := tx.FailDelivery(ctx, id, cause); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// The publishes already happened. Failing to record that costs a
		// duplicate delivery on the next sweep and nothing else.
		return fmt.Errorf("record delivery outcomes: %w", err)
	}
	return nil
}

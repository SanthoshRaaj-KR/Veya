// Package lease reclaims work whose owner has gone silent.
//
// The reaper is the liveness half of ownership. Leases expire so that a dead
// worker cannot hold a task forever; something has to notice that expiry and
// put the work back, and that is all this package does.
//
// # The reaper is not above the concurrency model
//
// It is just another process. It can be slow, it can be wrong about the clock,
// and there can be more than one of it running. So it does not reach past the
// mechanisms everyone else obeys: to take a task away from its previous owner
// it acquires the lease itself, which issues a strictly higher fencing token
// and thereby fences the old owner out. A reaper that reassigned tasks by
// writing directly would be the largest hole in the system, because it would
// be the one actor able to create two live owners.
//
// For the same reason it does not publish. A reclaimed task's delivery intent
// is written inside the transaction that makes it PENDING again, exactly as it
// was at creation — this path exists because something already failed, and it
// is the last place that should introduce a new way to lose work.
package lease

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// ReaperWorkerID is the identity the reaper uses when it takes a lease. It is
// a real registered worker because a lease must reference one.
const ReaperWorkerID = "veya-reaper"

// Reaper returns expired work to the queue.
type Reaper struct {
	store    core.Store
	clock    core.Clock
	interval time.Duration
	batch    int
	wake     func()
	log      *slog.Logger

	registerOnce sync.Once
	registerErr  error
}

// Config wires a Reaper. Store and Clock are required.
type Config struct {
	Store core.Store
	Clock core.Clock

	// Wake nudges the outbox relay after a reclaim, and may be nil.
	//
	// The reaper does not publish. A reclaimed task gets its delivery intent
	// inside the same transaction that makes it PENDING again, so there is no
	// moment where it is runnable and unannounced — which matters more here
	// than anywhere else, since this code path exists precisely because
	// something already went wrong.
	Wake func()

	// Interval between sweeps. Defaults to 5s.
	//
	// Recovery latency is roughly the lease TTL plus this, and both are
	// liveness costs: a slow sweep delays work, it does not corrupt anything.
	// That is why an approximate value here is acceptable in a way that an
	// approximate fencing check never would be.
	Interval time.Duration

	// Batch caps how many leases one sweep reclaims. Defaults to 100.
	Batch int

	Logger *slog.Logger
}

// New validates the configuration and returns a Reaper.
func New(cfg Config) (*Reaper, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("lease: Store is required")
	case cfg.Clock == nil:
		return nil, errors.New("lease: Clock is required")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = 100
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	wake := cfg.Wake
	if wake == nil {
		wake = func() {}
	}

	return &Reaper{
		store:    cfg.Store,
		clock:    cfg.Clock,
		interval: interval,
		batch:    batch,
		wake:     wake,
		log:      log,
	}, nil
}

// Run sweeps until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	if err := r.ensureRegistered(ctx); err != nil {
		return err
	}
	r.log.Info("lease reaper started", "interval", r.interval, "batch", r.batch)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Sweep immediately: a restart should not wait out a full interval before
	// noticing work abandoned by the process that just died.
	r.sweep(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("lease reaper stopped")
			return ctx.Err()
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

// ensureRegistered records the reaper as a worker, once.
//
// Taking a lease requires a worker row, and the reaper takes leases in order
// to bump fencing tokens. Registering here rather than only in Run keeps
// ReapOnce usable on its own — by a test, or by an operator triggering a sweep
// — instead of failing in a way that looks like "nothing needed reclaiming".
func (r *Reaper) ensureRegistered(ctx context.Context) error {
	r.registerOnce.Do(func() {
		r.registerErr = r.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
			return tx.RegisterWorker(ctx, core.Worker{
				ID:     ReaperWorkerID,
				Type:   "reaper",
				Status: core.WorkerActive,
			})
		})
	})
	if r.registerErr != nil {
		return fmt.Errorf("lease: register reaper: %w", r.registerErr)
	}
	return nil
}

func (r *Reaper) sweep(ctx context.Context) {
	reclaimed, err := r.ReapOnce(ctx)
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		r.log.Debug("sweep interrupted by shutdown")
		return
	default:
		r.log.Error("lease sweep failed", "error", err)
		return
	}
	if reclaimed > 0 {
		r.log.Info("reclaimed abandoned tasks", "count", reclaimed)
	}
}

// ReapOnce performs a single sweep and reports how many tasks it reclaimed.
func (r *Reaper) ReapOnce(ctx context.Context) (int, error) {
	if err := r.ensureRegistered(ctx); err != nil {
		return 0, err
	}
	now := r.clock.Now()

	expired, err := r.store.ExpiredLeases(ctx, now, r.batch)
	if err != nil {
		return 0, fmt.Errorf("list expired leases: %w", err)
	}

	reclaimed := 0
	for _, stale := range expired {
		requeued, err := r.reclaim(ctx, stale, now)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return reclaimed, err
		case err != nil:
			// One stuck task must not stop the others being recovered. The
			// next sweep will try again.
			r.log.Error("could not reclaim task", "task_id", stale.TaskID, "error", err)
			continue
		}
		if requeued {
			reclaimed++
		}
	}
	if reclaimed > 0 {
		// The deliveries are committed; this only saves the relay a tick.
		r.wake()
	}
	return reclaimed, nil
}

// reclaim takes one abandoned task back, and reports whether it is runnable
// again.
//
// Everything happens in one transaction, including the token bump, so the old
// owner is fenced out at the same instant the task becomes available. Any
// other ordering leaves a window in which the task is claimable while its
// previous owner can still write.
func (r *Reaper) reclaim(ctx context.Context, stale core.Lease, now time.Time) (bool, error) {
	requeued := false

	err := r.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		task, err := tx.GetTask(ctx, stale.TaskID)
		if err != nil {
			return err
		}

		// Somebody already dealt with it between the query and here — a
		// competing reaper, or the original worker finishing just in time.
		if task.Status != core.TaskRunning {
			return nil
		}

		// Take the lease. This is the whole mechanism: acquiring it issues a
		// strictly higher token, so the previous owner's next heartbeat or
		// report is refused. The reaper holds it only for this transaction.
		fresh, err := tx.AcquireLease(ctx, stale.TaskID, ReaperWorkerID, now, now.Add(time.Minute))
		if err != nil {
			// A live lease means somebody legitimately owns it again.
			if errors.Is(err, core.ErrLeaseHeld) {
				return nil
			}
			return err
		}

		if err := core.Append(ctx, tx, task.RunID, core.EventLeaseExpired, task.StepID, core.LeaseExpiredData{
			TaskID:        task.ID,
			PreviousOwner: stale.WorkerID,
			PreviousToken: stale.Token,
		}); err != nil {
			return err
		}

		// A task that has already used its last attempt must not go round
		// again; the run fails on the next advance instead of looping.
		next := core.TaskPending
		event := core.EventTaskRetryScheduled
		if task.Exhausted() {
			next = core.TaskDeadLetter
			event = core.EventTaskDeadLettered
		}
		reason := fmt.Sprintf("owner %s stopped reporting; lease expired at %s",
			stale.WorkerID, stale.ExpiresAt.Format(time.RFC3339))

		if err := tx.TransitionTask(ctx, task.ID, core.TaskRunning, next,
			core.TaskOutcome{Error: reason}); err != nil {
			return err
		}
		if err := core.Append(ctx, tx, task.RunID, event, task.StepID, core.TaskRetryScheduledData{
			TaskID:  task.ID,
			Attempt: task.Attempt,
			Reason:  reason,
		}); err != nil {
			return err
		}

		// Delivery intent joins the same transaction, so the task becomes
		// runnable and announced at the same instant. A dead-lettered task gets
		// none: nobody should be told to work on it.
		if next == core.TaskPending {
			if err := tx.EnqueueDelivery(ctx, task.ID); err != nil {
				return err
			}
		}

		// Release straight away. The reaper does not execute anything, so
		// holding the lease would only stop a real worker taking the task.
		// The token stays where the acquisition left it, which is what keeps
		// the old owner fenced.
		if err := tx.ReleaseLease(ctx, task.ID, fresh.Token, now); err != nil {
			return err
		}

		requeued = next == core.TaskPending
		r.log.Info("reclaimed an abandoned task",
			"task_id", task.ID, "previous_owner", stale.WorkerID,
			"previous_token", stale.Token, "new_token", fresh.Token,
			"attempt", task.Attempt, "outcome", next)
		return nil
	})

	return requeued, err
}

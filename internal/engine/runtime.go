package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Runtime is the recovery loop: it periodically re-reads the store and
// republishes or re-advances anything that has stalled.
//
// It exists because the engine holds no authoritative state in memory.
//
// The outbox closed the window this used to be the only answer for — a task can
// no longer commit without its delivery intent committing too. What is left is
// the failure the outbox cannot see:
//
//   - A delivery that was published and then lost in transit. The outbox row
//     says published, which is true, and the message is gone anyway: the
//     process holding an in-process channel died, or a stream was purged. Only
//     the task's own PENDING status still reflects that nothing happened.
//   - A run started by a different process — the CLI creates a run, and the
//     runtime process is what has to notice and advance it.
//   - A run whose park has expired. From Layer 5 the scan is also the timer
//     wheel: nothing holds a pending wake-up in memory, so a sleeping run is
//     simply one the scan is not yet allowed to pick up, and the moment it is
//     allowed it is indistinguishable from any other run owed a decision.
//
// So the outbox guarantees a task is announced, and the scan covers the case
// where it was announced to nobody. The scan republishes tasks that may already
// be in flight, which is deliberate and safe: the conditional claim rejects the
// duplicate. Preferring a redundant delivery over a missed one is the same
// trade the whole system makes — a duplicate is visible and cheap, stalled work
// is silent.
type Runtime struct {
	engine     *Engine
	store      core.Store
	dispatcher core.Dispatcher
	clock      core.Clock
	interval   time.Duration
	batch      int
	log        *slog.Logger
}

// RuntimeConfig configures the recovery loop.
type RuntimeConfig struct {
	Engine *Engine
	// Interval between scans. Defaults to 5s.
	Interval time.Duration
	// Batch caps how much each scan picks up. Defaults to 100.
	Batch int
}

// NewRuntime returns a recovery loop over an engine.
func NewRuntime(cfg RuntimeConfig) *Runtime {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = 100
	}
	return &Runtime{
		engine:     cfg.Engine,
		store:      cfg.Engine.store,
		dispatcher: cfg.Engine.dispatcher,
		clock:      cfg.Engine.clock,
		interval:   interval,
		batch:      batch,
		log:        cfg.Engine.log,
	}
}

// Run scans until ctx is cancelled.
//
// The gap between scans is the interval, or less when a parked run is due
// sooner. A fixed ticker alone would make every sleep round up: a run asked to
// wait 200ms would wait for the next five-second tick, and "sleep for a
// second" would be a phrase the runtime could not honour.
//
// The shorter gap is recomputed from the store on every pass and never held.
// That is the whole distinction this commit is about — a time.After holding a
// pending wake-up is state above the store, and state above the store is state
// a restart loses. Here the worst a lost or stale hint can do is make a
// wake-up late by one interval.
func (r *Runtime) Run(ctx context.Context) error {
	r.log.Info("recovery loop started", "interval", r.interval, "batch", r.batch)

	// Scan immediately, so a restart picks up stranded work -- including a
	// wake-up that expired while the process was down -- without waiting out
	// a full interval.
	r.ScanOnce(ctx)

	timer := time.NewTimer(r.nextDelay(ctx))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("recovery loop stopped")
			return ctx.Err()
		case <-timer.C:
			r.ScanOnce(ctx)
			timer.Reset(r.nextDelay(ctx))
		}
	}
}

// nextDelay is how long to wait before the next scan.
//
// It is a hint from the store, clamped on both ends: never longer than the
// configured interval, because the interval is the guarantee, and never
// shorter than a floor, because a due run that fails to advance would
// otherwise be retried as fast as the machine allows.
func (r *Runtime) nextDelay(ctx context.Context) time.Duration {
	floor := minScanDelay
	if r.interval < floor {
		floor = r.interval
	}

	wake, ok, err := r.store.NextWakeUp(ctx, r.clock.Now())
	if err != nil {
		// Not worth logging at anything above debug. The next tick still
		// happens, on the interval, and the work still gets picked up.
		r.log.Debug("scan: next wake up unavailable", "error", err)
		return r.interval
	}
	if !ok {
		return r.interval
	}

	d := wake.Sub(r.clock.Now())
	if d > r.interval {
		return r.interval
	}
	if d < floor {
		return floor
	}
	return d
}

// minScanDelay stops a run that is due and cannot advance from being retried
// in a tight loop.
const minScanDelay = 50 * time.Millisecond

// ScanOnce performs a single recovery pass.
//
// Errors are logged rather than returned. A scan is best-effort by nature: one
// unreachable run must not stop the others from being recovered, and the next
// tick will try again.
func (r *Runtime) ScanOnce(ctx context.Context) {
	tasks, err := r.store.PendingTasks(ctx, r.batch)
	if err != nil {
		r.logFailure("scan: list pending tasks", err)
	}
	for _, t := range tasks {
		if err := r.dispatcher.Publish(ctx, t.ID); err != nil {
			r.logFailure("scan: republish", err, "task_id", t.ID)
		}
	}

	runs, err := r.store.RunsAwaitingAdvance(ctx, r.clock.Now(), r.batch)
	if err != nil {
		r.logFailure("scan: list runs awaiting advance", err)
	}
	for _, id := range runs {
		if err := r.engine.Advance(ctx, id); err != nil {
			r.logFailure("scan: advance", err, "run_id", id)
			// A cancelled context means shutdown, not a bad run. Abandon the
			// pass rather than grinding through the rest to fail identically.
			if isShutdown(err) {
				return
			}
		}
	}

	if len(tasks) > 0 || len(runs) > 0 {
		r.log.Debug("scan complete", "republished", len(tasks), "advanced", len(runs))
	}
}

// logFailure records a scan failure, quietly if it is just shutdown.
//
// Work interrupted by a cancelled context is not lost — it is committed in the
// store and the next process to scan will find it. Logging that at ERROR
// trains operators to ignore ERROR, which is worse than saying nothing.
func (r *Runtime) logFailure(msg string, err error, args ...any) {
	args = append(args, "error", err)
	if isShutdown(err) {
		r.log.Debug(msg+" (interrupted by shutdown)", args...)
		return
	}
	r.log.Error(msg, args...)
}

func isShutdown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

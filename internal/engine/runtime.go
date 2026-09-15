package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/santhoshraajkr/veya/internal/core"
)

// Runtime is the recovery loop: it periodically re-reads the store and
// republishes or re-advances anything that has stalled.
//
// It exists because the engine holds no authoritative state in memory. Two
// things can leave work stranded, and both are found by scanning:
//
//   - A task committed but never published, because the process died between
//     the transaction and the Publish call. Layer 3's outbox closes that
//     window; until then the scan is what covers it.
//   - A run started by a different process — the CLI creates a run, and the
//     runtime process is what has to notice and advance it.
//
// The scan republishes tasks that may already be in flight. That is deliberate
// and safe: the conditional claim rejects the duplicate. Preferring a
// redundant delivery over a missed one is the same trade the whole system
// makes — a duplicate is visible and cheap, stalled work is silent.
type Runtime struct {
	engine     *Engine
	store      core.Store
	dispatcher core.Dispatcher
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
		interval:   interval,
		batch:      batch,
		log:        cfg.Engine.log,
	}
}

// Run scans until ctx is cancelled.
func (r *Runtime) Run(ctx context.Context) error {
	r.log.Info("recovery loop started", "interval", r.interval, "batch", r.batch)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Scan immediately, so a restart picks up stranded work without waiting
	// out a full interval.
	r.ScanOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("recovery loop stopped")
			return ctx.Err()
		case <-ticker.C:
			r.ScanOnce(ctx)
		}
	}
}

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

	runs, err := r.store.RunsAwaitingAdvance(ctx, r.batch)
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

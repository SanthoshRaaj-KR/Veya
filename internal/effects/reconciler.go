package effects

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Reconciler settles effects that no live task will ever settle.
//
// The executor resolves ambiguity for a task someone is holding. This handles
// what is left over: an effect whose task dead-lettered, whose run failed, or
// whose worker died at a moment no retry ever reached. Without it those rows
// sit in RUNNING or UNKNOWN forever, and "we do not know" quietly becomes "we
// never found out".
//
// # It resolves knowledge; it never performs actions
//
// This is the important constraint, and it is the difference between this and
// the executor. An IDEMPOTENT_BY_KEY effect is normally resolved by
// re-sending, which is safe because the provider deduplicates — but only when
// a live task still wants the outcome. Here the task is usually gone and the
// run has usually failed, so re-sending would perform a real external action
// on behalf of work that nobody is waiting for any more.
//
// So this loop only ever asks. An effect it cannot ask about stays unresolved
// and visible, which is the honest outcome: a person has to decide.
type Reconciler struct {
	store      core.Store
	tools      core.ToolRegistry
	clock      core.Clock
	interval   time.Duration
	staleAfter time.Duration
	batch      int
	log        *slog.Logger
}

// ReconcilerConfig wires a Reconciler. Store, Tools and Clock are required.
type ReconcilerConfig struct {
	Store core.Store
	Tools core.ToolRegistry
	Clock core.Clock

	// Interval between sweeps. Defaults to 30s.
	Interval time.Duration

	// StaleAfter is how long an effect must sit untouched before this loop
	// takes an interest. Defaults to 1 minute.
	//
	// It exists to keep the reconciler away from live work. An effect marked
	// RUNNING a second ago is almost certainly a request in flight, and asking
	// the provider about it would be both wasteful and misleading — the answer
	// would be "no" right up until it is "yes".
	StaleAfter time.Duration

	// Batch caps how many effects one sweep examines. Defaults to 50.
	Batch int

	Logger *slog.Logger
}

// NewReconciler validates the configuration and returns a Reconciler.
func NewReconciler(cfg ReconcilerConfig) (*Reconciler, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("effects: Store is required")
	case cfg.Tools == nil:
		return nil, errors.New("effects: Tools is required")
	case cfg.Clock == nil:
		return nil, errors.New("effects: Clock is required")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	staleAfter := cfg.StaleAfter
	if staleAfter <= 0 {
		staleAfter = time.Minute
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = 50
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	return &Reconciler{
		store:      cfg.Store,
		tools:      cfg.Tools,
		clock:      cfg.Clock,
		interval:   interval,
		staleAfter: staleAfter,
		batch:      batch,
		log:        log,
	}, nil
}

// Run sweeps until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	r.log.Info("effect reconciler started",
		"interval", r.interval, "stale_after", r.staleAfter, "batch", r.batch)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("effect reconciler stopped")
			return ctx.Err()
		case <-ticker.C:
			settled, err := r.ReconcileOnce(ctx)
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				r.log.Debug("reconciliation interrupted by shutdown")
			case err != nil:
				r.log.Error("reconciliation sweep failed", "error", err)
			case settled > 0:
				r.log.Info("settled previously unknown effects", "count", settled)
			}
		}
	}
}

// ReconcileOnce performs one sweep and reports how many effects it settled.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (int, error) {
	now := r.clock.Now()

	unresolved, err := r.store.UnresolvedEffects(ctx, now.Add(-r.staleAfter), r.batch)
	if err != nil {
		return 0, fmt.Errorf("list unresolved effects: %w", err)
	}

	settled := 0
	for _, effect := range unresolved {
		done, err := r.settle(ctx, effect, now)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return settled, err
		case err != nil:
			// One unsettleable effect must not stop the others. It stays
			// visible either way.
			r.log.Error("could not settle effect", "key", effect.Key, "error", err)
			continue
		}
		if done {
			settled++
		}
	}
	return settled, nil
}

// settle tries to establish what actually happened to one effect.
func (r *Reconciler) settle(ctx context.Context, effect core.Effect, now time.Time) (bool, error) {
	descriptor, err := r.tools.Lookup(effect.Type)
	if err != nil {
		// A tool this process does not host. Another worker type may own it,
		// so silence is correct rather than an error.
		r.log.Debug("no local descriptor for effect; leaving it alone",
			"key", effect.Key, "tool", effect.Type)
		return false, nil
	}

	// A RUNNING effect this old has no live owner. Recording that is worth
	// doing on its own: the ledger should not keep asserting that somebody is
	// working on it when nobody is.
	if effect.Status == core.EffectRunning {
		if err := r.markUnknown(ctx, effect); err != nil {
			return false, err
		}
		effect.Status = core.EffectUnknown
	}

	if descriptor.KeyExpired(effect, now) {
		// Past the provider's retention, a lookup cannot distinguish "never
		// happened" from "happened and was forgotten". Acting on that answer
		// is exactly the duplicate this system exists to prevent.
		r.log.Warn("effect is past its provider's key retention and needs a human",
			"key", effect.Key, "tool", effect.Type, "key_ttl", descriptor.KeyTTL,
			"age", now.Sub(effect.CreatedAt).Truncate(time.Second))
		return false, nil
	}

	if descriptor.Reconciler == nil {
		// Nothing to ask. Re-sending would resolve an IDEMPOTENT_BY_KEY effect
		// in the executor, but not here: there is no live task waiting for the
		// result, so a send would be an external action nobody asked for.
		r.log.Warn("effect cannot be settled automatically and needs a human",
			"key", effect.Key, "tool", effect.Type, "class", descriptor.Class)
		return false, nil
	}

	resolution, err := descriptor.Reconciler.Reconcile(ctx, effect)
	if err != nil {
		// Failing to ask is not an answer. It stays unknown and is asked again
		// next sweep.
		r.log.Warn("reconciliation lookup failed; effect stays unknown",
			"key", effect.Key, "error", err)
		return false, nil
	}

	switch resolution.Kind {
	case core.ResolvedCommitted:
		return true, r.record(ctx, effect, core.EffectCommitted, core.EffectOutcome{
			Response:    resolution.Response,
			ExternalRef: resolution.ExternalRef,
		}, resolution.Detail)

	case core.ResolvedNotExecuted:
		// Definite, and therefore settled. The run it belonged to has already
		// failed; what this buys is a ledger that says "this did not happen"
		// instead of "we never found out".
		return true, r.record(ctx, effect, core.EffectFailed, core.EffectOutcome{
			Error: "reconciled: the provider confirms this never happened",
		}, resolution.Detail)

	case core.ResolvedUnknown:
		r.log.Warn("reconciliation could not settle the outcome",
			"key", effect.Key, "detail", resolution.Detail)
		return false, nil

	default:
		return false, fmt.Errorf("effect %s: unknown resolution %q", effect.Key, resolution.Kind)
	}
}

func (r *Reconciler) markUnknown(ctx context.Context, effect core.Effect) error {
	err := r.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		out := core.EffectOutcome{Error: "owner stopped without reporting an outcome"}
		if err := tx.TransitionEffect(ctx, effect.Key, core.EffectRunning, core.EffectUnknown, out); err != nil {
			return err
		}
		updated := effect
		updated.Status = core.EffectUnknown
		updated.LastError = out.Error
		return core.Append(ctx, tx, effect.RunID, core.EventEffectUnknown, "", effectData(updated, ""))
	})
	if errors.Is(err, core.ErrConflict) {
		// Someone settled it between the query and here. Good.
		return nil
	}
	if err != nil {
		return fmt.Errorf("mark %s unknown: %w", effect.Key, err)
	}
	return nil
}

func (r *Reconciler) record(ctx context.Context, effect core.Effect, to core.EffectStatus, out core.EffectOutcome, detail string) error {
	err := r.store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.TransitionEffect(ctx, effect.Key, core.EffectUnknown, to, out); err != nil {
			return err
		}
		updated := effect
		updated.Status = to
		updated.Response = out.Response
		updated.ExternalRef = out.ExternalRef
		updated.LastError = out.Error
		return core.Append(ctx, tx, effect.RunID, core.EventEffectReconciled, "", effectData(updated, detail))
	})
	if errors.Is(err, core.ErrConflict) {
		return nil // already settled by someone else
	}
	if err != nil {
		return fmt.Errorf("record %s for %s: %w", to, effect.Key, err)
	}

	r.log.Info("settled a previously unknown effect",
		"key", effect.Key, "status", to, "external_ref", out.ExternalRef, "detail", detail)
	return nil
}

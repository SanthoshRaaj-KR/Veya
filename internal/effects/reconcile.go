package effects

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// resolve settles an effect whose outcome is not known.
//
// The one thing it never does is assume. Each class gets the only treatment
// that is actually sound for it:
//
//	IDEMPOTENT_BY_KEY  re-send under the same key; the provider deduplicates
//	QUERYABLE          ask the provider what it did, then record that
//	anything else      escalate, because no runtime can resolve this
//
// Blind retry is absent from that list on purpose. It is the tempting option
// and it is a coin flip: half the time it fixes a lost response, half the time
// it sends a second refund.
func (x *Executor) resolve(ctx context.Context, task core.Task, d core.ToolDescriptor, effect core.Effect) (json.RawMessage, error) {
	// A RUNNING effect that reached here has no live owner — whoever marked it
	// RUNNING is gone, so the claim that someone is working on it is stale.
	// Record the truth before acting on it, both because the ledger should not
	// keep asserting something false and because RUNNING -> RUNNING is not a
	// legal edge.
	if effect.Status == core.EffectRunning {
		out := core.EffectOutcome{Error: "owner stopped without reporting an outcome"}
		if err := x.transition(ctx, task, effect, core.EffectRunning, core.EffectUnknown,
			out, core.EventEffectUnknown); err != nil {
			return nil, fmt.Errorf("mark %s unknown: %w", effect.Key, err)
		}
		effect.Status = core.EffectUnknown
		effect.LastError = out.Error
	}

	// The provider's key retention bounds what reconciliation can prove. Past
	// it, a lookup that says "no such key" means the provider forgot, not that
	// nothing happened — and re-sending on that basis is precisely the
	// duplicate this whole package exists to prevent.
	if d.KeyExpired(effect, x.clock.Now()) {
		return nil, x.escalate(ctx, task, effect, fmt.Sprintf(
			"key retention of %s has elapsed, so the provider can no longer be trusted "+
				"to recognise %s or to answer for it", d.KeyTTL, effect.Key))
	}

	switch d.Class {
	case core.ClassIdempotentByKey:
		// Safe by construction: the provider honours the key, so a repeat is
		// the same logical action rather than a second one.
		x.log.Info("re-sending an unknown effect under its original key",
			"key", effect.Key, "tool", task.Type)
		return x.perform(ctx, task, d, effect)

	case core.ClassQueryable:
		return x.query(ctx, task, d, effect)

	default:
		return nil, x.escalate(ctx, task, effect,
			fmt.Sprintf("%s outcomes cannot be resolved automatically", d.Class))
	}
}

// query asks the provider what it actually did, and records the answer.
func (x *Executor) query(ctx context.Context, task core.Task, d core.ToolDescriptor, effect core.Effect) (json.RawMessage, error) {
	if d.Reconciler == nil {
		// The registry rejects this at startup, so reaching it means something
		// constructed a descriptor by hand. Escalating is still the right
		// answer: a tool that cannot be asked is unreconcilable in practice,
		// whatever it claims to be.
		return nil, x.escalate(ctx, task, effect,
			"declared QUERYABLE but has no reconciler")
	}

	resolution, err := d.Reconciler.Reconcile(ctx, effect)
	if err != nil {
		// Failing to ask is not an answer. The effect stays UNKNOWN and will
		// be asked again later; concluding anything from a failed lookup would
		// be inventing information.
		x.log.Warn("reconciliation lookup failed; effect stays unknown",
			"key", effect.Key, "error", err)
		return nil, fmt.Errorf("%s: %w: lookup failed: %w", effect.Key, core.ErrNeedsReconciliation, err)
	}

	switch resolution.Kind {
	case core.ResolvedCommitted:
		out := core.EffectOutcome{
			Response:    resolution.Response,
			ExternalRef: resolution.ExternalRef,
		}
		if err := x.transition(ctx, task, effect, core.EffectUnknown, core.EffectCommitted,
			out, core.EventEffectReconciled); err != nil {
			return nil, fmt.Errorf("record reconciled commit for %s: %w", effect.Key, err)
		}
		x.log.Info("reconciled: the action had already happened",
			"key", effect.Key, "external_ref", resolution.ExternalRef)
		return resolution.Response, nil

	case core.ResolvedNotExecuted:
		// The provider is certain nothing happened, so executing now is a
		// first attempt rather than a repeat.
		x.log.Info("reconciled: the action never happened; executing it now", "key", effect.Key)
		return x.perform(ctx, task, d, effect)

	case core.ResolvedUnknown:
		x.log.Warn("reconciliation could not settle the outcome; leaving it unknown",
			"key", effect.Key, "detail", resolution.Detail)
		return nil, fmt.Errorf("%s: %w: %s", effect.Key, core.ErrNeedsReconciliation, resolution.Detail)

	default:
		return nil, fmt.Errorf("effect %s: reconciler returned unknown resolution %q",
			effect.Key, resolution.Kind)
	}
}

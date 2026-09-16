package memory

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The effect ledger, in memory.
//
// keyIndex mirrors UNIQUE (idempotency_key) in migration 0001. It is the
// reason ReserveEffect can be a single conditional insert here as it is on
// PostgreSQL: both adapters reject the second reservation for the same logical
// action, and the contract suite asserts they do it identically.

func (s *Store) GetEffect(_ context.Context, key core.IdempotencyKey) (core.Effect, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.st.effects[key]
	if !ok {
		return core.Effect{}, fmt.Errorf("effect %s: %w", key, core.ErrNotFound)
	}
	return e, nil
}

func (s *Store) ListEffects(_ context.Context, runID core.RunID) ([]core.Effect, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Effect
	for _, e := range s.st.effects {
		if e.RunID == runID {
			out = append(out, e)
		}
	}
	sortEffects(out)
	return out, nil
}

func (s *Store) UnresolvedEffects(_ context.Context, before time.Time, limit int) ([]core.Effect, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Effect
	for _, e := range s.st.effects {
		// RUNNING and UNKNOWN are the two states that mean "the outside world
		// may have changed and we have not written down whether it did".
		if e.Status != core.EffectRunning && e.Status != core.EffectUnknown {
			continue
		}
		if !e.UpdatedAt.Before(before) {
			continue // too fresh to be abandoned
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].UpdatedAt.Before(out[j].UpdatedAt)
		}
		return out[i].Key < out[j].Key
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func sortEffects(es []core.Effect) {
	sort.Slice(es, func(i, j int) bool {
		if !es[i].CreatedAt.Equal(es[j].CreatedAt) {
			return es[i].CreatedAt.Before(es[j].CreatedAt)
		}
		return es[i].Key < es[j].Key
	})
}

// --- transactional writes -------------------------------------------------

func (t *tx) ReserveEffect(_ context.Context, e core.Effect) error {
	if _, ok := t.st.runs[e.RunID]; !ok {
		return fmt.Errorf("run %s: %w", e.RunID, core.ErrNotFound)
	}
	if _, ok := t.st.tasks[e.TaskID]; !ok {
		return fmt.Errorf("task %s: %w", e.TaskID, core.ErrNotFound)
	}
	if !e.Status.Valid() {
		return fmt.Errorf("effect %s status %q: %w", e.Key, e.Status, core.ErrInvalidTransition)
	}
	if !e.Class.Valid() {
		return fmt.Errorf("effect %s class %q: %w", e.Key, e.Class, core.ErrInvalidTransition)
	}

	// UNIQUE (idempotency_key). One logical action, one row, enforced here and
	// not by anything the caller had to remember to do.
	if _, exists := t.st.effects[e.Key]; exists {
		return fmt.Errorf("effect %s: %w", e.Key, core.ErrEffectExists)
	}

	now := t.clock.Now()
	e.CreatedAt, e.UpdatedAt = now, now
	t.st.effects[e.Key] = e
	return nil
}

func (t *tx) GetEffect(_ context.Context, key core.IdempotencyKey) (core.Effect, error) {
	e, ok := t.st.effects[key]
	if !ok {
		return core.Effect{}, fmt.Errorf("effect %s: %w", key, core.ErrNotFound)
	}
	return e, nil
}

func (t *tx) TransitionEffect(_ context.Context, key core.IdempotencyKey, from, to core.EffectStatus, out core.EffectOutcome) error {
	e, ok := t.st.effects[key]
	if !ok {
		return fmt.Errorf("effect %s: %w", key, core.ErrNotFound)
	}
	if e.Status != from {
		return fmt.Errorf("effect %s is %s, expected %s: %w", key, e.Status, from, core.ErrConflict)
	}
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("effect %s %s -> %s: %w", key, from, to, core.ErrInvalidTransition)
	}

	now := t.clock.Now()
	e.Status = to
	e.UpdatedAt = now
	if out.Response != nil {
		e.Response = out.Response
	}
	if out.ExternalRef != "" {
		e.ExternalRef = out.ExternalRef
	}
	if out.Error != "" {
		e.LastError = out.Error
	}
	if to == core.EffectCommitted {
		committed := now
		e.CommittedAt = &committed
	}
	t.st.effects[key] = e
	return nil
}

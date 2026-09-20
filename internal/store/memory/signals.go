package memory

import (
	"context"
	"fmt"
	"sort"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Signals returns every signal recorded against a run under a name, oldest
// first.
//
// Consumed signals are included. Which ones a run has taken is recorded in its
// history; a second copy of that fact here would be free to disagree with it.
func (s *Store) Signals(_ context.Context, runID core.RunID, name string) ([]core.Signal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Signal
	for _, sig := range s.st.signals {
		if sig.RunID == runID && sig.Name == name {
			out = append(out, sig)
		}
	}
	sortSignals(out)
	return out, nil
}

func (t *tx) RecordSignal(_ context.Context, sig core.Signal) error {
	if _, ok := t.st.runs[sig.RunID]; !ok {
		return fmt.Errorf("run %s: %w", sig.RunID, core.ErrNotFound)
	}
	// PRIMARY KEY (run_id, signal_id). The sender's id is the dedup, so a
	// retried callback collides here rather than approving a second refund.
	key := signalKey{run: sig.RunID, id: sig.ID}
	if _, exists := t.st.signals[key]; exists {
		return fmt.Errorf("run %s signal %s: %w", sig.RunID, sig.ID, core.ErrSignalExists)
	}

	sig.CreatedAt = t.clock.Now()
	t.st.signals[key] = sig
	return nil
}

func (t *tx) ReleaseRun(_ context.Context, id core.RunID) error {
	r, ok := t.st.runs[id]
	if !ok {
		return fmt.Errorf("run %s: %w", id, core.ErrNotFound)
	}
	// No version bump. Releasing is not advancing: it says the run is worth
	// looking at again, and leaves deciding to a decider.
	r.AvailableAt = nil
	r.UpdatedAt = t.clock.Now()
	t.st.runs[id] = r
	return nil
}

func sortSignals(sigs []core.Signal) {
	sort.Slice(sigs, func(i, j int) bool {
		if !sigs[i].CreatedAt.Equal(sigs[j].CreatedAt) {
			return sigs[i].CreatedAt.Before(sigs[j].CreatedAt)
		}
		// A tiebreak that does not depend on map order, so two signals that
		// share a timestamp are still returned in the same order every time.
		// Without it a run could consume them in one order on the first pass
		// and the other on replay.
		return sigs[i].ID < sigs[j].ID
	})
}

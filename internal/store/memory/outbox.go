package memory

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The transactional outbox, in memory.
//
// A published row is kept, not deleted, so that "was this task ever announced,
// and when?" stays answerable after the fact. PostgreSQL does the same by
// setting published_at rather than deleting, and the two adapters have to agree
// about what a store remembers or the contract suite is describing two
// different systems.

func (s *Store) PendingDeliveries(_ context.Context, now time.Time, limit int) ([]core.Delivery, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Delivery
	for _, d := range s.st.deliveries {
		if !s.st.published[d.ID] && (d.AvailableAt.IsZero() || !d.AvailableAt.After(now)) {
			out = append(out, d)
		}
	}
	// Oldest first, by the same ordering PostgreSQL gets from its BIGSERIAL:
	// newest-first would let a busy system starve the delivery that has been
	// waiting longest, which is the one someone is most likely waiting on.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// --- transactional writes -------------------------------------------------

func (t *tx) EnqueueDelivery(_ context.Context, taskID core.TaskID, availableAt time.Time) error {
	if _, ok := t.st.tasks[taskID]; !ok {
		return fmt.Errorf("task %s: %w", taskID, core.ErrNotFound)
	}

	// The counter is shared with the Store rather than held in the transaction's
	// copy of the state, so a rolled-back transaction does not hand the next
	// delivery an ID that has already been seen. A PostgreSQL sequence behaves
	// the same way: it is not rolled back, and gaps are not a defect.
	*t.st.deliverySeq++
	id := core.DeliveryID(*t.st.deliverySeq)

	t.st.deliveries[id] = core.Delivery{
		ID:          id,
		TaskID:      taskID,
		CreatedAt:   t.clock.Now(),
		AvailableAt: availableAt,
	}
	return nil
}

func (t *tx) MarkDelivered(_ context.Context, ids []core.DeliveryID) error {
	for _, id := range ids {
		// Marking an unknown or already-marked row is how a relay that crashed
		// mid-batch recovers, so it is deliberately not an error.
		t.st.published[id] = true
	}
	return nil
}

func (t *tx) FailDelivery(_ context.Context, id core.DeliveryID, cause string) error {
	d, ok := t.st.deliveries[id]
	if !ok {
		return fmt.Errorf("delivery %d: %w", id, core.ErrNotFound)
	}
	// The row stays pending. A delivery is never given up on: a task nobody is
	// told about is a stuck run, and no attempt count makes that acceptable.
	d.Attempts++
	d.LastError = cause
	t.st.deliveries[id] = d
	return nil
}

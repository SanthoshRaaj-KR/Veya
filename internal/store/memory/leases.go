package memory

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Leases and fencing tokens, in memory.
//
// The lease row outlives the lease. Releasing sets the expiry to now rather
// than deleting the row, because the fencing token lives there and has to keep
// counting up: a token that could repeat after a release would stop
// distinguishing a new owner from an old one, which is the only thing it is
// for.

func (s *Store) ExpiredLeases(_ context.Context, now time.Time, limit int) ([]core.Lease, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []core.Lease
	for taskID, l := range s.st.leases {
		if !l.Expired(now) {
			continue
		}
		// A finished task's lease is released and so looks expired forever.
		// Handing those to the reaper would give it an ever-growing pile of
		// work that is already done.
		task, ok := s.st.tasks[taskID]
		if !ok || task.Status.IsTerminal() {
			continue
		}
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ExpiresAt.Equal(out[j].ExpiresAt) {
			return out[i].ExpiresAt.Before(out[j].ExpiresAt)
		}
		return out[i].TaskID < out[j].TaskID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// --- transactional writes -------------------------------------------------

func (t *tx) RegisterWorker(_ context.Context, w core.Worker) error {
	if w.ID == "" {
		return fmt.Errorf("worker id is empty: %w", core.ErrInvalidTransition)
	}
	now := t.clock.Now()
	existing, ok := t.st.workers[w.ID]
	if ok {
		w.RegisteredAt = existing.RegisteredAt
	} else {
		w.RegisteredAt = now
	}
	w.LastHeartbeat = &now
	t.st.workers[w.ID] = w
	return nil
}

func (t *tx) AcquireLease(_ context.Context, taskID core.TaskID, workerID string, now, expiresAt time.Time) (core.Lease, error) {
	if _, ok := t.st.tasks[taskID]; !ok {
		return core.Lease{}, fmt.Errorf("task %s: %w", taskID, core.ErrNotFound)
	}
	if _, ok := t.st.workers[workerID]; !ok {
		return core.Lease{}, fmt.Errorf("worker %s: %w", workerID, core.ErrNotFound)
	}

	token := core.FencingToken(1)
	if existing, ok := t.st.leases[taskID]; ok {
		if !existing.Expired(now) {
			return core.Lease{}, fmt.Errorf("task %s held by %s until %s: %w",
				taskID, existing.WorkerID, existing.ExpiresAt.Format(time.RFC3339), core.ErrLeaseHeld)
		}
		// Strictly higher than anything previously issued for this task.
		token = existing.Token + 1
	}

	l := core.Lease{
		TaskID:      taskID,
		WorkerID:    workerID,
		Token:       token,
		AcquiredAt:  now,
		ExpiresAt:   expiresAt,
		HeartbeatAt: now,
	}
	t.st.leases[taskID] = l
	return l, nil
}

func (t *tx) GetLease(_ context.Context, taskID core.TaskID) (core.Lease, error) {
	l, ok := t.st.leases[taskID]
	if !ok {
		return core.Lease{}, fmt.Errorf("lease for task %s: %w", taskID, core.ErrNotFound)
	}
	return l, nil
}

func (t *tx) ExtendLease(_ context.Context, taskID core.TaskID, token core.FencingToken, expiresAt time.Time) error {
	l, ok := t.st.leases[taskID]
	if !ok {
		return fmt.Errorf("lease for task %s: %w", taskID, core.ErrNotFound)
	}
	// A heartbeat is a mutation, so it is fenced like every other one.
	if l.Token != token {
		return fmt.Errorf("task %s is at token %d, presented %d: %w",
			taskID, l.Token, token, core.ErrFenced)
	}

	now := t.clock.Now()
	l.ExpiresAt = expiresAt
	l.HeartbeatAt = now
	t.st.leases[taskID] = l
	return nil
}

func (t *tx) ReleaseLease(_ context.Context, taskID core.TaskID, token core.FencingToken, now time.Time) error {
	l, ok := t.st.leases[taskID]
	if !ok {
		return fmt.Errorf("lease for task %s: %w", taskID, core.ErrNotFound)
	}
	if l.Token != token {
		return fmt.Errorf("task %s is at token %d, presented %d: %w",
			taskID, l.Token, token, core.ErrFenced)
	}

	// Expire it rather than delete it, so the token keeps counting.
	l.ExpiresAt = now
	t.st.leases[taskID] = l
	return nil
}

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The transactional outbox, on PostgreSQL.
//
// EnqueueDelivery is an ordinary INSERT with no special handling, and that is
// the point: it is ordinary precisely because it runs inside the same
// transaction as the task it announces. There is no code here that makes the
// guarantee — the guarantee is that this statement and CreateTask share a
// COMMIT.
//
// A published row is marked, never deleted. An operator debugging a stuck run
// asks "was this task ever announced, and when?", and a deleted row answers
// that question with silence, which reads identically to "never announced".
// Trimming old published rows belongs with the rest of retention in Layer 6.

const deliveryColumns = `outbox_id, task_id, attempts, COALESCE(last_error, ''), created_at, available_at`

func (s *Store) PendingDeliveries(ctx context.Context, now time.Time, limit int) ([]core.Delivery, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+deliveryColumns+`
		 FROM task_outbox
		 WHERE published_at IS NULL AND (available_at IS NULL OR available_at <= $1)
		 ORDER BY outbox_id
		 LIMIT $2`, now, limitOrAll(limit))
	if err != nil {
		return nil, translate("pending deliveries", err)
	}
	defer func() { _ = rows.Close() }()

	var out []core.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, translate("scan delivery", err)
		}
		out = append(out, d)
	}
	return out, translate("pending deliveries", rows.Err())
}

func scanDelivery(sc scanner) (core.Delivery, error) {
	var d core.Delivery
	var availableAt sql.NullTime
	if err := sc.Scan(&d.ID, &d.TaskID, &d.Attempts, &d.LastError, &d.CreatedAt, &availableAt); err != nil {
		return core.Delivery{}, err
	}
	if availableAt.Valid {
		d.AvailableAt = availableAt.Time
	}
	return d, nil
}

// --- transactional writes -------------------------------------------------

// EnqueueDelivery records the intent to hand a task to a worker, not before
// availableAt. The zero time is stored as NULL, which PendingDeliveries reads
// as ready now, exactly like every row written before this parameter existed.
//
// The foreign key on task_id is what turns "announce a task that does not
// exist" into ErrNotFound rather than a row the relay will trip over later.
func (t *tx) EnqueueDelivery(ctx context.Context, taskID core.TaskID, availableAt time.Time) error {
	payload, err := core.EncodeDelivery(taskID)
	if err != nil {
		return err
	}
	var at sql.NullTime
	if !availableAt.IsZero() {
		at = sql.NullTime{Time: availableAt, Valid: true}
	}
	_, err = t.tx.ExecContext(ctx,
		`INSERT INTO task_outbox (task_id, subject, payload, created_at, available_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		string(taskID), core.DeliverySubject, payload, t.clock.Now(), at)
	return translate(fmt.Sprintf("enqueue delivery for task %s", taskID), err)
}

// MarkDelivered stamps rows as published.
//
// One statement for the whole batch, and unconditional: a row that is already
// published is stamped again with a later time and nothing else changes. That
// is what makes a relay which crashed mid-batch able to simply redo the batch.
func (t *tx) MarkDelivered(ctx context.Context, ids []core.DeliveryID) error {
	if len(ids) == 0 {
		return nil
	}
	raw := make([]int64, len(ids))
	for i, id := range ids {
		raw[i] = int64(id)
	}
	_, err := t.tx.ExecContext(ctx,
		`UPDATE task_outbox SET published_at = $1 WHERE outbox_id = ANY($2)`,
		t.clock.Now(), pq.Array(raw))
	return translate("mark deliveries published", err)
}

// FailDelivery counts a failed publish. The row stays pending: a delivery is
// never given up on, because a task nobody is told about is a stuck run.
func (t *tx) FailDelivery(ctx context.Context, id core.DeliveryID, cause string) error {
	op := fmt.Sprintf("fail delivery %d", id)

	res, err := t.tx.ExecContext(ctx,
		`UPDATE task_outbox
		 SET attempts = attempts + 1, last_error = $2
		 WHERE outbox_id = $1`,
		int64(id), nullIfEmpty(cause))
	if err != nil {
		return translate(op, err)
	}
	// Unlike MarkDelivered, a miss here is worth reporting: the relay only
	// records a failure for a row it was just handed, so a missing row means
	// something else deleted it underneath.
	affected, err := res.RowsAffected()
	if err != nil {
		return translate(op, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", op, core.ErrNotFound)
	}
	return nil
}

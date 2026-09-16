package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

const leaseColumns = `task_id, worker_id, fencing_token, acquired_at, expires_at, heartbeat_at`

// ExpiredLeases is the reaper's input: tasks whose owner went silent but which
// still need doing.
//
// The join onto tasks is what keeps this bounded. Releasing a lease expires it
// rather than deleting it — the fencing token lives in that row and has to
// keep counting — so every finished task leaves behind a lease that looks
// expired forever. Without the status filter the reaper's queue would grow
// with every completed task in the database.
func (s *Store) ExpiredLeases(ctx context.Context, now time.Time, limit int) ([]core.Lease, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT l.task_id, l.worker_id, l.fencing_token, l.acquired_at, l.expires_at, l.heartbeat_at
		 FROM leases l
		 JOIN tasks t ON t.task_id = l.task_id
		 WHERE l.expires_at <= $1
		   AND t.status IN ('PENDING','RUNNING')
		 ORDER BY l.expires_at, l.task_id
		 LIMIT $2`, now, limitOrAll(limit))
	if err != nil {
		return nil, translate("expired leases", err)
	}
	defer func() { _ = rows.Close() }()

	var out []core.Lease
	for rows.Next() {
		l, err := scanLease(rows)
		if err != nil {
			return nil, translate("scan lease", err)
		}
		out = append(out, l)
	}
	return out, translate("expired leases", rows.Err())
}

func scanLease(sc scanner) (core.Lease, error) {
	var l core.Lease
	err := sc.Scan(&l.TaskID, &l.WorkerID, &l.Token, &l.AcquiredAt, &l.ExpiresAt, &l.HeartbeatAt)
	return l, err
}

// --- transactional writes -------------------------------------------------

func (t *tx) RegisterWorker(ctx context.Context, w core.Worker) error {
	if w.ID == "" {
		return fmt.Errorf("register worker: id is empty: %w", core.ErrInvalidTransition)
	}
	if w.Status == "" {
		w.Status = core.WorkerActive
	}
	if w.Type == "" {
		w.Type = "generic"
	}

	now := t.clock.Now()
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO workers (worker_id, worker_type, status, registered_at, last_heartbeat)
		 VALUES ($1, $2, $3, $4, $4)
		 ON CONFLICT (worker_id) DO UPDATE
		 SET worker_type    = EXCLUDED.worker_type,
		     status         = EXCLUDED.status,
		     last_heartbeat = EXCLUDED.last_heartbeat`,
		w.ID, w.Type, string(w.Status), now)
	return translate(fmt.Sprintf("register worker %s", w.ID), err)
}

// AcquireLease takes ownership, issuing a strictly higher fencing token.
//
// This is one statement on purpose. The three cases — no lease, an expired
// lease, a live lease — are resolved atomically by the upsert's conditional
// DO UPDATE, so two workers arriving at an expired lease at the same instant
// cannot both be told they took it over. Splitting this into a read and a
// write would reintroduce exactly the race the fencing token exists to survive.
func (t *tx) AcquireLease(ctx context.Context, taskID core.TaskID, workerID string, now, expiresAt time.Time) (core.Lease, error) {
	op := fmt.Sprintf("acquire lease on %s", taskID)

	row := t.tx.QueryRowContext(ctx,
		`INSERT INTO leases (task_id, worker_id, fencing_token, acquired_at, expires_at, heartbeat_at)
		 VALUES ($1, $2, 1, $3, $4, $3)
		 ON CONFLICT (task_id) DO UPDATE
		 SET worker_id     = EXCLUDED.worker_id,
		     fencing_token = leases.fencing_token + 1,
		     acquired_at   = EXCLUDED.acquired_at,
		     expires_at    = EXCLUDED.expires_at,
		     heartbeat_at  = EXCLUDED.heartbeat_at
		 WHERE leases.expires_at <= $3
		 RETURNING `+leaseColumns,
		string(taskID), workerID, now, expiresAt)

	l, err := scanLease(row)
	if errors.Is(err, sql.ErrNoRows) {
		// The upsert's WHERE rejected it, which can only mean a live lease.
		// Reported as ErrLeaseHeld rather than ErrNotFound, because the task
		// exists and is simply owned by somebody else right now.
		return core.Lease{}, fmt.Errorf("%s: %w", op, core.ErrLeaseHeld)
	}
	if err != nil {
		return core.Lease{}, translate(op, err)
	}
	return l, nil
}

func (t *tx) GetLease(ctx context.Context, taskID core.TaskID) (core.Lease, error) {
	row := t.tx.QueryRowContext(ctx,
		`SELECT `+leaseColumns+` FROM leases WHERE task_id = $1`, string(taskID))

	l, err := scanLease(row)
	if err != nil {
		return core.Lease{}, translate(fmt.Sprintf("get lease on %s", taskID), err)
	}
	return l, nil
}

// ExtendLease is the heartbeat, and it is fenced.
//
// A stale worker that could renew a lease it no longer owns would make the
// runtime believe live work belonged to a process that is gone, which is worse
// than the worker simply dying: the task would never be reclaimed.
func (t *tx) ExtendLease(ctx context.Context, taskID core.TaskID, token core.FencingToken, expiresAt time.Time) error {
	op := fmt.Sprintf("extend lease on %s", taskID)

	res, err := t.tx.ExecContext(ctx,
		`UPDATE leases
		 SET expires_at = $1, heartbeat_at = $2
		 WHERE task_id = $3 AND fencing_token = $4`,
		expiresAt, t.clock.Now(), string(taskID), int64(token))
	if err != nil {
		return translate(op, err)
	}
	return t.assertFenced(ctx, op, taskID, token, res)
}

func (t *tx) ReleaseLease(ctx context.Context, taskID core.TaskID, token core.FencingToken, now time.Time) error {
	op := fmt.Sprintf("release lease on %s", taskID)

	// Expired, not deleted: the fencing token lives in this row and has to
	// keep counting up across owners.
	res, err := t.tx.ExecContext(ctx,
		`UPDATE leases SET expires_at = $1 WHERE task_id = $2 AND fencing_token = $3`,
		now, string(taskID), int64(token))
	if err != nil {
		return translate(op, err)
	}
	return t.assertFenced(ctx, op, taskID, token, res)
}

// assertFenced turns "no rows updated" into the right error.
//
// A conditional update that changes nothing is ambiguous: either the lease is
// absent or the caller's token is stale, and those mean different things to a
// worker. One extra read resolves it, and it only happens on the failure path.
func (t *tx) assertFenced(ctx context.Context, op string, taskID core.TaskID, token core.FencingToken, res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return translate(op, err)
	}
	if n > 0 {
		return nil
	}

	var current core.FencingToken
	err = t.tx.QueryRowContext(ctx,
		`SELECT fencing_token FROM leases WHERE task_id = $1`, string(taskID)).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, core.ErrNotFound)
	}
	if err != nil {
		return translate(op, err)
	}
	return fmt.Errorf("%s: at token %d, presented %d: %w", op, current, token, core.ErrFenced)
}

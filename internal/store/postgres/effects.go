package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

const effectColumns = `effect_id, task_id, run_id, effect_type, effect_class,
	idempotency_key, status, request, response, external_ref, last_error,
	created_at, updated_at, committed_at`

func (s *Store) GetEffect(ctx context.Context, key core.IdempotencyKey) (core.Effect, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+effectColumns+` FROM effects WHERE idempotency_key = $1`, string(key))

	e, err := scanEffect(row)
	if err != nil {
		return core.Effect{}, translate(fmt.Sprintf("get effect %s", key), err)
	}
	return e, nil
}

func (s *Store) ListEffects(ctx context.Context, runID core.RunID) ([]core.Effect, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+effectColumns+` FROM effects WHERE run_id = $1
		 ORDER BY created_at, idempotency_key`, string(runID))
	if err != nil {
		return nil, translate("list effects", err)
	}
	return collectEffects(rows, "list effects")
}

// UnresolvedEffects drives reconciliation. It is backed by
// idx_effects_unresolved, which is partial on exactly these two statuses —
// the two that mean "the outside world may have changed and we have not
// written down whether it did".
func (s *Store) UnresolvedEffects(ctx context.Context, before time.Time, limit int) ([]core.Effect, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+effectColumns+`
		 FROM effects
		 WHERE status IN ('RUNNING','UNKNOWN') AND updated_at < $1
		 ORDER BY updated_at, idempotency_key
		 LIMIT $2`, before, limitOrAll(limit))
	if err != nil {
		return nil, translate("unresolved effects", err)
	}
	return collectEffects(rows, "unresolved effects")
}

func scanEffect(sc scanner) (core.Effect, error) {
	var (
		e                 core.Effect
		request, response []byte
		externalRef       sql.NullString
		lastErr           sql.NullString
		committed         sql.NullTime
	)
	err := sc.Scan(&e.ID, &e.TaskID, &e.RunID, &e.Type, &e.Class, &e.Key, &e.Status,
		&request, &response, &externalRef, &lastErr, &e.CreatedAt, &e.UpdatedAt, &committed)
	if err != nil {
		return core.Effect{}, err
	}
	e.Request = rawOrNil(request)
	e.Response = rawOrNil(response)
	e.ExternalRef = externalRef.String
	e.LastError = lastErr.String
	if committed.Valid {
		ts := committed.Time
		e.CommittedAt = &ts
	}
	return e, nil
}

func collectEffects(rows *sql.Rows, op string) ([]core.Effect, error) {
	defer func() { _ = rows.Close() }()

	var out []core.Effect
	for rows.Next() {
		e, err := scanEffect(rows)
		if err != nil {
			return nil, translate("scan effect", err)
		}
		out = append(out, e)
	}
	return out, translate(op, rows.Err())
}

// --- transactional writes -------------------------------------------------

// ReserveEffect is one INSERT and deliberately nothing else.
//
// There is no "does this key already exist?" read before it, because that read
// and this write would not be atomic, and the window between them is precisely
// where a second worker gets through. The unique constraint on
// idempotency_key does the excluding; the error it raises is translated to
// ErrEffectExists, which tells the caller to load the existing row rather than
// to give up.
func (t *tx) ReserveEffect(ctx context.Context, e core.Effect) error {
	if !e.Status.Valid() {
		return fmt.Errorf("reserve effect %s: status %q: %w", e.Key, e.Status, core.ErrInvalidTransition)
	}
	if !e.Class.Valid() {
		return fmt.Errorf("reserve effect %s: class %q: %w", e.Key, e.Class, core.ErrInvalidTransition)
	}

	now := t.clock.Now()
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO effects (effect_id, task_id, run_id, effect_type, effect_class,
		                      idempotency_key, status, request, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
		string(e.ID), string(e.TaskID), string(e.RunID), e.Type, string(e.Class),
		string(e.Key), string(e.Status), jsonOrNull(e.Request), now)
	return translate(fmt.Sprintf("reserve effect %s", e.Key), err)
}

func (t *tx) GetEffect(ctx context.Context, key core.IdempotencyKey) (core.Effect, error) {
	row := t.tx.QueryRowContext(ctx,
		`SELECT `+effectColumns+` FROM effects WHERE idempotency_key = $1`, string(key))

	e, err := scanEffect(row)
	if err != nil {
		return core.Effect{}, translate(fmt.Sprintf("get effect %s", key), err)
	}
	return e, nil
}

// TransitionEffect moves an effect between statuses, conditional on its
// current one. The row is locked first so that the status check and the write
// cannot be separated by another transaction.
func (t *tx) TransitionEffect(ctx context.Context, key core.IdempotencyKey, from, to core.EffectStatus, out core.EffectOutcome) error {
	op := fmt.Sprintf("transition effect %s", key)

	var current core.EffectStatus
	err := t.tx.QueryRowContext(ctx,
		`SELECT status FROM effects WHERE idempotency_key = $1 FOR UPDATE`,
		string(key)).Scan(&current)
	if err != nil {
		return translate(op, err)
	}

	if current != from {
		return fmt.Errorf("%s: is %s, expected %s: %w", op, current, from, core.ErrConflict)
	}
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("%s: %s -> %s: %w", op, from, to, core.ErrInvalidTransition)
	}

	now := t.clock.Now()
	var committedAt any
	if to == core.EffectCommitted {
		committedAt = now
	}

	_, err = t.tx.ExecContext(ctx,
		`UPDATE effects
		 SET status = $1,
		     response     = COALESCE($2, response),
		     external_ref = COALESCE($3, external_ref),
		     last_error   = COALESCE($4, last_error),
		     updated_at   = $5,
		     committed_at = COALESCE($6, committed_at)
		 WHERE idempotency_key = $7 AND status = $8`,
		string(to), jsonOrNull(out.Response), nullIfEmpty(out.ExternalRef),
		nullIfEmpty(out.Error), now, committedAt, string(key), string(from))
	return translate(op, err)
}

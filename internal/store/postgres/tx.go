package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/santhoshraajkr/veya/internal/core"
)

// tx is the transactional write surface. It satisfies core.Tx without exposing
// *sql.Tx, so nothing upstream can reach the driver.
type tx struct {
	tx    *sql.Tx
	clock core.Clock
}

func (t *tx) GetTask(ctx context.Context, id core.TaskID) (core.Task, error) {
	row := t.tx.QueryRowContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE task_id = $1`, string(id))

	task, err := scanTask(row)
	if err != nil {
		return core.Task{}, translate(fmt.Sprintf("get task %s", id), err)
	}
	return task, nil
}

func (t *tx) CreateRun(ctx context.Context, r core.Run) error {
	if !r.Status.Valid() {
		return fmt.Errorf("create run %s: status %q: %w", r.ID, r.Status, core.ErrInvalidTransition)
	}
	now := t.clock.Now()
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO runs (run_id, agent_name, agent_version, status, version,
		                   input, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		string(r.ID), r.AgentName, r.AgentVersion, string(r.Status), r.Version,
		jsonOrNull(r.Input), now)
	return translate(fmt.Sprintf("create run %s", r.ID), err)
}

// AdvanceRun is the compare half of compare-and-swap.
//
// The row is locked, then the caller's expected version is checked, then the
// transition is validated, then the write applies. Reading first costs a round
// trip but buys precise errors: a caller needs to distinguish "the run moved
// on" (re-read and carry on) from "that transition is illegal" (a logic bug),
// and a bare UPDATE ... WHERE version = $n reports zero rows for both.
func (t *tx) AdvanceRun(ctx context.Context, id core.RunID, expectedVersion int64, next core.RunState) error {
	op := fmt.Sprintf("advance run %s", id)

	var (
		current core.RunStatus
		version int64
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT status, version FROM runs WHERE run_id = $1 FOR UPDATE`,
		string(id)).Scan(&current, &version)
	if err != nil {
		return translate(op, err)
	}

	if version != expectedVersion {
		return fmt.Errorf("%s: at version %d, expected %d: %w",
			op, version, expectedVersion, core.ErrConflict)
	}
	if next.Status != current && !current.CanTransitionTo(next.Status) {
		return fmt.Errorf("%s: %s -> %s: %w", op, current, next.Status, core.ErrInvalidTransition)
	}

	now := t.clock.Now()
	var completedAt any
	if next.Status.IsTerminal() {
		completedAt = now
	}

	_, err = t.tx.ExecContext(ctx,
		`UPDATE runs
		 SET status = $1, output = $2, last_error = $3,
		     version = version + 1, updated_at = $4, completed_at = $5
		 WHERE run_id = $6 AND version = $7`,
		string(next.Status), jsonOrNull(next.Output), nullIfEmpty(next.LastError),
		now, completedAt, string(id), expectedVersion)
	return translate(op, err)
}

// CreateTask inserts a task. UNIQUE (run_id, step_id) is what makes creation
// idempotent, so a duplicate surfaces as ErrTaskExists rather than a second row.
func (t *tx) CreateTask(ctx context.Context, task core.Task) error {
	if !task.Status.Valid() {
		return fmt.Errorf("create task %s: status %q: %w", task.ID, task.Status, core.ErrInvalidTransition)
	}
	now := t.clock.Now()
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO tasks (task_id, run_id, step_id, task_type, payload, status,
		                    attempt, max_attempts, available_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $9)`,
		string(task.ID), string(task.RunID), string(task.StepID), task.Type,
		jsonOrEmptyObject(task.Payload), string(task.Status),
		task.Attempt, task.MaxAttempts, now)
	return translate(fmt.Sprintf("create task %s", task.ID), err)
}

// TransitionTask moves a task between statuses, conditional on it currently
// being in from.
//
// PENDING -> RUNNING is the claim. Two workers handed the same task both run
// this; the row lock serializes them and exactly one sees PENDING, so the
// other gets ErrConflict and drops the delivery. This holds with no lease and
// no fencing token, which is why at-least-once delivery is safe.
func (t *tx) TransitionTask(ctx context.Context, id core.TaskID, from, to core.TaskStatus, out core.TaskOutcome) error {
	op := fmt.Sprintf("transition task %s", id)

	var current core.TaskStatus
	err := t.tx.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = $1 FOR UPDATE`, string(id)).Scan(&current)
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
	var completedAt any
	if to.IsTerminal() {
		completedAt = now
	}
	// A claim consumes an attempt; every other transition leaves the count alone.
	attemptDelta := 0
	if to == core.TaskRunning {
		attemptDelta = 1
	}

	_, err = t.tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = $1,
		     attempt = attempt + $2,
		     last_error = COALESCE($3, last_error),
		     updated_at = $4,
		     completed_at = $5
		 WHERE task_id = $6 AND status = $7`,
		string(to), attemptDelta, nullIfEmpty(out.Error), now, completedAt,
		string(id), string(from))
	return translate(op, err)
}

func (t *tx) NextSeq(ctx context.Context, runID core.RunID) (int64, error) {
	op := fmt.Sprintf("next seq for run %s", runID)

	// Existence is checked separately so that a missing run reports
	// ErrNotFound rather than silently returning 1.
	var exists bool
	if err := t.tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1)`, string(runID)).Scan(&exists); err != nil {
		return 0, translate(op, err)
	}
	if !exists {
		return 0, fmt.Errorf("%s: %w", op, core.ErrNotFound)
	}

	var next int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE run_id = $1`,
		string(runID)).Scan(&next)
	if err != nil {
		return 0, translate(op, err)
	}
	return next, nil
}

// AppendEvent writes one event. PRIMARY KEY (run_id, seq) makes the append
// conditional, so a racing retry collides instead of duplicating history.
func (t *tx) AppendEvent(ctx context.Context, e core.Event) error {
	now := t.clock.Now()
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO events (run_id, seq, event_type, step_id, payload, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		string(e.RunID), e.Seq, string(e.Type), nullIfEmpty(string(e.StepID)),
		jsonOrEmptyObject(e.Payload), now)
	return translate(fmt.Sprintf("append event %s seq %d", e.RunID, e.Seq), err)
}

// --- parameter helpers ----------------------------------------------------

// jsonOrNull sends NULL for an absent document rather than the four bytes
// "null", so that "no output" and "an output that is JSON null" stay distinct.
func jsonOrNull(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return []byte(raw)
}

// jsonOrEmptyObject is for NOT NULL JSONB columns.
func jsonOrEmptyObject(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return []byte(`{}`)
	}
	return raw
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

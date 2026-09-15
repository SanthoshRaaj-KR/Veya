// Package postgres implements core.Store against PostgreSQL.
//
// This is the authoritative store: runs, events, tasks, and from Layer 2 the
// effect ledger and leases. NATS JetStream carries work; nothing durable lives
// there. Every guarantee the runtime makes is a guarantee about what commits
// together, which is why all writes go through RunInTx.
//
// # Boundary
//
// database/sql and lib/pq appear here and in no other package. Driver errors
// are translated into core sentinels in errors.go before they leave. If a
// *pq.Error or a *sql.Tx ever escapes, the port has leaked and the contract
// suite's claim that adapters are interchangeable is no longer true.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver

	"github.com/santhoshraajkr/veya/internal/core"
)

// Store is the PostgreSQL-backed core.Store.
type Store struct {
	db    *sql.DB
	clock core.Clock
}

// Open connects to PostgreSQL and verifies the connection.
func Open(ctx context.Context, dsn string, clk core.Clock) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	// Conservative defaults. The runtime holds a connection for the length of
	// a transaction and no longer, so a small pool is sufficient; Layer 7's
	// benchmarks are what should change these numbers, not intuition.
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: db, clock: clk}, nil
}

// New wraps an existing pool. Used by tests that manage their own database.
func New(db *sql.DB, clk core.Clock) *Store {
	return &Store{db: db, clock: clk}
}

// DB exposes the pool so the composition root can run migrations.
//
// This is the one sanctioned leak of database/sql out of this package, and it
// is limited to cmd/. Nothing in engine/ or core/ may call it.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error { return s.db.Close() }

// RunInTx runs fn in a transaction, committing on nil and rolling back
// otherwise.
func (s *Store) RunInTx(ctx context.Context, fn func(context.Context, core.Tx) error) error {
	sqlTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return translate("begin", err)
	}
	// Rollback after a successful commit is a no-op, so this is safe
	// unconditionally and guarantees no transaction is ever leaked.
	defer func() { _ = sqlTx.Rollback() }()

	if err := fn(ctx, &tx{tx: sqlTx, clock: s.clock}); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return translate("commit", err)
	}
	return nil
}

// --- reads ----------------------------------------------------------------

const runColumns = `run_id, agent_name, agent_version, status, version,
	input, output, last_error, created_at, updated_at, completed_at`

const taskColumns = `task_id, run_id, step_id, task_type, payload, status,
	attempt, max_attempts, last_error, created_at, updated_at, completed_at`

func (s *Store) GetRun(ctx context.Context, id core.RunID) (core.Run, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+runColumns+` FROM runs WHERE run_id = $1`, string(id))

	r, err := scanRun(row)
	if err != nil {
		return core.Run{}, translate(fmt.Sprintf("get run %s", id), err)
	}
	return r, nil
}

func (s *Store) GetTask(ctx context.Context, id core.TaskID) (core.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE task_id = $1`, string(id))

	t, err := scanTask(row)
	if err != nil {
		return core.Task{}, translate(fmt.Sprintf("get task %s", id), err)
	}
	return t, nil
}

func (s *Store) ListTasks(ctx context.Context, runID core.RunID) ([]core.Task, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE run_id = $1 ORDER BY created_at, task_id`,
		string(runID))
	if err != nil {
		return nil, translate("list tasks", err)
	}
	return collectTasks(rows, "list tasks")
}

func (s *Store) History(ctx context.Context, runID core.RunID) ([]core.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT run_id, seq, event_type, step_id, payload, created_at
		 FROM events WHERE run_id = $1 ORDER BY seq`, string(runID))
	if err != nil {
		return nil, translate("history", err)
	}
	defer func() { _ = rows.Close() }()

	var out []core.Event
	for rows.Next() {
		var (
			e       core.Event
			step    sql.NullString
			payload []byte
		)
		if err := rows.Scan(&e.RunID, &e.Seq, &e.Type, &step, &payload, &e.CreatedAt); err != nil {
			return nil, translate("scan event", err)
		}
		e.StepID = core.StepID(step.String)
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, translate("history", rows.Err())
}

// PendingTasks returns dispatchable work oldest first. It is how a restarted
// process rediscovers tasks that were committed but never delivered.
func (s *Store) PendingTasks(ctx context.Context, limit int) ([]core.Task, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+taskColumns+`
		 FROM tasks
		 WHERE status = 'PENDING' AND available_at <= $1
		 ORDER BY priority DESC, created_at, task_id
		 LIMIT $2`,
		s.clock.Now(), limitOrAll(limit))
	if err != nil {
		return nil, translate("pending tasks", err)
	}
	return collectTasks(rows, "pending tasks")
}

// RunsAwaitingAdvance returns runs that are RUNNING with nothing in flight,
// meaning the next decision is owed. A run started by the CLI and advanced by
// the runtime process is found this way.
func (s *Store) RunsAwaitingAdvance(ctx context.Context, limit int) ([]core.RunID, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT r.run_id
		 FROM runs r
		 WHERE r.status = 'RUNNING'
		   AND NOT EXISTS (
		       SELECT 1 FROM tasks t
		       WHERE t.run_id = r.run_id AND t.status IN ('PENDING','RUNNING')
		   )
		 ORDER BY r.run_id
		 LIMIT $1`, limitOrAll(limit))
	if err != nil {
		return nil, translate("runs awaiting advance", err)
	}
	defer func() { _ = rows.Close() }()

	var out []core.RunID
	for rows.Next() {
		var id core.RunID
		if err := rows.Scan(&id); err != nil {
			return nil, translate("scan run id", err)
		}
		out = append(out, id)
	}
	return out, translate("runs awaiting advance", rows.Err())
}

// --- scanning -------------------------------------------------------------

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface{ Scan(dest ...any) error }

func scanRun(sc scanner) (core.Run, error) {
	var (
		r             core.Run
		input, output []byte
		lastErr       sql.NullString
		completed     sql.NullTime
	)
	err := sc.Scan(&r.ID, &r.AgentName, &r.AgentVersion, &r.Status, &r.Version,
		&input, &output, &lastErr, &r.CreatedAt, &r.UpdatedAt, &completed)
	if err != nil {
		return core.Run{}, err
	}
	r.Input = rawOrNil(input)
	r.Output = rawOrNil(output)
	r.LastError = lastErr.String
	if completed.Valid {
		t := completed.Time
		r.CompletedAt = &t
	}
	return r, nil
}

func scanTask(sc scanner) (core.Task, error) {
	var (
		t         core.Task
		payload   []byte
		lastErr   sql.NullString
		completed sql.NullTime
	)
	err := sc.Scan(&t.ID, &t.RunID, &t.StepID, &t.Type, &payload, &t.Status,
		&t.Attempt, &t.MaxAttempts, &lastErr, &t.CreatedAt, &t.UpdatedAt, &completed)
	if err != nil {
		return core.Task{}, err
	}
	t.Payload = rawOrNil(payload)
	t.LastError = lastErr.String
	if completed.Valid {
		ts := completed.Time
		t.CompletedAt = &ts
	}
	return t, nil
}

func collectTasks(rows *sql.Rows, op string) ([]core.Task, error) {
	defer func() { _ = rows.Close() }()

	var out []core.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, translate("scan task", err)
		}
		out = append(out, t)
	}
	return out, translate(op, rows.Err())
}

func rawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

// limitOrAll turns a non-positive limit into a very large one, so callers can
// ask for "everything" without the query needing a second form.
func limitOrAll(limit int) int {
	if limit <= 0 {
		return 1 << 30
	}
	return limit
}

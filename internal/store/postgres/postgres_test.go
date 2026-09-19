//go:build integration

// Build-tagged so that `make test` needs no Docker. `make test-integration`
// sets VEYA_TEST_DSN and runs this against a live database.
package postgres_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/core/storetest"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/migrations"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/pgtest"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
)

// TestStoreContract runs the identical suite the memory adapter runs. That the
// file below contains no PostgreSQL-specific assertions is the point: if the
// adapters were distinguishable from above, the port would be leaking.
func TestStoreContract(t *testing.T) {
	ctx := context.Background()

	// A scratch database rather than the shared one. The suite truncates every
	// table between subtests, so on a shared database any other connection —
	// a veya-runtime left running in another terminal, or a sibling test
	// package — deadlocks it against a lock that is never released. Nobody
	// else knows this database's name, so nobody else can be holding it.
	db := pgtest.Scratch(t)
	dsn := pgtest.ReplaceDB(pgtest.RequireDSN(t), currentDatabase(t, db))

	storetest.RunStoreSuite(t, func(t *testing.T) core.Store {
		pgtest.TruncateAll(t, db)
		s, err := postgres.Open(ctx, dsn, storetest.TickingClock())
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return s
	})
}

// currentDatabase asks the connection what it is connected to, so the DSN
// handed to postgres.Open cannot drift from the database pgtest created.
func currentDatabase(t *testing.T, db *sql.DB) string {
	t.Helper()

	var name string
	if err := db.QueryRowContext(context.Background(), "SELECT current_database()").Scan(&name); err != nil {
		t.Fatalf("current_database: %v", err)
	}
	return name
}

// TestMigrationsAreIdempotent covers the Phase 1 exit criterion that applying
// twice is a no-op.
func TestMigrationsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	db := pgtest.Scratch(t) // already migrated once by Scratch

	ran, err := migrations.Apply(ctx, db)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(ran) != 0 {
		t.Fatalf("second apply ran %v, want nothing", ran)
	}

	pending, err := migrations.Pending(ctx, db)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %v, want empty after apply", pending)
	}
}

// TestEffectsResistRunDeletion pins a schema property the contract suite
// cannot express, because core.Store has no delete and deliberately never will.
//
// Effect foreign keys are RESTRICT, never CASCADE. An effect record erased
// while a redelivery is still possible is a side effect executed twice, so an
// attempt to delete a run that still has effects must fail loudly rather than
// quietly take the duplicate-prevention ledger with it. This is the kind of
// constraint that gets "tidied up" by a later migration, so it gets a test.
func TestEffectsResistRunDeletion(t *testing.T) {
	ctx := context.Background()
	db := pgtest.Scratch(t)

	seed := []string{
		`INSERT INTO runs (run_id, agent_name, agent_version, status)
		 VALUES ('run-fk', 'a', 'v1', 'RUNNING')`,
		`INSERT INTO tasks (task_id, run_id, step_id, task_type, payload, status)
		 VALUES ('task-fk', 'run-fk', 'S1', 'send_email', '{}', 'RUNNING')`,
		`INSERT INTO effects (effect_id, task_id, run_id, effect_type, effect_class,
		                      idempotency_key, status)
		 VALUES ('eff-fk', 'task-fk', 'run-fk', 'send_email', 'IDEMPOTENT_BY_KEY',
		         'run-fk:S1:E1', 'COMMITTED')`,
	}
	for _, stmt := range seed {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM runs WHERE run_id = 'run-fk'`); err == nil {
		t.Fatal("deleting a run with effects succeeded; the FK must be RESTRICT, never CASCADE")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM tasks WHERE task_id = 'task-fk'`); err == nil {
		t.Fatal("deleting a task with effects succeeded; the FK must be RESTRICT, never CASCADE")
	}

	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM effects WHERE idempotency_key = 'run-fk:S1:E1'`).Scan(&count); err != nil {
		t.Fatalf("count effects: %v", err)
	}
	if count != 1 {
		t.Fatalf("effect row count = %d, want 1: the ledger must have survived", count)
	}
}

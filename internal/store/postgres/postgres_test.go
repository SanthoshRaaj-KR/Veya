//go:build integration

// Build-tagged so that `make test` needs no Docker. `make test-integration`
// sets VEYA_TEST_DSN and runs this against a live database.
package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/santhoshraajkr/veya/internal/core"
	"github.com/santhoshraajkr/veya/internal/core/storetest"
	"github.com/santhoshraajkr/veya/internal/store/migrations"
	"github.com/santhoshraajkr/veya/internal/store/postgres"
)

// TestStoreContract runs the identical suite the memory adapter runs. That the
// file below contains no PostgreSQL-specific assertions is the point: if the
// adapters were distinguishable from above, the port would be leaking.
func TestStoreContract(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %v", dsn, err)
	}
	if _, err := migrations.Apply(ctx, admin); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	storetest.RunStoreSuite(t, func(t *testing.T) core.Store {
		truncateAll(t, admin)
		s, err := postgres.Open(ctx, dsn, storetest.TickingClock())
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return s
	})
}

// TestMigrationsAreIdempotent covers the Phase 1 exit criterion that applying
// twice is a no-op.
func TestMigrationsAreIdempotent(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := migrations.Apply(ctx, db); err != nil {
		t.Fatalf("first apply: %v", err)
	}
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

func requireDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("VEYA_TEST_DSN")
	if dsn == "" {
		t.Skip("VEYA_TEST_DSN not set; run `make up` then `make test-integration`")
	}
	return dsn
}

// truncateAll gives each subtest an empty database. RESTART IDENTITY resets
// the outbox sequence so that identifiers do not drift between subtests.
func truncateAll(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`TRUNCATE leases, task_outbox, effects, tasks, events, runs, workers
		 RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

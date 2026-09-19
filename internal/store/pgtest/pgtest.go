// Package pgtest gives an integration test its own PostgreSQL database.
//
// It exists because sharing one database between tests is not merely untidy.
// The store contract suite truncates every table between subtests, so anything
// else holding a connection — a veya-runtime left running in another terminal,
// or a sibling test package — deadlocks it against a lock it will never get.
// The failure surfaces as a hang with no explanation of what is waiting on
// what, which is the worst kind of test failure to debug.
//
// A scratch database nobody else knows the name of cannot have that problem.
// internal/dispatch/postgres arrived at the same conclusion independently; this
// package is that pattern, named and shared, so the next adapter does not have
// to rediscover it a third time.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/SanthoshRaaj-KR/Veya/internal/store/migrations"
)

// DSNEnv names the environment variable holding the administrative DSN. It
// points at a database that exists; the scratch databases are created beside
// it.
const DSNEnv = "VEYA_TEST_DSN"

// RequireDSN returns the administrative DSN, skipping the test when it is
// unset.
//
// Skipping rather than failing is what lets `make test` run with no Docker at
// all: an integration test with nothing to integrate against has not failed,
// it has not run.
func RequireDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		t.Skipf("%s is not set; run `make up` then `make test-integration`", DSNEnv)
	}
	return dsn
}

// Scratch creates an empty, migrated database for this test and returns a
// connection to it.
//
// The database is dropped when the test finishes, WITH (FORCE) so that a
// connection leaked by a failing test cannot keep the corpse alive and leave
// the next run to trip over it.
func Scratch(t *testing.T) *sql.DB {
	t.Helper()

	admin := RequireDSN(t)
	name := scratchName(t)

	if err := exec(admin, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("pgtest: create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		// Best effort. A leaked scratch database is noise; failing the test
		// over it would turn a clean-up problem into a false red.
		_ = exec(admin, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	db, err := sql.Open("postgres", ReplaceDB(admin, name))
	if err != nil {
		t.Fatalf("pgtest: open %s: %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatalf("pgtest: migrate %s: %v", name, err)
	}
	return db
}

// scratchName derives a database name that is unique and says which test owns
// it, so a leaked one can be traced back to its source.
func scratchName(t *testing.T) string {
	t.Helper()

	var b strings.Builder
	b.WriteString("veya_t_")
	for _, r := range strings.ToLower(t.Name()) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name := b.String()
	// PostgreSQL truncates identifiers at 63 bytes, and a truncated name could
	// collide with another test's. Cutting it short ourselves leaves room for
	// the suffix that makes it unique.
	if len(name) > 40 {
		name = name[:40]
	}
	return fmt.Sprintf("%s_%d", name, time.Now().UnixNano())
}

// ReplaceDB swaps the database name in a DSN, leaving credentials and query
// parameters alone.
func ReplaceDB(dsn, name string) string {
	slash := strings.LastIndex(dsn, "/")
	if slash < 0 {
		return dsn
	}
	tail := ""
	if q := strings.Index(dsn[slash:], "?"); q >= 0 {
		tail = dsn[slash+q:]
	}
	return dsn[:slash+1] + name + tail
}

// TruncateAll empties every table, leaving the schema in place.
//
// RESTART IDENTITY resets the outbox sequence too, so identifiers do not drift
// between subtests sharing one scratch database and a test cannot accidentally
// depend on the sequence value a previous one left behind.
func TruncateAll(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.ExecContext(context.Background(),
		`TRUNCATE leases, task_outbox, effects, tasks, events, runs, workers
		 RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("pgtest: truncate: %v", err)
	}
}

// exec opens a short-lived administrative connection and runs one statement.
// CREATE DATABASE and DROP DATABASE cannot run inside a transaction or against
// the database being dropped, so they get their own connection.
func exec(dsn, stmt string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(context.Background(), stmt)
	return err
}

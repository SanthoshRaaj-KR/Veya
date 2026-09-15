// Package migrations holds the SQL schema and applies it.
//
// The runner is deliberately small — read embedded .sql files in name order,
// skip the ones already recorded, apply the rest, each in its own transaction.
// That is the entire requirement, and an external migration tool would be a
// dependency and a second thing to install for it.
//
// # Rules
//
// Forward-only. Never edit a migration that has been applied anywhere: change
// the schema by adding 000N_description.sql. The file name is the version, so
// names must sort in application order.
//
// Each file runs inside one transaction, so a migration that fails part way
// leaves the schema untouched and can be fixed and re-run. PostgreSQL supports
// transactional DDL, which is what makes that possible.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

// versionTable records what has been applied. Created on first run.
const versionTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT        PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

// Migration is one schema change.
type Migration struct {
	Version string // the file name, e.g. "0001_init.sql"
	SQL     string
}

// All returns every embedded migration in application order.
func All() ([]Migration, error) {
	entries, err := fs.Glob(files, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	sort.Strings(entries)

	out := make([]Migration, 0, len(entries))
	for _, name := range entries {
		body, err := files.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		out = append(out, Migration{Version: name, SQL: string(body)})
	}
	return out, nil
}

// Apply brings the database up to date and reports which migrations it ran.
//
// Applying twice is a no-op: the second call finds every version recorded and
// applies nothing.
func Apply(ctx context.Context, db *sql.DB) ([]string, error) {
	if _, err := db.ExecContext(ctx, versionTable); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedVersions(ctx, db)
	if err != nil {
		return nil, err
	}

	all, err := All()
	if err != nil {
		return nil, err
	}

	var ran []string
	for _, m := range all {
		if applied[m.Version] {
			continue
		}
		if err := applyOne(ctx, db, m); err != nil {
			return ran, err
		}
		ran = append(ran, m.Version)
	}
	return ran, nil
}

func applyOne(ctx context.Context, db *sql.DB, m Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s: %w", m.Version, err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
		return fmt.Errorf("apply %s: %w", m.Version, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`, m.Version); err != nil {
		return fmt.Errorf("record %s: %w", m.Version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", m.Version, err)
	}
	return nil
}

func appliedVersions(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		out[v] = true
	}
	return out, rows.Err()
}

// Pending reports which migrations would run, without applying them.
func Pending(ctx context.Context, db *sql.DB) ([]string, error) {
	if _, err := db.ExecContext(ctx, versionTable); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}
	applied, err := appliedVersions(ctx, db)
	if err != nil {
		return nil, err
	}
	all, err := All()
	if err != nil {
		return nil, err
	}

	var out []string
	for _, m := range all {
		if !applied[m.Version] {
			out = append(out, m.Version)
		}
	}
	return out, nil
}

// Reset drops everything this schema owns. Development only — it is what
// `make down && make up` would do, without the container restart.
func Reset(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`DROP TABLE IF EXISTS leases, workers, task_outbox, effects, tasks, events, runs, schema_migrations CASCADE`,
		`DROP TYPE IF EXISTS worker_status, effect_status, task_status, run_status`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("reset (%s): %w", strings.SplitN(s, " ", 3)[1], err)
		}
	}
	return nil
}

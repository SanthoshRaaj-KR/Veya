package postgres

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"github.com/santhoshraajkr/veya/internal/core"
)

// This file is the only place in the codebase that knows what a pq.Error is.
//
// Every driver error is translated into a core sentinel here, so that upstream
// code cannot tell a PostgreSQL unique violation from an in-memory map
// collision. That indistinguishability is the whole claim the contract suite
// makes; a raw driver error escaping this package would quietly falsify it.

// PostgreSQL error codes, from the SQLSTATE appendix.
const (
	codeUniqueViolation     = "23505"
	codeForeignKeyViolation = "23503"
	codeSerializationFail   = "40001"
	codeDeadlockDetected    = "40P01"
)

// Constraint names, assigned by PostgreSQL from the table and column names in
// migration 0001. They are load-bearing: which constraint was violated is what
// distinguishes "this step already has a task" from "this sequence number is
// taken", and the two mean different things to the caller.
const (
	constraintTaskStep  = "tasks_run_id_step_id_key" // UNIQUE (run_id, step_id)
	constraintEventSeq  = "events_pkey"              // PRIMARY KEY (run_id, seq)
	constraintRunPK     = "runs_pkey"
	constraintTaskPK    = "tasks_pkey"
	constraintEffectKey = "effects_idempotency_key_key" // Layer 2
)

// translate maps a driver error onto a core sentinel, wrapping it with context.
func translate(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, core.ErrNotFound)
	}

	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return fmt.Errorf("%s: %w", op, err)
	}

	switch string(pqErr.Code) {
	case codeUniqueViolation:
		switch pqErr.Constraint {
		case constraintTaskStep:
			return fmt.Errorf("%s: %w", op, core.ErrTaskExists)
		case constraintEventSeq:
			return fmt.Errorf("%s: %w", op, core.ErrSeqConflict)
		case constraintRunPK, constraintTaskPK, constraintEffectKey:
			return fmt.Errorf("%s: %w", op, core.ErrConflict)
		default:
			return fmt.Errorf("%s (constraint %s): %w", op, pqErr.Constraint, core.ErrConflict)
		}

	case codeForeignKeyViolation:
		// The only foreign keys in this schema point at a parent row that the
		// caller believed existed, so a violation means it does not.
		return fmt.Errorf("%s (constraint %s): %w", op, pqErr.Constraint, core.ErrNotFound)

	case codeSerializationFail, codeDeadlockDetected:
		// Both mean "you lost a race; read again and retry", which is exactly
		// what ErrConflict tells the caller to do.
		return fmt.Errorf("%s: %w", op, core.ErrConflict)
	}

	return fmt.Errorf("%s: %w", op, err)
}

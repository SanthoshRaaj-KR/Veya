package postgres

import (
	"context"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

const signalColumns = `run_id, signal_id, name, payload, created_at`

// Signals returns every signal recorded against a run under a name, oldest
// first.
//
// Consumed signals are included, because which ones a run has taken is
// recorded in its history. A `consumed` column here would be a second copy of
// that fact, and a run whose history says it took a signal the table calls
// unconsumed is a run that takes it twice on the next replay.
func (s *Store) Signals(ctx context.Context, runID core.RunID, name string) ([]core.Signal, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+signalColumns+`
		 FROM signals
		 WHERE run_id = $1 AND name = $2
		 ORDER BY created_at, signal_id`, string(runID), name)
	if err != nil {
		return nil, translate("list signals", err)
	}
	defer func() { _ = rows.Close() }()

	var out []core.Signal
	for rows.Next() {
		var (
			sig     core.Signal
			payload []byte
		)
		if err := rows.Scan(&sig.RunID, &sig.ID, &sig.Name, &payload, &sig.CreatedAt); err != nil {
			return nil, translate("scan signal", err)
		}
		sig.Payload = rawOrNil(payload)
		out = append(out, sig)
	}
	return out, translate("list signals", rows.Err())
}

// RecordSignal inserts one arrival.
//
// One conditional insert, never a read followed by a write: the gap between
// those two is where a retried callback slips through, and the primary key is
// the only thing that closes it.
func (t *tx) RecordSignal(ctx context.Context, sig core.Signal) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO signals (run_id, signal_id, name, payload, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		string(sig.RunID), string(sig.ID), sig.Name,
		jsonOrNull(sig.Payload), t.clock.Now())
	return translate("record signal", err)
}

// ReleaseRun clears a park without advancing the run.
//
// No version bump, deliberately. The sender of a signal is not a decider: all
// it is entitled to say is that the run is worth looking at again. Leaving the
// version alone also keeps it out of the compare-and-swap, so it can neither
// lose a race with whoever is deciding nor make that decider lose one.
func (t *tx) ReleaseRun(ctx context.Context, id core.RunID) error {
	res, err := t.tx.ExecContext(ctx,
		`UPDATE runs SET available_at = NULL, updated_at = $1 WHERE run_id = $2`,
		t.clock.Now(), string(id))
	if err != nil {
		return translate("release run", err)
	}
	// An UPDATE that matched nothing is a run that does not exist. Reporting
	// it as success would let a signal be recorded against a typo.
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return translate("release run", core.ErrNotFound)
	}
	return nil
}

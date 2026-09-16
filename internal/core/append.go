package core

import "context"

// Append reads the next sequence number and writes one event, inside tx.
//
// Reading the sequence and using it in the same transaction is what makes the
// conditional append on (run_id, seq) a real check rather than a formality: a
// racing writer that read the same number loses at commit, so history stays
// gapless without anyone holding a lock across the gap.
//
// It lives in core because every package that records a fact needs it, and a
// second copy of this four-line function is a second chance to get the
// ordering wrong.
func Append(ctx context.Context, tx Tx, runID RunID, typ EventType, step StepID, data any) error {
	seq, err := tx.NextSeq(ctx, runID)
	if err != nil {
		return err
	}
	ev, err := NewEvent(runID, seq, typ, step, data)
	if err != nil {
		return err
	}
	return tx.AppendEvent(ctx, ev)
}

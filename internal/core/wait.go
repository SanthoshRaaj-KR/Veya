package core

import "time"

// WaitKind is what a parked run is waiting for.
type WaitKind string

const (
	// WaitTimer is a run sleeping until a wall-clock instant.
	WaitTimer WaitKind = "TIMER"
)

// Wait is one suspension, read back out of history.
type Wait struct {
	Kind   WaitKind
	StepID StepID

	// Until is when the wait may end on its own: the wake instant for a
	// timer. Indefinite when nothing is scheduled.
	Until time.Time
}

// PendingWait returns the run's unresolved suspension, if it has one.
//
// It is a pure function over history, which is the point. The engine needs to
// know whether a run is mid-wait before it asks the decider anything, and the
// alternative — a column saying what the run is waiting for — would be a
// second copy of a fact history already holds, free to drift from it. History
// is the authority here for the same reason it is the authority for replay.
//
// At most one wait can be open at a time. A decision either calls tools or
// parks, never both, so a run waits on time or on tasks and never both — see
// docs/execution-model.md section 1. This walks the whole history anyway
// rather than assuming that, because an assumption that holds today and is
// checked nowhere is an assumption that stops holding quietly.
func PendingWait(history []Event) (Wait, bool) {
	var (
		open    Wait
		waiting bool
	)
	for _, e := range history {
		switch e.Type {
		case EventTimerSet:
			var data TimerSetData
			if err := e.Decode(&data); err != nil {
				// A wait this build cannot read is still a wait. Reporting it
				// as open parks the run, which an operator will notice;
				// ignoring it would resume a run whose suspension nobody
				// understood, which is the failure that cannot be seen.
				open, waiting = Wait{Kind: WaitTimer, StepID: e.StepID, Until: Indefinite}, true
				continue
			}
			open, waiting = Wait{Kind: WaitTimer, StepID: e.StepID, Until: data.WakeAt}, true

		case EventTimerFired:
			if waiting && e.StepID == open.StepID {
				waiting = false
			}
		}
	}
	return open, waiting
}

// Elapsed reports whether a wait may end of its own accord by now.
//
// Inclusive, so a wait due at exactly now is over. The same boundary as
// Run.IsWaiting, and for the same reason: a wake-up in the past is ready, not
// late.
func (w Wait) Elapsed(now time.Time) bool {
	return !w.Until.After(now)
}

package core

import (
	"encoding/json"
	"time"
)

// RunStatus is the lifecycle state of a run. Values match the run_status
// enum in migration 0001.
type RunStatus string

const (
	RunRunning   RunStatus = "RUNNING"
	RunCompleted RunStatus = "COMPLETED"
	RunFailed    RunStatus = "FAILED"
	RunCancelled RunStatus = "CANCELLED"
)

// Run is one agent execution.
type Run struct {
	ID           RunID
	AgentName    string
	AgentVersion string // pinned at start; a resumed run never changes it
	Status       RunStatus
	Version      int64 // advancement counter; see RunState
	Input        json.RawMessage
	Output       json.RawMessage
	LastError    string

	// AvailableAt is the instant before which this run must not be advanced.
	//
	// Nil means ready, and it has to: every run that predates the column has
	// nil here and every one of them is ready. A run that is sleeping, or
	// waiting on a signal, carries the instant it may next be looked at —
	// Indefinite when there is no schedule at all.
	//
	// A run waits on time or on tasks and never both, so this is only ever
	// set on a run with nothing in flight. See docs/execution-model.md.
	AvailableAt *time.Time

	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
}

// IsWaiting reports whether the run is parked past now.
//
// A park whose instant has passed is *ready*, not late. A run whose wake-up
// went by while the runtime was down has to be picked up by the ordinary scan
// with no special case at all — anything that treats an overdue timer as an
// error loses every timer that expired during an outage, which is the exact
// failure durable timers exist to prevent.
func (r Run) IsWaiting(now time.Time) bool {
	return r.AvailableAt != nil && r.AvailableAt.After(now)
}

// RunState is the mutable slice of a run that advancement writes.
//
// Advancement is a compare-and-swap on Run.Version: the write applies only if
// the run is still at the version the caller read. Two workers finishing
// sibling tasks at the same instant both try to advance; one wins, the other
// sees ErrConflict, re-reads, and finds the work already done. Without this,
// both would call the model and the run would fork into two timelines.
//
// Serialization is per run. Runs never contend with each other.
type RunState struct {
	Status    RunStatus
	Output    json.RawMessage
	LastError string

	// AvailableAt parks the run until an instant. Nil clears any existing
	// park, which is why it is safe for every caller that is not parking to
	// leave it unset: a run that advances is, by that fact, no longer
	// waiting. Only the two suspending decisions set it.
	AvailableAt *time.Time
}

// IsTerminal reports whether no further advancement is possible.
func (s RunStatus) IsTerminal() bool {
	switch s {
	case RunCompleted, RunFailed, RunCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether s -> next is legal.
//
// Enforced in the domain layer rather than by a database constraint, so that
// the rule is testable without I/O and identical across every adapter.
func (s RunStatus) CanTransitionTo(next RunStatus) bool {
	if s.IsTerminal() {
		return false
	}
	switch next {
	case RunCompleted, RunFailed, RunCancelled:
		return s == RunRunning
	default:
		return false
	}
}

func (s RunStatus) Valid() bool {
	switch s {
	case RunRunning, RunCompleted, RunFailed, RunCancelled:
		return true
	default:
		return false
	}
}

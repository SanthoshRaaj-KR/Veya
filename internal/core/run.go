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
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  *time.Time
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

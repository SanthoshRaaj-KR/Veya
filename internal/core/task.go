package core

import (
	"encoding/json"
	"time"
)

// TaskStatus is the lifecycle state of a task. Values match the task_status
// enum in migration 0001.
type TaskStatus string

const (
	TaskPending    TaskStatus = "PENDING"
	TaskRunning    TaskStatus = "RUNNING"
	TaskCompleted  TaskStatus = "COMPLETED"
	TaskFailed     TaskStatus = "FAILED"
	TaskCancelled  TaskStatus = "CANCELLED"
	TaskDeadLetter TaskStatus = "DEAD_LETTER"
)

// Task is one unit of work belonging to a run.
//
// Ownership columns are deliberately absent. From Layer 2, who holds a task
// lives in exactly one place — the leases table — so that two records cannot
// disagree about it. A drifted fencing token silently disables the mechanism
// it implements, so there is only ever one copy of it.
type Task struct {
	ID          TaskID
	RunID       RunID
	StepID      StepID
	Type        string // resolved through ToolRegistry
	Payload     json.RawMessage
	Status      TaskStatus
	Attempt     int
	MaxAttempts int
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
}

// TaskOutcome carries the operational detail of a task transition.
//
// Deliberately thin. A task's result is not here and is not a column: it
// lives in the TASK_COMPLETED event, which is the authoritative record and
// the one replay reads. Keeping a second copy on the task row would be two
// records asserting the same fact, which is the drift README section 16
// rejects. LastError is the one exception, present because operators need it
// without reading history, and README section 8.1 sanctions it.
type TaskOutcome struct {
	Error string
}

// IsTerminal reports whether the task will not be worked on again.
func (s TaskStatus) IsTerminal() bool {
	switch s {
	case TaskCompleted, TaskFailed, TaskCancelled, TaskDeadLetter:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether s -> next is legal.
//
// PENDING -> RUNNING is the claim, and it is the first concurrency barrier:
// the transition is conditional, so of two workers handed the same task
// exactly one succeeds and the other drops it. This is why duplicate delivery
// is safe by construction rather than by convention, and it holds before
// leases exist.
func (s TaskStatus) CanTransitionTo(next TaskStatus) bool {
	if s.IsTerminal() {
		return false
	}
	switch s {
	case TaskPending:
		return next == TaskRunning || next == TaskCancelled
	case TaskRunning:
		switch next {
		case TaskCompleted, TaskFailed, TaskCancelled, TaskDeadLetter:
			return true
		case TaskPending:
			// Requeue after a failed attempt with retries remaining.
			return true
		}
	}
	return false
}

func (s TaskStatus) Valid() bool {
	switch s {
	case TaskPending, TaskRunning, TaskCompleted, TaskFailed, TaskCancelled, TaskDeadLetter:
		return true
	default:
		return false
	}
}

// Exhausted reports whether the task has used its final attempt.
func (t Task) Exhausted() bool {
	return t.Attempt >= t.MaxAttempts
}

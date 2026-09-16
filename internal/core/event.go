package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventType names a fact that was recorded. The set here is what Layer 1
// emits; README section 8.2 lists the full vocabulary, the remainder of which
// arrives with the layer that can honestly emit it.
type EventType string

const (
	EventRunStarted    EventType = "RUN_STARTED"
	EventTaskCreated   EventType = "TASK_CREATED"
	EventTaskClaimed   EventType = "TASK_CLAIMED"
	EventTaskCompleted EventType = "TASK_COMPLETED"
	EventTaskFailed    EventType = "TASK_FAILED"
	EventRunCompleted  EventType = "RUN_COMPLETED"
	EventRunFailed     EventType = "RUN_FAILED"

	EventTaskRetryScheduled EventType = "TASK_RETRY_SCHEDULED"
	EventTaskDeadLettered   EventType = "TASK_DEAD_LETTERED"

	EventEffectCreated    EventType = "EFFECT_CREATED"
	EventEffectCommitted  EventType = "EFFECT_COMMITTED"
	EventEffectFailed     EventType = "EFFECT_FAILED"
	EventEffectUnknown    EventType = "EFFECT_UNKNOWN"
	EventEffectReconciled EventType = "EFFECT_RECONCILED"

	// EventEffectEscalated records that an outcome cannot be resolved
	// automatically and a human must decide. The run parks here rather than
	// guessing in either direction.
	EventEffectEscalated EventType = "EFFECT_ESCALATED"

	// EventLeaseExpired records that a task's owner went silent and the task
	// was reclaimed under a higher fencing token.
	EventLeaseExpired EventType = "LEASE_EXPIRED"
)

// PayloadVersion is the schema version stamped on every event body written by
// this build.
//
// Versioning from the first commit costs one integer now. Retrofitting it onto
// existing history costs a data migration, because there is no way to tell an
// unversioned body apart from a v1 body after the fact.
const PayloadVersion = 1

// Event is one immutable fact in a run's history.
//
// (RunID, Seq) is the primary key, which makes appends conditional: a caller
// supplies the sequence number it believes comes next, and a retry that races
// another writer collides instead of silently duplicating. History is
// therefore gapless and ordered by construction.
type Event struct {
	RunID     RunID
	Seq       int64
	Type      EventType
	StepID    StepID // empty for run-level events
	Payload   json.RawMessage
	CreatedAt time.Time
}

// envelope wraps every event body so the schema can evolve without a data
// migration. Stored shape: {"v":1,"data":{...}}.
type envelope struct {
	V    int             `json:"v"`
	Data json.RawMessage `json:"data"`
}

// NewEvent builds an event, marshalling data into the versioned envelope.
func NewEvent(runID RunID, seq int64, typ EventType, step StepID, data any) (Event, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("marshal %s payload: %w", typ, err)
	}
	payload, err := json.Marshal(envelope{V: PayloadVersion, Data: body})
	if err != nil {
		return Event{}, fmt.Errorf("wrap %s payload: %w", typ, err)
	}
	return Event{RunID: runID, Seq: seq, Type: typ, StepID: step, Payload: payload}, nil
}

// Decode unwraps the envelope and unmarshals the body into dst.
//
// An unrecognised version is returned as an error rather than being decoded
// optimistically: a newer writer must not have its history silently
// misinterpreted by an older reader.
func (e Event) Decode(dst any) error {
	var env envelope
	if err := json.Unmarshal(e.Payload, &env); err != nil {
		return fmt.Errorf("unwrap %s payload: %w", e.Type, err)
	}
	if env.V != PayloadVersion {
		return fmt.Errorf("%w: event %s seq %d has payload version %d, this build understands %d",
			ErrUnknownPayloadVersion, e.Type, e.Seq, env.V, PayloadVersion)
	}
	if err := json.Unmarshal(env.Data, dst); err != nil {
		return fmt.Errorf("decode %s payload: %w", e.Type, err)
	}
	return nil
}

// Event payload bodies. One type per EventType, so that reading history is a
// type switch rather than a map lookup with string keys.
type (
	RunStartedData struct {
		AgentName    string          `json:"agent_name"`
		AgentVersion string          `json:"agent_version"`
		Input        json.RawMessage `json:"input,omitempty"`
	}

	TaskCreatedData struct {
		TaskID   TaskID          `json:"task_id"`
		TaskType string          `json:"task_type"`
		Payload  json.RawMessage `json:"payload,omitempty"`
	}

	TaskClaimedData struct {
		TaskID   TaskID `json:"task_id"`
		WorkerID string `json:"worker_id"`
		Attempt  int    `json:"attempt"`
	}

	TaskCompletedData struct {
		TaskID TaskID          `json:"task_id"`
		Result json.RawMessage `json:"result,omitempty"`
	}

	TaskFailedData struct {
		TaskID  TaskID `json:"task_id"`
		Error   string `json:"error"`
		Attempt int    `json:"attempt"`
		Final   bool   `json:"final"`
	}

	RunCompletedData struct {
		Output json.RawMessage `json:"output,omitempty"`
	}

	RunFailedData struct {
		Error string `json:"error"`
	}

	TaskRetryScheduledData struct {
		TaskID  TaskID `json:"task_id"`
		Attempt int    `json:"attempt"`
		Reason  string `json:"reason"`
	}

	LeaseExpiredData struct {
		TaskID        TaskID       `json:"task_id"`
		PreviousOwner string       `json:"previous_owner"`
		PreviousToken FencingToken `json:"previous_token"`
	}

	// EffectData is the body of every effect event. One shape for all of them
	// so that an effect's whole story reads the same way at each step.
	EffectData struct {
		TaskID      TaskID          `json:"task_id"`
		Key         IdempotencyKey  `json:"idempotency_key"`
		EffectType  string          `json:"effect_type"`
		Class       EffectClass     `json:"effect_class"`
		Status      EffectStatus    `json:"status"`
		ExternalRef string          `json:"external_ref,omitempty"`
		Response    json.RawMessage `json:"response,omitempty"`
		Error       string          `json:"error,omitempty"`
		// Detail explains a reconciliation or an escalation in words an
		// operator reading history can act on.
		Detail string `json:"detail,omitempty"`
	}
)

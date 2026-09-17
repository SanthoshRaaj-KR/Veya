package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// The outbox: the intent to deliver a task, committed with the task itself.
//
// # The problem it solves
//
// Creating a task and publishing it are two different systems. Doing them as
// two independent operations leaves a window:
//
//	BEGIN; INSERT task; COMMIT;   <- the process dies here
//	publish(task)                 <- never happens
//
// The task is durable and nobody will ever be told about it. That is a stuck
// run, and it is silent: nothing failed, nothing retried, the work simply sits
// there. This is the same class of bug the effect ledger exists to prevent for
// user tool calls, so permitting it in the runtime's own plumbing was not
// defensible.
//
// # How it is closed
//
// The intent to deliver is a row, written in the same transaction as the task
// and its event. Either all three commit or none do. A separate relay then
// reads unpublished rows and hands them to the dispatcher.
//
// The cost is that delivery is no longer instantaneous, and that a crash
// between publishing and marking the row published produces a duplicate
// delivery. Both are cheap: delivery is at-least-once regardless, and the
// conditional PENDING -> RUNNING claim is what makes a duplicate harmless.
//
// This is the safety/liveness split in miniature. The row is safety and is
// written transactionally; the relay is liveness and is allowed to be late.

// DeliveryID identifies one outbox row. Assigned by the store.
type DeliveryID int64

// Delivery is a committed intent to hand one task to a worker.
//
// It carries identity and bookkeeping, never task state. What is true about
// the task is read from the store by whoever receives the delivery, which is
// why a duplicated or delayed delivery carries nothing that could be stale.
type Delivery struct {
	ID     DeliveryID
	TaskID TaskID

	// Attempts counts publish failures. It is not a retry budget — the relay
	// never gives up — it is the signal an operator needs to notice that one
	// row has been failing to publish for an hour.
	Attempts  int
	LastError string
	CreatedAt time.Time
}

// DeliverySubject is where task deliveries are published.
//
// One subject for everything today. Per-tool subjects would let a worker
// subscribe to only the tools it hosts, but nothing routes by capability until
// Layer 5, and a routing key that nothing routes on is a field that drifts out
// of sync with reality unnoticed.
const DeliverySubject = "veya.tasks.work"

// deliveryPayloadVersion is the envelope version, matching the convention
// events use: a consumer that meets a version it does not know must say so
// rather than silently misread the message.
const deliveryPayloadVersion = 1

// deliveryPayload is what crosses the wire. Identity, and the version needed
// to keep reading it after the format changes.
type deliveryPayload struct {
	V      int    `json:"v"`
	TaskID TaskID `json:"task_id"`
}

// EncodeDelivery renders a task identity for transport.
func EncodeDelivery(id TaskID) ([]byte, error) {
	b, err := json.Marshal(deliveryPayload{V: deliveryPayloadVersion, TaskID: id})
	if err != nil {
		return nil, fmt.Errorf("encode delivery %s: %w", id, err)
	}
	return b, nil
}

// DecodeDelivery reads a task identity back off the wire.
//
// An unreadable message is an error rather than a dropped message. A broker
// carrying something this build cannot parse is a deployment problem, and
// discarding it quietly would turn that into a stuck run.
func DecodeDelivery(b []byte) (TaskID, error) {
	var p deliveryPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return "", fmt.Errorf("decode delivery: %w", err)
	}
	if p.V != deliveryPayloadVersion {
		return "", fmt.Errorf("decode delivery: version %d: %w", p.V, ErrUnknownPayloadVersion)
	}
	if p.TaskID == "" {
		return "", fmt.Errorf("decode delivery: no task id: %w", ErrNotFound)
	}
	return p.TaskID, nil
}

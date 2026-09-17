package core_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// The delivery envelope is small enough to look self-evidently correct, which
// is exactly why it is worth pinning: it is the one format that has to keep
// being readable by a process built from different source than the one that
// wrote it.

func TestDeliveryEnvelopeRoundTrips(t *testing.T) {
	encoded, err := core.EncodeDelivery("task-42")
	if err != nil {
		t.Fatalf("EncodeDelivery: %v", err)
	}

	got, err := core.DecodeDelivery(encoded)
	if err != nil {
		t.Fatalf("DecodeDelivery: %v", err)
	}
	if got != "task-42" {
		t.Fatalf("task id = %q, want task-42", got)
	}
}

// TestDeliveryCarriesIdentityOnly is a design constraint, not an encoding
// detail. A delivery that carried task state would let a stale message
// contradict the store, and the receiver would have to decide which to believe.
// Carrying only an identity means there is nothing to be wrong about.
func TestDeliveryCarriesIdentityOnly(t *testing.T) {
	encoded, err := core.EncodeDelivery("task-42")
	if err != nil {
		t.Fatalf("EncodeDelivery: %v", err)
	}

	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	for key := range fields {
		switch key {
		case "v", "task_id":
		default:
			t.Fatalf("envelope carries %q; a delivery must carry identity and nothing else", key)
		}
	}
}

// TestDeliveryRejectsAnUnknownVersion covers the upgrade path. A consumer that
// silently misreads a newer envelope is worse than one that refuses it: the
// first executes the wrong task, the second stops and says so.
func TestDeliveryRejectsAnUnknownVersion(t *testing.T) {
	_, err := core.DecodeDelivery([]byte(`{"v":99,"task_id":"task-42"}`))
	if !errors.Is(err, core.ErrUnknownPayloadVersion) {
		t.Fatalf("decode of a future envelope = %v, want ErrUnknownPayloadVersion", err)
	}
}

func TestDeliveryRejectsAnEmptyIdentity(t *testing.T) {
	if _, err := core.DecodeDelivery([]byte(`{"v":1}`)); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("decode without a task id = %v, want ErrNotFound", err)
	}
	if _, err := core.DecodeDelivery([]byte(`not json`)); err == nil {
		t.Fatal("decode of malformed bytes succeeded; it must be an error, never a dropped message")
	}
}

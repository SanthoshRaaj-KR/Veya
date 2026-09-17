package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/outbox"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
)

// The relay's job is narrow: publish what has been committed, then record that
// it did. Almost everything interesting is about the order of those two steps
// and about what happens when the second one does not get to run.

// TestRelayPublishesCommittedWork is the ordinary path.
func TestRelayPublishesCommittedWork(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-1")

	if n := f.relayOnce(t); n != 1 {
		t.Fatalf("published %d deliveries, want 1", n)
	}
	if got := f.dispatcher.published(); len(got) != 1 || got[0] != "task-1" {
		t.Fatalf("dispatcher saw %v, want [task-1]", got)
	}
	if pending := f.pending(t); len(pending) != 0 {
		t.Fatalf("%d deliveries still pending after a successful sweep", len(pending))
	}
}

// TestRelayRecoversWorkCommittedByADeadProcess is the whole reason this layer
// exists.
//
// A process commits a task and dies before anything is published. Under the
// old dual write that task was durable and unannounced — nothing failed,
// nothing retried, the run simply stopped. Here the intent committed with the
// task, so a completely different process picks it up.
func TestRelayRecoversWorkCommittedByADeadProcess(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-orphan")

	// The process that committed it never ran a sweep: it is gone.
	if got := f.dispatcher.published(); len(got) != 0 {
		t.Fatalf("dispatcher saw %v before any relay ran", got)
	}

	// A fresh relay, over the same store, with its own dispatcher.
	successor := newDispatcher()
	relay, err := outbox.New(outbox.Config{
		Store:      f.store,
		Dispatcher: successor,
		Logger:     quiet(),
	})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}
	if _, err := relay.RelayOnce(context.Background()); err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}

	if got := successor.published(); len(got) != 1 || got[0] != "task-orphan" {
		t.Fatalf("successor published %v, want [task-orphan]: committed work must "+
			"survive the process that committed it", got)
	}
}

// TestFailedPublishKeepsTheDelivery covers a broker that is refusing work. The
// row is not dropped and not dead-lettered: a task nobody is told about is a
// stuck run, and there is no attempt count at which that becomes acceptable.
func TestFailedPublishKeepsTheDelivery(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-unlucky")
	f.dispatcher.failWith(errors.New("broker unreachable"))

	if n := f.relayOnce(t); n != 0 {
		t.Fatalf("published %d deliveries while the broker was down, want 0", n)
	}

	pending := f.pending(t)
	if len(pending) != 1 {
		t.Fatalf("%d deliveries pending after a failed publish, want 1", len(pending))
	}
	if pending[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", pending[0].Attempts)
	}
	if pending[0].LastError == "" {
		t.Fatal("a failed delivery must record why; attempts alone says something is wrong without saying what")
	}

	// The broker comes back.
	f.dispatcher.failWith(nil)
	if n := f.relayOnce(t); n != 1 {
		t.Fatalf("published %d deliveries after recovery, want 1", n)
	}
	if got := f.dispatcher.published(); len(got) != 1 || got[0] != "task-unlucky" {
		t.Fatalf("dispatcher saw %v, want [task-unlucky]", got)
	}
}

// TestRelayMarksOnlyWhatItPublished keeps one bad delivery from taking the
// batch down with it.
func TestRelayMarksOnlyWhatItPublished(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-ok-1")
	f.commitTask(t, "task-poison")
	f.commitTask(t, "task-ok-2")
	f.dispatcher.failFor("task-poison", errors.New("subject rejected"))

	if n := f.relayOnce(t); n != 2 {
		t.Fatalf("published %d deliveries, want 2 (the third is poison)", n)
	}

	pending := f.pending(t)
	if len(pending) != 1 {
		t.Fatalf("%d deliveries pending, want 1", len(pending))
	}
	if pending[0].TaskID != "task-poison" {
		t.Fatalf("the wrong delivery is still pending: %s", pending[0].TaskID)
	}
}

// TestRelayDoesNotRepublishWhatItDelivered is the property that stops the
// outbox becoming a duplicate generator. Duplicates are safe, but a relay that
// produced one on every sweep would multiply work without bound.
func TestRelayDoesNotRepublishWhatItDelivered(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-once")

	for i := 0; i < 5; i++ {
		f.relayOnce(t)
	}
	if got := f.dispatcher.published(); len(got) != 1 {
		t.Fatalf("five sweeps published %d times, want 1", len(got))
	}
}

// TestWakeIsNeverBlocking pins the one property Wake has to have. It is called
// from inside the engine right after a commit, and a Wake that could block
// would let a slow relay stall the engine — for a hint the design explicitly
// does not need.
func TestWakeIsNeverBlocking(t *testing.T) {
	f := newFixture(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Far more calls than the one-slot buffer, with nothing draining it.
		for i := 0; i < 10_000; i++ {
			f.relay.Wake()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wake blocked; it is a hint and must never be able to stall a committed transaction")
	}
}

// --- fixture --------------------------------------------------------------

type fixture struct {
	store      core.Store
	dispatcher *fakeDispatcher
	relay      *outbox.Relay
	seq        int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	clk := clock.NewVirtual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New(clk)
	disp := newDispatcher()

	relay, err := outbox.New(outbox.Config{
		Store:      store,
		Dispatcher: disp,
		Logger:     quiet(),
	})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })
	return &fixture{store: store, dispatcher: disp, relay: relay}
}

// commitTask writes a task and its delivery intent in one transaction, which
// is what the engine does.
func (f *fixture) commitTask(t *testing.T, id core.TaskID) {
	t.Helper()

	f.seq++
	runID := core.RunID("run-relay")
	err := f.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateRun(ctx, core.Run{
			ID: runID, AgentName: "fixture", AgentVersion: "v1", Status: core.RunRunning,
		}); err != nil && !errors.Is(err, core.ErrConflict) {
			return err
		}
		if err := tx.CreateTask(ctx, core.Task{
			ID: id, RunID: runID, StepID: core.Step(f.seq), Type: "noop",
			Payload: json.RawMessage(`{}`), Status: core.TaskPending, MaxAttempts: 3,
		}); err != nil {
			return err
		}
		return tx.EnqueueDelivery(ctx, id)
	})
	if err != nil {
		t.Fatalf("commit task %s: %v", id, err)
	}
}

func (f *fixture) relayOnce(t *testing.T) int {
	t.Helper()
	n, err := f.relay.RelayOnce(context.Background())
	if err != nil {
		t.Fatalf("RelayOnce: %v", err)
	}
	return n
}

func (f *fixture) pending(t *testing.T) []core.Delivery {
	t.Helper()
	out, err := f.store.PendingDeliveries(context.Background(), 100)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	return out
}

// fakeDispatcher records what it was given and can be told to refuse.
type fakeDispatcher struct {
	mu      sync.Mutex
	seen    []core.TaskID
	err     error
	perTask map[core.TaskID]error
}

func newDispatcher() *fakeDispatcher {
	return &fakeDispatcher{perTask: map[core.TaskID]error{}}
}

func (d *fakeDispatcher) Publish(_ context.Context, id core.TaskID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.perTask[id]; err != nil {
		return err
	}
	if d.err != nil {
		return d.err
	}
	d.seen = append(d.seen, id)
	return nil
}

func (d *fakeDispatcher) Claim(context.Context) (core.TaskID, error) {
	return "", core.ErrDispatcherClosed
}

func (d *fakeDispatcher) Close() error { return nil }

func (d *fakeDispatcher) failWith(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
}

func (d *fakeDispatcher) failFor(id core.TaskID, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.perTask[id] = err
}

func (d *fakeDispatcher) published() []core.TaskID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]core.TaskID(nil), d.seen...)
}

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

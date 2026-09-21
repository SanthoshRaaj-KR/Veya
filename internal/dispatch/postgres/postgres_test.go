//go:build integration

// Build-tagged so that `make test` needs no Docker. `make test-integration`
// sets VEYA_TEST_DSN and runs this against a live database.
package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	dispatch "github.com/SanthoshRaaj-KR/Veya/internal/dispatch/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/pgtest"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
)

// TestDispatchesPendingWork is the base case: a committed task is dispatchable
// with nothing having published it, because with this adapter the task row is
// the queue.
func TestDispatchesPendingWork(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-pg-1")

	got, err := f.dispatcher.Claim(f.ctx(t, 5*time.Second))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != "task-pg-1" {
		t.Fatalf("claimed %q, want task-pg-1", got)
	}
}

// TestOneTaskGoesToOneWorker is what SKIP LOCKED plus the visibility bump buys.
//
// Handing the same task to several workers would be safe — the conditional
// claim rejects all but one — but it would also mean every worker spending its
// time losing races instead of doing work.
func TestOneTaskGoesToOneWorker(t *testing.T) {
	f := newFixture(t)

	const tasks = 20
	for i := 0; i < tasks; i++ {
		f.commitTask(t, core.TaskID(fmt.Sprintf("task-pg-share-%d", i)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		mu    sync.Mutex
		seen  = map[core.TaskID]int{}
		total int
		wg    sync.WaitGroup
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id, err := f.dispatcher.Claim(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				seen[id]++
				total++
				done := total >= tasks
				mu.Unlock()
				if done {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	if total != tasks {
		t.Fatalf("four pollers claimed %d tasks between them, want %d", total, tasks)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s was handed out %d times; SKIP LOCKED and the visibility "+
				"window exist to make that once", id, n)
		}
	}
}

// TestATakenTaskIsNotOfferedAgainImmediately covers the gap SKIP LOCKED does
// not cover: the task stays PENDING until a worker claims it, so without the
// visibility bump the next poll would hand out the same work.
func TestATakenTaskIsNotOfferedAgainImmediately(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-pg-visible")

	if _, err := f.dispatcher.Claim(f.ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("first Claim: %v", err)
	}

	// The task is still PENDING — nothing claimed it — but must not come back.
	if got := f.task(t, "task-pg-visible").Status; got != core.TaskPending {
		t.Fatalf("task status = %s, want PENDING: the dispatcher must not change "+
			"what is true, only what is offered", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := f.dispatcher.Claim(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Claim = %v, want to find nothing", err)
	}
}

// TestAnUnclaimedTaskComesBack is the other side of the same window. A worker
// that dies between being handed a task and claiming it must not strand it.
func TestAnUnclaimedTaskComesBack(t *testing.T) {
	f := newFixtureWith(t, dispatch.Config{Visibility: 150 * time.Millisecond})
	f.commitTask(t, "task-pg-returns")

	if _, err := f.dispatcher.Claim(f.ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	// The worker dies here, having claimed nothing.

	got, err := f.dispatcher.Claim(f.ctx(t, 5*time.Second))
	if err != nil {
		t.Fatalf("Claim after the visibility window: %v", err)
	}
	if got != "task-pg-returns" {
		t.Fatalf("claimed %q, want task-pg-returns", got)
	}
}

// TestPublishIsANoOp documents the adapter's oddest property, so that it is a
// decision rather than something someone later "fixes".
func TestPublishIsANoOp(t *testing.T) {
	f := newFixture(t)
	f.commitTask(t, "task-pg-noop")

	// Publishing a task that does not exist is not an error: there is nothing
	// to send anywhere, because the row already is the queue.
	if err := f.dispatcher.Publish(context.Background(), "task-that-does-not-exist"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := f.dispatcher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := f.dispatcher.Publish(context.Background(), "task-pg-noop"); !errors.Is(err, core.ErrDispatcherClosed) {
		t.Fatalf("Publish after Close = %v, want ErrDispatcherClosed", err)
	}
	if _, err := f.dispatcher.Claim(context.Background()); !errors.Is(err, core.ErrDispatcherClosed) {
		t.Fatalf("Claim after Close = %v, want ErrDispatcherClosed", err)
	}
}

// --- fixture --------------------------------------------------------------

type fixture struct {
	db         *sql.DB
	store      *postgres.Store
	dispatcher *dispatch.Dispatcher
	seq        int
}

func newFixture(t *testing.T) *fixture { return newFixtureWith(t, dispatch.Config{}) }

// newFixtureWith gives each test its own database, so pending tasks from one
// test are never offered to another.
func newFixtureWith(t *testing.T, cfg dispatch.Config) *fixture {
	t.Helper()

	db := pgtest.Scratch(t)

	clk := clock.System{}
	store := postgres.New(db, clk)

	cfg.DB = db
	cfg.Clock = clk
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	d, err := dispatch.New(cfg)
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	return &fixture{db: db, store: store, dispatcher: d}
}

// fixtureRun is the run every seeded task belongs to. It is created once,
// deliberately not "create it and ignore the conflict": a failed statement
// aborts a PostgreSQL transaction outright, so every later statement in the
// same transaction fails too, however the Go error was handled.
const fixtureRun = core.RunID("run-dispatch")

func (f *fixture) commitTask(t *testing.T, id core.TaskID) {
	t.Helper()

	f.ensureRun(t)
	f.seq++

	err := f.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateTask(ctx, core.Task{
			ID: id, RunID: fixtureRun, StepID: core.Step(f.seq), Type: "noop",
			Payload: json.RawMessage(`{}`), Status: core.TaskPending, MaxAttempts: 3,
		}); err != nil {
			return err
		}
		// The task and the intent to announce it, in one transaction, exactly
		// as the engine writes them.
		return tx.EnqueueDelivery(ctx, id, time.Time{})
	})
	if err != nil {
		t.Fatalf("commit task %s: %v", id, err)
	}
}

func (f *fixture) ensureRun(t *testing.T) {
	t.Helper()
	if f.seq > 0 {
		return
	}
	err := f.store.RunInTx(context.Background(), func(ctx context.Context, tx core.Tx) error {
		return tx.CreateRun(ctx, core.Run{
			ID: fixtureRun, AgentName: "fixture", AgentVersion: "v1", Status: core.RunRunning,
		})
	})
	if err != nil {
		t.Fatalf("create fixture run: %v", err)
	}
}

func (f *fixture) task(t *testing.T, id core.TaskID) core.Task {
	t.Helper()
	task, err := f.store.GetTask(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	return task
}

func (f *fixture) ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

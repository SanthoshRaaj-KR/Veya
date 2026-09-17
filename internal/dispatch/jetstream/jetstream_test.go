//go:build integration

// Build-tagged so that `make test` needs no Docker. `make test-integration`
// sets VEYA_TEST_NATS_URL and runs this against a live NATS server.
package jetstream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/jetstream"
)

// These tests assert the transport's properties, not the runtime's. The
// interesting claims — that a duplicate is harmless, that a lost message is
// recoverable — belong to the claim transition and the recovery scan, and are
// tested where those live.

func TestPublishAndClaimRoundTrip(t *testing.T) {
	d := open(t)

	if err := d.Publish(context.Background(), "task-round-trip"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := claim(t, d)
	if got != "task-round-trip" {
		t.Fatalf("claimed %q, want task-round-trip", got)
	}
}

// TestWorkIsSharedNotCopied is why a durable consumer is shared by every
// worker. If each worker got its own, adding a worker would duplicate the work
// instead of dividing it — safe, because of the conditional claim, and
// completely pointless.
func TestWorkIsSharedNotCopied(t *testing.T) {
	d := open(t)

	const tasks = 12
	for i := 0; i < tasks; i++ {
		if err := d.Publish(context.Background(), core.TaskID(fmt.Sprintf("task-shared-%d", i))); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		mu    sync.Mutex
		seen  = map[core.TaskID]int{}
		total int
		wg    sync.WaitGroup
	)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id, err := d.Claim(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				seen[id]++
				total++
				done := total >= tasks
				mu.Unlock()
				if done {
					// Release the workers still parked in Claim rather than
					// leaving them to wait out the timeout.
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	if total != tasks {
		t.Fatalf("three workers together claimed %d deliveries, want %d", total, tasks)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s was delivered %d times to the pool; a shared consumer divides "+
				"work, it does not copy it", id, n)
		}
	}
}

// TestClaimRespectsItsContext keeps a worker from parking forever on an empty
// queue. Shutdown has to be prompt, or a deploy waits out the fetch window on
// every worker.
func TestClaimRespectsItsContext(t *testing.T) {
	d := open(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := d.Claim(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Claim on an empty stream = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Claim took %s to notice its context; shutdown must not wait out the fetch window", elapsed)
	}
}

// TestClosedDispatcherRefusesWork covers orderly shutdown: once closed, nothing
// is accepted and nothing is handed out.
func TestClosedDispatcherRefusesWork(t *testing.T) {
	d := open(t)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Idempotent, because Serve closes it and so does Stack.Close.
	if err := d.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if err := d.Publish(context.Background(), "task-too-late"); !errors.Is(err, core.ErrDispatcherClosed) {
		t.Fatalf("Publish after Close = %v, want ErrDispatcherClosed", err)
	}
	if _, err := d.Claim(context.Background()); !errors.Is(err, core.ErrDispatcherClosed) {
		t.Fatalf("Claim after Close = %v, want ErrDispatcherClosed", err)
	}
}

// --- helpers --------------------------------------------------------------

// open connects to the test server with a stream and durable name unique to
// this test, so tests do not inherit each other's leftover messages.
func open(t *testing.T) *jetstream.Dispatcher {
	t.Helper()

	url := os.Getenv("VEYA_TEST_NATS_URL")
	if url == "" {
		t.Skip("VEYA_TEST_NATS_URL is not set; run `make up` and use `make test-integration`")
	}

	name := fmt.Sprintf("VEYA_TEST_%d", time.Now().UnixNano())
	d, err := jetstream.Open(context.Background(), jetstream.Config{
		URL:       url,
		Stream:    name,
		Durable:   name,
		Subject:   fmt.Sprintf("veya.test.%d", time.Now().UnixNano()),
		FetchWait: 200 * time.Millisecond,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("jetstream.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func claim(t *testing.T, d *jetstream.Dispatcher) core.TaskID {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id, err := d.Claim(ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return id
}

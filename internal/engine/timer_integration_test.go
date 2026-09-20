//go:build integration

// One real timer, on a real clock, against a real database.
//
// Everything else about durable timers is tested on the virtual clock, in
// microseconds, and that is the right default: a suite that waits is a suite
// nobody runs. But a virtual clock cannot catch the two bugs that live between
// Go's time.Time and PostgreSQL's TIMESTAMPTZ, because in a virtual test both
// sides are the same Go value and the database is a map.
//
//	zone         a wake-up written as local time and read back as UTC is off
//	             by the offset -- hours early or hours late depending on which
//	             side of Greenwich the machine is. The virtual clock never
//	             notices, because nothing ever converts.
//
//	truncation   PostgreSQL stores microseconds; Go carries nanoseconds. A
//	             wake-up at .0000005s comes back at .000000s, half a
//	             microsecond earlier — harmless here, and the same rounding
//	             applied to a comparison rather than a value is how a run
//	             wakes on the tick before its own.
//
// So this test is deliberately short — a sub-second sleep — and deliberately
// real. It is the only place in the suite that waits on a wall clock.
package engine_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/inproc"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/outbox"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/pgtest"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
)

func TestARealTimerOnARealClock(t *testing.T) {
	const nap = 400 * time.Millisecond

	db := pgtest.Scratch(t)
	sys := clock.System{}
	store := postgres.New(db, sys)
	t.Cleanup(func() { _ = store.Close() })

	// The wake-up is computed in local time on purpose. If anything in the
	// path — the driver, the column, the scan predicate — normalises to UTC
	// without converting, this is where it shows up, as a run that either
	// wakes hours early or never wakes at all.
	wake := time.Now().Add(nap)

	agent := deciderFunc(func(_ core.Run, history []core.Event) (core.Decision, error) {
		if !hasEvent(history, core.EventTimerFired) {
			return core.Decision{Kind: core.DecideSleep, StepID: core.Step(1), WakeAt: wake}, nil
		}
		return core.Decision{Kind: core.DecideComplete, Output: json.RawMessage(`{"ok":true}`)}, nil
	})

	dispatcher := inproc.New(16)
	t.Cleanup(func() { _ = dispatcher.Close() })

	relay, err := outbox.New(outbox.Config{
		Store: store, Dispatcher: dispatcher,
		Interval: 20 * time.Millisecond, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("outbox.New: %v", err)
	}

	eng, err := engine.New(engine.Config{
		Store: store, Dispatcher: dispatcher, Decider: agent,
		IDGen: idgen.Random{}, Clock: sys,
		Agent: testAgent, AgentVersion: "v1",
		Wake: relay.Wake, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := engine.NewRuntime(engine.RuntimeConfig{Engine: eng, Interval: time.Second})
	go func() { _ = rt.Run(ctx) }()

	started := time.Now()
	runID, err := eng.StartRun(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// Parked, and parked for about as long as was asked for. A zone bug
	// usually shows up here as a park that is already in the past.
	run, err := eng.Run(ctx, runID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.AvailableAt == nil {
		t.Fatal("the run did not park")
	}
	if remaining := time.Until(*run.AvailableAt); remaining < nap/2 {
		t.Fatalf("the run is parked for %s, want about %s; the instant did not survive "+
			"the round trip through TIMESTAMPTZ", remaining, nap)
	}

	// It must not wake before its instant. The scan interval is a second and
	// the nap is shorter, so the runtime's NextWakeUp hint is also under test:
	// waking on the interval alone would be late, and waking immediately would
	// mean the predicate is not comparing what it should.
	time.Sleep(nap / 2)
	if run, err = eng.Run(ctx, runID); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if run.Status.IsTerminal() {
		t.Fatalf("the run reached %s after %s of a %s sleep", run.Status, nap/2, nap)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if run, err = eng.Run(ctx, runID); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if run.Status.IsTerminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if run.Status != core.RunCompleted {
		t.Fatalf("status = %s (%s) after %s, want COMPLETED",
			run.Status, run.LastError, time.Since(started))
	}
	if elapsed := time.Since(started); elapsed < nap {
		t.Fatalf("the run finished in %s, sooner than the %s it slept for", elapsed, nap)
	}
}

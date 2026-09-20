package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

// cmdSignal tells a waiting run that something happened.
//
// It writes to PostgreSQL and stops there, like every other command here. The
// run is un-parked in the same transaction, so whichever runtime is serving
// picks it up on its next scan — the same path a crash recovery takes, which
// is why this command needs no runtime to be reachable and no port to be open.
//
// That is also why it is the whole proof of the signal port. An HTTP ingestion
// endpoint would add a server, a bind address and an authentication question
// this project has deliberately not answered; it belongs with the dashboard in
// Layer 6. What it would not add is a second way for a signal to be recorded.
func cmdSignal(args []string) error {
	fs := flag.NewFlagSet("veya signal", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	payload := fs.String("payload", "", "JSON handed to the waiting agent")
	id := fs.String("id", "", "sender's id for this delivery; generated when omitted")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("signal: expected RUN_ID and a signal NAME")
	}

	runID, name := core.RunID(fs.Arg(0)), fs.Arg(1)

	// A generated id makes every invocation a distinct delivery, which is the
	// right default for a person typing the command: they mean it to happen,
	// and two approvals typed twice are two approvals. Pass --id to make a
	// repeat idempotent, which is what an automated sender should always do.
	signalID := core.SignalID(*id)
	if signalID == "" {
		signalID = core.SignalID(idgen.Random{}.NewEffectID())
	}

	var body json.RawMessage
	if *payload != "" {
		if !json.Valid([]byte(*payload)) {
			return fmt.Errorf("signal: --payload is not valid JSON: %s", *payload)
		}
		body = json.RawMessage(*payload)
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	if err := engine.DeliverSignal(ctx, store, core.Signal{
		RunID:   runID,
		ID:      signalID,
		Name:    name,
		Payload: body,
	}); err != nil {
		return err
	}

	// Say what was recorded, including the generated id, because it is the
	// only thing that makes a retry of this command idempotent and the
	// operator cannot get it back afterwards.
	fmt.Printf("recorded  %s %s (id %s)\n", runID, name, signalID)

	run, err := store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	// Whether anything was waiting is worth saying. A signal delivered to a
	// run that is busy is stored and will be found later, which is correct and
	// looks like nothing happening.
	history, err := store.History(ctx, runID)
	if err != nil {
		return err
	}
	if wait, waiting := core.PendingWait(history); waiting && wait.Signal == name {
		fmt.Printf("          the run is waiting for this at step %s\n", wait.StepID)
	} else if run.Status == core.RunRunning {
		fmt.Printf("          the run is not waiting for it yet; it will be found when it is\n")
	}
	return nil
}

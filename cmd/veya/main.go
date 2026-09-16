// Command veya is the operator CLI.
//
// It reads and writes PostgreSQL directly, which is not a shortcut — PostgreSQL
// is the authoritative store, so asking it is the same as asking the runtime,
// and the answer does not depend on a runtime being up.
//
// A run started here is picked up by whichever veya-runtime process is
// serving: the task commits as PENDING, and the recovery scan finds it. That
// is the same path a crash recovery takes, exercised on every CLI start.
//
// Usage:
//
//	veya migrate                 apply database migrations
//	veya run start --agent NAME  create a run for the runtime to pick up
//	veya run show RUN_ID         status, output, tasks
//	veya run history RUN_ID      the full event log
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/migrations"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "veya: %v\n", err)
		os.Exit(1)
	}
}

const usage = `veya — operator CLI for the Veya durable execution runtime

Commands:
  migrate               apply database migrations
  run start             create a run
  run show RUN_ID       status, output, tasks and effects
  run history RUN_ID    the full event log
  effects               external actions whose outcome is still unknown
  effects show KEY      one effect in full
  effects resolve KEY   record what a human established

Run "veya COMMAND --help" for flags.
`

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}

	switch args[0] {
	case "migrate":
		return cmdMigrate(args[1:])
	case "run":
		return cmdRun(args[1:])
	case "effects":
		return cmdEffects(args[1:])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usage)
	}
}

// --- migrate --------------------------------------------------------------

func cmdMigrate(args []string) error {
	fs := flag.NewFlagSet("veya migrate", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	dryRun := fs.Bool("dry-run", false, "list pending migrations without applying them")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	if *dryRun {
		pending, err := migrations.Pending(ctx, store.DB())
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			fmt.Println("schema is up to date")
			return nil
		}
		for _, v := range pending {
			fmt.Printf("pending  %s\n", v)
		}
		return nil
	}

	applied, err := migrations.Apply(ctx, store.DB())
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		fmt.Println("schema is up to date")
		return nil
	}
	for _, v := range applied {
		fmt.Printf("applied  %s\n", v)
	}
	return nil
}

// --- run ------------------------------------------------------------------

func cmdRun(args []string) error {
	if len(args) == 0 {
		return errors.New("run: expected start, show or history")
	}
	switch args[0] {
	case "start":
		return cmdRunStart(args[1:])
	case "show":
		return cmdRunShow(args[1:])
	case "history":
		return cmdRunHistory(args[1:])
	default:
		return fmt.Errorf("run: unknown subcommand %q", args[0])
	}
}

func cmdRunStart(args []string) error {
	fs := flag.NewFlagSet("veya run start", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	agent := fs.String("agent", "meeting_assistant", "agent name")
	version := fs.String("agent-version", "v1", "agent version, pinned for the life of the run")
	input := fs.String("input", "{}", "run input as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if !json.Valid([]byte(*input)) {
		return fmt.Errorf("--input is not valid JSON: %s", *input)
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// This binary is its own composition root, so it names the generator
	// directly instead of receiving it as a port.
	id := idgen.Random{}.NewRunID()

	// The run is created directly rather than through an engine: the CLI does
	// not decide anything, and running a decider here would mean two processes
	// advancing the same run. A run that is RUNNING with no tasks is exactly
	// what RunsAwaitingAdvance looks for, so the runtime picks it up on its
	// next scan — the same path crash recovery takes.
	err = store.RunInTx(ctx, func(ctx context.Context, tx core.Tx) error {
		if err := tx.CreateRun(ctx, core.Run{
			ID:           id,
			AgentName:    *agent,
			AgentVersion: *version,
			Status:       core.RunRunning,
			Input:        json.RawMessage(*input),
		}); err != nil {
			return err
		}
		seq, err := tx.NextSeq(ctx, id)
		if err != nil {
			return err
		}
		ev, err := core.NewEvent(id, seq, core.EventRunStarted, "", core.RunStartedData{
			AgentName:    *agent,
			AgentVersion: *version,
			Input:        json.RawMessage(*input),
		})
		if err != nil {
			return err
		}
		return tx.AppendEvent(ctx, ev)
	})
	if err != nil {
		return fmt.Errorf("start run: %w", err)
	}

	fmt.Printf("%s\n", id)
	fmt.Fprintf(os.Stderr, "run created; a running veya-runtime will pick it up within one scan interval\n")
	return nil
}

func cmdRunShow(args []string) error {
	fs := flag.NewFlagSet("veya run show", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("run show: expected exactly one RUN_ID")
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	id := core.RunID(fs.Arg(0))
	r, err := store.GetRun(ctx, id)
	if err != nil {
		return err
	}

	fmt.Printf("run       %s\n", r.ID)
	fmt.Printf("agent     %s %s\n", r.AgentName, r.AgentVersion)
	fmt.Printf("status    %s\n", r.Status)
	fmt.Printf("version   %d\n", r.Version)
	fmt.Printf("created   %s\n", r.CreatedAt.Format("2006-01-02 15:04:05"))
	if r.CompletedAt != nil {
		fmt.Printf("finished  %s\n", r.CompletedAt.Format("2006-01-02 15:04:05"))
	}
	if len(r.Output) > 0 {
		fmt.Printf("output    %s\n", r.Output)
	}
	if r.LastError != "" {
		fmt.Printf("error     %s\n", r.LastError)
	}

	tasks, err := store.ListTasks(ctx, id)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	fmt.Printf("\ntasks\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  STEP\tTOOL\tSTATUS\tATTEMPT\tERROR")
	for _, t := range tasks {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%d/%d\t%s\n",
			t.StepID, t.Type, t.Status, t.Attempt, t.MaxAttempts, t.LastError)
	}
	return w.Flush()
}

func cmdRunHistory(args []string) error {
	fs := flag.NewFlagSet("veya run history", flag.ExitOnError)
	dsn := fs.String("dsn", wiring.DefaultDSN, "PostgreSQL connection string")
	verbose := fs.Bool("v", false, "include event payloads")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("run history: expected exactly one RUN_ID")
	}

	ctx := context.Background()
	store, err := postgres.Open(ctx, *dsn, clock.System{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	history, err := store.History(ctx, core.RunID(fs.Arg(0)))
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return fmt.Errorf("run %s: %w", fs.Arg(0), core.ErrNotFound)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SEQ\tEVENT\tSTEP\tAT")
	for _, ev := range history {
		step := string(ev.StepID)
		if step == "" {
			step = "-"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n",
			ev.Seq, ev.Type, step, ev.CreatedAt.Format("15:04:05.000"))
		if *verbose {
			fmt.Fprintf(w, "\t  %s\t\t\n", ev.Payload)
		}
	}
	return w.Flush()
}

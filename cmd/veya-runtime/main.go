// Command veya-runtime is the execution engine.
//
// It advances runs, dispatches tasks, and hosts the workers that execute them.
// In Layer 1 the engine and its workers share a process and a channel; Layer 3
// moves delivery onto NATS JetStream and lets workers live elsewhere, which
// should change this file and nothing in internal/engine.
//
// Usage:
//
//	veya-runtime                       # serve against PostgreSQL
//	veya-runtime --demo                # start one demo run, wait for it, exit
//	veya-runtime --store memory --demo  # same, no database needed
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "veya-runtime: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := wiring.DefaultConfig()
	demo := false

	fs := flag.NewFlagSet("veya-runtime", flag.ExitOnError)
	fs.StringVar(&cfg.Store, "store", cfg.Store, "state store: memory or postgres")
	fs.StringVar(&cfg.DSN, "dsn", cfg.DSN, "PostgreSQL connection string")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "in-process workers to run")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", cfg.ScanInterval, "recovery scan period")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug, info, warn or error")
	fs.BoolVar(&demo, "demo", false, "start one demo run, wait for it to finish, then exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	// Cancelled on SIGINT/SIGTERM so shutdown is orderly: workers finish what
	// they hold, and anything still PENDING is recovered by the next process
	// to scan. Nothing durable depends on a clean exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stack, err := wiring.Build(ctx, cfg, wiring.Agent{
		Name:    demoAgentName,
		Version: demoAgentVersion,
		Decider: demoDecider(),
		Tools:   demoTools(),
	})
	if err != nil {
		return err
	}
	defer func() { _ = stack.Close() }()

	stack.Logger.Info("veya-runtime starting",
		"agent", stack.Engine.Agent(), "store", cfg.Store,
		"workers", cfg.Workers, "tools", stack.Tools.Names())

	if demo {
		return runDemo(ctx, stack)
	}

	stack.Serve(ctx)
	return nil
}

// runDemo starts one run and waits for it, so that `make demo` is a single
// command that either prints a completed run or fails.
func runDemo(ctx context.Context, stack *wiring.Stack) error {
	demoCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Serve in the background; the demo run needs workers to execute it.
	served := make(chan struct{})
	go func() {
		defer close(served)
		stack.Serve(demoCtx)
	}()

	runID, err := stack.Engine.StartRun(demoCtx, []byte(`{"meeting_id":"M-1042"}`))
	if err != nil {
		return fmt.Errorf("start demo run: %w", err)
	}

	run, err := awaitTerminal(demoCtx, stack, runID)
	cancel()
	<-served

	if err != nil {
		return err
	}

	fmt.Printf("\nrun      %s\nstatus   %s\nversion  %d\n", run.ID, run.Status, run.Version)
	if len(run.Output) > 0 {
		fmt.Printf("output   %s\n", run.Output)
	}
	if run.LastError != "" {
		fmt.Printf("error    %s\n", run.LastError)
	}

	history, err := stack.Engine.History(context.WithoutCancel(demoCtx), runID)
	if err != nil {
		return fmt.Errorf("read history: %w", err)
	}
	fmt.Printf("\nhistory  %d events\n", len(history))
	for _, ev := range history {
		step := ev.StepID
		if step == "" {
			step = "-"
		}
		fmt.Printf("  %3d  %-16s %s\n", ev.Seq, ev.Type, step)
	}

	if run.Status != core.RunCompleted {
		return fmt.Errorf("demo run finished %s, want COMPLETED", run.Status)
	}
	return nil
}

func awaitTerminal(ctx context.Context, stack *wiring.Stack, id core.RunID) (core.Run, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return core.Run{}, fmt.Errorf("waiting for run %s: %w", id, ctx.Err())
		case <-ticker.C:
			run, err := stack.Engine.Run(ctx, id)
			if err != nil {
				return core.Run{}, err
			}
			if run.Status.IsTerminal() {
				return run, nil
			}
		}
	}
}

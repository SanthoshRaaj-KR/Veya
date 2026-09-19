// Command veya-runtime is the execution engine.
//
// It advances runs, publishes committed work, reclaims abandoned leases,
// reconciles unresolved effects, and — unless told otherwise — hosts workers
// too. Everything single-purpose lives here: one runtime process per
// deployment, with veya-worker for capacity.
//
// Usage:
//
//	veya-runtime                            # serve, workers in this process
//	veya-runtime --dispatch jetstream       # workers may live elsewhere
//	veya-runtime --dispatch postgres        # same, with no broker
//	veya-runtime --workers 0                # engine only; veya-worker executes
//	veya-runtime --demo                     # one demo run, wait for it, exit
//	veya-runtime --store memory --demo      # same, no database needed
//	veya-runtime --grpc 127.0.0.1:50551 //	  --agent refund_agent --agent-version v1   # serve an agent defined in Python
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

	"github.com/SanthoshRaaj-KR/Veya/internal/agents"
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
	fs.StringVar(&cfg.Dispatch, "dispatch", cfg.Dispatch, "task delivery: inproc, postgres or jetstream")
	fs.StringVar(&cfg.NATSURL, "nats", cfg.NATSURL, "NATS server, when --dispatch=jetstream")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "workers to run in this process; 0 leaves execution to veya-worker")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", cfg.ScanInterval, "recovery scan period")
	fs.DurationVar(&cfg.RelayInterval, "relay-interval", cfg.RelayInterval, "outbox relay backstop period")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug, info, warn or error")
	fs.StringVar(&cfg.GatewayAddr, "grpc", cfg.GatewayAddr,
		"serve the worker protocol on this address; empty leaves the port closed")
	agentName := fs.String("agent", agents.Default,
		"the agent this runtime serves; a name that is not built in is expected from a worker")
	agentVersion := fs.String("agent-version", "",
		"the agent version pinned on new runs; required for an agent defined over the protocol")
	fs.BoolVar(&demo, "demo", false, "start one demo run, wait for it to finish, then exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	cfg.Role = wiring.RoleRuntime

	agent, err := agents.Select(*agentName, *agentVersion, cfg.GatewayAddr)
	if err != nil {
		return err
	}

	// Cancelled on SIGINT/SIGTERM so shutdown is orderly: workers finish what
	// they hold, and anything still PENDING is recovered by the next process
	// to scan. Nothing durable depends on a clean exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stack, err := wiring.Build(ctx, cfg, agent)
	if err != nil {
		return err
	}
	defer func() { _ = stack.Close() }()

	stack.Logger.Info("veya-runtime starting",
		"agent", stack.Engine.Agent(), "version", agent.Version,
		"store", cfg.Store, "dispatch", cfg.Dispatch,
		"workers", cfg.Workers, "tools", stack.Tools.Names(), "grpc", cfg.GatewayAddr)

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

	effects, err := stack.Engine.Effects(context.WithoutCancel(demoCtx), runID)
	if err != nil {
		return fmt.Errorf("read effects: %w", err)
	}
	fmt.Printf("\neffects  %d ledger rows (a pure read writes none)\n", len(effects))
	for _, e := range effects {
		fmt.Printf("  %-16s %-18s %-10s %s\n", e.Key, e.Class, e.Status, e.ExternalRef)
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

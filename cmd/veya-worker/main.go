// Command veya-worker executes tasks and nothing else.
//
// It is how capacity is added: start more of these, anywhere with a route to
// the database and the broker. They claim work, run tools, and report
// outcomes. They do not sweep for stranded work, reclaim leases, or reconcile
// effects — those are single-purpose loops that belong to veya-runtime, and a
// second copy of each would not be unsafe, merely two processes doing one
// process's job.
//
// It still holds a full engine, which surprises people. Completing a task
// advances the run, and advancing asks the decider what comes next, so a worker
// needs the agent definition as much as the runtime does. That is also why the
// agent lives in internal/agents rather than inside a binary: two copies that
// drifted would have one process treating a send as QUERYABLE and the other as
// UNRECONCILABLE.
//
// Concurrent advancement between processes is safe for the same reason it is
// safe between goroutines — the compare-and-swap on the run's version lets
// exactly one caller win, and the losers observe that the work is already done.
//
// Usage:
//
//	veya-worker --dispatch postgres              # no broker needed
//	veya-worker --dispatch jetstream --nats ...  # over NATS
//	veya-worker --workers 8                      # eight in this process
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/SanthoshRaaj-KR/Veya/internal/agents"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "veya-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := wiring.DefaultConfig()
	cfg.Role = wiring.RoleWorker

	// A worker's default transport cannot be the in-process channel: there is
	// no engine in this process to be the other end of it. Validate() refuses
	// that combination, so this default is what makes the plain command work.
	cfg.Dispatch = wiring.DispatchPostgres

	fs := flag.NewFlagSet("veya-worker", flag.ExitOnError)
	fs.StringVar(&cfg.DSN, "dsn", cfg.DSN, "PostgreSQL connection string")
	fs.StringVar(&cfg.Dispatch, "dispatch", cfg.Dispatch, "task delivery: postgres or jetstream")
	fs.StringVar(&cfg.NATSURL, "nats", cfg.NATSURL, "NATS server, when --dispatch=jetstream")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "workers to run in this process")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "how long a claim lasts without a heartbeat")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug, info, warn or error")
	name := fs.String("name", hostname(), "worker name prefix, for identifying this process in leases and logs")
	fs.StringVar(&cfg.GatewayAddr, "grpc", cfg.GatewayAddr,
		"serve the worker protocol on this address, so tool hosts can attach to this process")
	agentName := fs.String("agent", agents.Default, "the agent this process executes tools for")
	agentVersion := fs.String("agent-version", "", "the agent version, for an agent defined over the protocol")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	cfg.WorkerPrefix = *name

	agent, err := agents.Select(*agentName, *agentVersion, cfg.GatewayAddr)
	if err != nil {
		return err
	}

	// Cancelled on SIGINT/SIGTERM. A worker killed mid-task loses nothing: its
	// lease lapses, the reaper reclaims the task, and the effect ledger holds
	// whatever was learned about anything already sent.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stack, err := wiring.Build(ctx, cfg, agent)
	if err != nil {
		return err
	}
	defer func() { _ = stack.Close() }()

	stack.Logger.Info("veya-worker starting",
		"agent", stack.Engine.Agent(), "version", agent.Version, "dispatch", cfg.Dispatch,
		"workers", cfg.Workers, "tools", stack.Tools.Names(), "grpc", cfg.GatewayAddr)

	stack.Serve(ctx)
	return nil
}

// hostname names the process in leases and logs. A lease pointing at
// "worker-0" is useless across a fleet; one pointing at "web-3-worker-0" says
// which machine to go and look at.
func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "worker"
	}
	return h
}

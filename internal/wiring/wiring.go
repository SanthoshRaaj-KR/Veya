// Package wiring is the composition root: the one place that names concrete
// adapters and assembles them into a working runtime.
//
// It exists so that both binaries in cmd/ stay thin and identical in how they
// build a stack. Everywhere else, code depends on core interfaces and has no
// idea whether state lives in PostgreSQL or a map.
//
// This is the package to read to find out what Veya is actually made of.
//
// Layer 3 was the test of that claim: adding PostgreSQL and JetStream dispatch
// changed OpenDispatcher below, cmd/, and nothing in engine/ or core/. Three
// transports that share no mechanism, and everything above this file is written
// against one interface and cannot tell which it got.
package wiring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/clock"
	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/inproc"
	"github.com/SanthoshRaaj-KR/Veya/internal/dispatch/jetstream"
	dispatchpg "github.com/SanthoshRaaj-KR/Veya/internal/dispatch/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/effects"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/lease"
	"github.com/SanthoshRaaj-KR/Veya/internal/outbox"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/gateway"
	"github.com/SanthoshRaaj-KR/Veya/internal/signalhttp"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/worker"
)

// Store kinds.
const (
	StoreMemory   = "memory"
	StorePostgres = "postgres"
)

// Dispatch kinds — how a committed task reaches a worker.
//
// All three are at-least-once and none of them is trusted: a delivery says
// "task T may need attention" and the worker reads what is true from the store.
// Choosing between them is an operational question, not a correctness one.
const (
	// DispatchInproc is a channel. Only workers in this process can hear it.
	DispatchInproc = "inproc"

	// DispatchPostgres polls the tasks table with SKIP LOCKED. Workers in other
	// processes, with nothing to install beyond the database.
	DispatchPostgres = "postgres"

	// DispatchJetStream pushes over NATS. Workers anywhere, without every
	// worker polling the database.
	DispatchJetStream = "jetstream"
)

// Roles — which loops a process runs.
//
// The split is about what a process is responsible for, not about capability.
// A worker process holds a full engine, because completing a task advances the
// run and advancing needs a decider; what it does not do is sweep for stranded
// work, reclaim leases, or reconcile effects. Those are single-purpose loops
// that every extra copy of merely duplicates.
const (
	RoleRuntime = "runtime" // engine loops, relay, reaper, reconciler, workers
	RoleWorker  = "worker"  // workers only
)

// DefaultDSN points at the container docker-compose.yml starts. Port 5433, not
// 5432, so Veya does not collide with a PostgreSQL already installed on the
// host — a collision there fails confusingly rather than loudly.
const DefaultDSN = "postgres://veya:veya@localhost:5433/veya?sslmode=disable"

// Config is everything needed to build a runtime.
type Config struct {
	Role         string        // runtime | worker
	Store        string        // memory | postgres
	DSN          string        // required when Store is postgres
	Dispatch     string        // inproc | postgres | jetstream
	NATSURL      string        // required when Dispatch is jetstream
	Workers      int           // in-process workers to run
	ScanInterval time.Duration // recovery loop period

	// WorkerPrefix distinguishes this process's workers from every other
	// process's.
	//
	// Worker IDs were "worker-0" when one process held them all. With workers
	// in several processes that name collides, and a colliding worker ID is not
	// cosmetic: a lease points at a worker row, so two machines would be
	// claiming tasks as the same identity and a lease would no longer say which
	// process to go and look at. Defaults to the hostname.
	WorkerPrefix string

	LeaseTTL time.Duration // how long a claim lasts without a heartbeat

	// GatewayAddr is where the worker protocol listens. Empty means this
	// process does not serve it at all, which is the default: a deployment
	// running only Go agents should not have an open port it never uses.
	//
	// The gateway is built before the engine, because an agent defined in
	// another language gets its decider and its tool registry from it.
	GatewayAddr string

	// SignalAddr is where the HTTP signal endpoint listens. Empty means this
	// process does not serve it at all -- the same "off unless asked for"
	// default GatewayAddr uses, for the same reason: a deployment that only
	// ever sends signals through `veya signal` should not have a second open
	// port it never uses.
	SignalAddr string

	// RelayInterval is the outbox relay's backstop period. The engine wakes the
	// relay on commit, so this only bounds how long work committed by another
	// process waits.
	RelayInterval time.Duration

	// ReconcileInterval and ReconcileStaleAfter govern the background sweep
	// that settles effects no live task will ever settle.
	ReconcileInterval   time.Duration
	ReconcileStaleAfter time.Duration
	LogLevel            string // debug | info | warn | error
}

// DefaultConfig returns the configuration the binaries start from.
func DefaultConfig() Config {
	return Config{
		Role:          RoleRuntime,
		Store:         StorePostgres,
		DSN:           DefaultDSN,
		Dispatch:      DispatchInproc,
		NATSURL:       jetstream.DefaultURL,
		Workers:       1,
		ScanInterval:  2 * time.Second,
		LeaseTTL:      engine.DefaultLeaseTTL,
		RelayInterval: time.Second,

		ReconcileInterval:   30 * time.Second,
		ReconcileStaleAfter: time.Minute,
		LogLevel:            "info",
	}
}

// Validate checks the configuration and fills in defaults.
//
// Unknown values fail loudly here rather than being silently ignored, so a
// typo in a flag is a startup error instead of a runtime surprise.
func (c *Config) Validate() error {
	switch c.Role {
	case "":
		c.Role = RoleRuntime
	case RoleRuntime, RoleWorker:
	default:
		return fmt.Errorf("wiring: unknown role %q (want runtime or worker)", c.Role)
	}

	switch c.Store {
	case StoreMemory:
	case StorePostgres:
		if c.DSN == "" {
			return errors.New("wiring: --dsn is required when --store=postgres")
		}
	case "":
		return errors.New("wiring: --store is required (memory or postgres)")
	default:
		return fmt.Errorf("wiring: unknown store %q (want memory or postgres)", c.Store)
	}

	switch c.Dispatch {
	case "":
		c.Dispatch = DispatchInproc
	case DispatchInproc:
	case DispatchPostgres:
		if c.Store != StorePostgres {
			return errors.New("wiring: --dispatch=postgres needs --store=postgres; " +
				"the tasks table is the queue")
		}
	case DispatchJetStream:
		if c.NATSURL == "" {
			return errors.New("wiring: --nats is required when --dispatch=jetstream")
		}
	default:
		return fmt.Errorf("wiring: unknown dispatch %q (want inproc, postgres or jetstream)", c.Dispatch)
	}

	// An in-process channel cannot reach another process. Failing here turns a
	// worker that silently never receives anything into a startup error.
	if c.Role == RoleWorker && c.Dispatch == DispatchInproc {
		return errors.New("wiring: a worker process needs --dispatch=postgres or " +
			"--dispatch=jetstream; an in-process channel has no other end")
	}
	if c.Role == RoleWorker && c.Store == StoreMemory {
		return errors.New("wiring: a worker process needs --store=postgres; " +
			"an in-memory store is not shared with the runtime")
	}

	// Zero workers is a legitimate runtime: the engine advances runs and the
	// relay publishes them, while veya-worker processes do the executing. A
	// worker process with zero workers is just a process that does nothing.
	if c.Workers < 0 {
		c.Workers = 0
	}
	if c.Role == RoleWorker && c.Workers < 1 {
		return errors.New("wiring: --workers must be at least 1 for a worker process")
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = 2 * time.Second
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = engine.DefaultLeaseTTL
	}
	if c.RelayInterval <= 0 {
		c.RelayInterval = time.Second
	}
	// A worker process may host a gateway too — a Python tool host can attach
	// to it — but it is off unless asked for, like everywhere else.
	if c.WorkerPrefix == "" {
		if host, err := os.Hostname(); err == nil && host != "" {
			c.WorkerPrefix = host
		} else {
			c.WorkerPrefix = "local"
		}
	}
	if _, err := parseLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}

// NewLogger builds the structured logger the whole process shares.
func NewLogger(level string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})), nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("wiring: unknown log level %q", s)
	}
}

// OpenStore builds the configured store.
func OpenStore(ctx context.Context, cfg Config, clk core.Clock) (core.Store, error) {
	switch cfg.Store {
	case StoreMemory:
		return memory.New(clk), nil
	case StorePostgres:
		return postgres.Open(ctx, cfg.DSN, clk)
	default:
		return nil, fmt.Errorf("wiring: unknown store %q", cfg.Store)
	}
}

// OpenDispatcher builds the configured dispatcher.
//
// This function is the whole of Layer 3's claim about the port. Three
// transports that share no mechanism — a channel, a table, a broker — and
// everything above this line is written against one interface and cannot tell
// which it got.
func OpenDispatcher(ctx context.Context, cfg Config, store core.Store, clk core.Clock, log *slog.Logger) (core.Dispatcher, error) {
	switch cfg.Dispatch {
	case DispatchInproc:
		// Buffered generously: a full channel blocks the relay, and stalling
		// the relay for want of a slot would delay every task behind it.
		return inproc.New(1024), nil

	case DispatchPostgres:
		pg, ok := store.(*postgres.Store)
		if !ok {
			return nil, errors.New("wiring: --dispatch=postgres needs the PostgreSQL store")
		}
		return dispatchpg.New(dispatchpg.Config{
			DB:         pg.DB(),
			Clock:      clk,
			Visibility: cfg.LeaseTTL,
			Logger:     log,
		})

	case DispatchJetStream:
		return jetstream.Open(ctx, jetstream.Config{
			URL:    cfg.NATSURL,
			Logger: log,
		})

	default:
		return nil, fmt.Errorf("wiring: unknown dispatch %q", cfg.Dispatch)
	}
}

// Stack is an assembled runtime.
type Stack struct {
	Role       string
	Store      core.Store
	Dispatcher core.Dispatcher
	Engine     *engine.Engine
	Runtime    *engine.Runtime
	Tools      core.ToolRegistry
	Executor   *effects.Executor
	Relay      *outbox.Relay
	Reaper     *lease.Reaper
	Reconciler *effects.Reconciler
	Workers    []*worker.Worker

	// Gateway is nil unless this process serves the worker protocol. It is
	// the only part of a Stack that other processes connect *to*, rather than
	// something this process connects to or loops over.
	Gateway *gateway.Server

	// SignalServer is nil unless this process serves HTTP signal ingestion.
	// Like Gateway, it is something other processes connect to.
	SignalServer *signalhttp.Server

	Logger *slog.Logger
}

// Agent identifies the agent a stack serves. An engine advances only its own
// agent's runs, so this is what keeps two runtimes sharing a database from
// driving each other's work through the wrong decider.
//
// Decider and Tools are interfaces rather than the concrete Go types, because
// from Layer 4 an agent may be defined somewhere else entirely: the gateway
// supplies a decider that replays a Python body and a registry whose tools
// live in another process. Nothing below this struct can tell the difference,
// which is the whole of what the worker protocol had to achieve.
type Agent struct {
	Name    string
	Version string
	Decider core.Decider
	Tools   core.ToolRegistry
}

// Build assembles a runtime that serves one agent.
func Build(ctx context.Context, cfg Config, agent Agent) (*Stack, error) {
	if agent.Name == "" || agent.Version == "" {
		return nil, errors.New("wiring: agent name and version are required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log, err := NewLogger(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	// The gateway comes first, because an agent defined in another language
	// gets its decider and its registry from it.
	var gw *gateway.Server
	if cfg.GatewayAddr != "" {
		gw, err = gateway.Listen(cfg.GatewayAddr, gateway.New(log), log)
		if err != nil {
			return nil, err
		}
	}
	agent, err = resolveAgent(agent, gw)
	if err != nil {
		if gw != nil {
			gw.Close()
		}
		return nil, err
	}

	clk := clock.System{}
	store, err := OpenStore(ctx, cfg, clk)
	if err != nil {
		return nil, fmt.Errorf("wiring: open store: %w", err)
	}

	dispatcher, err := OpenDispatcher(ctx, cfg, store, clk, log)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("wiring: open dispatcher: %w", err)
	}

	// The relay is built before the engine because the engine holds its Wake.
	relay, err := outbox.New(outbox.Config{
		Store:      store,
		Dispatcher: dispatcher,
		Clock:      clk,
		Interval:   cfg.RelayInterval,
		Logger:     log,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("wiring: build relay: %w", err)
	}

	eng, err := engine.New(engine.Config{
		Store:        store,
		Dispatcher:   dispatcher,
		Decider:      agent.Decider,
		IDGen:        idgen.Random{},
		Clock:        clk,
		LeaseTTL:     cfg.LeaseTTL,
		Agent:        agent.Name,
		AgentVersion: agent.Version,
		Wake:         relay.Wake,
		Logger:       log,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("wiring: build engine: %w", err)
	}

	// Every tool call goes through the ledger, so that a duplicate attempt is
	// harmless rather than merely unlikely.
	executor, err := effects.New(effects.Config{
		Store: store,
		Tools: agent.Tools,
		IDGen: idgen.Random{},
		Clock: clk,
		Log:   log,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("wiring: build executor: %w", err)
	}

	reaper, err := lease.New(lease.Config{
		Store:    store,
		Clock:    clk,
		Interval: cfg.ScanInterval,
		Wake:     relay.Wake,
		Logger:   log,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("wiring: build reaper: %w", err)
	}

	reconciler, err := effects.NewReconciler(effects.ReconcilerConfig{
		Store:      store,
		Tools:      agent.Tools,
		Clock:      clk,
		Interval:   cfg.ReconcileInterval,
		StaleAfter: cfg.ReconcileStaleAfter,
		Logger:     log,
	})
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("wiring: build reconciler: %w", err)
	}

	workers := make([]*worker.Worker, 0, cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w, err := worker.New(worker.Config{
			ID:         fmt.Sprintf("%s-worker-%d", cfg.WorkerPrefix, i),
			Engine:     eng,
			Dispatcher: dispatcher,
			Executor:   executor,
			Logger:     log,
		})
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("wiring: build worker: %w", err)
		}
		workers = append(workers, w)
	}

	var sigSrv *signalhttp.Server
	if cfg.SignalAddr != "" {
		sigSrv, err = signalhttp.Listen(cfg.SignalAddr, eng, log)
		if err != nil {
			_ = store.Close()
			if gw != nil {
				gw.Close()
			}
			return nil, fmt.Errorf("wiring: build signal server: %w", err)
		}
	}

	return &Stack{
		Role:         cfg.Role,
		Store:        store,
		Dispatcher:   dispatcher,
		Engine:       eng,
		Runtime:      engine.NewRuntime(engine.RuntimeConfig{Engine: eng, Interval: cfg.ScanInterval}),
		Tools:        agent.Tools,
		Executor:     executor,
		Relay:        relay,
		Reaper:       reaper,
		Reconciler:   reconciler,
		Workers:      workers,
		Gateway:      gw,
		SignalServer: sigSrv,
		Logger:       log,
	}, nil
}

// resolveAgent fills in a decider and a registry for an agent that did not
// bring its own.
//
// An agent with neither is one defined over the worker protocol: its body and
// its tools live in another process, and this process learns what they are
// when that process connects. An agent with exactly one of the two is a wiring
// mistake, and guessing which half was meant would produce a runtime that
// half-works.
func resolveAgent(agent Agent, gw *gateway.Server) (Agent, error) {
	switch {
	case agent.Decider != nil && agent.Tools != nil:
		return agent, nil

	case agent.Decider != nil || agent.Tools != nil:
		return Agent{}, fmt.Errorf(
			"wiring: agent %q brought only half a definition; supply both a decider "+
				"and a tool registry, or neither and let a worker register them",
			agent.Name)

	case gw == nil:
		return Agent{}, fmt.Errorf(
			"wiring: agent %q has no decider or tools, and --grpc is not set; "+
				"an agent defined in another language needs the gateway to be listening",
			agent.Name)
	}

	g := gw.Gateway()
	agent.Decider = g.Decider(agent.Name)
	agent.Tools = g.Registry(agent.Name)
	return agent, nil
}

// Serve runs the loops this process is responsible for until ctx is cancelled.
//
// A worker process runs workers and nothing else. It still holds a full engine
// — completing a task advances the run, and advancing needs a decider — but the
// sweeps belong to the runtime process. Running a second reaper would not be
// unsafe, since the reaper obeys the same fencing rules as everyone else; it
// would simply be two processes doing one process's work.
func (s *Stack) Serve(ctx context.Context) {
	var wg sync.WaitGroup

	for _, w := range s.Workers {
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				s.Logger.Error("worker exited", "worker_id", w.ID(), "error", err)
			}
		}(w)
	}

	// Before the loops, so that a worker connecting at the same moment as the
	// first recovery scan finds somewhere to register. A runtime that swept
	// for stranded work before it could possibly have a worker would fail
	// every task it found for want of a tool.
	if s.Gateway != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Gateway.Run(ctx); err != nil {
				s.Logger.Error("worker gateway exited", "error", err)
			}
		}()
	}

	if s.SignalServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.SignalServer.Run(ctx); err != nil {
				s.Logger.Error("signal HTTP server exited", "error", err)
			}
		}()
	}

	if s.Role == RoleRuntime {
		// The relay before the recovery loop: a restart should publish what the
		// process that died already committed before it starts looking for what
		// else might be stuck.
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Relay.Run(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Runtime.Run(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Reaper.Run(ctx)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Reconciler.Run(ctx)
		}()
	}

	<-ctx.Done()
	// Closing the dispatcher unblocks workers parked in Claim, so shutdown
	// does not wait out a poll interval.
	_ = s.Dispatcher.Close()
	wg.Wait()
}

// Close releases resources.
func (s *Stack) Close() error {
	_ = s.Dispatcher.Close()
	return s.Store.Close()
}

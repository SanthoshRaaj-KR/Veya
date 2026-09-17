// Package wiring is the composition root: the one place that names concrete
// adapters and assembles them into a working runtime.
//
// It exists so that both binaries in cmd/ stay thin and identical in how they
// build a stack. Everywhere else, code depends on core interfaces and has no
// idea whether state lives in PostgreSQL or a map.
//
// This is the package to read to find out what Veya is actually made of, and
// the package that should change when Layer 3 adds JetStream dispatch — if
// that change reaches into engine/ or core/, the abstraction was wrong.
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
	"github.com/SanthoshRaaj-KR/Veya/internal/effects"
	"github.com/SanthoshRaaj-KR/Veya/internal/engine"
	"github.com/SanthoshRaaj-KR/Veya/internal/idgen"
	"github.com/SanthoshRaaj-KR/Veya/internal/lease"
	"github.com/SanthoshRaaj-KR/Veya/internal/outbox"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/memory"
	"github.com/SanthoshRaaj-KR/Veya/internal/store/postgres"
	"github.com/SanthoshRaaj-KR/Veya/internal/tool"
	"github.com/SanthoshRaaj-KR/Veya/internal/worker"
)

// Store kinds.
const (
	StoreMemory   = "memory"
	StorePostgres = "postgres"
)

// DefaultDSN points at the container docker-compose.yml starts. Port 5433, not
// 5432, so Veya does not collide with a PostgreSQL already installed on the
// host — a collision there fails confusingly rather than loudly.
const DefaultDSN = "postgres://veya:veya@localhost:5433/veya?sslmode=disable"

// Config is everything needed to build a runtime.
type Config struct {
	Store        string        // memory | postgres
	DSN          string        // required when Store is postgres
	Workers      int           // in-process workers to run
	ScanInterval time.Duration // recovery loop period
	LeaseTTL     time.Duration // how long a claim lasts without a heartbeat

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
		Store:         StorePostgres,
		DSN:           DefaultDSN,
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

	if c.Workers < 1 {
		c.Workers = 1
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

// Stack is an assembled runtime.
type Stack struct {
	Store      core.Store
	Dispatcher core.Dispatcher
	Engine     *engine.Engine
	Runtime    *engine.Runtime
	Tools      *tool.Registry
	Executor   *effects.Executor
	Relay      *outbox.Relay
	Reaper     *lease.Reaper
	Reconciler *effects.Reconciler
	Workers    []*worker.Worker
	Logger     *slog.Logger
}

// Agent identifies the agent a stack serves. An engine advances only its own
// agent's runs, so this is what keeps two runtimes sharing a database from
// driving each other's work through the wrong decider.
type Agent struct {
	Name    string
	Version string
	Decider core.Decider
	Tools   *tool.Registry
}

// Build assembles a runtime that serves one agent.
func Build(ctx context.Context, cfg Config, agent Agent) (*Stack, error) {
	if agent.Name == "" || agent.Version == "" {
		return nil, errors.New("wiring: agent name and version are required")
	}
	if agent.Decider == nil || agent.Tools == nil {
		return nil, errors.New("wiring: agent decider and tools are required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log, err := NewLogger(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	clk := clock.System{}
	store, err := OpenStore(ctx, cfg, clk)
	if err != nil {
		return nil, fmt.Errorf("wiring: open store: %w", err)
	}

	// Buffered generously: a full channel blocks the relay, and stalling the
	// relay for want of a slot would delay every committed task behind it.
	dispatcher := inproc.New(1024)

	// The relay is built before the engine because the engine holds its Wake.
	relay, err := outbox.New(outbox.Config{
		Store:      store,
		Dispatcher: dispatcher,
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
			ID:         fmt.Sprintf("worker-%d", i),
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

	return &Stack{
		Store:      store,
		Dispatcher: dispatcher,
		Engine:     eng,
		Runtime:    engine.NewRuntime(engine.RuntimeConfig{Engine: eng, Interval: cfg.ScanInterval}),
		Tools:      agent.Tools,
		Executor:   executor,
		Relay:      relay,
		Reaper:     reaper,
		Reconciler: reconciler,
		Workers:    workers,
		Logger:     log,
	}, nil
}

// Serve runs the workers and the recovery loop until ctx is cancelled.
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

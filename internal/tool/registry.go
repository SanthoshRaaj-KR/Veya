// Package tool holds the registry that maps a task type to the code that runs
// it.
//
// Layer 1 registers Go functions directly. Layer 4 registers tools declared in
// the Python SDK over the worker protocol; both satisfy core.ToolRegistry, so
// the worker loop does not change when that happens.
//
// From Layer 2 the descriptor gains an effect class — whether an ambiguous
// outcome may be retried, must be queried, or has to be escalated to a human.
// That belongs on the descriptor rather than in a switch statement upstream:
// a `switch taskType` anywhere above this package is the coupling the port
// exists to prevent.
package tool

import (
	"fmt"
	"sort"
	"sync"

	"github.com/santhoshraajkr/veya/internal/core"
)

// Registry is a concurrency-safe name to handler map.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]core.ToolHandler
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{tools: make(map[string]core.ToolHandler)}
}

// Register adds a tool. Registering the same name twice panics: it is always a
// wiring mistake, and discovering it at startup is far better than discovering
// it when the wrong handler runs against a customer's account.
func (r *Registry) Register(name string, h core.ToolHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tools[name]; exists {
		panic(fmt.Sprintf("tool: %q registered twice", name))
	}
	r.tools[name] = h
}

// Lookup resolves a task type. Returns ErrToolNotFound if no worker here can
// run it.
func (r *Registry) Lookup(name string) (core.ToolHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, ok := r.tools[name]
	if !ok {
		return nil, fmt.Errorf("tool %q: %w", name, core.ErrToolNotFound)
	}
	return h, nil
}

// Names lists registered tools in sorted order, for startup logs and the CLI.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.tools))
	for name := range r.tools {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Package tool holds the registry that maps a task type to what the runtime
// needs to know about it.
//
// A tool is not just a function. It also declares what happens if its outcome
// is ever ambiguous — whether a repeat is safe, whether the provider can be
// asked what it did, or whether only a human can settle it. That declaration
// travels on the descriptor, so the engine reads behaviour off the tool rather
// than switching on its name.
//
// Layer 1 registered Go functions. Layer 4 registers tools declared in the
// Python SDK over the worker protocol; both satisfy core.ToolRegistry, so the
// worker loop does not change when that happens.
package tool

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Registry is a concurrency-safe name to descriptor map.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]core.ToolDescriptor
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{tools: make(map[string]core.ToolDescriptor)}
}

// Register adds a tool.
//
// Registering the same name twice panics, and a descriptor that contradicts
// itself panics too. Both are wiring mistakes, and a panic at startup is far
// better than discovering at runtime that a tool the operator believed was
// queryable has no way to answer — which would surface as a run parked
// forever, or worse, as a duplicate.
func (r *Registry) Register(d core.ToolDescriptor) {
	if d.Name == "" {
		panic("tool: descriptor has no name")
	}
	if d.Handler == nil {
		panic(fmt.Sprintf("tool: %q has no handler", d.Name))
	}
	if !d.Class.Valid() {
		panic(fmt.Sprintf("tool: %q has invalid effect class %q", d.Name, d.Class))
	}
	if d.Class == core.ClassQueryable && d.Reconciler == nil {
		panic(fmt.Sprintf("tool: %q is QUERYABLE but has no reconciler; "+
			"a tool that cannot be asked what it did is UNRECONCILABLE, not QUERYABLE", d.Name))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tools[d.Name]; exists {
		panic(fmt.Sprintf("tool: %q registered twice", d.Name))
	}
	r.tools[d.Name] = d
}

// Func registers a pure read — a tool with no external consequence.
//
// These bypass the effect ledger entirely, so this shorthand exists to make
// the common harmless case easy to declare correctly.
func (r *Registry) Func(name string, h core.ToolHandler) {
	r.Register(core.ToolDescriptor{Name: name, Class: core.ClassNone, Handler: h})
}

// Effectful registers a tool that changes the outside world.
func (r *Registry) Effectful(name string, class core.EffectClass, keyTTL time.Duration, h core.ToolHandler, rec core.Reconciler) {
	r.Register(core.ToolDescriptor{
		Name:       name,
		Class:      class,
		Handler:    h,
		Reconciler: rec,
		KeyTTL:     keyTTL,
	})
}

// Lookup resolves a task type. Returns ErrToolNotFound if no worker here can
// run it.
func (r *Registry) Lookup(name string) (core.ToolDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d, ok := r.tools[name]
	if !ok {
		return core.ToolDescriptor{}, fmt.Errorf("tool %q: %w", name, core.ErrToolNotFound)
	}
	return d, nil
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

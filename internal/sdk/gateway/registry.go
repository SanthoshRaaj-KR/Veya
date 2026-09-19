package gateway

import (
	"context"
	"fmt"
	"sort"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// Registry returns a core.ToolRegistry backed by whatever workers are
// currently connected for an agent.
//
// It satisfies the same interface as internal/tool.Registry, so the executor,
// the reconciler and the worker loop cannot tell whether a tool is a Go
// function in this process or a Python function three machines away. The one
// visible difference is that this registry's contents change while the process
// runs, which is why Lookup resolves at call time rather than at startup.
func (g *Gateway) Registry(agent string) core.ToolRegistry {
	return &remoteRegistry{gw: g, agent: agent}
}

type remoteRegistry struct {
	gw    *Gateway
	agent string
}

// Lookup resolves a task type to a descriptor whose handler and reconciler
// call back over the stream.
//
// A tool nobody has registered is ErrToolNotFound, exactly as an unknown Go
// tool would be. The engine then fails the task and retries it, which is the
// correct treatment of a worker that has not connected yet: the task stays
// durable and the next worker to arrive gets it.
func (r *remoteRegistry) Lookup(name string) (core.ToolDescriptor, error) {
	declared, err := r.gw.declaration(r.agent, name)
	if err != nil {
		return core.ToolDescriptor{}, err
	}

	descriptor := declared
	descriptor.Handler = r.handler(name, declared.Class)
	if declared.Class == core.ClassQueryable {
		descriptor.Reconciler = r.reconciler(name, declared.Class)
	}
	return descriptor, nil
}

// Names lists every tool any connected worker offers for this agent.
func (r *remoteRegistry) Names() []string { return r.gw.toolNames(r.agent) }

// handler performs one call on whichever worker is free.
//
// The session is chosen at call time, not at Lookup time. A descriptor may be
// held across a lease's worth of work, and the session that answered Lookup
// may have disconnected since; resolving late means a disconnection costs one
// call rather than every call made with a stale descriptor.
func (r *remoteRegistry) handler(name string, class core.EffectClass) core.ToolHandler {
	return func(ctx context.Context, call core.ToolCall) ([]byte, error) {
		sess, err := r.gw.pickForTool(r.agent, name, class)
		if err != nil {
			return nil, err
		}

		reply, err := resultOf(sess.call(ctx, func(callID string) *pb.ServerMessage {
			return &pb.ServerMessage{Message: &pb.ServerMessage_Execute{
				Execute: wire.ExecuteRequest(callID, name, call),
			}}
		}))
		if err != nil {
			// Not wrapped in core.NotExecuted, and that is the decision this
			// whole path turns on. A stream that died, a context that expired
			// or a worker that vanished all mean the request may have been
			// transmitted and the answer lost. The executor classifies this as
			// UNKNOWN and reconciles; classifying it as FAILED would retry
			// something that may already have taken effect.
			return nil, fmt.Errorf("tool %s: %w", name, err)
		}

		result := reply.GetExecuteResult()
		if result == nil {
			return nil, fmt.Errorf("tool %s: %w: expected an ExecuteResult, got %T",
				name, wire.ErrProtocol, reply.GetMessage())
		}
		if failure := failureOf(result.GetFailure()); failure != nil {
			// wire.Failure preserved whatever the worker asserted about
			// certainty, including the NOT_EXECUTED claim if it made one.
			return nil, failure
		}
		return result.GetResult(), nil
	}
}

// reconciler asks the provider, through a worker, whether an action under this
// key actually happened.
//
// It is only ever built for a QUERYABLE tool. The other classes have no use
// for one: IDEMPOTENT_BY_KEY resolves by re-sending, NONE has nothing to
// resolve, and UNRECONCILABLE is unresolvable by definition.
func (r *remoteRegistry) reconciler(name string, class core.EffectClass) core.Reconciler {
	return core.ReconcilerFunc(func(ctx context.Context, e core.Effect) (core.Resolution, error) {
		sess, err := r.gw.pickForTool(r.agent, name, class)
		if err != nil {
			return core.Resolution{}, err
		}

		reply, err := resultOf(sess.call(ctx, func(callID string) *pb.ServerMessage {
			return &pb.ServerMessage{Message: &pb.ServerMessage_Reconcile{
				Reconcile: &pb.ReconcileRequest{CallId: callID, Tool: name, Effect: wire.Effect(e)},
			}}
		}))
		if err != nil {
			return core.Resolution{}, fmt.Errorf("reconcile %s: %w", e.Key, err)
		}

		result := reply.GetReconcileResult()
		if result == nil {
			return core.Resolution{}, fmt.Errorf("reconcile %s: %w: expected a ReconcileResult, got %T",
				e.Key, wire.ErrProtocol, reply.GetMessage())
		}
		if failure := failureOf(result.GetFailure()); failure != nil {
			// A reconciler that errored has established nothing. Returning the
			// error rather than a guess leaves the effect UNKNOWN, which is
			// what it is, and the sweep will ask again.
			return core.Resolution{}, fmt.Errorf("reconcile %s: %w", e.Key, failure)
		}
		return wire.Resolution(result.GetResolution()), nil
	})
}

// --- session selection ----------------------------------------------------

// declaration returns what the connected workers say a tool is.
//
// Workers serving the same agent must agree about a tool's class. Two
// processes disagreeing — one treating a send as QUERYABLE, another as
// UNRECONCILABLE — is the failure docs/worker-protocol.md §6 exists to prevent,
// and it is exactly the kind that is invisible until an outage. Here it is a
// refusal with both claims named.
func (g *Gateway) declaration(agent, tool string) (core.ToolDescriptor, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var (
		found   bool
		agreed  core.ToolDescriptor
		fromWho string
	)
	for _, s := range g.byAgent[agent] {
		d, ok := s.tools[tool]
		if !ok {
			continue
		}
		if !found {
			found, agreed, fromWho = true, d, s.worker
			continue
		}
		if d.Class != agreed.Class {
			return core.ToolDescriptor{}, fmt.Errorf(
				"%w: workers disagree about tool %q of agent %q: %s calls it %s, %s calls it %s",
				wire.ErrProtocol, tool, agent, fromWho, agreed.Class, s.worker, d.Class)
		}
	}
	if !found {
		return core.ToolDescriptor{}, fmt.Errorf("tool %q: %w", tool, core.ErrToolNotFound)
	}
	return agreed, nil
}

// pickForTool returns a worker offering a tool, refusing one whose declaration
// has changed since the descriptor was looked up.
//
// The class is re-checked rather than trusted. A worker can disconnect and a
// differently-configured one connect between Lookup and the call, and running
// an effect under the wrong class is the difference between reconciling an
// ambiguous send and silently re-sending it.
func (g *Gateway) pickForTool(agent, tool string, class core.EffectClass) (*session, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	candidates := make([]*session, 0, len(g.byAgent[agent]))
	for _, s := range g.byAgent[agent] {
		d, ok := s.tools[tool]
		if !ok {
			continue
		}
		if d.Class != class {
			return nil, fmt.Errorf(
				"%w: worker %s now declares tool %q as %s, but this call was prepared as %s",
				wire.ErrProtocol, s.worker, tool, d.Class, class)
		}
		candidates = append(candidates, s)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("tool %q for agent %q: %w", tool, agent, core.ErrToolNotFound)
	}

	key := agent + "\x00" + tool
	i := g.rr[key] % len(candidates)
	g.rr[key] = (i + 1) % len(candidates)
	return candidates[i], nil
}

// toolNames returns the union of what every connected worker offers, sorted.
func (g *Gateway) toolNames(agent string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, s := range g.byAgent[agent] {
		for name := range s.tools {
			seen[name] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

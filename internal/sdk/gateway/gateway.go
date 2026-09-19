// Package gateway serves the worker protocol.
//
// It is the near side of the boundary described in docs/worker-protocol.md: a
// worker in another language dials in, registers an agent and its tools, and
// from then on answers requests the runtime sends down the stream.
//
// # What this package is, structurally
//
// Two adapters and the plumbing between them. The plumbing is here; the
// adapters are in decider.go and registry.go, and they implement core.Decider
// and core.ToolRegistry — interfaces that existed before this package did and
// did not change to accommodate it. That is the whole claim Layer 4 makes
// about its own coupling, and it is checkable: delete this package and the
// runtime still builds, still passes its tests, and still runs the Go demo
// agent.
//
// # What it deliberately does not do
//
// It does not claim tasks, hold leases, issue fencing tokens, or touch the
// effect ledger. Those stay in Go, in the packages that already have them,
// for the reason set out in docs/worker-protocol.md §2.1: the
// reserve-commit-act ordering must have exactly one implementation, and this
// is the layer that would otherwise give it a second one in a language with
// no compiler to check it.
package gateway

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// Version identifies this build to connecting workers. It appears in logs on
// both sides, which is the only thing that makes "worked yesterday" a
// tractable question.
const Version = "veya-gateway/1"

// Errors a caller above this package may reasonably branch on.
var (
	// ErrNoWorker reports that no worker is currently registered for an agent.
	//
	// It is a liveness failure, not a correctness one, and it is transient by
	// nature: the task stays PENDING and the next worker to connect gets it.
	ErrNoWorker = errors.New("veya: no worker is registered for this agent")

	// ErrAgentVersion reports that the registered worker serves a different
	// version of the agent than the run was pinned to.
	//
	// This is the loud failure docs/worker-protocol.md §6 promises. A v2 body
	// may have different steps, so letting it decide for a v1 run would fork
	// the run at replay — silently, and only visible later as a duplicate or a
	// missing step.
	ErrAgentVersion = errors.New("veya: the run was started under a different agent version")
)

// Gateway holds the registered worker sessions.
type Gateway struct {
	log *slog.Logger

	mu       sync.RWMutex
	sessions map[string]*session   // by session id
	byAgent  map[string][]*session // by agent name, in registration order
	nextID   uint64
	rr       map[string]int // round-robin cursor per agent
}

// New returns a Gateway with no sessions.
func New(log *slog.Logger) *Gateway {
	if log == nil {
		log = slog.Default()
	}
	return &Gateway{
		log:      log,
		sessions: make(map[string]*session),
		byAgent:  make(map[string][]*session),
		rr:       make(map[string]int),
	}
}

// Session implements the gRPC service. It runs for the life of one worker
// connection and returns when the stream ends.
func (g *Gateway) Session(srv pb.Worker_SessionServer) error { return g.serve(srv) }

// serve is Session against the narrow stream interface, so that tests can
// drive it without a gRPC server.
func (g *Gateway) serve(s stream) error {
	sess, err := g.register(s)
	if err != nil {
		// A refused registration closes the stream with a status the client
		// can print. It is never retried into a working state by retrying, so
		// saying exactly what was wrong is the entire remedy.
		g.log.Warn("worker registration refused", "error", err)
		return status.Error(codes.InvalidArgument, err.Error())
	}

	defer g.remove(sess)
	g.log.Info("worker registered",
		"session", sess.id, "agent", sess.agent, "version", sess.version,
		"worker_id", sess.worker, "runtime", sess.runtime,
		"tools", len(sess.tools), "decides", sess.decides)

	err = g.pump(sess)
	g.log.Info("worker disconnected",
		"session", sess.id, "agent", sess.agent, "worker_id", sess.worker, "reason", err)
	return err
}

// register reads the opening message and admits the worker, or does not.
//
// Everything about a worker that the runtime will ever believe is established
// here: which agent it serves, which version, and what its tools are declared
// to be. Nothing later in the stream can change any of it, which is what makes
// a tool's effect class a fact about the session rather than a claim per call.
func (g *Gateway) register(s stream) (*session, error) {
	first, err := s.Recv()
	if err != nil {
		return nil, fmt.Errorf("reading registration: %w", err)
	}

	reg := first.GetRegister()
	if reg == nil {
		return nil, fmt.Errorf("%w: the first message on a session must be a Register", wire.ErrProtocol)
	}
	if v := reg.GetProtocolVersion(); v != pb.ProtocolVersion_PROTOCOL_VERSION_1 {
		return nil, fmt.Errorf("%w: protocol version %q is not one this runtime speaks",
			wire.ErrProtocol, v.String())
	}
	switch {
	case reg.GetAgentName() == "":
		return nil, fmt.Errorf("%w: Register has no agent name", wire.ErrProtocol)
	case reg.GetAgentVersion() == "":
		return nil, fmt.Errorf("%w: agent %q registered without a version; "+
			"a run pins the version it started under and cannot pin nothing",
			wire.ErrProtocol, reg.GetAgentName())
	}

	tools, err := toolsOf(reg)
	if err != nil {
		return nil, err
	}

	g.mu.Lock()
	g.nextID++
	id := fmt.Sprintf("s%d", g.nextID)
	sess := newSession(id, s, reg, tools)
	g.sessions[id] = sess
	g.byAgent[sess.agent] = append(g.byAgent[sess.agent], sess)
	g.mu.Unlock()

	if err := sess.send(&pb.ServerMessage{
		Message: &pb.ServerMessage_Registered{Registered: &pb.Registered{
			SessionId:      id,
			RuntimeVersion: Version,
		}},
	}); err != nil {
		g.remove(sess)
		return nil, fmt.Errorf("acknowledging registration: %w", err)
	}
	return sess, nil
}

// toolsOf converts and validates a registration's tool descriptors.
//
// A duplicate name is refused rather than resolved. The Go registry panics on
// the same mistake; here it is a connection that fails to establish, which
// says the same thing at the same moment to the same person.
func toolsOf(reg *pb.Register) (map[string]core.ToolDescriptor, error) {
	tools := make(map[string]core.ToolDescriptor, len(reg.GetTools()))
	for _, d := range reg.GetTools() {
		converted, err := wire.ToolDescriptor(d)
		if err != nil {
			return nil, fmt.Errorf("agent %q: %w", reg.GetAgentName(), err)
		}
		if _, dup := tools[converted.Name]; dup {
			return nil, fmt.Errorf("%w: agent %q registered tool %q twice",
				wire.ErrProtocol, reg.GetAgentName(), converted.Name)
		}
		tools[converted.Name] = converted
	}
	return tools, nil
}

// pump reads results off the stream and hands each to whoever is waiting.
//
// It is the only reader, which is what a gRPC stream requires. Everything it
// receives is a reply; the gateway never expects an unsolicited message from a
// worker after registration, and one that arrives is logged and dropped rather
// than treated as a fault — a client one revision ahead may send something
// this build has no opinion about, and closing the stream over it would be a
// worse answer than ignoring it.
func (g *Gateway) pump(sess *session) error {
	for {
		msg, err := sess.stream.Recv()
		if err != nil {
			cause := err
			if errors.Is(err, io.EOF) {
				cause = ErrWorkerGone
			}
			sess.close(cause)
			if errors.Is(err, io.EOF) {
				return nil // an orderly goodbye
			}
			return err
		}

		callID, ok := callIDOf(msg)
		if !ok {
			g.log.Warn("ignoring an unrecognised message from a worker",
				"session", sess.id, "worker_id", sess.worker)
			continue
		}
		if !sess.deliver(callID, msg) {
			// Almost always a caller whose context expired just before the
			// answer arrived. Worth seeing when a tool answers consistently
			// too late; not worth failing anything over.
			g.log.Debug("result arrived with nobody waiting",
				"session", sess.id, "call_id", callID)
		}
	}
}

// callIDOf extracts the correlation identifier, and reports whether the
// message was one this build knows how to route.
func callIDOf(msg *pb.ClientMessage) (string, bool) {
	switch m := msg.GetMessage().(type) {
	case *pb.ClientMessage_DecideResult:
		return m.DecideResult.GetCallId(), true
	case *pb.ClientMessage_ExecuteResult:
		return m.ExecuteResult.GetCallId(), true
	case *pb.ClientMessage_ReconcileResult:
		return m.ReconcileResult.GetCallId(), true
	default:
		return "", false
	}
}

// remove unregisters a session and wakes anything still waiting on it.
func (g *Gateway) remove(sess *session) {
	g.mu.Lock()
	delete(g.sessions, sess.id)
	remaining := g.byAgent[sess.agent][:0]
	for _, s := range g.byAgent[sess.agent] {
		if s != sess {
			remaining = append(remaining, s)
		}
	}
	if len(remaining) == 0 {
		delete(g.byAgent, sess.agent)
		delete(g.rr, sess.agent)
	} else {
		g.byAgent[sess.agent] = remaining
	}
	g.mu.Unlock()

	sess.close(ErrWorkerGone)
}

// pick returns a session for an agent, spreading calls across workers.
//
// Round-robin rather than least-loaded. Load-aware placement needs a measure
// of load, the runtime has none yet, and a wrong measure would concentrate
// work rather than spread it. Round-robin is the honest version of "no
// information": it is never clever and it is never pathological.
func (g *Gateway) pick(agent string) (*session, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	sessions := g.byAgent[agent]
	if len(sessions) == 0 {
		return nil, fmt.Errorf("%w: agent %q", ErrNoWorker, agent)
	}
	i := g.rr[agent] % len(sessions)
	g.rr[agent] = (i + 1) % len(sessions)
	return sessions[i], nil
}

// pickDecider returns a session willing to decide for an agent at a specific
// pinned version.
//
// The version check is the loud failure. A run pins its agent version at start
// and never changes it, so a worker on v2 must not be allowed to produce the
// next step for a v1 run: its body may call different tools in a different
// order, and replay would diverge in a way nothing downstream could detect.
func (g *Gateway) pickDecider(agent, version string) (*session, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	sessions := g.byAgent[agent]
	if len(sessions) == 0 {
		return nil, fmt.Errorf("%w: agent %q", ErrNoWorker, agent)
	}

	// Registered versions, for an error message that says what is actually
	// connected rather than only what is missing.
	seen := make([]string, 0, len(sessions))
	candidates := make([]*session, 0, len(sessions))
	for _, s := range sessions {
		if !s.decides {
			continue
		}
		seen = append(seen, s.version)
		if s.version == version {
			candidates = append(candidates, s)
		}
	}

	switch {
	case len(candidates) > 0:
		i := g.rr[agent] % len(candidates)
		g.rr[agent] = (i + 1) % len(candidates)
		return candidates[i], nil

	case len(seen) == 0:
		return nil, fmt.Errorf("%w: agent %q has workers, but none of them decides", ErrNoWorker, agent)

	default:
		return nil, fmt.Errorf("%w: the run is pinned to agent %q %s, and the connected workers serve %v",
			ErrAgentVersion, agent, version, seen)
	}
}

// Registered reports the agents and versions currently connected. For the CLI
// and for startup logs; nothing branches on it.
func (g *Gateway) Registered() map[string][]string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	out := make(map[string][]string, len(g.byAgent))
	for agent, sessions := range g.byAgent {
		for _, s := range sessions {
			out[agent] = append(out[agent], s.version)
		}
	}
	return out
}

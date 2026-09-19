package gateway

import (
	"context"
	"fmt"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
	"github.com/SanthoshRaaj-KR/Veya/internal/sdk/wire"
	pb "github.com/SanthoshRaaj-KR/Veya/internal/sdk/workerpb"
)

// Decider returns a core.Decider that asks a connected worker what the run
// owes next.
//
// It sits beside decider.Static and satisfies the same interface, which is the
// point: the engine gained the ability to run an agent written in another
// language without a line changing in internal/engine. The demo agent's static
// decider stays in the suite permanently, because a bug that only reproduces
// with a real agent body attached is nearly impossible to isolate.
func (g *Gateway) Decider(agent string) core.Decider { return &remoteDecider{gw: g, agent: agent} }

type remoteDecider struct {
	gw    *Gateway
	agent string
}

// Decide sends the run and its whole history to a worker and converts the
// answer.
//
// History goes whole, every time. Sending deltas would mean the worker held
// state between requests, and a worker holding the position of a run is a
// worker that restarts, loses it, and replays from a place history does not
// agree with. The replay contract in core.Decider's doc comment is only
// satisfiable if history is the sole input.
func (d *remoteDecider) Decide(ctx context.Context, run core.Run, history []core.Event) (core.Decision, error) {
	// run.AgentVersion, not the version this process was configured with. The
	// run pinned its version at start and a resumed run never changes it, so
	// the pin is what a worker has to match — otherwise a runtime upgraded to
	// v2 would happily drive every in-flight v1 run through v2's steps.
	sess, err := d.gw.pickDecider(run.AgentName, run.AgentVersion)
	if err != nil {
		return core.Decision{}, fmt.Errorf("decide for run %s: %w", run.ID, err)
	}

	reply, err := resultOf(sess.call(ctx, func(callID string) *pb.ServerMessage {
		return &pb.ServerMessage{Message: &pb.ServerMessage_Decide{Decide: &pb.DecideRequest{
			CallId:  callID,
			Run:     wire.Run(run),
			History: wire.Events(history),
		}}}
	}))
	if err != nil {
		return core.Decision{}, fmt.Errorf("decide for run %s: %w", run.ID, err)
	}

	result := reply.GetDecideResult()
	if result == nil {
		return core.Decision{}, fmt.Errorf("decide for run %s: %w: expected a DecideResult, got %T",
			run.ID, wire.ErrProtocol, reply.GetMessage())
	}
	if failure := failureOf(result.GetFailure()); failure != nil {
		// The engine turns a decider error into a failed run, which is the
		// right outcome: a run nobody will ever advance is invisible, and
		// invisible stalled work is worse than a recorded failure.
		return core.Decision{}, fmt.Errorf("agent %s: %w", d.agent, failure)
	}

	decision, err := wire.Decision(result.GetDecision())
	if err != nil {
		return core.Decision{}, fmt.Errorf("decide for run %s: %w", run.ID, err)
	}
	return decision, nil
}

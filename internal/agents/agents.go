// Package agents decides which agent a process serves.
//
// Both binaries need the same answer, and they must not answer it separately.
// A runtime that thinks the agent is one thing while a worker thinks it is
// another is the drift internal/agents/meeting exists to prevent, one level
// up: the disagreement would be about which agent, rather than about what one
// of its tools is.
package agents

import (
	"fmt"
	"sort"

	"github.com/SanthoshRaaj-KR/Veya/internal/agents/meeting"
	"github.com/SanthoshRaaj-KR/Veya/internal/wiring"
)

// Builtin names the agents defined in Go, in this repository.
//
// Everything else is defined over the worker protocol, which is to say by a
// process that has not connected yet. That is why an unknown name is not an
// error here: it is the normal case for a Python agent, and refusing it would
// mean this list had to be edited every time someone wrote an agent.
var builtin = map[string]func() wiring.Agent{
	meeting.Name: func() wiring.Agent {
		return wiring.Agent{
			Name:    meeting.Name,
			Version: meeting.Version,
			Decider: meeting.NewDecider(),
			Tools:   meeting.NewTools(),
		}
	},
}

// Default is the agent a process serves when nothing says otherwise.
const Default = meeting.Name

// Names lists the built-in agents, for flag help and startup logs.
func Names() []string {
	out := make([]string, 0, len(builtin))
	for name := range builtin {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Select returns the agent to serve.
//
// A built-in name yields a fully-defined agent. Anything else yields one with
// no decider and no tools, which wiring.Build reads as "this agent is defined
// over the worker protocol" and completes from the gateway.
//
// The version argument overrides a built-in agent's own version, which exists
// for one purpose: proving that a run pinned to v1 refuses to be decided by a
// v2 worker. Outside that test it should be left empty, because an agent
// version that does not correspond to the code is a lie told to the one
// mechanism that catches replay divergence.
func Select(name, version string, gatewayAddr string) (wiring.Agent, error) {
	if name == "" {
		name = Default
	}

	if build, ok := builtin[name]; ok {
		agent := build()
		if version != "" {
			agent.Version = version
		}
		return agent, nil
	}

	if gatewayAddr == "" {
		return wiring.Agent{}, fmt.Errorf(
			"agent %q is not built in, so it must be defined by a worker over the "+
				"protocol; start with --grpc so that worker has somewhere to connect. "+
				"Built-in agents: %v", name, Names())
	}
	if version == "" {
		return wiring.Agent{}, fmt.Errorf(
			"agent %q needs --agent-version: a run pins the version it started under, "+
				"and this process has no source for it other than you", name)
	}
	return wiring.Agent{Name: name, Version: version}, nil
}

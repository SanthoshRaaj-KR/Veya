"""Write durable agents in Python.

The runtime keeps the guarantees; this package supplies the behaviour. See
``docs/worker-protocol.md`` in the repository for why the boundary is there.
"""

from __future__ import annotations

from veya.agent import Agent, Call, Context, Decision, DecisionKind, Join, Outcome, agent
from veya.effects import Effect, EffectClass, Resolution, ResolutionKind, ToolCall
from veya.errors import (
    Cancel,
    Fail,
    NonDeterminismError,
    NotExecuted,
    ProtocolError,
    SignalTimeout,
    ToolFailed,
    VeyaError,
)
from veya.session import Worker
from veya.tools import Tool, ToolDeclarationError, tool

__all__ = [
    "Agent",
    "Call",
    "Cancel",
    "Context",
    "Decision",
    "DecisionKind",
    "Effect",
    "EffectClass",
    "Fail",
    "Join",
    "NonDeterminismError",
    "NotExecuted",
    "Outcome",
    "ProtocolError",
    "Resolution",
    "ResolutionKind",
    "SignalTimeout",
    "Tool",
    "ToolCall",
    "ToolDeclarationError",
    "ToolFailed",
    "VeyaError",
    "Worker",
    "agent",
    "tool",
]

__version__ = "0.4.0"

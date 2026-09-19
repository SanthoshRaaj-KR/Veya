"""Write durable agents in Python.

The runtime keeps the guarantees; this package supplies the behaviour. See
``docs/worker-protocol.md`` in the repository for why the boundary is there.
"""

from __future__ import annotations

from veya.agent import Agent, Context, Decision, DecisionKind, agent
from veya.effects import Effect, EffectClass, Resolution, ResolutionKind, ToolCall
from veya.errors import (
    Fail,
    NonDeterminismError,
    NotExecuted,
    ProtocolError,
    ToolFailed,
    VeyaError,
)
from veya.tools import Tool, ToolDeclarationError, tool

__all__ = [
    "Agent",
    "Context",
    "Decision",
    "DecisionKind",
    "Effect",
    "EffectClass",
    "Fail",
    "NonDeterminismError",
    "NotExecuted",
    "ProtocolError",
    "Resolution",
    "ResolutionKind",
    "Tool",
    "ToolCall",
    "ToolDeclarationError",
    "ToolFailed",
    "VeyaError",
    "agent",
    "tool",
]

__version__ = "0.4.0"

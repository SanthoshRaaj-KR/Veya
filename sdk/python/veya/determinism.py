"""A cheap check for the things that most often break replay.

An agent body has to reach its durable calls in the same order every time, or
the same logical action gets a different idempotency key and the ledger stops
protecting it. ``NonDeterminismError`` catches that when it happens. This
module tries to catch it before it happens, by looking at what the function
references.

It is a warning and not an error, on purpose. The reference may be in a branch
that never runs, or inside a helper the agent only calls for logging, and
refusing to start over a false positive would be worse than the disease. The
check that actually holds is the comparison against history; this one is here
to shorten the distance between writing the bug and hearing about it.
"""

from __future__ import annotations

import types
import warnings
from collections.abc import Callable
from typing import Any

__all__ = ["DeterminismWarning", "inspect_body"]


class DeterminismWarning(UserWarning):
    """An agent body references something that may differ between replays."""


# Modules whose every use is suspect inside an agent body, and what to use
# instead. These produce a different answer each time by design, which is the
# one property an agent body must not have.
RISKY_MODULES: dict[str, str] = {
    "random": "ctx.random(), which is seeded from the run id",
    "secrets": "a tool — if a value must be unguessable it must not be recomputed on replay",
    "uuid": "a value derived from ctx.run_id, or a value returned by a tool",
}

# Specific callables from modules that are otherwise perfectly fine. `time` and
# `datetime` are constantly used for formatting and arithmetic; it is reading
# the present moment that diverges.
RISKY_CALLS: dict[tuple[str, str], str] = {
    ("time", "time"): "ctx.now()",
    ("time", "time_ns"): "ctx.now()",
    ("time", "monotonic"): "ctx.now()",
    ("time", "perf_counter"): "ctx.now()",
    ("time", "sleep"): "a durable timer — ctx.sleep() arrives in Layer 5; until then, a tool",
    ("datetime", "now"): "ctx.now()",
    ("datetime", "utcnow"): "ctx.now()",
    ("datetime", "today"): "ctx.now()",
    ("date", "today"): "ctx.now().date()",
    ("os", "urandom"): "ctx.random()",
    ("os", "getenv"): "the run's input, or a tool — an environment can change between replays",
}


def inspect_body(fn: Callable[..., Any], agent_name: str) -> list[str]:
    """Warn about references that look like they will diverge on replay.

    Returns the findings as well as warning about them, so a test can assert on
    them without having to capture warnings.
    """
    names = _names_in(fn)
    bindings = _bindings(fn)
    findings: list[str] = []

    for name in sorted(names):
        bound = bindings.get(name)
        if bound is None:
            continue

        if isinstance(bound, types.ModuleType):
            findings.extend(_module_findings(bound, name, names))
        else:
            findings.extend(_callable_findings(bound, name, names))

    for finding in findings:
        warnings.warn(
            f"agent {agent_name!r} {finding}",
            DeterminismWarning,
            stacklevel=3,
        )
    return findings


def _module_findings(module: types.ModuleType, alias: str, names: set[str]) -> list[str]:
    """Findings for `import random` / `import datetime` style references."""
    origin = getattr(module, "__name__", "")

    if origin in RISKY_MODULES:
        return [
            f"references the {origin!r} module, which returns something different on "
            f"every replay. Use {RISKY_MODULES[origin]}."
        ]

    out = []
    for (mod, attr), alternative in RISKY_CALLS.items():
        if mod != origin and mod != alias:
            continue
        if attr in names:
            out.append(
                f"calls {alias}.{attr}(), which returns something different on every "
                f"replay. Use {alternative}."
            )
    return out


def _callable_findings(bound: Any, alias: str, names: set[str]) -> list[str]:
    """Findings for `from random import randint` / `from datetime import datetime`."""
    origin = getattr(bound, "__module__", "") or ""

    if origin in RISKY_MODULES:
        return [
            f"references {alias!r} from {origin!r}, which returns something different "
            f"on every replay. Use {RISKY_MODULES[origin]}."
        ]

    # `from datetime import datetime` binds the class, not the module, so the
    # risky part is a classmethod called on it — which shows up as a plain
    # attribute name in the same code object.
    own = getattr(bound, "__name__", alias)
    return [
        f"calls {alias}.{attr}(), which returns something different on every replay. "
        f"Use {alternative}."
        for (mod, attr), alternative in RISKY_CALLS.items()
        if mod == own and attr in names
    ]


def _bindings(fn: Callable[..., Any]) -> dict[str, Any]:
    """Everything a name in the body could resolve to.

    Module globals, plus the closure. An agent declared inside a factory
    function has its imports as free variables rather than globals, and looking
    only at globals would silently pass every such agent — which is exactly the
    kind of gap that makes a check worse than no check, because it is trusted.
    """
    env: dict[str, Any] = dict(getattr(fn, "__globals__", {}))

    code = getattr(fn, "__code__", None)
    closure = getattr(fn, "__closure__", None)
    if code is not None and closure:
        for name, cell in zip(code.co_freevars, closure, strict=False):
            try:
                env[name] = cell.cell_contents
            except ValueError:
                # An empty cell: the variable is not bound yet, which happens
                # inside a recursive definition. Nothing to inspect.
                continue

    return env


def _names_in(fn: Callable[..., Any]) -> set[str]:
    """Every global and attribute name the function's code mentions.

    Nested code objects are walked too — a comprehension, a lambda, or an inner
    function is still part of the body as far as replay is concerned, and an
    author who moved ``datetime.now()`` into a comprehension did not thereby
    make it deterministic.

    Free variables count as names. An agent declared inside a factory function
    reaches its imports through the closure rather than through globals.
    """
    code = getattr(fn, "__code__", None)
    if code is None:
        return set()

    names: set[str] = set()
    pending = [code]
    seen: set[int] = set()

    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))

        names.update(current.co_names)
        names.update(current.co_freevars)
        pending.extend(c for c in current.co_consts if isinstance(c, types.CodeType))

    return names

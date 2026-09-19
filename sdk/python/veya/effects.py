"""What a tool declares about its consequences, and what it can be asked later.

These mirror the Go types of the same names exactly. They are re-declared here
rather than derived from the generated protobuf enums because an agent author
reads them constantly, and ``EffectClass.QUERYABLE`` says something that
``worker_pb2.EFFECT_CLASS_QUERYABLE`` does not.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "Effect",
    "EffectClass",
    "Resolution",
    "ToolCall",
]


class EffectClass(enum.Enum):
    """What can be done about this tool's outcome when it is ambiguous.

    The choice is a statement about the *provider*, not about the code. A tool
    is IDEMPOTENT_BY_KEY because the API it calls deduplicates by idempotency
    key — not because calling it twice feels harmless.
    """

    NONE = "NONE"
    """A pure read, with no external consequence.

    It bypasses the effect ledger entirely, gets no idempotency key, and may be
    run any number of times. Running it twice costs latency and nothing else.

    Be honest about this one. A model call looks like a harmless question and
    is not: it is billed, it is non-deterministic, and the provider cannot
    replay it. Classify it IDEMPOTENT_BY_KEY.
    """

    IDEMPOTENT_BY_KEY = "IDEMPOTENT_BY_KEY"
    """The provider honours an idempotency key, so a re-send is safe.

    Your handler must accept ``idempotency_key`` and actually send it. The
    class is a claim about what happens when the same key arrives twice, and
    the claim is false unless the key goes out with the request.
    """

    QUERYABLE = "QUERYABLE"
    """The provider can be asked afterwards whether it did the thing.

    Requires a reconcile hook, registered with ``@the_tool.reconcile``. A
    QUERYABLE tool without one is refused at registration, because a tool that
    cannot be asked what it did is UNRECONCILABLE whatever it calls itself.

    The hook must *query*, never perform. One that executes turns the
    reconciliation of an already-committed effect into the duplicate the whole
    ledger exists to prevent.
    """

    UNRECONCILABLE = "UNRECONCILABLE"
    """The provider neither deduplicates nor answers questions.

    An ambiguous outcome here cannot be settled by any runtime. It escalates to
    a human, and the run parks until someone resolves it with
    ``veya effects resolve``. That is not a gap in the design; it is the honest
    treatment of an action nobody can check.
    """


class ResolutionKind(enum.Enum):
    """What a reconcile hook established."""

    COMMITTED = "COMMITTED"
    """It definitely happened. Return the provider's record of it."""

    NOT_EXECUTED = "NOT_EXECUTED"
    """It definitely did not happen, so performing it now is safe."""

    STILL_UNKNOWN = "STILL_UNKNOWN"
    """Still cannot tell.

    A valid and useful answer. The effect stays UNKNOWN and will be asked about
    again, or escalated once the provider's key retention has expired. Guessing
    in either direction would be worse than saying this.
    """


@dataclass(frozen=True, slots=True)
class Resolution:
    """A reconcile hook's answer.

    Build one with the constructors rather than by hand, so the kind and the
    fields that go with it cannot disagree.
    """

    kind: ResolutionKind
    response: Any = None
    external_ref: str = ""
    detail: str = ""

    @staticmethod
    def committed(response: Any = None, external_ref: str = "", detail: str = "") -> Resolution:
        """It happened. ``response`` becomes the step's result on replay."""
        return Resolution(
            kind=ResolutionKind.COMMITTED,
            response=response,
            external_ref=external_ref,
            detail=detail or "found by idempotency key",
        )

    @staticmethod
    def not_executed(detail: str = "") -> Resolution:
        """It did not happen.

        Only say this when the provider genuinely has no record of the key
        *and* is still within its retention window. Past that window a "not
        found" means the provider forgot, not that nothing happened, and the
        runtime escalates rather than taking your word for it.
        """
        return Resolution(
            kind=ResolutionKind.NOT_EXECUTED,
            detail=detail or "no record for this idempotency key",
        )

    @staticmethod
    def unknown(detail: str = "") -> Resolution:
        """Cannot tell. Returning ``None`` from a hook means this too."""
        return Resolution(kind=ResolutionKind.STILL_UNKNOWN, detail=detail)


@dataclass(frozen=True, slots=True)
class ToolCall:
    """Everything the runtime tells a tool about the call it is making.

    Handlers usually take their arguments unpacked from the payload and never
    see this. It is available for a tool that wants to log its own retries, and
    for a reconcile hook that needs the key.
    """

    run_id: str
    task_id: str
    step_id: str
    idempotency_key: str
    """Stable across every retry, reassignment and replay of this logical step.

    Empty for a NONE-class tool, which has no ledger row and nothing to
    deduplicate. For every other class this is what makes the class mean
    anything: hand it to the provider as ``Idempotency-Key`` or its equivalent.
    """

    payload: dict[str, Any] = field(default_factory=dict)
    attempt: int = 1
    """Which try this is, from 1. For logging. Nothing about correctness
    depends on it, and a tool that branches on it is usually about to
    reintroduce the problem the ledger solves."""


@dataclass(frozen=True, slots=True)
class Effect:
    """A ledger row, as a reconcile hook sees it."""

    effect_id: str
    run_id: str
    task_id: str
    effect_type: str
    effect_class: EffectClass
    idempotency_key: str
    status: str
    external_ref: str = ""
    request: Any = None
    response: Any = None
    created_at_unix_nano: int = 0

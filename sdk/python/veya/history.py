"""Reading a run's recorded history.

History is the only input a decision may depend on. Everything in this module
exists to turn the event log into a form the replay driver can consult by
logical position, without ever needing to remember anything between requests.

That constraint is the whole of the replay contract, stated in Go on
``core.Decider``:

    Past decisions are replayed. Future decisions are generated. A decider that
    consults history and returns the first unrecorded step is replay-safe; one
    that ignores history and counts its own invocations is not, and will fork a
    run on recovery.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from veya.errors import ProtocolError

__all__ = ["Event", "History", "RecordedStep", "RecordedWait"]

# The event payload envelope this build understands: {"v": 1, "data": {...}}.
#
# Kept in step with core.PayloadVersion in Go. An unrecognised version is an
# error rather than an optimistic decode, because a newer writer must not have
# its history silently misinterpreted by an older reader — and misinterpreted
# history is a forked run.
PAYLOAD_VERSION = 1

RUN_STARTED = "RUN_STARTED"
TASK_CREATED = "TASK_CREATED"
TASK_COMPLETED = "TASK_COMPLETED"
TASK_FAILED = "TASK_FAILED"

TIMER_SET = "TIMER_SET"
TIMER_FIRED = "TIMER_FIRED"
SIGNAL_WAIT_STARTED = "SIGNAL_WAIT_STARTED"
SIGNAL_RECEIVED = "SIGNAL_RECEIVED"
SIGNAL_WAIT_TIMED_OUT = "SIGNAL_WAIT_TIMED_OUT"


@dataclass(frozen=True, slots=True)
class Event:
    """One immutable fact in a run's history."""

    run_id: str
    seq: int
    type: str
    step_id: str
    payload: bytes
    created_at_unix_nano: int

    def data(self) -> dict[str, Any]:
        """Unwrap the versioned envelope and return the body."""
        try:
            envelope = json.loads(self.payload or b"{}")
        except json.JSONDecodeError as exc:
            raise ProtocolError(f"event {self.type} seq {self.seq}: payload is not JSON") from exc

        if not isinstance(envelope, dict):
            raise ProtocolError(f"event {self.type} seq {self.seq}: payload is not an object")

        version = envelope.get("v")
        if version != PAYLOAD_VERSION:
            raise ProtocolError(
                f"event {self.type} seq {self.seq} has payload version {version!r}, "
                f"and this SDK understands {PAYLOAD_VERSION}. The runtime is newer than "
                f"this package; upgrade it rather than guessing at the contents."
            )

        body = envelope.get("data")
        return body if isinstance(body, dict) else {}


@dataclass(frozen=True, slots=True)
class RecordedStep:
    """What history says about one logical step.

    ``tool`` and ``payload`` are what was *decided*, and are what a replay is
    checked against. ``result`` is what came back, and is what the replay
    returns in place of performing the call again.
    """

    step_id: str
    tool: str
    payload: dict[str, Any]
    result: Any = None
    completed: bool = False
    error: str = ""

    def describe(self) -> str:
        """A one-line rendering, for divergence messages.

        Payload keys are sorted so that two dicts with the same contents read
        identically — otherwise the error message would itself be
        non-deterministic, which is a particularly unhelpful thing for this
        error to be.
        """
        return f"{self.tool}({json.dumps(self.payload, sort_keys=True, default=str)})"


@dataclass(frozen=True, slots=True)
class RecordedWait:
    """What history says about one suspension.

    ``resolved`` is the field the body cannot work out for itself. An agent is
    forbidden to read a clock -- an answer that changes between replays
    diverges the step after the one that read it -- so it cannot tell a sleep
    that is over from one still running, or a signal that arrived from one that
    never did. The runtime observes once, records what it saw, and this is that
    record.
    """

    step_id: str
    kind: str  # "TIMER" or "SIGNAL"
    name: str = ""
    wake_at: str = ""
    resolved: bool = False
    timed_out: bool = False
    signal_id: str = ""
    payload: Any = None


@dataclass(slots=True)
class History:
    """A run's history, indexed by logical position."""

    run_id: str = ""
    agent_name: str = ""
    agent_version: str = ""
    run_input: dict[str, Any] = field(default_factory=dict)
    events: list[Event] = field(default_factory=list)
    steps: dict[str, RecordedStep] = field(default_factory=dict)
    waits: dict[str, RecordedWait] = field(default_factory=dict)

    def step(self, step_id: str) -> RecordedStep | None:
        """What history records at this position, if anything."""
        return self.steps.get(step_id)

    def wait(self, step_id: str) -> RecordedWait | None:
        """What history records about a suspension at this position."""
        return self.waits.get(step_id)

    def __len__(self) -> int:
        return len(self.events)


def read(
    events: list[Event], *, run_id: str = "", run_input: dict[str, Any] | None = None
) -> History:
    """Fold an event list into a history indexed by step.

    Indexed by step id rather than by task id, because a step is the identity
    that survives. A retried step keeps its step id and gets a new attempt; a
    reassigned one keeps it too. Task ids do not survive those things, and an
    index built on them would lose track of a step precisely when the run was
    being recovered.
    """
    history = History(run_id=run_id, run_input=dict(run_input or {}), events=list(events))

    for event in events:
        if event.type == RUN_STARTED:
            body = event.data()
            history.agent_name = str(body.get("agent_name", ""))
            history.agent_version = str(body.get("agent_version", ""))
            if not history.run_input:
                history.run_input = _as_dict(body.get("input"))

        elif event.type == TASK_CREATED:
            body = event.data()
            history.steps[event.step_id] = RecordedStep(
                step_id=event.step_id,
                tool=str(body.get("task_type", "")),
                payload=_as_dict(body.get("payload")),
            )

        elif event.type == TASK_COMPLETED:
            existing = history.steps.get(event.step_id)
            if existing is None:
                # A completion with no creation cannot happen against a real
                # store, where both are written by the same code path. Saying
                # so beats carrying on with a step whose decision is unknown,
                # because the replay check would then have nothing to compare.
                raise ProtocolError(
                    f"history records {TASK_COMPLETED} for step {event.step_id} "
                    f"with no {TASK_CREATED} before it"
                )
            history.steps[event.step_id] = RecordedStep(
                step_id=existing.step_id,
                tool=existing.tool,
                payload=existing.payload,
                result=_decode(event.data().get("result")),
                completed=True,
            )

        elif event.type == TASK_FAILED:
            body = event.data()
            existing = history.steps.get(event.step_id)
            # A non-final failure is a retry, and the step is still owed. Only
            # a final one is worth recording: the step will not complete, and
            # the agent body should see the failure where it happened.
            if existing is not None and body.get("final") and not existing.completed:
                history.steps[event.step_id] = RecordedStep(
                    step_id=existing.step_id,
                    tool=existing.tool,
                    payload=existing.payload,
                    error=str(body.get("error", "the step failed")),
                )

        elif event.type == TIMER_SET:
            body = event.data()
            history.waits[event.step_id] = RecordedWait(
                step_id=event.step_id,
                kind="TIMER",
                wake_at=str(body.get("wake_at", "")),
            )

        elif event.type == SIGNAL_WAIT_STARTED:
            body = event.data()
            history.waits[event.step_id] = RecordedWait(
                step_id=event.step_id,
                kind="SIGNAL",
                name=str(body.get("name", "")),
            )

        elif event.type in (TIMER_FIRED, SIGNAL_RECEIVED, SIGNAL_WAIT_TIMED_OUT):
            opened = history.waits.get(event.step_id)
            if opened is None:
                # A resolution with no wait before it cannot happen against a
                # real store, where the runtime writes both. Saying so beats
                # carrying on with a suspension whose terms are unknown.
                raise ProtocolError(
                    f"history records {event.type} for step {event.step_id} with no wait before it"
                )
            body = event.data()
            history.waits[event.step_id] = RecordedWait(
                step_id=opened.step_id,
                kind=opened.kind,
                name=opened.name or str(body.get("name", "")),
                wake_at=opened.wake_at,
                resolved=True,
                timed_out=event.type == SIGNAL_WAIT_TIMED_OUT,
                signal_id=str(body.get("signal_id", "")),
                payload=_decode(body.get("payload")),
            )

    return history


def _as_dict(raw: Any) -> dict[str, Any]:
    """Coerce an embedded JSON value to a dict.

    Payloads cross the wire as raw JSON inside the envelope, so they arrive
    either already decoded or as a string. A non-object payload becomes an
    empty dict rather than an error: the runtime never interprets payloads, and
    an agent that sent a bare array knows what it meant.
    """
    decoded = _decode(raw)
    return decoded if isinstance(decoded, dict) else {}


def _decode(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    return raw

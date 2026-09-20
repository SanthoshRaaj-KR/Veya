"""Declaring an agent, and replaying one against history.

An agent is an ordinary Python function. What makes it durable is that it is
never run to completion in one go: it is run repeatedly, each time against the
history recorded so far, and each time it is stopped at the first call that
history does not already have an answer for. That call becomes the next step.

This is the same shape as the Go static decider — consult history, return the
first unrecorded step — expressed as a function body instead of a list.
"""

from __future__ import annotations

import datetime as _datetime
import enum
import inspect
import json
import random as _random
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from typing import Any

from veya.determinism import inspect_body
from veya.errors import Fail, NonDeterminismError, SignalTimeout, ToolFailed, VeyaError
from veya.history import History, RecordedStep
from veya.tools import Tool

__all__ = ["Agent", "Context", "Decision", "DecisionKind", "agent"]


class DecisionKind(enum.Enum):
    """What the agent decided to do next."""

    CALL_TOOL = "CALL_TOOL"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"
    SLEEP = "SLEEP"
    WAIT_FOR_SIGNAL = "WAIT_FOR_SIGNAL"


@dataclass(frozen=True, slots=True)
class Decision:
    """One step of agent intent, expressed without reference to how it was
    arrived at."""

    kind: DecisionKind
    step_id: str = ""
    tool: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    output: Any = None
    error: str = ""

    # Set when kind is SLEEP: the instant to wake at, in nanoseconds since the
    # epoch. An absolute instant rather than a duration, because a duration
    # would be measured from whenever the runtime happened to read the
    # decision -- so a decision delayed in a queue would sleep longer than the
    # body asked, by an amount nobody can reproduce.
    wake_at_unix_nano: int = 0

    # Set when kind is WAIT_FOR_SIGNAL. A zero deadline waits indefinitely.
    signal_name: str = ""
    signal_deadline_unix_nano: int = 0


class _Suspend(Exception):
    """Raised by ``ctx.call`` when history has no answer for this step yet.

    It unwinds the agent body, which is the point: the body has reached a call
    that has not happened, so there is nothing further it can honestly compute.
    The runtime commits the call as a task, and the body is run again from the
    top once there is a result to feed it.

    An agent must never catch this. A bare ``except Exception`` around a
    ``ctx.call`` will swallow it and silently change what the run does, so this
    inherits from Exception on purpose: BaseException would dodge that, at the
    cost of also dodging every ``finally`` the author wrote.
    """

    def __init__(self, step_id: str, tool: str, payload: dict[str, Any]) -> None:
        super().__init__(f"step {step_id} ({tool}) has not run yet")
        self.step_id = step_id
        self.tool = tool
        self.payload = payload


class _Wait(Exception):
    """Raised by ``ctx.sleep`` and ``ctx.wait_for`` when the suspension they
    describe has not finished yet.

    The sibling of ``_Suspend``, and it unwinds the body for the same reason:
    the body has reached a point it cannot honestly compute past. What differs
    is that there is no task to wait on, so the runtime parks the run instead
    of dispatching work.

    An agent must never catch this. See ``_Suspend`` for why it inherits from
    Exception rather than BaseException.
    """

    def __init__(self, decision: Decision) -> None:
        super().__init__(f"step {decision.step_id} is waiting")
        self.decision = decision


class Context:
    """What an agent body is given: a way to make durable calls, and a clock.

    One context is built per decision and discarded afterwards. It holds the
    run's history and a step counter, and nothing survives between decisions —
    which is what keeps the body's behaviour a function of history alone.
    """

    __slots__ = ("_agent", "_history", "_position", "_random")

    def __init__(self, agent: Agent, history: History) -> None:
        self._agent = agent
        self._history = history
        self._position = 0
        self._random: _random.Random | None = None

    # --- the durable primitive -------------------------------------------

    async def call(self, tool: Tool | str, **payload: Any) -> Any:
        """Perform a tool call as a durable step.

        On replay this returns the recorded result without performing anything.
        The first time the body reaches a call history has no answer for, it
        stops here and the runtime turns the call into a task.

        Which means: everything before a ``ctx.call`` runs again on every
        decision. Keep it cheap, and keep it free of side effects — a print
        statement will print once per step, and anything more consequential
        than a print belongs in a tool.
        """
        name = tool.name if isinstance(tool, Tool) else tool
        self._position += 1
        step_id = f"S{self._position}"

        if isinstance(tool, Tool) and name not in self._agent.tools:
            raise VeyaError(
                f"agent {self._agent.name!r} called tool {name!r}, which it did not "
                f"register. Add it to the agent's tools= list, or the runtime will "
                f"have no worker to send it to."
            )

        recorded = self._history.step(step_id)
        if recorded is None:
            raise _Suspend(step_id, name, payload)

        _check_matches(step_id, recorded, name, payload)

        if recorded.error:
            raise ToolFailed(step_id, name, recorded.error)

        if not recorded.completed:
            # The step was decided and has not finished: it is mid-retry, or in
            # flight. There is no result to return, and returning None would
            # hand the body a value it would then compute with — producing a
            # different payload at the next step and a divergence that had
            # nothing to do with the author's code.
            #
            # Suspending is safe. Task creation is idempotent per (run, step),
            # so re-deciding the same step is recorded once and dispatched
            # once.
            raise _Suspend(step_id, name, payload)

        return recorded.result

    # --- suspension -------------------------------------------------------

    async def sleep(
        self,
        duration: _datetime.timedelta | float | None = None,
        *,
        until: _datetime.datetime | None = None,
    ) -> None:
        """Park the run until an instant, durably.

        Nothing is held anywhere while this waits. The instant goes in the
        database and the runtime's ordinary scan is what notices it has
        arrived, so a sleep survives a restart of everything -- the worker,
        the runtime and the database -- with no timer to lose.

        The wake-up is computed from :meth:`now`, which is the run's start
        time and therefore the same on every replay. A wall clock here would
        produce a different instant each time the body ran, and the step after
        this one would diverge.

        A day is an ordinary thing to pass. So is a month::

            await ctx.sleep(timedelta(days=30))
            await ctx.sleep(until=renewal_date)
        """
        if (duration is None) == (until is None):
            raise VeyaError("ctx.sleep takes a duration or until=, and exactly one of them")

        self._position += 1
        step_id = f"S{self._position}"

        recorded = self._history.wait(step_id)
        if recorded is not None and recorded.resolved:
            return  # the instant arrived; history says so
        if recorded is not None:
            # Still parked. Re-issue the suspension rather than recomputing it,
            # so a re-decision cannot move a wake-up that is already durable.
            raise _Wait(
                Decision(
                    kind=DecisionKind.SLEEP,
                    step_id=step_id,
                    wake_at_unix_nano=_nanos_from_iso(recorded.wake_at),
                )
            )

        if until is None:
            if isinstance(duration, _datetime.timedelta):
                seconds = duration.total_seconds()
            else:
                seconds = float(duration or 0.0)
            if seconds < 0:
                raise VeyaError("ctx.sleep was given a negative duration")
            until = self.now() + _datetime.timedelta(seconds=seconds)

        raise _Wait(
            Decision(
                kind=DecisionKind.SLEEP,
                step_id=step_id,
                wake_at_unix_nano=_to_nanos(until),
            )
        )

    async def wait_for(
        self,
        name: str,
        *,
        timeout: _datetime.timedelta | float | None = None,
    ) -> Any:
        """Park the run until a named signal arrives, and return its payload.

        A signal that arrived *before* the body got here is already stored, and
        this finds it on the first pass. There is no delivery to miss and no
        race to lose: the runtime stores signals on arrival and this is a read.

        Without ``timeout`` it waits indefinitely, which is the honest thing
        for a human approval and a poor idea for anything automated -- a run
        blocked on a signal nobody will ever send is invisible until somebody
        goes looking. With one, :class:`veya.errors.SignalTimeout` is raised
        at this step when the deadline passes with nothing having arrived.
        """
        if not name:
            raise VeyaError("ctx.wait_for needs a signal name")

        self._position += 1
        step_id = f"S{self._position}"

        recorded = self._history.wait(step_id)
        if recorded is not None and recorded.resolved:
            if recorded.timed_out:
                raise SignalTimeout(step_id, recorded.name or name)
            return recorded.payload

        deadline_nanos = 0
        if recorded is None and timeout is not None:
            seconds = (
                timeout.total_seconds()
                if isinstance(timeout, _datetime.timedelta)
                else float(timeout)
            )
            if seconds < 0:
                raise VeyaError("ctx.wait_for was given a negative timeout")
            deadline_nanos = _to_nanos(self.now() + _datetime.timedelta(seconds=seconds))

        raise _Wait(
            Decision(
                kind=DecisionKind.WAIT_FOR_SIGNAL,
                step_id=step_id,
                signal_name=name,
                signal_deadline_unix_nano=deadline_nanos,
            )
        )

    # --- replay-safe substitutes for things that would diverge ------------

    def now(self) -> _datetime.datetime:
        """The run's start time, in UTC.

        It is the run's start rather than the present moment because it has to
        give the same answer on every replay. A wall clock does not, and a
        timestamp that changes between replays goes into a payload, changes the
        payload, and raises NonDeterminismError at the step after the one that
        read it.

        A per-step durable clock arrives with durable timers, in Layer 5. Until
        then this is the honest thing to offer: a time that is real, related to
        the run, and stable.
        """
        started = self._history.events[0].created_at_unix_nano if self._history.events else 0
        if started <= 0:
            return _datetime.datetime.fromtimestamp(0, tz=_datetime.timezone.utc)
        return _datetime.datetime.fromtimestamp(started / 1e9, tz=_datetime.timezone.utc)

    def random(self) -> _random.Random:
        """A random source seeded from the run id.

        Deterministic across replays as long as the body draws from it in the
        same order, which is the same condition the durable calls are already
        under. Not suitable for anything that needs to be unguessable: the seed
        is the run id, which is not a secret.
        """
        if self._random is None:
            self._random = _random.Random(self._history.run_id or "veya")
        return self._random

    # --- read-only views --------------------------------------------------

    @property
    def run_id(self) -> str:
        return self._history.run_id

    @property
    def agent_version(self) -> str:
        """The version this run was pinned to when it started."""
        return self._history.agent_version or self._agent.version

    def history(self) -> History:
        """The run's recorded history. For inspection and logging."""
        return self._history

    @property
    def step_count(self) -> int:
        """How many durable calls the body has reached so far this decision."""
        return self._position


class Agent:
    """One declared agent."""

    __slots__ = ("fn", "name", "takes_input_dict", "tools", "version")

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        name: str,
        version: str,
        tools: dict[str, Tool],
    ) -> None:
        self.fn = fn
        self.name = name
        self.version = version
        self.tools = tools
        self.takes_input_dict = "input" in inspect.signature(fn).parameters

    def __repr__(self) -> str:
        return f"<Agent {self.name} {self.version} tools={sorted(self.tools)}>"

    def decide(self, history: History) -> Decision:
        """Replay the body against history and return the first unrecorded step.

        Every outcome of running the body maps to a decision:

        * it reached an unrecorded call  → CALL_TOOL
        * it reached an unfinished wait  → SLEEP or WAIT_FOR_SIGNAL
        * it returned                    → COMPLETE
        * it raised ``Fail``             → FAIL, deliberately
        * it raised ``ToolFailed``       → FAIL, carrying the step that failed
        * it raised anything else        → FAIL, with the exception's message

        A crash in the body is a failed run rather than a stalled one. A run
        nobody will ever advance is invisible, and invisible stalled work is
        worse than a recorded failure.
        """
        ctx = Context(self, history)
        try:
            output = _drive(self.fn, ctx, self._arguments(history))
        except _Suspend as suspended:
            return Decision(
                kind=DecisionKind.CALL_TOOL,
                step_id=suspended.step_id,
                tool=suspended.tool,
                payload=suspended.payload,
            )
        except _Wait as waiting:
            return waiting.decision
        except Fail as deliberate:
            return Decision(
                kind=DecisionKind.FAIL, error=str(deliberate) or "the agent failed the run"
            )
        except ToolFailed as failed:
            return Decision(kind=DecisionKind.FAIL, error=str(failed))
        except NonDeterminismError:
            # Deliberately not converted to a FAIL decision here. It is raised
            # to the session, which reports it as a failure with its full
            # message, because this is the one error an author has to see in
            # full to be able to act on it.
            raise
        except Exception as crashed:
            # A crash in the body is a run outcome, not a runtime fault.
            return Decision(
                kind=DecisionKind.FAIL,
                error=f"{type(crashed).__name__}: {crashed}",
            )

        return Decision(kind=DecisionKind.COMPLETE, output=output)

    def _arguments(self, history: History) -> dict[str, Any]:
        """Bind the run's input to the body's parameters.

        A body declaring ``input`` gets the whole dict; anything else gets the
        input unpacked as keyword arguments, which is what makes the worked
        example read like an ordinary function.
        """
        if self.takes_input_dict:
            return {"input": history.run_input}
        return dict(history.run_input)


def agent(
    *,
    name: str,
    version: str,
    tools: Iterable[Tool] = (),
    check_determinism: bool = True,
) -> Callable[[Callable[..., Any]], Agent]:
    """Declare a function as an agent.

    ``version`` is pinned onto every run this agent starts and never changes
    for the life of that run. Bump it whenever the body's sequence of durable
    calls could change — a resumed run whose agent version no longer matches is
    refused loudly by the runtime, which is a far better failure than a replay
    that diverges quietly.

    Declaring an agent also inspects its body for references that will not
    reproduce on replay, and warns about each with the alternative to use. The
    warning is not an error: see :mod:`veya.determinism` for why. Silence it
    for one agent with ``check_determinism=False``, having decided that the
    reference is in a branch that cannot affect a durable call.
    """

    def declare(fn: Callable[..., Any]) -> Agent:
        if not name:
            raise VeyaError("an agent needs a name")
        if not version:
            raise VeyaError(
                f"agent {name!r} needs a version: a run pins the version it started "
                f"under, and that pin is what catches a body edited mid-flight"
            )

        registry: dict[str, Tool] = {}
        for t in tools:
            if not isinstance(t, Tool):
                raise VeyaError(
                    f"agent {name!r} was given {t!r} as a tool; decorate it with @tool first"
                )
            if t.name in registry:
                raise VeyaError(f"agent {name!r} registered tool {t.name!r} twice")
            registry[t.name] = t

        params = list(inspect.signature(fn).parameters)
        if not params:
            raise VeyaError(
                f"agent {name!r} takes no arguments; its first parameter must be the "
                f"context, conventionally named ctx"
            )

        if check_determinism:
            inspect_body(fn, name)

        return Agent(fn, name=name, version=version, tools=registry)

    return declare


def _drive(fn: Callable[..., Any], ctx: Context, kwargs: dict[str, Any]) -> Any:
    """Run an agent body to its first suspension or to completion.

    An async body is stepped by hand rather than through an event loop. Every
    await inside it is a ``ctx.call``, which either returns immediately from
    history or raises to unwind — so the coroutine never actually yields, and
    one ``send`` runs it to its conclusion.

    A body that yields has awaited something else: real I/O, ``asyncio.sleep``,
    a task group. That is a durability bug rather than a style preference — the
    awaited thing is invisible to history and will happen again on every
    replay — so it is reported rather than driven.
    """
    result = fn(ctx, **kwargs)
    if not isinstance(result, Coroutine):
        return result

    coro: Coroutine[Any, Any, Any] = result
    try:
        coro.send(None)
    except StopIteration as done:
        return done.value

    coro.close()
    raise VeyaError(
        "the agent body awaited something other than ctx.call. Everything an agent "
        "waits on has to be a durable step, or it becomes invisible to history and "
        "happens again on every replay. Move the I/O into a tool and await "
        "ctx.call(that_tool, ...)."
    )


def _check_matches(
    step_id: str, recorded: RecordedStep, tool: str, payload: dict[str, Any]
) -> None:
    """Compare a replayed call against what history records at this position.

    This is the check the whole determinism story rests on. It cannot miss a
    divergence that changed a durable call, because the comparison is against
    the durable call itself.
    """
    attempted = RecordedStep(step_id=step_id, tool=tool, payload=payload)
    if recorded.tool != tool or _canonical(recorded.payload) != _canonical(payload):
        raise NonDeterminismError(step_id, recorded.describe(), attempted.describe())


def _canonical(payload: dict[str, Any]) -> str:
    """A comparable rendering of a payload.

    JSON with sorted keys, because a payload crosses the wire as JSON and it is
    equality *after that round trip* that matters. Comparing the dicts directly
    would call a tuple different from the list it becomes, and would report a
    divergence where the wire sees none.
    """
    return json.dumps(payload, sort_keys=True, default=str)


def _to_nanos(when: _datetime.datetime) -> int:
    """An instant as nanoseconds since the epoch.

    A naive datetime is read as UTC rather than as local time. Local time here
    would make the same agent sleep for different lengths on machines in
    different zones, which is the kind of difference that shows up once, in
    production, at the end of a month.
    """
    if when.tzinfo is None:
        when = when.replace(tzinfo=_datetime.timezone.utc)
    return int(when.timestamp() * 1_000_000_000)


def _nanos_from_iso(value: str) -> int:
    """Parse the wake instant as history records it, tolerating a trailing Z."""
    if not value:
        return 0
    try:
        return _to_nanos(_datetime.datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return 0

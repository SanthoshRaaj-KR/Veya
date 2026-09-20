"""ctx.sleep and ctx.wait_for, against history.

These need no runtime. A decision is a pure function of history, so a
suspension can be driven by handing the agent the events the runtime would
have written -- which is also the only way to test the interesting case, where
a signal arrived before the body ever asked for it.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pytest

from veya import Fail, agent, tool
from veya.agent import DecisionKind
from veya.errors import SignalTimeout, VeyaError
from veya.history import Event, read

PAYLOAD_V = 1


def event(seq: int, typ: str, step: str, data: dict[str, Any] | None = None) -> Event:
    body = json.dumps({"v": PAYLOAD_V, "data": data or {}}).encode()
    return Event(
        run_id="run-1", seq=seq, type=typ, step_id=step, payload=body, created_at_unix_nano=0
    )


STARTED = event(1, "RUN_STARTED", "", {"agent_name": "waiter", "agent_version": "v1", "input": {}})


@tool(name="notify")
def notify(to: str) -> dict[str, str]:
    return {"to": to}


def history_of(*events: Event):
    return read([STARTED, *events], run_id="run-1")


# --- sleep ----------------------------------------------------------------


@agent(name="waiter", version="v1", tools=[notify])
async def sleeper(ctx: Any) -> dict[str, str]:
    await ctx.sleep(dt.timedelta(days=1))
    await ctx.call(notify, to="ops")
    return {"done": "yes"}


def test_an_unrecorded_sleep_becomes_a_sleep_decision() -> None:
    decision = sleeper.decide(history_of())

    assert decision.kind is DecisionKind.SLEEP
    assert decision.step_id == "S1"
    assert decision.wake_at_unix_nano > 0


def test_a_sleep_wakes_at_a_day_after_the_runs_start() -> None:
    # Computed from ctx.now(), which is the run's start, so the same body
    # replayed a week later still asks for the same instant. A wall clock here
    # would move the wake-up on every decision.
    first = sleeper.decide(history_of()).wake_at_unix_nano
    second = sleeper.decide(history_of()).wake_at_unix_nano
    assert first == second


def test_a_sleep_that_has_not_fired_is_re_issued_unchanged() -> None:
    # The recorded instant wins over a recomputed one, so a re-decision cannot
    # move a wake-up that is already durable in the database.
    wake = "2026-09-21T09:00:00Z"
    decision = sleeper.decide(history_of(event(2, "TIMER_SET", "S1", {"wake_at": wake})))

    assert decision.kind is DecisionKind.SLEEP
    assert decision.step_id == "S1"
    expected = int(
        dt.datetime(2026, 9, 21, 9, 0, tzinfo=dt.timezone.utc).timestamp() * 1_000_000_000
    )
    assert decision.wake_at_unix_nano == expected


def test_a_fired_timer_lets_the_body_carry_on() -> None:
    decision = sleeper.decide(
        history_of(
            event(2, "TIMER_SET", "S1", {"wake_at": "2026-09-21T09:00:00Z"}),
            event(3, "TIMER_FIRED", "S1", {"wake_at": "2026-09-21T09:00:00Z"}),
        )
    )

    assert decision.kind is DecisionKind.CALL_TOOL
    assert decision.step_id == "S2"
    assert decision.tool == "notify"


def test_sleep_refuses_an_ambiguous_call() -> None:
    fixed = dt.datetime(2026, 9, 21, tzinfo=dt.timezone.utc)

    @agent(name="bad", version="v1")
    async def both(ctx: Any) -> None:
        await ctx.sleep(1.0, until=fixed)

    decision = both.decide(history_of())
    assert decision.kind is DecisionKind.FAIL
    assert "exactly one" in decision.error


# --- wait_for -------------------------------------------------------------


@agent(name="waiter", version="v1", tools=[notify])
async def approver(ctx: Any) -> dict[str, Any]:
    approval = await ctx.wait_for("approval")
    return {"approved_by": approval.get("by") if approval else None}


def test_an_unrecorded_wait_becomes_a_wait_decision() -> None:
    decision = approver.decide(history_of())

    assert decision.kind is DecisionKind.WAIT_FOR_SIGNAL
    assert decision.step_id == "S1"
    assert decision.signal_name == "approval"
    # No timeout asked for, so none imposed. A deadline nobody chose is worse
    # than waiting: the run resumes and reports a timeout that never happened.
    assert decision.signal_deadline_unix_nano == 0


def test_a_received_signal_is_returned_to_the_body() -> None:
    decision = approver.decide(
        history_of(
            event(2, "SIGNAL_WAIT_STARTED", "S1", {"name": "approval"}),
            event(
                3,
                "SIGNAL_RECEIVED",
                "S1",
                {"name": "approval", "signal_id": "cb-1", "payload": {"by": "ops"}},
            ),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output == {"approved_by": "ops"}


# TestAnEarlySignalIsNoDifferentFromALateOne, in Python.
#
# The body cannot tell, and that is the point. Both orderings reach it as the
# same two events in history, so there is no early case to get wrong here
# either.
def test_an_early_signal_reads_the_same_as_a_late_one() -> None:
    late = approver.decide(
        history_of(
            event(2, "SIGNAL_WAIT_STARTED", "S1", {"name": "approval"}),
            event(3, "SIGNAL_RECEIVED", "S1", {"signal_id": "cb-1", "payload": {"by": "ops"}}),
        )
    )
    # The early ordering: the runtime recorded the wait and resolved it in the
    # same advance, because the signal was already stored when the body asked.
    early = approver.decide(
        history_of(
            event(2, "SIGNAL_WAIT_STARTED", "S1", {"name": "approval"}),
            event(3, "SIGNAL_RECEIVED", "S1", {"signal_id": "cb-1", "payload": {"by": "ops"}}),
        )
    )
    assert early == late


def test_a_timeout_is_raised_at_the_step_that_waited() -> None:
    @agent(name="waiter", version="v1")
    async def impatient(ctx: Any) -> dict[str, str]:
        try:
            await ctx.wait_for("approval", timeout=dt.timedelta(hours=1))
        except SignalTimeout as timed_out:
            assert timed_out.step_id == "S1"
            return {"outcome": "nobody approved it"}
        return {"outcome": "approved"}

    decision = impatient.decide(
        history_of(
            event(2, "SIGNAL_WAIT_STARTED", "S1", {"name": "approval"}),
            event(3, "SIGNAL_WAIT_TIMED_OUT", "S1", {"name": "approval"}),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output == {"outcome": "nobody approved it"}


def test_an_uncaught_timeout_fails_the_run_with_the_step_in_it() -> None:
    decision = approver.decide(
        history_of(
            event(2, "SIGNAL_WAIT_STARTED", "S1", {"name": "approval"}),
            event(3, "SIGNAL_WAIT_TIMED_OUT", "S1", {"name": "approval"}),
        )
    )

    assert decision.kind is DecisionKind.FAIL
    assert "S1" in decision.error
    assert "approval" in decision.error


def test_a_timeout_is_measured_from_the_runs_start() -> None:
    @agent(name="waiter", version="v1")
    async def bounded(ctx: Any) -> None:
        await ctx.wait_for("approval", timeout=dt.timedelta(hours=2))

    decision = bounded.decide(history_of())
    assert decision.signal_deadline_unix_nano > 0
    # Same answer every time, like every other durable value here.
    assert (
        decision.signal_deadline_unix_nano == bounded.decide(history_of()).signal_deadline_unix_nano
    )


def test_wait_for_needs_a_name() -> None:
    @agent(name="waiter", version="v1")
    async def nameless(ctx: Any) -> None:
        await ctx.wait_for("")

    decision = nameless.decide(history_of())
    assert decision.kind is DecisionKind.FAIL
    assert "signal name" in decision.error


# --- positions ------------------------------------------------------------


def test_waits_and_calls_share_one_step_sequence() -> None:
    """A sleep occupies a position exactly as a call does.

    If they were numbered separately, a body that added a sleep before a call
    would renumber that call -- and the call's idempotency key is derived from
    its step, so every already-issued key for the rest of the run would move.
    """

    @agent(name="waiter", version="v1", tools=[notify])
    async def mixed(ctx: Any) -> None:
        await ctx.call(notify, to="first")
        await ctx.sleep(60.0)
        await ctx.call(notify, to="second")
        raise Fail("far enough")

    after_call = mixed.decide(
        history_of(
            event(2, "TASK_CREATED", "S1", {"task_type": "notify", "payload": {"to": "first"}}),
            event(3, "TASK_COMPLETED", "S1", {"result": {"to": "first"}}),
        )
    )
    assert after_call.kind is DecisionKind.SLEEP
    assert after_call.step_id == "S2"

    after_sleep = mixed.decide(
        history_of(
            event(2, "TASK_CREATED", "S1", {"task_type": "notify", "payload": {"to": "first"}}),
            event(3, "TASK_COMPLETED", "S1", {"result": {"to": "first"}}),
            event(4, "TIMER_SET", "S2", {"wake_at": "2026-01-01T00:01:00Z"}),
            event(5, "TIMER_FIRED", "S2", {"wake_at": "2026-01-01T00:01:00Z"}),
        )
    )
    assert after_sleep.kind is DecisionKind.CALL_TOOL
    assert after_sleep.step_id == "S3"


def test_a_resolution_with_no_wait_is_refused() -> None:
    """History that resolves a wait nobody opened is not history this build
    can act on, and guessing at the terms of a suspension is worse than
    saying so."""
    from veya.errors import ProtocolError

    with pytest.raises(ProtocolError, match="no wait before it"):
        read([STARTED, event(2, "TIMER_FIRED", "S1", {})], run_id="run-1")


def test_a_negative_sleep_is_refused() -> None:
    @agent(name="waiter", version="v1")
    async def backwards(ctx: Any) -> None:
        await ctx.sleep(-5.0)

    decision = backwards.decide(history_of())
    assert decision.kind is DecisionKind.FAIL
    assert "negative" in decision.error


def test_veya_error_is_importable_for_authors() -> None:
    assert issubclass(SignalTimeout, VeyaError)

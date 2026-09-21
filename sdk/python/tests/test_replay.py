"""Replay, without a gateway in the way.

These drive Agent.decide directly, so a failure here is a failure in the replay
logic rather than in the wire.
"""

from __future__ import annotations

import json
import warnings

import pytest

from veya import Cancel, Fail, NonDeterminismError, agent, tool
from veya.agent import DecisionKind
from veya.history import PAYLOAD_VERSION, Event, read


def ev(seq: int, type_: str, step: str, **data: object) -> Event:
    return Event(
        run_id="run-1",
        seq=seq,
        type=type_,
        step_id=step,
        payload=json.dumps({"v": PAYLOAD_VERSION, "data": data}).encode(),
        created_at_unix_nano=1_700_000_000_000_000_000 + seq,
    )


def history(*events: Event, **run_input: object):
    started = ev(1, "RUN_STARTED", "", agent_name="t", agent_version="v1", input=run_input)
    return read([started, *events], run_id="run-1", run_input=run_input)


@tool
def step_one(n: int = 0) -> dict:
    return {"n": n}


@tool
def step_two(n: int = 0) -> dict:
    return {"n": n}


# --- the shape of a decision ---------------------------------------------


def test_an_empty_history_asks_for_the_first_step():
    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_one, n=1)

    decision = a.decide(history())
    assert decision.kind is DecisionKind.CALL_TOOL
    assert decision.step_id == "S1"
    assert decision.payload == {"n": 1}


def test_steps_are_numbered_by_invocation_order():
    @agent(name="t", version="v1", tools=[step_one, step_two])
    async def a(ctx) -> dict:
        await ctx.call(step_one)
        return await ctx.call(step_two)

    decided = history(
        ev(2, "TASK_CREATED", "S1", task_type="step_one", payload={}),
        ev(3, "TASK_COMPLETED", "S1", result={"n": 1}),
    )
    assert a.decide(decided).step_id == "S2"


def test_a_body_with_no_calls_completes_at_once():
    @agent(name="t", version="v1", tools=[])
    async def a(ctx) -> dict:
        return {"done": True}

    decision = a.decide(history())
    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output == {"done": True}


def test_a_sync_body_works_too():
    @agent(name="t", version="v1", tools=[])
    def a(ctx) -> dict:
        return {"sync": True}

    assert a.decide(history()).output == {"sync": True}


def test_the_run_input_is_unpacked_as_arguments():
    @agent(name="t", version="v1", tools=[])
    async def a(ctx, order_id: int, currency: str) -> dict:
        return {"order": order_id, "currency": currency}

    decision = a.decide(history(order_id=7, currency="GBP"))
    assert decision.output == {"order": 7, "currency": "GBP"}


def test_a_body_naming_input_gets_the_whole_dict():
    @agent(name="t", version="v1", tools=[])
    async def a(ctx, input: dict) -> dict:
        return {"keys": sorted(input)}

    decision = a.decide(history(order_id=7, currency="GBP"))
    assert decision.output == {"keys": ["currency", "order_id"]}


# --- divergence -----------------------------------------------------------


def test_a_different_tool_at_a_step_is_a_divergence():
    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_one)

    diverged = history(ev(2, "TASK_CREATED", "S1", task_type="step_two", payload={}))
    with pytest.raises(NonDeterminismError) as caught:
        a.decide(diverged)

    assert caught.value.step_id == "S1"
    assert "step_two" in caught.value.expected
    assert "step_one" in caught.value.actual


def test_a_different_payload_at_a_step_is_a_divergence():
    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_one, n=2)

    diverged = history(ev(2, "TASK_CREATED", "S1", task_type="step_one", payload={"n": 1}))
    with pytest.raises(NonDeterminismError):
        a.decide(diverged)


def test_key_order_in_a_payload_is_not_a_divergence():
    """Payloads are compared as they cross the wire. Two dicts with the same
    contents in a different order are the same payload."""

    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_one, b=2, a=1)

    same = history(ev(2, "TASK_CREATED", "S1", task_type="step_one", payload={"a": 1, "b": 2}))
    assert a.decide(same).kind is DecisionKind.CALL_TOOL  # suspended at S1, not diverged


def test_a_list_and_the_tuple_it_came_from_are_the_same_payload():
    """A tuple becomes a JSON array, and history holds the array. Reporting a
    divergence there would be reporting one the wire cannot see."""

    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_one, items=(1, 2, 3))

    recorded = history(
        ev(2, "TASK_CREATED", "S1", task_type="step_one", payload={"items": [1, 2, 3]}),
        ev(3, "TASK_COMPLETED", "S1", result={"ok": True}),
    )
    assert a.decide(recorded).kind is DecisionKind.COMPLETE


# --- failure paths --------------------------------------------------------


def test_a_final_failure_reaches_the_body_at_its_step():
    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_one)

    broken = history(
        ev(2, "TASK_CREATED", "S1", task_type="step_one", payload={}),
        ev(3, "TASK_FAILED", "S1", error="upstream is down", final=True),
    )
    decision = a.decide(broken)
    assert decision.kind is DecisionKind.FAIL
    assert "upstream is down" in decision.error


def test_a_body_may_handle_a_failed_step_itself():
    """ToolFailed is an ordinary exception, so an agent can decide a step's
    failure is survivable."""

    @agent(name="t", version="v1", tools=[step_one, step_two])
    async def a(ctx) -> dict:
        from veya import ToolFailed

        try:
            return await ctx.call(step_one)
        except ToolFailed:
            return await ctx.call(step_two, n=99)

    broken = history(
        ev(2, "TASK_CREATED", "S1", task_type="step_one", payload={}),
        ev(3, "TASK_FAILED", "S1", error="nope", final=True),
    )
    decision = a.decide(broken)
    assert decision.kind is DecisionKind.CALL_TOOL
    assert decision.tool == "step_two"
    assert decision.payload == {"n": 99}


def test_fail_is_reported_as_a_deliberate_failure():
    @agent(name="t", version="v1", tools=[])
    async def a(ctx) -> dict:
        raise Fail("not refundable")

    decision = a.decide(history())
    assert decision.kind is DecisionKind.FAIL
    assert decision.error == "not refundable"


def test_cancel_is_reported_as_a_deliberate_cancellation():
    @agent(name="t", version="v1", tools=[])
    async def a(ctx) -> dict:
        raise Cancel("the customer withdrew the request")

    decision = a.decide(history())
    assert decision.kind is DecisionKind.CANCEL
    assert decision.error == "the customer withdrew the request"


def test_an_unexplained_cancel_is_left_unexplained():
    # Unlike Fail, which invents a reason when the body gave none, an
    # unexplained Cancel stays that way -- cancelling for no stated reason is
    # itself something a body can mean.
    @agent(name="t", version="v1", tools=[])
    async def a(ctx) -> dict:
        raise Cancel

    decision = a.decide(history())
    assert decision.kind is DecisionKind.CANCEL
    assert decision.error == ""


def test_calling_an_unregistered_tool_says_so():
    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        return await ctx.call(step_two)

    decision = a.decide(history())
    assert decision.kind is DecisionKind.FAIL
    assert "did not register" in decision.error


# --- the replay-safe substitutes -----------------------------------------


def test_ctx_now_gives_the_same_answer_every_time():
    seen: list[str] = []

    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        seen.append(ctx.now().isoformat())
        return await ctx.call(step_one)

    h = history()
    a.decide(h)
    a.decide(h)
    assert seen[0] == seen[1]


def test_ctx_random_is_seeded_from_the_run():
    draws: list[int] = []

    @agent(name="t", version="v1", tools=[step_one])
    async def a(ctx) -> dict:
        draws.append(ctx.random().randint(1, 1_000_000))
        return await ctx.call(step_one)

    h = history()
    a.decide(h)
    a.decide(h)
    assert draws[0] == draws[1]


def test_a_wall_clock_in_a_payload_diverges():
    """The failure this is all arranged to prevent, demonstrated.

    A body that puts the current time in a payload produces a different payload
    on the next decision, and the check catches it at the step that recorded
    the first one.
    """
    import time

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        @agent(name="t", version="v1", tools=[step_one, step_two])
        async def a(ctx) -> dict:
            await ctx.call(step_one, at=time.time())
            return await ctx.call(step_two)

    first = a.decide(history())
    recorded = history(
        ev(2, "TASK_CREATED", "S1", task_type="step_one", payload=first.payload),
        ev(3, "TASK_COMPLETED", "S1", result={}),
    )
    time.sleep(0.01)

    with pytest.raises(NonDeterminismError) as caught:
        a.decide(recorded)
    assert caught.value.step_id == "S1"

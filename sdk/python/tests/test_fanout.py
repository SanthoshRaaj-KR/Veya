"""ctx.call_parallel, against history.

The ordering claim is the one worth most of this file. Children finish in an
order nobody controls, so a fan-out that let completion order reach the body
would answer the same question differently on every replay -- and every payload
derived from the answer would diverge one step later.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from veya import Call, Join, agent, tool
from veya.agent import DecisionKind
from veya.errors import NonDeterminismError, ToolFailed, VeyaError
from veya.history import Event, read

PAYLOAD_V = 1


def event(seq: int, typ: str, step: str, data: dict[str, Any] | None = None) -> Event:
    body = json.dumps({"v": PAYLOAD_V, "data": data or {}}).encode()
    return Event(
        run_id="run-1", seq=seq, type=typ, step_id=step, payload=body, created_at_unix_nano=0
    )


STARTED = event(1, "RUN_STARTED", "", {"agent_name": "checker", "agent_version": "v1", "input": {}})


def history_of(*events: Event):
    return read([STARTED, *events], run_id="run-1")


def created(seq: int, step: str, sku: str) -> Event:
    return event(seq, "TASK_CREATED", step, {"task_type": "check", "payload": {"sku": sku}})


def completed(seq: int, step: str, result: Any) -> Event:
    return event(seq, "TASK_COMPLETED", step, {"result": result})


def failed(seq: int, step: str, why: str) -> Event:
    return event(seq, "TASK_FAILED", step, {"error": why, "final": True})


@tool(name="check")
def check(sku: str) -> dict[str, str]:
    return {"sku": sku}


SKUS = ["a", "b", "c"]


def checker(join: Join | tuple[Join, int]):
    @agent(name="checker", version="v1", tools=[check])
    async def body(ctx: Any) -> dict[str, Any]:
        outcomes = await ctx.call_parallel(
            [Call(check, sku=sku) for sku in SKUS],
            join=join,
        )
        return {
            "order": [o.tool for o in outcomes],
            "skus": [(o.result or {}).get("sku") if o.ok else None for o in outcomes],
            "ok": [o.ok for o in outcomes],
            "settled": [o.settled for o in outcomes],
        }

    return body


def test_an_unrecorded_fan_out_becomes_a_parallel_decision() -> None:
    decision = checker(Join.ALL).decide(history_of())

    assert decision.kind is DecisionKind.CALL_TOOL_PARALLEL
    assert decision.step_id == "S1"
    assert decision.join is Join.ALL
    assert [c.name for c in decision.calls] == ["check", "check", "check"]
    assert [c.payload["sku"] for c in decision.calls] == SKUS


def test_all_waits_for_every_child() -> None:
    two_of_three = history_of(
        created(2, "S1.0", "a"),
        created(3, "S1.1", "b"),
        created(4, "S1.2", "c"),
        completed(5, "S1.0", {"sku": "a"}),
        completed(6, "S1.2", {"sku": "c"}),
    )
    decision = checker(Join.ALL).decide(two_of_three)

    # Still waiting, so the identical fan-out is re-issued. The runtime finds
    # the children already exist and does nothing, which is how it waits.
    assert decision.kind is DecisionKind.CALL_TOOL_PARALLEL
    assert decision.step_id == "S1"


def test_results_come_back_in_invocation_order_not_completion_order() -> None:
    # Completed deliberately backwards: c, then b, then a.
    decision = checker(Join.ALL).decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            completed(5, "S1.2", {"sku": "c"}),
            completed(6, "S1.1", {"sku": "b"}),
            completed(7, "S1.0", {"sku": "a"}),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output["skus"] == ["a", "b", "c"]


def test_all_settles_on_failures_too() -> None:
    """ALL joins on settling, not succeeding. A join that only ended on success
    would hang forever the first time a child failed for good."""
    decision = checker(Join.ALL).decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            completed(5, "S1.0", {"sku": "a"}),
            failed(6, "S1.1", "provider is down"),
            completed(7, "S1.2", {"sku": "c"}),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output["ok"] == [True, False, True]
    assert decision.output["skus"] == ["a", None, "c"]


def test_any_finishes_on_the_first_success_with_siblings_running() -> None:
    decision = checker(Join.ANY).decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            completed(5, "S1.1", {"sku": "b"}),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    # The siblings are not cancelled, so they are simply not settled yet.
    assert decision.output["settled"] == [False, True, False]
    assert decision.output["ok"] == [False, True, False]


def test_any_also_finishes_when_every_child_has_failed() -> None:
    """The exit that gets forgotten. Without it, ANY hangs forever on the day
    everything is broken -- which is the day you least want a silent run."""
    decision = checker(Join.ANY).decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            failed(5, "S1.0", "down"),
            failed(6, "S1.1", "down"),
            failed(7, "S1.2", "down"),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output["ok"] == [False, False, False]


def test_quorum_finishes_at_enough_successes() -> None:
    decision = checker(Join.of(2)).decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            completed(5, "S1.0", {"sku": "a"}),
            completed(6, "S1.2", {"sku": "c"}),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output["settled"] == [True, False, True]


def test_quorum_stops_when_it_becomes_impossible() -> None:
    """Two failures out of three make a quorum of two unreachable. Waiting for
    the last child would keep a run alive for an answer already decided."""
    decision = checker(Join.of(2)).decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            failed(5, "S1.0", "down"),
            failed(6, "S1.1", "down"),
        )
    )

    assert decision.kind is DecisionKind.COMPLETE
    assert decision.output["ok"] == [False, False, False]


def test_an_unsatisfiable_quorum_is_refused_at_the_call() -> None:
    @agent(name="checker", version="v1", tools=[check])
    async def greedy(ctx: Any) -> None:
        await ctx.call_parallel([Call(check, sku="a")], join=Join.of(3))

    decision = greedy.decide(history_of())
    assert decision.kind is DecisionKind.FAIL
    assert "never be satisfied" in decision.error


def test_an_empty_fan_out_is_refused() -> None:
    @agent(name="checker", version="v1", tools=[check])
    async def empty(ctx: Any) -> None:
        await ctx.call_parallel([])

    decision = empty.decide(history_of())
    assert decision.kind is DecisionKind.FAIL
    assert "at least one call" in decision.error


def test_a_child_whose_payload_changed_is_a_divergence() -> None:
    """The replay check applies per child. A body that reordered its skus
    between replays would hand the same idempotency key a different payload,
    and this is where that is caught."""

    @agent(name="checker", version="v1", tools=[check])
    async def reordered(ctx: Any) -> None:
        await ctx.call_parallel([Call(check, sku=sku) for sku in ["c", "b", "a"]])

    with pytest.raises(NonDeterminismError, match=r"S1\.0"):
        reordered.decide(
            history_of(
                created(2, "S1.0", "a"),
                created(3, "S1.1", "b"),
                created(4, "S1.2", "c"),
            )
        )


def test_unwrap_raises_for_a_failed_child() -> None:
    @agent(name="checker", version="v1", tools=[check])
    async def strict(ctx: Any) -> list[Any]:
        outcomes = await ctx.call_parallel([Call(check, sku=s) for s in SKUS])
        return [o.unwrap() for o in outcomes]

    decision = strict.decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            completed(5, "S1.0", {"sku": "a"}),
            failed(6, "S1.1", "provider is down"),
            completed(7, "S1.2", {"sku": "c"}),
        )
    )

    assert decision.kind is DecisionKind.FAIL
    assert "provider is down" in decision.error


def test_unwrap_is_importable_and_typed() -> None:
    assert issubclass(ToolFailed, VeyaError)


def test_a_fan_out_occupies_one_position() -> None:
    """A fan-out is one step, and its children hang off it. If it consumed
    three positions, adding a call to the list would renumber everything after
    it -- and each of those steps is an idempotency key."""

    @agent(name="checker", version="v1", tools=[check])
    async def after(ctx: Any) -> str:
        await ctx.call_parallel([Call(check, sku=s) for s in SKUS])
        await ctx.call(check, sku="last")
        return "done"

    decision = after.decide(
        history_of(
            created(2, "S1.0", "a"),
            created(3, "S1.1", "b"),
            created(4, "S1.2", "c"),
            completed(5, "S1.0", {"sku": "a"}),
            completed(6, "S1.1", {"sku": "b"}),
            completed(7, "S1.2", {"sku": "c"}),
        )
    )

    assert decision.kind is DecisionKind.CALL_TOOL
    assert decision.step_id == "S2"

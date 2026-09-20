"""Replaying a body across suspensions, one decision at a time.

Every other test here hands the agent a history and checks one answer. This
one plays the runtime: it asks for a decision, writes the events the runtime
would have written for it, and asks again — for a body that calls, sleeps, fans
out and waits for a human, which is every shape this layer added.

The property under test is the one a suspension makes easy to break. A body is
re-run from the top on every decision, so it reaches each of its waits again
and again. A wait that history has already satisfied must fall through
silently. If it re-suspends, the run never gets past it, and the failure looks
like a runtime that will not wake anything up rather than a body that keeps
going back to sleep.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from veya import Call, Join, agent, tool
from veya.agent import Decision, DecisionKind
from veya.history import Event, read

PAYLOAD_V = 1


@tool(name="reserve")
def reserve(sku: str) -> dict[str, str]:
    return {"sku": sku}


@tool(name="charge")
def charge(amount: int) -> dict[str, str]:
    return {"reference": f"ch_{amount}"}


@tool(name="notify")
def notify(to: str) -> dict[str, str]:
    return {"to": to}


SKUS = ["a", "b"]


@agent(name="order", version="v1", tools=[reserve, charge, notify])
async def order(ctx: Any, amount: int = 100) -> dict[str, Any]:
    """Call, fan out, sleep, wait for a person, call again."""
    charged = await ctx.call(charge, amount=amount)

    reserved = await ctx.call_parallel([Call(reserve, sku=sku) for sku in SKUS], join=Join.ALL)

    await ctx.sleep(dt.timedelta(hours=1))

    approval = await ctx.wait_for("approval")

    await ctx.call(notify, to="ops")
    return {
        "reference": charged["reference"],
        "reserved": [o.result["sku"] for o in reserved if o.ok],
        "approved_by": approval.get("by"),
    }


# The run's start. Every replay-safe instant the body computes is derived from
# it, via ctx.now(), so it has to be a real time rather than the epoch -- the
# same reason the runtime stamps RUN_STARTED from the clock.
STARTED_AT = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


class Runtime:
    """Enough of the runtime to replay a body against: an event log, and the
    writes the engine would make for each decision.

    It is deliberately faithful about *what it records*. An early draft parked
    runs at an instant of its own choosing rather than the one the decision
    asked for, and the walk below caught it -- which is the same bug a real
    engine would have if it rounded or defaulted a wake-up.
    """

    def __init__(self) -> None:
        self.events: list[Event] = []
        self.seq = 0
        self.parked_at: dict[str, str] = {}
        self._append(
            "RUN_STARTED",
            "",
            {"agent_name": "order", "agent_version": "v1", "input": {"amount": 100}},
        )

    def _append(self, typ: str, step: str, data: dict[str, Any]) -> None:
        self.seq += 1
        self.events.append(
            Event(
                run_id="run-1",
                seq=self.seq,
                type=typ,
                step_id=step,
                payload=json.dumps({"v": PAYLOAD_V, "data": data}).encode(),
                created_at_unix_nano=int(STARTED_AT.timestamp() * 1_000_000_000),
            )
        )

    def decide(self) -> Decision:
        return order.decide(read(self.events, run_id="run-1", run_input={"amount": 100}))

    # --- the engine's side of each decision -------------------------------

    def dispatch(self, decision: Decision, result: Any) -> None:
        self._append(
            "TASK_CREATED",
            decision.step_id,
            {"task_type": decision.tool, "payload": decision.payload},
        )
        self._append("TASK_COMPLETED", decision.step_id, {"result": result})

    def fan_out(self, decision: Decision, results: list[Any]) -> None:
        self._append(
            "FAN_OUT_STARTED",
            decision.step_id,
            {"calls": [c.name for c in decision.calls], "join": decision.join.value},
        )
        for index, call in enumerate(decision.calls):
            self._append(
                "TASK_CREATED",
                f"{decision.step_id}.{index}",
                {"task_type": call.name, "payload": call.payload},
            )
        # Completed backwards, because the order children finish in is not the
        # order they were invoked in and the body must not notice.
        for index in reversed(range(len(decision.calls))):
            self._append(
                "TASK_COMPLETED", f"{decision.step_id}.{index}", {"result": results[index]}
            )

    def park(self, decision: Decision) -> None:
        # The instant the decision asked for, not one of the runtime's
        # choosing. Recording anything else is how a sleep silently becomes a
        # different sleep on the next replay.
        self.parked_at[decision.step_id] = _iso(decision.wake_at_unix_nano)
        self._append("TIMER_SET", decision.step_id, {"wake_at": self.parked_at[decision.step_id]})

    def wake(self, step: str) -> None:
        self._append("TIMER_FIRED", step, {"wake_at": self.parked_at[step]})

    def start_wait(self, decision: Decision) -> None:
        self._append("SIGNAL_WAIT_STARTED", decision.step_id, {"name": decision.signal_name})

    def deliver(self, step: str, payload: dict[str, Any]) -> None:
        self._append(
            "SIGNAL_RECEIVED", step, {"name": "approval", "signal_id": "cb-1", "payload": payload}
        )


def test_a_body_replays_across_every_suspension_it_has() -> None:
    rt = Runtime()
    seen: list[tuple[DecisionKind, str]] = []

    def step() -> Decision:
        d = rt.decide()
        seen.append((d.kind, d.step_id))
        return d

    # S1: an ordinary call.
    d = step()
    assert (d.kind, d.step_id, d.tool) == (DecisionKind.CALL_TOOL, "S1", "charge")
    rt.dispatch(d, {"reference": "ch_100"})

    # S2: the fan-out.
    d = step()
    assert (d.kind, d.step_id) == (DecisionKind.CALL_TOOL_PARALLEL, "S2")
    rt.fan_out(d, [{"sku": "a"}, {"sku": "b"}])

    # S3: the sleep. The two settled steps before it must not be re-issued.
    d = step()
    assert (d.kind, d.step_id) == (DecisionKind.SLEEP, "S3")
    rt.park(d)

    # While parked the runtime would not ask again, but asking must be safe:
    # the answer is the same sleep, with the same instant, and nothing moves.
    parked = rt.decide()
    assert (parked.kind, parked.step_id) == (DecisionKind.SLEEP, "S3")
    assert parked.wake_at_unix_nano == d.wake_at_unix_nano

    rt.wake("S3")

    # S4: the signal wait. The satisfied sleep falls through.
    d = step()
    assert (d.kind, d.step_id, d.signal_name) == (DecisionKind.WAIT_FOR_SIGNAL, "S4", "approval")
    rt.start_wait(d)
    rt.deliver("S4", {"by": "ops"})

    # S5: the last call. Both suspensions are behind it now.
    d = step()
    assert (d.kind, d.step_id, d.tool) == (DecisionKind.CALL_TOOL, "S5", "notify")
    rt.dispatch(d, {"to": "ops"})

    # And the run completes, with the fan-out's results in invocation order
    # even though they were recorded backwards.
    d = step()
    assert d.kind is DecisionKind.COMPLETE
    assert d.output == {
        "reference": "ch_100",
        "reserved": ["a", "b"],
        "approved_by": "ops",
    }

    # Each position was decided exactly once, in order. A wait that re-suspended
    # after being satisfied would show up here as a repeat.
    assert seen == [
        (DecisionKind.CALL_TOOL, "S1"),
        (DecisionKind.CALL_TOOL_PARALLEL, "S2"),
        (DecisionKind.SLEEP, "S3"),
        (DecisionKind.WAIT_FOR_SIGNAL, "S4"),
        (DecisionKind.CALL_TOOL, "S5"),
        (DecisionKind.COMPLETE, ""),
    ]


def test_every_decision_is_reproducible_from_its_own_history() -> None:
    """The same history, asked ten times, gives the same answer.

    A body that let anything outside history reach its output -- a clock, a
    counter, the order a dict happened to iterate in -- would pass the walk
    above and fail here, because the walk only ever asks once.
    """
    rt = Runtime()

    for _ in range(6):
        first = rt.decide()
        for _ in range(9):
            again = rt.decide()
            assert (again.kind, again.step_id, again.tool, again.payload) == (
                first.kind,
                first.step_id,
                first.tool,
                first.payload,
            )
            assert again.output == first.output
            assert again.wake_at_unix_nano == first.wake_at_unix_nano
            assert [c.payload for c in again.calls] == [c.payload for c in first.calls]

        if first.kind is DecisionKind.CALL_TOOL:
            rt.dispatch(first, {"reference": "ch_100", "to": "ops"})
        elif first.kind is DecisionKind.CALL_TOOL_PARALLEL:
            rt.fan_out(first, [{"sku": "a"}, {"sku": "b"}])
        elif first.kind is DecisionKind.SLEEP:
            rt.park(first)
            rt.wake(first.step_id)
        elif first.kind is DecisionKind.WAIT_FOR_SIGNAL:
            rt.start_wait(first)
            rt.deliver(first.step_id, {"by": "ops"})
        else:
            break


def test_a_suspension_does_not_renumber_the_steps_around_it() -> None:
    """The step ids a run issues are its idempotency keys.

    If a sleep occupied no position, the call after it would be numbered as
    though the sleep were not there -- and a body edited to add a sleep would
    reuse the keys of the steps it displaced, against ledger rows recording
    different actions.
    """
    rt = Runtime()

    d = rt.decide()
    rt.dispatch(d, {"reference": "ch_100"})
    d = rt.decide()
    rt.fan_out(d, [{"sku": "a"}, {"sku": "b"}])

    sleep = rt.decide()
    assert sleep.step_id == "S3"
    rt.park(sleep)
    rt.wake("S3")

    wait = rt.decide()
    assert wait.step_id == "S4"
    rt.start_wait(wait)
    rt.deliver("S4", {"by": "ops"})

    after = rt.decide()
    assert after.step_id == "S5", (
        "the call after two suspensions must be S5; if suspensions were free, "
        "it would be S3 and would collide with the sleep's position"
    )


def _iso(unix_nano: int) -> str:
    """Render an instant the way the runtime writes it into an event body."""
    return (
        dt.datetime.fromtimestamp(unix_nano / 1_000_000_000, tz=dt.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

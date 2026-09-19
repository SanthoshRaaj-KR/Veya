"""The SDK end to end, against a gateway that speaks the real protocol.

These are the tests that would catch a mistake in the wiring rather than in the
logic: a payload encoded the wrong way, a failure whose certainty was dropped, a
tool that registered under the wrong class.
"""

from __future__ import annotations

import threading

import pytest

from tests.conftest import completed, created, failed, run, started
from veya import EffectClass, Fail, NotExecuted, Resolution, agent, tool
from veya.worker.v1 import worker_pb2 as pb

# --- an agent to serve ----------------------------------------------------


@tool
def lookup_order(order_id: int) -> dict:
    return {"id": order_id, "status": "DELIVERED", "amount": 500}


@tool(effect=EffectClass.IDEMPOTENT_BY_KEY, key_ttl_hours=24)
def create_refund(order_id: int, amount: int, *, idempotency_key: str) -> dict:
    return {"reference": f"rf_{idempotency_key}", "amount": amount, "order_id": order_id}


@tool(effect=EffectClass.QUERYABLE, key_ttl_hours=24)
def send_receipt(to: str, *, idempotency_key: str) -> dict:
    return {"reference": f"msg_{idempotency_key}", "to": to}


_sent: set[str] = set()


@send_receipt.reconcile
def _(idempotency_key: str) -> dict | None:
    return {"reference": f"msg_{idempotency_key}"} if idempotency_key in _sent else None


@agent(name="refund_agent", version="v1", tools=[lookup_order, create_refund, send_receipt])
async def refund_agent(ctx, order_id: int) -> dict:
    order = await ctx.call(lookup_order, order_id=order_id)
    if order["status"] != "DELIVERED":
        return {"refunded": False, "reason": "order not delivered"}

    refund = await ctx.call(create_refund, order_id=order["id"], amount=order["amount"])
    await ctx.call(send_receipt, to="ana@example.com")
    return {"refunded": True, "reference": refund["reference"]}


@pytest.fixture
def served(connect):
    return connect(agent=refund_agent)


# --- registration ---------------------------------------------------------


def test_registration_declares_the_agent_and_its_tools(served):
    reg = served.registration

    assert reg.protocol_version == pb.PROTOCOL_VERSION_1
    assert reg.agent_name == "refund_agent"
    assert reg.agent_version == "v1"
    assert reg.decides is True
    assert reg.runtime.startswith("python ")

    by_name = {t.name: t for t in reg.tools}
    assert set(by_name) == {"lookup_order", "create_refund", "send_receipt"}
    assert by_name["lookup_order"].effect_class == pb.EFFECT_CLASS_NONE
    assert by_name["create_refund"].effect_class == pb.EFFECT_CLASS_IDEMPOTENT_BY_KEY
    assert by_name["create_refund"].key_ttl_seconds == 24 * 3600

    # A QUERYABLE tool must announce its hook, or the runtime refuses the whole
    # registration: a tool that cannot be asked what it did is UNRECONCILABLE.
    assert by_name["send_receipt"].effect_class == pb.EFFECT_CLASS_QUERYABLE
    assert by_name["send_receipt"].reconcilable is True
    assert by_name["lookup_order"].reconcilable is False


def test_a_tool_host_says_it_does_not_decide(connect):
    service = connect(tools=[lookup_order])
    # Without an agent body there is nothing to decide, and the runtime is told
    # so rather than left to discover it by asking and waiting.
    assert service.registration.decides is False


def test_a_tool_host_needs_to_be_told_whose_tools_these_are(gateway):
    from veya import VeyaError, Worker

    _, address = gateway
    worker = Worker(address=address, tools=[lookup_order])
    with pytest.raises(VeyaError, match="for_agent"):
        worker.serve(reconnect=False)


# --- deciding -------------------------------------------------------------


def test_the_first_decision_is_the_first_step(served):
    result = served.decide(run(order_id=987), [started(order_id=987)])

    assert result.HasField("decision")
    assert result.decision.kind == pb.DECISION_KIND_CALL_TOOL
    assert result.decision.step_id == "S1"
    assert result.decision.tool == "lookup_order"
    assert result.decision.payload == b'{"order_id": 987}'


def test_a_recorded_step_replays_instead_of_running_again(served):
    history = [
        started(order_id=987),
        created(2, "S1", "lookup_order", order_id=987),
        completed(3, "S1", {"id": 987, "status": "DELIVERED", "amount": 500}),
    ]
    result = served.decide(run(order_id=987), history)

    assert result.decision.step_id == "S2"
    assert result.decision.tool == "create_refund"
    assert b'"amount": 500' in result.decision.payload


def test_a_run_with_every_step_recorded_completes(served):
    history = [
        started(order_id=987),
        created(2, "S1", "lookup_order", order_id=987),
        completed(3, "S1", {"id": 987, "status": "DELIVERED", "amount": 500}),
        created(4, "S2", "create_refund", order_id=987, amount=500),
        completed(5, "S2", {"reference": "rf_1"}),
        created(6, "S3", "send_receipt", to="ana@example.com"),
        completed(7, "S3", {"reference": "msg_1"}),
    ]
    result = served.decide(run(order_id=987), history)

    assert result.decision.kind == pb.DECISION_KIND_COMPLETE
    assert b'"reference": "rf_1"' in result.decision.output


def test_a_branch_taken_on_a_recorded_result_is_honoured(served):
    """The condition is evaluated against what history recorded, not against a
    fresh call — which is what makes a data-dependent branch replay."""
    history = [
        started(order_id=1),
        created(2, "S1", "lookup_order", order_id=1),
        completed(3, "S1", {"id": 1, "status": "PENDING"}),
    ]
    result = served.decide(run(order_id=1), history)

    assert result.decision.kind == pb.DECISION_KIND_COMPLETE
    assert b'"refunded": false' in result.decision.output


def test_divergence_is_reported_with_both_sides_named(served):
    history = [
        started(order_id=987),
        # History says S1 called something else entirely.
        created(2, "S1", "send_receipt", to="ana@example.com"),
    ]
    result = served.decide(run(order_id=987), history)

    assert result.HasField("failure")
    assert result.failure.type == "NonDeterminismError"
    assert "S1" in result.failure.message
    assert "send_receipt" in result.failure.message
    assert "lookup_order" in result.failure.message


def test_a_failed_step_fails_the_run_at_the_step_that_failed(served):
    history = [
        started(order_id=987),
        created(2, "S1", "lookup_order", order_id=987),
        failed(3, "S1", "the orders API is down", final=True),
    ]
    result = served.decide(run(order_id=987), history)

    assert result.decision.kind == pb.DECISION_KIND_FAIL
    assert "S1" in result.decision.error
    assert "orders API" in result.decision.error


def test_a_retry_leaves_the_step_still_owed(served):
    """A non-final failure means the step will be tried again.

    The body must ask for the same step again rather than being handed a
    result that does not exist. Returning None here would give the body a value
    it would go on to compute with, producing a different payload at the next
    step — a divergence with nothing to do with the author's code.
    """
    history = [
        started(order_id=987),
        created(2, "S1", "lookup_order", order_id=987),
        failed(3, "S1", "timed out", final=False),
    ]
    result = served.decide(run(order_id=987), history)

    assert result.HasField("decision")
    assert result.decision.kind == pb.DECISION_KIND_CALL_TOOL
    assert result.decision.step_id == "S1"
    assert result.decision.tool == "lookup_order"


def test_a_deliberate_failure_is_reported_as_one(connect):
    @agent(name="quitter", version="v1", tools=[])
    async def quitter(ctx) -> dict:
        raise Fail("this order is not refundable")

    service = connect(agent=quitter)
    result = service.decide(run(agent="quitter"), [started(agent="quitter")])

    assert result.decision.kind == pb.DECISION_KIND_FAIL
    assert "not refundable" in result.decision.error


def test_a_crash_in_the_body_fails_the_run_rather_than_stalling_it(connect):
    @agent(name="crasher", version="v1", tools=[])
    async def crasher(ctx) -> dict:
        return {"value": 1 // 0}

    service = connect(agent=crasher)
    result = service.decide(run(agent="crasher"), [started(agent="crasher")])

    assert result.decision.kind == pb.DECISION_KIND_FAIL
    assert "ZeroDivisionError" in result.decision.error


# --- executing ------------------------------------------------------------


def test_a_tool_receives_its_payload_and_its_key(served):
    result = served.execute(
        "create_refund",
        {"order_id": 987, "amount": 500},
        idempotency_key="run-1:S2:E1",
    )

    assert result.HasField("result")
    assert result.result == (b'{"reference": "rf_run-1:S2:E1", "amount": 500, "order_id": 987}')


def test_a_none_result_leaves_nothing_in_history(connect):
    @tool
    def quiet() -> None:
        return None

    service = connect(tools=[quiet])
    # Encoding None as the word "null" would leave an operator reading the run
    # to decide what it meant.
    assert service.execute("quiet").result == b""


def test_an_unknown_tool_is_the_one_honest_not_executed(served):
    result = served.execute("no_such_tool")

    assert result.HasField("failure")
    # Nothing was sent, because there was nothing to send it with. Saying so
    # lets the runtime retry cleanly once a worker that has the tool connects.
    assert result.failure.certainty == pb.CERTAINTY_NOT_EXECUTED


def test_an_ordinary_exception_is_ambiguous(connect):
    @tool(effect=EffectClass.QUERYABLE, key_ttl_hours=1)
    def flaky(*, idempotency_key: str) -> dict:
        raise TimeoutError("the provider did not answer")

    @flaky.reconcile
    def _(idempotency_key: str) -> dict | None:
        return None

    service = connect(tools=[flaky])
    result = service.execute("flaky")

    # The request may have gone out and the answer been lost. Reporting this as
    # NOT_EXECUTED would authorise a retry of something that may have happened.
    assert result.failure.certainty == pb.CERTAINTY_UNKNOWN
    assert result.failure.type == "TimeoutError"


def test_a_deliberate_not_executed_survives_the_wire(connect):
    @tool(effect=EffectClass.IDEMPOTENT_BY_KEY)
    def picky(amount: int, *, idempotency_key: str) -> dict:
        raise NotExecuted("amount must be positive; nothing was sent")

    service = connect(tools=[picky])
    result = service.execute("picky", {"amount": -1})

    assert result.failure.certainty == pb.CERTAINTY_NOT_EXECUTED
    assert "nothing was sent" in result.failure.message


def test_calls_are_served_concurrently(connect):
    """One slow tool must not stall every other call in flight."""
    release = threading.Event()

    @tool
    def slow() -> dict:
        release.wait(timeout=5)
        return {"slow": True}

    @tool
    def quick() -> dict:
        return {"quick": True}

    service = connect(tools=[slow, quick], max_concurrency=4)

    answers: list[bytes] = []
    slow_call = threading.Thread(
        target=lambda: answers.append(service.execute("slow").result), daemon=True
    )
    slow_call.start()

    # The slow call is parked. The quick one must still come back.
    assert service.execute("quick").result == b'{"quick": true}'

    release.set()
    slow_call.join(timeout=5)
    assert answers == [b'{"slow": true}']


# --- reconciling ----------------------------------------------------------


def test_a_hook_that_finds_the_key_says_committed(served):
    _sent.add("run-1:S3:E1")
    try:
        result = served.reconcile("send_receipt", "run-1:S3:E1")
    finally:
        _sent.discard("run-1:S3:E1")

    assert result.resolution.kind == pb.RESOLUTION_KIND_COMMITTED
    assert result.resolution.external_ref == "msg_run-1:S3:E1"


def test_a_hook_that_finds_nothing_says_not_executed(served):
    result = served.reconcile("send_receipt", "run-1:S9:E1")
    assert result.resolution.kind == pb.RESOLUTION_KIND_NOT_EXECUTED


def test_a_hook_that_cannot_reach_the_provider_establishes_nothing(connect):
    @tool(effect=EffectClass.QUERYABLE, key_ttl_hours=1)
    def unreachable(*, idempotency_key: str) -> dict:
        return {}

    @unreachable.reconcile
    def _(idempotency_key: str) -> dict | None:
        raise ConnectionError("billing is down")

    service = connect(tools=[unreachable])
    result = service.reconcile("unreachable", "k")

    # Not a verdict. The effect stays UNKNOWN, which is what it is, and the
    # runtime's sweep will ask again.
    assert result.HasField("failure")
    assert result.failure.type == "ConnectionError"


def test_a_hook_may_say_it_still_cannot_tell(connect):
    @tool(effect=EffectClass.QUERYABLE, key_ttl_hours=1)
    def ambiguous(*, idempotency_key: str) -> dict:
        return {}

    @ambiguous.reconcile
    def _(idempotency_key: str) -> Resolution:
        return Resolution.unknown("the provider's search index is stale")

    service = connect(tools=[ambiguous])
    result = service.reconcile("ambiguous", "k")

    assert result.resolution.kind == pb.RESOLUTION_KIND_STILL_UNKNOWN
    assert "stale" in result.resolution.detail

"""Tool declaration, and the declarations that are refused.

Most of these assert that something does *not* work. A tool's effect class is a
claim about what happens when its action is ambiguous, and a claim the function
cannot honour is worse than no claim at all: it silences the mechanism that
would otherwise have caught the duplicate.
"""

from __future__ import annotations

import pytest

from veya import EffectClass, Resolution, ToolCall, ToolDeclarationError, tool
from veya.effects import Effect, ResolutionKind


def call(key: str = "run-1:S1:E1", **payload: object) -> ToolCall:
    return ToolCall(
        run_id="run-1",
        task_id="task-1",
        step_id="S1",
        idempotency_key=key,
        payload=payload,
    )


def effect(key: str = "run-1:S1:E1") -> Effect:
    return Effect(
        effect_id="e-1",
        run_id="run-1",
        task_id="task-1",
        effect_type="send",
        effect_class=EffectClass.QUERYABLE,
        idempotency_key=key,
        status="UNKNOWN",
    )


# --- declaring ------------------------------------------------------------


def test_a_bare_decorator_declares_a_pure_read():
    @tool
    def lookup(order_id: int) -> dict:
        return {"id": order_id}

    assert lookup.name == "lookup"
    assert lookup.effect is EffectClass.NONE
    assert lookup.key_ttl_seconds == 0
    assert lookup.reconcilable is False


def test_a_tool_is_still_an_ordinary_function():
    @tool
    def double(n: int) -> int:
        return n * 2

    # So it stays testable without the runtime. Calling it this way performs
    # the action with no ledger row behind it, which is right for a test.
    assert double(n=21) == 42


def test_the_key_ttl_is_recorded_in_seconds():
    @tool(effect=EffectClass.IDEMPOTENT_BY_KEY, key_ttl_hours=24)
    def refund(*, idempotency_key: str) -> dict:
        return {}

    assert refund.key_ttl_seconds == 24 * 3600


def test_a_tool_may_be_renamed():
    @tool(name="create_refund")
    def _impl(order_id: int) -> dict:
        return {}

    assert _impl.name == "create_refund"


# --- refusals -------------------------------------------------------------


def test_an_effectful_tool_that_cannot_receive_the_key_is_refused():
    """The class asserts the provider deduplicates by key. It cannot be true
    unless the key goes out with the request."""
    with pytest.raises(ToolDeclarationError, match="cannot receive the idempotency key"):

        @tool(effect=EffectClass.IDEMPOTENT_BY_KEY)
        def refund(order_id: int) -> dict:
            return {}


def test_a_queryable_tool_that_cannot_receive_the_key_is_refused():
    with pytest.raises(ToolDeclarationError, match="cannot receive the idempotency key"):

        @tool(effect=EffectClass.QUERYABLE)
        def send(to: str) -> dict:
            return {}


def test_receiving_the_whole_call_counts_as_receiving_the_key():
    @tool(effect=EffectClass.IDEMPOTENT_BY_KEY)
    def refund(order_id: int, call: ToolCall) -> dict:
        return {"key": call.idempotency_key}

    assert refund.invoke(call(order_id=1))["key"] == "run-1:S1:E1"


def test_a_pure_read_that_asks_for_a_key_is_refused():
    """It bypasses the ledger and is never issued one, so the parameter would
    always be empty — which reads as "there was no key" rather than "this tool
    was misclassified"."""
    with pytest.raises(ToolDeclarationError, match="is NONE but takes"):

        @tool
        def lookup(*, idempotency_key: str) -> dict:
            return {}


def test_a_reconcile_hook_is_refused_on_anything_but_queryable():
    @tool(effect=EffectClass.IDEMPOTENT_BY_KEY)
    def refund(*, idempotency_key: str) -> dict:
        return {}

    with pytest.raises(ToolDeclarationError, match="IDEMPOTENT_BY_KEY resolves by"):

        @refund.reconcile
        def _(idempotency_key: str) -> dict | None:
            return None


def test_two_reconcile_hooks_are_refused():
    @tool(effect=EffectClass.QUERYABLE)
    def send(*, idempotency_key: str) -> dict:
        return {}

    @send.reconcile
    def _first(idempotency_key: str) -> dict | None:
        return None

    with pytest.raises(ToolDeclarationError, match="already has a reconcile hook"):

        @send.reconcile
        def _second(idempotency_key: str) -> dict | None:
            return None


def test_a_hook_asking_for_something_nothing_supplies_is_refused():
    @tool(effect=EffectClass.QUERYABLE)
    def send(*, idempotency_key: str) -> dict:
        return {}

    with pytest.raises(ToolDeclarationError, match="which nothing supplies"):

        @send.reconcile
        def _(order_id: int) -> dict | None:
            return None


def test_a_negative_ttl_is_refused():
    with pytest.raises(ToolDeclarationError, match="cannot be negative"):

        @tool(effect=EffectClass.IDEMPOTENT_BY_KEY, key_ttl_hours=-1)
        def refund(*, idempotency_key: str) -> dict:
            return {}


# --- invoking -------------------------------------------------------------


def test_the_payload_binds_to_the_parameters():
    @tool
    def add(a: int, b: int) -> int:
        return a + b

    assert add.invoke(call(a=2, b=3)) == 5


def test_an_async_tool_works():
    @tool(effect=EffectClass.IDEMPOTENT_BY_KEY)
    async def refund(amount: int, *, idempotency_key: str) -> dict:
        return {"amount": amount, "key": idempotency_key}

    assert refund.is_async is True
    assert refund.invoke(call(amount=5)) == {"amount": 5, "key": "run-1:S1:E1"}


# --- reconciling ----------------------------------------------------------


def test_a_returned_value_is_read_as_committed():
    @tool(effect=EffectClass.QUERYABLE)
    def send(*, idempotency_key: str) -> dict:
        return {}

    @send.reconcile
    def _(idempotency_key: str) -> dict:
        return {"reference": "msg_1"}

    got = send.run_reconcile(effect())
    assert got.kind is ResolutionKind.COMMITTED
    assert got.external_ref == "msg_1"
    assert got.response == {"reference": "msg_1"}


def test_none_is_read_as_not_executed():
    """Safe only because the runtime escalates past the declared key TTL: a
    "not found" from a provider that forgot the key is not evidence that
    nothing happened."""

    @tool(effect=EffectClass.QUERYABLE, key_ttl_hours=24)
    def send(*, idempotency_key: str) -> dict:
        return {}

    @send.reconcile
    def _(idempotency_key: str) -> dict | None:
        return None

    assert send.run_reconcile(effect()).kind is ResolutionKind.NOT_EXECUTED


def test_an_explicit_resolution_is_passed_through():
    @tool(effect=EffectClass.QUERYABLE)
    def send(*, idempotency_key: str) -> dict:
        return {}

    @send.reconcile
    def _(idempotency_key: str) -> Resolution:
        return Resolution.unknown("the search index is stale")

    got = send.run_reconcile(effect())
    assert got.kind is ResolutionKind.STILL_UNKNOWN
    assert "stale" in got.detail


def test_a_hook_may_take_the_whole_effect():
    @tool(effect=EffectClass.QUERYABLE)
    def send(*, idempotency_key: str) -> dict:
        return {}

    seen: list[str] = []

    @send.reconcile
    def _(effect: Effect) -> None:
        seen.append(effect.idempotency_key)
        return None

    send.run_reconcile(effect("run-9:S2:E1"))
    assert seen == ["run-9:S2:E1"]

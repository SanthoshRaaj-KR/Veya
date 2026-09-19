"""The refund agent from README §7.2, actually runnable.

Two terminals:

    veya-runtime --store memory --dispatch inproc \\
      --grpc 127.0.0.1:50551 --agent refund_agent --agent-version v1

    python -m veya.serve examples/refund_agent.py

Then, in a third:

    veya run start --input '{"order_id": 987}'

Or run the whole thing as one command with `make demo-python`.

The providers here are fakes, and they are fakes that honour their declared
contracts — which is the part that matters. A tool that claims to be QUERYABLE
and cannot actually answer is a lie the runtime only discovers during an
outage, so the fake mailbox below really does record what it sent under the
caller's idempotency key, and really can be asked about it afterwards.
"""

from __future__ import annotations

import threading
from typing import Any

from veya import EffectClass, Fail, NotExecuted, agent, tool

# --- the providers, such as they are --------------------------------------

_ORDERS: dict[int, dict[str, Any]] = {
    987: {"id": 987, "status": "DELIVERED", "amount": 4_200, "email": "ana@example.com"},
    988: {"id": 988, "status": "IN_TRANSIT", "amount": 1_500, "email": "bo@example.com"},
    989: {"id": 989, "status": "DELIVERED", "amount": 91_000, "email": "cy@example.com"},
}

_lock = threading.Lock()

# What the payment provider has actually refunded, keyed by idempotency key.
# A real provider keeps this; ours keeps it so the IDEMPOTENT_BY_KEY claim is
# true rather than merely asserted.
_refunds: dict[str, dict[str, Any]] = {}

# What the mail provider has actually sent, keyed the same way. Recording under
# the caller's key is what makes the QUERYABLE lookup able to answer at all.
_sent: dict[str, str] = {}

_counter = 0


def _next_reference(prefix: str) -> str:
    global _counter
    _counter += 1
    return f"{prefix}_{9_000 + _counter}"


# --- the tools ------------------------------------------------------------


@tool(effect=EffectClass.NONE)
def lookup_order(order_id: int) -> dict[str, Any]:
    """A pure read.

    It changes nothing, so running it twice costs latency and nothing else, and
    it bypasses the effect ledger entirely — no row, no key, no reconciliation.
    """
    order = _ORDERS.get(order_id)
    if order is None:
        # Provably local: we never reached a provider, because there is nothing
        # to reach. A NONE tool has no ledger row either way, but being precise
        # here costs nothing and the habit is worth keeping.
        raise NotExecuted(f"no order {order_id}")
    return dict(order)


@tool(effect=EffectClass.IDEMPOTENT_BY_KEY, key_ttl_hours=24)
def create_refund(order_id: int, amount: int, *, idempotency_key: str) -> dict[str, Any]:
    """Money moves. The provider deduplicates by idempotency key.

    ``key_ttl_hours=24`` is not decoration. It is how long this provider
    promises to remember a key, and past that window the runtime escalates
    rather than reconciling — because "not found" from a provider that has
    forgotten the key is not evidence that nothing happened.
    """
    with _lock:
        # This is the deduplication the effect class claims. A real provider
        # does it on its side; the point is that the key must reach it, which
        # is why the handler is given one.
        existing = _refunds.get(idempotency_key)
        if existing is not None:
            return {**existing, "replayed": True}

        record = {
            "reference": _next_reference("rf"),
            "order_id": order_id,
            "amount": amount,
        }
        _refunds[idempotency_key] = record
        return {**record, "replayed": False}


@tool(effect=EffectClass.QUERYABLE, key_ttl_hours=24)
def send_confirmation(to: str, refund_ref: str, *, idempotency_key: str) -> dict[str, Any]:
    """An email goes out. It cannot be unsent, but the provider can be asked.

    Declared QUERYABLE because the reconcile hook below can genuinely answer
    "did the message under this key go out?". If it could not, the honest
    declaration would be UNRECONCILABLE, and an ambiguous send would park the
    run for a human rather than being resolved automatically.
    """
    with _lock:
        if idempotency_key in _sent:
            return {"to": to, "delivered": True, "reference": _sent[idempotency_key]}

        reference = _next_reference("msg")
        _sent[idempotency_key] = reference
        return {"to": to, "delivered": True, "reference": reference, "refund_ref": refund_ref}


@send_confirmation.reconcile
def _(idempotency_key: str) -> dict[str, Any] | None:
    """Ask the provider what it did, and never do it.

    A hook that sends rather than looks would turn the reconciliation of an
    already-delivered message into a second one — the exact duplicate the
    ledger exists to prevent.

    Returning None means the provider was reached and had no record of this
    key. The runtime treats that as "it did not happen", which is safe only
    because it escalates past the 24-hour TTL declared above.
    """
    with _lock:
        reference = _sent.get(idempotency_key)
    return {"reference": reference, "delivered": True} if reference else None


# --- the agent ------------------------------------------------------------


@agent(
    name="refund_agent",
    version="v1",
    tools=[lookup_order, create_refund, send_confirmation],
)
async def refund_agent(ctx: Any, order_id: int) -> dict[str, Any]:
    """Refund an order and tell the customer.

    Every ``await ctx.call`` is a durable step. If this process dies between
    the refund and the confirmation, the run replays the completed steps from
    history, reconciles the refund's effect, and resumes at the email — without
    re-refunding.

    Note what the branch is made of: ``order["status"]`` came out of a
    ``ctx.call``, so it is recorded in history and the branch reproduces. A
    branch on the wall clock or a fresh random number would not, and would
    raise NonDeterminismError at the following step.
    """
    order = await ctx.call(lookup_order, order_id=order_id)

    if order["status"] != "DELIVERED":
        return {"refunded": False, "reason": f"order is {order['status']}, not delivered"}

    if order["amount"] > 10_000:
        # Layer 5 turns this into `await ctx.wait_for("manager_approval")`.
        # Until durable signals exist, failing deliberately is the honest
        # version: the run stops, says why, and nobody is misled into thinking
        # an approval was recorded.
        raise Fail(f"order {order['id']} is {order['amount']} and needs manager approval")

    refund = await ctx.call(create_refund, order_id=order["id"], amount=order["amount"])

    await ctx.call(
        send_confirmation,
        to=order["email"],
        refund_ref=refund["reference"],
    )

    return {"refunded": True, "reference": refund["reference"], "amount": order["amount"]}

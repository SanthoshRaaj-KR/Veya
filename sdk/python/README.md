# veya (Python SDK)

Write an agent as an ordinary Python function. The Veya runtime keeps it
durable: every `ctx.call` is a step recorded in history, every effect gets an
idempotency key derived from its logical position, and a process that dies
mid-call reconciles rather than guessing.

```python
from veya import EffectClass, agent, tool


@tool(effect=EffectClass.NONE)
def lookup_order(order_id: int) -> dict:
    return payments.get_order(order_id)


@tool(effect=EffectClass.IDEMPOTENT_BY_KEY, key_ttl_hours=24)
def create_refund(order_id: int, amount: int, *, idempotency_key: str) -> dict:
    return payments.refund(
        order_id=order_id,
        amount=amount,
        headers={"Idempotency-Key": idempotency_key},
    )


@agent(name="refund_agent", version="v1", tools=[lookup_order, create_refund])
async def refund_agent(ctx, order_id: int) -> dict:
    order = await ctx.call(lookup_order, order_id=order_id)
    if order["status"] != "DELIVERED":
        return {"refunded": False, "reason": "order not delivered"}

    refund = await ctx.call(create_refund, order_id=order["id"], amount=order["amount"])
    return {"refunded": True, "reference": refund["reference"]}
```

Then:

```bash
veya-runtime --grpc 127.0.0.1:50551 --agent refund_agent --agent-version v1
python -m veya.serve refund_agent.py            # in another terminal
```

## The one rule your code has to follow

**Issue your durable calls in the same order every time.**

An idempotency key is `run_id:step_id:effect_seq`, and the step number comes
from invocation order. If a replay of your function reaches its calls in a
different order, the same logical action gets a different key and the ledger
stops protecting it.

In practice this means: branch on values that came out of `ctx.call`, not on
the wall clock, a random number, or anything else that can differ between the
first execution and the replay. `ctx.now()` and `ctx.random()` exist so the
rule has a compliant path.

Divergence that reaches a durable call raises `NonDeterminismError`, naming the
step, what history recorded, and what your function produced this time.

## What runs where

Your process holds the agent body and the tool bodies. The runtime holds the
ledger, the leases and the claim loop, and it is the only thing that decides
when an effect is safe to perform. See `docs/worker-protocol.md` in the
repository for why the boundary is drawn there.

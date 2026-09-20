"""An agent that fans out, sleeps, and waits for a human.

The refund agent is a straight line: decide, call, decide, call. This one is
every shape Layer 5 added, in the order an onboarding flow actually needs them.

    check three providers at once, and carry on when two have answered
    wait a day before the follow-up
    stop until a person approves the account
    send the welcome

Two terminals:

    veya-runtime --store memory --dispatch inproc \\
      --grpc 127.0.0.1:50551 --agent onboarding_agent --agent-version v1

    python -m veya.serve examples/onboarding_agent.py

Then, in a third:

    veya run start --agent onboarding_agent --input '{"email": "ana@example.com"}'
    veya signal RUN_ID approval --payload '{"by": "ops"}'

Or run the whole thing as one command with `make demo-onboarding`.

# What to look at while it runs

`veya run show RUN_ID` twice: once while it is asleep and once while it is
waiting for the approval. The two look completely different, which is the
point of the `waiting` line — before it, a run parked until tomorrow and a run
that is wedged were the same three words of output.

The screening checks are deliberately uneven and one of them deliberately
fails. A quorum of two means the run carries on without the third, which is
the behaviour worth seeing: the slow provider is not cancelled, its result
still lands in the ledger, and the run did not wait for it.
"""

from __future__ import annotations

import datetime as dt
import threading
import time
from typing import Any

from veya import Call, EffectClass, Fail, Join, Resolution, ResolutionKind, agent, tool

# --- the providers, such as they are --------------------------------------

_lock = threading.Lock()

# What the mail provider has actually sent, keyed by idempotency key. Recording
# under the caller's key is what lets the reconcile hook answer at all.
_sent: dict[str, str] = {}

_counter = 0


def _next_reference(prefix: str) -> str:
    """Called with _lock already held. threading.Lock is not reentrant, so
    taking it again here deadlocks the tool -- which presents as a task stuck
    in RUNNING with an EFFECT_CREATED and nothing after it."""
    global _counter
    _counter += 1
    return f"{prefix}_{5_000 + _counter}"


# --- the tools ------------------------------------------------------------


@tool(effect=EffectClass.NONE)
def screen_sanctions(email: str) -> dict[str, Any]:
    """A read. Fast, and no consequence if it happens twice."""
    return {"check": "sanctions", "clear": not email.startswith("blocked")}


@tool(effect=EffectClass.NONE)
def screen_credit(email: str) -> dict[str, Any]:
    """A read that is slow, the way a third-party credit check is slow.

    Under a quorum of two this is usually the one the run does not wait for.
    """
    time.sleep(0.4)
    return {"check": "credit", "clear": True, "score": 720}


@tool(effect=EffectClass.NONE)
def screen_identity(email: str) -> dict[str, Any]:
    """A read that fails, on purpose.

    A screening provider being down is ordinary, and a flow that cannot
    proceed without all three of them is a flow that stops every time one
    provider has a bad afternoon. The run decides what two out of three means;
    the runtime does not.
    """
    raise RuntimeError("identity provider returned 503")


@tool(effect=EffectClass.QUERYABLE, key_ttl_hours=24)
def send_welcome(to: str, *, idempotency_key: str) -> dict[str, Any]:
    """An email: it happens once or the customer notices.

    QUERYABLE, and it means it -- the fake provider records what it sent under
    the caller's idempotency key, so the hook below can be asked afterwards
    whether it went. A tool that claims this class and cannot actually answer
    is a lie the runtime only discovers during an outage.
    """
    with _lock:
        if idempotency_key in _sent:
            return {"reference": _sent[idempotency_key], "to": to, "deduplicated": True}
        reference = _next_reference("msg")
        _sent[idempotency_key] = reference
    return {"reference": reference, "to": to}


@send_welcome.reconcile
def _did_the_welcome_send(*, idempotency_key: str) -> Resolution:
    with _lock:
        reference = _sent.get(idempotency_key)

    if reference is not None:
        return Resolution(
            kind=ResolutionKind.COMMITTED,
            external_ref=reference,
            detail="the provider has a record under this key",
        )
    return Resolution(
        kind=ResolutionKind.NOT_EXECUTED,
        detail="the provider has no record under this key, and it keeps them indefinitely",
    )


# --- the agent ------------------------------------------------------------


@agent(
    name="onboarding_agent",
    version="v1",
    tools=[screen_sanctions, screen_credit, screen_identity, send_welcome],
)
async def onboarding_agent(
    ctx: Any, email: str = "", wait_seconds: float = 86_400
) -> dict[str, Any]:
    if not email:
        raise Fail("onboarding needs an email address")

    # Three screening checks at once. A quorum of two, because waiting for all
    # three means one flaky provider stops every signup -- and because the
    # third's answer rarely changes the decision.
    #
    # The failures come back as outcomes rather than exceptions. Whether one
    # failed check out of three is a problem is a question about onboarding,
    # not about the runtime, so it is answered here.
    screened = await ctx.call_parallel(
        [
            Call(screen_sanctions, email=email),
            Call(screen_credit, email=email),
            Call(screen_identity, email=email),
        ],
        join=Join.of(2),
    )

    answered = [o for o in screened if o.ok]
    if any(not o.result.get("clear", False) for o in answered):
        raise Fail(f"screening refused {email}")

    # A day before the follow-up, by default. Nothing is held anywhere while
    # this waits: the instant is in the database and the runtime's ordinary
    # scan is what notices it has arrived, so this survives a restart of the
    # worker, the runtime and PostgreSQL.
    #
    # wait_seconds is an input rather than a constant so the demo can pass a
    # couple of seconds. It is part of the run's input, so it is recorded, and
    # every replay computes the same instant from it.
    await ctx.sleep(dt.timedelta(seconds=wait_seconds))

    # And now a person. No deadline, which is the honest thing here: an
    # approval that times out on its own approves nothing, and a run visibly
    # waiting beats a run that gave up quietly.
    approval = await ctx.wait_for("approval")

    sent = await ctx.call(send_welcome, to=email)

    return {
        "email": email,
        "checks_answered": [o.result["check"] for o in answered],
        "checks_failed": [o.tool for o in screened if o.settled and not o.ok],
        "checks_still_running": [o.tool for o in screened if not o.settled],
        "approved_by": approval.get("by"),
        "welcome_reference": sent["reference"],
    }

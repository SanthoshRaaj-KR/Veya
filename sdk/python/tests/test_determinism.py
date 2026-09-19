"""The static guard, and the limits of it.

The last test in this file is the important one: it records that the guard is a
convenience and not the mechanism. A reader who mistakes it for the mechanism
will eventually write an agent that passes it and still forks a run.
"""

from __future__ import annotations

import datetime
import random
import time
import uuid
import warnings

import pytest

from veya import agent
from veya.determinism import DeterminismWarning, inspect_body


def findings(fn) -> list[str]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return inspect_body(fn, "test_agent")


def test_the_wall_clock_is_flagged():
    def body(ctx):
        return time.time()

    assert any("ctx.now()" in f for f in findings(body))


def test_datetime_now_is_flagged():
    def body(ctx):
        return datetime.datetime.now()

    assert any("ctx.now()" in f for f in findings(body))


def test_the_random_module_is_flagged():
    def body(ctx):
        return random.randint(1, 10)

    assert any("ctx.random()" in f for f in findings(body))


def test_uuid_is_flagged():
    def body(ctx):
        return str(uuid.uuid4())

    assert any("ctx.run_id" in f for f in findings(body))


def test_an_import_from_style_reference_is_flagged():
    from random import randint

    def body(ctx):
        return randint(1, 10)

    assert any("ctx.random()" in f for f in findings(body))


def test_a_class_imported_from_datetime_is_flagged():
    from datetime import datetime as dt

    def body(ctx):
        return dt.now()

    assert any("ctx.now()" in f for f in findings(body))


def test_a_reference_inside_a_comprehension_is_flagged():
    """Moving it into a comprehension did not make it deterministic, only
    harder to see."""

    def body(ctx):
        return [time.time() for _ in range(3)]

    assert any("ctx.now()" in f for f in findings(body))


def test_a_clean_body_is_silent():
    def body(ctx, order_id: int):
        return {"order": order_id}

    assert findings(body) == []


def test_arithmetic_on_datetimes_is_not_flagged():
    """`datetime` is used constantly for formatting and arithmetic. Only
    reading the present moment diverges, and flagging the rest would train
    authors to ignore the warning."""

    def body(ctx):
        return datetime.timedelta(days=1)

    assert findings(body) == []


def test_declaring_an_agent_warns():
    with pytest.warns(DeterminismWarning, match="ctx.now"):

        @agent(name="drifty", version="v1", tools=[])
        async def drifty(ctx) -> dict:
            return {"at": time.time()}


def test_the_check_can_be_turned_off():
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        @agent(name="considered", version="v1", tools=[], check_determinism=False)
        async def considered(ctx) -> dict:
            return {"at": time.time()}


def test_the_guard_does_not_see_through_a_function_call():
    """The limit, recorded on purpose.

    The guard reads one function's code. A body that calls a helper defined
    elsewhere passes it, and the helper may do anything at all. The check that
    actually holds is the comparison against history — see test_replay.py,
    test_a_wall_clock_in_a_payload_diverges — and this test exists so that
    nobody reads the guard as a guarantee.
    """

    def helper():
        return time.time()

    def body(ctx):
        return helper()

    assert findings(body) == []

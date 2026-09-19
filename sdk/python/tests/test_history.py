"""Folding an event log into something indexed by logical position."""

from __future__ import annotations

import json

import pytest

from veya.errors import ProtocolError
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


def test_run_started_supplies_the_agent_and_the_input():
    h = read(
        [ev(1, "RUN_STARTED", "", agent_name="a", agent_version="v2", input={"order_id": 7})],
        run_id="run-1",
    )
    assert h.agent_name == "a"
    assert h.agent_version == "v2"
    assert h.run_input == {"order_id": 7}


def test_a_created_step_is_recorded_as_owed():
    h = read([ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={"n": 1})])
    step = h.step("S1")

    assert step is not None
    assert step.tool == "lookup"
    assert step.payload == {"n": 1}
    assert step.completed is False
    assert step.result is None


def test_a_completed_step_carries_its_result():
    h = read(
        [
            ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={}),
            ev(3, "TASK_COMPLETED", "S1", result={"status": "DELIVERED"}),
        ]
    )
    step = h.step("S1")
    assert step is not None
    assert step.completed is True
    assert step.result == {"status": "DELIVERED"}


def test_a_retry_leaves_the_step_owed():
    """A non-final failure means the step will be tried again, so it must not
    be recorded as finished in either direction."""
    h = read(
        [
            ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={}),
            ev(3, "TASK_FAILED", "S1", error="timeout", final=False),
        ]
    )
    step = h.step("S1")
    assert step is not None
    assert step.completed is False
    assert step.error == ""


def test_a_final_failure_is_recorded():
    h = read(
        [
            ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={}),
            ev(3, "TASK_FAILED", "S1", error="timeout", final=False),
            ev(4, "TASK_FAILED", "S1", error="timeout again", final=True),
        ]
    )
    step = h.step("S1")
    assert step is not None
    assert step.error == "timeout again"
    assert step.completed is False


def test_a_late_failure_does_not_unsettle_a_completed_step():
    """A report arriving after a step completed — from a worker that lost its
    lease, say — must not be able to reopen it."""
    h = read(
        [
            ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={}),
            ev(3, "TASK_COMPLETED", "S1", result={"ok": True}),
            ev(4, "TASK_FAILED", "S1", error="stale report", final=True),
        ]
    )
    step = h.step("S1")
    assert step is not None
    assert step.completed is True
    assert step.result == {"ok": True}


def test_an_unknown_payload_version_is_refused():
    """A newer writer must not have its history silently misinterpreted by an
    older reader, because misinterpreted history is a forked run."""
    newer = Event("run-1", 1, "RUN_STARTED", "", b'{"v":99,"data":{}}', 0)
    with pytest.raises(ProtocolError, match="payload version"):
        read([newer])


def test_a_completion_with_no_creation_is_refused():
    with pytest.raises(ProtocolError, match="with no TASK_CREATED"):
        read([ev(2, "TASK_COMPLETED", "S1", result={})])


def test_unknown_event_types_are_ignored():
    """The runtime emits more events than the SDK has any use for — claims,
    leases, effects. Ignoring them means a new event type is not a breaking
    change for every worker in a fleet."""
    h = read(
        [
            ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={}),
            ev(3, "TASK_CLAIMED", "S1", worker_id="w-1"),
            ev(4, "EFFECT_CREATED", "S1", idempotency_key="run-1:S1:E1"),
            ev(5, "EFFECT_COMMITTED", "S1", idempotency_key="run-1:S1:E1"),
            ev(6, "TASK_COMPLETED", "S1", result={"ok": True}),
        ]
    )
    step = h.step("S1")
    assert step is not None
    assert step.completed is True
    assert len(h) == 5


def test_describe_sorts_payload_keys():
    """A divergence message that was itself non-deterministic would be a
    particularly unhelpful thing for this error to be."""
    h = read([ev(2, "TASK_CREATED", "S1", task_type="lookup", payload={"b": 2, "a": 1})])
    step = h.step("S1")
    assert step is not None
    assert step.describe() == 'lookup({"a": 1, "b": 2})'

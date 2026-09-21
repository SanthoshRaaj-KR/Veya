"""The errors an agent author sees, and the one they are asked to raise.

The interesting type here is :class:`NotExecuted`. Everything else names a
situation; that one makes a claim, and the claim changes what the runtime does
with an effect.
"""

from __future__ import annotations

__all__ = [
    "Cancel",
    "Fail",
    "NonDeterminismError",
    "NotExecuted",
    "ProtocolError",
    "SignalTimeout",
    "ToolFailed",
    "VeyaError",
]


class VeyaError(Exception):
    """Base class for everything this package raises."""


class ProtocolError(VeyaError):
    """The runtime sent something this build does not understand.

    Never worth retrying: a runtime that sent it once will send it again. It
    almost always means the runtime and this SDK were built against different
    revisions of the contract.
    """


class NotExecuted(VeyaError):
    """Assert that a tool's external action provably did not happen.

    Raise this, or pass ``not_executed=True`` to a plain exception's place,
    only when the failure is provably local: a payload that would not
    serialize, a required argument that was missing, a connection refused
    before anything was sent.

    It is a claim, not a hint. The runtime moves the effect to FAILED, which
    permits a clean retry. An ambiguous failure wrapped in this has defeated
    the protection the whole system exists to provide — so when in doubt, do
    not use it. An ordinary exception means "I do not know", the runtime
    reconciles instead of retrying, and being pessimistic costs a lookup.

    Wrap the original so the cause is not lost::

        try:
            resp = requests.post(url, json=body, timeout=5)
        except requests.exceptions.InvalidJSONError as exc:
            raise NotExecuted("payload would not serialize") from exc
    """


class ToolFailed(VeyaError):
    """A tool this run called has already failed, in recorded history.

    Raised during replay at the step that failed, so an agent body can see the
    failure at the point it happened rather than being restarted from a history
    it cannot explain.
    """

    def __init__(self, step_id: str, tool: str, message: str) -> None:
        super().__init__(f"step {step_id} ({tool}) failed: {message}")
        self.step_id = step_id
        self.tool = tool
        self.message = message


class Fail(VeyaError):
    """Raise this from an agent body to fail the run deliberately.

    Distinct from an unexpected exception only in intent, but the distinction
    is worth having: an operator reading a failed run should be able to tell
    "the agent decided this cannot proceed" from "the agent crashed".
    """


class Cancel(VeyaError):
    """Raise this from an agent body to end the run cancelled, not failed.

    Cancellation is cooperative and forward-looking: it prevents the *next*
    durable step, not an effect already in flight, and does not reverse one
    already committed. Marking a run CANCELLED after an email has been sent
    records a decision, not an undo -- reversing a committed effect needs an
    authored compensation, issued as ordinary tool calls before this is
    raised. See README section 11.

    Distinct from ``Fail`` in the same way ``Fail`` is distinct from a crash:
    an operator reading history should be able to tell "the agent decided to
    stop" from "the agent decided this cannot proceed".
    """


class NonDeterminismError(VeyaError):
    """The agent body did not reproduce itself on replay.

    History records that step S called tool T with payload P. Replaying the
    body produced something else at that position, which means the function's
    behaviour depends on something that is not in history.

    This is a check rather than a heuristic. It cannot miss a divergence that
    changed a durable call, because the comparison is against the durable call
    itself. It also cannot see a divergence that changed nothing durable — and
    that one did not matter.

    The usual causes, in the order they actually occur:

    * ``datetime.now()``, ``time.time()`` or ``random`` inside the body.
      Use ``ctx.now()`` and ``ctx.random()``.
    * iterating a ``set`` or a dict built from one, whose order varies.
    * reading a config file, an environment variable, or a database directly
      from the body instead of through a tool.
    * editing the agent and restarting it while a run was in flight. Bump the
      agent version instead; a version change is refused loudly, which is the
      better failure of the two.
    """

    def __init__(self, step_id: str, expected: str, actual: str) -> None:
        super().__init__(
            f"the agent body diverged at step {step_id}: "
            f"history records {expected}, but this replay produced {actual}. "
            f"See NonDeterminismError's documentation for the usual causes."
        )
        self.step_id = step_id
        self.expected = expected
        self.actual = actual


class SignalTimeout(VeyaError):
    """A ``ctx.wait_for`` reached its deadline with nothing having arrived.

    Raised at the step that waited, during replay, so an agent body handles it
    where it happened -- exactly like ToolFailed. The distinction it draws is
    the one an author needs: nothing arrived, as opposed to something arrived
    and was unwelcome.

    Catching it is the normal thing to do. A wait with a deadline and no
    handler is a run that fails on a timeout its author chose, which is rarely
    what they meant by choosing one.
    """

    def __init__(self, step_id: str, name: str) -> None:
        super().__init__(f"step {step_id}: nothing arrived for signal {name!r} before its deadline")
        self.step_id = step_id
        self.name = name

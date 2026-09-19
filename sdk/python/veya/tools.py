"""Declaring a tool.

A tool is a function plus a statement about its consequences. The statement is
the part the runtime acts on, so most of this module is about refusing
statements a function cannot actually honour.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from veya.effects import Effect, EffectClass, Resolution, ResolutionKind, ToolCall
from veya.errors import VeyaError

__all__ = ["Tool", "tool"]

F = TypeVar("F", bound=Callable[..., Any])

# The parameter a tool receives its idempotency key through. Named rather than
# positional so that adding it to an existing function does not reorder
# anything the payload fills in.
KEY_PARAM = "idempotency_key"

# The parameter a tool receives the whole ToolCall through, for a handler that
# wants the attempt number or the run id.
CALL_PARAM = "call"


class ToolDeclarationError(VeyaError):
    """A tool declares something its function cannot honour.

    Raised at import time, on purpose. The Go registry panics at startup for
    the same class of mistake, and for the same reason: discovering during an
    outage that a tool believed to be QUERYABLE has no way to answer is far
    worse than failing to start.
    """


class Tool:
    """One declared tool.

    Instances are created by the :func:`tool` decorator and are callable, so a
    tool can still be used as an ordinary function in a unit test without the
    runtime being involved.
    """

    __slots__ = ("_reconciler", "effect", "fn", "is_async", "key_ttl_seconds", "name")

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        name: str,
        effect: EffectClass,
        key_ttl_seconds: int,
    ) -> None:
        self.fn = fn
        self.name = name
        self.effect = effect
        self.key_ttl_seconds = key_ttl_seconds
        self.is_async = inspect.iscoroutinefunction(fn)
        self._reconciler: Callable[..., Any] | None = None

    def __repr__(self) -> str:
        return f"<Tool {self.name} {self.effect.value}>"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the underlying function directly.

        Present so that a tool remains testable in isolation. Calling it this
        way performs the action with no ledger row behind it, which is correct
        for a test and wrong for an agent: inside an agent body, reach a tool
        through ``ctx.call``.
        """
        return self.fn(*args, **kwargs)

    # --- reconciliation ---------------------------------------------------

    def reconcile(self, fn: F) -> F:
        """Register the hook that answers "did this actually happen?".

        The hook is given the idempotency key the original call used, and must
        *query* the provider — never perform the action. A hook that executes
        turns the reconciliation of an already-committed effect into exactly
        the duplicate the ledger exists to prevent.

        Three shapes are accepted, by parameter name::

            @send_invoice.reconcile
            def _(idempotency_key: str) -> dict | None: ...

            @send_invoice.reconcile
            def _(effect: Effect) -> Resolution: ...

            @send_invoice.reconcile
            def _(idempotency_key: str, effect: Effect) -> Resolution: ...

        Returning a ``Resolution`` says exactly what was established.
        Returning any other value is read as COMMITTED with that value as the
        recorded response. Returning ``None`` is read as NOT_EXECUTED: the
        provider was reached and had no record of the key.

        That last mapping is safe only because of something the runtime does
        separately. A "not found" from a provider that has *forgotten* the key
        is not evidence that nothing happened, so past the tool's declared
        ``key_ttl`` the runtime escalates rather than believing this answer.
        Declare a ``key_ttl`` that matches what the provider documents.

        If the hook cannot reach the provider at all, raise. An exception
        leaves the effect UNKNOWN, which is what it is, and it will be asked
        again.
        """
        if self.effect is not EffectClass.QUERYABLE:
            raise ToolDeclarationError(
                f"tool {self.name!r} is {self.effect.value} and cannot take a reconcile hook. "
                f"Only QUERYABLE tools are reconciled: IDEMPOTENT_BY_KEY resolves by "
                f"re-sending, NONE has nothing to resolve, and UNRECONCILABLE is "
                f"unresolvable by definition."
            )
        if self._reconciler is not None:
            raise ToolDeclarationError(f"tool {self.name!r} already has a reconcile hook")

        _require_params(fn, {KEY_PARAM, "effect"}, self.name, "a reconcile hook")
        self._reconciler = fn
        return fn

    @property
    def reconcilable(self) -> bool:
        """Whether a reconcile hook has been registered."""
        return self._reconciler is not None

    def run_reconcile(self, effect: Effect) -> Resolution:
        """Ask the hook about one effect and normalise its answer."""
        if self._reconciler is None:
            raise ToolDeclarationError(f"tool {self.name!r} was asked to reconcile but has no hook")

        kwargs: dict[str, Any] = {}
        params = inspect.signature(self._reconciler).parameters
        if KEY_PARAM in params:
            kwargs[KEY_PARAM] = effect.idempotency_key
        if "effect" in params:
            kwargs["effect"] = effect

        answer = _maybe_await(self._reconciler(**kwargs))

        if isinstance(answer, Resolution):
            return answer
        if answer is None:
            return Resolution.not_executed()
        return Resolution(
            kind=ResolutionKind.COMMITTED,
            response=answer,
            external_ref=_reference_in(answer),
            detail="the provider had a record under this key",
        )

    # --- execution --------------------------------------------------------

    def invoke(self, call: ToolCall) -> Any:
        """Perform one call, binding the payload to the function's parameters."""
        kwargs = dict(call.payload)
        params = inspect.signature(self.fn).parameters

        if KEY_PARAM in params:
            kwargs[KEY_PARAM] = call.idempotency_key
        if CALL_PARAM in params:
            kwargs[CALL_PARAM] = call

        return _maybe_await(self.fn(**kwargs))


def tool(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    effect: EffectClass = EffectClass.NONE,
    key_ttl_hours: float | None = None,
) -> Any:
    """Declare a function as a tool.

    ``effect`` says what can be done about an ambiguous outcome; see
    :class:`~veya.effects.EffectClass`, and read it before defaulting.

    ``key_ttl_hours`` is how long the provider actually honours an idempotency
    key. Half of an exactly-once guarantee lives in someone else's system under
    someone else's retention policy — Stripe forgets keys after 24 hours — and
    an effect reconciled past that window gets "not found" from a provider that
    forgot rather than from one that never acted. Past the TTL the runtime
    escalates instead of believing the answer. Leaving it unset claims
    indefinite retention, which is only honest for a provider that documents
    it.
    """

    def declare(f: Callable[..., Any]) -> Tool:
        tool_name = name or f.__name__
        _check_key_parameter(f, tool_name, effect)

        ttl = 0 if key_ttl_hours is None else int(key_ttl_hours * 3600)
        if ttl < 0:
            raise ToolDeclarationError(f"tool {tool_name!r}: key_ttl_hours cannot be negative")

        return Tool(f, name=tool_name, effect=effect, key_ttl_seconds=ttl)

    if fn is not None:
        return declare(fn)
    return declare


def _check_key_parameter(fn: Callable[..., Any], name: str, effect: EffectClass) -> None:
    """Refuse a declaration the function cannot honour.

    A tool classified IDEMPOTENT_BY_KEY is asserting that its provider
    deduplicates by idempotency key, and it cannot make that true unless it is
    given the key to send. The same goes for QUERYABLE: a reconcile hook is
    asked "did the action under this key happen?", which only has an answer if
    the original call recorded the key with the provider.

    So a function with external consequences that cannot receive the key is
    refused here rather than run and quietly duplicated later.
    """
    params = inspect.signature(fn).parameters
    has_key = KEY_PARAM in params or CALL_PARAM in params
    takes_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())

    if effect is EffectClass.NONE:
        if KEY_PARAM in params:
            raise ToolDeclarationError(
                f"tool {name!r} is NONE but takes {KEY_PARAM!r}. A NONE tool bypasses the "
                f"effect ledger and is never issued a key, so the parameter would always "
                f"be empty. If this call has an external consequence, classify it."
            )
        return

    if effect is EffectClass.UNRECONCILABLE:
        # It still gets a key and a ledger row; it simply has no way to use
        # either to resolve an ambiguity. Sending the key is harmless and
        # occasionally useful to a human reading the provider's logs.
        return

    if not has_key and not takes_kwargs:
        raise ToolDeclarationError(
            f"tool {name!r} is {effect.value} but its function cannot receive the "
            f"idempotency key. Add a keyword-only {KEY_PARAM!r} parameter and send it to "
            f"the provider — without it the class is a claim the tool has no way to "
            f"honour, and a retry after an ambiguous failure is a duplicate."
        )


def _require_params(fn: Callable[..., Any], allowed: set[str], tool_name: str, what: str) -> None:
    """Check that a hook asks only for things that can be supplied."""
    params = inspect.signature(fn).parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return

    unknown = [
        n
        for n, p in params.items()
        if n not in allowed and p.kind is not inspect.Parameter.VAR_POSITIONAL
    ]
    if unknown:
        raise ToolDeclarationError(
            f"tool {tool_name!r}: {what} asks for {unknown}, which nothing supplies. "
            f"Available: {sorted(allowed)}."
        )


def _maybe_await(result: Any) -> Any:
    """Resolve a coroutine from an async handler.

    Handlers run on a worker thread with no event loop of their own, so an
    async one gets a private loop for the duration of the call. This keeps
    ``async def`` tools working without making the whole session asynchronous,
    which would buy nothing: a tool call is one blocking request, and the
    concurrency that matters comes from the thread pool above.
    """
    if inspect.isawaitable(result):
        return asyncio.run(_await(result))
    return result


async def _await(awaitable: Any) -> Any:
    return await awaitable


def _reference_in(response: Any) -> str:
    """Pull the provider's own identifier out of a response, if it offered one.

    Best effort, and deliberately so. The reference is for operators and for
    lookups; the idempotency key, which we control, is what correctness rests
    on.
    """
    if not isinstance(response, dict):
        return ""
    for field in ("reference", "external_ref", "id", "reference_id"):
        value = response.get(field)
        if isinstance(value, str) and value:
            return value
    return ""

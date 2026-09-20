"""The client half of the worker protocol.

One process, one connection, one stream. The worker dials the runtime,
registers what it can do, and from then on answers requests that arrive down
the stream. It holds nothing durable and recovers nothing: if the connection
drops it reconnects and registers again, and whatever was in flight is the
runtime's problem — which it is equipped for, and this process is not.
"""

from __future__ import annotations

import json
import logging
import platform
import queue
import socket
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import grpc

from veya.agent import Agent, DecisionKind
from veya.effects import Effect, EffectClass, ResolutionKind, ToolCall
from veya.errors import NonDeterminismError, NotExecuted, ProtocolError, VeyaError
from veya.history import Event, read
from veya.tools import Tool
from veya.worker.v1 import worker_pb2 as pb
from veya.worker.v1 import worker_pb2_grpc as rpc

__all__ = ["Worker"]

log = logging.getLogger("veya")

_EFFECT_CLASS = {
    EffectClass.NONE: pb.EFFECT_CLASS_NONE,
    EffectClass.IDEMPOTENT_BY_KEY: pb.EFFECT_CLASS_IDEMPOTENT_BY_KEY,
    EffectClass.QUERYABLE: pb.EFFECT_CLASS_QUERYABLE,
    EffectClass.UNRECONCILABLE: pb.EFFECT_CLASS_UNRECONCILABLE,
}
_EFFECT_CLASS_BACK = {v: k for k, v in _EFFECT_CLASS.items()}

_RESOLUTION = {
    ResolutionKind.COMMITTED: pb.RESOLUTION_KIND_COMMITTED,
    ResolutionKind.NOT_EXECUTED: pb.RESOLUTION_KIND_NOT_EXECUTED,
    ResolutionKind.STILL_UNKNOWN: pb.RESOLUTION_KIND_STILL_UNKNOWN,
}

_DECISION = {
    DecisionKind.CALL_TOOL: pb.DECISION_KIND_CALL_TOOL,
    DecisionKind.COMPLETE: pb.DECISION_KIND_COMPLETE,
    DecisionKind.FAIL: pb.DECISION_KIND_FAIL,
    DecisionKind.SLEEP: pb.DECISION_KIND_SLEEP,
    DecisionKind.WAIT_FOR_SIGNAL: pb.DECISION_KIND_WAIT_FOR_SIGNAL,
}

# Reconnection backoff. A worker that reconnects instantly in a tight loop
# against a runtime that is down turns one outage into two.
_BACKOFF_FIRST = 0.5
_BACKOFF_MAX = 30.0


class Worker:
    """A process that serves one agent's body, its tools, or both.

    ``agent`` may be omitted for a tool host: a process that performs tool
    calls but has no agent body. It is then never asked to decide, which the
    runtime enforces rather than merely expecting.
    """

    def __init__(
        self,
        *,
        address: str = "127.0.0.1:50551",
        agent: Agent | None = None,
        tools: Iterable[Tool] = (),
        worker_id: str | None = None,
        max_concurrency: int = 8,
    ) -> None:
        if agent is None and not tools:
            raise VeyaError("a worker needs an agent, some tools, or both")

        self.address = address
        self.agent = agent
        self.worker_id = worker_id or f"{socket.gethostname()}-{_short_id()}"
        self.max_concurrency = max(1, max_concurrency)

        self.tools: dict[str, Tool] = {}
        for t in tools:
            self.tools[t.name] = t
        if agent is not None:
            # An agent's own tools are served by default, so a single-process
            # setup does not have to list them twice and cannot list them
            # inconsistently.
            for name, t in agent.tools.items():
                self.tools.setdefault(name, t)

        self._agent_name = agent.name if agent else ""
        self._agent_version = agent.version if agent else ""
        self._stopping = threading.Event()
        self._outbound: queue.Queue[pb.ClientMessage | None] = queue.Queue()

    # --- naming -----------------------------------------------------------

    def for_agent(self, name: str, version: str) -> Worker:
        """Serve tools for an agent this process has no body for.

        Needed only by a tool host: the tools belong to some agent, and the
        runtime routes by agent name, so the name has to come from somewhere.
        """
        self._agent_name = name
        self._agent_version = version
        return self

    # --- lifecycle --------------------------------------------------------

    def serve(self, *, reconnect: bool = True) -> None:
        """Connect and serve until stopped.

        Reconnects with backoff by default, because a worker that gives up on
        the first blip leaves an operator wondering why their agent stopped
        working an hour after a deploy.
        """
        if not self._agent_name or not self._agent_version:
            raise VeyaError(
                "this worker has tools but no agent name; call .for_agent(name, version) "
                "so the runtime knows whose tools these are"
            )

        backoff = _BACKOFF_FIRST
        while not self._stopping.is_set():
            try:
                self._session_once()
                backoff = _BACKOFF_FIRST
            except grpc.RpcError as rpc_error:
                if self._stopping.is_set():
                    return
                # RpcError is also a Call on every path that raises it, but
                # the stubs do not say so.
                failed: Any = rpc_error
                code = failed.code()
                if code is grpc.StatusCode.INVALID_ARGUMENT:
                    # The runtime refused the registration. Retrying cannot fix
                    # a declaration it rejected, and a loop that retries it
                    # forever buries the one message that says what is wrong.
                    raise VeyaError(
                        f"the runtime refused this worker: {failed.details()}"
                    ) from rpc_error
                log.warning("veya: disconnected (%s); reconnecting in %.1fs", code, backoff)

            if not reconnect or self._stopping.is_set():
                return
            if self._stopping.wait(backoff):
                return
            backoff = min(backoff * 2, _BACKOFF_MAX)

    def stop(self) -> None:
        """Ask serve() to return. Safe to call from a signal handler."""
        self._stopping.set()
        self._outbound.put(None)

    # --- one connection ---------------------------------------------------

    def _session_once(self) -> None:
        options = [
            # Match the server's keepalive so a long-idle agent does not have
            # its connection quietly dropped by something in between.
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
        ]
        with grpc.insecure_channel(self.address, options=options) as channel:
            stub: Any = rpc.WorkerStub(channel)
            self._outbound = queue.Queue()
            self._outbound.put(self._registration())

            responses = stub.Session(self._outgoing())

            # Handlers run on the pool so that one slow tool does not stall the
            # stream. The runtime issues requests concurrently and expects
            # replies matched by call id, never by order.
            with ThreadPoolExecutor(
                max_workers=self.max_concurrency, thread_name_prefix="veya-tool"
            ) as pool:
                try:
                    for message in responses:
                        if self._stopping.is_set():
                            break
                        self._dispatch(message, pool)
                finally:
                    self._outbound.put(None)

    def _registration(self) -> pb.ClientMessage:
        descriptors = [
            pb.ToolDescriptor(
                name=t.name,
                effect_class=_EFFECT_CLASS[t.effect],
                key_ttl_seconds=t.key_ttl_seconds,
                reconcilable=t.reconcilable,
            )
            for t in sorted(self.tools.values(), key=lambda t: t.name)
        ]
        return pb.ClientMessage(
            register=pb.Register(
                protocol_version=pb.PROTOCOL_VERSION_1,
                agent_name=self._agent_name,
                agent_version=self._agent_version,
                worker_id=self.worker_id,
                runtime=f"python {platform.python_version()}",
                decides=self.agent is not None,
                tools=descriptors,
            )
        )

    def _outgoing(self) -> Iterator[pb.ClientMessage]:
        """Drain the outbound queue onto the stream.

        gRPC consumes this on its own thread, which is what makes the queue
        necessary: several tool threads may finish at once, and a stream
        permits exactly one writer.
        """
        while True:
            message = self._outbound.get()
            if message is None:
                return
            yield message

    def _dispatch(self, message: pb.ServerMessage, pool: ThreadPoolExecutor) -> None:
        kind = message.WhichOneof("message")

        if kind == "registered":
            log.info(
                "veya: registered as %s with %s (%s), serving %d tool(s)",
                self.worker_id,
                self.address,
                message.registered.runtime_version,
                len(self.tools),
            )
        elif kind == "decide":
            pool.submit(self._send, self._decide, message.decide)
        elif kind == "execute":
            pool.submit(self._send, self._execute, message.execute)
        elif kind == "reconcile":
            pool.submit(self._send, self._reconcile, message.reconcile)
        else:
            # A runtime one revision ahead may send something this build has no
            # opinion about. Ignoring it beats closing a working connection.
            log.debug("veya: ignoring an unrecognised message %r", kind)

    def _send(self, handler: Any, request: Any) -> None:
        try:
            self._outbound.put(handler(request))
        except Exception:
            # A handler that cannot even build a reply would otherwise leave
            # the runtime waiting out a lease for nothing.
            log.exception("veya: failed to answer call %s", request.call_id)

    # --- the three request kinds -----------------------------------------

    def _decide(self, request: pb.DecideRequest) -> pb.ClientMessage:
        if self.agent is None:
            return pb.ClientMessage(
                decide_result=pb.DecideResult(
                    call_id=request.call_id,
                    failure=_failure(VeyaError("this worker hosts tools and has no agent body")),
                )
            )

        try:
            history = read(
                [_event(e) for e in request.history],
                run_id=request.run.run_id,
                run_input=_json(request.run.input),
            )
            decision = self.agent.decide(history)
        except NonDeterminismError as diverged:
            # Reported rather than converted into a FAIL decision, so the full
            # message reaches the runtime's log intact. It is the one error an
            # author has to read in full to act on.
            log.error("veya: %s", diverged)
            return pb.ClientMessage(
                decide_result=pb.DecideResult(call_id=request.call_id, failure=_failure(diverged))
            )
        except Exception as crashed:
            return pb.ClientMessage(
                decide_result=pb.DecideResult(call_id=request.call_id, failure=_failure(crashed))
            )

        return pb.ClientMessage(
            decide_result=pb.DecideResult(
                call_id=request.call_id,
                decision=pb.Decision(
                    kind=_DECISION[decision.kind],
                    step_id=decision.step_id,
                    tool=decision.tool,
                    payload=_encode(decision.payload) if decision.tool else b"",
                    output=_encode(decision.output),
                    error=decision.error,
                    wake_at_unix_nano=decision.wake_at_unix_nano,
                    signal=(
                        pb.SignalWait(
                            name=decision.signal_name,
                            deadline_unix_nano=decision.signal_deadline_unix_nano,
                        )
                        if decision.signal_name
                        else None
                    ),
                ),
            )
        )

    def _execute(self, request: pb.ExecuteRequest) -> pb.ClientMessage:
        tool = self.tools.get(request.tool)
        if tool is None:
            # Provably local: nothing was sent, because there was nothing to
            # send it with. This is one of the few places NOT_EXECUTED is
            # honest, and saying so lets the runtime retry cleanly once a
            # worker that does have the tool connects.
            return pb.ClientMessage(
                execute_result=pb.ExecuteResult(
                    call_id=request.call_id,
                    failure=_failure(
                        NotExecuted(f"this worker does not have a tool named {request.tool!r}")
                    ),
                )
            )

        call = ToolCall(
            run_id=request.run_id,
            task_id=request.task_id,
            step_id=request.step_id,
            idempotency_key=request.idempotency_key,
            payload=_json(request.payload),
            attempt=request.attempt or 1,
        )

        try:
            result = tool.invoke(call)
        except Exception as failed:
            return pb.ClientMessage(
                execute_result=pb.ExecuteResult(call_id=request.call_id, failure=_failure(failed))
            )

        return pb.ClientMessage(
            execute_result=pb.ExecuteResult(call_id=request.call_id, result=_encode(result))
        )

    def _reconcile(self, request: pb.ReconcileRequest) -> pb.ClientMessage:
        tool = self.tools.get(request.tool)
        if tool is None or not tool.reconcilable:
            return pb.ClientMessage(
                reconcile_result=pb.ReconcileResult(
                    call_id=request.call_id,
                    failure=_failure(
                        VeyaError(f"this worker has no reconcile hook for {request.tool!r}")
                    ),
                )
            )

        try:
            resolution = tool.run_reconcile(_effect(request.effect))
        except Exception as failed:
            # A hook that could not reach the provider has established nothing.
            # The effect stays UNKNOWN, which is what it is, and the runtime
            # will ask again.
            return pb.ClientMessage(
                reconcile_result=pb.ReconcileResult(
                    call_id=request.call_id, failure=_failure(failed)
                )
            )

        return pb.ClientMessage(
            reconcile_result=pb.ReconcileResult(
                call_id=request.call_id,
                resolution=pb.Resolution(
                    kind=_RESOLUTION[resolution.kind],
                    response=_encode(resolution.response),
                    external_ref=resolution.external_ref,
                    detail=resolution.detail,
                ),
            )
        )


# --- conversions ----------------------------------------------------------


def _failure(error: BaseException) -> pb.Failure:
    """Report an exception, saying what it proves about whether the action ran.

    Only :class:`~veya.errors.NotExecuted` claims the action did not happen.
    Everything else is ambiguous, and the runtime reconciles rather than
    retrying. That is the pessimistic answer, it is sometimes wasteful, and it
    is never wrong.
    """
    certainty = (
        pb.CERTAINTY_NOT_EXECUTED if isinstance(error, NotExecuted) else pb.CERTAINTY_UNKNOWN
    )
    return pb.Failure(
        message=str(error) or type(error).__name__,
        type=type(error).__name__,
        certainty=certainty,
    )


def _event(e: pb.Event) -> Event:
    return Event(
        run_id=e.run_id,
        seq=e.seq,
        type=e.type,
        step_id=e.step_id,
        payload=e.payload,
        created_at_unix_nano=e.created_at_unix_nano,
    )


def _effect(e: pb.Effect) -> Effect:
    return Effect(
        effect_id=e.effect_id,
        run_id=e.run_id,
        task_id=e.task_id,
        effect_type=e.effect_type,
        effect_class=_EFFECT_CLASS_BACK.get(e.effect_class, EffectClass.UNRECONCILABLE),
        idempotency_key=e.idempotency_key,
        status=e.status,
        external_ref=e.external_ref,
        request=_json(e.request),
        response=_json(e.response),
        created_at_unix_nano=e.created_at_unix_nano,
    )


def _json(raw: bytes) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"the runtime sent a payload that is not JSON: {raw!r}") from exc


def _encode(value: Any) -> bytes:
    """Render a result for the wire.

    ``None`` becomes empty rather than ``null``, so a tool that returns nothing
    leaves no result in history rather than the word "null" — which an operator
    reading a run would have to decide the meaning of.
    """
    if value is None:
        return b""
    return json.dumps(value, default=str).encode()


def _short_id() -> str:
    """A short per-process suffix, so two workers on one host do not collide.

    Derived from the clock rather than from `uuid`, which this SDK warns agent
    bodies against importing; a worker id is not replayed, but a module that
    preaches determinism should not need an exception for itself.
    """
    return f"{int(time.time() * 1000) % 1_000_000:06d}"

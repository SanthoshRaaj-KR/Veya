"""A gateway made of threads.

The SDK's suite runs against a real gRPC server implementing the same
``.proto`` the Go runtime does, and against nothing else. No PostgreSQL, no
NATS, no Go binary. That is not a convenience — it is the boundary doing its
job. If this file ever needs a database to work, the SDK has grown a dependency
on runtime internals the protocol was supposed to hide.
"""

from __future__ import annotations

import itertools
import json
import queue
import threading
from collections.abc import Callable, Iterator
from concurrent import futures
from typing import Any

import grpc
import pytest

from veya.history import PAYLOAD_VERSION
from veya.session import Worker
from veya.worker.v1 import worker_pb2 as pb
from veya.worker.v1 import worker_pb2_grpc as rpc

_call_ids = itertools.count(1)


class FakeGateway(rpc.WorkerServicer):
    """Stands in for the Go runtime's worker gateway.

    It speaks the protocol the same way round: the client registers, and from
    then on this side asks the questions.
    """

    def __init__(self) -> None:
        self.registration: pb.Register | None = None
        self.registered = threading.Event()
        self.disconnected = threading.Event()

        self._outbound: queue.Queue[pb.ServerMessage | None] = queue.Queue()
        self._replies: dict[str, queue.Queue[pb.ClientMessage]] = {}
        self._lock = threading.Lock()

    # --- the service ------------------------------------------------------

    # Named by the generated service, not by us.
    def Session(
        self,
        request_iterator: Iterator[pb.ClientMessage],
        context: grpc.ServicerContext,
    ) -> Iterator[pb.ServerMessage]:
        first = next(request_iterator)
        if not first.HasField("register"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "first message must be a Register")

        self.registration = first.register
        self.registered.set()
        yield pb.ServerMessage(
            registered=pb.Registered(session_id="fake-1", runtime_version="fake-gateway/1")
        )

        reader = threading.Thread(target=self._read, args=(request_iterator,), daemon=True)
        reader.start()

        while True:
            message = self._outbound.get()
            if message is None:
                self.disconnected.set()
                return
            yield message

    def _read(self, request_iterator: Iterator[pb.ClientMessage]) -> None:
        """Route each reply to whoever asked the question."""
        try:
            for message in request_iterator:
                kind = message.WhichOneof("message")
                call_id = getattr(getattr(message, kind or ""), "call_id", "")
                with self._lock:
                    waiting = self._replies.get(call_id)
                if waiting is not None:
                    waiting.put(message)
        except grpc.RpcError:
            pass
        finally:
            self.disconnected.set()

    def hang_up(self) -> None:
        self._outbound.put(None)

    # --- asking questions -------------------------------------------------

    def ask(
        self, build: Callable[[str], pb.ServerMessage], timeout: float = 5.0
    ) -> pb.ClientMessage:
        call_id = f"c{next(_call_ids)}"
        reply: queue.Queue[pb.ClientMessage] = queue.Queue(maxsize=1)
        with self._lock:
            self._replies[call_id] = reply

        self._outbound.put(build(call_id))
        try:
            return reply.get(timeout=timeout)
        finally:
            with self._lock:
                self._replies.pop(call_id, None)

    def decide(self, run: pb.Run, history: list[pb.Event]) -> pb.DecideResult:
        return self.ask(
            lambda cid: pb.ServerMessage(
                decide=pb.DecideRequest(call_id=cid, run=run, history=history)
            )
        ).decide_result

    def execute(
        self,
        tool: str,
        payload: Any = None,
        *,
        idempotency_key: str = "run-1:S1:E1",
        attempt: int = 1,
    ) -> pb.ExecuteResult:
        return self.ask(
            lambda cid: pb.ServerMessage(
                execute=pb.ExecuteRequest(
                    call_id=cid,
                    tool=tool,
                    run_id="run-1",
                    task_id="task-1",
                    step_id="S1",
                    idempotency_key=idempotency_key,
                    payload=json.dumps(payload or {}).encode(),
                    attempt=attempt,
                )
            )
        ).execute_result

    def reconcile(self, tool: str, idempotency_key: str) -> pb.ReconcileResult:
        return self.ask(
            lambda cid: pb.ServerMessage(
                reconcile=pb.ReconcileRequest(
                    call_id=cid,
                    tool=tool,
                    effect=pb.Effect(
                        effect_id="e-1",
                        run_id="run-1",
                        task_id="task-1",
                        effect_type=tool,
                        effect_class=pb.EFFECT_CLASS_QUERYABLE,
                        idempotency_key=idempotency_key,
                        status="UNKNOWN",
                    ),
                )
            )
        ).reconcile_result


@pytest.fixture
def gateway() -> Iterator[tuple[FakeGateway, str]]:
    """A running fake gateway and the address to dial it on."""
    service = FakeGateway()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_WorkerServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    yield service, f"127.0.0.1:{port}"

    service.hang_up()
    server.stop(grace=1).wait(timeout=5)


@pytest.fixture
def connect(gateway: tuple[FakeGateway, str]) -> Iterator[Callable[..., FakeGateway]]:
    """Start a worker against the fake gateway and wait for it to register."""
    service, address = gateway
    started: list[Worker] = []

    def start(**kwargs: Any) -> FakeGateway:
        worker = Worker(address=address, **kwargs)
        if kwargs.get("agent") is None:
            # A tool host has no agent body, so the runtime has no other way to
            # learn whose tools these are.
            worker.for_agent("refund_agent", "v1")
        started.append(worker)

        thread = threading.Thread(target=lambda: worker.serve(reconnect=False), daemon=True)
        thread.start()

        assert service.registered.wait(timeout=5), "the worker never registered"
        return service

    yield start

    for worker in started:
        worker.stop()


# --- building history -----------------------------------------------------


def event(seq: int, type_: str, step_id: str, **data: Any) -> pb.Event:
    """One history event, in the versioned envelope the runtime stores."""
    return pb.Event(
        run_id="run-1",
        seq=seq,
        type=type_,
        step_id=step_id,
        payload=json.dumps({"v": PAYLOAD_VERSION, "data": data}).encode(),
        created_at_unix_nano=1_700_000_000_000_000_000 + seq,
    )


def started(agent: str = "refund_agent", version: str = "v1", **run_input: Any) -> pb.Event:
    return event(1, "RUN_STARTED", "", agent_name=agent, agent_version=version, input=run_input)


def created(seq: int, step: str, tool: str, **payload: Any) -> pb.Event:
    return event(seq, "TASK_CREATED", step, task_id=f"t-{step}", task_type=tool, payload=payload)


def completed(seq: int, step: str, result: Any) -> pb.Event:
    return event(seq, "TASK_COMPLETED", step, task_id=f"t-{step}", result=result)


def failed(seq: int, step: str, error: str, *, final: bool = True) -> pb.Event:
    return event(seq, "TASK_FAILED", step, task_id=f"t-{step}", error=error, final=final)


def run(agent: str = "refund_agent", version: str = "v1", **run_input: Any) -> pb.Run:
    return pb.Run(
        run_id="run-1",
        agent_name=agent,
        agent_version=version,
        status="RUNNING",
        input=json.dumps(run_input).encode(),
    )

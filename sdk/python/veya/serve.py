"""Run a Python file's agent as a worker.

    python -m veya.serve refund_agent.py
    python -m veya.serve refund_agent.py --address 127.0.0.1:50551

The file is imported, the agent declared in it is found, and a worker serves
it until interrupted. There is nothing here that could not be written by hand
in six lines with :class:`veya.Worker`; it exists so that the first thing
someone runs is a command rather than a program they have to write first.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import signal
import sys
import types
from pathlib import Path
from typing import Any

from veya.agent import Agent
from veya.errors import VeyaError
from veya.session import Worker
from veya.tools import Tool

__all__ = ["main"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m veya.serve",
        description="Serve an agent defined in a Python file.",
    )
    parser.add_argument("path", help="the .py file declaring the agent")
    parser.add_argument(
        "--address",
        default="127.0.0.1:50551",
        help="the runtime's worker gateway (default: %(default)s)",
    )
    parser.add_argument(
        "--agent",
        default="",
        help="which agent to serve, when the file declares more than one",
    )
    parser.add_argument(
        "--worker-id",
        default="",
        help="how this process identifies itself in the runtime's logs",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="tool calls to run at once (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="default: %(default)s",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-5s %(message)s",
    )

    try:
        module = _import(Path(args.path))
        agent, tools = _find(module, args.agent, args.path)
    except VeyaError as problem:
        print(f"veya: {problem}", file=sys.stderr)
        return 1

    worker = Worker(
        address=args.address,
        agent=agent,
        tools=tools,
        worker_id=args.worker_id or None,
        max_concurrency=args.concurrency,
    )

    # SIGINT and SIGTERM ask the session to finish rather than killing it. A
    # tool call in flight is an external effect that may be halfway through
    # happening, and dropping the stream underneath it turns a clean shutdown
    # into an ambiguous outcome someone has to reconcile.
    def shut_down(*_: Any) -> None:
        print("\nveya: stopping", file=sys.stderr)
        worker.stop()

    signal.signal(signal.SIGINT, shut_down)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shut_down)

    if agent is not None:
        print(
            f"veya: serving agent {agent.name} {agent.version} "
            f"({len(worker.tools)} tool(s)) at {args.address}",
            file=sys.stderr,
        )

    try:
        worker.serve()
    except VeyaError as refused:
        print(f"veya: {refused}", file=sys.stderr)
        return 1
    return 0


def _import(path: Path) -> types.ModuleType:
    """Import a file by path, as its own module.

    The file's directory goes on sys.path first, so an agent that imports a
    sibling module works the way it would if it were run directly. Anything
    else would make `python -m veya.serve x.py` behave differently from
    `python x.py`, for no reason the author could guess.
    """
    if not path.exists():
        raise VeyaError(f"no such file: {path}")

    directory = str(path.parent.resolve())
    if directory not in sys.path:
        sys.path.insert(0, directory)

    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise VeyaError(f"{path} is not an importable Python file")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _find(module: types.ModuleType, wanted: str, path: str) -> tuple[Agent | None, list[Tool]]:
    """Find the agent to serve, and every tool declared beside it."""
    agents = [v for v in vars(module).values() if isinstance(v, Agent)]
    tools = [v for v in vars(module).values() if isinstance(v, Tool)]

    if wanted:
        for candidate in agents:
            if candidate.name == wanted:
                return candidate, tools
        raise VeyaError(
            f"{path} declares no agent named {wanted!r}; it has {[a.name for a in agents]}"
        )

    if len(agents) == 1:
        return agents[0], tools

    if not agents:
        if tools:
            # Legitimate: a tool host, which serves calls for an agent whose
            # body lives elsewhere. It needs to be told whose tools they are,
            # and this entry point has no way to know.
            raise VeyaError(
                f"{path} declares tools but no agent. To run it as a tool host, use "
                f"veya.Worker(tools=[...]).for_agent(name, version) directly."
            )
        raise VeyaError(f"{path} declares no agent and no tools")

    raise VeyaError(
        f"{path} declares {len(agents)} agents ({[a.name for a in agents]}); "
        f"say which one with --agent"
    )


if __name__ == "__main__":
    raise SystemExit(main())

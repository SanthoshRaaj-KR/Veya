#!/usr/bin/env bash
#
# Run the Python refund agent end to end, in one command.
#
# Two processes: a runtime serving the worker protocol, and a Python worker
# holding the agent. The runtime starts the run before the worker exists, which
# is deliberate — it is the normal opening state of any deployment, and the
# recovery loop drives the run as soon as a worker registers.
#
# Nothing durable is involved: --store memory, --dispatch inproc. The point
# here is the protocol, not the storage, and requiring Docker to see an agent
# run would put a container between someone and their first working example.
#
# Usage:  make demo-python
#         scripts/demo-python.sh            # the same thing, directly
set -euo pipefail

cd "$(dirname "$0")/.."

ADDR="${VEYA_GRPC:-127.0.0.1:50551}"
AGENT_FILE="${VEYA_AGENT_FILE:-examples/refund_agent.py}"
INPUT="${VEYA_DEMO_INPUT:-{\"order_id\": 987}}"

# The SDK is used from the source tree rather than from site-packages, so that
# this runs against the code in front of you rather than against whatever
# happens to be installed.
#
# The path is made native first. Under Git Bash on Windows the shell speaks
# POSIX paths and the Python interpreter does not, so a relative or
# slash-separated PYTHONPATH is silently ignored and the import fails with
# "No module named 'veya'" — which reads as a missing install rather than as a
# path that was never understood.
SDK_PATH="$PWD/sdk/python"
if command -v cygpath >/dev/null 2>&1; then
  SDK_PATH="$(cygpath -w "$SDK_PATH")"
  export PYTHONPATH="${SDK_PATH}${PYTHONPATH:+;$PYTHONPATH}"
else
  export PYTHONPATH="${SDK_PATH}${PYTHONPATH:+:$PYTHONPATH}"
fi

PYTHON="${PYTHON:-python}"
if ! "$PYTHON" -c "import grpc" >/dev/null 2>&1; then
  echo "demo-python: this needs grpcio. Install the SDK's dependencies:" >&2
  echo "    $PYTHON -m pip install -e sdk/python" >&2
  exit 1
fi

runtime_pid=""
worker_pid=""

cleanup() {
  # The worker first: it is the one holding a stream the runtime is waiting on,
  # and a gateway shutting down gracefully will wait for it.
  [ -n "$worker_pid" ] && kill "$worker_pid" 2>/dev/null || true
  [ -n "$runtime_pid" ] && kill "$runtime_pid" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "demo-python: starting the runtime on $ADDR"
GOFLAGS=-mod=mod go run ./cmd/veya-runtime \
  --store memory \
  --dispatch inproc \
  --grpc "$ADDR" \
  --agent refund_agent \
  --agent-version v1 \
  --demo \
  --demo-input "$INPUT" &
runtime_pid=$!

echo "demo-python: starting the Python worker for $AGENT_FILE"
"$PYTHON" -m veya.serve "$AGENT_FILE" --address "$ADDR" &
worker_pid=$!

# The runtime exits when the demo run reaches a terminal state, and its exit
# status is the demo's: a run that finished anything other than COMPLETED is a
# failed demo, not a quiet one.
set +e
wait "$runtime_pid"
status=$?
set -e

if [ "$status" -ne 0 ]; then
  echo "demo-python: the run did not complete (exit $status)" >&2
fi
exit "$status"

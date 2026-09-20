#!/usr/bin/env bash
#
# The Layer 5 worked example, end to end: a run that fans out, sleeps, and
# waits for a person.
#
# Unlike demo-python.sh this one needs PostgreSQL, and that is the point rather
# than an inconvenience. A signal comes from *outside* the process holding the
# run -- that is the whole reason signals exist -- so `veya signal` has to
# reach the same store the runtime is using. An in-memory store would mean the
# demo sending itself a signal, which proves nothing.
#
# Usage:  make up && make migrate && make demo-onboarding
set -euo pipefail

cd "$(dirname "$0")/.."

DSN="${VEYA_DSN:-postgres://veya:veya@localhost:5433/veya?sslmode=disable}"
ADDR="${VEYA_GRPC:-127.0.0.1:50551}"
AGENT_FILE="${VEYA_AGENT_FILE:-examples/onboarding_agent.py}"
EMAIL="${VEYA_DEMO_EMAIL:-ana@example.com}"

# See scripts/demo-python.sh for why this is made native first.
SDK_PATH="$PWD/sdk/python"
if command -v cygpath >/dev/null 2>&1; then
  SDK_PATH="$(cygpath -w "$SDK_PATH")"
  export PYTHONPATH="${SDK_PATH}${PYTHONPATH:+;$PYTHONPATH}"
else
  export PYTHONPATH="${SDK_PATH}${PYTHONPATH:+:$PYTHONPATH}"
fi

PYTHON="${PYTHON:-python}"
if ! "$PYTHON" -c "import grpc" >/dev/null 2>&1; then
  echo "demo-onboarding: this needs grpcio. Install the SDK's dependencies:" >&2
  echo "    $PYTHON -m pip install -e sdk/python" >&2
  exit 1
fi

runtime_pid=""
worker_pid=""

cleanup() {
  [ -n "$worker_pid" ] && kill "$worker_pid" 2>/dev/null || true
  [ -n "$runtime_pid" ] && kill "$runtime_pid" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Built once rather than `go run` per call: the polling below asks the store
# every second, and a compile per question makes the demo look slower than the
# runtime is.
echo "demo-onboarding: building"
GOFLAGS=-mod=mod go build -o bin/ ./cmd/veya ./cmd/veya-runtime
veya() { ./bin/veya "$@"; }

echo "demo-onboarding: starting the runtime on $ADDR"
./bin/veya-runtime \
  --store postgres \
  --dsn "$DSN" \
  --dispatch postgres \
  --grpc "$ADDR" \
  --agent onboarding_agent \
  --agent-version v1 \
  --workers 4 &
runtime_pid=$!

echo "demo-onboarding: starting the Python worker for $AGENT_FILE"
"$PYTHON" -m veya.serve "$AGENT_FILE" --address "$ADDR" &
worker_pid=$!

# The run is created before the worker is certainly registered, which is the
# normal opening state of any deployment: the recovery loop drives it as soon
# as a worker appears.
sleep 3
RUN_ID="$(veya run start --dsn "$DSN" --agent onboarding_agent \
  --input "{\"email\": \"$EMAIL\", \"wait_seconds\": 2}" 2>/dev/null | tail -1)"
echo "demo-onboarding: run $RUN_ID"

# Wait for the run to park on the approval. It fans out, joins two of three,
# sleeps, and only then asks for a person -- so this is also waiting out the
# fan-out and the timer.
echo "demo-onboarding: waiting for the run to ask for an approval"
for _ in $(seq 1 60); do
  if veya run show --dsn "$DSN" "$RUN_ID" 2>/dev/null | grep -q 'on signal "approval"'; then
    break
  fi
  sleep 1
done

echo
veya run show --dsn "$DSN" "$RUN_ID" | sed -n '1,8p'
echo

echo "demo-onboarding: approving"
veya signal --dsn "$DSN" --id demo-approval --payload '{"by":"ops"}' "$RUN_ID" approval

for _ in $(seq 1 60); do
  status="$(veya run show --dsn "$DSN" "$RUN_ID" 2>/dev/null | awk '/^status/{print $2}')"
  case "$status" in
    COMPLETED|FAILED|CANCELLED) break ;;
  esac
  sleep 1
done

echo
veya run show --dsn "$DSN" "$RUN_ID"
echo

if [ "${status:-}" != "COMPLETED" ]; then
  echo "demo-onboarding: the run finished as ${status:-nothing} rather than COMPLETED" >&2
  exit 1
fi
echo "demo-onboarding: ok"

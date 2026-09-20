# Veya

Veya is a **durable execution runtime for AI agents**.

It separates:
- **Agent reasoning** (what should happen next)
- **Execution guarantees** (what must not be lost or duplicated)

In practical terms, Veya helps you run long-lived, side-effecting agent workflows (refunds, emails, approvals, API calls) safely across crashes, restarts, and distributed workers.

---

## What Veya Solves

Agent workflows are hard to run reliably because they are:
- **Dynamic**: the next step is decided at runtime by a model.
- **Long-lived**: runs can survive deploys or process restarts.
- **Side-effecting**: tools can charge cards, send emails, or mutate external systems.

Without durable execution, crashes create ambiguity:
- Did the external action happen?
- Should we retry?
- Could retrying cause duplicates?

Veya addresses this with durable state, explicit effect tracking, idempotency keys, ownership leases, and serialized run advancement.

---

## Core Capabilities

- **Durable run state in PostgreSQL**
  - Run/task/event/effect/lease data is persisted and recoverable.
- **At-least-once task delivery**
  - Supports in-process, PostgreSQL dispatch, and NATS JetStream dispatch.
- **Safe external side effects**
  - Effect ledger records `PENDING`, `RUNNING`, `COMMITTED`, `UNKNOWN`, etc.
  - Uses stable idempotency keys per logical effect.
- **Crash-safe task ownership**
  - Leases + fencing tokens prevent stale workers from corrupting state.
- **Replay-oriented agent execution**
  - Decisions and outcomes are persisted for reliable continuation.
- **Python agent SDK + Go runtime**
  - Write agent/tool logic in Python; run durable execution engine in Go.
- **Operational CLI tooling**
  - Start runs, inspect runs/effects, signal waiting workflows, and run demos.

---

## High-Level Architecture

```text
Python Agent SDK  <->  Go Runtime/Engine  <->  PostgreSQL (truth)
                                   |\
                                   | \-> Outbox Relay -> NATS JetStream (delivery)
                                   |
                                   \-> Worker Processes -> External Tools/APIs
```

### Key idea
- **PostgreSQL is the source of truth** (state and history).
- **JetStream/Postgres dispatch is delivery infrastructure** (work notification), not authoritative state.

---

## Repository Layout

- `/cmd/veya` – main CLI (migrate, run/effect commands, etc.)
- `/cmd/veya-runtime` – runtime process (engine + optional workers)
- `/cmd/veya-worker` – standalone worker process
- `/internal/core` – domain types and core interfaces (ports)
- `/internal/engine` – run/task lifecycle and advancement logic
- `/internal/effects` – effect execution and reconciliation behavior
- `/internal/store` – memory + PostgreSQL store adapters and migrations
- `/internal/dispatch` – inproc/postgres/jetstream dispatch adapters
- `/internal/outbox` – transactional outbox relay
- `/proto/veya/worker/v1` – worker protocol definition
- `/sdk/python` – Python SDK for agent/tool authoring
- `/examples` – runnable examples (refund agent, onboarding flow)
- `/docs` – architecture, execution model, protocol, and status docs

---

## Prerequisites

### Required
- Go `1.26+`
- Python `3.11+` recommended

### Optional (for integration and distributed demos)
- Docker + Docker Compose
- NATS (via docker compose)
- PostgreSQL (via docker compose)

---

## Quick Start

### 1) Clone and build

```bash
make build
```

### 2) Run fast local checks (no Docker)

```bash
make test
make sdk-install
make sdk-test
```

### 3) Run a no-Docker demo

```bash
make demo-memory
```

### 4) Run with PostgreSQL + NATS

```bash
make up
make migrate
make test-integration
make demo-jetstream
```

### 5) Shut down local infra

```bash
make down
```

---

## Common Make Targets

- `make build` – compile binaries into `./bin`
- `make test` – Go unit tests
- `make test-race` – Go race tests (needs C toolchain)
- `make test-integration` – integration suite against Postgres + NATS
- `make migrate` – apply DB migrations
- `make up` / `make down` – start/stop local infra
- `make demo` – PostgreSQL-backed demo
- `make demo-memory` – in-memory demo
- `make demo-jetstream` – JetStream-backed demo
- `make demo-python` – Python refund agent demo
- `make demo-onboarding` – fan-out + timer + approval example
- `make runtime` – run engine without workers
- `make worker` – run standalone workers
- `make proto` / `make proto-check` – regenerate/verify protocol stubs

---

## Running Runtime and Worker Separately

Use this mode to simulate a distributed deployment:

```bash
# terminal 1
make runtime

# terminal 2
make worker
```

Then start runs through CLI commands.

---

## Python SDK (Agent Authoring)

The Python SDK supports:
- agent declaration
- tool registration
- effect-aware tool calls
- replay-compatible execution patterns
- durable primitives like sleep/wait/parallel calls

See:
- `/sdk/python/README.md`
- `/examples/refund_agent.py`
- `/examples/onboarding_agent.py`

Install SDK in editable mode:

```bash
make sdk-install
```

---

## Reliability Model (Short Version)

Veya is designed around **safety** (preventing bad outcomes) and **liveness** (eventual progress):

- **Task claim semantics** prevent concurrent ownership from advancing the same task.
- **Leases + heartbeats + fencing tokens** prevent stale workers from writing new state.
- **Effect ledger + idempotency keys** represent side effects explicitly, including ambiguous outcomes (`UNKNOWN`).
- **Run version checks (CAS-style advancement)** prevent divergent run history under concurrency.
- **Transactional outbox** ensures committed tasks are eventually delivered.

This favors correctness under failure rather than best-effort retries alone.

---

## What Veya Does Not Guarantee

- Not universal exactly-once side effects for every external system.
  - Exactly-once depends on tool/provider idempotency or lookup support.
- Not deterministic model reasoning across different runs.
- Not automatic semantic rollback of external operations.
- Not a policy/alignment engine; it is an execution runtime.

---

## Documentation Guide

Start here based on what you need:

- Architecture overview: `docs/architecture-primer.md`
- Code navigation map: `docs/code-map.md`
- Execution semantics: `docs/execution-model.md`
- Worker boundary/protocol rationale: `docs/worker-protocol.md`
- Current build/test/status snapshot: `docs/status.md`

---

## Current Project State

The project has implemented foundational runtime layers including:
- durable run/task/effect lifecycle
- distributed dispatch and outbox relay
- Python SDK with replay model
- suspension, signals, and fan-out/fan-in primitives

For the exact phase-level status and verification matrix, see:
- `docs/status.md`

---

## Contributing

1. Create a branch
2. Make focused changes
3. Run relevant checks (`make test`, `make sdk-test`, integration when needed)
4. Update docs when behavior or workflows change
5. Open a PR

---

## License

See `LICENSE`.

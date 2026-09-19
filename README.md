# Veya

A durable execution runtime for AI agents — it takes an agent's decisions, turns them into durable work, executes that work across workers, and recovers safely when anything crashes.

---

## Table of Contents

1. [Overview](#1-overview)
2. [The Problem](#2-the-problem)
3. [The Solution](#3-the-solution)
4. [Architecture](#4-architecture)
5. [Core Design](#5-core-design)
6. [Failure Scenarios](#6-failure-scenarios)
7. [Agent SDK](#7-agent-sdk)
8. [Data Model](#8-data-model)
9. [Execution Example](#9-execution-example)
10. [Recovery Example](#10-recovery-example)
11. [Guarantees & Limitations](#11-guarantees--limitations)
12. [Benchmarks](#12-benchmarks)
13. [Running Locally](#13-running-locally)
14. [Testing Failure Scenarios](#14-testing-failure-scenarios)
15. [Project Structure](#15-project-structure)
16. [Design Decisions & Tradeoffs](#16-design-decisions--tradeoffs)
17. [Roadmap](#17-roadmap)
18. [License](#18-license)

---

## 1. Overview

Veya is a durable execution runtime for AI agents. The agent decides *what* should happen; Veya guarantees that once a decision is made, failure does not turn it into lost work or an unintended duplicate side effect.

It runs locally. No cloud account, no hosted control plane — `docker compose up` and a `pip install`.

> **New here?** Read [**docs/architecture-primer.md**](docs/architecture-primer.md) first. It walks through the execution model in plain English, assuming only that you know what a transaction, a worker, and a queue are. This README is the specification; the primer is the explanation.

---

## 2. The Problem

### 2.1 Why AI agents are different

A conventional backend request follows a predictable path:

```
Request → Validate → Process → Database → Response
```

An AI agent's path is decided at runtime by a model:

```
User Request → LLM → Tool A → LLM → Tool B → LLM → Answer
```

The consequences are structural, not incidental:

- The sequence of steps is **not known before execution begins**.
- The same agent, run twice on the same input, may take **different paths**.
- Runs are **long-lived** — minutes to days — spanning deploys, restarts, and network partitions.
- Steps have **real-world side effects**: refunds, emails, deployments, database writes.

A process that holds agent state in memory loses all of it when the process dies. For a read-only agent that is an annoyance. For an agent that moves money, it is a correctness failure.

### 2.2 Failure scenario

Consider a refund agent:

```
LLM decides "issue refund"
        ↓
Worker calls Payment API
        ↓
Payment API refunds ₹500   ✓
        ↓
💥 Worker crashes before recording the result
```

The work is done in the real world. Nothing in the system knows it.

A replacement worker sees a task that was started and never finished. It has exactly two options, and both are wrong:

| Assumption | Consequence |
| --- | --- |
| "It probably didn't happen" → retry | Customer refunded twice |
| "It probably happened" → skip | Customer never refunded, and no record of the gap |

The system cannot choose correctly because it never recorded the one fact that matters: *an external side effect was attempted, and its outcome is unknown.*

### 2.3 Why retries are insufficient

Retries answer "run it again." They do not answer any of the questions that actually arise after a crash:

- **What was the agent doing?** — Retrying restarts the run from scratch, re-paying for every LLM call and re-executing every completed tool call.
- **What has it already decided?** — Re-invoking the LLM may produce a *different* decision than the one already partially executed, leaving the run in a state no code path anticipated.
- **Who owns this task right now?** — Without ownership, a hung worker and its replacement both execute the same task concurrently.
- **Did the external side effect happen?** — Retries assume failure. Sometimes the truth is *unknown*, and that distinction is the entire problem.

Retries are a mechanism. Durable execution is a model — one in which those four questions always have an answer stored somewhere that survives a crash.

---

## 3. The Solution

### 3.1 What the runtime does

Veya sits between the agent and the external world:

```
AI AGENT  →  VEYA RUNTIME  →  WORKERS  →  EXTERNAL SERVICES
```

It provides durable workflow execution, dynamic task scheduling, crash recovery, execution replay, task ownership and leasing, effect tracking, idempotent external operations, retries with reconciliation, and observability.

The governing principle:

> **The AI decides what should happen. The runtime guarantees the decision is executed reliably.**

Veya does not constrain the agent's reasoning. It constrains the *execution* of that reasoning.

### 3.2 Guarantees

| Guarantee | Meaning |
| --- | --- |
| **Durable progress** | A worker crash does not restart the run. Completed steps stay completed. |
| **Replayable execution** | Recorded decisions and results are replayed, not recomputed. Past LLM calls are not re-paid for. |
| **Dynamic workflows** | Execution paths are generated at runtime by the model, not declared up front. |
| **At-least-once delivery** | A task may be delivered more than once during recovery. Execution is built around idempotency. |
| **Protected side effects** | Every external effect has a stable identity and an independently tracked lifecycle, including `UNKNOWN`. |
| **Safe worker recovery** | Expired workers are replaced; stale workers cannot corrupt state after losing ownership. |
| **Serialized run advancement** | Concurrent task completions cannot fork a run into two divergent decision paths. |
| **Tool independence** | The runtime is agnostic to what a tool does. |

### 3.3 What it does NOT guarantee

Stated plainly, because the boundaries matter more than the features:

- **Not exactly-once execution of arbitrary tools.** Exactly-once external effects require the *tool* to accept an idempotency key or expose a lookup. Veya provides stable identities and tracks outcomes; a tool that supports neither can only be made at-most-once or at-least-once, never exactly-once.
- **Not deterministic LLM output.** Past decisions are replayed from history. Future decisions are freshly generated and may differ between runs. This is intentional.
- **Not automatic rollback.** A committed effect is not undone when a run later fails or is cancelled. Compensation is modeled explicitly, per tool — the runtime does not invent inverse operations.
- **Not a safety or alignment layer.** Veya executes decisions reliably. It does not evaluate whether a decision is correct, safe, or authorized.
- **Not a scheduler for arbitrary background jobs.** It is built for agent-shaped workloads: dynamic, decision-driven, side-effecting.

---

## 4. Architecture

### 4.1 High-level diagram

```
                       ┌──────────────────┐
                       │     AI AGENT     │
                       │    Python SDK    │
                       └────────┬─────────┘
                                │
                                ↓
                  ┌───────────────────────────┐
                  │       GO RUNTIME          │
                  │                           │
                  │   Execution Engine        │
                  │   Run Advancement         │
                  │   Effect Tracking         │
                  │   Lease Management        │
                  └──────┬──────────────┬─────┘
                         │              │
            atomic commit│              │outbox relay
                         ↓              ↓
              ┌────────────────┐  ┌──────────────────┐
              │   PostgreSQL   │  │  NATS JetStream  │
              │                │  │                  │
              │ runs           │  │ task delivery    │
              │ events         │  │ (work queue)     │
              │ tasks          │  └────────┬─────────┘
              │ effects        │           │
              │ leases         │           ↓
              │ workers        │      ┌─────────┐
              │ task_outbox    │      │ WORKERS │
              └────────────────┘      └────┬────┘
                         ↑                 │
                         │                 ↓
                         │        ┌──────────────────┐
                         └────────┤  EXTERNAL TOOLS  │
                        results   │  APIs / DB / LLM │
                                  └──────────────────┘
```

Two flows run through this system and should never be conflated:

- **Work flow** — moves executable tasks to workers. JetStream.
- **Truth flow** — records what happened and what is currently true. PostgreSQL.

### 4.2 Component responsibilities

| Component | Owns | Does not own |
| --- | --- | --- |
| **Python SDK** | Agent logic, prompt construction, tool definitions | Execution ordering, durability, retries |
| **Execution Engine (Go)** | Run lifecycle, decision → task translation, advancement | Agent reasoning |
| **PostgreSQL** | Authoritative execution state **and** event history | Work distribution |
| **Outbox Relay** | Publishing committed tasks to JetStream, retrying until acked | Deciding what work exists |
| **NATS JetStream** | Durable at-least-once task delivery to workers | Being a system of record |
| **Workers** | Executing tool calls, heartbeating, reporting results | Deciding what to execute next |
| **Effect Ledger** | Lifecycle of every external side effect | Workflow control flow |

### 4.3 Execution lifecycle

```
 1. Agent starts a run                  → RUN_STARTED
 2. Engine calls LLM for next decision  → LLM_CALL_STARTED / LLM_CALL_COMPLETED
 3. Decision becomes a task             → TASK_CREATED   ┐
 4. Task + event + outbox row           →   ONE Postgres transaction
 5. Relay publishes to JetStream        → at-least-once delivery
 6. Worker claims task, acquires lease  → TASK_CLAIMED (fencing token issued)
 7. Side-effecting? Create effect row   → EFFECT_CREATED (status PENDING)
 8. Worker executes tool                → external call carries idempotency key
 9. Worker reports result (with token)  → EFFECT_COMMITTED, TASK_COMPLETED
10. Engine advances the run             → serialized by run version
11. Repeat from 2 until the model stops → RUN_COMPLETED
```

Steps 3–4 are one atomic commit. That is what closes the window in which a task could be recorded but never delivered.

---

## 5. Core Design

### 5.1 Durable execution

Every fact required to resume a run is written to PostgreSQL before it is acted upon. The runtime holds no authoritative state in memory. A restarted engine reconstructs everything it needs from the database.

The unit of durability is the **run**. Within a run, the unit of addressing is the **step**. Within a step, the unit of external consequence is the **effect**.

```
run_id   → R123          the agent execution
step_id  → S7            a logical position in that execution
effect_id→ R123:S7:E1    a specific external side effect
```

These identities are stable across retries, crashes, and worker reassignment. That stability is what makes every other guarantee possible.

### 5.2 Event history

The event history is the append-only journal of a run. It answers: *what happened?*

```
seq  event                        detail
───────────────────────────────────────────────────────
 1   RUN_STARTED                  input = {...}
 2   LLM_CALL_COMPLETED           decision = lookup_order
 3   TASK_CREATED                 T456, step S1
 4   TASK_CLAIMED                 worker-4, token 10
 5   TASK_COMPLETED               result = {...}
 6   LLM_CALL_COMPLETED           decision = create_refund
 7   TASK_CREATED                 T457, step S2
 8   EFFECT_CREATED               R123:S2:E1, PENDING
 9   EFFECT_COMMITTED             external_ref = pay_98374
10   TASK_COMPLETED
11   RUN_COMPLETED
```

Events are immutable and strictly ordered per run. Appends are conditional on the expected next sequence number, so a retried write cannot double-append.

The event history is distinct from current state. History is what *happened*; current state is what is *true now*. Current state is derivable from history, but is maintained directly for efficient querying — you do not want to fold eleven events to answer "is this run still running?"

### 5.3 Tasks

A task is a unit of work the runtime expects a worker to execute.

```
task_id    T456
run_id     R123
step_id    S1
task_type  lookup_order
payload    {"order_id": 987}
status     PENDING
attempt    0
priority   0
available_at  2026-09-11T14:32:00Z
```

Task types fall into three categories, and the category determines the protection applied:

| Category | Example | Effect row? | Rationale |
| --- | --- | --- | --- |
| **Read** | `lookup_order`, `web_search` | No | Free to repeat |
| **Side-effecting** | `create_refund`, `send_email` | Yes | Must not repeat |
| **Model call** | `llm_call` | Yes | Costly and not safely repeatable |

The third row is the one most systems get wrong. An LLM call is not a read — it is billed, non-deterministic, and cannot be replayed by the provider. It receives the same effect-ledger treatment as a refund.

`(run_id, step_id)` is unique. Task creation is therefore idempotent by construction: a redelivered or reprocessed creation cannot produce two rows for the same logical step.

### 5.4 Transactional outbox

The runtime writes durable state to PostgreSQL and delivers work through JetStream. Writing to both and hoping both succeed is exactly the dual-write bug this project exists to prevent — so the runtime does not do it.

```
BEGIN
  INSERT INTO tasks        (...)   -- the work exists
  INSERT INTO events       (...)   -- the history records it
  INSERT INTO task_outbox  (...)   -- the intent to deliver it
COMMIT
        │
        ↓
  Outbox Relay
        │  polls unpublished rows, FOR UPDATE SKIP LOCKED
        ↓
  JetStream publish  ──ack──→  UPDATE task_outbox SET published_at = now()
```

If the relay crashes before publishing, the row is still unpublished and will be picked up again. If it crashes after publishing but before marking, the task is published twice — which is safe, because delivery is at-least-once by design and task execution is idempotent.

The commit is the single point at which a task becomes real. There is no window in which a task is recorded but undeliverable.

### 5.5 NATS JetStream

JetStream is used strictly for durable task delivery. It is a work queue, not a system of record.

Two streams, with deliberately different retention semantics:

| Stream | Retention | Purpose |
| --- | --- | --- |
| `VEYA_TASKS` | WorkQueue — message removed on ack | Deliver work to exactly one consumer, redeliver on ack timeout |
| `VEYA_SIGNALS` | Limits — retained | External signals, cancellations, timer fires |

These are separate because acknowledging a task must never destroy a record of it. The record lives in PostgreSQL; the message is a transient delivery vehicle.

Workers consume via pull consumers with an ack wait. Ack timeout triggers redelivery, which is the transport-level backstop beneath the runtime's own lease expiry.

### 5.6 Effect state & idempotency

These are two distinct mechanisms that are frequently confused.

**Effect state** is the runtime's internal memory of an external action:

| Status | Meaning |
| --- | --- |
| `PENDING` | We intend to execute it. Nothing has been sent. |
| `RUNNING` | A worker is executing it right now. |
| `COMMITTED` | The external world definitely changed. |
| `FAILED` | It definitely did not happen. |
| `UNKNOWN` | We genuinely do not know. |

`UNKNOWN` is the state that justifies the entire design. It is not a synonym for failure; it is an explicit admission of uncertainty that triggers reconciliation rather than blind retry.

**Idempotency** is the external system's protection against duplicates. Every effect carries a stable key derived from logical position, not content:

```
effect_id = run_id : step_id : effect_seq
          = R123:S2:E1
```

The key is deliberately *not* a hash of the request. Two logically identical refunds with different `reason` strings hash differently and would both execute. Logical identity does not have that failure mode.

Because `idempotency_key` is unique in the database, a retried attempt cannot create a second effect row — it must load and transition the existing one. That constraint, not application logic, is what enforces one-effect-per-logical-operation.

**Tool reconciliation contract.** Exactly-once external effects require cooperation from the tool. Every tool adapter declares its class:

| Class | Behavior on `UNKNOWN` |
| --- | --- |
| `IDEMPOTENT_BY_KEY` | Safe to re-send with the same key; provider deduplicates |
| `QUERYABLE` | Look up by key to determine actual outcome, then resolve |
| `UNRECONCILABLE` | Escalate to a human. Never auto-retry. |

A tool that is neither idempotent nor queryable cannot be made exactly-once by any runtime. Veya's contribution is to make that fact explicit and fail loudly rather than silently guessing.

**Key retention is the provider's, not ours.** Effect records persist far longer than most providers honor an idempotency key — Stripe expires keys after 24 hours, and many services are less generous. A `QUERYABLE` effect reconciled after that window returns "not found," which means *the provider forgot*, not *it never happened*. Re-sending on that basis is precisely the duplicate this design exists to prevent.

Every adapter therefore declares the window in which its key is actually meaningful:

```python
@tool(effect=EffectClass.QUERYABLE, key_ttl=timedelta(hours=24))
def send_invoice(customer_id: int, *, idempotency_key: str) -> dict:
    ...
```

Past `key_ttl`, an unresolved effect escalates to a human rather than reconciling automatically. Half of an exactly-once guarantee lives in someone else's system, under someone else's retention policy; the tool contract has to encode that rather than assume it away.

### 5.7 Leases & fencing

**Leases** answer *who owns this task right now?*

```
task_id       T456
worker_id     worker-A
fencing_token 10
expires_at    14:35:00
```

The owning worker heartbeats; the runtime extends the lease. Silence past expiry makes the task eligible for reclaim. Lease duration is per task type — an LLM call and a service deployment do not warrant the same timeout.

**Fencing tokens** answer *may this worker still modify state?* Each reassignment issues a strictly higher token:

```
Worker A  freezes, holds token 10
Lease expires
Worker B  claims task, receives token 11
Worker A  wakes, submits result with token 10  →  REJECTED
Worker B  submits result with token 11         →  ACCEPTED
```

Every mutating API — `complete_task`, `commit_effect`, `append_event`, `extend_lease` — requires a token and rejects stale ones. Checking at a single chokepoint is insufficient.

Note the division of labor, because it is subtle: fencing tokens protect *state writes back into the runtime*. They do not prevent two workers from both believing they hold a lease during clock skew. What prevents a duplicate *external call* is the unique constraint on `idempotency_key` — the conditional insert of the effect row is the real mutual-exclusion primitive. Both mechanisms are required; they guard different boundaries.

### 5.8 Replay & recovery

Recovery reconstructs a run's state from durable history, then continues from the last durable decision.

The critical distinction:

> **Past decisions are replayed. Future decisions are generated normally.**

Replay does not make the LLM deterministic and does not try to. If history records that decision #1 was `lookup_order`, recovery does not re-ask the model what it decided — it reads the answer. Once execution reaches a point with no recorded decision, a fresh LLM call is made and the future proceeds non-deterministically, as it should.

Recovery resolves each in-flight task by inspecting its effect state:

```
lease expired + no effect row        → clean retry, nothing was sent
lease expired + effect PENDING       → nothing sent yet, safe to execute
lease expired + effect RUNNING       → UNKNOWN, reconcile by tool class
lease expired + effect COMMITTED     → already done, advance
```

A worker whose external call times out *after transmission* writes `UNKNOWN` itself before dying. Timeout is not evidence of failure.

### 5.9 Run advancement & concurrency

Only one actor may advance a given run at a time. Without this, two task completions arriving concurrently would both trigger an LLM call and fork the run into divergent paths.

Advancement is serialized per run via the `runs.version` column:

```sql
UPDATE runs
SET status = $1, version = version + 1, updated_at = now()
WHERE run_id = $2 AND version = $3;
```

A losing writer observes zero rows updated, re-reads state, and retries. Serialization is per run, so throughput across runs is unaffected.

**Fan-out and fan-in** are modeled explicitly. A decision may emit N parallel tasks; the run advances only when the join condition is satisfied:

```
LLM decision → [T1, T2, T3]  (step S4, children S4.0, S4.1, S4.2)
                    ↓
        all children COMPLETED?
                    ↓
        advance to next decision
```

Child step IDs are assigned by **invocation order in the decision**, never by completion order. Completion order is non-deterministic; invocation order is not. This is what keeps effect identities stable across replay when tasks run in parallel.

---

## 6. Failure Scenarios

### 6.1 Worker crash

```
Worker A claims T456 (token 10) → executes → 💥 crash before reporting
```

The lease expires. The task returns to `PENDING`. Worker B claims it with token 11. If an effect row exists, recovery follows §5.8; otherwise the task re-executes cleanly. Worker A, if it revives, is rejected on token.

### 6.2 Network timeout

```
Worker sends create_refund → request transmitted → no response within timeout
```

The worker does **not** record `FAILED`. A transmitted request with no response is genuinely ambiguous, so the effect is marked `UNKNOWN` and reconciled by the tool's declared class. Treating timeouts as failures is the single most common source of duplicate side effects in naive systems.

### 6.3 External side effect succeeds but worker dies

The scenario that motivates the project.

```
create_refund(idempotency_key=R123:S2:E1) → Payment API refunds ₹500 ✓ → 💥
```

Runtime state: effect `RUNNING`, no commit recorded. On lease expiry the effect becomes `UNKNOWN`. Worker B reconciles:

```
Payment API ← "what happened to R123:S2:E1?"
Payment API → "already processed, pay_98374"
Effect      → COMMITTED, external_ref = pay_98374
Run         → advances
```

No duplicate refund. No lost refund. No human involved.

### 6.4 Runtime crash

The engine holds no authoritative in-memory state. On restart it re-reads PostgreSQL: in-flight runs, unexpired leases, unpublished outbox rows, unresolved effects. The outbox relay resumes from unpublished rows. Workers mid-task continue heartbeating against a runtime that recognizes their leases on recovery.

### 6.5 Duplicate delivery

JetStream is at-least-once; duplicate delivery is expected, not exceptional.

```
T456 delivered twice → both workers attempt to claim
```

Claiming is a conditional state transition: the first claim moves `PENDING → RUNNING` and issues a token; the second observes the task is no longer `PENDING` and drops the message. For side-effecting tasks, the unique `idempotency_key` provides a second, independent barrier.

### 6.6 Concurrent task completion

```
T1 and T2 (parallel children of S4) complete at the same instant
```

Both completions attempt to advance the run. Both are gated on `runs.version`; one commits, one observes a stale version and retries. On retry it sees the run already advanced and takes no further action. Exactly one LLM call is made.

---

## 7. Agent SDK

The SDK lives in [`sdk/python`](sdk/python). Your process holds the agent body
and the tool bodies; the runtime holds the ledger, the leases and the claim
loop. [docs/worker-protocol.md](docs/worker-protocol.md) explains why the
boundary is drawn there.

```bash
pip install -e sdk/python
```

### 7.1 Python API

**Defining tools.** A tool declares whether it has external consequences and
how an ambiguous outcome may be settled:

```python
from veya import EffectClass, tool


@tool(effect=EffectClass.NONE)
def lookup_order(order_id: int) -> dict:
    return payments.get_order(order_id)


@tool(effect=EffectClass.IDEMPOTENT_BY_KEY, key_ttl_hours=24)
def create_refund(order_id: int, amount: int, *, idempotency_key: str) -> dict:
    return payments.refund(
        order_id=order_id,
        amount=amount,
        headers={"Idempotency-Key": idempotency_key},
    )


@tool(effect=EffectClass.QUERYABLE, key_ttl_hours=24)
def send_invoice(customer_id: int, *, idempotency_key: str) -> dict:
    return billing.send(customer_id, ref=idempotency_key)


@send_invoice.reconcile
def _(idempotency_key: str) -> dict | None:
    """Return the prior result if it exists, else None. Query; never send."""
    return billing.lookup(ref=idempotency_key)
```

`idempotency_key` is injected by the runtime and is stable across every retry,
reassignment and replay of the same logical step. A tool declaring
`IDEMPOTENT_BY_KEY` or `QUERYABLE` whose function cannot receive it is refused
at import time: the class is an assertion about the provider, and it is false
unless the key goes out with the request.

`key_ttl_hours` is how long the provider actually honours a key. Past that
window a "not found" means the provider forgot rather than that nothing
happened, so the runtime escalates instead of believing a reconcile hook that
says `None`.

**Defining an agent.** An ordinary function. Every `await ctx.call` is a
durable step:

```python
from veya import agent


@agent(name="refund_agent", version="v1",
       tools=[lookup_order, create_refund, send_invoice])
async def refund_agent(ctx, order_id: int) -> dict:
    order = await ctx.call(lookup_order, order_id=order_id)

    if order["status"] != "DELIVERED":
        return {"refunded": False, "reason": "order not delivered"}

    refund = await ctx.call(
        create_refund, order_id=order["id"], amount=order["amount"]
    )
    await ctx.call(send_invoice, customer_id=order["customer_id"])

    return {"refunded": True, "reference": refund["reference"]}
```

**Running it.** Two processes:

```bash
veya-runtime --store memory --grpc 127.0.0.1:50551   --agent refund_agent --agent-version v1

python -m veya.serve examples/refund_agent.py
```

Or `make demo-python`, which starts both, runs one refund, and prints the
history and the ledger.

### 7.2 How replay works, and the one rule it imposes

The body is never run to completion in one go. It is run repeatedly — once per
decision — against the history recorded so far, and stopped at the first
`ctx.call` history has no answer for. That call becomes the next step. Calls
history *does* have answers for return them without performing anything.

Which means everything before a `ctx.call` runs again on every decision, and
one rule falls out of it:

> **Issue your durable calls in the same order every time.**

An idempotency key is `run_id:step_id:effect_seq`, and the step number comes
from invocation order. If a replay reaches the calls in a different order, the
same logical action gets a different key and the ledger stops protecting it.

In practice: branch on values that came out of a `ctx.call`, not on the wall
clock or a fresh random number. `ctx.now()` and `ctx.random()` exist so the
rule has a compliant path — the first returns the run's start time, the second
is seeded from the run id, and both give the same answer on every replay.

This is checked rather than hoped for. Each call the body produces is compared
against the `TASK_CREATED` recorded at that position, and a mismatch raises
`NonDeterminismError` naming the step, what history holds, and what the body
produced this time:

```
the agent body diverged at step S2: history records
create_refund({"amount": 500, "order_id": 987}), but this replay produced
create_refund({"amount": 500, "at": 1758300142.7})
```

`@agent` also warns at import about references to `datetime.now`, `random`,
`uuid` and their neighbours. That is a convenience and not the mechanism: it
reads one function's code and cannot see through a call to a helper. The check
that holds is the comparison against history.

**Editing an agent mid-flight.** Bump `version`. A run pins the version it
started under, and a worker serving a different one is refused the chance to
decide for it, with both versions named. The run stays `RUNNING` and resumes
when a matching worker is available — a deploy that rolls v2 in front of
in-flight v1 runs pauses them rather than destroying them.

**Model calls are effects.** An agent that asks a model what to do next reaches
it through `ctx.call` on a tool classified `IDEMPOTENT_BY_KEY`, like any other
consequential call. It is billed and non-deterministic and the provider cannot
replay it, so treating it as a read means every crash mid-completion quietly
pays for it again. Reached through `ctx.call`, the model's answer is in
history, and the branch it drives replays from that answer rather than from a
fresh one.

### 7.3 What is not here yet

`run.sleep()`, `run.wait_for()`, cancellation and compensation are Layer 5:
they are decision kinds the engine does not have, and the SDK gains them when
`core.Decision` does. Until then an agent that needs to wait for a human fails
deliberately with `Fail(...)`, which is honest, rather than pretending an
approval was recorded.

There is no authentication on the worker port. That is stated rather than
half-built, and it is why the gateway binds loopback unless told otherwise.

---

## 8. Data Model

### 8.1 PostgreSQL schema

PostgreSQL is the authoritative store for both execution state and event history. JetStream carries work, not truth.

```sql
CREATE TYPE run_status    AS ENUM ('RUNNING','COMPLETED','FAILED','CANCELLED');
CREATE TYPE task_status   AS ENUM ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED','DEAD_LETTER');
CREATE TYPE effect_status AS ENUM ('PENDING','RUNNING','COMMITTED','FAILED','UNKNOWN');
CREATE TYPE worker_status AS ENUM ('ACTIVE','DRAINING','DEAD');
```

**runs** — one row per agent execution.

```sql
CREATE TABLE runs (
    run_id        UUID PRIMARY KEY,
    agent_name    TEXT        NOT NULL,
    agent_version TEXT        NOT NULL,   -- pinned for replay safety
    status        run_status  NOT NULL,
    version       BIGINT      NOT NULL DEFAULT 0,   -- advancement serialization
    input         JSONB,
    output        JSONB,
    context       JSONB,                  -- working memory for next decision
    last_error    TEXT,
    deadline_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ
);
```

**events** — append-only history, ordered per run.

```sql
CREATE TABLE events (
    run_id     UUID        NOT NULL REFERENCES runs(run_id),
    seq        BIGINT      NOT NULL,
    event_type TEXT        NOT NULL,
    step_id    TEXT,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, seq)
);
```

The composite primary key makes appends conditional on the expected next sequence — a retried append collides rather than duplicating.

**tasks** — units of work.

```sql
CREATE TABLE tasks (
    task_id      UUID        PRIMARY KEY,
    run_id       UUID        NOT NULL REFERENCES runs(run_id),
    step_id      TEXT        NOT NULL,
    parent_step  TEXT,                        -- fan-out parent
    task_type    TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    status       task_status NOT NULL,
    priority     INT         NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt      INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 3,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    UNIQUE (run_id, step_id)                  -- idempotent task creation
);
```

Ownership columns are deliberately absent here; ownership lives in exactly one place (`leases`) to make drift impossible.

**effects** — external side effects.

```sql
CREATE TABLE effects (
    effect_id       UUID          PRIMARY KEY,
    task_id         UUID          NOT NULL REFERENCES tasks(task_id),
    run_id          UUID          NOT NULL REFERENCES runs(run_id),
    effect_type     TEXT          NOT NULL,
    effect_class    TEXT          NOT NULL,   -- IDEMPOTENT_BY_KEY | QUERYABLE | UNRECONCILABLE
    idempotency_key TEXT          NOT NULL UNIQUE,
    status          effect_status NOT NULL,
    request         JSONB,
    response        JSONB,
    external_ref    TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    committed_at    TIMESTAMPTZ
);
```

`effect_id` is the internal identity; `idempotency_key` is the provider-facing string, kept separate because providers impose their own format and length constraints. Foreign keys are `RESTRICT` by design — **never** add `ON DELETE CASCADE` here. Effect records must outlive run compaction, or an archived run becomes a refund executed twice.

**task_outbox** — the delivery intent (see §5.4).

```sql
CREATE TABLE task_outbox (
    outbox_id    BIGSERIAL   PRIMARY KEY,
    task_id      UUID        NOT NULL REFERENCES tasks(task_id),
    subject      TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    published_at TIMESTAMPTZ,
    attempts     INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**leases** and **workers** — ownership and liveness.

```sql
CREATE TABLE workers (
    worker_id      TEXT          PRIMARY KEY,
    worker_type    TEXT          NOT NULL,
    status         worker_status NOT NULL,
    capabilities   TEXT[],
    last_heartbeat TIMESTAMPTZ,
    registered_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    metadata       JSONB
);

CREATE TABLE leases (
    task_id       UUID        PRIMARY KEY REFERENCES tasks(task_id),
    worker_id     TEXT        NOT NULL REFERENCES workers(worker_id),
    fencing_token BIGINT      NOT NULL,
    acquired_at   TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    heartbeat_at  TIMESTAMPTZ NOT NULL
);
```

**Indexes.**

```sql
CREATE INDEX idx_tasks_dispatchable ON tasks (priority DESC, available_at, created_at)
    WHERE status = 'PENDING';
CREATE INDEX idx_tasks_run          ON tasks (run_id);
CREATE INDEX idx_effects_run        ON effects (run_id);
CREATE INDEX idx_effects_unresolved ON effects (updated_at)
    WHERE status IN ('RUNNING','UNKNOWN');
CREATE INDEX idx_leases_expiry      ON leases (expires_at);
CREATE INDEX idx_workers_heartbeat  ON workers (last_heartbeat);
CREATE INDEX idx_outbox_unpublished ON task_outbox (outbox_id)
    WHERE published_at IS NULL;
```

### 8.2 Event model

| Event | Emitted when |
| --- | --- |
| `RUN_STARTED` | A run is created |
| `LLM_CALL_STARTED` | A model call is dispatched |
| `LLM_CALL_COMPLETED` | A decision is recorded |
| `TASK_CREATED` | A decision becomes work |
| `TASK_CLAIMED` | A worker acquires a lease |
| `TASK_COMPLETED` / `TASK_FAILED` | A worker reports an outcome |
| `TASK_RETRY_SCHEDULED` | A failed task is requeued with backoff |
| `TASK_DEAD_LETTERED` | Retries exhausted |
| `EFFECT_CREATED` | A side effect is registered as `PENDING` |
| `EFFECT_COMMITTED` / `EFFECT_FAILED` | Outcome is known |
| `EFFECT_UNKNOWN` | Outcome is genuinely uncertain |
| `EFFECT_RECONCILED` | An `UNKNOWN` effect was resolved, by lookup or by a human |
| `EFFECT_ESCALATED` | An outcome cannot be resolved automatically; a person must decide |
| `LEASE_EXPIRED` | An owner went silent and the task was reclaimed under a higher token |
| `TIMER_SET` / `TIMER_FIRED` | Durable sleep |
| `SIGNAL_RECEIVED` | External event delivered to a waiting run |
| `RUN_CANCELLED` / `RUN_COMPLETED` / `RUN_FAILED` | Terminal |

Events carry `run_id`, `seq`, and where applicable `step_id`, making every event addressable to a logical position in the execution.

---

## 9. Execution Example

A complete refund run, from decision to result.

```
┌─ t0 ────────────────────────────────────────────────────────────┐
│ SDK: runtime.start("refund_agent", {"order_id": 987})           │
│ PG:  INSERT runs (R123, RUNNING, version=0)                     │
│      INSERT events (R123, 1, RUN_STARTED)                       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ t1 ─ decision ─────────────────────────────────────────────────┐
│ Engine → LLM: "user wants a refund for #987"                    │
│ LLM   → "look up the order first"                               │
│ PG:  INSERT events (R123, 2, LLM_CALL_COMPLETED)                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ t2 ─ dispatch (ONE transaction) ───────────────────────────────┐
│ PG:  INSERT tasks   (T456, R123, S1, lookup_order, PENDING)     │
│      INSERT events  (R123, 3, TASK_CREATED)                     │
│      INSERT outbox  (→ veya.tasks.lookup_order)                 │
│ COMMIT                                                          │
│ Relay → JetStream publish → ack → published_at = now()          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ t3 ─ execution ────────────────────────────────────────────────┐
│ worker-4 pulls T456, claims lease (token 10)                    │
│ PG:  tasks.status = RUNNING; INSERT leases; events(4, CLAIMED)  │
│ Tool: lookup_order(987) → {amount: 500, status: DELIVERED}      │
│ PG:  tasks.status = COMPLETED; events(5, TASK_COMPLETED)        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ t4 ─ advance (serialized on runs.version) ─────────────────────┐
│ UPDATE runs SET version = 1 WHERE run_id = R123 AND version = 0 │
│ Engine → LLM with new context                                   │
│ LLM   → "issue the refund"                                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ t5 ─ side effect ──────────────────────────────────────────────┐
│ PG:  INSERT tasks   (T457, S2, create_refund)                   │
│      INSERT effects (R123:S2:E1, PENDING, IDEMPOTENT_BY_KEY)    │
│      INSERT outbox                                              │
│ worker-7 claims (token 11), effect → RUNNING                    │
│ Tool: POST /refund  Idempotency-Key: R123:S2:E1  amount: 500    │
│ Payment API → 200 {reference: "pay_98374"}                      │
│ PG:  effects.status = COMMITTED, external_ref = pay_98374       │
│      events(9, EFFECT_COMMITTED); tasks.status = COMPLETED      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ t6 ─ completion ───────────────────────────────────────────────┐
│ LLM → "send confirmation, then done"                            │
│ T458 send_email executes and commits                            │
│ LLM → terminal                                                  │
│ PG:  runs.status = COMPLETED, output = {...}                    │
│      events(N, RUN_COMPLETED)                                   │
└─────────────────────────────────────────────────────────────────┘
```

Final state:

```
runs     R123        → COMPLETED
tasks    T456,T457,T458 → COMPLETED
effects  R123:S2:E1  → COMMITTED (pay_98374)
         R123:S3:E1  → COMMITTED (email)
events   1..N        → immutable history
```

---

## 10. Recovery Example

The same run, crashed at the worst possible moment.

```
┌─ t5 ─ side effect, interrupted ─────────────────────────────────┐
│ worker-7 claims T457 (token 11), effect → RUNNING               │
│ Tool: POST /refund  Idempotency-Key: R123:S2:E1                 │
│ Payment API: ₹500 REFUNDED ✓                                    │
│ 💥 worker-7 dies before the response is recorded                │
└─────────────────────────────────────────────────────────────────┘
```

Runtime state at the moment of the crash:

```
tasks    T457        → RUNNING
leases   T457        → worker-7, token 11, expires 14:35:00
effects  R123:S2:E1  → RUNNING        ← the critical fact
```

Recovery:

```
┌─ 14:35:00 ─ lease expiry ───────────────────────────────────────┐
│ Reaper (one transaction):                                       │
│   DELETE leases WHERE task_id = T457                            │
│   UPDATE tasks SET status = 'PENDING', attempt = attempt + 1    │
│   UPDATE effects SET status = 'UNKNOWN'   ← not FAILED          │
│   INSERT events (EFFECT_UNKNOWN)                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ reconciliation ────────────────────────────────────────────────┐
│ worker-2 claims T457 (token 12)                                 │
│ Sees effect R123:S2:E1 = UNKNOWN, class = IDEMPOTENT_BY_KEY     │
│ Re-sends with the SAME key: Idempotency-Key: R123:S2:E1         │
│ Payment API → "already processed" + {reference: "pay_98374"}    │
│ PG: effects.status = COMMITTED, external_ref = pay_98374        │
│     INSERT events (EFFECT_RECONCILED)                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─ resume ────────────────────────────────────────────────────────┐
│ Replay history: RUN_STARTED, decision #1, T456 result,          │
│                 decision #2 (create_refund)                     │
│ The model is NOT re-asked what it already decided.              │
│ Run advances to the confirmation email and completes normally.  │
└─────────────────────────────────────────────────────────────────┘
```

If worker-7 revives at 14:36 and submits its result with token 11, the runtime rejects it — the current token is 12.

Outcome: **one refund, no lost work, no human intervention.**

---

## 11. Guarantees & Limitations

| Property | Status | Notes |
| --- | --- | --- |
| Durable run progress | ✅ Guaranteed | State survives runtime and worker crashes |
| At-least-once task delivery | ✅ Guaranteed | Outbox + JetStream; duplicates expected and handled |
| Task creation idempotency | ✅ Guaranteed | `UNIQUE (run_id, step_id)` |
| One effect row per logical operation | ✅ Guaranteed | `UNIQUE (idempotency_key)` |
| Stale worker write rejection | ✅ Guaranteed | Fencing tokens on every mutating call |
| Serialized run advancement | ✅ Guaranteed | Optimistic concurrency on `runs.version` |
| Replay without re-invoking the model | ✅ Guaranteed | Decisions read from history |
| Exactly-once external effect | ⚠️ Conditional | Requires `IDEMPOTENT_BY_KEY` or `QUERYABLE` tools |
| `UNRECONCILABLE` tool outcomes | ❌ Not guaranteed | Escalated to a human; runtime will not guess |
| Automatic compensation / rollback | ❌ Not provided | Compensation must be authored per tool |
| Deterministic agent output | ❌ By design | Only the past is made durable |
| Cross-run transactions | ❌ Not supported | Durability is per run |

**Known constraints:**

- Agent orchestration code must reach durable calls in a deterministic order. Use `run.now()` and `run.sleep()` rather than `datetime.now()` and `time.sleep()`; avoid un-journaled I/O between steps.
- Deploying a new agent version does not migrate in-flight runs. Runs pin `agent_version` and complete under the version they started on.
- Large payloads are stored inline. Payload offloading to blob storage is on the roadmap; very large tool results will inflate the database until then.
- Effect retention is intentionally longer than event retention. Compacting effects too aggressively reintroduces duplicate-side-effect risk.
- Cancellation is cooperative and forward-looking. `run.cancel()` prevents the *next* durable step; it does not interrupt an effect already in flight and does not reverse one already committed. Marking a run `CANCELLED` after an email has been sent records a decision, not an undo. Reversing a committed effect requires an authored compensation.
- Exactly-once is bounded by the provider's key retention, not ours. An effect unresolved past its tool's `key_ttl` escalates rather than reconciling (§5.6).

---

## 12. Benchmarks

> Benchmarks are not yet published. This section documents the methodology and the metrics that will be reported, so the numbers — when they exist — are interpretable rather than decorative.

**Workload.** A synthetic refund-agent run: 3 LLM decisions, 3 tool calls, 1 side effect, against a mock provider with configurable latency.

**Metrics.**

| Metric | Definition |
| --- | --- |
| Task throughput | Tasks completed per second, steady state, N workers |
| Dispatch latency (p50/p95/p99) | Task commit → worker receipt |
| End-to-end run latency | `RUN_STARTED` → `RUN_COMPLETED`, excluding model time |
| Recovery time | Worker kill → task reclaimed by a replacement |
| Reconciliation time | `EFFECT_UNKNOWN` → resolved |
| **Duplicate side effects** | Must be zero. The primary correctness metric. |
| Lost work | Runs stuck in a non-terminal state after chaos. Must be zero. |

**Queue comparison.** NATS JetStream is the decided transport for distributed dispatch. The dispatch layer nonetheless sits behind an interface, for two reasons that outlive the decision: `PostgreSQL SKIP LOCKED` is the backend Layers 1–2 run on, since proving the execution model requires no broker at all, and keeping alternatives runnable is what makes this comparison measurable rather than asserted. `PostgreSQL SKIP LOCKED` and `Redis Streams` are benchmarked on identical workloads so the cost of the broker is a number rather than an assumption. Kafka is deliberately excluded: it is a log, not a work queue, and lacks the per-message ack, visibility timeout, and selective redelivery this design depends on.

Correctness metrics are reported under chaos, not just at steady state. Throughput on a system that duplicates refunds is not a result.

---

## 13. Running Locally

Veya runs entirely on one machine. No cloud account, no hosted control plane.

**Requirements:** Go 1.26+. Docker is optional — the memory store needs
neither it nor a database. Python 3.10+ only if you are writing agents in
Python.

```bash
git clone https://github.com/SanthoshRaaj-KR/Veya.git
cd Veya

make up          # PostgreSQL on :5433, NATS on :4222
make migrate     # apply the schema
make demo        # run the built-in agent end to end
```

Port 5433, not 5432: a machine that already runs PostgreSQL would otherwise
shadow the container, and the resulting error is confusing rather than loud.

No Docker? The memory store runs the identical code path:

```bash
make demo-memory
```

**Serving, and starting runs from elsewhere:**

```bash
veya-runtime --dsn "$VEYA_DSN"              # engine + workers + every loop
veya run start  --input '{"meeting_id":"M-1"}'
veya run show    RUN_ID                      # status, output, tasks, effects
veya run history RUN_ID                      # full event log (-v for payloads)
veya outbox                                  # committed work not yet delivered
```

**Workers in their own processes.** Delivery moves off the in-process channel;
everything above `internal/dispatch` is unchanged.

```bash
make runtime     # terminal 1: engine only (--workers 0)
make worker      # terminal 2: four workers, PostgreSQL SKIP LOCKED dispatch
veya run start   # terminal 3
```

Or over NATS, with workers anywhere that can reach it:

```bash
veya-runtime --dispatch jetstream --nats "$VEYA_NATS" --workers 0
veya-worker  --dispatch jetstream --nats "$VEYA_NATS" --workers 8
```

A worker holds a full engine — completing a task advances the run, and
advancing needs a decider. Two processes advancing the same run is safe for the
same reason two goroutines were: the compare-and-swap on `runs.version` lets one
win and the losers find the work already done.

**An agent written in Python.** The runtime serves the worker protocol and a
Python process holds the agent:

```bash
make sdk-install                             # pip install -e sdk/python[dev]
make demo-python                             # both halves, one refund, no Docker
```

Or the two halves by hand:

```bash
veya-runtime --store memory --grpc 127.0.0.1:50551   --agent refund_agent --agent-version v1    # terminal 1
python -m veya.serve examples/refund_agent.py  # terminal 2
veya run start --input '{"order_id": 987}'     # terminal 3
```

The runtime may be started before the worker. A run with nobody to decide for
it stays `RUNNING` and is picked up by the recovery scan as soon as a worker
registers — a deployment gap is not a run's fault. The gateway binds loopback
because the protocol has no authentication; see §7.3.

**When the runtime refuses to guess:**

```bash
veya effects                                 # outcomes still unknown
veya effects show KEY                        # one effect in full
veya effects resolve KEY --committed --ref PROVIDER_REF
veya effects resolve KEY --not-executed
```

An `UNRECONCILABLE` tool whose outcome is ambiguous parks here permanently —
no runtime can settle it, so a person checks the provider and records what
they found. `resolve` takes no default and has no `--probably`: the operator
is asserting a fact they established, not a guess.

The CLI writes to PostgreSQL directly, which is not a shortcut: PostgreSQL is
authoritative, so asking it is the same as asking the runtime, and the answer
does not depend on a runtime being up. A run created this way is `RUNNING` with
no tasks — exactly what the recovery scan looks for — so the runtime adopts it
on its next pass. Every CLI-started run therefore exercises the crash-recovery
path.

**Tests:**

```bash
make test              # unit; no Docker, milliseconds
make test-integration  # the same suites against live PostgreSQL and NATS
make test-race         # needs a C toolchain
```

**Not yet available.** The Python SDK, `examples/refund_agent.py`, and the
`veya workers` command arrive with Layer 4; see the roadmap in §17 and the
"deliberately not built yet" table in [docs/code-map.md](docs/code-map.md).

---

## 14. Testing Failure Scenarios

Failures are injected deliberately, not waited for.

```bash
make chaos SCENARIO=worker_crash_mid_effect
make chaos SCENARIO=network_timeout_after_send
make chaos SCENARIO=runtime_crash_before_outbox_publish
make chaos SCENARIO=duplicate_delivery
make chaos SCENARIO=concurrent_completion
make chaos SCENARIO=clock_skew
make chaos SCENARIO=database_partition
```

Every scenario asserts the same invariants:

```
1. No effect is COMMITTED more than once
2. No run remains non-terminal after recovery completes
3. No task executes while another worker holds a valid lease
4. Every UNKNOWN effect is eventually resolved or escalated
5. Event sequences are gap-free and strictly ordered per run
```

**Deterministic simulation.** Beyond scripted scenarios, the runtime is exercised under a simulation harness with a seeded RNG, virtual clock, and simulated network — thousands of randomized crash schedules per run, with the invariants above asserted after each. A failing seed reproduces exactly, which makes concurrency bugs debuggable instead of folkloric. This harness is built alongside the runtime rather than retrofitted; it is effectively impossible to add later.

---

## 15. Project Structure

Entries marked ✅ exist today; the rest arrive with the layer that needs them.

```
veya/
├── cmd/
│   ├── veya-runtime/    ✅ # engine, relay, recovery scan, reaper, reconciler
│   ├── veya-worker/     ✅ # standalone worker process: execute and report
│   └── veya/            ✅ # operator CLI: migrate, run, effects, outbox
├── internal/
│   ├── core/            ✅ # domain types + port interfaces; imports nothing
│   │   └── storetest/   ✅ # contract suite every Store adapter must pass
│   ├── engine/          ✅ # run lifecycle, advancement, recovery scan
│   ├── effects/         ✅ # the ledger in the execution path + reconciler
│   ├── lease/           ✅ # the reaper: reclaiming abandoned work
│   ├── outbox/          ✅ # the relay: committed delivery intent → dispatcher
│   ├── worker/          ✅ # claim → execute → report
│   ├── decider/         ✅ # what happens next (static; a Python body via sdk/)
│   ├── agents/
│   │   └── meeting/     ✅ # the built-in demo agent, shared by both binaries
│   ├── tool/            ✅ # task type → handler registry
│   ├── wiring/          ✅ # composition root; the only place naming adapters
│   ├── clock/           ✅ # system + virtual time
│   ├── idgen/           ✅ # random + deterministic identity
│   ├── store/
│   │   ├── memory/      ✅ # in-process adapter
│   │   ├── postgres/    ✅ # the authoritative adapter
│   │   └── migrations/  ✅ # embedded SQL + runner
│   ├── dispatch/
│   │   ├── inproc/      ✅ # channel; one process
│   │   ├── postgres/    ✅ # SKIP LOCKED; many processes, no broker
│   │   ├── jetstream/   ✅ # NATS; workers anywhere
│   │   └── redis/          # (benchmark comparison, L7)
│   ├── sdk/
│   │   ├── workerpb/    ✅ # generated protocol stubs (make proto)
│   │   ├── wire/        ✅ # protocol ↔ core conversion, and its defaults
│   │   └── gateway/     ✅ # serves the protocol; a remote Decider + Registry
│   └── telemetry/          # metrics, tracing, structured logs (L6)
├── proto/
│   └── veya/worker/v1/  ✅ # worker.proto — the public wire contract
├── sdk/
│   └── python/          ✅ # @agent, @tool, ctx, replay, the session loop
├── examples/
│   └── refund_agent.py  ✅ # the README's agent, runnable
├── scripts/
│   └── demo-python.sh   ✅ # make demo-python: both halves, one command
├── test/
│   ├── chaos/              # scripted failure scenarios (L7)
│   ├── simulation/         # deterministic simulation harness (L7)
│   └── integration/        # (L7; adapter integration tests currently live
│                           #   beside their package, behind a build tag)
├── docs/
│   ├── architecture-primer.md ✅ # plain-English walkthrough of the ideas
│   ├── code-map.md            ✅ # file-by-file guide: what exists, what does not
│   ├── status.md              ✅ # what is done, what is open, what is next
│   ├── worker-protocol.md     ✅ # why the boundary is drawn where it is
│   ├── architecture.md
│   ├── data-model.md
│   └── tool-contract.md
├── docker-compose.yml   ✅ # PostgreSQL (:5433) + NATS (:4222)
├── Makefile             ✅ # build, test, proto, migrate, up/down, demos
├── .github/workflows/   ✅ # vet, race detector, integration, SDK, the example
└── go.mod               ✅
```

Tests sit next to the code they cover. Unit tests need no Docker and run in
milliseconds against the memory adapter; anything needing a live database or
broker is behind `//go:build integration` and runs via `make test-integration`.

**New to the codebase?** [docs/code-map.md](docs/code-map.md) walks through
every directory, says which layer it belongs to, and lists what is deliberately
not built yet and why. [docs/status.md](docs/status.md) is the running status
file: what is done and verified, what is knowingly left open, and what the next
phase needs before it starts.

---

## 16. Design Decisions & Tradeoffs

**Safety mechanisms must be exact; liveness mechanisms may be approximate.**
This is the principle the rest of the design is organized around. Safety properties — no duplicate external effect, no stale write accepted, no run advanced twice, no committed work lost — are enforced by mechanisms that cannot be partially applied: fencing tokens, unique constraints, version CAS, database transactions. Liveness properties — a dead worker's task is eventually reclaimed, a pending task eventually runs, a committed outbox row is eventually published — are served by mechanisms that are allowed to be wrong occasionally: lease expiry, the reaper, retries, the relay. This is what makes an imperfect lease acceptable. A lease that expires while its owner is healthy costs duplicated work and latency, which is a liveness defect and survivable. A fencing check that is merely usually applied is a safety defect and is not. When evaluating any new mechanism, the first question is which of the two it is, because that determines how exact it has to be.

**PostgreSQL is authoritative for both state and history; JetStream carries only work.**
The alternative — JetStream as the event log with PostgreSQL as a projection — is a legitimate event-sourcing design, but it makes every state read depend on projection lag and requires the projector itself to be crash-recoverable and exactly-once. Keeping truth in one transactional store means a task, its event, and its delivery intent commit atomically. The cost is that PostgreSQL becomes the throughput ceiling; the benefit is that there is exactly one place to look when answering "what is true?"

**A transactional outbox instead of dual writes.**
Writing to PostgreSQL and publishing to JetStream as two independent operations leaves a window where a task exists but will never be delivered — a silently stuck run. Since the entire project exists to eliminate exactly this class of bug for user tool calls, allowing it in the runtime's own plumbing was not defensible. The cost is relay latency and occasional duplicate publishes; both are cheap, because delivery is at-least-once regardless.

**A broker's redelivery is not a recovery mechanism.**
The JetStream adapter acknowledges a message as soon as it is decoded, before the task is claimed. Holding the acknowledgement across the tool call looks safer and buys nothing: the broker cannot tell whether the task was claimed — only PostgreSQL can — so its redelivery would be a guess, and a correct guess is indistinguishable from the recovery scan finding the same `PENDING` task a moment later. The alternatives were worse in kind, not degree: thread acknowledgement through a port that exists to carry identity and nothing else, or run an ack deadline alongside the lease, which is two liveness mechanisms asserting the same thing and free to disagree. The cost is that a crash between ack and claim waits for the next scan.

**The outbox is a boundary-crossing device, not a universal good.**
Under the PostgreSQL dispatcher the transport and the store are the same database, so a committed task is already visible to every poller and `Publish` is a no-op. The outbox exists because a broker cannot participate in a PostgreSQL commit; remove that boundary and it has no work to do. Keeping the row anyway costs one write per task and keeps every backend on one code path, which is the cheaper of the two mistakes available.

**A tool must be given the key it is supposed to send.**
`IDEMPOTENT_BY_KEY` asserts that a provider deduplicates by idempotency key. Layer 3's audit found that handlers were never passed the key, so the runtime computed it, stored it, enforced uniqueness on it, and reasoned about it, while the one participant that had to transmit it never saw it. The class was a declaration a tool had no mechanism to honour. Handlers now receive a `core.ToolCall`; the lesson is that a classification means nothing unless the code path it describes is actually reachable.

**Logical identity for idempotency, not request hashing.**
Two refunds differing only in a free-text `reason` field hash differently and would both execute. `run_id:step_id:effect_seq` is stable under retry, replay, and payload edits. This requires deterministic step numbering, which in turn constrains agent code to deterministic ordering of durable calls — an accepted and documented tradeoff.

**LLM calls are treated as effects, not reads.**
A model call is billed, non-deterministic, and unreplayable by the provider. Classifying it alongside `web_search` means every crash mid-completion silently re-bills. It gets an effect row and a stable identity, like a refund does.

**Ownership lives in one table.**
An earlier design carried `worker_id` and `fencing_token` on both `tasks` and `leases`. Two columns asserting the same fact drift, and a drifted fencing token defeats the mechanism it implements. Ownership is now read from `leases` alone, at the cost of a join.

**Effect retention outlives event retention.**
Compaction snapshots completed runs and trims history, but effect records persist far longer. An effect record archived while a redelivery is still possible is a refund executed twice. Foreign keys on `effects` are `RESTRICT`, never `CASCADE`, so an accidental run deletion fails loudly instead of quietly erasing the duplicate-prevention ledger.

**Fencing tokens and unique keys guard different boundaries.**
Fencing tokens reject stale writes *into* the runtime. They do not prevent two workers from both believing they hold a lease under clock skew. The unique constraint on `idempotency_key` is what prevents a duplicate *external* call. Both are load-bearing, and neither substitutes for the other.

**At-least-once, never at-most-once.**
Where the two conflict, the runtime prefers duplicate delivery over lost work, and pushes duplicate suppression down to idempotency keys. Lost work is silent; duplicate delivery is detectable and defensible.

**Kafka is not on the shortlist.**
Not for performance reasons. Kafka is a distributed log without per-message acknowledgement, visibility timeouts, or selective redelivery — the exact semantics a task queue requires. Adopting it would mean rebuilding those primitives on top of offsets.

---

## 17. Roadmap

**Layer 1 — Foundation** ✅ *complete*
- [x] Run and task lifecycle in PostgreSQL
- [x] Single worker, single tool, end to end
- [x] Event history with conditional appends

> Runnable now: `make up && make migrate && make demo`, or `make demo-memory`
> for the same run with no Docker. Both store adapters pass one contract suite
> (`internal/core/storetest`), so the engine cannot tell them apart.
>
> Layer 1 deliberately has no effect ledger, no leases, and no broker. Task
> delivery is an in-process channel, and duplicate suppression rests entirely
> on the conditional `PENDING → RUNNING` claim — which is enough to make
> at-least-once delivery safe, and is proven by a test that delivers one task
> 21 times across 4 workers and executes it once.

**Layer 2 — Effect safety** *(the core value proposition)* ✅ *complete*
- [x] Effect ledger with `UNKNOWN` state
- [x] Idempotency keys and the tool contract
- [x] Leases, heartbeats, fencing tokens
- [x] Reconciliation by tool class

> Every tool call with an external consequence now goes through the ledger.
> Proven by test: 51 deliveries of one task across 4 workers call the provider
> once; a worker killed after the provider acted recovers by *asking* rather
> than re-sending; an `UNRECONCILABLE` outcome stops and escalates instead of
> guessing. Unresolved effects are visible and settleable with `veya effects`.
>
> Still absent by design: no backoff or per-tool retry policy (Layer 5), and
> `effect_seq` is always 1 because a task performs one tool call — the numbering
> exists so sub-effects do not invalidate every key when the SDK lands.

**Layer 3 — Distribution** ✅ *complete*
- [x] Transactional outbox and relay
- [x] JetStream dispatch, multi-worker
- [x] Lease reaper and recovery paths *(landed early, with Layer 2)*

> The dual write is gone: a task, its event, and the intent to deliver it
> commit together, and a relay publishes what committed. Three transports —
> an in-process channel, PostgreSQL `SKIP LOCKED`, NATS JetStream — are
> interchangeable, and adding the second and third changed nothing in `core/`
> or `engine/`. Workers run as their own processes (`veya-worker`); a runtime
> started with `--workers 0` creates work that a separate process executes and
> then advances, serialized by the same run-version CAS that serialized
> goroutines.
>
> This layer was also the audit, and it found one real hole: `ToolHandler`
> never received the idempotency key, so a tool declaring `IDEMPOTENT_BY_KEY`
> had nothing to send its provider. Handlers now take a `core.ToolCall`.
>
> Still absent by design: outbox rows are never trimmed (Layer 6), and every
> delivery goes to one subject because nothing routes by capability yet
> (Layer 5).

**Layer 4 — Agent execution** ✅ *complete*
- [x] Python SDK, `@agent` / `@tool`
- [x] Dynamic LLM-driven workflows
- [x] Replay of recorded decisions
- [x] Serialized run advancement *(landed with Layer 1's run-version CAS)*
- [x] Agent version pinning *(roadmap lists this under Layer 5; it belongs here)*

> An agent is now a Python function. A worker process dials the runtime over
> gRPC, registers its agent and its tools, and answers three questions:
> what happens next, run this tool, did this effect happen. Both answers plug
> into interfaces that existed before the protocol did — `core.Decider` and
> `core.ToolRegistry` — so nothing in `core/`, `engine/`, `effects/` or
> `worker/` changed to accommodate a second language.
>
> The ledger stays in Go, deliberately. A protocol where Python drove the
> reserve→commit→act ordering would give the one load-bearing rule in this
> design a second implementation, in the language with no compiler to check
> it. See [docs/worker-protocol.md](docs/worker-protocol.md) §2.1 — including
> the cost, which is that a slow Python tool occupies a Go worker slot.
>
> A model call needs no new machinery either: it *is* an effect, reached
> through `ctx.call` like anything else, and the branch it drives replays
> because its result is in history. No `DECISION_RECORDED` event exists,
> because it would be a weaker second copy of a fact history already holds.
>
> Replay is checked, not hoped for. Each call the body produces is compared
> against the `TASK_CREATED` at that position, and a mismatch raises
> `NonDeterminismError` naming the step, what history recorded, and what the
> body produced this time. `@agent` also warns at import about `datetime.now`,
> `random` and friends — a convenience, not the mechanism, and the SDK's tests
> say so.
>
> Still absent by design: `effect_seq` is still always 1 (Layer 5, with
> fan-out), `ctx.now()` returns the run's start time rather than a per-step
> durable clock (Layer 5, with timers), and there is no auth on the worker
> port, which is why it binds loopback.

**Layer 5 — Execution model completeness**
- [ ] Fan-out / fan-in with deterministic child step IDs
- [ ] Durable timers and external signals
- [ ] Cancellation and compensation hooks
- [ ] Retry policies, backoff, dead-letter handling

**Layer 6 — Operability**
- [ ] Compaction, snapshots, tiered retention
- [ ] Payload offloading for large results
- [ ] Local dashboard: run timeline, stuck-run detection, effect audit
- [ ] Metrics and distributed tracing

**Layer 7 — Validation**
- [ ] Deterministic simulation harness
- [ ] Chaos scenario suite
- [ ] Dispatch-layer benchmarks across backends

---

## 18. License

MIT. See [LICENSE](LICENSE).

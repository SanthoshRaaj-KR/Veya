# The Worker Protocol

**Status:** settled, Layer 4 · supersedes the message list sketched in
`.planning/IMPLEMENTATION_PLAN.md` Phase 6

This document answers the five questions `docs/status.md` §4.2 left open, and
explains the one place where the answer departs from the written plan. It is
the document to read before touching `proto/veya/worker/v1/worker.proto`.

- Ideas and reasoning → [architecture-primer.md](architecture-primer.md)
- Where code lives → [code-map.md](code-map.md)
- What is done → [status.md](status.md)

---

## 1. What the protocol is for

Layer 4's goal is that the agent stops being a fixed list of steps and becomes
a program someone writes. The program is written in Python. Everything Veya
already guarantees — exactly-once effects, leases, fencing, replay — is written
in Go and proven by 151 tests.

So the protocol's job is to let a Python process supply *behaviour* without
supplying *correctness*. Two things cross the boundary:

| Crossing | Python answers | Go seam it satisfies |
|---|---|---|
| **What happens next in this run?** | by replaying the agent body against history | `core.Decider` |
| **Run this tool / did this tool's effect happen?** | by calling the user's function | `core.ToolRegistry` |

Both are interfaces that already exist and are already exercised by the Go demo
agent. Nothing in `internal/engine`, `internal/effects`, `internal/worker` or
`internal/core` changes to accommodate a second language, which is the claim
§4.1 made and the thing this design is arranged to keep true.

---

## 2. Question 1 — the worker protocol

**Settled: gRPC, one bidirectional stream, Python dials the runtime.**

```
Python worker ──dials──▶ Go gateway (in veya-runtime)

  ClientMessage: Register | DecideResult | ExecuteResult | ReconcileResult
  ServerMessage: Registered | DecideRequest | ExecuteRequest | ReconcileRequest
```

The client opens one `Session` stream and registers: agent name, agent version,
and one descriptor per tool carrying its name, effect class, and key TTL. From
then on the *server* is the one making requests and the client is the one
answering them, which is backwards from how gRPC usually reads and is the
reason the stream is bidirectional rather than two unary services.

Three things fall out of the client dialling rather than listening:

- A Python process needs no inbound port, no DNS name, and no certificate. It
  works from a laptop behind NAT, which is where agents are actually written.
- Disconnection is unambiguous. The stream *is* the registration; when it
  drops, the tools it registered are gone, and the runtime knows immediately
  rather than discovering it on the next call to a dead address.
- There is one connection per worker process, not one per call, so liveness is
  the stream's business and not something layered on top of it.

Every request carries a `call_id`, and every result echoes it. Requests are
concurrent: the runtime does not wait for one tool call before sending the
next, so one Python process serves as many in-flight calls as its executor
allows.

### 2.1 Where this departs from the plan, and why

`.planning/IMPLEMENTATION_PLAN.md` Phase 6 lists the messages as `Claim`,
`TaskAssignment`, `Heartbeat`, `Report`, `EffectReserve`, `EffectResolve` — a
protocol in which the Python process claims tasks and drives the effect ledger
itself.

That is not what this implements, and the reason is the comment at the top of
`internal/effects/executor.go`:

> Write down what is about to happen, commit that, and only then act. Reverse
> the order — call first, record after — and a crash in between leaves no trace
> at all, so recovery sees a clean slate and runs the action a second time.
> There is no mechanism anywhere else in the system that could catch that.

A protocol where Python calls `EffectReserve` and then `EffectResolve` puts
that ordering in Python's hands. The rule would then have two implementations,
in two languages, and the second one would be in the language with no compiler
to check it and no 151-test suite behind it. A Python SDK that got the ordering
subtly wrong would produce duplicate refunds with the Go ledger looking
entirely healthy — the exact failure this project exists to prevent,
reintroduced by the layer that was supposed to make it usable.

So the claim loop, the lease, the fencing token and the reserve→commit→act
ordering all stay in Go, on the near side of the boundary. Python is handed a
`ToolCall` that already has a committed `PENDING` ledger row behind it and an
idempotency key in hand, and is asked only to perform the call.

**The cost, stated honestly.** A Python tool that blocks for a long time
occupies a Go worker slot for that whole time, because the Go worker is what
holds the lease. Scaling the Python side alone does not scale throughput; you
scale `veya-worker` processes alongside it. That is a real constraint and it is
the price of having one implementation of the ordering rule rather than two. If
it ever becomes the binding constraint, the fix is more `veya-worker`
processes, not moving the ledger into Python.

---

## 3. Question 2 — how a decision is recorded

**Settled: a model call is a tool call. There is no separate decision record.**

The question was whether an LLM-backed decider needs a `DECISION_RECORDED`
event or an effect row per model call, on the grounds that a model call is
billed and non-deterministic and is therefore an effect rather than a read.

The premise is right and the conclusion is that it needs no new machinery. A
model call *is* an effect, and this system already has exactly one way to
perform an effect durably: `ctx.call` on a tool declared with an effect class.
The demo agent has classified `summarize` as `IDEMPOTENT_BY_KEY` since Layer 2
for precisely this reason.

So an agent that asks a model what to do next writes:

```python
plan = await ctx.call(choose_next_step, context=notes)
if plan["action"] == "refund":
    ...
```

and gets, with no new event type:

- a `TASK_CREATED` / `TASK_COMPLETED` pair in history recording the call and
  its result,
- a ledger row under `run:step:E1`, so a crash mid-completion reconciles
  instead of paying for a second generation,
- a branch that replays deterministically, because on replay the condition is
  evaluated against the recorded result rather than a fresh one.

The decision is recorded because its *input* is recorded. A `DECISION_RECORDED`
event would be a second, weaker copy of a fact history already holds, and a
second copy of a fact is a second thing that can disagree.

**What this rules out.** An agent body must not call a model directly — only
through `ctx.call`. A bare `openai.chat(...)` in an agent body is a billed,
non-deterministic call outside the ledger, and it will be re-made on every
replay. §4 is about catching that.

---

## 4. Question 3 — determinism rules the SDK imposes

An idempotency key is `run_id:step_id:effect_seq`. Step IDs come from
invocation order. So user code must issue its durable calls in the same order
every time it replays, or the same logical action gets two different keys and
the ledger stops protecting it.

The SDK enforces this in three places, weakest first:

1. **A static warning at declaration.** `@agent` inspects the function's code
   object for references to `random`, `time.time`, `datetime.now`, `uuid.uuid4`
   and friends, and warns naming the offender and its replay-safe alternative.
   It is a warning, not an error: the reference may be in a branch that never
   runs, and refusing to start over a false positive would be worse than the
   disease.

2. **Replay-safe alternatives that make the rule followable.** `ctx.now()`
   returns the run's recorded time rather than the wall clock, and
   `ctx.random()` is seeded from the run ID. A rule with no compliant path is
   not a rule, it is a complaint.

3. **`NonDeterminismError` at replay, which is the one that actually holds.**
   Replay compares each call the body produces against the `TASK_CREATED`
   already in history at that position. A mismatch in tool name or payload
   means the body did not reproduce itself, and the run stops with an error
   naming the step, what history recorded, and what the body produced this
   time. This is a check, not a heuristic: it cannot miss a divergence that
   changed a durable call, and a divergence that changed nothing durable did
   not matter.

The honest limit: only divergence that reaches a durable call is detectable. An
agent that computes a different number and never calls a tool with it is
non-deterministic and invisible, and no runtime can see inside a function body.

---

## 5. Question 4 — when `effect_seq` stops being 1

**Settled: it stays 1 in Layer 4. It becomes real in Layer 5, with fan-out.**

One task performs one tool call, so there is one external action per step. The
SDK does not let a tool declare sub-effects, and adding that in the same layer
that introduces a second language would mean debugging two new things at once.

The key format already carries the field, which is the whole point of having
threaded it through since Layer 2: keys issued now stay valid when the number
starts varying. `internal/effects.effectSeq` is the one constant to change.

---

## 6. Question 5 — where the agent definition lives

**Settled: exactly one language owns any given agent.**

The worry was two processes disagreeing about a tool's class — one treating a
send as `QUERYABLE`, another as `UNRECONCILABLE` — which is the kind of
disagreement nobody notices until an outage.

Go binaries solve this by sharing `internal/agents/meeting`. A Python agent
solves it by being the only definition that exists: the Python process
registers its tools with their classes and TTLs, and the runtime builds its
registry from what was registered. Go holds no second copy to drift from.

Two consequences worth stating:

- **A runtime serving a Python agent cannot execute its tools until a worker
  registers.** A task for an unregistered tool is not dispatched to nobody; it
  fails with `ErrToolNotFound` the same as any other unknown tool, and the
  operator sees which tool and which agent.
- **Version pinning becomes load-bearing.** `runs.agent_version` is pinned at
  start and never changes. A worker registering `v2` while a `v1` run is still
  in flight must not decide for that run: its body may have different steps,
  and replay would diverge silently. The gateway refuses, loudly, naming both
  versions. This is the "fails loudly, not silently" exit criterion.

---

## 7. What is deliberately not in this protocol

| Not here | Why | Where it goes |
|---|---|---|
| Auth, TLS, multi-tenancy | the project's stated posture is local and plug-and-play; a half-built auth story is worse than an absent one | stated in the README, not half-built |
| `Sleep`, `WaitForSignal`, `Cancel`, `Compensate` | they are decision kinds the engine does not have yet; the protocol gains them when `core.Decision` does | Layer 5 |
| Python claiming tasks | §2.1 | not planned |
| A TypeScript SDK | one language is enough to prove the boundary is language-neutral, and a second Go client proves it more cheaply | deferred |

The last row has a test rather than a promise: a minimal Go client implements
the same `.proto` and passes the same suite as the Python one. If the protocol
grows Python-shaped assumptions — payload encoding, error formats — that test
is what fails.

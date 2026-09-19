# Code Map

A file-by-file guide to what exists, what each piece is for, and what is still
a plan. Read [architecture-primer.md](architecture-primer.md) first if you want
the ideas; this document is for finding your way around the code, and
[status.md](status.md) is for what is verified and what comes next.

Status labels used throughout:

| | meaning |
|---|---|
| **✅ done** | built, tested, running |
| **🔜 planned** | named in the roadmap, no code yet |
| **➖ absent** | deliberately not built; the reason is given |

---

## 1. The shortest possible tour

If you read four files, read these:

| File | Why |
|---|---|
| `internal/core/ports.go` | Every interface in the system. The whole contract, in one file. |
| `internal/effects/executor.go` | The ordering rule — write intent down, commit, *then* act. This is the project's central claim. |
| `internal/engine/engine.go` | How a decision becomes durable work. |
| `internal/wiring/wiring.go` | What the thing is actually made of. The only file that names concrete adapters. |
| `proto/veya/worker/v1/worker.proto` | The boundary an agent in another language crosses, and why its defaults are pessimistic. |

---

## 2. Layers, and where each one lives

| Layer | What it delivers | Status | Mostly lives in |
|---|---|---|---|
| **1 — Foundation** | runs, tasks, event history | ✅ done | `engine/`, `store/` |
| **2 — Effect safety** | the ledger, `UNKNOWN`, leases, fencing | ✅ done | `effects/`, `lease/` |
| **3 — Distribution** | outbox + relay, three transports, worker processes | ✅ done | `outbox/`, `dispatch/`, `cmd/veya-worker` |
| **4 — Agent execution** | worker protocol, Python SDK, replay | ✅ done | `proto/`, `internal/sdk/`, `sdk/python/` |
| **5 — Execution model** | fan-out, timers, retry policy, cancellation | 🔜 planned | — |
| **6 — Operability** | compaction, dashboard, metrics | 🔜 planned | `telemetry/` |
| **7 — Validation** | simulation harness, chaos suite, benchmarks | 🔜 planned | `test/` |

---

## 3. `internal/core` — the domain, and every port ✅

Imports nothing from `internal/`. If a type from an adapter appears in a
signature here, the boundary has leaked.

| File | Holds | Read it when |
|---|---|---|
| `doc.go` | the dependency rule | first |
| `ids.go` | `RunID`, `TaskID`, `StepID`, `Step(n)` | you want to know where step numbering comes from |
| `run.go` | `Run` + its state machine, `runs.version` | asking who may advance a run |
| `task.go` | `Task` + its state machine | asking who may claim a task |
| `event.go` | `Event`, event types, the versioned `{"v":1,"data":…}` envelope | adding an event type |
| `effect.go` | the ledger: statuses incl. `UNKNOWN`, `NewIdempotencyKey`, `ClassifyFailure`, `RecoveryFor` | **the most important file in the project** |
| `tool.go` | `ToolDescriptor`, `ToolCall`, effect classes, `Reconciler`, `KeyTTL` | writing a tool |
| `lease.go` | `Lease`, `FencingToken` | asking who may *report* on a task |
| `outbox.go` | `Delivery`, the wire envelope | asking why publishing is not a function call |
| `decision.go` | `Decider` — what happens next, independent of how | replacing the static decider |
| `errors.go` | every sentinel error | ever — no error string is matched anywhere |
| `ports.go` | `Store`, `Tx`, `Dispatcher`, `Clock`, `IDGen` | always |
| `append.go` | the conditional-append helper | adding an event |

**Two functions carry most of the design's weight.** `ClassifyFailure` decides
what an error *means* — it defaults to `UNKNOWN`, because an error returned
after a request may have been transmitted is not evidence that it was not.
`RecoveryFor` is the recovery table as a pure function, with no I/O, which is
why the behaviour that only runs during a crash is directly testable.

### `internal/core/storetest` — the contract suite ✅

Not tests *of* core; tests every `Store` adapter must pass. Run identically
against the memory and PostgreSQL adapters, which is the entire mechanism
keeping them interchangeable. It has caught two real divergences.

`storetest.go` (lifecycle, claims, history) · `effects.go` (the ledger) ·
`leases.go` (ownership) · `outbox.go` (delivery intent)

---

## 4. `internal/store` — where truth lives ✅

| Path | Status | What |
|---|---|---|
| `memory/` | ✅ | in-process, copy-on-write transactions. Not a toy: it passes the same suite, and it is the substrate for fast tests and (Layer 7) simulation |
| `postgres/` | ✅ | the authoritative adapter. `database/sql` and `lib/pq` appear here and nowhere else |
| `migrations/` | ✅ | embedded `.sql` + a ~150-line runner. Forward-only |

`postgres/errors.go` is worth a look on its own: it is the only file that knows
what a `*pq.Error` is, and it maps constraint names onto sentinels. Getting one
of those mappings wrong is how "this action already happened" turns into "some
conflict occurred", which is a different instruction to the caller.

Schema: `migrations/0001_init.sql` creates everything; `0002_outbox_relay.sql`
adds what writing the relay turned out to need.

---

## 5. `internal/engine` — run lifecycle ✅

| File | What |
|---|---|
| `engine.go` | `StartRun`, `Advance`, `dispatch`. The engine is bound to **one agent** and refuses to advance anyone else's runs |
| `tasks.go` | `ClaimTask`, `Heartbeat`, `CompleteTask`, `FailTask`, `assertToken` |
| `runtime.go` | the recovery scan |

**`assertToken` is called on every mutating path**, not at one chokepoint. A
single well-placed check is the version of this that looks correct and is not:
ownership can lapse between any two statements.

**What the recovery scan is for changed in Layer 3.** It used to cover
"committed but never published"; the outbox closed that. What is left is a
delivery that was published and then lost in transit, where the outbox row
honestly says published and the message is gone anyway.

---

## 6. `internal/effects` — the ledger in the execution path ✅

| File | What |
|---|---|
| `executor.go` | the ordering rule: reserve → commit → mark RUNNING → commit → call → record |
| `reconcile.go` | resolving an ambiguous outcome for a live task |
| `reconciler.go` | the background sweep for effects no live task will ever settle |

The constraint that separates the two: **the executor may act; the background
reconciler may only ask.** An `IDEMPOTENT_BY_KEY` effect is normally settled by
re-sending, which is safe because the provider deduplicates — but in the
background sweep the task is usually dead-lettered and the run failed, so a
re-send would be a real external action on behalf of work nobody is waiting
for. `TestReconcilerNeverPerformsTheAction` pins this.

---

## 7. `internal/lease` — reclaiming abandoned work ✅

One file, `reaper.go`, and one idea: **the reaper is not above the concurrency
model.** To take a task from its previous owner it acquires the lease itself,
which issues a strictly higher fencing token and fences the old owner out. A
reaper that reassigned tasks by writing directly would be the one actor able to
create two live owners.

Since Layer 3 it does not publish either. A reclaimed task's delivery intent is
written inside the transaction that makes it `PENDING` again.

---

## 8. `internal/outbox` — the relay ✅ *(new in Layer 3)*

`relay.go`. Reads committed delivery intent and hands it to the dispatcher.

Publishing could never have been part of the transaction — a broker cannot
participate in a PostgreSQL commit — so the fix was not to try harder at
publishing but to make the *intent* durable and let a separate loop be late
instead of lossy.

The ordering is the whole contract: **publish, then mark.** Marking first loses
a delivery whenever the process dies in between; marking after only duplicates
one, and the conditional claim absorbs duplicates.

`Relay.Wake` is a plain func, not a port, and the type is the documentation:
safety went into the transaction, and this is the liveness half that is allowed
to be missed.

---

## 9. `internal/dispatch` — three transports ✅

All three are at-least-once and none is trusted. A delivery says *"task T may
need attention"*; the receiver reads what is true from the store.

| Adapter | Status | Use it when | Note |
|---|---|---|---|
| `inproc/` | ✅ | everything in one process; tests | a channel. Cannot lose a message in transit because it never leaves the process |
| `postgres/` | ✅ | workers in other processes, no broker | `SKIP LOCKED` + a visibility window. `Publish` is a **no-op** and that is correct — the task row *is* the queue |
| `jetstream/` | ✅ | workers anywhere, no database polling | one work-queue stream, one durable consumer shared by every worker |
| `redis/` | ➖ | — | a benchmark comparison, Layer 7 at the earliest |

**Why two of them exist.** One adapter proves nothing: `core.Dispatcher` could
simply be a description of NATS. Two that share no mechanism and are
indistinguishable from above is evidence that the port is a port.

**The JetStream adapter acks immediately, before anything is claimed.** This
looks careless and is deliberate: the broker cannot tell whether the task was
claimed — only PostgreSQL knows — so its redelivery would be a guess, and a
correct guess is indistinguishable from the recovery scan finding the same
`PENDING` task a moment later. The cost is that a crash between ack and claim
waits for the next scan instead of an immediate redelivery.

---

## 10. `internal/worker` — claim, execute, report ✅

`worker.go`. Deliberately thin. It decides nothing: not what runs next, not
whether a failure is retryable, not whether a task is finished with.

Layer 4 did **not** replace it, which was the plan and turned out to be the
wrong plan. Moving the claim loop into Python would have moved the
reserve→commit→act ordering with it, into the language with no compiler to
check it — see `docs/worker-protocol.md` §2.1. What Layer 4 replaced instead is
the two things above this loop: the decider and the tool registry. A Python
process supplies behaviour; this loop still supplies correctness.

It abandons the tool the moment a heartbeat is fenced. Continuing would be
working on someone else's task, and its report would be rejected anyway.

---

## 11. `internal/sdk` — the worker protocol ✅ *(new in Layer 4)*

The near side of the boundary between the Go runtime and an agent written
somewhere else. `docs/worker-protocol.md` is the argument; this is the code.

| Path | What |
|---|---|
| `workerpb/` | generated from `proto/veya/worker/v1/worker.proto`. Committed, so a Go-only machine needs no protoc; `make proto-check` fails if they drift |
| `wire/` | protocol ↔ core conversion. One file decides what a message *means* |
| `gateway/` | serves the protocol, and adapts it to `core.Decider` and `core.ToolRegistry` |

**Read `wire/wire.go` for the defaults.** Three of them are load-bearing and
each is the pessimistic choice: an unset `Certainty` reads as UNKNOWN so a
forgotten field cannot authorise a retry of something that already happened; an
unrecognised `ResolutionKind` reads as STILL_UNKNOWN, which leads to
escalation; an unspecified `EffectClass` is refused outright, because both
defaults are wrong in different directions.

**`gateway/` is two adapters and the plumbing between them.** `decider.go` and
`registry.go` implement interfaces that existed before this package did and did
not change to accommodate it — which is checkable: delete `internal/sdk` and
the runtime still builds, still passes its tests, and still runs the Go demo
agent.

What it deliberately does not do: claim tasks, hold leases, issue fencing
tokens, or touch the ledger. Those stay where they already are.

---

## 12. `sdk/python` — writing an agent ✅ *(new in Layer 4)*

| File | What |
|---|---|
| `effects.py` | `EffectClass`, `Resolution`, `ToolCall`, `Effect` — the vocabulary an author reads constantly |
| `errors.py` | the exception hierarchy. `NotExecuted` is the one that makes a claim rather than naming a situation |
| `tools.py` | `@tool`, and the declaration-time refusals that keep an effect class honest |
| `history.py` | folding the event log into something indexed by step |
| `agent.py` | `@agent`, `ctx.call`, the replay driver, `NonDeterminismError` |
| `determinism.py` | the import-time warning. A convenience, not the mechanism |
| `session.py` | the client half of the protocol: one connection, one stream |
| `serve.py` | `python -m veya.serve agent.py` |

Its tests need no PostgreSQL, no NATS and no Go binary — they run against a
fake gateway that is a real gRPC server. That is the boundary doing its job; if
the suite ever needs a database, the SDK has grown a dependency on runtime
internals the protocol was supposed to hide.

---

## 13. Supporting packages

| Path | Status | What |
|---|---|---|
| `internal/wiring/` | ✅ | the composition root. The only place that names adapters. Read this to find out what Veya is made of |
| `internal/tool/` | ✅ | task type → descriptor. Panics at startup on a contradictory descriptor, e.g. `QUERYABLE` with no reconciler |
| `internal/decider/` | ✅ | `Static` — a fixed list of steps. A model-driven decider is a Python body reached through `internal/sdk/gateway`, satisfying the same interface |
| `internal/agents/` | ✅ | which agent a process serves. Both binaries ask here, so they cannot answer differently |
| `internal/agents/meeting/` | ✅ | the built-in demo agent. Shared by both binaries so they cannot disagree about a tool's class |
| `internal/clock/` | ✅ | `System` and `Virtual`. No `time.Now()` above the composition root |
| `internal/idgen/` | ✅ | `Random` and `Sequential`. No `crypto/rand` above the composition root either |
| `internal/telemetry/` | 🔜 | metrics, tracing (Layer 6) |

---

## 14. `cmd/` — the binaries

| Binary | Status | What it does |
|---|---|---|
| `veya-runtime` | ✅ | engine, relay, recovery scan, reaper, reconciler — and workers unless `--workers 0`. Serves the worker protocol with `--grpc` |
| `veya-worker` | ✅ | executes tasks and nothing else. Start more for capacity |
| `veya` | ✅ | operator CLI: `migrate`, `run start/show/history`, `effects list/show/resolve`, `outbox` |

**A worker holds a full engine, which surprises people.** Completing a task
advances the run, and advancing asks the decider what comes next. Two processes
advancing the same run is safe for exactly the reason two goroutines were — the
compare-and-swap on the run's version lets one win and the losers find the work
already done.

Try it:

```bash
make up && make migrate
make runtime          # terminal 1: engine only, --workers 0
make worker           # terminal 2: four workers
veya run start        # terminal 3
```

---

## 15. What is deliberately not built yet

Not omissions — decisions, with reasons.

| Missing | Why | Arrives |
|---|---|---|
| Retry backoff, per-tool retry policy | retries are immediate and the policy is a fixed attempt count. A scheduler with nothing to schedule against would be guesswork | Layer 5 |
| `effect_seq > 1` | a task performs one tool call, so it is always 1. The numbering exists so sub-effects do not invalidate every key already issued. Layer 4 decided against adding it alongside a second language — one new thing at a time | Layer 5 |
| Per-task-type lease TTL | an LLM call and a deployment do not deserve the same timeout, but one number is honest until tools differ enough to matter | Layer 5 |
| A per-step durable clock | `ctx.now()` returns the run's start time. A time that changes between replays goes into a payload and diverges the step after the one that read it; a real one needs durable timers | Layer 5 |
| `ctx.sleep` / `ctx.wait_for` / cancel / compensate | they are decision kinds `core.Decision` does not have. The protocol gains them when the engine does | Layer 5 |
| Auth on the worker port | the project's posture is local and plug-and-play, and a half-built auth story is worse than an absent one. It binds loopback | — |
| A TypeScript SDK | one language proves the boundary is language-neutral. A second Go client proves it more cheaply, and does, in `gateway/client_test.go` | — |
| Fan-out / fan-in | needs deterministic child step IDs, which needs the SDK's ordering rules | Layer 5 |
| Outbox retention | published rows accumulate. Trimming belongs with compaction, not scattered | Layer 6 |
| Per-tool subjects / capability routing | a routing key nothing routes on drifts out of sync with reality unnoticed | Layer 5 |
| `make test-race` on the dev machine | needs a C toolchain, which this machine lacks. CI runs it on every push, with `-count=2` | ✅ CI |

---

## 16. Where the interesting tests are

The tests are where the reasoning is checked, so they are worth reading as
documentation.

| Test | Proves |
|---|---|
| `engine/effects_test.go` → `TestSideEffectHappensOnceUnderDuplicateDelivery` | 51 deliveries across 4 workers, provider called once |
| `engine/outbox_test.go` → `TestEngineDoesNotPublishDirectly` | the dual write cannot come back unnoticed |
| `outbox/relay_test.go` → `TestRelayRecoversWorkCommittedByADeadProcess` | committed work survives the process that committed it |
| `effects/reconciler_test.go` → `TestReconcilerNeverPerformsTheAction` | the background sweep resolves knowledge, never acts |
| `effects/toolcall_test.go` → `TestTheKeyIsTheSameOnEveryAttempt` | the idempotency key a tool sends is stable across retries |
| `engine/reaper_test.go` → `TestReaperFencesThePreviousOwner` | a frozen worker's report is refused after reclaim |
| `dispatch/postgres/postgres_test.go` → `TestOneTaskGoesToOneWorker` | `SKIP LOCKED` plus the visibility window divides work |
| `core/effect_test.go` → `TestIdempotencyKeyIsStableAcrossPayloadChanges` | the regression test for the hash-the-request anti-pattern |
| `sdk/gateway/endtoend_test.go` → `TestAnAmbiguousRemoteEffectIsReconciledNotRepeated` | the exactly-once claim, performed by a worker across a socket |
| `sdk/gateway/endtoend_test.go` → `TestARunPinnedToAnotherVersionIsRefusedLoudly` | a v2 worker cannot decide for a v1 run, and the run survives the refusal |
| `sdk/wire/wire_test.go` → `TestUnsetCertaintyIsUnknown` | a forgotten field cannot authorise a retry of something that happened |
| `sdk/gateway/client_test.go` → `TestAGoWorkerSpeaksTheSameProtocol` | the guard on Python-shaped assumptions creeping into the runtime |
| `sdk/python/tests/test_replay.py` → `test_a_wall_clock_in_a_payload_diverges` | the determinism failure, caught by history rather than by the warning |
| `sdk/python/tests/test_determinism.py` → `test_the_guard_does_not_see_through_a_function_call` | the static check's limit, written down so nobody trusts it as a guarantee |

```bash
make test              # unit; no Docker, milliseconds
make test-integration  # the same suites against live PostgreSQL and NATS
make sdk-test          # the Python SDK: ruff, mypy, pytest
make demo-python       # the worked example, both halves, end to end
```

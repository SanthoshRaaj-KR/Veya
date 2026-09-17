# Code Map

A file-by-file guide to what exists, what each piece is for, and what is still
a plan. Read [architecture-primer.md](architecture-primer.md) first if you want
the ideas; this document is for finding your way around the code.

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

---

## 2. Layers, and where each one lives

| Layer | What it delivers | Status | Mostly lives in |
|---|---|---|---|
| **1 — Foundation** | runs, tasks, event history | ✅ done | `engine/`, `store/` |
| **2 — Effect safety** | the ledger, `UNKNOWN`, leases, fencing | ✅ done | `effects/`, `lease/` |
| **3 — Distribution** | outbox + relay, three transports, worker processes | ✅ done | `outbox/`, `dispatch/`, `cmd/veya-worker` |
| **4 — Agent execution** | Python SDK, LLM deciders, replay | 🔜 planned | `sdk/python/` |
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

`worker.go`. Deliberately thin, because Layer 4 replaces it with a Python
process speaking the same protocol. It decides nothing: not what runs next, not
whether a failure is retryable, not whether a task is finished with.

It abandons the tool the moment a heartbeat is fenced. Continuing would be
working on someone else's task, and its report would be rejected anyway.

---

## 11. Supporting packages

| Path | Status | What |
|---|---|---|
| `internal/wiring/` | ✅ | the composition root. The only place that names adapters. Read this to find out what Veya is made of |
| `internal/tool/` | ✅ | task type → descriptor. Panics at startup on a contradictory descriptor, e.g. `QUERYABLE` with no reconciler |
| `internal/decider/` | ✅ | `Static` — a fixed list of steps. LLM deciders are Layer 4 |
| `internal/agents/meeting/` | ✅ | the built-in demo agent. Shared by both binaries so they cannot disagree about a tool's class |
| `internal/clock/` | ✅ | `System` and `Virtual`. No `time.Now()` above the composition root |
| `internal/idgen/` | ✅ | `Random` and `Sequential`. No `crypto/rand` above the composition root either |
| `internal/telemetry/` | 🔜 | metrics, tracing (Layer 6) |

---

## 12. `cmd/` — the binaries

| Binary | Status | What it does |
|---|---|---|
| `veya-runtime` | ✅ | engine, relay, recovery scan, reaper, reconciler — and workers unless `--workers 0` |
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

## 13. What is deliberately not built yet

Not omissions — decisions, with reasons.

| Missing | Why | Arrives |
|---|---|---|
| Retry backoff, per-tool retry policy | retries are immediate and the policy is a fixed attempt count. A scheduler with nothing to schedule against would be guesswork | Layer 5 |
| `effect_seq > 1` | a task performs one tool call, so it is always 1. The numbering exists so sub-effects do not invalidate every key already issued | Layer 4 |
| Per-task-type lease TTL | an LLM call and a deployment do not deserve the same timeout, but one number is honest until tools differ enough to matter | Layer 5 |
| Replay of recorded decisions | needs an SDK with a decision log to replay | Layer 4 |
| Fan-out / fan-in | needs deterministic child step IDs, which needs the SDK's ordering rules | Layer 5 |
| Outbox retention | published rows accumulate. Trimming belongs with compaction, not scattered | Layer 6 |
| Per-tool subjects / capability routing | a routing key nothing routes on drifts out of sync with reality unnoticed | Layer 5 |
| `make test-race` actually run | needs a C toolchain, which this dev machine lacks. The target exists for CI | — |

---

## 14. Where the interesting tests are

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

```bash
make test              # unit; no Docker, milliseconds
make test-integration  # the same suites against live PostgreSQL and NATS
```

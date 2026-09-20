# Project Status and Phase Handoff

**As of:** 20 September 2026 · `main` at `a228ef7` · working tree clean
**Complete:** Layers 1, 2, 3, 4, 5 · **Next:** Layer 6 — Policy and operability

This is the running status file. It records what is done and verified, what is
knowingly left open, and what the next layer needs before it starts. Update it
at the end of each phase.

- Ideas and reasoning → [architecture-primer.md](architecture-primer.md)
- Where code lives → [code-map.md](code-map.md)
- How a run waits and branches → [execution-model.md](execution-model.md)
- Why the worker boundary is where it is → [worker-protocol.md](worker-protocol.md)
- Specification → [../README.md](../README.md)

---

## 1. Done, and verified

### Layer 1 — Foundation ✅

- [x] Run and task lifecycle in PostgreSQL
- [x] Single worker, single tool, end to end
- [x] Event history with conditional appends
- [x] One contract suite passed by both store adapters

### Layer 2 — Effect safety ✅

- [x] Effect ledger with `UNKNOWN` as a first-class outcome
- [x] Idempotency keys from logical position, and the tool contract
- [x] Leases, heartbeats, fencing tokens
- [x] Reconciliation by tool class; escalation when no mechanism can settle it
- [x] Lease reaper *(roadmap lists this under Layer 3; it landed early, here)*
- [x] `veya effects list/show/resolve` for human resolution

### Layer 3 — Distribution ✅

- [x] Transactional outbox: task + event + delivery intent in one transaction
- [x] Relay publishes what committed; the engine never publishes
- [x] Three interchangeable transports: in-process channel, PostgreSQL
      `SKIP LOCKED`, NATS JetStream
- [x] `veya-worker` as a standalone process; `veya-runtime --workers 0`
- [x] `veya outbox` for committed work that has not reached anyone
- [x] Audit fix: tools now receive their idempotency key (`core.ToolCall`)

### Layer 4 — Agent execution ✅

- [x] Worker protocol: `veya.worker.v1`, one bidirectional gRPC stream,
      versioned and treated as public from its first commit
- [x] Python SDK: `@agent`, `@tool`, `ctx.call`, `ctx.now`, `ctx.random`,
      `@t.reconcile`, `python -m veya.serve`
- [x] Dynamic, model-driven workflows — a model call is an effect reached
      through `ctx.call`, needing no new event type
- [x] Replay of recorded decisions, checked against history, with
      `NonDeterminismError` naming the divergent step
- [x] Agent version pinning, refused loudly and recoverably
      *(roadmap listed this under Layer 5; the protocol needed it here)*
- [x] `examples/refund_agent.py` and `make demo-python`
- [x] CI: race detector, integration suite, SDK suite, and the worked example

**What it actually changed in the runtime:** nothing in `core/`, `engine/`,
`effects/`, `worker/`, `store/` or `dispatch/`. The two new adapters implement
`core.Decider` and `core.ToolRegistry`, which existed since Layer 1. The one
engine change was a bug the layer *found* rather than one it needed — see §3.

### Layer 5 — Suspension and fan-out ✅

- [x] Durable timers: `ctx.sleep`, `runs.available_at`, and a recovery scan
      that is the timer wheel — nothing holds a pending wake-up in memory
- [x] External signals: stored on arrival, consumed by a `wait_for` that is a
      read, so the early-signal race has nowhere to happen
- [x] Fan-out / fan-in: one decision, N children named by invocation order,
      joined under `ALL`, `ANY` or `QUORUM(k)`
- [x] `ctx.sleep`, `ctx.wait_for`, `ctx.call_parallel` in the Python SDK
- [x] `veya signal`, and `veya run show` saying what a run is waiting for
- [x] `examples/onboarding_agent.py` and `make demo-onboarding`

**What it changed in the runtime.** More than Layer 4 did, and in one place
that matters: `engine.Advance` gained a `resume` step that runs before the
decider, and `FailTask` stopped failing a run when a *child* dead-letters. Both
are described in §3. `effects/`, `outbox/`, `dispatch/`, `lease/` and
`worker/` are untouched — the ledger did not need to learn about parallelism,
because children get distinct keys by getting distinct step ids.

### Verification state

| Check | Result |
|---|---|
| `make test` (no Docker) | **254 PASS**, all green |
| `make test-integration` (PostgreSQL 16 + NATS 2) | **315 PASS**, all green |
| `make sdk-test` (ruff, mypy strict, pytest) | **112 PASS**, all green |
| `make demo-python` (Go runtime + Python worker) | exit 0 |
| `make demo-onboarding` (fan-out + timer + human approval) | exit 0, COMPLETED |
| `go vet ./...` and `go vet -tags=integration ./...` | clean |
| `make proto-check` | clean |
| `make demo` / `demo-memory` / `demo-jetstream` | complete |
| `make test-race` | **not run locally** — no C toolchain; CI runs it with `-count=2` |

Size: 102 Go files (~24,300 lines), 25 Python files (~5,400 lines).

### To re-verify from scratch

No Docker needed for most of it:

```bash
make test
make sdk-install && make sdk-test
make demo-python        # the Go runtime and a Python agent, end to end
make demo-memory
```

With Docker:

```bash
make up && make migrate
make test-integration
make demo && make demo-jetstream
make demo-onboarding    # the Layer 5 example: fan-out, a timer, an approval
```

**Gotcha that will waste your time:** `PYTHONPATH` must be a *native* path.
Under Git Bash on Windows the shell speaks POSIX paths and the Python
interpreter does not, so `PYTHONPATH=sdk/python` is silently ignored and the
import fails with `No module named 'veya'` — which reads as a missing install
rather than as a path nothing understood. `scripts/demo-python.sh` runs it
through `cygpath` for this reason. The simpler fix is `make sdk-install`.

---

## 2. Commit index

Newest first. Each phase's commits are self-contained and the messages carry
the reasoning, so `git show` is the place to look for *why*.

**Layer 5**

```
a228ef7 feat(examples): an agent that fans out, sleeps, and waits for a human
3934af1 test(sdk/python): replay across a suspension
afedc73 feat(sdk/python): ctx.call_parallel and the join policies
edffaae feat(sdk/python): ctx.sleep and ctx.wait_for
242f11c test(engine): the same fan-out replays identically
4f18f5e fix(engine): a failed child is an outcome, not a failed run
f48b4e8 feat(engine): join all, any and quorum, with a stated partial-failure policy
987abf1 feat(engine): dispatch a parallel decision as N tasks in one transaction
4a6b863 feat(effects): several effects from one decision, through one ledger
29236c4 feat(core): effect_seq stops being a constant
9dd6e56 feat(core): child step IDs by invocation order, never completion order
6b62f93 feat(cmd): send a signal from the CLI
5046b21 test(engine): a signal that arrives before its wait is not lost
00ab2e5 feat(engine): WaitForSignal reads, and parks only if there is nothing to read
7b5c50e feat(store): persist signals on both adapters
d437cfc feat(core): model a signal, and the port that stores one
0639d63 feat(cmd): show what a run is waiting for
fd3e605 test(integration): a real timer on a real clock
0f180d3 test(engine): a sleeping run survives a restart
b2d9663 feat(runtime): wake parked runs without holding them in memory
bc78c7c feat(engine): Sleep parks a run until a wall-clock instant
f520536 feat(engine): a decision can park a run instead of advancing it
4a04ee0 feat(store): the recovery scan leaves a waiting run alone
fac97a7 feat(store): persist available_at on both adapters
2f4ee46 feat(core): a run can be waiting without being in flight
29d6258 feat(sdk): map the new decisions to core types
3d0d887 build(proto): regenerate both languages
48f8ae8 feat(proto): the whole Layer 5 decision surface, added once
d382fa6 feat(core): name the new decisions and the join policy
dab2959 chore: re-verify what Layer 4 could not run locally
27553ff docs: settle the five questions Layer 5 is blocked on
```

**Layer 4**

```
b50be0e docs(readme): the SDK section now describes something that exists
629bbca ci: run the worked example end to end
ac66852 feat: run the Python agent end to end with one command
3638862 test(gateway): a whole run, driven by an agent on the far side of a socket
b24ffd7 fix(engine): a deployment gap is not a run's fault
7134a85 test(gateway): a Go worker on the same proto, over a real socket
53b65a5 feat(examples): the refund agent from the README, runnable
a38a21f test(sdk/python): a suite that needs no infrastructure
2cddc04 feat(sdk/python): a command that serves an agent file
7c713fc feat(sdk/python): run the session loop
d71a6cb feat(sdk/python): warn about what will not reproduce on replay
c3816bf feat(sdk/python): declare agents and replay them against history
38b1065 feat(sdk/python): read a run's history, indexed by logical position
5c3f095 feat(sdk/python): declare tools with an effect class
1289922 feat(sdk/python): the vocabulary an agent author actually reads
f2da7b2 feat(sdk/python): package skeleton, typing and lint config
1498f17 feat(cmd): serve an agent that is not defined in Go
5073142 feat(wiring): let an agent be defined outside this process
7d4ed5a test(gateway): drive the protocol with a worker made of channels
8fddaf6 feat(gateway): serve the protocol over gRPC
1338028 feat(gateway): run a remote worker's tools through the existing ledger
d428582 feat(gateway): let a remote agent body decide what happens next
85df1aa feat(gateway): accept worker sessions and route their replies
325a89b feat(sdk): map protocol messages to core types
891d023 build(proto): generate Go and Python stubs, and commit them
e313e7b feat(proto): define the worker protocol
54f6eb6 docs: settle the five questions Layer 4 was blocked on
573be73 ci: run the race detector on a machine that has a C toolchain
35af687 test(store): give the contract suite a database of its own
```

**Layer 3**

```
86d3cee docs: map the codebase, and record what Layer 3 settled
86b6cd6 feat(cmd): run workers as their own process
360b93e fix(core): give tools the idempotency key they are supposed to send
c382e9c feat(dispatch): deliver from PostgreSQL with SKIP LOCKED
eeb7536 feat(dispatch): deliver over NATS JetStream
8636ba6 feat(outbox): publish from a relay, and never from the engine
c15d842 feat(store): persist delivery intent on both adapters
07db195 feat(core): model delivery intent as something that commits
```

**Layer 2**

```
fcaf06c feat(cmd): surface unresolved effects and let a human settle them
3493cf9 feat(effects): settle abandoned effects with a background reconciler
9f463d5 feat(lease): reclaim abandoned work with the reaper
34d1ec4 feat(effects): run tool calls through the ledger, with ownership
2fde812 feat(store): add leases and fencing tokens to both adapters
470e16d feat(store): persist the effect ledger on both adapters
93e0296 feat(core): model effects, tool classes, leases, and the recovery table
```

**Layer 1**

```
4cb0562 fix: correct the module path and refresh the Running Locally guide
db82da7 feat(cmd): wire the runtime and CLI, and bind an engine to one agent
3876386 feat(engine): drive runs end to end with a single worker
88c649d feat(store): implement the PostgreSQL adapter against the same contract
04866a0 feat(store): add PostgreSQL schema and an embedded migration runner
ed10fa6 feat(store): add in-memory adapter, determinism seams, and contract suite
9a0f35d feat(core): define domain types, state machines, and port interfaces
491d6bb chore: scaffold Go module, build tooling, and local infrastructure
```

---

## 3. What Layer 5 settled, and what it found

### Settled, with the argument written down

All five open questions from the previous handoff are answered in
[execution-model.md](execution-model.md), written before the code rather than
after it, because two of them are one-way doors. The three that shaped
everything else:

1. **A suspension is a column, not a state.** A run waiting on a timer is
   RUNNING with nothing in flight — which is exactly what `RunsAwaitingAdvance`
   returns, so without a change the recovery scan would advance every sleeping
   run on every pass. A `WAITING` run state looks tidier and costs more: run
   state lives in `core`, in both adapters, in the CLI's output and in every
   transition assertion written since Layer 1, and it buys nothing, because a
   waiting run *is* still running. The column is one predicate in one query,
   and it generalises — a signal wait and a retry backoff are both "not before
   time T".

2. **A signal is stored on arrival, and a wait is a read.** The bug everyone
   writes here is the early signal: a callback arrives before the run reaches
   its `wait_for`, finds no waiter, and is dropped, and the run waits forever
   for something that already happened. It is a race, so it passes every test
   written by someone who has not thought about it and fails in production
   under load. Storing deletes the race rather than narrowing it: there is no
   delivery path at all, so early and late arrival run identical code and the
   case that is hard to reproduce is the one that is always exercised.

3. **The engine decides when a join is finished, never what it means.** `ALL`,
   `ANY` and `QUORUM(k)` each have two exits, and the second — every child
   failed, or too few left for a quorum to be possible — is the one that gets
   forgotten. A join satisfiable only by success hangs forever on a bad day.
   But whether three failures out of ten is a disaster or a Tuesday is a
   question about the agent, so every outcome goes back to the body in
   invocation order and the body decides.

**The cost that is real and is written down:** an agent cannot fan out and
sleep in one turn. It joins, then suspends on the next decision — one more
round trip, and a history that reads in the order things happened. That
constraint is load-bearing: it is why a run waits on time *or* on tasks and
never both, which is why suspension is one nullable column.

### Found, and fixed

| Found by | Bug | Fix |
|---|---|---|
| Writing `TestTenChildrenThreeFailuresOneDeterministicOrder` | A dead-lettered task failed the whole run. Right before fan-out — one step was in flight at a time, so a step that would never complete was a run that could never proceed — and wrong after it, where a child is one outcome among several | A dead-lettered *child* advances the run; a top-level step still fails it. Escalation stays fatal in both shapes, because an escalated effect's outcome is unknown and carrying on is how a run acts twice on something nobody has looked at (`4f18f5e`) |
| Writing the engine's `WaitForSignal` case | Parking on a signal that had *already arrived* parked forever. The release happens on arrival, and arrival had happened, so nothing was left to wake the run — the early-signal bug, one level up | Park, then look immediately in the same advance, via `Advance` rather than an inline check, so a stored signal is consumed by exactly the code that consumes a late one (`00ab2e5`) |
| Writing `examples/onboarding_agent.py` | The fake mail provider took a non-reentrant lock and then called a helper that took it again. It presents as a task stuck in RUNNING with an `EFFECT_CREATED` and nothing after it | The helper is documented as being called with the lock held. Worth keeping because the symptom is indistinguishable from a worker that vanished mid-call, which is exactly what the ledger is meant to look like when an outcome is genuinely unknown (`a228ef7`) |

Plus one in a test rather than in the code: the first draft of the Python
replay harness parked runs at an instant of its own choosing rather than the
one the decision asked for, and the step-by-step walk caught it. That is the
same bug a real engine would have if it rounded or defaulted a wake-up.

### Deviations from the plan, and why

- **`effect_seq` is still 1.** The plan's exit criteria say it should no longer
  be. That bullet predates the plan's own §2.5, which defers sub-effects — and
  sub-effects are the only thing that puts a second external action inside one
  step. Fan-out makes more *steps*, each with its own key. What this layer did
  instead is stop the sequence being a package constant, so adding sub-effects
  later changes a caller rather than a constant, and pin the keys already
  issued with a literal-valued regression test. The argued decision won over
  the inherited bullet.
- **32 commits became 31.** Plan commits 22 and 23 both turned out to be about
  `effect_seq` and the ledger under fan-out, and the ledger needed no change —
  children get distinct keys by getting distinct step ids. They became one
  `feat(core)` commit and one `feat(effects)` commit of tests plus a contract.
  The `fix(engine)` above was not in the plan at all.

---

## 4. Open items carried into Layer 6

Nothing here blocks Layer 6. Ordered by how likely it is to bite.

| # | Item | Why it is open | Cost to fix |
|---|---|---|---|
| 1 | **No cancellation** | `CANCEL` is named in the `.proto` and refused by the runtime. It is about the effect ledger rather than about suspension: mark `CANCELLED`, stop dispatching, let in-flight effects land and be recorded. It interacts with fan-out — cancelling a run with eight children in flight — which is why it waited until fan-out existed | Layer 6 |
| 2 | **No retry backoff or per-tool policy** | retries are immediate with a fixed three-attempt limit. Backoff is "not before time T", which `runs.available_at` now provides, so this is policy on the tool descriptor next to `KeyTTL` rather than new machinery | Layer 6 |
| 3 | **No compensation** | settled as a sequence of ordinary `CallTool` decisions, so it is an SDK convention and a decider habit, not engine work. It needs cancellation first, since the thing that triggers a rollback is usually a cancel | Layer 6 |
| 4 | **`effect_seq` is still always 1** | see §3. Fan-out made child *step* ids real; sub-effects are the separate feature, and shipping both at once gives an unexpected key two candidate causes and no way to bisect | Layer 6 |
| 5 | **Signals arrive only through the CLI** | `veya signal` proves the port, and an HTTP endpoint would call the same `engine.DeliverSignal`. What it adds is a server, a bind address and an auth question this project has deliberately not answered | Layer 6, with the dashboard |
| 6 | **A satisfied `ANY` leaves siblings running** | they are not cancelled, their effects land, and they are recorded. That is correct — a ledger with an orphan in it is worse than a slow child — but it means a run can complete with work still in flight, and its history gains `TASK_COMPLETED` events after `RUN_COMPLETED` | fixed by #1, not before |
| 7 | **A slow Python tool holds a Go worker slot** | the price of one implementation of the ordering rule; see worker-protocol.md §2.1. Mitigated by running more `veya-worker` processes | not planned; revisit if it binds |
| 8 | **One lease TTL for all task types** | an LLM call and a deployment do not deserve the same timeout. More visible now that a fan-out puts ten tools in flight at once | Layer 6 |
| 9 | **No auth on the worker port** | deliberate, and stated rather than half-built. It binds loopback | not planned |
| 10 | **Outbox rows are never trimmed** | published rows accumulate forever. Trimming belongs with compaction | Layer 6 |
| 11 | **`signals` rows are never trimmed either** | same shape as #10, and now the same size problem: a run that takes a hundred callbacks keeps a hundred rows after it finishes | Layer 6, with #10 |

Resolved since the last handoff: the integration suite runs locally again
(`dab2959`), and all five of Layer 5's blocking questions are answered
(`27553ff`).

---

## 5. Layer 6 — what it is, and what it needs

**Goal:** the policies that sit on top of the execution model, and enough
operability to run the thing without reading the database by hand.

Roadmap items:

- [ ] Cancellation, and compensation as a decider convention
- [ ] Retry policy, backoff, dead-letter handling, deadline propagation
- [ ] HTTP signal ingestion
- [ ] Compaction, snapshots, retention — including the outbox and signals
- [ ] Local dashboard: run timeline, stuck-run detection, effect audit
- [ ] Metrics and tracing

### 5.1 What is already in place for it

| Seam | Where | Layer 6 uses it to |
|---|---|---|
| `runs.available_at` | migration `0003` | back a retry off, and bound a deadline — both are "not before time T", and the column and its index already exist |
| `Store.NextWakeUp` | `core/ports.go` | wake on a backoff sooner than the scan interval, with nothing held in memory |
| `core.Wait` / `PendingWait` | `core/wait.go` | describe why a run is parked, which a dashboard needs and the CLI already prints |
| `engine.DeliverSignal` | `engine/signals.go` | an HTTP endpoint calls this and adds nothing else |
| `tool.KeyTTL` | `core/tool.go` | per-tool policy already lives on the descriptor; retry policy joins it there rather than in a switch |
| `DECISION_KIND_CANCEL` / `COMPENSATE` | the `.proto` | already numbered, already refused by name. Layer 6 implements them without touching the wire contract |
| `core.FanOut` | `core/join.go` | know which children are in flight when a run is cancelled |
| `TaskFailedData.Final` | `core/event.go` | distinguish a retryable attempt from a settled failure, which a backoff policy needs and the join already uses |

### 5.2 Decisions Layer 6 has to make first

1. **What cancelling a run with children in flight means.** Mark the run
   `CANCELLED` and let the children land, or stop dispatching and dead-letter
   them? The first keeps the ledger honest and leaves external actions
   happening after the run ended; the second needs a story for an effect that
   was already `RUNNING`. Open item #6 is this question wearing a different
   hat.

2. **Where retry policy lives on the descriptor.** `KeyTTL` is a duration and
   a precedent, but backoff is a curve plus a cap plus a jitter, and a
   `RetryPolicy` struct on `ToolDescriptor` is a bigger thing to make public
   than one field.

3. **Whether a dead-lettered child should be retryable by hand.** The CLI can
   already resolve an effect; it cannot re-arm a task. A fan-out where one
   child failed and nine succeeded is exactly the case where somebody wants to
   fix the provider and push one button.

4. **What compaction may throw away.** History is the authority for replay, so
   compacting it is compacting the thing everything else is derived from. A
   snapshot has to be re-derivable, or it is a second source of truth.

### 5.3 Suggested entry criteria

- [ ] Cancellation semantics for a run with children in flight written down
      (5.2 #1), because it is the one that can invalidate work
- [ ] `make test-integration` green — it is, as of this handoff
- [ ] A decision on whether `RetryPolicy` is public API (5.2 #2), since the
      tool descriptor is what every agent author touches

---

## 6. Environment notes

| | |
|---|---|
| Go | 1.26, `GOFLAGS=-mod=mod` |
| Go dependencies | `lib/pq`, `nats.go`, `grpc`, `protobuf` |
| Python | 3.10+; the SDK depends on `grpcio` and `protobuf` and nothing else |
| Codegen | `make proto-tools` then `make proto`. grpcio-tools bundles protoc, so there is no separate protoc to find. Generated code is committed; `make proto-check` and CI fail on drift |
| PostgreSQL | container on **:5433**, not 5432, to avoid shadowing a host install |
| NATS | container on :4222, monitoring on :8222 (needs `-m 8222`) |
| Worker gateway | **:50551**, loopback only by default |
| DSN | `postgres://veya:veya@localhost:5433/veya?sslmode=disable` |
| Migrations | forward-only, embedded; `0001_init.sql`, `0002_outbox_relay.sql`, `0003_run_suspension.sql`, `0004_signals.sql` |
| CI | `.github/workflows/ci.yml` — vet + gofmt + proto drift, race detector, Python SDK on 3.10 and 3.13, the worked example, and the integration suite |
| Demos needing Docker | `make demo`, `demo-jetstream`, `demo-onboarding`. The onboarding one needs PostgreSQL because a signal must come from outside the runtime process |
| Shell | Windows; git is set to `core.autocrlf=true`, so the repo stores LF and the worktree has CRLF. `gofmt -l` occasionally flags a file for that reason alone |

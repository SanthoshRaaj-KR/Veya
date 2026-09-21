# Project Status and Phase Handoff

**As of:** 21 September 2026 · `main` at `074a55f` · working tree clean
**Complete:** Layers 1, 2, 3, 4, 5 · **In progress:** Layer 6 — cancellation,
retry backoff and HTTP signal ingestion landed; compensation is a documented
convention rather than an example; compaction, the dashboard and metrics are
not started.

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

### Layer 6 — Policy and operability 🚧 (three of six roadmap items)

- [x] **Cancellation.** `DecideCancel` / `DECISION_KIND_CANCEL`, numbered
      since Layer 5 and refused by name until now. Cooperative and
      forward-looking exactly as README §11 always said it would be:
      `finish()` is the whole engine-side implementation, and a child
      dispatched before the `CANCEL` lands its outcome after `RUN_CANCELLED`
      the same way one lands after `RUN_COMPLETED` under an `ANY` join (open
      item #6 — resolved, because the mechanism needed no change to cover it)
- [x] **Compensation, as a decider convention.** Not engine work, per §5.2's
      own prediction: `Cancel`/`DecideCancel`'s doc comments are the worked
      example — issue ordinary `CALL_TOOL` decisions to reverse what already
      committed, then raise `Cancel`. No dedicated example script exists yet
      (`examples/` has none that cancels), which is the honest gap here
- [x] **Retry backoff.** `core.RetryPolicy` (`MaxAttempts`, `InitialBackoff`,
      `MaxBackoff`, `Multiplier`, `Jitter`) is engine-wide, not per tool —
      settling §5.2 decision 2 in the direction its own text hinted at
      ("like `LeaseTTL`"), with the identical follow-on item (#8, still open).
      `task_outbox.available_at` (migration `0005`) is `runs.available_at`'s
      idiom one column over: a retry's `EnqueueDelivery` waits for its
      instant instead of firing immediately, and `PendingDeliveries` takes a
      `now` for the same reason `RunsAwaitingAdvance` does
- [x] **HTTP signal ingestion.** `internal/signalhttp`, `POST
      /v1/runs/{run}/signals/{name}`, over the same `Engine.Signal` /
      `DeliverSignal` `veya signal` already used. Off unless
      `--signal-http` is set; no auth, loopback default, the same posture
      `internal/sdk/gateway` already took and for the same reason (open item
      #9's shape, now shared by a second port rather than answered)
- [ ] **Deadline propagation.** Not started. `runs.available_at` and
      `task_outbox.available_at` are both "not before time T"; a deadline is
      "not after", the opposite comparison, and nothing here builds it
- [ ] **Compaction, snapshots, retention.** Not started, including open
      items #10 and #11 (outbox and signal rows are never trimmed). §5.2
      decision 4 is unresolved: what compaction may throw away, with history
      as the authority for replay
- [ ] **Local dashboard.** Not started
- [ ] **Metrics and tracing.** Not started

**Scope note.** Cancellation and retry/backoff needed real engine and store
changes (a new decision kind, a new column on two adapters, a migration, a
required `Clock` on the outbox relay); HTTP signal ingestion needed a new
package but no runtime change beneath it. Dashboard and metrics are a
different kind of work — a UI and an observability library choice,
respectively — and were not attempted in this pass.

### Verification state

| Check | Result |
|---|---|
| `make test` (no Docker) | **275 PASS**, all green |
| `make test-integration` (PostgreSQL 16 + NATS 2) | **337 PASS**, all green |
| `make sdk-test` (ruff, mypy strict, pytest) | **115 PASS**, all green |
| `make demo-python` (Go runtime + Python worker) | exit 0 |
| `make demo-onboarding` (fan-out + timer + human approval) | exit 0, COMPLETED |
| HTTP signal ingestion, manual | `curl -X POST .../signals/approval` against a live `onboarding_agent` run — COMPLETED, payload carried through to output |
| `go vet ./...` and `go vet -tags=integration ./...` | clean |
| `make proto-check` equivalent (`make proto`, diffed) | clean |
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

**Layer 6 (in progress)**

```
074a55f feat(signalhttp): HTTP signal ingestion, as an optional door onto DeliverSignal
dbfa3e5 feat(engine): FailTask backs off, and the relay learns to wait
654af6a feat(store): persist a delivery's AvailableAt on both adapters
13309ca feat(core): a retry curve, and a delivery that waits for its instant
37b5ba5 feat(sdk/python): raise Cancel to end a run cancelled, not failed
98cfabe feat(engine): cancellation, cooperative and forward-looking
```

One commit immediately before these is not a Layer 6 feature: `b0706e2
fix(core): a fan-out needs at least one call` closed a gap the Layer 5
handoff's own audit found (a zero-call `ALL`/`ANY` fan-out was accepted and
trivially self-satisfied) and corrected three stale "arrives in Layer 5"
comments left over from the roadmap rename.

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
- **32 planned commits, 33 delivered, not the same ones.** Plan commits 22 and 23
  both turned out to be the same question — `effect_seq` and the ledger under
  fan-out — and the ledger needed no change at all, because children get
  distinct keys by getting distinct step ids. They became one `feat(core)`
  commit about the numbering and one `feat(effects)` commit of tests plus a
  store contract. The `fix(engine)` in the table above was not in the plan — a
  test found it — and this correction is the thirty-third.

---

## 4. Open items carried into Layer 6

Nothing here blocks the rest of Layer 6. Ordered by how likely it is to bite.
Struck items are resolved; the entry stays so the table remains a record of
what was true, not just what is true now.

| # | Item | Why it is open | Status |
|---|---|---|---|
| 1 | ~~No cancellation~~ | `CANCEL` was named in the `.proto` and refused by the runtime. | **Resolved** (`98cfabe`): `DecideCancel` ends a run `CANCELLED`, cooperatively and forward-looking. Children in flight land after it, exactly like #6 |
| 2 | ~~No retry backoff or per-tool policy~~ | retries were immediate with a fixed three-attempt limit. | **Resolved, engine-wide** (`13309ca`, `dbfa3e5`): `core.RetryPolicy` on `engine.Config`. *Per-tool* remains open — see #8, which has the identical shape |
| 3 | ~~No compensation~~ | needs cancellation first. | **Resolved as a convention**, not as an example: `Cancel`'s doc comment states the pattern (ordinary `CALL_TOOL` decisions, then raise `Cancel`). No `examples/` script demonstrates it yet |
| 4 | **`effect_seq` is still always 1** | see §3 of the Layer 5 section above. Fan-out made child *step* ids real; sub-effects are the separate feature, and shipping both at once gives an unexpected key two candidate causes and no way to bisect | Untouched this pass |
| 5 | ~~Signals arrive only through the CLI~~ | `veya signal` proved the port; an HTTP endpoint calls the same `engine.DeliverSignal`. | **Resolved** (`074a55f`): `internal/signalhttp`, off by default, same no-auth/loopback posture as the worker gateway rather than an answer to the auth question |
| 6 | **A satisfied `ANY` leaves siblings running** | they are not cancelled, their effects land, and they are recorded. That is correct — a ledger with an orphan in it is worse than a slow child — but it means a run can complete with work still in flight, and its history gains `TASK_COMPLETED` events after `RUN_COMPLETED` | Still true, and now shared by `CANCELLED`: `TestACancelDoesNotStopAChildAlreadyInFlight` covers it. Not itself a defect — see #1's resolution |
| 7 | **A slow Python tool holds a Go worker slot** | the price of one implementation of the ordering rule; see worker-protocol.md §2.1. Mitigated by running more `veya-worker` processes | not planned; revisit if it binds |
| 8 | **One lease TTL, and one retry policy, for all task types** | an LLM call and a deployment do not deserve the same timeout or the same backoff curve. More visible now that a fan-out puts ten tools in flight at once, and now that retry policy exists engine-wide but not per tool | Open. §5.2 decision 2 named this trade-off explicitly rather than resolving it |
| 9 | **No auth on the worker port, or on the signal HTTP port** | deliberate, and stated rather than half-built. Both bind loopback by default | not planned |
| 10 | **Outbox rows are never trimmed** | published rows accumulate forever. Trimming belongs with compaction | Open |
| 11 | **`signals` rows are never trimmed either** | same shape as #10, and now the same size problem: a run that takes a hundred callbacks keeps a hundred rows after it finishes | Open, with #10 |

Resolved since the last handoff: items 1, 2 (engine-wide), 3 (as a
convention) and 5 above, plus the fan-out validity gap `b0706e2` closed and
three stale comments it corrected.

---

## 5. Layer 6 — what it is, and what it needs

**Goal:** the policies that sit on top of the execution model, and enough
operability to run the thing without reading the database by hand.

Roadmap items:

- [x] Cancellation, and compensation as a decider convention (`98cfabe`,
      `37b5ba5`; compensation has no worked example yet)
- [x] Retry policy and backoff, engine-wide (`13309ca`, `dbfa3e5`);
      dead-letter handling predates this pass; deadline propagation is not
      started
- [x] HTTP signal ingestion (`074a55f`)
- [ ] Compaction, snapshots, retention — including the outbox and signals
- [ ] Local dashboard: run timeline, stuck-run detection, effect audit
- [ ] Metrics and tracing

### 5.1 What is already in place for it

Updated from the version written before this pass: rows for what landed now
say so, rather than describing it as a seam something later will use.

| Seam / feature | Where | Status |
|---|---|---|
| Cancellation | `core.DecideCancel`, `engine.go` `case core.DecideCancel` | **Landed.** `finish()` is the whole engine-side implementation |
| Retry backoff | `core.RetryPolicy`, `engine.Config.Retry`, `task_outbox.available_at` (migration `0005`) | **Landed, engine-wide.** Per-tool remains open item #8 |
| HTTP signal ingestion | `internal/signalhttp`, over `engine.Engine.Signal` | **Landed.** Off by default; no auth, same posture as the worker gateway |
| `runs.available_at` | migration `0003` | Used by retry backoff's sibling column, `task_outbox.available_at` — the same "not before time T" idiom, one table over |
| `Store.NextWakeUp` | `core/ports.go` | Still only wakes the *run* recovery scan early. The outbox relay has no equivalent — it relies on its fixed interval to notice a backed-off retry became ready, which is a liveness cost the relay's own doc comment already says it is allowed to have |
| `core.Wait` / `PendingWait` | `core/wait.go` | Unused by anything new this pass; still what a dashboard would read |
| `DECISION_KIND_COMPENSATE` | the `.proto` | Still numbered, still refused by name. No engine work is expected to ever land here — see roadmap item 1 |
| `core.FanOut` | `core/join.go` | Used by `TestACancelDoesNotStopAChildAlreadyInFlight` to assert children in flight are unaffected by a `CANCEL` |
| `TaskFailedData.Final` | `core/event.go` | Unchanged; backoff computes from `Task.Attempt`, not from this field |

### 5.2 Decisions made this pass, and what is still open

1. **What cancelling a run with children in flight means — decided.** Mark
   the run `CANCELLED` and let the children land; `finish()` does not
   special-case which terminal event it is, so the mechanism that already let
   a satisfied `ANY` leave siblings running needed no change to also cover
   cancellation. Open item #6 was this question wearing a different hat, and
   is now closed the same way.

2. **Where retry policy lives — decided, provisionally.** Engine-wide, on
   `engine.Config`, not on `ToolDescriptor`. `RetryPolicy` is public API
   (`core.RetryPolicy`), but attached to the engine rather than the
   descriptor — matching where `LeaseTTL` already lives, and inheriting its
   open item: per-task-type policy is still open item #8. A future per-tool
   `RetryPolicy` would need the executor (which resolves `ToolDescriptor`) to
   pass a policy back to the engine's `FailTask`, which does not happen today.

3. **Whether a dead-lettered child should be retryable by hand — not
   decided.** Still open. The CLI can resolve an effect; it cannot re-arm a
   task.

4. **What compaction may throw away — not decided.** Still open, and still
   the one that matters most before starting on it: history is the authority
   for replay, so compacting it is compacting the thing everything else is
   derived from.

### 5.3 Remaining entry criteria, for whoever picks this up next

- [x] Cancellation semantics for a run with children in flight written down
      and implemented (5.2 #1)
- [x] `make test-integration` green — it is, as of this handoff (337 PASS)
- [x] A decision on whether `RetryPolicy` is public API (5.2 #2) — yes,
      engine-wide
- [ ] A decision on what compaction may throw away (5.2 #4), before starting
      on outbox/signal trimming (items #10, #11) or run history compaction

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
| Signal HTTP server | **:8089** by default when `--signal-http` is set; off otherwise. Loopback default, no auth |
| DSN | `postgres://veya:veya@localhost:5433/veya?sslmode=disable` |
| Migrations | forward-only, embedded; `0001_init.sql`, `0002_outbox_relay.sql`, `0003_run_suspension.sql`, `0004_signals.sql`, `0005_retry_backoff.sql` |
| CI | `.github/workflows/ci.yml` — vet + gofmt + proto drift, race detector, Python SDK on 3.10 and 3.13, the worked example, and the integration suite |
| Demos needing Docker | `make demo`, `demo-jetstream`, `demo-onboarding`. The onboarding one needs PostgreSQL because a signal must come from outside the runtime process |
| Shell | Windows; git is set to `core.autocrlf=true`, so the repo stores LF and the worktree has CRLF. `gofmt -l` occasionally flags a file for that reason alone |

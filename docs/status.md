# Project Status and Phase Handoff

**As of:** 20 September 2026 · `main` at `27553ff` · working tree clean
**Complete:** Layers 1, 2, 3, 4 · **In progress:** Layer 5 — Suspension and fan-out

This is the running status file. It records what is done and verified, what is
knowingly left open, and what Layer 5 needs before it starts. Update it at the
end of each phase.

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

### Verification state

| Check | Result |
|---|---|
| `make test` (no Docker) | **169 PASS**, all green |
| `make sdk-test` (ruff, mypy strict, pytest) | **79 PASS**, all green |
| `make demo-python` (Go runtime + Python worker) | exit 0, 15 events, 2 ledger rows |
| `go vet ./...` and `go vet -tags=integration ./...` | clean |
| `gofmt -l .` | clean |
| `make demo` / `demo-memory` / `demo-jetstream` | complete |
| Two-process run (runtime `--workers 0` + `veya-worker`) | completes |
| `make test-integration` (PostgreSQL 16 + NATS 2) | **217 PASS**, all green — re-run locally on 20 September, first time since Layer 3 |
| `make test-race` | **not run locally** — no C toolchain; CI runs it with `-count=2` |

Size: 78 Go files (~18,700 lines), 16 Python files (~3,600 lines).

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

## 3. What Layer 4 settled, and what it found

### Settled, with the argument written down

All five open questions from the previous handoff are answered in
[worker-protocol.md](worker-protocol.md). The two that shaped everything else:

1. **Python supplies behaviour, not correctness.** The plan called for a
   protocol where the Python process claims tasks and drives the ledger —
   `Claim`, `EffectReserve`, `EffectResolve`. That would give the
   reserve→commit→act ordering a second implementation in a language with no
   compiler to check it, and a Python SDK that got it subtly wrong would
   produce duplicate refunds with the Go ledger looking healthy. So the claim
   loop, the lease, the fencing token and the ledger stayed in Go. **The cost
   is real and is written down:** a slow Python tool occupies a Go worker slot
   for its whole duration, so scaling the Python side alone does not scale
   throughput. You scale `veya-worker` processes alongside it.

2. **A model call needs no new machinery.** It is an effect, this system has
   exactly one way to perform an effect durably, and an agent reaches a model
   through `ctx.call` like anything else. The decision replays because its
   *input* is recorded. There is no `DECISION_RECORDED` event, because it
   would be a weaker second copy of a fact history already holds.

### Found, and fixed

Three bugs surfaced by building the layer rather than by reviewing it:

| Found by | Bug | Fix |
|---|---|---|
| Writing the end-to-end test | `engine.Advance` failed a run on *any* decider error. So a runtime started before its workers destroyed every run in that window, and a rolling deploy destroyed every in-flight run of the previous version — permanently, for a condition that fixes itself by waiting | `core.ErrUnavailable`. A decider returning it leaves the run RUNNING for the recovery loop; anything else still fails the run (`b24ffd7`) |
| Running `make demo-python` | The gateway's shutdown never returned. `GracefulStop` waits for every RPC, and a worker session is a stream open for the worker's whole life — so it waited for every worker to disconnect, which an idle one never does. Took 87 seconds and a SIGKILL to notice | bounded drain: graceful, then `Stop` (`ac66852`) |
| Writing the SDK's test suite | A step recorded as created but not completed returned `None` to the agent body instead of suspending. The body computed with that `None` and raised `NonDeterminismError` about code the author had not written wrongly | suspend instead; task creation is idempotent per (run, step) (`a38a21f`) |

Plus one in the determinism guard itself: it resolved names only through module
globals, so an agent declared inside a factory function passed silently. It now
resolves free variables too. A check that is trusted and has a hole is worse
than no check.

---

## 4. Open items carried into Layer 5

Nothing here blocks Layer 5. Ordered by how likely it is to bite.

| # | Item | Why it is open | Cost to fix |
|---|---|---|---|
| 1 | **`effect_seq` is still always 1** | Layer 4 deliberately did not add sub-effects alongside a second language — one new thing at a time. Fan-out is what makes the numbering real, and the key format has carried the field since Layer 2 so nothing already issued is invalidated | `internal/effects.effectSeq` is the one constant; Layer 5 |
| 2 | **`ctx.now()` is the run's start time** | a clock that changes between replays goes into a payload and diverges the step *after* the one that read it. The honest alternative needs durable timers | Layer 5, with timers |
| 3 | **A slow Python tool holds a Go worker slot** | the price of one implementation of the ordering rule; see §3 and worker-protocol.md §2.1. Mitigated by running more `veya-worker` processes | not planned; revisit if it binds |
| 4 | **No retry backoff or per-tool policy** | retries are immediate with a fixed three-attempt limit. A scheduler with nothing to schedule against would be guesswork | Layer 5 |
| 5 | **One lease TTL for all task types** | an LLM call and a deployment do not deserve the same timeout, but one number is honest until tools differ enough to matter. Layer 4 made this more visible: a Python tool's latency is now somebody else's code | Layer 5 |
| 6 | **One subject for every delivery** | `core.DeliverySubject` is a single constant. A routing key that nothing routes on drifts out of sync unnoticed | Layer 5 |
| 7 | **No auth on the worker port** | deliberate, and stated rather than half-built. It binds loopback; an operator widening it is doing so having read README §7.3 | not planned |
| 8 | **Outbox rows are never trimmed** | published rows accumulate forever. Trimming belongs with compaction and retention | Layer 6 |
| 9 | **`redis` dispatch adapter absent** | only ever intended as a benchmark comparison | Layer 7 |

Resolved since the last handoff: the store contract suite now takes a scratch
database (`35af687`), and `make test-race` runs in CI on every push
(`573be73`).

---

## 5. Layer 5 — what it is, and what it needs

**Goal:** the execution model handles real agent shapes, not just linear
sequences.

Roadmap items:

- [ ] Fan-out / fan-in with deterministic child step IDs
- [ ] Durable timers and external signals
- [ ] Cancellation and compensation hooks
- [ ] Retry policies, backoff, dead-letter handling

### 5.1 What is already in place for it

| Seam | Where | Layer 5 uses it to |
|---|---|---|
| `StepID.Child(n)` | `internal/core/ids.go` | number fan-out children by invocation order — one definition, unused until now |
| `effect_seq` in the key format | `internal/core/effect.go` | let one step declare several sub-effects without invalidating a key |
| `core.Decision` | `internal/core/decision.go` | gains `CallToolParallel`, `Sleep`, `WaitForSignal`, `Cancel`, `Compensate` |
| `pb.DecisionKind` | `proto/veya/worker/v1/worker.proto` | the same, on the wire. Add enum values; never renumber |
| `ctx.call` | `sdk/python/veya/agent.py` | the shape `ctx.sleep` / `ctx.wait_for` copy: suspend, and resume from history |
| `core.ErrUnavailable` | `internal/core/errors.go` | a run waiting on a timer or a signal is not a failed run, and the engine already knows the difference |
| `tool.KeyTTL` | `internal/core/tool.go` | per-tool policy already lives on the descriptor; retry policy joins it there rather than in a switch |

### 5.2 Decisions Layer 5 had to make first

All five are settled, with the argument, in
[execution-model.md](execution-model.md). In one line each:

| # | Question | Answer |
|---|---|---|
| 1 | Where a suspension lives | a nullable `available_at` on `runs` and one predicate in `RunsAwaitingAdvance`, not a `WAITING` run state |
| 2 | What a parallel decision looks like on the wire | `repeated ToolCall` inside one `Decision`, with a `JoinPolicy`, not `repeated Decision` |
| 3 | How a signal reaches a waiting run | stored on arrival keyed `(run_id, signal_id)`; `wait_for` is a read, so early and late arrival run the same code |
| 4 | Whether compensation is a decision or a mode | ordinary `CallTool` decisions the decider emits; the engine keeps knowing nothing about meaning |
| 5 | Whether Python gets `effect_seq` now | no. Fan-out makes child *step* IDs real; sub-effects stay deferred |

The same document writes down join semantics for a partially-complete fan-out,
which the plan named as the sequencing risk: it is cheap to state now and a
migration to discover later.

### 5.3 Entry criteria

- [x] `make test-integration` green on a machine with Docker — **217 PASS**,
      re-run on 20 September, first time since Layer 3
- [x] Suspension model chosen and written down (5.2 #1)
- [x] Parallel decision shape chosen (5.2 #2), since the `.proto` is public
- [x] Join semantics for a partially-complete fan-out written down

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
| Migrations | forward-only, embedded; `0001_init.sql`, `0002_outbox_relay.sql` |
| CI | `.github/workflows/ci.yml` — vet + gofmt + proto drift, race detector, Python SDK on 3.10 and 3.13, the worked example, and the integration suite |
| Shell | Windows; git is set to `core.autocrlf=true`, so the repo stores LF and the worktree has CRLF. `gofmt -l` occasionally flags a file for that reason alone |

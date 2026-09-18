# Project Status and Phase Handoff

**As of:** 18 September 2026 · `main` at `86d3cee` · working tree clean
**Complete:** Layers 1, 2, 3 · **Next:** Layer 4 — Agent execution

This is the running status file. It records what is done and verified, what is
knowingly left open, and what Layer 4 needs before it starts. Update it at the
end of each phase.

- Ideas and reasoning → [architecture-primer.md](architecture-primer.md)
- Where code lives → [code-map.md](code-map.md)
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

### Verification state

| Check | Result |
|---|---|
| `make test` (no Docker) | **103 PASS**, all green |
| `make test-integration` (PostgreSQL 16 + NATS 2) | **151 PASS**, all green |
| `go vet ./...` | clean |
| `gofmt -l .` | clean |
| `make demo` (PostgreSQL) | completes, 15 events, 2 ledger rows |
| `make demo-memory` (no Docker) | completes |
| `make demo-jetstream` | completes |
| Two-process run (runtime `--workers 0` + `veya-worker`) | completes |
| `make test-race` | **NOT RUN** — no C toolchain on this machine |

Size: 64 Go files, ~12,700 lines.

### To re-verify from scratch

```bash
make up && make migrate
make test
make test-integration
make demo
```

Then the distributed path, in three terminals:

```bash
make runtime     # engine only, --workers 0
make worker      # four workers over PostgreSQL SKIP LOCKED
veya run start   # expect COMPLETED within a second or two
```

**Gotcha that will waste your time:** the store contract suite shares the one
`veya` database and truncates it per subtest, so it deadlocks if anything else
is connected. Stop any `veya-runtime` / `veya-worker` before
`make test-integration`. See §3.

---

## 2. Commit index

Newest first. Each phase's commits are self-contained and the messages carry
the reasoning, so `git show` is the place to look for *why*.

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

## 3. Open items carried into Layer 4

Nothing here blocks Layer 4. Ordered by how likely it is to bite.

| # | Item | Why it is open | Cost to fix |
|---|---|---|---|
| 1 | **`make test-race` never run** | this machine has no C compiler (`cgo: gcc not found`). Layer 3 added real concurrency — heartbeat goroutines, the relay, the reaper, multi-process workers — so this is the highest-value unrun check | run it on any machine with gcc, or in CI |
| 2 | **Store contract suite shares one database** | it truncates `veya` per subtest, so any other connection deadlocks it. Cost me a confusing failure already. `internal/dispatch/postgres` already creates a database per test and does not have the problem | ~15 lines, copy the pattern from `internal/dispatch/postgres/postgres_test.go` |
| 3 | **Outbox rows are never trimmed** | published rows accumulate forever. Deliberate: trimming belongs with compaction and retention rather than scattered across layers | Layer 6 |
| 4 | **One subject for every delivery** | `core.DeliverySubject` is a single constant. A routing key that nothing routes on drifts out of sync unnoticed, so per-tool subjects wait until capability routing exists | Layer 5 |
| 5 | **No retry backoff or per-tool policy** | retries are immediate with a fixed three-attempt limit. A scheduler with nothing to schedule against would be guesswork | Layer 5 |
| 6 | **One lease TTL for all task types** | an LLM call and a deployment do not deserve the same timeout, but one number is honest until tools differ enough to matter | Layer 5 |
| 7 | **`redis` dispatch adapter absent** | only ever intended as a benchmark comparison | Layer 7 |

---

## 4. Layer 4 — what it is, and what it needs

**Goal:** the agent stops being a fixed list of steps and becomes a program
someone writes.

Roadmap items:

- [ ] Python SDK: `@agent`, `@tool`, `ctx` primitives
- [ ] Dynamic LLM-driven workflows
- [ ] Replay of recorded decisions
- [ ] Serialized run advancement

### 4.1 What is already in place for it

These seams exist and are tested; Layer 4 plugs into them rather than changing
them.

| Seam | Where | Layer 4 uses it to |
|---|---|---|
| `core.Decider` | `internal/core/decision.go` | add an LLM-backed decider beside `decider.Static`, with no engine change |
| `core.ToolRegistry` | `internal/core/tool.go` | register tools declared in Python instead of Go |
| `core.ToolCall` | `internal/core/tool.go` | carry `Key`, `RunID`, `TaskID`, `StepID`, `Attempt` across the wire to a Python handler |
| `core.Clock` / `core.IDGen` | `internal/clock`, `internal/idgen` | keep replay deterministic — no `time.Now()` or `rand` above the composition root |
| `runs.version` CAS | `engine.Advance` | serialize advancement, already proven safe across processes |
| `agent_version` pinning | `runs` table | a resumed run never changes the version it started under |
| `StepID.Child(n)` | `internal/core/ids.go` | exists so child numbering has one definition when fan-out lands in Layer 5 |

The replay contract is already written down in the `Decider` doc comment and is
the constraint the SDK must satisfy:

> Past decisions are replayed. Future decisions are generated. A decider that
> consults history and returns the first unrecorded step is replay-safe; one
> that ignores history and counts its own invocations is not, and will fork a
> run on recovery.

### 4.2 Decisions Layer 4 has to make first

These are open questions, not tasks. Worth settling before writing code.

1. **The worker protocol.** How does a Python process register its tools and
   receive tasks? Options seen so far: gRPC, HTTP long-poll, or NATS subjects.
   Whatever it is, `internal/worker` was kept deliberately thin so that a
   Python process can replace it — check that claim is still true before
   committing to a design.

2. **How a decision is recorded.** Replay reads history. Today history records
   `TASK_CREATED` with the tool and payload, which is enough to replay a
   *static* decider. An LLM decider probably needs the model call itself
   recorded — it is billed and non-deterministic, so by this project's own
   rules it is an **effect**, not a read. Likely needs a `DECISION_RECORDED`
   event type or an effect row per model call. Decide which.

3. **Determinism rules the SDK must impose on user code.** Idempotency keys
   come from logical position, which requires deterministic step numbering,
   which constrains user code to a deterministic ordering of durable calls.
   That constraint has to be stated, checked where possible, and documented —
   it is the one thing a user can break from outside.

4. **`effect_seq` stops being 1.** One task performs one tool call today. When
   the SDK lets a tool declare several sub-effects, the numbering becomes real.
   The key format already carries it (`run_id:step_id:E<seq>`) precisely so
   this does not invalidate keys already issued.

5. **Where the Python process gets its agent definition.** Go binaries share
   `internal/agents/meeting` so they cannot disagree about a tool's class. Two
   languages holding the same agent definition need an answer to the same
   problem — a process treating a send as `QUERYABLE` while another treats it
   as `UNRECONCILABLE` is the failure nobody notices until an outage.

### 4.3 Suggested entry criteria

Before starting, ideally:

- [ ] `make test-race` run somewhere with a C toolchain, and green (item 1 above)
- [ ] Worker protocol chosen and written down (4.2 #1)
- [ ] Decision-recording shape chosen (4.2 #2)

---

## 5. Environment notes

| | |
|---|---|
| Go | 1.26, `GOFLAGS=-mod=mod` |
| Dependencies | `lib/pq`, `nats.go` — that is all |
| PostgreSQL | container on **:5433**, not 5432, to avoid shadowing a host install |
| NATS | container on :4222, monitoring on :8222 (needs `-m 8222`, fixed in Layer 3) |
| DSN | `postgres://veya:veya@localhost:5433/veya?sslmode=disable` |
| Migrations | forward-only, embedded; `0001_init.sql`, `0002_outbox_relay.sql` |
| Shell | Windows; git is set to `core.autocrlf=true`, so the repo stores LF and the worktree has CRLF. `gofmt -l` occasionally flags a file for that reason alone |

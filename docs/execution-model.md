# The Execution Model

How a run waits, how a run branches, and why those two are one layer.

Every run Veya has executed up to now is a straight line: decide, call one
tool, record the result, decide again. This document settles the five
questions that stood between that and the two shapes which are not a straight
line — a run that **waits**, and a run that **branches**.

It is written before the code rather than after it, because two of the
decisions below are one-way doors. The `.proto` is public from its first
commit, so an enum value added in the wrong shape is a shape we keep.

- What exists now → [status.md](status.md)
- Where the code lives → [code-map.md](code-map.md)
- Why the worker boundary is where it is → [worker-protocol.md](worker-protocol.md)
- The plan these decisions unblock → [../.planning/LAYER_5_PLAN.md](../.planning/LAYER_5_PLAN.md)

---

## 1. The one invariant

Everything below follows from a single sentence, so it is worth stating before
anything else:

> **A run waits on time, or on tasks, and never on both.**

A run with a task in flight is waiting for a worker. A run with nothing in
flight is either owed a decision *now* or parked until some instant. Those are
disjoint by construction rather than by convention, because section 3 forbids
the one decision that could produce both at once — an agent cannot say "call
these three and also go to sleep" in a single turn.

That invariant is what lets suspension be one nullable column instead of a
state machine, and it is the thing to check first if any of this ever stops
making sense.

---

## 2. Where a suspension lives

**Decision:** a nullable `available_at` column on `runs`, plus one predicate in
`RunsAwaitingAdvance`. Not a new run state.

A run waiting on a timer is RUNNING with no task in flight. That is *exactly*
the condition `RunsAwaitingAdvance` exists to find — it is how the recovery
loop notices a run that was dropped mid-flight — so without a change, a
sleeping run would be swept up on every scan and advanced immediately, which
is the opposite of sleeping.

There were two ways out: add a `WAITING` run state, or teach the query that a
run with a future `available_at` is not stuck. The state looks tidier and
costs more. Run state lives in `core`, in both store adapters, in the state
machine tests, in the CLI's output, and in every transition assertion written
since Layer 1 — and it buys nothing, because a waiting run *is* still running,
and anything that can happen to a RUNNING run can happen to it.

The column is one predicate in one query, and it generalises: a signal wait and
a retry backoff are both "not before time T", so section 4 and the deferred
retry policy reuse it rather than adding a second mechanism.

### 2.1 The predicate, and the thing it must not do

```
status = 'RUNNING'
AND NOT EXISTS (a task for this run in PENDING or RUNNING)
AND (available_at IS NULL OR available_at <= now)
```

`NULL` means ready. That is the direction the default has to point, because
every run that existed before the migration has `NULL` in this column and
every one of them is ready.

**An `available_at` in the past means *ready*, not *late*.** A run whose
wake-up passed while the runtime was down has to be picked up by the ordinary
scan with no special case at all. Anything that treats an overdue timer as an
error condition loses every timer that expired during an outage, which is the
exact failure durable timers exist to prevent. The scan is the timer wheel;
there is no second mechanism, and nothing holds a pending wake-up in memory
where a restart could drop it.

### 2.2 Waiting with no scheduled wake

A signal wait has no deadline of its own. It ends when a signal arrives, which
may be in a second or never.

`NULL` cannot express that, because `NULL` already means ready. So an
indefinite park stores a sentinel instant far beyond any real schedule,
exposed as one named constant (`core.Indefinite`) rather than written out at
call sites. The alternative is a second column whose only job is to say which
of two meanings the first column carries, and two columns asserting one fact is
the drift this codebase has avoided elsewhere — see `tasks` having no owner
column because `leases` already holds ownership.

The sentinel is not how a signal-waiting run wakes up. Section 4 makes signal
arrival a transactional write that clears the column outright, so the
far-future value is never actually reached; it exists so that nothing sweeps
the run in the meantime. The CLI renders it as what the run is waiting *for*,
never as a date in the year 9999.

---

## 3. What a parallel decision looks like on the wire

**Decision:** `repeated ToolCall calls` inside the existing `Decision` message,
with a `JoinPolicy` alongside it. Not `repeated Decision`.

A `repeated Decision` is the obvious encoding and it permits nonsense: a
`Sleep` and a `Cancel` in the same batch, two `Complete`s, a `Complete`
followed by a `CallTool`. Every one of those has to be rejected at runtime, in
Go, with an error message, and the Python SDK has to take care never to
construct one. One `Decision` carrying N calls makes those states
unrepresentable — the wire format does the rejecting, and there is no error
message to write because there is no error to report.

It also keeps a decision atomic, which matters more than it looks. The engine
commits one decision as one transaction. N decisions in a batch raises "what
if three of them are applied and the process dies", which is a question this
design has never had to answer and should not start now.

**The constraint this accepts:** an agent cannot fan out and sleep in one turn.
It gets that by fanning out, joining, and then sleeping on the next decision —
one more round trip, and a history that reads in the order things happened.
This is also the constraint section 1 rests on.

---

## 4. How a signal reaches a waiting run

**Decision:** a `signals` table and a `SignalStore` port. Signals are stored on
arrival, keyed `(run_id, signal_id)`, whether or not anything is waiting.

The bug everyone writes here is the early signal: an external system calls back
faster than the run reaches its `wait_for`, the delivery finds no waiter, and
the signal is dropped. The run then waits forever for something that already
happened. It is a race, so it passes every test written by someone who has not
thought about it, and it fails in production under load.

Storing on arrival deletes the race rather than narrowing it. There is no
"deliver to a waiting run" path at all. Arrival writes a row; `WaitForSignal`
is a **read** that either finds the row and proceeds, or parks the run until
one appears. Early and late arrival run identical code, so the case that is
hard to test is the case that is always exercised.

Arrival also clears `available_at` in the same transaction that writes the row.
That is the wake-up, and it is transactional rather than a hint — unlike the
outbox relay's `Wake`, which is allowed to be missed because the delivery is
already durable. Here the run is parked at a sentinel instant, so a missed wake
would be a run that never resumes, and nothing about that is a mere latency
cost.

`signal_id` carries the dedup. At-least-once delivery is the assumption
everywhere else in this system and the signal port does not get to be special.

---

## 5. Whether compensation is a decision or a mode

**Decision:** a sequence of ordinary `CallTool` decisions that the decider
emits. Not a runtime-driven phase.

The alternative — the runtime walks the effect ledger backwards and invokes
registered compensation handlers — is harder to get wrong from user code, and
puts rollback semantics inside the engine, where the engine would have to
understand what a compensation *is*. This design's habit is that the engine
knows about effects and knows nothing about meaning, and every time that habit
has been kept the result has been smaller.

As decisions, compensations are ordinary effects: same executor, own
idempotency keys, same ledger. That is not a nicety. A compensation that runs
twice is a double refund reversal — the same bug one layer down — and the only
thing in this system that prevents it is the ledger the ordinary path already
goes through.

---

## 6. Whether Python gets `effect_seq` at the same time

**Decision:** no. Fan-out makes child *step* IDs real; sub-effects within one
step stay deferred.

Both features make the `effect_seq` field in the idempotency key real, and they
are independent — one step declaring three effects is not the same thing as one
decision making three calls. Shipping both at once is precisely the mistake
Layer 4 avoided when it declined to add sub-effects alongside a whole second
language: when the ledger then produces a key nobody expected, there are two
candidate causes and no way to bisect.

Keys issued before this layer must not change. `run:step:E1` for a single-call
step stays `run:step:E1` forever; what becomes real is that the step part can
now read `S3.0` as well as `S3`.

---

## 7. Join semantics

This section is the guard named in the plan's section 5: fan-out is the phase
that can invalidate the suspension model, so what a partially-complete fan-out
means is settled here, before the migration that would be expensive to redo.

### 7.1 Children are numbered by invocation order

A fan-out of N calls from step `S3` creates children `S3.0` through `S3.(N-1)`,
indexed by position in the `calls` list exactly as the decider returned it.
`StepID.Child(n)` has had this definition since Layer 1 and is finally used.

Completion order is not part of it and never can be: the idempotency key is
derived from the step ID, so numbering by whichever child finished first would
give the same logical call a different key on every replay, and the ledger's
one guarantee would evaporate.

Results are handed back in invocation order too, regardless of when each
arrived.

### 7.2 The policies

| Policy | Satisfied when | Also satisfied when |
|---|---|---|
| `ALL` | every child is terminal | — |
| `ANY` | one child completed successfully | every child has failed |
| `QUORUM(k)` | k children completed successfully | fewer than k can still succeed |

The second column is the half that gets forgotten. A join that can only be
satisfied by success is a join that hangs forever on a bad day, and a run
hanging forever is the failure mode this whole layer exists to remove.

### 7.3 Partial failure is the decider's business

When a join is satisfied, the body receives **every** outcome — successes and
failures alike, in invocation order. The engine does not fail the run because
three of ten children failed, and does not succeed it because seven did.
Whether that is a disaster or a Tuesday is a question about meaning, and the
engine does not do meaning.

### 7.4 A satisfied join can leave children in flight

`ANY` and `QUORUM` both finish while siblings are still running. Those siblings
are not cancelled — cancellation is Layer 6, and stopping them is a different
mechanism from ignoring them.

So their effects land, and **are still recorded**: the ledger row was reserved
before the call, the result is written when it returns, and the run's effect
list remains a complete account of what the run did to the outside world even
though the body stopped reading. A ledger with an orphan in it would be worse
than a slow child.

The same is true of a run that completes with children in flight. History gains
their `TASK_COMPLETED` events after `RUN_COMPLETED`, which is not tidy and is
true, and true beats tidy in an audit trail.

### 7.5 A fan-out needs no new "still waiting" decision

When the join is not yet satisfied, the body suspends and the decider re-issues
**the same fan-out decision**. The engine tries to create the same N children,
`CreateTask` reports `ErrTaskExists` on the `(run_id, step_id)` unique
constraint, the transaction rolls back, and the engine treats it as a no-op —
which is the identical path a re-decided single `CallTool` has taken since
Layer 1.

This is the reason fan-out adds no waiting state anywhere. The mechanism that
makes re-deciding one step harmless makes re-deciding N steps harmless, and it
is a database constraint rather than a check somebody has to remember to write.

### 7.6 `available_at` is never set on a run with children

By section 1. The engine sets `available_at` only from `Sleep` and
`WaitForSignal`, both of which are decisions a body reaches with nothing in
flight, because section 3 forbids combining them with calls. A fan-out
therefore cannot produce a run that is both parked and busy, and the scan never
has to reason about one.

---

## 8. What this layer deliberately does not do

Four things that sound like they belong here and do not. Each is **policy
layered on the execution model, not part of it** — none changes how a run
suspends, how a step is numbered, or how replay works, which is the test.

| Deferred | Why it separates cleanly |
|---|---|
| Retry policy, backoff, dead-letter | policy on the tool descriptor, next to `KeyTTL`. Backoff is "not before time T", which section 2 already built |
| Deadline propagation | the same column mechanics as `available_at`, opposite comparison; worth nothing until there is a retry policy to bound |
| Cancellation | about the effect ledger, not about suspension. It interacts with fan-out (7.4), which argues for doing it *after* fan-out exists |
| Compensation / SAGA | section 5 made it an SDK convention over ordinary decisions, so it needs cancellation first and no engine work at all |

They arrive in Layer 6, which is therefore *Policy and operability* rather than
*Operability*.

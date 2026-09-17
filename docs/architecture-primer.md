# Veya: How It Works

A plain-English walkthrough of Veya's execution model.

**Who this is for:** anyone who knows roughly what a database transaction, a worker process, and a message queue are. You don't need prior experience with durable execution or workflow engines.

**What this covers:** why the system is built the way it is, and what each moving part protects you from. For exact schemas, API signatures, and the full list of tradeoffs, see the [README](../README.md).

---

## Before we start: the two pieces of infrastructure

Veya runs on **PostgreSQL and NATS JetStream**. Not one or the other — both, doing two jobs that should never be confused.

```
        ┌──────────────────┐         ┌──────────────────┐
        │    PostgreSQL    │         │  NATS JetStream  │
        │                  │         │                  │
        │     THE TRUTH    │         │   THE DELIVERY   │
        │                  │         │                  │
        │  what is true    │         │  "hey worker,    │
        │  what happened   │         │   do this one"   │
        │  kept forever    │         │  gone once acked │
        └──────────────────┘         └──────────────────┘
```

| | PostgreSQL | NATS JetStream |
| --- | --- | --- |
| Its job | Records what is true and what happened | Carries work to whichever worker is free |
| Holds | Runs, events, tasks, effects, leases | Task messages, briefly |
| Answers | "What is the state of run R123?" | "Is there work for me?" |
| Data lifetime | Permanent — that is the point | Seconds. Deleted once a worker acks it |

Think of JetStream as a **pipe**, not a filing cabinet. A task message exists in it only for the moment between "this work was created" and "a worker picked it up." Then it's gone. Nothing you'd ever want to look up later lives there.

**Why not put everything in JetStream?** Because every safety barrier in this document needs something a message stream structurally cannot do: enforce that a key is unique forever, update a row only if it hasn't changed, commit several records together or not at all, and answer questions like "which effects have been stuck for over a day?" Those are database operations. Streams move messages; they don't do any of that.

**Why not put everything in Postgres?** You can, and early on we do — Postgres alone works fine as a task queue (§4). JetStream is added later, when the reason is throughput rather than correctness.

Throughout this document, "the database" means PostgreSQL and "the queue" means JetStream.

---

## Table of Contents

1. [The problem, in one story](#1-the-problem-in-one-story)
2. [Why one big lock is the wrong answer](#2-why-one-big-lock-is-the-wrong-answer)
3. [The idea that organizes everything: safety vs. liveness](#3-the-idea-that-organizes-everything-safety-vs-liveness)
4. [Barrier 1: claiming a task](#4-barrier-1-claiming-a-task)
5. [Barrier 2: the lease](#5-barrier-2-the-lease)
6. [Barrier 3: the fencing token](#6-barrier-3-the-fencing-token)
7. [Barrier 4: the idempotency key](#7-barrier-4-the-idempotency-key)
8. [Barrier 5: version CAS](#8-barrier-5-version-cas)
9. [The effect ledger and the word UNKNOWN](#9-the-effect-ledger-and-the-word-unknown)
10. [The most important ordering rule](#10-the-most-important-ordering-rule)
11. [Not every tool can be made safe](#11-not-every-tool-can-be-made-safe)
12. [Getting work to workers: the outbox](#12-getting-work-to-workers-the-outbox)
13. [What makes this an *agent* runtime](#13-what-makes-this-an-agent-runtime)
14. [The whole thing, end to end](#14-the-whole-thing-end-to-end)
15. [Now let's break it](#15-now-lets-break-it)
16. [What we deliberately don't promise](#16-what-we-deliberately-dont-promise)
17. [Cheat sheet](#17-cheat-sheet)

---

## 1. The problem, in one story

Throughout this document we'll follow one job:

> **A meeting assistant is asked to send the meeting summary to all participants.**

Simple enough. The agent looks up the meeting, asks an LLM to write a summary, and calls an email API.

Now run it on more than one machine.

```
              Task T456: "send summary"
                        |
             ┌──────────┴──────────┐
             ↓                     ↓
         Worker A              Worker B
```

We want many workers so the system is fast and survives a machine dying. But the moment there are two workers, both can end up holding the same task. If both run it, the participant gets:

```
"Meeting summary..."
"Meeting summary..."
```

Two emails. And an email cannot be un-sent.

That is the whole problem. Everything below exists to make sure that doesn't happen — while still letting the system keep working when machines crash, networks stall, and clocks disagree.

---

## 2. Why one big lock is the wrong answer

The obvious fix is a lock: only one worker may hold T456 at a time.

The trouble is that a distributed lock is a promise you cannot actually keep. Suppose Worker A holds the lock and then its network connection freezes for 30 seconds. The lock service can't tell the difference between "A is slow" and "A is dead." It has to pick:

- **Never expire the lock.** Then one frozen machine stalls that task forever.
- **Expire the lock.** Then A might wake up mid-operation still believing it holds it.

There's no third option. Any lock with a timeout can be held by two parties at once, at least briefly. Any lock without a timeout can deadlock the system.

So Veya doesn't try. Instead of one mechanism that's supposed to be perfect, it uses **several independent mechanisms, each guarding a different boundary**. No single one has to be perfect, because the others catch what it misses.

The key realization:

> **There isn't one concurrency problem. There are several, and they need different answers.**

| The question | The mechanism |
| --- | --- |
| Who should pick up this task? | Task claim |
| Who currently owns it? | Lease |
| Whose writes back into the system are still valid? | Fencing token |
| Can this external action happen twice? | Idempotency key |
| Who gets to move this run forward? | Version CAS |

---

## 3. The idea that organizes everything: safety vs. liveness

Before the details, here's the frame that makes the rest make sense.

Every guarantee in a distributed system is one of two kinds.

**Safety — "nothing bad ever happens."**

```
No duplicate email is ever sent
No stale worker ever overwrites good state
No two workers ever advance the same run
No committed work is ever lost
```

**Liveness — "something good eventually happens."**

```
A dead worker's task eventually gets picked up by someone else
A pending task eventually runs
A committed event eventually reaches the queue
```

This distinction is the single most useful thing to hold in your head, because it tells you **which imperfections you're allowed to tolerate**.

If a lease expires too early, a healthy worker gets interrupted and its task is handed to someone else. Annoying, wasteful — but nothing is corrupted. That's a *liveness* problem, and liveness problems are survivable.

If a stale worker could overwrite current state, or send a second email, that's a *safety* problem. Safety problems are not survivable, so those mechanisms must be exact.

That's why the design can afford a lease that's merely "usually right," but cannot afford a fencing check that's merely "usually applied."

```
Safety mechanisms  →  must be exact  →  fencing tokens, UNIQUE constraints,
                                         version CAS, database transactions

Liveness mechanisms →  may be fuzzy   →  lease expiry, the reaper,
                                         retries, the outbox relay
```

Keep this split in mind for everything that follows.

---

## 4. Barrier 1: claiming a task

A task sitting in the database looks like this:

```
task_id    status
--------------------
T456       PENDING
```

Both workers can see it. Both want it. So claiming isn't "read it and start" — it's a **conditional state change**:

```
Worker A:   PENDING → RUNNING     ✅ succeeded
Worker B:   PENDING → RUNNING     ❌ it wasn't PENDING anymore
```

The database does this atomically. Exactly one worker wins. The loser doesn't retry or complain — it simply drops the task and moves on.

This is also why duplicate messages from the queue are harmless. If T456 is delivered twice, the second delivery just loses the claim.

### Doing this with Postgres alone

If Postgres is acting as the queue, `SELECT ... FOR UPDATE SKIP LOCKED` handles this neatly. Normally, if Worker A has locked row T1, Worker B asking for a task would *wait* for T1 to free up. `SKIP LOCKED` says: don't wait, just pass over anything locked and take the next one.

```
Queue:   T1    T2    T3

Worker A  →  locks T1
Worker B  →  T1 is locked, skip it  →  takes T2
Worker C  →  T1, T2 locked, skip    →  takes T3
```

No waiting, no broker, no extra infrastructure. This is why early development uses the Postgres dispatch backend: you can prove the execution model works with nothing but a database. A message broker gets added later, when the reason to add one is throughput rather than correctness.

---

## 5. Barrier 2: the lease

A lease records **temporary ownership**:

```
task_id   owner      fencing_token   expires_at
------------------------------------------------
T456      Worker-A   10              21:10
```

Read it as: *Worker A is expected to be working on T456 until 21:10.*

While Worker A is alive it sends a heartbeat, and the runtime pushes the expiry forward:

```
Worker A ──heartbeat──→ runtime ──→ expires_at = 21:15
         ──heartbeat──→ runtime ──→ expires_at = 21:20
```

If Worker A dies, the heartbeats stop. The clock passes 21:10. A background process called the **reaper** notices the expired lease and makes the task available again:

```
Worker A                    Reaper                Worker B
   💥                          |                     |
 (silent)                      |                     |
                     lease expired → reclaim         |
                                      ──────────────→ picks up T456
```

Lease durations are set per task type. Waiting on an LLM call and waiting on a long-running deployment shouldn't have the same timeout.

### A lease is not a lock

This is the part people get wrong, so it's worth being blunt.

A lock claims: *only one party can possibly hold this.*
A lease claims something much weaker: *one party is **expected** to hold this, for now.*

Here's why the weaker claim is all you get. Worker A's network freezes:

```
Worker A                Database               Worker B
  owns T456
     |
   ❄️ frozen
     |                  21:10 passes
     |                  lease expires
     |                            ────────────→  Worker B claims T456
     |
  thaws out
  "I still own T456!"                            "I own T456!"
```

Worker A was never told its lease expired. It couldn't be — it was unreachable. So for a moment, **both workers sincerely believe they own the task.**

That is not a bug to be fixed. It is a permanent property of distributed systems, and any design that assumes otherwise is wrong. So Veya treats the lease as what it actually is:

> **A lease is a scheduling hint. It decides who *should* work. It does not, and cannot, guarantee that only one worker *does*.**

Which means correctness has to come from somewhere else. That's the next two barriers.

---

## 6. Barrier 3: the fencing token

Every time a task is handed to a new owner, the new owner gets a number that is **always higher than the last one**:

```
Worker A claims T456   →   token 10
(lease expires)
Worker B claims T456   →   token 11
```

The runtime remembers that 11 is current. Now when the frozen Worker A wakes up and tries to write its result:

```
Worker A: "T456 is done, here's the result."   (token 10)
Runtime:  current token is 11. 10 < 11.
Runtime:  REJECTED.

Worker B: "T456 is done, here's the result."   (token 11)
Runtime:  ACCEPTED.
```

Worker A is now a zombie. It can shout all it likes; it has no authority. It learns it's stale from the rejection and stops.

So the division of labor is:

> **The lease answers "who should be working?"**
> **The fencing token answers "whose writes still count?"**

### Every mutation needs the token — including the heartbeat

It's tempting to check the token only when a worker submits its final result. That's not enough. **Every** call that changes state must carry and verify it: completing a task, recording an effect, appending an event, and — easy to miss — extending the lease.

Consider the heartbeat. Worker A is stale with token 10. Worker B holds token 11. If A's heartbeat isn't fenced, A renews a lease it no longer owns, and now the system thinks the task belongs to a dead worker. The renewal must be conditional on the token, so a stale heartbeat updates zero rows and A learns it's out.

> Ownership lives in one place — the `leases` table — and is checked there. Storing the same token in two tables invites them to drift, and a drifted token silently disables the protection it was meant to provide.

### The reaper plays by the same rules

The reaper reclaims tasks whose leases look expired. But the reaper is just another process. It can be slow, it can be wrong about the clock, and there can be more than one of it running.

```
Worker A                Reaper
  still working         "looks expired to me"
      |                        |
      |                   reclaims task
      |                        ↓
      |                    Worker B picks it up
      ↓
  tries to finish  →  fenced out (token too low)
```

The reaper is not above the concurrency model. Its writes are fenced like everyone else's. A reaper that could reassign tasks without respecting tokens would be the biggest hole in the system.

---

## 7. Barrier 4: the idempotency key

Fencing stops a stale worker from writing *into Veya*. It does nothing about the outside world. Nothing stops a zombie worker from calling the email API — that call doesn't go through us.

So we need a different barrier at that boundary.

Every external action gets a key derived from **where it sits in the run**, not from what it contains:

```
idempotency_key = run_id : step_id : effect_seq
                = R123 : S2 : E1
                = "R123:S2:E1"
```

Read it as: *the first external action of step 2 of run R123.*

The `effects` table has a `UNIQUE` constraint on that column. So:

```
Worker A:  INSERT effect "R123:S2:E1"   →  ✅ created
Worker B:  INSERT effect "R123:S2:E1"   →  ❌ UNIQUE violation
```

Only one effect row can ever exist for that logical action. The database enforces it — not application code, not careful programming, not a code review.

### Why "position" and not "a hash of the request"

A tempting alternative is to hash the request body. It breaks in a way that's easy to miss.

Imagine two refunds that are logically the same operation, but one carries `reason: "damaged"` and a retry carries `reason: "damaged item"`. Different bytes, different hash, two different keys — so both execute. The customer is refunded twice, and the key that was supposed to prevent it did nothing.

Logical position doesn't have that failure mode. Step 2 of run R123 is step 2 of run R123 no matter what the payload says, no matter how many times it's retried, no matter how the text was edited.

### What the loser must do

This is the detail implementations get wrong.

When Worker B hits the `UNIQUE` violation, it must **not** treat it as an error and fail the task. That would turn a safely-prevented duplicate into a stuck run.

The violation means *this action already has a record — go read it*. Worker B loads the existing row and looks at its status:

```
row says COMMITTED  →  already done, nothing to do, move on
row says PENDING    →  nothing was sent yet, safe to execute
row says RUNNING    →  someone else may be mid-flight, or crashed — see §9
```

The constraint doesn't just block the duplicate. It forces the second worker onto the path where it has to find out what actually happened.

### The key only works if the provider honors it

One more thing, and it's a real limitation rather than an implementation detail.

Our database now knows "this is action R123:S2:E1." That stops *us* from creating a second record. It does **not** stop the email provider from sending a second email, unless we pass the key along and the provider deduplicates on it:

```
Veya                             Email provider
  key = R123:S2:E1
      ──── send, Idempotency-Key: R123:S2:E1 ────→
                                   sees the key,
                                   recognizes the repeat,
                                   does not send again
```

Half of this guarantee lives in someone else's system. Which leads to §11.

---

## 8. Barrier 5: version CAS

One more concurrency problem, at a different level.

A run is a sequence of decisions. After each step finishes, something has to decide what happens next — usually by calling the LLM again. Now suppose a decision fanned out into three parallel tasks and two of them finish at the same instant:

```
Task T1 completes  →  "I should advance the run"
Task T2 completes  →  "I should advance the run"
```

If both do, you get two LLM calls, two different answers, and the run splits into two divergent timelines. That's much worse than a duplicate email.

The fix is **compare-and-swap** on a version number. Each run carries one:

```
Run R123, version = 20
```

To advance it, you say: *change it, but only if it's still at the number I read.*

```sql
UPDATE runs
SET status = $1, version = version + 1
WHERE run_id = 'R123' AND version = 20;
```

```
Worker handling T1:   version 20 → 21   ✅  1 row updated
Worker handling T2:   version 20 → 21   ❌  0 rows updated
```

The loser sees zero rows changed, re-reads the run, notices it's already at 21, and concludes someone else handled it. Exactly one LLM call happens.

### Why this is per-run, not global

A single global lock would work and would also be useless:

```
Run A advancing  →  Run B waits  →  Run C waits
```

Instead each run has its own version, so they never contend:

```
Run A:  20 → 21
Run B:   7 → 8      all at the same time
Run C:  42 → 43
```

Ordering is protected **inside** a run. Throughput is untouched **across** runs.

### Make the safe path the only path

The same warning as fencing. CAS only protects you if every ordering-sensitive write actually participates in it. If someone can write:

```go
UpdateRun(runID, status)      // no version, no token
```

then the protection can be skipped by accident, and it will be.

The fix is to make it impossible rather than discouraged — the token and expected version are required parameters, so omitting them doesn't compile:

```go
UpdateRun(ctx, runID, expectedVersion, fencingToken, status)
```

Safety mechanisms that depend on everyone remembering to use them are not safety mechanisms.

---

## 9. The effect ledger and the word UNKNOWN

We've covered the machinery. Now the idea the whole project is built around.

Every external action gets a row that tracks its real-world status:

| Status | What it means |
| --- | --- |
| `PENDING` | We intend to do this. Nothing has been sent. |
| `RUNNING` | A worker is doing it right now. |
| `COMMITTED` | The outside world definitely changed. |
| `FAILED` | It definitely did not happen. |
| `UNKNOWN` | **We genuinely do not know.** |

Most systems have the first four. `UNKNOWN` is the one that matters.

### Why "timed out" is not "failed"

Worker A sends the email request. Twenty seconds pass. No response.

What happened? Any of these:

- The request never left the machine. *(No email.)*
- The provider received it, sent the email, and the reply was lost. *(Email sent.)*
- The provider received it and is still working. *(Email in progress.)*

We cannot tell these apart. That's not a gap in our logging — the information genuinely does not exist on our side.

So the naive thing to do — treat "no response" as "failed" and retry — is a coin flip that sends a second email half the time. **Treating timeouts as failures is the single most common cause of duplicate side effects in real systems.**

Veya refuses to guess. It writes `UNKNOWN`, which means exactly what it says, and triggers a different code path:

```
UNKNOWN  →  find out what actually happened  →  resolve
```

not:

```
UNKNOWN  →  assume the worst  →  retry  →  💥 duplicate
```

`UNKNOWN` is not a failure state. It's an honest one.

### Recovering an interrupted task

When a lease expires and a task comes back, recovery reads the effect row to decide what's safe:

```
no effect row exists     →  nothing was ever sent. Run it cleanly.
effect is PENDING        →  recorded but never sent. Safe to execute.
effect is RUNNING        →  may or may not have happened → UNKNOWN → reconcile
effect is COMMITTED      →  already done. Skip it and move on.
```

Four states, four unambiguous actions. No guessing anywhere.

---

## 10. The most important ordering rule

If you remember one implementation detail from this document, make it this one.

**The correct order:**

```
1.  Write the effect row to the database
2.  COMMIT
3.  Call the external API
4.  Record the outcome
```

**Why it has to be this way.** Suppose we crash right after the commit, before the API call. The database says `PENDING`. Recovery reads that and knows: *nothing was sent, it is safe to run.* Correct outcome.

Now suppose we crash right after the API call. The database says `RUNNING`. Recovery knows: *this might have happened.* It marks it `UNKNOWN` and goes and checks. Correct outcome.

In both cases the database knew about the action **before** it could possibly have occurred.

**The broken order:**

```
1.  Call the external API     ← email is sent
2.  💥 crash
3.  (database never hears about it)
```

Now the database has no record at all. Recovery looks, sees nothing, and concludes nothing was sent. It runs the task cleanly. The participant gets a second email — and there is no mechanism anywhere in the system that could have caught it, because from the database's point of view the first email never existed.

> **Write down what you're about to do, and commit it, before you do it.**

Get this backwards and every other barrier in this document stops working. The effect row is created as `PENDING` in the same transaction that creates the task itself, so a task can never exist without its record.

---

## 11. Not every tool can be made safe

Here is the limit of what any runtime can promise. Veya's approach is to state it explicitly rather than pretend.

When an effect is `UNKNOWN`, what we can do depends entirely on the external service. Every tool declares which kind it is:

### `IDEMPOTENT_BY_KEY`

The provider accepts our key and guarantees a repeat is not a new action.

```
send(key = R123:S2:E1)   →  email sent
send(key = R123:S2:E1)   →  "already handled this" — no second email
```

On `UNKNOWN`, just send it again. Safe by construction. **Best case.**

### `QUERYABLE`

The provider won't dedupe, but it will let us ask:

```
GET /messages?ref=R123:S2:E1

  found     →  it happened  →  mark COMMITTED
  not found →  it didn't    →  safe to send
```

On `UNKNOWN`, look it up, then resolve. **Also fine.**

### `UNRECONCILABLE`

The provider neither dedupes nor answers questions. Fire-and-forget with no receipt and no lookup.

```
UNKNOWN  →  ...no way to find out.
```

There is no clever algorithm here. If the outside world won't tell you what it did, you cannot know what it did. Guessing means either a duplicate or a silent drop, and both are bad.

So these escalate to a human and stop. The runtime will not guess.

> A tool that is neither idempotent nor queryable **cannot** be made exactly-once by any runtime, anywhere. Veya's contribution isn't solving that — it's making it visible in the type system and failing loudly instead of quietly rolling the dice.

### Keys expire — theirs, not ours

A trap worth knowing about. We keep effect records for a long time. Providers usually don't:

```
Our database:      "R123:S2:E1" — remembered indefinitely
Email provider:    idempotency keys retained for 24 hours
```

Three days later, reconciling a `QUERYABLE` effect:

```
Us:        "What happened to R123:S2:E1?"
Provider:  "Never heard of it."
```

That does **not** mean it never happened. It means the provider forgot. Treating it as "didn't happen" and re-sending is exactly the duplicate we've spent this whole document avoiding.

So each tool declares how long its provider actually honors keys. Past that window, an unresolved effect escalates to a human instead of being reconciled automatically. Part of this guarantee lives in someone else's system, on someone else's retention policy, and the design has to respect that.

---

## 12. Getting work to workers: the outbox

This is where NATS JetStream enters. Everything up to now has been Postgres doing its job — recording what is true. Now we need to get that work *out* to workers, and the moment a second system is involved, a new failure appears.

Two systems, one problem. Postgres holds the truth. JetStream delivers the work. How do you update both without them disagreeing?

### The dual-write problem

The obvious approach is to do them one after the other. It fails in both orders.

**Database first:**

```
UPDATE Postgres   ✅ task created
💥 crash
PUBLISH to queue  ✗ never happens
```

The task exists and will never be delivered. The run is stuck forever, silently. Nothing is broken enough to alert on.

**Queue first:**

```
PUBLISH to queue  ✅ worker picks it up
💥 crash
UPDATE Postgres   ✗ never happens
```

A worker is processing a task the database doesn't believe exists.

There is no ordering that works, because two separate systems cannot be updated atomically. This is the **dual-write problem**, and it's the same class of bug Veya exists to eliminate for user tool calls — so allowing it in our own plumbing would be indefensible.

### The fix: write the intent to send as part of the transaction

Instead of publishing directly, we record that we *want* to publish — in the same transaction as everything else:

```
BEGIN
  INSERT tasks        (T456, ...)      -- the work exists
  INSERT events       (TASK_CREATED)   -- history records it
  INSERT task_outbox  (deliver T456)   -- the intent to deliver it
COMMIT
```

All three, or none. No window where they disagree.

Then a small background process — the **relay** — reads unpublished outbox rows and pushes them to the queue:

```
   ┌──────── one Postgres transaction ────────┐
   │  task created                            │
   │  event appended                          │
   │  outbox row: "deliver this"              │
   └──────────────────┬───────────────────────┘
                   COMMIT
                      ↓
                 Outbox relay
                      ↓
                    Queue  ──ack──→  mark outbox row published
                      ↓
                   Worker
```

If the relay is down, rows pile up harmlessly and get sent when it returns. Nothing is lost, because the truth was committed before the relay was ever involved.

### The relay can crash too — and that's fine

```
Relay publishes to queue   ✅
💥 relay crashes before marking the row published
Relay restarts, sees an unpublished row, publishes again
```

The same task gets delivered twice. That's expected and by design: the outbox gives you **at-least-once** delivery, not exactly-once.

Which is fine, because we already handle duplicate delivery — the second copy loses the `PENDING → RUNNING` claim (§4), and if it somehow got past that, the `UNIQUE` key blocks the duplicate effect (§7). Duplicate delivery was never the thing we were afraid of.

> **Where at-least-once and at-most-once conflict, always choose at-least-once.** A duplicate is visible and defensible. Lost work is silent, and you find out from a customer.

### Speeding up the relay

Polling every second works but adds latency: a task committed at 12:00:01 might not be published until 12:00:02. So the engine *nudges* the relay the moment its transaction commits — `relay.Wake()`, a plain function call.

The important part is that the nudge is an *optimization*, not a dependency. It is not durable and it is not delivered anywhere; if the relay is busy, the nudge is simply dropped. The relay keeps polling as a fallback. A lost nudge costs a second of latency; it never loses the row.

This is the safety/liveness split showing up in the shape of the code. The outbox row went into the transaction because it must never be lost. The nudge is a bare function that returns nothing and cannot fail, because losing it costs nothing.

Across processes the same idea needs Postgres `LISTEN/NOTIFY` — a runtime on one machine cannot call a function on another. The reasoning is identical: `LISTEN/NOTIFY` is not durable either, so it would be an optimization over the same poll.

> Polling hurts latency. It does not hurt correctness. Keep the poll whatever you add on top.

### JetStream is not the source of truth

One principle that makes everything downstream easier: a message arriving from JetStream means *something probably happened* — not *here is the truth*.

```
JetStream message  →  "task T456 may need attention"
                              ↓
                          Postgres  →  "here is what is actually true"
```

A worker never trusts the message body as fact. It treats the message as a nudge, then reads the real state from the database. This is why JetStream can lose a message, duplicate a message, or deliver messages out of order without any of it mattering.

Because of this, message duplication and message ordering stop being problems you have to solve. Duplicates lose the claim. Ordering across tasks doesn't matter, because tasks are independent units of work — order *within* a run comes from the version CAS and the fan-in join (§13), never from the order messages happen to arrive in.

That's a stronger position than "our consumers tolerate reordering." The design makes message order irrelevant by construction.

### The same argument, applied to the transport itself

If the queue is only a nudge, then *which* queue barely matters — which is why Veya has three, and why they are interchangeable:

| Transport | Workers can live | Needs |
|---|---|---|
| in-process channel | in this process only | nothing |
| Postgres `SKIP LOCKED` | in any process | the database you already have |
| NATS JetStream | anywhere | a NATS server |

The Postgres one is worth a second look, because it exposes what the outbox is actually *for*. There, the queue and the database are the same system, so a committed task is already visible to every poller — publishing has nothing left to do. The outbox exists to cross a boundary between two systems that cannot share a transaction. Remove the boundary and it has no work to do.

---

## 13. What makes this an *agent* runtime

Everything so far would apply to any durable job system. These next three are specific to running AI agents, and they're the reason Veya exists rather than a generic task queue.

### Replay: don't re-ask questions you already have answers to

An agent run is a chain of LLM decisions. If a run crashes at step 7, we must not start over from step 1 — that would re-bill every model call and, because LLMs are non-deterministic, could send the agent down a completely different path than the one it was already halfway through.

So recovery **replays the past and generates only the future**:

```
Step 1  →  decision recorded in history  →  read it back, don't re-ask
Step 2  →  decision recorded in history  →  read it back, don't re-ask
Step 3  →  decision recorded in history  →  read it back, don't re-ask
Step 4  →  nothing recorded              →  call the LLM for real
Step 5  →  (the future, generated normally)
```

Note what this is *not*. We are not trying to make the LLM deterministic — that's impossible and also undesirable. We're saying: **a decision that has already been made is a fact, and facts get read from history, not re-derived.** Everything past the last recorded decision proceeds non-deterministically, exactly as it should.

> **The past is replayed. The future is generated.**

### LLM calls are effects, not reads

It's natural to think of an LLM call as a read — ask a question, get an answer, no consequences. That's wrong in a way that costs money.

A model call is billed, non-deterministic, and cannot be replayed by the provider. If a worker crashes mid-completion and we treat that call as a harmless read, every recovery silently re-bills it, and there's no record that it happened.

So LLM calls get an effect row and a stable identity, exactly like a refund does. The ledger is what makes replay possible in the first place — you can only read a decision back from history if you wrote it down when it happened.

### Parallel steps need stable names

A decision can fan out into several tasks at once, and the run continues only when they all finish:

```
Decision at step S4  →  [ T1, T2, T3 ]
                              ↓
                    all three COMPLETED?
                              ↓
                   advance to the next decision
```

Here's the subtlety. Those three tasks need step IDs, and the IDs must come out the same on a replay — otherwise the idempotency keys change (`R123:S4.1:E1` becomes `R123:S4.2:E1`), the `UNIQUE` constraint doesn't recognize the retry, and the duplicate protection in §7 silently stops working.

So child steps are numbered by **the order they were invoked in**, never by the order they finished:

```
Invocation order:   T1 = S4.0,  T2 = S4.1,  T3 = S4.2     ← stable, always
Completion order:   T2, then T3, then T1                   ← different every run
```

Completion order is a race. Invocation order is a property of the decision, and it's the same every time you replay it. This is what keeps effect identity stable when work runs in parallel — without it, everything in §7 quietly breaks under concurrency.

---

## 14. The whole thing, end to end

Back to the meeting assistant. Here's a clean run.

**1 — The run starts.**

```
runs:    R123, status = RUNNING, version = 0
events:  1. RUN_STARTED
```

**2 — The agent makes a decision.**

The engine asks the LLM what to do. It answers: *look up the meeting first.* That decision is written to history, so a replay will read it back instead of asking again.

**3 — A task is dispatched.**

```
BEGIN
  INSERT tasks       (T456, R123, step S1, "lookup_meeting")
  INSERT events      (2. TASK_CREATED)
  INSERT task_outbox (deliver T456)
COMMIT
        ↓
  relay publishes → queue
```

**4 — A worker claims it.**

```
Worker A:  PENDING → RUNNING  ✅   (Worker B loses the claim and drops it)
lease:     owner = Worker A, token = 10, expires 21:10
```

It heartbeats while it works, each heartbeat carrying token 10.

**5 — The task completes, and the run advances.**

```
tasks:   T456 → COMPLETED
runs:    version 20 → 21   ✅ CAS succeeded
```

The engine calls the LLM with the new information. It answers: *send the summary email.*

**6 — The side effect is recorded before it happens.**

```
BEGIN
  INSERT tasks    (T457, step S2, "send_email")
  INSERT effects  (R123:S2:E1, PENDING, IDEMPOTENT_BY_KEY)
  INSERT outbox   (deliver T457)
COMMIT
```

The intent is now durable. Nothing has been sent yet.

**7 — A worker executes it.**

```
Worker C claims T457, token = 11
effects:  R123:S2:E1 → RUNNING
          ↓
POST /send   Idempotency-Key: R123:S2:E1
          ↓
200 OK  { id: "msg_98374" }
```

**8 — The outcome is recorded.**

```
effects:  R123:S2:E1 → COMMITTED, external_ref = msg_98374
tasks:    T457 → COMPLETED
events:   ... EFFECT_COMMITTED
```

**9 — The run finishes.**

```
runs:    R123 → COMPLETED
events:  N. RUN_COMPLETED
```

One email. One record of it. A complete, immutable history of everything that happened.

---

## 15. Now let's break it

The clean path is the easy part. Here's what happens when things go wrong — the same run, interrupted at each dangerous moment.

### Crash after committing the effect, before sending

```
effects:  R123:S2:E1 = PENDING
💥
```

The lease expires. Another worker picks up T457, reads the row, sees `PENDING`, and knows with certainty that nothing was sent. It executes normally.

**Result:** one email. ✅

### Crash after sending, before recording

```
POST /send  →  200 OK, email delivered
💥 before writing COMMITTED

effects:  R123:S2:E1 = RUNNING
```

The database says `RUNNING`; reality says the email went out. Recovery marks it `UNKNOWN` and reconciles. The tool is `IDEMPOTENT_BY_KEY`, so re-sending with the same key is safe — the provider recognizes the repeat and doesn't send again. (Had it been `QUERYABLE`, we'd look it up instead and mark it `COMMITTED`.)

**Result:** one email. ✅

### A frozen worker comes back

```
Worker A freezes holding token 10
lease expires  →  Worker B takes over with token 11
Worker A thaws: "T457 is done, here's the result"  (token 10)
                 →  10 < 11  →  REJECTED
```

Worker A learns it's stale and stops. Worker B's work stands.

**Result:** state stays consistent. ✅

### Two workers both think they own the task

The worst case — the lease genuinely failed to provide exclusion.

```
Worker A:  INSERT effect R123:S2:E1  →  ✅
Worker B:  INSERT effect R123:S2:E1  →  ❌ UNIQUE violation
                                          reads the row instead
```

Only one of them proceeds to the API. And if both somehow reached the provider, both carried the same key, so the provider deduplicates too.

**Result:** one email. ✅ — and note the lease being wrong didn't matter.

### Two parallel tasks finish simultaneously

```
Worker X:  advance run, version 20 → 21  ✅
Worker Y:  advance run, version 20 → 21  ❌ 0 rows
           re-reads, sees version 21, does nothing
```

**Result:** exactly one LLM call. ✅

### The relay crashes mid-publish

```
published to queue ✅  →  💥  →  restart  →  publishes again
T457 delivered twice
```

The second delivery loses the claim at `PENDING → RUNNING`.

**Result:** executed once. ✅

### The whole runtime restarts

The engine keeps no authoritative state in memory. On restart it re-reads Postgres: which runs are in flight, which leases are still valid, which outbox rows are unpublished, which effects are unresolved. Workers mid-task keep heartbeating against a runtime that recognizes their leases again.

**Result:** picks up where it left off. ✅

Notice the pattern. In every case, the failure was **caught by a different mechanism than the one that failed**. That's the whole design.

---

## 16. What we deliberately don't promise

An honest system is clear about its edges.

**Exactly-once external effects are conditional.** They require the tool to be `IDEMPOTENT_BY_KEY` or `QUERYABLE`. For `UNRECONCILABLE` tools, a human decides. See §11.

**Agent output is not deterministic — on purpose.** We make the *past* durable. We don't make the model repeatable, and we're not trying to.

**Cancellation is cooperative.** `cancel()` stops the *next* step. It cannot interrupt an effect that's already in flight, and it certainly cannot un-send an email. Marking a row `CANCELLED` does not reach into the world and undo what happened there. The rule is simple and worth stating plainly: cancellation prevents future actions, it does not reverse past ones.

**Rollback isn't automatic.** If an action needs undoing, someone has to write the compensating action. There's no generic "undo" — the system can't know that the reverse of `send_email` is `send_correction`.

**Durability is per run.** There are no transactions spanning multiple runs.

**Agent versions are pinned.** Deploying a new version doesn't migrate runs already in progress; they finish on the version they started with.

**Postgres is the throughput ceiling.** Tasks, leases, effects, events, versions, and the outbox all live there. Eventually that's the bottleneck. That's an accepted tradeoff for having exactly one place to look when you ask "what is actually true?" — and the right response is to measure where the limit really is, not to distribute things preemptively.

---

## 17. Cheat sheet

**The mechanisms, and what each one is for:**

| # | Mechanism | Answers | Kind |
| --- | --- | --- | --- |
| 1 | **Task claim** — atomic `PENDING → RUNNING` | Who picks this up? | Safety |
| 2 | **Lease** — ownership with an expiry, kept alive by heartbeats | Who should be working? | Liveness |
| 3 | **Fencing token** — a number that only goes up | Whose writes still count? | Safety |
| 4 | **Idempotency key** — `run_id:step_id:effect_seq`, `UNIQUE` | Can this happen twice? | Safety |
| 5 | **Version CAS** — update only if unchanged | Who advances the run? | Safety |
| 6 | **Effect ledger** — with an explicit `UNKNOWN` | What actually happened out there? | Safety |
| 7 | **Transactional outbox** — state and send-intent commit together | Can these two stores disagree? | Safety |
| 8 | **Reconciliation** — ask the provider what it did | How do we resolve `UNKNOWN`? | Safety |
| 9 | **Replay** — read past decisions, generate future ones | Do we re-bill on recovery? | Safety |
| 10 | **Reaper** — reclaims expired leases | Does dead work get picked up? | Liveness |

**The rules worth memorizing:**

```
A lease is a hint, not a lock.
A timeout is not a failure.
Write it down and commit before you do it.
Repeats are cheap; lost work is not.
Postgres is the truth; JetStream is just delivery.
Position makes a stable key. Content does not.
Invocation order is stable. Completion order is a race.
The past is replayed. The future is generated.
Make the safe call the only call that compiles.
```

**And the principle underneath all of it:**

> **Don't try to prevent every failure. Make failure safe.**

You cannot guarantee that workers never crash, networks never partition, clocks never drift, messages never duplicate, or APIs never time out. Assuming otherwise is how systems break in production.

What you *can* do is put a specific, well-chosen barrier at each boundary where that mess can cause harm — and make sure no two barriers fail for the same reason.

```
Worker crashes            →  lease expires, reaper reclaims
Stale worker wakes up     →  fencing token rejects it
Task delivered twice      →  claim transition drops the duplicate
Two workers, one effect   →  UNIQUE key allows only one
Two workers, one run      →  version CAS lets one win
External result unclear   →  UNKNOWN, then reconcile
Two stores to update      →  one transaction plus an outbox
Relay republishes         →  duplicates were always expected
Process restarts          →  re-read Postgres and continue
```

That's the system. Not one perfect lock — a set of imperfect mechanisms arranged so that the things they miss don't overlap.

---

## What is built today

Layers 1, 2 and 3 are implemented and tested against PostgreSQL and NATS.
Everything this document describes about claims, leases, fencing tokens, the
effect ledger, `UNKNOWN`, reconciliation, **and the outbox (§12)** is running
code rather than a plan.

The outbox in particular is now real, which changes one sentence of §12: a task,
its event, and the intent to deliver it commit in one transaction, and a relay
publishes what committed. The window §12 warns about — a task that is durable
and will never be announced — no longer exists. Delivery itself runs over any of
three transports (an in-process channel, PostgreSQL `SKIP LOCKED`, or NATS
JetStream), and workers can live in their own processes.

Two things this document describes are still ahead:

- **Replay** (§13) is Layer 4, and arrives with the agent SDK. Today a run's
  next step is decided by walking its recorded history, which is the same
  shape, but there is no language model whose decisions need reading back yet.
- **Fan-out** (§13) is Layer 5. `StepID.Child` exists so that child numbering
  has one definition, but nothing produces parallel steps yet.

When a tool's outcome cannot be settled by any mechanism, the effect parks in
`UNKNOWN` and `veya effects` lists it. A person checks the provider and records
what they found. That is the design working, not failing: §11 is where it says
so.

For a file-by-file guide to where all of this lives, see
[code-map.md](code-map.md).

---

*See the [README](../README.md) for the schema, the API, the full tradeoff list, and the roadmap.*

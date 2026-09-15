-- 0001_init — the full schema from README section 8.1.
--
-- Every table is created now, including the ones nothing reads yet:
--
--   effects      Layer 2 — the effect ledger
--   leases       Layer 2 — ownership and fencing tokens
--   workers      Layer 3 — worker registration
--   task_outbox  Layer 3 — the transactional outbox
--
-- Creating them up front means later layers ship code rather than a schema
-- change. In particular the outbox row is written from Layer 1 onward, so
-- Layer 3 adds a relay that drains a table that is already being filled,
-- rather than introducing the dual-write window it exists to close.
--
-- One deviation from README section 8.1: identifiers are TEXT, not UUID.
-- Identity is generated behind the core.IDGen port, and pinning the column
-- type to UUID would make the database reject the deterministic identifiers
-- the simulation harness and the contract suite depend on. The production
-- generator still emits UUIDv4; the column simply does not insist on it.

-- --- enums ------------------------------------------------------------------

CREATE TYPE run_status    AS ENUM ('RUNNING','COMPLETED','FAILED','CANCELLED');
CREATE TYPE task_status   AS ENUM ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED','DEAD_LETTER');
CREATE TYPE effect_status AS ENUM ('PENDING','RUNNING','COMMITTED','FAILED','UNKNOWN');
CREATE TYPE worker_status AS ENUM ('ACTIVE','DRAINING','DEAD');

-- --- runs -------------------------------------------------------------------
-- One row per agent execution. `version` serializes advancement: it is the
-- compare half of the compare-and-swap that stops two workers finishing
-- sibling tasks from both calling the model and forking the run.

CREATE TABLE runs (
    run_id        TEXT        PRIMARY KEY,
    agent_name    TEXT        NOT NULL,
    agent_version TEXT        NOT NULL,   -- pinned for replay safety
    status        run_status  NOT NULL,
    version       BIGINT      NOT NULL DEFAULT 0,
    input         JSONB,
    output        JSONB,
    context       JSONB,                  -- working memory for next decision
    last_error    TEXT,
    deadline_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ
);

-- --- events -----------------------------------------------------------------
-- Append-only history, ordered per run. The composite primary key is what
-- makes appends conditional: a caller supplies the sequence it believes comes
-- next, so a racing retry collides rather than duplicating, and history is
-- gapless by construction.

CREATE TABLE events (
    run_id     TEXT        NOT NULL REFERENCES runs(run_id),
    seq        BIGINT      NOT NULL,
    event_type TEXT        NOT NULL,
    step_id    TEXT,
    payload    JSONB       NOT NULL,      -- versioned envelope: {"v":1,"data":…}
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, seq)
);

-- --- tasks ------------------------------------------------------------------
-- Units of work. Ownership columns are deliberately absent: from Layer 2 who
-- holds a task lives only in `leases`, because two records asserting the same
-- fact drift, and a drifted fencing token silently disables itself.

CREATE TABLE tasks (
    task_id      TEXT        PRIMARY KEY,
    run_id       TEXT        NOT NULL REFERENCES runs(run_id),
    step_id      TEXT        NOT NULL,
    parent_step  TEXT,                        -- fan-out parent (Layer 5)
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

-- --- effects ----------------------------------------------------------------
-- External side effects (Layer 2). `idempotency_key` is UNIQUE, and that
-- constraint — not application logic — is what enforces one effect per logical
-- operation. It is the real mutual-exclusion primitive of the whole design.
--
-- Foreign keys are RESTRICT by design. NEVER add ON DELETE CASCADE here: an
-- effect record archived while redelivery is still possible is a side effect
-- executed twice.

CREATE TABLE effects (
    effect_id       TEXT          PRIMARY KEY,
    task_id         TEXT          NOT NULL REFERENCES tasks(task_id) ON DELETE RESTRICT,
    run_id          TEXT          NOT NULL REFERENCES runs(run_id)   ON DELETE RESTRICT,
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

-- --- task_outbox ------------------------------------------------------------
-- The intent to deliver a task (Layer 3). Written in the same transaction as
-- the task and its event, so there is no window in which work exists but will
-- never be delivered.

CREATE TABLE task_outbox (
    outbox_id    BIGSERIAL   PRIMARY KEY,
    task_id      TEXT        NOT NULL REFERENCES tasks(task_id),
    subject      TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    published_at TIMESTAMPTZ,
    attempts     INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- --- workers and leases -----------------------------------------------------
-- Ownership and liveness (Layer 2/3). A lease is a scheduling hint with an
-- expiry, not a lock: under clock skew two workers can both believe they hold
-- one. `fencing_token` is what makes that survivable — it is strictly
-- increasing per task, and every mutating call is rejected if it presents a
-- stale one.

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
    task_id       TEXT        PRIMARY KEY REFERENCES tasks(task_id),
    worker_id     TEXT        NOT NULL REFERENCES workers(worker_id),
    fencing_token BIGINT      NOT NULL,
    acquired_at   TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    heartbeat_at  TIMESTAMPTZ NOT NULL
);

-- --- indexes ----------------------------------------------------------------

CREATE INDEX idx_tasks_dispatchable ON tasks (priority DESC, available_at, created_at)
    WHERE status = 'PENDING';
CREATE INDEX idx_tasks_run          ON tasks (run_id);
CREATE INDEX idx_tasks_open         ON tasks (run_id)
    WHERE status IN ('PENDING','RUNNING');
CREATE INDEX idx_events_run         ON events (run_id, seq);
CREATE INDEX idx_runs_active        ON runs (updated_at)
    WHERE status = 'RUNNING';
CREATE INDEX idx_effects_run        ON effects (run_id);
CREATE INDEX idx_effects_unresolved ON effects (updated_at)
    WHERE status IN ('RUNNING','UNKNOWN');
CREATE INDEX idx_leases_expiry      ON leases (expires_at);
CREATE INDEX idx_workers_heartbeat  ON workers (last_heartbeat);
CREATE INDEX idx_outbox_unpublished ON task_outbox (outbox_id)
    WHERE published_at IS NULL;

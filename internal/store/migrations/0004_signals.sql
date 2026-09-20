-- 0004_signals — something outside the system happened, recorded against a run.
--
-- The table exists because signals are stored on arrival rather than delivered
-- to a waiting run. That is not an implementation convenience: it is what
-- deletes the early-signal race, where a callback arrives before the run
-- reaches its wait, finds no waiter, and is dropped — leaving the run waiting
-- forever for something that already happened. See docs/execution-model.md
-- section 4.
--
-- A wait is therefore a read, not a rendezvous, and this is what it reads.

CREATE TABLE signals (
    run_id     TEXT        NOT NULL REFERENCES runs(run_id),
    signal_id  TEXT        NOT NULL,
    name       TEXT        NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- The dedup. signal_id comes from the sender, because only the sender
    -- knows its second call is a retry of its first. Delivery is at-least-once
    -- everywhere else in this system and signals do not get to be special: a
    -- callback retried three times must approve one refund, not three.
    PRIMARY KEY (run_id, signal_id)
);

-- The wait's query: "is there a signal for this run under this name?", oldest
-- first, because several signals may share a name on one run and a wait
-- consumes them in arrival order.
CREATE INDEX idx_signals_run_name ON signals (run_id, name, created_at);

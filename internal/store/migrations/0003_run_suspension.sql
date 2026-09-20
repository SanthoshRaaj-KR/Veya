-- 0003_run_suspension — a run can be waiting without being in flight.
--
-- Layer 5 adds runs that sleep and runs that wait on a signal. Both are
-- RUNNING with no task in flight, which is exactly the condition
-- RunsAwaitingAdvance exists to find, so without this column the recovery scan
-- would sweep up every sleeping run and advance it immediately — the opposite
-- of sleeping.
--
-- One column rather than a WAITING run state. The state looks tidier and costs
-- more: run state lives in core, in both store adapters, in the CLI's output
-- and in every transition assertion written since Layer 1, and it buys nothing
-- because a waiting run is still running. The reasoning is in full in
-- docs/execution-model.md section 2.
--
-- NULL means ready, and it has to. Every run that exists when this migration
-- runs has NULL here, and every one of them is ready.

ALTER TABLE runs ADD COLUMN available_at TIMESTAMPTZ;

-- The recovery scan's query: RUNNING runs with nothing in flight that are not
-- parked into the future.
--
-- Partial on status, because a terminal run is never a candidate and the
-- overwhelming majority of rows in a long-lived database are terminal. The
-- index carries available_at so the predicate is answered from it, including
-- for the NULL rows that are the common case — a b-tree indexes NULLs, so
-- "IS NULL OR <= now" is still an index range and does not fall back to a scan
-- of every RUNNING row.
CREATE INDEX idx_runs_available ON runs (available_at)
    WHERE status = 'RUNNING';

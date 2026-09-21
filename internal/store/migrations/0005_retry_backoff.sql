-- 0005_retry_backoff — a retry can wait before it is redelivered.
--
-- Retries have been immediate since Layer 1: FailTask re-enqueues a delivery
-- in the same transaction that requeues the task, and the relay finds it on
-- its next sweep. That is honest when there is no policy to schedule against,
-- and unkind to a downstream service once there is one — retrying it in a
-- tight loop the instant it starts failing is closer to a denial of service
-- than a recovery.
--
-- available_at on task_outbox is the same idea as available_at on runs
-- (0003_run_suspension), one level down: "not before time T", answered by a
-- column and an index rather than anything that holds a pending wake-up in
-- memory. NULL means ready, and it has to — every row written before this
-- migration, and every immediate dispatch after it, has NULL here and every
-- one of them is ready now.

ALTER TABLE task_outbox ADD COLUMN available_at TIMESTAMPTZ;

-- The relay's query gains a second predicate: unpublished AND ready. Both
-- come from this index, including the NULL rows that are the overwhelming
-- majority — a b-tree indexes NULLs, so "IS NULL OR <= now" does not fall
-- back to a scan of the partial index it already had.
DROP INDEX IF EXISTS idx_outbox_unpublished;
CREATE INDEX idx_outbox_unpublished ON task_outbox (available_at, outbox_id)
    WHERE published_at IS NULL;

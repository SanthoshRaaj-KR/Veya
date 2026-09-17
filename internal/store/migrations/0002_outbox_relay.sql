-- 0002_outbox_relay — what the relay needs that 0001 did not anticipate.
--
-- 0001 created task_outbox with the columns a producer needs. Writing the
-- consumer turned up two things it also needs:
--
--   last_error   A delivery is never abandoned — there is no attempt count at
--                which stranding a task becomes acceptable — so `attempts`
--                alone tells an operator that something is wrong without ever
--                saying what. The reason has to be on the row, because by the
--                time anyone looks the log line has rotated away.
--
--   an index     idx_outbox_unpublished covers "what is unpublished", which is
--                the relay's query. It does not cover the Postgres dispatcher's
--                query, which takes the oldest unpublished row under
--                FOR UPDATE SKIP LOCKED and therefore needs the order to come
--                from the index rather than from a sort of the whole partial
--                index.
--
-- Forward-only, per the rules in migrations.go: 0001 is not edited, even though
-- nothing had consumed the table yet, because "applied nowhere" is a claim
-- about every database that exists and no one can check it.

ALTER TABLE task_outbox ADD COLUMN last_error TEXT;

-- The relay and the Postgres dispatcher both read oldest-unpublished-first.
-- outbox_id is a BIGSERIAL, so ordering by it is ordering by insertion.
DROP INDEX IF EXISTS idx_outbox_unpublished;
CREATE INDEX idx_outbox_unpublished ON task_outbox (outbox_id)
    WHERE published_at IS NULL;

-- Finding the delivery for a given task, which is what an operator asks when a
-- run is stuck: "was this ever announced, and if so when?"
CREATE INDEX idx_outbox_task ON task_outbox (task_id);

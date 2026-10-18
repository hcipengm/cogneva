-- Migration: 061_producer_heartbeat_trigger (down)
-- The old key holds one row per producer, so the extra rows have to go before
-- the key narrows.
--
-- The row that survives is the latest delivery of that source, whichever
-- trigger wrote it: that is the row the old reader would have found, because
-- under the old key each delivery overwrote the previous one. Keeping the
-- timer's row instead would be a different history -- one the old schema never
-- held -- and a downgrade should restore the old reading, not improve it.
DELETE FROM principle_producer_pushes p
USING principle_producer_pushes newer
WHERE p.source = newer.source
  AND (
      newer.pushed_at > p.pushed_at
      OR (newer.pushed_at = p.pushed_at AND newer.trigger < p.trigger)
  );

ALTER TABLE principle_producer_pushes
    DROP CONSTRAINT IF EXISTS principle_producer_pushes_pkey;

ALTER TABLE principle_producer_pushes
    ADD PRIMARY KEY (source);

ALTER TABLE principle_producer_pushes
    DROP CONSTRAINT IF EXISTS principle_producer_pushes_trigger_check;

ALTER TABLE principle_producer_pushes
    DROP COLUMN IF EXISTS trigger;

-- Migration: 047_change_ledger_declared_priority (down)
-- Dropping the column keeps every declaration, every identity and every state,
-- and loses only the band the documents asked for. A band is recoverable from
-- the documents themselves -- it is transcribed from them rather than measured,
-- so the downgrade costs the re-reading of those documents and no identity.

ALTER TABLE change_ledger_declared
    DROP COLUMN IF EXISTS priority;

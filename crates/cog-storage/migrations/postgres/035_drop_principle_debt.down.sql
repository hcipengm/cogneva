-- Migration: 035_drop_principle_debt (down)
-- Puts the table back, empty.
--
-- The schema is restored exactly as 033 created it, and that is all that can be
-- restored: the rows were written by the loop and by nothing else, so once the
-- table is dropped there is no second copy to rebuild them from. A rollback
-- therefore returns the shape and not the series, and a process that reads it
-- after a rollback finds no passes rather than old ones -- which is the honest
-- reading, since it has not run any.
--
-- A table with no writer is the state this migration leaves, so the rolled-back
-- schema is only useful to a loop that has the old dispatcher in it again.

CREATE TABLE IF NOT EXISTS principle_debt (
    id           BIGSERIAL PRIMARY KEY,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    by_kind      TEXT NOT NULL,
    submitted    BIGINT NOT NULL DEFAULT 0,
    already      BIGINT NOT NULL DEFAULT 0,
    failed       BIGINT NOT NULL DEFAULT 0,
    over_bound   BIGINT NOT NULL DEFAULT 0,
    bound        BIGINT NOT NULL,
    bound_source VARCHAR(64) NOT NULL,
    rev          VARCHAR(64),
    delta        BIGINT
);

CREATE INDEX IF NOT EXISTS idx_principle_debt_observed_at
    ON principle_debt(observed_at);

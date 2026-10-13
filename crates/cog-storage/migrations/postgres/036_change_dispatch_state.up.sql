-- Migration: 036_change_dispatch_state
-- What each dispatch pass decided, and why it decided that much.
--
-- A dispatch pass is the account's only executor-facing half: it hands the rows
-- still owed to the goal path. On 2026-10-13 every dispatch of that day landed on
-- a pool with no usable upstream -- 61 tasks submitted inside two seconds, all
-- 61 with the same failure -- and the account could not say so. A pass that hands
-- nothing over and a pass with nothing to hand over are identical when read from
-- `change_ledger` alone: both leave the same pending rows. Holding work back
-- without recording that would repeat the incident, only quieter, which is why
-- the pause is a row here rather than an absence.
--
-- One row per pass, appended, rather than one row updated in place: the series is
-- the point -- how long dispatch was stopped, how often, and whether anything
-- moved while it was -- and an upserted row only ever answers the last question.
-- `pending` is what keeps "nothing is owed" apart from "work is owed and was not
-- handed over"; `resume_at_unix` carries the bound the pass derived from the pool
-- snapshot's own evidence, so "stopped until when" is answerable here rather than
-- by reading sixty-one task payloads; `evidenced_recovery_unix` is the upstream's
-- own statement kept beside it, so a reader can tell a bound this pass computed
-- from a claim it invented.
--
-- The two closed sets are CHECK constraints because that is what makes them
-- closed: a decision or a pool reading that can be written without a migration is
-- a value nobody agreed to, and a reader enumerating the set would learn about it
-- from an unexpected row instead.

CREATE TABLE IF NOT EXISTS change_dispatch_state (
    id                      BIGSERIAL PRIMARY KEY,
    observed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    decision                VARCHAR(32) NOT NULL,
    pool_reading            VARCHAR(16) NOT NULL,
    pending                 INTEGER NOT NULL,
    submitted               INTEGER NOT NULL,
    already                 INTEGER NOT NULL,
    failed                  INTEGER NOT NULL,
    unmeasurable            INTEGER NOT NULL,
    resume_at_unix          BIGINT NOT NULL DEFAULT 0,
    evidenced_recovery_unix BIGINT NOT NULL DEFAULT 0,
    quota_window_secs       BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT change_dispatch_state_decision_check
        CHECK (decision IN ('proceed', 'paused_llm_pool_unavailable')),
    CONSTRAINT change_dispatch_state_pool_reading_check
        CHECK (pool_reading IN ('available', 'unavailable', 'absent'))
);

CREATE INDEX IF NOT EXISTS idx_change_dispatch_state_observed_at
    ON change_dispatch_state (observed_at);

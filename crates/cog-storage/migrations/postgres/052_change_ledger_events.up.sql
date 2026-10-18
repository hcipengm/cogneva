-- Migration: 052_change_ledger_events
-- The change account's event half: one row each time a row's state moves.
--
-- Why a table at all. `change_ledger` keeps one row per identity and overwrites
-- it in place, so it answers what is true now and only that. Both of its time
-- columns are written under a rule that erases the evidence a flow reading
-- needs: `measured_rev` is replaced by every measurement, and `landed_at` is
-- stamped on the first landing and cleared the moment a later measurement stops
-- finding it. Measured on 2026-10-18: the whole account held 4448 rows across
-- three surviving revisions, 417 rows with no revision at all, and every window
-- read zero new landings -- because every surviving `landed_at` predates the
-- oldest revision still in the table. So "how many rows landed, and which ones,
-- under this revision" could not be answered from the account, and no amount of
-- reading it more often would have helped: the statement that measured those
-- revisions had already replaced what it measured.
--
-- What this table holds is the transition, not the measurement. A pass that
-- re-asks a landed row and finds it still landed writes no row here: that fact
-- is already `change_ledger.measured_at`, and copying every measurement would
-- make this table a second copy of the measurement loop -- 4448 rows per pass --
-- without answering a question the account cannot already answer. What the
-- account cannot answer is *movement*: entering, landing, falling back out of
-- landed, retiring, and the reason that came with each. Those are the rows here.
--
-- `from_state` is NULL exactly when the row entered the account: an identity
-- that appears for the first time did not come from a state, and writing its own
-- state there would make "entered" indistinguishable from "moved from X to X".
--
-- `observed_at` is the database clock in the statement that moves the row, taken
-- the same way the account's own times are: a transition stamped from the
-- caller's clock would be reporting a time it supplied rather than the time the
-- move was recorded, and the two differ by however long a pass takes.
--
-- The reasons ride along because they are part of the transition, not a property
-- of the current row: the retirement reason of a row that was later re-measured
-- out of `retired` is cleared on the account, and without it here the account
-- would have no record that the retirement ever had one -- nor which one.
--
-- No foreign key to `change_ledger`. The identity is the content of the event,
-- not a reference to a cursor, and the event does not stop being a fact about the
-- tree if the account ever stops holding the row it is about.
--
-- Widths are the account's own, taken from the narrower dialect's DDL, so an
-- identity that fits there fits here and a reader joining the two halves compares
-- the same bytes.

CREATE TABLE IF NOT EXISTS change_ledger_event (
    id               BIGSERIAL PRIMARY KEY,
    observed_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    kind             VARCHAR(32)  NOT NULL,
    scope            VARCHAR(191) NOT NULL,
    literal          VARCHAR(512) NOT NULL,
    from_state       VARCHAR(16),
    to_state         VARCHAR(16)  NOT NULL,
    measured_rev     VARCHAR(64),
    retire_reason    VARCHAR(32),
    unsettled_reason VARCHAR(32),
    CONSTRAINT change_ledger_event_to_state_check
        CHECK (to_state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired')),
    -- NULL is the one value outside the set that is allowed, and it means
    -- "entered", so it is written as the exception rather than as a sixth member.
    CONSTRAINT change_ledger_event_from_state_check
        CHECK (from_state IS NULL
               OR from_state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired'))
);

-- Two readers, two indexes. "What moved since this instant" scans the whole
-- table by time; "when did this row move" reads one identity's history.
CREATE INDEX IF NOT EXISTS idx_change_ledger_event_observed_at
    ON change_ledger_event (observed_at);

CREATE INDEX IF NOT EXISTS idx_change_ledger_event_identity
    ON change_ledger_event (kind, scope, literal, observed_at);

-- The dispatch series gains the window it was spending against. Its rows say how
-- many rows a pass handed over, and nothing beside it said what the pool had left
-- to spend -- so "200 handovers on a window that allowed four" and "200 on a
-- window that allowed four thousand" read identically. The pace is published into
-- the pool snapshot already (`llm_pool_status` carries it); this records the copy
-- each pass actually read, at the instant it read it, which the snapshot cannot:
-- it is overwritten every few seconds.
--
-- Nullable rather than NOT NULL DEFAULT 0, unlike the per-pass counts added in
-- 044: zero of these means "the window allowed nothing", which is a fact about
-- the pool, while a row written before this column existed is not a measurement
-- of it. `NULL` is the only value that says "nothing was recorded here".
ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS window_allowance_tokens BIGINT;

ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS window_spent_tokens BIGINT;

ALTER TABLE change_dispatch_state
    ADD COLUMN IF NOT EXISTS window_reserve_tokens BIGINT;

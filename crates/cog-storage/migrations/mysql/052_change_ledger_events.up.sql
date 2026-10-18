-- Migration: 052_change_ledger_events
-- The change account's event half: one row each time a row's state moves. See
-- the Postgres migration of the same name for the whole of the reasoning: the
-- account keeps one row per identity and overwrites it, so the revision and the
-- landing time of every earlier measurement are gone, and "what moved under this
-- revision" has no answer that reading the account harder could recover.
--
-- Dialect differences, both deliberate: `DATETIME(6)` keeps microseconds so two
-- transitions inside one second stay distinguishable, and the collation is
-- utf8mb4_bin because kind, scope and literal are an identity, not a label --
-- under a case-insensitive collation two identities that differ only in case
-- would read as one row's history.
--
-- No foreign key to `change_ledger`, for the same reason as on the Postgres
-- side: the event is a fact about the tree, and it does not stop being one if
-- the account ever stops holding the row it is about.

CREATE TABLE IF NOT EXISTS change_ledger_event (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    observed_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    kind             VARCHAR(32)  NOT NULL,
    scope            VARCHAR(191) NOT NULL,
    literal          VARCHAR(512) NOT NULL,
    from_state       VARCHAR(16)  NULL,
    to_state         VARCHAR(16)  NOT NULL,
    measured_rev     VARCHAR(64)  NULL,
    retire_reason    VARCHAR(32)  NULL,
    unsettled_reason VARCHAR(32)  NULL,
    PRIMARY KEY (id),
    INDEX idx_change_ledger_event_observed_at (observed_at),
    INDEX idx_change_ledger_event_identity (kind, scope, literal, observed_at),
    CONSTRAINT change_ledger_event_to_state_check
        CHECK (to_state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired')),
    CONSTRAINT change_ledger_event_from_state_check
        CHECK (from_state IS NULL
               OR from_state IN ('conflict', 'pending', 'partial', 'undecided', 'landed', 'retired'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- The dispatch series gains the window each pass was spending against, so a
-- batch can be compared with the quota it was drawn from. See the Postgres file
-- for why these are nullable: zero means "the window allowed nothing", which is
-- a fact about the pool, while a row written before the column existed is not a
-- measurement of it.
--
-- No `IF NOT EXISTS` here, as on the other MySQL files: a second run fails on the
-- duplicate column and names it rather than passing for a migration that did
-- nothing.
ALTER TABLE change_dispatch_state
    ADD COLUMN window_allowance_tokens BIGINT NULL;

ALTER TABLE change_dispatch_state
    ADD COLUMN window_spent_tokens BIGINT NULL;

ALTER TABLE change_dispatch_state
    ADD COLUMN window_reserve_tokens BIGINT NULL;

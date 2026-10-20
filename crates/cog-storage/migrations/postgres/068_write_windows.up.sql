-- Migration: 068_write_windows
-- Who is allowed to write, who said so, and for how long.
--
-- A change to a running system is only as reversible as the record of who
-- approved it. Until now that record was a document: a person's name in a
-- paragraph, with no time window and nothing the live system could read. The
-- face that owes the reading "how many write windows are approved right now"
-- had nowhere to get it from, so the reading was absent -- and an absent
-- reading on this face is the difference between "no one is writing" and "no
-- one is keeping track".
--
-- One row per requested window. The row is the request from the moment it is
-- inserted, and becomes an approved window when `approved_at` is written. The
-- columns are deliberately separate rather than one status string: a window
-- that was approved and later revoked is two facts, and a single column would
-- have to forget the first to record the second.
--
-- Approved, for every reader, is: `approved_at IS NOT NULL AND revoked_at IS
-- NULL AND expires_at > now() AND approver <> writer`. A window with no expiry
-- is not approved -- it is a request that nobody bounded. A window whose
-- approver is empty is not approved -- an unnamed approval is not an approval.
-- A row whose approver is its own writer is never approved, however it was
-- stamped: self-approval cannot be what the reading counts.
--
-- Rows are never deleted. `revoked_at` ends a window and the row stays as the
-- record of the window that existed; a table that forgot revoked rows could not
-- answer who wrote under whose approval, which is the question a revert asks.
CREATE TABLE IF NOT EXISTS write_windows (
    window_id TEXT PRIMARY KEY,
    -- The path the window covers, as the approver wrote it. Compared for
    -- overlap as a string: two windows overlap when they name the same scope,
    -- and telling prefixes apart is a question the reading does not ask.
    path_scope TEXT NOT NULL,
    -- The account that asked to write, named as the operator names it.
    writer TEXT NOT NULL,
    -- The account that approved. Empty until someone does.
    approver TEXT NOT NULL DEFAULT '',
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL means "asked, not approved" -- not "approved at the epoch".
    approved_at TIMESTAMPTZ,
    -- Required for a window to count as approved: an open-ended approval is a
    -- permission nobody bounded, and it is refused by the reader rather than
    -- by a constraint, so a row that carries it can still be read and named.
    expires_at TIMESTAMPTZ,
    -- Set when the window is withdrawn before its expiry. Kept beside
    -- `approved_at` rather than replacing it.
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_write_windows_path_scope
    ON write_windows (path_scope);

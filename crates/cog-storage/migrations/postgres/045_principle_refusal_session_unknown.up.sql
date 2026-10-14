-- Migration: 045_principle_refusal_session_unknown
-- The session column's third state: the row's session was never recorded.
--
-- 043 gave this table a session, and every row written before it took the new
-- column's default. That default is `''`, and `''` already means something on
-- this column: a side with no session to name. The platform's own gate writes it
-- for exactly that reason (see `REPORT_REFUSAL_SQL`, which sets the column to
-- the empty string because its refusals happen in a process that belongs to no
-- session), and the host gate writes it when the payload it is judging names
-- none. So the column has read two ways since 043 -- "there was no session" and
-- "nobody looked" -- and a reader taking the blanks as the first meaning counts
-- rows that were never asked the question.
--
-- Measured 2026-10-14 on the live table: 64 rows, of which 4 name a session, 1
-- is a refusal both sides agree had no session to name, and 59 have no session
-- recorded. The 59 are two vintages of that same gap, and each carries its own
-- in-row fingerprint:
--
--   * 30 rows hold a real `briefed` value beside the blank session. The briefing
--     lookup answers `unknown` when the payload names no session -- the gate's
--     own guard ties the two columns together -- so a row that names no session
--     and still holds a real briefing state is one the gate wrote while it had a
--     session, which the record then did not keep. Neither writer can produce
--     that pair: the six-column gate cannot, because of the guard, and the
--     four-column spool line that produced these rows carried the briefing state
--     in its fourth field but had no session field at all.
--
--   * 29 rows sit before 037, the migration that added `briefed`. Their spool
--     line named neither a briefing nor a session, so both columns took their
--     defaults and nothing about the payload survives in the row. The boundary
--     is the ledger's own record of when 037 arrived, which is a measurement the
--     environment holds rather than a literal written here -- and it is exact on
--     this install: the last such row is 1h28m before that instant and the next
--     one is 11h33m after it, so no row lies near the boundary.
--
-- The third state is `-`, this repository's existing marker for a cell that does
-- not apply to the row (`HOST_REGISTRY_READ_NO_GATE` is the same value for the
-- same reason). Only this migration writes it: neither live path has a row whose
-- session is unknown, because both know at the moment they write -- one from the
-- payload, the other because it has none by construction.
--
-- Checked before writing, on the live table: the move cannot collide on the
-- primary key. The key is (slug, refused_at, source, session_id); the rows
-- moving are already distinct on its first three columns (0 groups of more than
-- one), and no row holds `-` today, so every moved row lands on a key nothing
-- occupies. Re-running this migration moves nothing: no row it moved still has
-- session_id = ''.
--
-- If 037's ledger row is missing -- the column applied by hand rather than
-- through the migrator -- the second branch matches nothing and only the 30
-- fingerprinted rows move. The reading is then incomplete rather than wrong,
-- which is the direction this is meant to fail in.
--
-- Existing rows' sessions are gone and are not recovered here. A session that
-- was never written down cannot be reconstructed from a row that does not hold
-- it, and inventing one would be worse than saying which rows are missing it.

UPDATE principle_refusals
SET session_id = '-'
WHERE session_id = ''
  AND (
        briefed <> 'unknown'
     OR refused_at < (SELECT applied_at FROM schema_migrations WHERE version = 37)
  );

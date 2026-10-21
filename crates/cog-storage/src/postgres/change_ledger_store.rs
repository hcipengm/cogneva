//!PostgreSQL-backed store for the change ledger.
//!
//!One writer, and that writer is a lease-elected loop. The ledger's whole value
//!is that there is *one* account, so the write path here is built for a single
//!caller rather than arbitrating between several: what a second concurrent
//!writer would produce is two versions of the same account and no way to say
//!which one is the ledger.
//!
//!The table is created by the storage migrations, not by an `init_schema` on
//!this type. Its columns are the contract's own key -- kind, scope, literal --
//!and a second CREATE TABLE here would be a second definition of that key: the
//!two would drift, and the one that wins is whichever ran last.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{
    BlockCensus, BlockingSurface, ChangeBlock, ChangeIdentity, ChangeLedgerRow, ChangeLedgerStore,
    ChangeMeasurement, ChangePriority, ChangeReading, ChangeReopen, ChangeSource, ChangeState,
    DemandClosure, DispatchState, RetireReason, SFError, SFResult, WriteWindow,
};

/// Every read returns these, in one place so no two readers can come back with
/// differently ordered columns.
const ROW_COLUMNS: &str = "kind, scope, literal, state, title, assertion, sources, \
                           first_seen_at, measured_at, measured_rev, delivered_rev, landed_at, \
                           unsettled_reason, retire_reason, retired_at";

/// Every write-window column the reading needs, in one place so the query and
/// the row it is read into cannot name different fields.
///
/// Unordered on purpose: the reduction to counts happens in the caller, which
/// needs no order, and an `ORDER BY` here would be a promise the reading never
/// reads.
const WRITE_WINDOW_COLUMNS_SQL: &str = "SELECT window_id, path_scope, writer, approver, \
     requested_at, approved_at, expires_at, revoked_at FROM write_windows";

/// How wide each keyed identity part may be, taken from the columns that key
/// them rather than from what a caller would like to write.
///
/// The bounds are the narrower dialect's (MySQL cannot key an unbounded TEXT
/// column), so one set governs both. Widths are counted in characters, which is
/// what both dialects' VARCHAR lengths mean.
const KIND_MAX: usize = 32;
const SCOPE_MAX: usize = 191;
const LITERAL_MAX: usize = 512;
const DOC_MAX: usize = 512;

/// PostgreSQL change ledger store.
#[derive(Clone)]
pub struct PostgresChangeLedgerStore {
    pool: PgPool,
}

impl PostgresChangeLedgerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// A row as callers see it, or why it cannot be one.
    ///
    /// An unreadable row fails the read instead of being skipped. A dropped row
    /// is indistinguishable from one that is not outstanding, and the reader
    /// acts on that difference: it would stop measuring a change that is still
    /// owed, and the ledger would look like progress.
    fn row_to_entry(row: &sqlx::postgres::PgRow) -> Result<ChangeLedgerRow, String> {
        let state: String = row.try_get("state").map_err(|e| format!("state: {e}"))?;
        let state = state
            .parse::<ChangeState>()
            .map_err(|e| format!("state column holds {e}"))?;
        let sources: serde_json::Value = row
            .try_get("sources")
            .map_err(|e| format!("sources: {e}"))?;
        let sources: Vec<ChangeSource> = serde_json::from_value(sources)
            .map_err(|e| format!("sources column unparsable: {e}"))?;
        // The reason is parsed from its name the same way the state is: a value
        // the closed set does not hold fails the read rather than being skipped,
        // because a retirement whose reason could not be read would be a deletion
        // again -- the row would be out of the outstanding set with nothing to
        // say why.
        let retire_reason: Option<String> = row
            .try_get("retire_reason")
            .map_err(|e| format!("retire_reason: {e}"))?;
        let retire_reason = retire_reason
            .map(|name| {
                name.parse::<RetireReason>()
                    .map_err(|e| format!("retire_reason column holds {e}"))
            })
            .transpose()?;
        // The unsettled reason is read as it stands rather than parsed into a
        // closed set: the vocabulary is the judge's, which this crate does not
        // depend on, and the column's own constraint already refuses a value
        // outside the set, so there is nothing here for a parse to catch that the
        // write did not.
        let unsettled_reason: Option<String> = row
            .try_get("unsettled_reason")
            .map_err(|e| format!("unsettled_reason: {e}"))?;
        Ok(ChangeLedgerRow {
            identity: ChangeIdentity::new(
                row.try_get::<String, _>("kind")
                    .map_err(|e| format!("kind: {e}"))?,
                row.try_get::<String, _>("scope")
                    .map_err(|e| format!("scope: {e}"))?,
                row.try_get::<String, _>("literal")
                    .map_err(|e| format!("literal: {e}"))?,
            ),
            state,
            title: row.try_get("title").map_err(|e| format!("title: {e}"))?,
            assertion: row
                .try_get("assertion")
                .map_err(|e| format!("assertion: {e}"))?,
            sources,
            first_seen_at: row
                .try_get("first_seen_at")
                .map_err(|e| format!("first_seen_at: {e}"))?,
            measured_at: row
                .try_get("measured_at")
                .map_err(|e| format!("measured_at: {e}"))?,
            measured_rev: row
                .try_get("measured_rev")
                .map_err(|e| format!("measured_rev: {e}"))?,
            delivered_rev: row
                .try_get("delivered_rev")
                .map_err(|e| format!("delivered_rev: {e}"))?,
            landed_at: row
                .try_get("landed_at")
                .map_err(|e| format!("landed_at: {e}"))?,
            unsettled_reason,
            retire_reason,
            retired_at: row
                .try_get("retired_at")
                .map_err(|e| format!("retired_at: {e}"))?,
        })
    }

    fn entries(
        rows: Result<Vec<sqlx::postgres::PgRow>, sqlx::Error>,
    ) -> SFResult<Vec<ChangeLedgerRow>> {
        let rows = rows.map_err(|e| SFError::Database(e.to_string()))?;
        rows.iter()
            .map(|row| Self::row_to_entry(row).map_err(SFError::Database))
            .collect()
    }

    /// A queued declaration as the reading it will be folded as.
    ///
    /// A row that cannot be read into a reading fails the fold rather than being
    /// skipped: a dropped declaration is indistinguishable from one that was
    /// never made, and the ledger would look caught up while a change it was
    /// told about sits queued forever.
    fn row_to_reading(row: &sqlx::postgres::PgRow) -> Result<ChangeReading, String> {
        Ok(ChangeReading {
            identity: ChangeIdentity::new(
                row.try_get::<String, _>("kind")
                    .map_err(|e| format!("kind: {e}"))?,
                row.try_get::<String, _>("scope")
                    .map_err(|e| format!("scope: {e}"))?,
                row.try_get::<String, _>("literal")
                    .map_err(|e| format!("literal: {e}"))?,
            ),
            title: row
                .try_get::<Option<String>, _>("title")
                .map_err(|e| format!("title: {e}"))?,
            assertion: row
                .try_get::<Option<String>, _>("assertion")
                .map_err(|e| format!("assertion: {e}"))?,
            source: ChangeSource {
                doc: row
                    .try_get::<String, _>("doc")
                    .map_err(|e| format!("doc: {e}"))?,
                verdict: row
                    .try_get::<Option<String>, _>("verdict")
                    .map_err(|e| format!("verdict: {e}"))?,
                priority: row
                    .try_get::<String, _>("priority")
                    .map_err(|e| format!("priority: {e}"))?
                    .parse()
                    .map_err(|e| format!("priority: {e}"))?,
                block: row
                    .try_get::<String, _>("block")
                    .map_err(|e| format!("block: {e}"))?
                    .parse()
                    .map_err(|e| format!("block: {e}"))?,
            },
        })
    }
}

/// Add a reading's source to a row that may already cite other documents.
///
/// A document cited once keeps its entry -- one entry per document, rebuilt when
/// what the document says about the change has moved. The rule used to be that an
/// existing entry was left exactly as it was, on the grounds that a document
/// whose verdict changed is applied by withdrawing its declaration and reading it
/// again. The withdrawal is real and works on the intake; what it does not reach
/// is this row. Withdrawing and re-declaring writes the corrected declaration,
/// the fold records it, and this clause then found the document already cited and
/// kept the entry it had, so the row went on citing the verdict the document made
/// first -- and the disagreement between a document's claim and a measurement,
/// which is the reading the account exists to produce, was reported against a
/// claim the document no longer made. Where the entry is keyed by the document,
/// "what this document said" has to be what it says now.
///
/// The rewrite is guarded twice: an entry is added for a document not yet cited,
/// and an entry already there is rebuilt only when what it says about the change
/// actually differs -- its verdict or the band it asked for. Without the second
/// guard every re-reading of an unchanged document would rewrite the row, which
/// is a write per citation per pass for no reading. Rebuilding rather than
/// appending keeps one entry per document: two entries for one document would be
/// the same merge the guard exists to avoid, in the other direction.
///
/// The band is part of what the entry says for the same reason the verdict is:
/// re-prioritising a change is a withdrawal and a fresh declaration, which lands
/// here as the same document citing the same identity with a different band, and
/// a clause that kept the old entry would leave the row reading at the urgency
/// the document no longer asks for. There is no default band anywhere in this
/// path -- a declaration that states none is stored as `unstated`, which is a
/// band a reader can see rather than a value it has to guess at.
///
/// `first_seen_rev` is written only on the row's first entry and never refreshed:
/// the conflict clause leaves it alone, so a second document citing an existing
/// change does not move the revision that change entered at. It is the tree the
/// row entered at, not the tree this reading was made against.
///
/// A new row enters as `pending`, not as `undecided`. `undecided` is not "nobody
/// has looked yet": it is the state a measurement writes when a check could not
/// be settled for a reason that is a property of the row, and it is coupled to
/// that reason -- there is no way to build a reason-less `undecided`
/// measurement, and a row carrying one reads as a stuck row whose reason was
/// lost. A declaration nobody has measured yet is neither: it is work still
/// owed, which is what `pending` says, and `measured_at IS NULL` is what tells a
/// reader it has not been looked at. Writing `undecided` here would state a
/// verdict no look reached and hide the row in the reason-less bucket reserved
/// for rows that predate the reason column.
///
/// The parameters are, in order: kind, scope, literal, title, assertion, doc,
/// verdict, band, face, entry revision. The tenth is the last *column* but not
/// the last parameter -- it takes the value slot after `sources`, whose
/// expression needs the band and the face before it. That gap is where a bind
/// can be written down in the wrong place and still compile: binding the band
/// after the revision made the band column hold a revision, and nothing but a
/// live database reading the row back could tell. The parameter list is written
/// here, next to the statement, because the numbering is not the column order.
const RECORD_READING_SQL: &str = "\
INSERT INTO change_ledger (kind, scope, literal, state, title, assertion, sources, first_seen_rev)
VALUES ($1, $2, $3, 'pending', $4, $5, jsonb_build_array(jsonb_build_object('doc', $6, 'verdict', $7, 'priority', $8, 'block', $9)), $10)
ON CONFLICT (kind, scope, literal) DO UPDATE SET
    title = COALESCE(EXCLUDED.title, change_ledger.title),
    assertion = COALESCE(EXCLUDED.assertion, change_ledger.assertion),
    sources = CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements(change_ledger.sources) AS cited(entry)
            WHERE cited.entry->>'doc' = $6
        ) THEN change_ledger.sources || EXCLUDED.sources
        WHEN EXISTS (
            SELECT 1 FROM jsonb_array_elements(change_ledger.sources) AS cited(entry)
            WHERE cited.entry->>'doc' = $6
              AND (cited.entry->>'verdict' IS DISTINCT FROM $7
                   OR cited.entry->>'priority' IS DISTINCT FROM $8
                   OR COALESCE(cited.entry->>'block', 'none') IS DISTINCT FROM $9)
        ) THEN (
            SELECT jsonb_agg(
                CASE
                    WHEN cited.entry->>'doc' = $6
                    THEN jsonb_build_object('doc', $6, 'verdict', $7, 'priority', $8, 'block', $9)
                    ELSE cited.entry
                END
            )
            FROM jsonb_array_elements(change_ledger.sources) AS cited(entry)
        )
        ELSE change_ledger.sources
    END";

/// Write one measurement, with every time taken from the database clock.
///
/// `landed_at` is kept from the first measurement that found it landed and
/// cleared when a later one does not, so a row that stopped being satisfied
/// reads as outstanding rather than as done-and-forgotten. `retired_at` and
/// `retire_reason` follow the same rule: a retirement is what the current
/// measurement found, so a later measurement of any other state clears both, and
/// a row that is re-examined back out of `retired` does not keep a reason beside
/// a state it no longer describes. The times come from `now()` in the same
/// statement that writes the state: taken from the caller instead, a completion
/// time would be a value the measured side supplies.
///
/// Both reasons are written exactly as the measurement carries them -- `NULL` for
/// every state but the one each belongs to -- so the row and the state agree by
/// construction, the same way `landed` agrees with `landed_at`. Every state
/// except `retired` clears `retire_reason`, and every state except `undecided`
/// clears `unsettled_reason`: a reason left beside a state it does not describe
/// would read as one, so a row re-measured out of `undecided` does not keep the
/// reason beside a state that is no longer undecided.
///
/// The revision is the caller's, and it takes the caller's `NULL` with it: a
/// measurement that named no revision must clear whatever revision was there
/// rather than leave a stale one beside a fresh state, which would read as a
/// state measured against a tree it was never measured against.
///
/// `first_seen_rev` is the one revision this statement does *not* overwrite: it
/// is the tree the row entered at, stamped once and left, the same rule
/// `first_seen_at` follows. Only a measurement that *creates* the row sets it --
/// such a row entered by being measured, so its entry revision is this
/// measurement's. A row that already existed, whether folded in from a reading or
/// measured before, keeps the revision it entered at, which is why the conflict
/// clause leaves the column alone.
///
/// One statement writes both halves. The account keeps the current state by
/// overwriting it, so the transition has to be captured in the same statement
/// that overwrites it -- a second statement could fail on its own and leave the
/// account moved with no event saying it moved, which is the exact reading this
/// table exists to make impossible. The `prior` branch reads the account at the
/// statement's snapshot, i.e. the state the row had *before* this measurement,
/// and the event is written only when the state actually differs: a
/// re-measurement that finds the same state is already `measured_at`, and writing
/// it here would make this table a copy of the measurement loop.
///
/// A row this statement creates has no prior state, so `from_state` is NULL and
/// the event reads as "entered". That is also why the comparison is
/// `IS DISTINCT FROM`: with plain `<>` a first measurement would compare against
/// NULL, match nothing, and the row would enter the account silently.
const APPLY_MEASUREMENT_SQL: &str = "\
WITH prior AS (
    SELECT state AS before FROM change_ledger
     WHERE kind = $1 AND scope = $2 AND literal = $3
), applied AS (
    INSERT INTO change_ledger (
        kind, scope, literal, state, measured_rev, retire_reason, unsettled_reason,
        first_seen_rev,
        measured_at, landed_at, retired_at
    )
    VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $5,
        now(),
        CASE WHEN $4 = 'landed' THEN now() ELSE NULL END,
        CASE WHEN $4 = 'retired' THEN now() ELSE NULL END
    )
    ON CONFLICT (kind, scope, literal) DO UPDATE SET
        state = EXCLUDED.state,
        measured_rev = EXCLUDED.measured_rev,
        retire_reason = EXCLUDED.retire_reason,
        unsettled_reason = EXCLUDED.unsettled_reason,
        measured_at = now(),
        landed_at = CASE
            WHEN EXCLUDED.state = 'landed' THEN COALESCE(change_ledger.landed_at, now())
            ELSE NULL
        END,
        retired_at = CASE
            WHEN EXCLUDED.state = 'retired' THEN COALESCE(change_ledger.retired_at, now())
            ELSE NULL
        END
    RETURNING state, measured_rev, retire_reason, unsettled_reason
), moved AS (
    INSERT INTO change_ledger_event (
        kind, scope, literal, from_state, to_state, measured_rev,
        retire_reason, unsettled_reason
    )
    SELECT $1::varchar, $2::varchar, $3::varchar,
           prior.before, applied.state, applied.measured_rev,
           applied.retire_reason, applied.unsettled_reason
      FROM applied LEFT JOIN prior ON TRUE
     WHERE prior.before IS DISTINCT FROM applied.state
)
SELECT 1";

/// Count the demands the tree has answered against the ones still owed.
///
/// One declaration per row of the intake, matched to the account on the identity
/// it names. `LEFT JOIN` and `IS DISTINCT FROM 'landed'` rather than `l.state <>
/// 'landed'`: a declaration whose change never entered the account has no state,
/// and a plain comparison against NULL matches nothing -- which would file the
/// most-owed demand there is as neither owed nor satisfied and quietly lose it
/// from both halves of the reading.
const DEMAND_CLOSURE_SQL: &str = "\
SELECT count(*) FILTER (WHERE l.state = 'landed') AS satisfied, \
       count(*) FILTER (WHERE l.state IS DISTINCT FROM 'landed') AS owed \
  FROM change_ledger_declared d \
  LEFT JOIN change_ledger l \
    ON (l.kind, l.scope, l.literal) = (d.kind, d.scope, d.literal)";

/// What the account is holding in front of the rest of itself.
///
/// A holder is a change a commit delivered while the account still owes it: the
/// line is carrying a claim the tree has not confirmed. What it holds is every
/// other row measured on that same revision -- those measurements stand on a
/// tree that contains a change nobody has confirmed, so the honest description
/// of them is "not settled yet", and the holder is the row to look at first.
/// This is the only blocking relation the account can measure about itself:
/// which gates are red and who is waiting on a build are held by processes
/// outside it, while delivery and measurement are its own facts.
///
/// `DISTINCT` on the held rows, because a row measured at a revision that two
/// holders both delivered is one row behind two changes, not two rows.
///
/// `block_conflicts` is the ambiguity the folded `block` column cannot state:
/// it folds both "nobody named a face" and "two documents named different
/// faces" to `none`, and a reader that saw only the column would read a
/// disagreement as silence.
const BLOCKING_SURFACE_SQL: &str = "\
WITH holder AS ( \
    SELECT kind, scope, literal, delivered_rev FROM change_ledger \
     WHERE delivered_rev IS NOT NULL AND state NOT IN ('landed', 'retired') \
), held AS ( \
    SELECT DISTINCT l.kind, l.scope, l.literal \
      FROM change_ledger l JOIN holder h ON l.measured_rev = h.delivered_rev \
     WHERE (l.kind, l.scope, l.literal) IS DISTINCT FROM (h.kind, h.scope, h.literal) \
) \
SELECT (SELECT count(*) FROM holder) AS holders, \
       (SELECT count(*) FROM held) AS held_landings, \
       (SELECT count(*) FROM change_ledger l \
         WHERE l.state NOT IN ('landed', 'retired') \
           AND (SELECT count(DISTINCT entry->>'block') \
                  FROM jsonb_array_elements(l.sources) AS entry \
                 WHERE entry->>'block' IN ('token', 'resources', 'principles', \
                                           'change', 'persistence', 'communication')) > 1) \
           AS block_conflicts";

/// The owed set and the landed set, grouped by the face the documents named.
///
/// Terminal states other than `landed` are left out for the reason the account's
/// own census leaves them out: a retired row's question is answered, and
/// counting it as owed would make every finished face look like it still owes
/// work forever.
///
/// The age is `COALESCE(measured_at, first_seen_at)` rather than `measured_at`
/// alone. A row that has never been measured is not ageless -- it has been
/// waiting since it entered -- and taking `NULL` as the answer would let a face
/// full of never-measured rows report no wait at all.
const BLOCK_CENSUS_SQL: &str = "\
SELECT block, \
       CASE WHEN state = 'landed' THEN 'landed' ELSE 'owed' END AS state, \
       count(*) AS n, \
       max(EXTRACT(EPOCH FROM (now() - COALESCE(measured_at, first_seen_at)))) \
         AS oldest_unmeasured_secs \
  FROM change_ledger \
 WHERE state <> 'retired' \
 GROUP BY 1, 2";

/// Append one reopen: a row the account had as done whose check no longer holds.
///
/// A plain insert with no conflict clause, like every other observation kept
/// here: the identity is not this table's key, because a row reopened twice was
/// two facts about the tree, and an upserted row could only ever answer the last
/// one. The time is the database's own, from the column default, so no caller
/// supplies the moment it is recorded.
const RECORD_REOPEN_SQL: &str = "\
INSERT INTO change_reopen (kind, scope, literal, was_landed_at, measured_rev) \
VALUES ($1, $2, $3, $4, $5)";

/// Stamp the commit that delivered a row's change, and only while nothing has.
///
/// The `delivered_rev IS NULL` guard is what makes the first sighting win: the
/// log is read newest first, and a change landed twice would otherwise have its
/// delivery moved to the later commit on every pass -- a reading that changes
/// while the fact it reports does not.
///
/// A row that does not exist matches nothing, which is the right answer: the
/// delivery arrives after the change entered the account, and a trailer whose
/// row is not there is a commit for a change this ledger was never told about.
const RECORD_DELIVERY_SQL: &str = "\
UPDATE change_ledger SET delivered_rev = $4
 WHERE kind = $1 AND scope = $2 AND literal = $3 AND delivered_rev IS NULL";

/// Append one dispatch pass's own reading.
///
/// Plain insert, no conflict clause: the row is an observation of a moment, and
/// a second pass is a second row. `observed_at` comes from the database clock in
/// the same statement, for the same reason a measurement's times do -- a pass
/// that stamped its own clock would be reporting a time it supplied.
const RECORD_DISPATCH_STATE_SQL: &str = "\
INSERT INTO change_dispatch_state (
    decision, pool_reading, pending, submitted, already, failed,
    dead_environment, held_back, rearmed, unmeasurable,
    resume_at_unix, evidenced_recovery_unix, quota_window_secs, sources_unread,
    window_allowance_tokens, window_spent_tokens, window_reserve_tokens, window_start_unix,
    delivered
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)";

/// Refuse a reading whose identity or document does not fit its column.
///
/// Checked here rather than left to the database because the dialects disagree
/// about an over-long value: Postgres raises, and MySQL shortens it. A key that
/// was silently shortened would land on a different change's row, and the
/// collision is invisible in the stored row -- the two would simply be one row,
/// and one of them would never be measured.
fn check_fits(what: &str, value: &str, max: usize) -> Result<(), String> {
    let width = value.chars().count();
    if width > max {
        return Err(format!(
            "{what} is {width} characters, over the {max} the column holds"
        ));
    }
    Ok(())
}

/// Every reading is checked before it is written, wherever it came from, so a
/// queued declaration and a direct write are held to the same width.
fn check_reading(reading: &ChangeReading) -> Result<(), String> {
    check_fits("kind", &reading.identity.kind, KIND_MAX)?;
    check_fits("scope", &reading.identity.scope, SCOPE_MAX)?;
    check_fits("literal", &reading.identity.literal, LITERAL_MAX)?;
    check_fits("source document", &reading.source.doc, DOC_MAX)
}

/// Refuse a measurement whose state and reason do not agree.
///
/// Two invariants: a row is retired if and only if it carries a `retire_reason`,
/// and a row is undecided if and only if it carries an `unsettled_reason`.
/// [`ChangeMeasurement::retired`] and [`ChangeMeasurement::undecided`] make a
/// reason-less retirement and a reason-less undecided row unbuildable, so the half
/// this catches is the other direction -- a reason left on a measurement of some
/// other state, which would write a row that is not in that state while holding a
/// reason it is not. Checked here as well as constrained in the schema because the
/// dialects disagree: Postgres can hold the coupling in a CHECK, MySQL 5.7 cannot,
/// and a rule that only one dialect enforces is a rule the store cannot rely on.
fn check_measurement(m: &ChangeMeasurement) -> Result<(), String> {
    match (m.state, m.retire_reason) {
        (ChangeState::Retired, None) => {
            return Err(
                "a retired measurement carries no reason, which would be a deletion \
                 wearing a state's clothes"
                    .to_string(),
            );
        }
        (state, Some(reason)) if state != ChangeState::Retired => {
            return Err(format!(
                "a {} measurement carries the retire reason {}, which belongs only to a \
                 retired row",
                state.as_str(),
                reason.as_str()
            ));
        }
        _ => {}
    }
    match (m.state, m.unsettled_reason.as_deref()) {
        (ChangeState::Undecided, None) => Err(
            "an undecided measurement carries no reason, which would read as answered".to_string(),
        ),
        (state, Some(reason)) if state != ChangeState::Undecided => Err(format!(
            "a {} measurement carries the unsettled reason {reason}, which belongs only to \
             an undecided row",
            state.as_str()
        )),
        _ => Ok(()),
    }
}

/// Queue a declaration, or leave the one already queued for this change and
/// document exactly as it is.
///
/// `DO NOTHING` rather than an update: a declaration is one reading of one
/// document, so a document that wants to say something different about a change
/// withdraws its declaration and makes a new one. Updating in place would make
/// "what this document asked for" a value that changes without the document
/// changing, which is the same merge the reading path refuses.
///
/// The withdrawal that rule depends on is real and lives on the producer side --
/// it deletes exactly the keys it is handed, so re-declaring is a fresh insert
/// rather than a conflict this clause has to resolve. What the account kept doing
/// was narrower and is fixed where it happened: the corrected declaration reached
/// the ledger and the ledger went on citing the verdict the document made first,
/// because the reading path kept a cited document's entry as it was. See
/// [`RECORD_READING_SQL`].
const DECLARE_SQL: &str = "\
INSERT INTO change_ledger_declared (kind, scope, literal, doc, title, assertion, verdict, priority, block)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (kind, scope, literal, doc) DO NOTHING";

/// The declarations one fold takes, oldest first.
///
/// The identity parts break ties on `declared_at` because a batch queued in one
/// statement shares a timestamp, and without the tiebreak which of them a
/// half-full fold takes would follow whatever order the scan returned -- a
/// different set on the next run, and a different one again on the other
/// dialect.
const FOLD_SELECT_SQL: &str = "\
SELECT kind, scope, literal, doc, title, assertion, verdict, priority, block
FROM change_ledger_declared
WHERE folded_at IS NULL
ORDER BY declared_at ASC, kind ASC, scope ASC, literal ASC, doc ASC
LIMIT $1";

/// Mark one declaration folded, and only if it is still unfolded.
///
/// The `folded_at IS NULL` guard is what keeps the stamp a statement about this
/// fold rather than an overwrite: it can be run again without moving a time that
/// an earlier fold set.
const FOLD_MARK_SQL: &str = "\
UPDATE change_ledger_declared SET folded_at = now()
WHERE folded_at IS NULL AND kind = $1 AND scope = $2 AND literal = $3 AND doc = $4";

const PENDING_SQL: &str =
    "SELECT count(*) AS n FROM change_ledger_declared WHERE folded_at IS NULL";

/// The retirements one fold takes, oldest first.
///
/// The identity breaks ties on `declared_at` for the same reason the declaration
/// fold does: a batch written in one statement shares a timestamp, and without the
/// tiebreak which of them a half-full fold takes would follow the scan order,
/// which differs between runs and between dialects.
const FOLD_RETIRE_SELECT_SQL: &str = "\
SELECT kind, scope, literal, reason
FROM change_ledger_retired
WHERE folded_at IS NULL
ORDER BY declared_at ASC, kind ASC, scope ASC, literal ASC
LIMIT $1";

/// Mark one retirement folded, and only if it is still unfolded. The guard makes
/// the stamp a statement about this fold rather than an overwrite, so a re-run
/// does not move a time an earlier fold set -- the same rule the declaration
/// fold's mark follows.
const FOLD_RETIRE_MARK_SQL: &str = "\
UPDATE change_ledger_retired SET folded_at = now()
WHERE folded_at IS NULL AND kind = $1 AND scope = $2 AND literal = $3";

const PENDING_RETIRE_SQL: &str =
    "SELECT count(*) AS n FROM change_ledger_retired WHERE folded_at IS NULL";

#[async_trait]
impl ChangeLedgerStore for PostgresChangeLedgerStore {
    async fn record_readings(
        &self,
        readings: &[ChangeReading],
        first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        let mut stored = 0usize;
        for reading in readings {
            check_reading(reading).map_err(SFError::Validation)?;
            sqlx::query(RECORD_READING_SQL)
                .bind(&reading.identity.kind)
                .bind(&reading.identity.scope)
                .bind(&reading.identity.literal)
                .bind(&reading.title)
                .bind(&reading.assertion)
                .bind(&reading.source.doc)
                .bind(&reading.source.verdict)
                .bind(reading.source.priority.as_str())
                .bind(reading.source.block.as_str())
                .bind(first_seen_rev)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!(
                        "record reading for {}: {e}",
                        reading.identity.as_key()
                    ))
                })?;
            stored += 1;
        }
        Ok(stored)
    }

    async fn declare(&self, readings: &[ChangeReading]) -> SFResult<usize> {
        let mut queued = 0usize;
        for reading in readings {
            check_reading(reading).map_err(SFError::Validation)?;
            let outcome = sqlx::query(DECLARE_SQL)
                .bind(&reading.identity.kind)
                .bind(&reading.identity.scope)
                .bind(&reading.identity.literal)
                .bind(&reading.source.doc)
                .bind(&reading.title)
                .bind(&reading.assertion)
                .bind(&reading.source.verdict)
                .bind(reading.source.priority.as_str())
                .bind(reading.source.block.as_str())
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!("declare {}: {e}", reading.identity.as_key()))
                })?;
            // The conflict clause writes nothing, so a declaration already queued
            // contributes no row here. Counting the statement rather than the
            // batch is what makes "how many were newly queued" answerable.
            queued += outcome.rows_affected() as usize;
        }
        Ok(queued)
    }

    async fn fold_declarations(
        &self,
        limit: usize,
        first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        let rows = sqlx::query(FOLD_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read declarations to fold: {e}")))?;
        let readings: Vec<ChangeReading> = rows
            .iter()
            .map(|row| Self::row_to_reading(row).map_err(SFError::Database))
            .collect::<SFResult<_>>()?;
        if readings.is_empty() {
            return Ok(0);
        }

        // The account first, the marks second. A fold that stops in between
        // leaves the declarations unfolded and repeats this work on the next
        // pass, which the reading path absorbs as an idempotent upsert; the
        // other order would drop the change instead, and nothing would say so.
        self.record_readings(&readings, first_seen_rev).await?;

        let mut folded = 0usize;
        for reading in &readings {
            sqlx::query(FOLD_MARK_SQL)
                .bind(&reading.identity.kind)
                .bind(&reading.identity.scope)
                .bind(&reading.identity.literal)
                .bind(&reading.source.doc)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!(
                        "mark declaration folded for {}: {e}",
                        reading.identity.as_key()
                    ))
                })?;
            folded += 1;
        }
        Ok(folded)
    }

    async fn fold_retirements(&self, limit: usize) -> SFResult<usize> {
        let rows = sqlx::query(FOLD_RETIRE_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read retirements to fold: {e}")))?;

        // A retirement whose reason does not parse fails the fold rather than
        // being skipped: a dropped retirement is indistinguishable from one that
        // was never made, and the row it names would stay outstanding with nothing
        // to say it was meant to be gone.
        let mut retirements: Vec<(ChangeIdentity, RetireReason)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let identity = ChangeIdentity::new(
                row.try_get::<String, _>("kind")
                    .map_err(|e| SFError::Database(format!("kind: {e}")))?,
                row.try_get::<String, _>("scope")
                    .map_err(|e| SFError::Database(format!("scope: {e}")))?,
                row.try_get::<String, _>("literal")
                    .map_err(|e| SFError::Database(format!("literal: {e}")))?,
            );
            let reason: String = row
                .try_get("reason")
                .map_err(|e| SFError::Database(format!("reason: {e}")))?;
            let reason = reason.parse::<RetireReason>().map_err(|e| {
                SFError::Database(format!("reason column holds {e} for {}", identity.as_key()))
            })?;
            retirements.push((identity, reason));
        }
        if retirements.is_empty() {
            return Ok(0);
        }

        // The account first, the marks second, for the reason the declaration fold
        // gives: a fold that stops in between repeats on the next pass, and
        // applying a retirement twice is applying it once, because the measurement
        // path writes the state rather than appending to it. The other order would
        // drop the retirement instead, and nothing would say so.
        for (identity, reason) in &retirements {
            self.apply_measurement(&ChangeMeasurement::retired(identity.clone(), *reason))
                .await?;
        }

        let mut folded = 0usize;
        for (identity, _) in &retirements {
            sqlx::query(FOLD_RETIRE_MARK_SQL)
                .bind(&identity.kind)
                .bind(&identity.scope)
                .bind(&identity.literal)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!(
                        "mark retirement folded for {}: {e}",
                        identity.as_key()
                    ))
                })?;
            folded += 1;
        }
        Ok(folded)
    }

    async fn pending_declarations(&self) -> SFResult<u64> {
        let row = sqlx::query(PENDING_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count pending declarations: {e}")))?;
        let n: i64 = row
            .try_get("n")
            .map_err(|e| SFError::Database(format!("pending count: {e}")))?;
        Ok(n.max(0) as u64)
    }

    async fn pending_retirements(&self) -> SFResult<u64> {
        let row = sqlx::query(PENDING_RETIRE_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count pending retirements: {e}")))?;
        let n: i64 = row
            .try_get("n")
            .map_err(|e| SFError::Database(format!("pending retirement count: {e}")))?;
        Ok(n.max(0) as u64)
    }

    async fn apply_measurement(&self, measurement: &ChangeMeasurement) -> SFResult<()> {
        check_measurement(measurement).map_err(SFError::Validation)?;
        sqlx::query(APPLY_MEASUREMENT_SQL)
            .bind(&measurement.identity.kind)
            .bind(&measurement.identity.scope)
            .bind(&measurement.identity.literal)
            .bind(measurement.state.as_str())
            .bind(&measurement.measured_rev)
            .bind(measurement.retire_reason.map(RetireReason::as_str))
            .bind(&measurement.unsettled_reason)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SFError::Database(format!(
                    "apply measurement for {}: {e}",
                    measurement.identity.as_key()
                ))
            })?;
        Ok(())
    }

    async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        // What holds the line leads. A change a commit delivered while the
        // account still owes it is carrying a claim the tree has not confirmed,
        // and every row measured at that revision is standing on it; no band
        // can outrank it, because a band is what a document asked for and this
        // is what is actually in the way. It is a term of the order rather than
        // a stored band for the reason the contract gives: the moment the claim
        // becomes true the state moves and the term answers false.
        //
        // The band follows, because the bound is what makes the order matter: a
        // caller takes a slice, and a slice ordered only by staleness hands this
        // pass whatever has waited longest, which is all of it equally. A row a
        // document asked for at P0 and one nobody prioritised would then compete
        // for the same slot on the same rule, and with thousands of unstated
        // rows the urgent one is reached only by luck of insertion order.
        //
        // Within a band, rows nothing has measured sort first: an unmeasured row
        // is the least known, not the freshest, and ordering it by insertion
        // would put every newly read change ahead of one that has waited for a
        // measurement. A row that was just measured goes to the back of its own
        // band, so the per-band rotation is the same one the staleness key gave
        // the whole set.
        //
        // Both terminal states are excluded together, so the set this answers is
        // the same one `landed` was excluded to make: the work. A retired row is
        // not work -- its question is answered -- and leaving it in would hand a
        // fixture identity back to the tree, whose check would only overwrite the
        // retirement with the unsettled answer it was retired to stop repeating.
        Self::entries(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM change_ledger \
                 WHERE state NOT IN ('landed', 'retired') \
                 ORDER BY (delivered_rev IS NOT NULL) DESC, priority_rank DESC, \
                          measured_at ASC NULLS FIRST, first_seen_at ASC \
                 LIMIT $1"
            ))
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn owed_by_band(&self) -> SFResult<Vec<(ChangePriority, usize)>> {
        // Counted by the rank column the reads are ordered by, so the two answer
        // about the same rows by construction rather than by both spelling the
        // outstanding predicate: a band that has rows in the order has them in
        // this count.
        //
        // Ranks the account does not know are folded into `unstated` here rather
        // than dropped, which is the same direction the generated column takes
        // them: a count that vanished would make the five cells stop adding up to
        // the owed set, and that sum is a reader's only check on the split.
        let counted: Vec<(i16, i64)> = sqlx::query_as(
            "SELECT priority_rank, count(*) FROM change_ledger \
             WHERE state NOT IN ('landed', 'retired') \
             GROUP BY priority_rank",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("count the owed set by band: {e}")))?;
        Ok(ChangePriority::ALL
            .into_iter()
            .map(|band| {
                let rank = i16::from(band.rank());
                let rows = counted
                    .iter()
                    .filter(|(found, _)| {
                        // Unknown ranks read as `unstated`, exactly as the
                        // generated column scores them.
                        if rank == 0 {
                            *found <= 0
                        } else {
                            *found == rank
                        }
                    })
                    .map(|(_, count)| usize::try_from(*count).unwrap_or(usize::MAX))
                    .sum();
                (band, rows)
            })
            .collect())
    }

    async fn landed_due(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        // The mirror of `outstanding`: the one state that call excludes, read
        // alone. Kept as its own statement rather than a parameter, because the
        // two reads answer two questions and their bounds are set by two
        // different callers -- a bound wide enough to sweep the work would put a
        // day-old landed row at the head of this one.
        Self::entries(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM change_ledger \
                 WHERE state = 'landed' \
                 ORDER BY measured_at ASC NULLS FIRST, first_seen_at ASC \
                 LIMIT $1"
            ))
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn landed_touching(
        &self,
        paths: &[String],
        limit: usize,
    ) -> SFResult<Vec<ChangeLedgerRow>> {
        // Matched in the query rather than by reading the landed set and
        // filtering it here: that set is in the thousands and this read runs on
        // a pass timer, while the paths one revision touched are tens. The scope
        // is normalized before the comparison because the account holds several
        // spellings of the same location (`crates`, `crates/`, `./crates`), and a
        // comparison that did not fold them would answer "untouched" for rows
        // written in the other spelling -- exactly the rows a sweep is too slow
        // to notice in time.
        //
        // `unnest` on one array binds one parameter regardless of how many paths
        // a revision touched, which is what keeps a wide commit from making the
        // statement's parameter count depend on the tree.
        Self::entries(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM change_ledger c \
                 CROSS JOIN LATERAL ( \
                     SELECT rtrim(regexp_replace(c.scope, '^\\./', ''), '/') AS s \
                 ) n \
                 WHERE c.state = 'landed' AND ( \
                     n.s = '' OR n.s = '.' OR EXISTS ( \
                         SELECT 1 FROM unnest($1::text[]) AS p(path) \
                         WHERE p.path = n.s OR p.path LIKE n.s || '/%' \
                     ) \
                 ) \
                 ORDER BY c.measured_at ASC NULLS FIRST, c.first_seen_at ASC \
                 LIMIT $2"
            ))
            .bind(paths.to_vec())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn retired_surface_moved(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        // The only read that can reach a retired row, and it exists because a
        // retirement is about the history while the row's truth is about the
        // tree that follows it. `outstanding` excludes `retired` on the ground
        // that its question is answered; a scope the tree brings back makes that
        // ground false, and no other statement would ever look again.
        //
        // Only `surface_moved`: a `fixture` or `superseded` retirement does not
        // come back when a path does, and `out_of_scope` returns through its own
        // door -- the tree being mounted is a fact about the ledger's owner, not
        // about a commit, so re-asking it here would measure it against a tree
        // that cannot answer for it either.
        //
        // Ordered by `retired_at` rather than by `measured_at`: a retirement is
        // written once with the revision the scope exited, and a recheck that
        // found the scope still gone writes nothing, so no timestamp on these
        // rows advances. An order by anything else would leave a bounded caller
        // re-reading the same head of the set.
        Self::entries(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM change_ledger \
                 WHERE state = 'retired' AND retire_reason = 'surface_moved' \
                 ORDER BY retired_at ASC NULLS FIRST, first_seen_at ASC \
                 LIMIT $1"
            ))
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn record_reopen(&self, reopen: &ChangeReopen) -> SFResult<()> {
        sqlx::query(RECORD_REOPEN_SQL)
            .bind(&reopen.identity.kind)
            .bind(&reopen.identity.scope)
            .bind(&reopen.identity.literal)
            .bind(reopen.was_landed_at)
            .bind(&reopen.measured_rev)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("record reopen: {e}")))?;
        Ok(())
    }

    async fn record_delivery(&self, identity: &ChangeIdentity, rev: &str) -> SFResult<()> {
        sqlx::query(RECORD_DELIVERY_SQL)
            .bind(&identity.kind)
            .bind(&identity.scope)
            .bind(&identity.literal)
            .bind(rev)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("record delivery: {e}")))?;
        Ok(())
    }

    async fn get(&self, identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>> {
        let row = sqlx::query(&format!(
            "SELECT {ROW_COLUMNS} FROM change_ledger \
             WHERE kind = $1 AND scope = $2 AND literal = $3"
        ))
        .bind(&identity.kind)
        .bind(&identity.scope)
        .bind(&identity.literal)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        match row {
            Some(row) => Self::row_to_entry(&row)
                .map(Some)
                .map_err(SFError::Database),
            None => Ok(None),
        }
    }

    async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>> {
        let rows = sqlx::query("SELECT state, count(*) AS n FROM change_ledger GROUP BY state")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        let mut counted: Vec<(ChangeState, u64)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let state: String = row
                .try_get("state")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let state = state
                .parse::<ChangeState>()
                .map_err(|e| SFError::Database(format!("state column holds {e}")))?;
            let n: i64 = row
                .try_get("n")
                .map_err(|e| SFError::Database(e.to_string()))?;
            counted.push((state, n.max(0) as u64));
        }
        // Every state is answered, zeroes included: a reader comparing two
        // censuses has to see a state that emptied, and a missing key reads the
        // same as a state nobody thought to count.
        Ok(ChangeState::ALL
            .into_iter()
            .map(|state| {
                let n = counted
                    .iter()
                    .find(|(seen, _)| *seen == state)
                    .map(|(_, n)| *n)
                    .unwrap_or(0);
                (state, n)
            })
            .collect())
    }

    async fn demand_closure(&self) -> SFResult<Option<DemandClosure>> {
        // A join rather than a column on the intake. The answer is a property of
        // the pair -- what a document asked for, and what the account currently
        // holds for that identity -- and a column would be a second copy of the
        // account's state, written by whoever last measured and stale for every
        // declaration folded before it. That copy would be wrong in the one
        // direction that matters: a change declared after it was already landed
        // would enter as owed and stay owed without anything ever being wrong
        // enough to notice.
        //
        // The outer join is what keeps a declaration whose change never entered
        // the account counted as owed instead of dropped: it is a demand nobody
        // has measured yet, which is the most owed a demand can be.
        let row = sqlx::query(DEMAND_CLOSURE_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read demand closure: {e}")))?;
        let satisfied: i64 = row
            .try_get("satisfied")
            .map_err(|e| SFError::Database(e.to_string()))?;
        let owed: i64 = row
            .try_get("owed")
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(Some(DemandClosure {
            satisfied: satisfied.max(0) as u64,
            owed: owed.max(0) as u64,
        }))
    }

    async fn blocking_surface(&self) -> SFResult<Option<BlockingSurface>> {
        // Three counts in one statement, because they are one reading: a holder
        // is a row whose delivery is in front of a claim the tree has not
        // confirmed, and what it holds is every other row measured at that same
        // revision. Counting the two in separate reads would let a row land
        // between them and be reported as holding work it had just released.
        //
        // The `DISTINCT` is the difference between "how much is behind this"
        // and "how many pairs": a row measured at a revision two holders both
        // delivered is one row held, not two.
        let row = sqlx::query(BLOCKING_SURFACE_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read blocking surface: {e}")))?;
        let holders: i64 = row
            .try_get("holders")
            .map_err(|e| SFError::Database(format!("holders: {e}")))?;
        let held: i64 = row
            .try_get("held_landings")
            .map_err(|e| SFError::Database(format!("held_landings: {e}")))?;
        let conflicts: i64 = row
            .try_get("block_conflicts")
            .map_err(|e| SFError::Database(format!("block_conflicts: {e}")))?;
        Ok(Some(BlockingSurface {
            holders: holders.max(0) as u64,
            held_landings: held.max(0) as u64,
            block_conflicts: conflicts.max(0) as u64,
        }))
    }

    async fn block_census(&self) -> SFResult<Option<Vec<BlockCensus>>> {
        // One row per face the account knows, including the ones with nothing
        // in them: a face that vanished from the census reads as a face that
        // finished, and the whole point of the grouping is to answer "how far
        // along is this one" without a reader supplying its existence.
        //
        // The oldest unmeasured age is taken over the owed rows only -- a landed
        // row's measurement stopped being owed work the moment it landed -- and
        // is `NULL` when every owed row in the face has been measured, which is
        // a different answer from zero seconds.
        let rows = sqlx::query(BLOCK_CENSUS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read block census: {e}")))?;
        let mut counted: Vec<(String, String, i64, Option<f64>)> = Vec::with_capacity(rows.len());
        for row in &rows {
            counted.push((
                row.try_get("block")
                    .map_err(|e| SFError::Database(format!("block: {e}")))?,
                row.try_get("state")
                    .map_err(|e| SFError::Database(format!("state: {e}")))?,
                row.try_get("n")
                    .map_err(|e| SFError::Database(format!("n: {e}")))?,
                row.try_get("oldest_unmeasured_secs")
                    .map_err(|e| SFError::Database(format!("oldest_unmeasured_secs: {e}")))?,
            ));
        }
        let mut census = Vec::with_capacity(ChangeBlock::ALL.len());
        for block in ChangeBlock::ALL {
            let landed = counted
                .iter()
                .filter(|(found, state, _, _)| found == block.as_str() && state == "landed")
                .map(|(_, _, n, _)| *n)
                .sum::<i64>();
            let owed = counted
                .iter()
                .filter(|(found, state, _, _)| {
                    found == block.as_str() && state != "landed" && state != "retired"
                })
                .map(|(_, _, n, _)| *n)
                .sum::<i64>();
            let oldest = counted
                .iter()
                .filter(|(found, state, _, _)| {
                    found == block.as_str() && state != "landed" && state != "retired"
                })
                .filter_map(|(_, _, _, secs)| *secs)
                .fold(None::<f64>, |acc, secs| {
                    Some(acc.map_or(secs, |a| a.max(secs)))
                });
            census.push(BlockCensus {
                block,
                landed: landed.max(0) as u64,
                owed: owed.max(0) as u64,
                oldest_unmeasured_secs: oldest.map(|secs| secs.max(0.0) as u64),
            });
        }
        Ok(Some(census))
    }

    async fn record_dispatch_state(&self, state: &DispatchState) -> SFResult<()> {
        sqlx::query(RECORD_DISPATCH_STATE_SQL)
            .bind(state.decision.as_str())
            .bind(state.pool.as_str())
            .bind(state.pending as i64)
            .bind(state.submitted as i64)
            .bind(state.already as i64)
            .bind(state.failed as i64)
            .bind(state.dead_environment as i64)
            .bind(state.held_back as i64)
            .bind(state.rearmed as i64)
            .bind(state.unmeasurable as i64)
            .bind(state.resume_at_unix)
            .bind(state.evidenced_recovery_unix)
            .bind(state.quota_window_secs as i64)
            .bind(state.sources_unread.as_deref())
            // The three travel as one `Option` on the contract: a pass that read
            // no pace writes three NULLs, and a pass that read one writes all
            // three, so a reader never sees a window figure without the two it
            // is read against.
            .bind(state.window_pace.map(|pace| pace.allowance_tokens as i64))
            .bind(state.window_pace.map(|pace| pace.spent_tokens as i64))
            .bind(state.window_pace.map(|pace| pace.reserve_tokens as i64))
            // The ramp's anchor, so the batch can be read against how long the
            // allowance had been accruing when it was sized. A pace that states
            // no window start keeps it NULL rather than writing epoch zero,
            // which would read as a window that began in 1970.
            .bind(
                state
                    .window_pace
                    .map(|pace| pace.window_start_unix)
                    .filter(|start| *start > 0),
            )
            // `None` is not zero: a pass whose commit log could not be read
            // says nothing about delivery, and writing zero would make that
            // failure read as "nothing was delivered".
            .bind(state.delivered.map(|count| count as i64))
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("record dispatch state: {e}")))?;
        Ok(())
    }

    async fn write_windows(&self) -> SFResult<Option<Vec<WriteWindow>>> {
        // Every row, whatever state it is in: the caller reduces them to a
        // reading with one rule, and a query that applied that rule here would
        // be a second copy of it -- and would make the revoked row, the
        // inconsistent one and the self-approved one unreadable, which are
        // precisely the rows the reading exists to surface.
        //
        // The table's absence is not caught and answered with an empty set: a
        // deployment that has not run the migration has no windows *and* no
        // way to say so, and turning a failed read into "nothing is approved"
        // would trade a missing reading for a false one. The caller publishes
        // no cell in that case, which is the reading for "not measured".
        let rows = sqlx::query(WRITE_WINDOW_COLUMNS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read write windows: {e}")))?;
        let mut windows = Vec::with_capacity(rows.len());
        for row in &rows {
            let field_error =
                |e: sqlx::Error| SFError::Database(format!("read write windows: {e}"));
            windows.push(WriteWindow {
                window_id: row.try_get("window_id").map_err(field_error)?,
                path_scope: row.try_get("path_scope").map_err(field_error)?,
                writer: row.try_get("writer").map_err(field_error)?,
                approver: row.try_get("approver").map_err(field_error)?,
                requested_at: row.try_get("requested_at").map_err(field_error)?,
                approved_at: row.try_get("approved_at").map_err(field_error)?,
                expires_at: row.try_get("expires_at").map_err(field_error)?,
                revoked_at: row.try_get("revoked_at").map_err(field_error)?,
            });
        }
        Ok(Some(windows))
    }
}

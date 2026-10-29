//! Running the schema statements a store owns, in one place.
//!
//! Every store keeps its own statements but hands them here to run, so the
//! application's first start and a restore drill execute the same list. Two
//! independent copies is how a table can exist everywhere the app looks and
//! nowhere a restore looks.
//!
//! Statements are idempotent by construction (`IF NOT EXISTS`), which is why a
//! restore can replay them after the migrations without checking anything
//! first.
//!
//! # Why an idempotent statement still needs a lock wait bound
//!
//! `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` takes ACCESS EXCLUSIVE *before*
//! it consults the catalog, so on a table whose column has been there for
//! months it still asks for the heaviest lock there is -- measured on a
//! throwaway database, the statement timed out after a full second against a
//! plain reader even though its column already existed and it had nothing to
//! do. During a rolling restart the outgoing replica is reading that same
//! table, and PostgreSQL's lock queue is first-come: once the statement waits
//! for ACCESS EXCLUSIVE, every later request on that relation -- including the
//! outgoing replica's plain `SELECT`s -- queues behind it. The newcomer's
//! schema sweep then freezes the replica it is waiting to replace. Same
//! database, same two sessions: a plain `SELECT count(*)` stayed blocked for
//! the whole ten seconds it was given, and with a 500 ms lock wait on the
//! ALTER the reader was never blocked at all.
//!
//! A schema text may hold several statements, and the driver's simple-query
//! path runs them as one implicit transaction (see `apply`), so a statement
//! that waits keeps every ACCESS EXCLUSIVE an earlier statement already took
//! until it finishes. Bounding the wait bounds both: an attempt either gets
//! its locks and commits, or gives them all back within one lock wait and is
//! replayed.
//!
//! Replaying is safe for the same reason the list is idempotent in the first
//! place, and it is the whole batch that replays -- splitting a text into
//! statements to retry one of them would mean parsing SQL, and a text that
//! holds a dollar-quoted body or a semicolon inside a literal would be split
//! wrongly by anything that did not.
//!
//! # Why a statement the catalog already satisfies is not run at all
//!
//! A bounded wait answers "how long may this hold the queue open", not "should
//! it queue at all". On a table of a few hundred megabytes and a host whose
//! storage is saturated, the reader an `ALTER` waits for can hold ACCESS SHARE
//! for minutes, and no retry budget short of that window gets the lock -- while
//! every attempt that gives up hands the queue back and re-joins it, which is
//! the reader's latency paid over and over for a statement that would have done
//! nothing. Measured on a rolling restart: the incoming replica retried sixteen
//! times over seventeen seconds against a read that took a hundred and fifteen,
//! and the start failed while the table it wanted to alter already had every
//! column the statement names.
//!
//! So the wait is not where the steady state should live. Every statement this
//! module can prove is already in the catalog is dropped before the batch is
//! sent: [`already_in_effect`] reads the text, keeps only the shapes whose
//! effect is a catalog fact (`CREATE TABLE IF NOT EXISTS`,
//! `CREATE INDEX IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`,
//! `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL`), and answers whether every
//! one of them already holds. A text holding anything else -- a statement whose
//! effect is rows rather than catalog, a shape this cannot read, a body it
//! cannot split -- is run as it always was, unread.
//!
//! The guard only ever removes work. It cannot skip a statement that would have
//! changed something: a check it cannot evaluate makes the whole text run, and
//! the shapes it does read are the ones PostgreSQL itself defines as no-ops
//! when the object is there.

use cog_core::{SFError, SFResult};
use sqlx::PgPool;
use std::time::Duration;
use tracing::warn;

/// The runner a store outside this crate is handed instead of a pool.
///
/// A store owns its statements; how an attempt bounds its lock wait and how a
/// blocked start explains itself are properties of the database, not of the
/// store, so they live here and reach the store as a contract. The store asks
/// for [`cog_core::SchemaRunner`], the assembly root hands over this, and the
/// crate that keeps the statements never learns which driver runs them.
pub struct PgSchemaRunner {
    pool: PgPool,
}

impl PgSchemaRunner {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// A runner with a pool of its own, for a process that starts a store
    /// before -- or outside -- the assembly.
    ///
    /// The pool is for schema text only: it is created on demand and never
    /// handed out, so the caller is not adding a second serving path to the
    /// same database.
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        Ok(Self::new(PgPool::connect(database_url).await?))
    }
}

#[async_trait::async_trait]
impl cog_core::SchemaRunner for PgSchemaRunner {
    async fn apply_schema(&self, ddl: &str) -> SFResult<()> {
        apply(&self.pool, ddl).await
    }
}

/// How long one attempt may wait for a lock before giving the locks back.
///
/// Short enough that a replica still serving the table is never queued behind
/// this statement for longer than a quarter second at a time.
const LOCK_WAIT: &str = "250ms";

/// How long the last attempt may wait -- twenty times what the others get.
///
/// The two jobs of this number pull in opposite directions from `LOCK_WAIT`,
/// and the last attempt is the only place where they stop pulling: there is no
/// next attempt to give the locks back for, so waiting longer costs the
/// outgoing replica nothing it has not already paid, while it turns the
/// attempt from "one more quarter second" into an actual last chance.
///
/// It is also the only attempt that can explain itself, and explaining needs
/// the relation to still be there when it is read: the request is released the
/// moment the statement gives up, so the reading has to happen while it waits.
/// At a quarter second the sampler got about a dozen chances and was measured
/// reporting "no relation could be sampled" on roughly one run in four; five
/// seconds gives it two hundred and fifty, and the same for the holder to let
/// go.
const FINAL_WAIT: &str = "5s";

/// Attempts one schema may make before the store is told it is blocked.
///
/// A schema is idempotent, so an attempt that gave its locks back costs
/// nothing to replay; the cap exists to turn "someone holds this table" into a
/// named failure instead of a start that never finishes. Fifteen attempts of a
/// quarter second, their growing pauses, and the last attempt's longer wait
/// come to about sixteen seconds -- inside a container's startup budget, so
/// the caller still gets to report *why* it could not start rather than being
/// killed mid-silence.
///
/// The attempts have to span more than one cycle of a periodic reader: the
/// observed collision was a reader on a ten second poll, and a set of attempts
/// that all landed inside the same poll would all hit the same lock.
const ATTEMPTS: u32 = 16;

/// How long the first replay waits before trying the schema again.
///
/// The collisions this exists for are a replica serving a table, not a lock
/// taken for milliseconds, so the first pause is already long enough to matter
/// to it; starting shorter would spend attempts inside the same reader's
/// window.
const FIRST_PAUSE_MS: u64 = 200;

/// How much longer each replay waits than the one before it.
///
/// The attempts have to spread across a reader's cycle rather than march in
/// step: with a fixed pause, sixteen attempts fit inside one ten second poll
/// and every one of them lands on the same lock.
const PAUSE_GROWTH_MS: u64 = 50;

/// The pause before attempt `n`, which grows so the attempts spread out
/// instead of landing together in one reader's window.
fn pause_before(attempt: u32) -> Duration {
    Duration::from_millis(FIRST_PAUSE_MS)
        .saturating_add(Duration::from_millis(PAUSE_GROWTH_MS) * (attempt - 1))
}

/// How often the last attempt's outstanding lock requests are sampled.
///
/// The request is released the moment the statement gives up, so the only
/// window in which the relation can be read is while it waits. Twenty
/// milliseconds puts two hundred and fifty readings inside `FINAL_WAIT` and
/// about a dozen inside a plain `LOCK_WAIT`; finer would spend more of the
/// wait reading the catalog than waiting.
const LOCK_SAMPLE_MS: u64 = 20;

/// SQLSTATE `lock_not_available`, which is what a `lock_timeout` expiry raises.
const LOCK_NOT_AVAILABLE: &str = "55P03";

/// The name an attempt gives its session for as long as it runs, so the
/// relations it is waiting on can be read out of `pg_locks` without guessing
/// which of the pool's sessions is ours.
///
/// Without it the sampler has to decide by lock mode, and the mode is exactly
/// what differs between two statements that stall the same way: an `ALTER`
/// waits for ACCESS EXCLUSIVE, a `CREATE INDEX` for ShareLock, and a sampler
/// written for one of them reports "nothing was waiting" for the other.
fn attempt_tag(attempt: u32) -> String {
    format!("cog_ddl_{}_{}", std::process::id(), attempt)
}

/// The same tag as a SQL comment, which the server has from the moment it
/// receives the message rather than from the moment the `SET` before the
/// schema has run.
fn mark(tag: &str) -> String {
    format!("/*{tag}*/")
}

/// Run one store's schema. The text may hold several statements; the driver's
/// simple-query path is what makes that legal, and it is also why this is not
/// a prepared statement.
///
/// The attempt runs inside an explicit transaction so `SET LOCAL` scopes the
/// lock wait to this statement list -- the pool hands the connection back with
/// no leftover setting -- and so a failed attempt rolls back everything it had
/// already done. That matters because a multi-statement simple query is one
/// implicit transaction anyway: without the explicit one, a late statement's
/// lock wait would still be holding every lock the earlier statements took,
/// and there would be no point at which they could be released.
pub async fn apply(pool: &PgPool, schema: &str) -> SFResult<()> {
    // Before anything asks for a lock: is there anything left to do? The
    // common start is a table that already has every column and index the text
    // names, and there the honest cost of the batch is zero -- asking for
    // ACCESS EXCLUSIVE to find that out is what put the outgoing replica's
    // readers at the back of a queue behind a statement with no work to do.
    if already_in_effect(pool, schema).await {
        return Ok(());
    }
    let mut attempt = 0;
    loop {
        attempt += 1;
        if attempt == ATTEMPTS {
            // The last attempt is the one that gets to explain itself: a lock
            // wait's relation can only be read while the request is still
            // outstanding, and after this one gives up there is nothing left
            // to look at. It also waits longer -- see `FINAL_WAIT`.
            let (outcome, waited_on) = watched_attempt(pool, schema).await;
            return match outcome {
                Attempt::Ok => Ok(()),
                Attempt::LockWait(e) => Err(blocked(&waited_on, e)),
                Attempt::Failed(message) => Err(SFError::Database(message)),
            };
        }
        match attempt_once(pool, schema, &attempt_tag(attempt), LOCK_WAIT).await {
            Attempt::Ok => return Ok(()),
            Attempt::LockWait(_) => tokio::time::sleep(pause_before(attempt)).await,
            Attempt::Failed(message) => return Err(SFError::Database(message)),
        }
    }
}

/// What one attempt concluded.
///
/// A lock wait is separated from every other failure because it is the only
/// one that is worth replaying; carrying it as a flag rather than re-reading
/// the driver's error keeps the retry decision in one place.
enum Attempt {
    Ok,
    LockWait(sqlx::Error),
    Failed(String),
}

async fn attempt_once(pool: &PgPool, schema: &str, tag: &str, wait: &str) -> Attempt {
    match apply_once(pool, schema, tag, wait).await {
        Ok(()) => Attempt::Ok,
        Err(e) if is_lock_wait_expiry(&e) => Attempt::LockWait(e),
        Err(e) => Attempt::Failed(e.to_string()),
    }
}

/// Run the attempt while reading the relations it is waiting on.
///
/// The lock request is released the moment the statement gives up, so a read
/// taken afterwards finds nothing and the failure could name no relation at
/// all. Sampling while the attempt is in flight is what keeps the answer --
/// and it is the answer an operator needs: which table is being held, not
/// merely that something was.
async fn watched_attempt(pool: &PgPool, schema: &str) -> (Attempt, Vec<String>) {
    // `watched_attempt` is only ever the last attempt, so the tag its worker
    // uses is the one the caller's loop would have given that attempt.
    let tag = attempt_tag(ATTEMPTS);
    let worker = {
        let pool = pool.clone();
        let schema = schema.to_string();
        let tag = tag.clone();
        tokio::spawn(async move { attempt_once(&pool, &schema, &tag, FINAL_WAIT).await })
    };

    let mut waited_on: Vec<String> = Vec::new();
    // Sampled before the first `is_finished` check, and again after each pause:
    // a loop that tests the condition first can exit having read nothing, and
    // then a failure that did have a reason reports that it could not find one.
    loop {
        for relation in unheld_locks(&tag, pool).await {
            if !waited_on.contains(&relation) {
                waited_on.push(relation);
            }
        }
        if worker.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(LOCK_SAMPLE_MS)).await;
    }

    let outcome = match worker.await {
        Ok(outcome) => outcome,
        Err(join) => Attempt::Failed(format!("the schema attempt could not run: {join}")),
    };
    (outcome, waited_on)
}

/// The relations the tagged attempt is currently queued on -- which, while it
/// is running, is what it is waiting for.
///
/// The filter is the session tag rather than a lock mode, so whatever the
/// statement asked for is what comes back; and it is the tag rather than "any
/// ungranted request in the database", so a relation another transaction is
/// stuck on cannot be reported as this one's.
///
/// The tag is looked for twice, because the two ways of finding the session do
/// not fail at the same moment. The comment carries it from the instant the
/// server parses the message; the session name only from the instant the `SET`
/// before the schema has run. Reading either is enough, and the first version
/// -- the name alone -- was measured reporting "no relation could be sampled"
/// on roughly one run in four.
async fn unheld_locks(tag: &str, pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT coalesce(c.relname, 'a lock on a dropped relation')
           FROM pg_locks l
           JOIN pg_stat_activity a ON a.pid = l.pid
           LEFT JOIN pg_class c ON c.oid = l.relation
          WHERE NOT l.granted
            AND (a.application_name = $1 OR a.query LIKE $2)
          LIMIT 3",
    )
    .bind(tag)
    .bind(format!("%{}%", mark(tag)))
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

/// Run several stores' schema in the order given. Order matters across stores
/// when one table's definition references another's.
pub async fn apply_all(pool: &PgPool, schemas: &[&str]) -> SFResult<()> {
    for schema in schemas {
        apply(pool, schema).await?;
    }
    Ok(())
}

/// Whether every statement in `schema` is one the catalog already satisfies,
/// so sending the batch would take locks to change nothing.
///
/// `false` is the safe answer and the one every uncertainty resolves to: a
/// statement this cannot read, a check that does not hold, a catalog that
/// cannot be read. The batch then runs exactly as it did before this existed.
///
/// A catalog read that fails is reported and treated as "no answer": a start
/// that cannot read `pg_catalog` has a larger problem than the lock, and
/// turning it into a different failure here would hide it.
async fn already_in_effect(pool: &PgPool, schema: &str) -> bool {
    let Some(checks) = catalog_checks(schema) else {
        return false;
    };
    match checks_hold(pool, &checks).await {
        Ok(all) => all,
        Err(e) => {
            warn!(
                error = %e,
                "schema_ddl: the catalog could not be read, so the schema is sent unread"
            );
            false
        }
    }
}

/// Whether the guard reads every statement in `schema` as a catalog fact, so
/// the batch can be answered without sending it.
///
/// This is the guard's own question, asked without a database, and it is public
/// because the answer is a property of the text while the texts belong to the
/// crates that own the statements. The only place that can hold the real
/// startup batch next to the guard is the assembly root, and a guard no real
/// text reaches is a guard that fixes nothing.
///
/// `false` says nothing about whether the batch is idempotent -- only that this
/// module cannot prove it from the catalog, and will therefore run it.
pub fn guard_reads(schema: &str) -> bool {
    catalog_checks(schema).is_some()
}

/// One fact the catalog can be asked about, per statement this can read.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Check {
    TableExists(String),
    IndexExists(String),
    ColumnExists { table: String, column: String },
    ColumnIsNotNull { table: String, column: String },
}

/// The checks a schema text reduces to, or `None` when it holds anything this
/// cannot prove is already done.
///
/// `Some(vec![])` is a text with no statements in it, which is satisfied by
/// doing nothing; `None` is always "run it".
fn catalog_checks(schema: &str) -> Option<Vec<Check>> {
    let mut checks = Vec::new();
    for statement in statements(schema)? {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        checks.push(classify(statement)?);
    }
    Some(checks)
}

/// Which catalog fact one statement establishes, if this can read it.
///
/// Only the leading tokens are read, and only as far as the object's name: the
/// body of a `CREATE TABLE` holds types, defaults and constraints, and nothing
/// there changes whether running it would be a no-op. A shape that is not one
/// of the four below -- including the same words with `IF NOT EXISTS` missing,
/// which is a statement that *fails* when the object is there -- is `None`.
fn classify(statement: &str) -> Option<Check> {
    let words: Vec<String> = statement
        .split_whitespace()
        .map(|word| word.to_ascii_lowercase())
        .collect();
    let word = |at: usize| words.get(at).map(String::as_str);
    // An identifier this hands to the catalog has to be the unquoted, folded
    // form PostgreSQL will look it up by; anything else (quoted, qualified,
    // carrying the opening parenthesis) is left to the batch to run.
    let name = |at: usize| -> Option<String> {
        let found = words.get(at)?;
        let usable = !found.is_empty()
            && found
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        usable.then(|| found.clone())
    };

    match word(0)? {
        "create" => match (word(1)?, word(2)?) {
            ("table", "if") if word(3) == Some("not") && word(4) == Some("exists") => {
                Some(Check::TableExists(name(5)?))
            }
            ("index", "if") if word(3) == Some("not") && word(4) == Some("exists") => {
                Some(Check::IndexExists(name(5)?))
            }
            ("unique", "index")
                if word(3) == Some("if") && word(4) == Some("not") && word(5) == Some("exists") =>
            {
                Some(Check::IndexExists(name(6)?))
            }
            _ => None,
        },
        "alter" => {
            if word(1)? != "table" {
                return None;
            }
            let table = name(2)?;
            if word(3) == Some("add")
                && word(4) == Some("column")
                && word(5) == Some("if")
                && word(6) == Some("not")
                && word(7) == Some("exists")
            {
                return Some(Check::ColumnExists {
                    table,
                    column: name(8)?,
                });
            }
            if word(3) == Some("alter")
                && word(4) == Some("column")
                && word(6) == Some("set")
                && word(7) == Some("not")
                && word(8) == Some("null")
            {
                return Some(Check::ColumnIsNotNull {
                    table,
                    column: name(5)?,
                });
            }
            None
        }
        _ => None,
    }
}

/// Split a schema text into statements at the semicolons that end them.
///
/// This is not a SQL parser and is not asked to be one: it tracks the four
/// things that can hide a semicolon -- a line comment, a (nesting) block
/// comment, a quoted string or identifier, a dollar-quoted body -- and returns
/// `None` when one of them is left open. A separator it gets wrong splits one
/// statement into two, and neither half then matches a shape, which makes the
/// text run unread rather than skip.
fn statements(text: &str) -> Option<Vec<String>> {
    let chars: Vec<char> = text.chars().collect();
    let mut found: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut at = 0;
    while at < chars.len() {
        let here = chars[at];
        let next = chars.get(at + 1).copied();
        if here == '-' && next == Some('-') {
            at += 2;
            while at < chars.len() && chars[at] != '\n' {
                at += 1;
            }
        } else if here == '/' && next == Some('*') {
            let mut depth = 1usize;
            at += 2;
            while at < chars.len() && depth > 0 {
                if chars[at] == '/' && chars.get(at + 1) == Some(&'*') {
                    depth += 1;
                    at += 2;
                } else if chars[at] == '*' && chars.get(at + 1) == Some(&'/') {
                    depth -= 1;
                    at += 2;
                } else {
                    at += 1;
                }
            }
            if depth > 0 {
                return None;
            }
        } else if here == '\'' || here == '"' {
            // A doubled quote is the escape; the run ends at the first one not
            // followed by its twin.
            current.push(here);
            at += 1;
            loop {
                let inner = *chars.get(at)?;
                current.push(inner);
                at += 1;
                if inner == here {
                    if chars.get(at) == Some(&here) {
                        current.push(here);
                        at += 1;
                    } else {
                        break;
                    }
                }
            }
        } else if here == '$' {
            // A dollar-quoted body, whose tag is read off the opening marker
            // and looked for again as the closing one.
            let mut end_of_tag = at + 1;
            while end_of_tag < chars.len()
                && (chars[end_of_tag].is_alphanumeric() || chars[end_of_tag] == '_')
            {
                end_of_tag += 1;
            }
            if chars.get(end_of_tag) != Some(&'$') {
                // A `$` that opens nothing -- a parameter, or an operator.
                current.push(here);
                at += 1;
                continue;
            }
            let tag: String = chars[at..=end_of_tag].iter().collect();
            let rest: String = chars[end_of_tag + 1..].iter().collect();
            let close = rest.find(&tag)?;
            let end = end_of_tag + 1 + rest[..close].chars().count() + tag.chars().count();
            current.extend(chars[at..end].iter());
            at = end;
        } else if here == ';' {
            found.push(std::mem::take(&mut current));
            at += 1;
        } else {
            current.push(here);
            at += 1;
        }
    }
    if !current.trim().is_empty() {
        found.push(current);
    }
    Some(found)
}

/// Whether the catalog already says everything `checks` asks for.
async fn checks_hold(pool: &PgPool, checks: &[Check]) -> SFResult<bool> {
    if checks.is_empty() {
        return Ok(true);
    }

    let tables: Vec<String> = checks
        .iter()
        .filter_map(|check| match check {
            Check::TableExists(table) => Some(table.clone()),
            _ => None,
        })
        .collect();
    if !tables.is_empty() {
        let present: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = current_schema() AND c.relname = ANY($1)",
        )
        .bind(&tables)
        .fetch_all(pool)
        .await
        .map_err(|e| SFError::Database(format!("read the catalog's relations: {e}")))?;
        if present.len() != tables.len() {
            return Ok(false);
        }
    }

    let indexes: Vec<String> = checks
        .iter()
        .filter_map(|check| match check {
            Check::IndexExists(index) => Some(index.clone()),
            _ => None,
        })
        .collect();
    if !indexes.is_empty() {
        let present: Vec<String> = sqlx::query_scalar(
            "SELECT indexname FROM pg_indexes
              WHERE schemaname = current_schema() AND indexname = ANY($1)",
        )
        .bind(&indexes)
        .fetch_all(pool)
        .await
        .map_err(|e| SFError::Database(format!("read the catalog's indexes: {e}")))?;
        if present.len() != indexes.len() {
            return Ok(false);
        }
    }

    let mut wanted: Vec<(String, String)> = Vec::new();
    let mut must_be_not_null: Vec<(String, String)> = Vec::new();
    for check in checks {
        match check {
            Check::ColumnExists { table, column } => wanted.push((table.clone(), column.clone())),
            Check::ColumnIsNotNull { table, column } => {
                wanted.push((table.clone(), column.clone()));
                must_be_not_null.push((table.clone(), column.clone()));
            }
            _ => {}
        }
    }
    if !wanted.is_empty() {
        // One round trip for every pair, and the answer carries `attnotnull`
        // so the two questions are read off the same row rather than two reads
        // that could disagree.
        let tables: Vec<String> = wanted.iter().map(|(table, _)| table.clone()).collect();
        let columns: Vec<String> = wanted.iter().map(|(_, column)| column.clone()).collect();
        let rows: Vec<(String, String, bool)> = sqlx::query_as(
            "SELECT c.relname, a.attname, a.attnotnull
               FROM unnest($1::text[], $2::text[]) AS want(tbl, col)
               JOIN pg_class c ON c.relname = want.tbl
               JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = current_schema()
               JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = want.col
              WHERE NOT a.attisdropped",
        )
        .bind(&tables)
        .bind(&columns)
        .fetch_all(pool)
        .await
        .map_err(|e| SFError::Database(format!("read the catalog's columns: {e}")))?;
        if rows.len() != wanted.len() {
            return Ok(false);
        }
        for (table, column) in &must_be_not_null {
            let held = rows.iter().any(|(found_table, found_column, not_null)| {
                found_table == table && found_column == column && *not_null
            });
            if !held {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

async fn apply_once(pool: &PgPool, schema: &str, tag: &str, wait: &str) -> Result<(), sqlx::Error> {
    // `SET LOCAL` goes first in the same message, and no `BEGIN` is written:
    // a multi-statement simple query is already one implicit transaction, so
    // the wait is scoped to this statement list and cannot follow the
    // connection back into the pool, where a quarter-second lock wait on
    // ordinary queries would be a far worse bug than the one being fixed.
    //
    // Writing `BEGIN` instead would make the block explicit, and an explicit
    // block that aborts stays open: PostgreSQL then refuses every statement on
    // that connection with 25P02 until someone rolls it back, and the pooled
    // connection that took the lock timeout would poison whoever checked it
    // out next. Measured on a throwaway database: without the `BEGIN`, a batch
    // that failed on the lock left the same session answering normally, the
    // statements before the failure rolled back, and `lock_timeout` read 0
    // again.
    //
    // The duration is interpolated rather than bound because `SET` takes no
    // parameters, and it is a constant, not anything a caller supplies.
    //
    // `application_name` is set the same way and for the same reason: scoped to
    // this statement list, gone before the connection returns to the pool, and
    // visible in `pg_stat_activity` while the statement is waiting, which is
    // the only window in which the relation it is stuck on can be read.
    let text = format!(
        "{} \nSET LOCAL lock_timeout = '{wait}';\nSET LOCAL application_name = '{tag}';\n{schema}",
        mark(tag)
    );
    sqlx::raw_sql(&text).execute(pool).await?;
    Ok(())
}

fn is_lock_wait_expiry(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some(LOCK_NOT_AVAILABLE))
}

/// Say what is holding the schema up, because the caller's alternative is a
/// container that is killed on a startup probe and reports nothing at all.
fn blocked(waited_on: &[String], cause: sqlx::Error) -> SFError {
    let what = if waited_on.is_empty() {
        "no relation could be sampled while the last attempt waited".to_string()
    } else {
        waited_on.join(", ")
    };
    SFError::Database(format!(
        "schema did not get its locks in {ATTEMPTS} attempts of {LOCK_WAIT} \
         (the last {FINAL_WAIT}; waiting on: {what}); a replica still serving these tables holds them \
         open, and the start is reported instead of being killed at a startup \
         probe with no reason. Last error: {cause}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the splitter is for: the semicolons that end statements, and only
    /// those. The DDL texts carry default values like `'{}'::jsonb` and index
    /// expressions like `LOWER(name)`, and a split inside one of them would
    /// hand `classify` a fragment instead of a statement.
    ///
    /// This is the half of the guard that needs no database, so it is the half
    /// that can be wrong without the live tests noticing: a splitter that
    /// over-splits makes the text run unread (slow, not wrong), but one that
    /// under-splits glues two statements together and then matches neither --
    /// also slow, not wrong. Only a splitter that quietly drops a statement
    /// could skip real work, which is why every piece below is counted.
    #[test]
    fn a_schema_splits_at_the_semicolons_that_end_statements() {
        let split = statements(
            "CREATE TABLE IF NOT EXISTS t (a TEXT DEFAULT 'a;b');\n\
             -- a comment; with a semicolon in it\n\
             ALTER TABLE t ADD COLUMN IF NOT EXISTS b TEXT DEFAULT E'x;y';\n\
             /* a block comment; /* nested; */ still inside */\n\
             CREATE INDEX IF NOT EXISTS i ON t (LOWER(a));",
        )
        .expect("every construct here is one the splitter follows");
        let trimmed: Vec<&str> = split.iter().map(|s| s.trim()).collect();
        assert_eq!(trimmed.len(), 3, "got {trimmed:#?}");
        assert!(trimmed[0].starts_with("CREATE TABLE"));
        assert!(trimmed[0].contains("'a;b'"), "the literal stays whole");
        assert!(trimmed[1].starts_with("ALTER TABLE"));
        assert!(trimmed[1].contains("E'x;y'"), "the literal stays whole");
        assert!(trimmed[2].starts_with("CREATE INDEX"));
    }

    /// A body the splitter cannot follow ends the answer, not the statement:
    /// the caller then runs the text exactly as it did before the guard
    /// existed. Returning a best guess here would be the one way the guard
    /// could skip a statement that has work to do.
    #[test]
    fn a_text_the_splitter_cannot_follow_is_run_rather_than_read() {
        for open in [
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS a TEXT DEFAULT 'never closed",
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS a TEXT /* never closed",
            "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ SELECT 1; /* ",
        ] {
            assert!(
                statements(open).is_none(),
                "an unclosed construct must not be guessed at: {open}"
            );
        }
        // A dollar-quoted body closes normally, so its semicolons are body text
        // rather than statement boundaries.
        let split = statements("CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;")
            .expect("a closed dollar body is followed");
        assert_eq!(
            split.len(),
            1,
            "the body's semicolon is not an end: {split:?}"
        );
    }

    /// The four shapes worth answering without the database, and the reason
    /// each one is exact rather than approximate: PostgreSQL's own `IF NOT
    /// EXISTS` (and, for `SET NOT NULL`, its own catalog check) already does
    /// nothing when the catalog already says so.
    #[test]
    fn the_guard_reads_the_shapes_postgres_itself_skips() {
        let read = |statement: &str| classify(statement.trim());
        assert_eq!(
            read("CREATE TABLE IF NOT EXISTS entries (id TEXT)"),
            Some(Check::TableExists("entries".into()))
        );
        assert_eq!(
            read("CREATE INDEX IF NOT EXISTS idx ON entries (id)"),
            Some(Check::IndexExists("idx".into()))
        );
        assert_eq!(
            read("CREATE UNIQUE INDEX IF NOT EXISTS idx ON entries (id)"),
            Some(Check::IndexExists("idx".into()))
        );
        assert_eq!(
            read("ALTER TABLE entries ADD COLUMN IF NOT EXISTS seen TIMESTAMPTZ"),
            Some(Check::ColumnExists {
                table: "entries".into(),
                column: "seen".into()
            })
        );
        assert_eq!(
            read("ALTER TABLE entries ALTER COLUMN seen SET NOT NULL"),
            Some(Check::ColumnIsNotNull {
                table: "entries".into(),
                column: "seen".into()
            })
        );
    }

    /// Everything else makes the whole text run. The cases here are the ones
    /// where answering "already done" would be a lie: a statement that has row
    /// effects, one that *fails* when the object is there, and two whose object
    /// this cannot name in the form the catalog would look it up by.
    #[test]
    fn a_shape_this_cannot_name_exactly_is_run() {
        for statement in [
            // Row effects, not catalog facts: the repair pass.
            "UPDATE entries SET seen = at WHERE seen IS NULL",
            "INSERT INTO entries (id) VALUES ('x') ON CONFLICT DO NOTHING",
            // No `IF NOT EXISTS`: this one errors when the object is there.
            "CREATE TABLE entries (id TEXT)",
            "ALTER TABLE entries ADD COLUMN seen TIMESTAMPTZ",
            // A table created elsewhere in the ALTER's own name.
            "ALTER TABLE \"Entries\" ADD COLUMN IF NOT EXISTS seen TIMESTAMPTZ",
            "ALTER TABLE public.entries ADD COLUMN IF NOT EXISTS seen TIMESTAMPTZ",
            // Not a shape this knows at all.
            "DROP TABLE IF EXISTS entries",
            "ALTER TABLE entries DROP COLUMN IF EXISTS seen",
        ] {
            assert_eq!(
                classify(statement),
                None,
                "this must not be answered from the catalog: {statement}"
            );
        }
    }

    /// One unreadable statement makes the batch run -- the guard is
    /// all-or-nothing per text, because the statements share one implicit
    /// transaction and skipping half of it would skip the half that matters.
    #[test]
    fn one_statement_the_guard_cannot_read_runs_the_whole_text() {
        assert!(
            guard_reads(
                "CREATE TABLE IF NOT EXISTS entries (id TEXT);\n\
                 ALTER TABLE entries ADD COLUMN IF NOT EXISTS seen TIMESTAMPTZ;"
            ),
            "a text made only of catalog facts is the one the guard reads"
        );
        assert!(
            !guard_reads(
                "ALTER TABLE entries ADD COLUMN IF NOT EXISTS seen TIMESTAMPTZ;\n\
                 UPDATE entries SET seen = at WHERE seen IS NULL;"
            ),
            "the repair's row effects must not be skipped along with the DDL"
        );
        assert!(
            guard_reads(" \n-- nothing but a comment\n"),
            "a text with no statements has nothing to do, which the guard can say"
        );
    }
}

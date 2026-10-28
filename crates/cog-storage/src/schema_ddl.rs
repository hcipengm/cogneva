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

use cog_core::{SFError, SFResult};
use sqlx::PgPool;
use std::time::Duration;

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

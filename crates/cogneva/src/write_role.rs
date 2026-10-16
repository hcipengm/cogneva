//! `cogneva write-role` — the out-of-band writer's ask, one round long.
//!
//! The in-band discovery loop holds a role, taken from the shared database so
//! that two replicas cannot both run it. It gives the role up on purpose while
//! the LLM pool is unusable: it drops its `RoleHold` and stops renewing, so the
//! term runs out and the role becomes free. The out-of-band writer is not in
//! that process and cannot reach that handle, so this subcommand is its side of
//! the same arbitration — the same row, the same statement, the same clock.
//!
//! Two things make this a subcommand rather than a line of SQL around `psql`:
//!
//! - **The arbitration is one statement and it lives here.** Taking a role is an
//!   upsert whose `WHERE` refuses a live term held by someone else. A second
//!   copy of that statement outside the tree is a second arbiter: the day the
//!   first one changes, the copy keeps handing the same role out and nothing
//!   says the two disagree. So this asks through
//!   [`cog_storage::PgOwnerLeaseBroker`] and lets the row decide — the readings
//!   taken before the ask are for the report, never the verdict.
//! - **Admission is decided by the pool's own verdict, and the cause is carried
//!   in the same write.** Whether the pool can carry LLM-dependent work is the
//!   security gateway's, published on the shared key-value face; nothing here
//!   forms a second opinion from local failures. What the *cause* decides is how
//!   the round is spent: a quota window that will run out is a top-up, while
//!   upstreams that will answer the same way forever are the whole stretch. That
//!   reading travels in the same output as the verdict, so the two cannot be
//!   read a part from each other or half-applied.
//!
//! One ask is one round: it prints, asks, and exits. The term it may take
//! outlives the process by design — [`cog_core::owner_lease::TERM`], renewed by
//! asking again on [`cog_core::owner_lease::ASK_PERIOD`] — and an ask made while
//! the pool carries the work renews nothing, so a writer that stops asking gives
//! the role back inside one term. That is the handover: nobody has to be
//! notified, and a notification that got lost cannot leave the role held.

use cog_core::owner_lease::{ASK_PERIOD, TERM};
use cog_core::OwnerLeaseBroker;
use cog_core::{KeyValueStore, LlmPoolStatus, LlmUpstreamEvidence, LLM_POOL_STATUS_KEY};
use cog_storage::{LeaseRow, PgOwnerLeaseBroker, RedisKeyValueStore};

/// The role asked for when the caller names none: the one the in-band gives up
/// while the pool is unusable, so the one the out-of-band takes over.
pub const DEFAULT_ROLE: &str = cog_reflection::plugin::SELF_DISCOVERY_ROLE;

/// Where the arbiter is configured. The name every other one-shot subcommand
/// reads (`backup`, `restore`), and the one the deployment actually sets.
const DATABASE_URL_ENV: &str = "COGNEVA_DATABASE_URL";

/// Where the pool snapshot's end is configured.
///
/// Read through the application's own configuration rather than off an
/// environment variable named here, because that is the only surface that
/// resolves to a usable URL in a deployed process: the running pod carries the
/// value in `cogneva.json` as `dag_executor.redis_url`, with `${REDIS_PASSWORD}`
/// in it for the loader to substitute — and the variable this command would
/// otherwise have guessed at (`COGNEVA_REDIS_URL`) is the *override* name for
/// that field, which the deployment leaves unset. Guessing it produced a
/// command that runs everywhere except where it is deployed.
const REDIS_URL_SOURCE: &str = "config:dag_executor.redis_url";

/// Explicit overrides, for a caller outside the deployment that reaches the two
/// ends some other way (a forwarded port, a throwaway database).
const ROLE_FLAG: &str = "--role";
const HOLDER_FLAG: &str = "--holder";
const DATABASE_URL_FLAG: &str = "--database-url";
const REDIS_URL_FLAG: &str = "--redis-url";

/// The role was taken or renewed: this writer may act this round.
pub const EXIT_HOLDS_THE_ROLE: i32 = 0;
/// The ask was made and the answer was no. Three different noes — the pool
/// carries the work, another writer holds a live term, or nothing on record
/// says the pool is down — and the printed line names which.
pub const EXIT_MUST_NOT_WRITE: i32 = 1;
/// The ask never left this process: unusable arguments, or an end that is not
/// configured. Nothing was learned about the role or the pool.
pub const EXIT_CANNOT_ASK: i32 = 2;
/// The arbiter could not be read. Not a verdict about the role: an unreachable
/// arbiter and a role someone else holds are different facts.
pub const EXIT_ARBITER_SILENT: i32 = 3;
/// No pool snapshot could be read. Not a verdict about the pool: absence must
/// not be read as health, because taking over moves real code.
pub const EXIT_POOL_SILENT: i32 = 4;

pub const USAGE: &str = "\
cogneva write-role [OPTIONS]

Asks the shared write-role lease from outside the process that usually holds
it, prints what it read and what it decided, and exits:

    --holder <id>      The identity this term is held under. Required: the
                       ambient HOSTNAME names every out-of-band session on a
                       host at once, and inside a pod it names the in-band
                       process.
    --role <name>      The role to ask for (default: the self-discovery role).
    --database-url <u> Override the arbiter's URL. Default: COGNEVA_DATABASE_URL.
    --redis-url <u>    Override the pool snapshot's URL. Default: the
                       application's own configuration (dag_executor.redis_url,
                       which COGNEVA_REDIS_URL overrides).

Exit codes: 0 holds the role, 1 must not write, 2 the ask could not be made,
3 the arbiter could not be read, 4 no pool snapshot could be read.";

/// What the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ask {
    pub role: String,
    pub holder: String,
    /// Explicit overrides, `None` when the caller left the end to its usual
    /// surface.
    pub database_url: Option<String>,
    pub redis_url: Option<String>,
}

/// The two ends of one ask, each with the surface it came from. The URLs
/// themselves are never printed: the deployment's carries the database and
/// Redis passwords, and a report that leaked them would be read by everyone who
/// reads the verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ends {
    pub database_url: String,
    pub database_source: &'static str,
    pub redis_url: String,
    pub redis_source: &'static str,
}

/// Parse the arguments after the subcommand name. Pure, so the rules about
/// what an ask needs are testable without a process around them.
pub fn parse_args(args: &[String]) -> Result<Ask, String> {
    let mut role: Option<String> = None;
    let mut holder: Option<String> = None;
    let mut database_url: Option<String> = None;
    let mut redis_url: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        let value = |i: usize| -> Result<String, String> {
            args.get(i + 1)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("{flag} needs a non-empty value"))
        };
        match flag {
            ROLE_FLAG => {
                role = Some(value(i)?);
                i += 2;
            }
            HOLDER_FLAG => {
                holder = Some(value(i)?);
                i += 2;
            }
            DATABASE_URL_FLAG => {
                database_url = Some(value(i)?);
                i += 2;
            }
            REDIS_URL_FLAG => {
                redis_url = Some(value(i)?);
                i += 2;
            }
            other => return Err(format!("{other} is not an argument of write-role")),
        }
    }
    let holder = holder.ok_or_else(|| {
        format!(
            "{HOLDER_FLAG} is required: a term has to name the writer holding it. The ambient \
             HOSTNAME cannot -- on a host running several out-of-band sessions it names all of \
             them at once, and inside a pod it names the in-band process."
        )
    })?;
    Ok(Ask {
        role: role.unwrap_or_else(|| DEFAULT_ROLE.to_string()),
        holder,
        database_url,
        redis_url,
    })
}

/// Resolve the two ends from the caller's overrides and the surfaces the
/// application itself reads.
///
/// A missing end is reported by name, because "the command could not make the
/// ask" has two quite different fixes: name the end, or configure it where the
/// application reads it.
pub fn resolve_ends(ask: &Ask) -> Result<Ends, String> {
    let (database_url, database_source) = match ask.database_url.clone() {
        Some(url) => (url, DATABASE_URL_FLAG),
        None => match std::env::var(DATABASE_URL_ENV) {
            Ok(url) if !url.trim().is_empty() => (url.trim().to_string(), DATABASE_URL_ENV),
            _ => {
                return Err(format!(
                    "no arbiter URL: pass {DATABASE_URL_FLAG} or set {DATABASE_URL_ENV}"
                ))
            }
        },
    };
    let (redis_url, redis_source) = match ask.redis_url.clone() {
        Some(url) => (url, REDIS_URL_FLAG),
        None => {
            let configured = crate::config_loader::load()
                .dag_executor
                .redis_url
                .trim()
                .to_string();
            if configured.is_empty() {
                return Err(format!(
                    "no pool-snapshot URL: pass {REDIS_URL_FLAG}, or configure \
                     dag_executor.redis_url in the application's config"
                ));
            }
            (configured, REDIS_URL_SOURCE)
        }
    };
    Ok(Ends {
        database_url,
        database_source,
        redis_url,
        redis_source,
    })
}

/// The lease row as a reader outside the loops sees it. Three states rather
/// than a bool, because "a live term belongs to someone else" and "the arbiter
/// could not be read" are the same answer to "may I write" and different
/// answers to "what now".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lease {
    /// No term on record for this role.
    NoTerm,
    /// A term on record, naming `holder`, ending in `seconds_left` seconds. A
    /// `seconds_left` at or below zero is a term that ran out and was not
    /// renewed — the holder that stood down, which is the handover's evidence.
    Term { holder: String, seconds_left: i64 },
    /// The arbiter could not be read.
    Unreadable(String),
}

/// The pool snapshot, in the three states a reader can be in. Absent and
/// unreadable are kept apart: the supervisor's reader answers `None` for both
/// because the gate keeps its last state either way, and an admission decision
/// cannot — "nobody says the pool is down" and "this process could not ask" are
/// different reasons to hold back.
#[derive(Debug, Clone, PartialEq)]
pub enum Pool {
    /// The key is there and parses.
    Snapshot(Box<LlmPoolStatus>),
    /// The key is not there: no snapshot has been published, or its TTL ran
    /// out. Nobody is saying the pool is broken.
    AbsolutelyNothing,
    /// The store could not be read, or what it held did not parse.
    Unreadable(String),
}

/// What this ask may do with the arbiter, from the readings alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Ask. The row's answer is the verdict — never this process's reading of
    /// it, which could be stale by the time the statement runs.
    Ask,
    /// Do not touch the arbiter; here is why.
    StandDown(Refusal),
}

/// Why this ask does not go to the arbiter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The pool carries the work, so the in-band is the writer. This is also
    /// the handback: a term this writer held is simply not renewed.
    TheInBandIsTheWriter,
    /// Nothing on record says the pool is down. Taking over moves real code, so
    /// an unproven outage is not enough to take it.
    ThePoolSaysNothing,
    /// The snapshot could not be read: no verdict about the pool either way.
    ThePoolCouldNotBeRead { detail: String },
}

/// The one decision this command makes. Pure, and it takes the pool reading
/// alone: of the two cross-process values, only the pool says whether the
/// in-band can do the work, and that is what decides whether an ask is owed.
/// The lease reading is the report's — who is writing — and the arbiter's
/// answer, not this process's reading of it, is the verdict.
///
/// The order of the arms is the policy: an unusable pool is the only case with
/// an ask left in it, and an unproven outage is not one.
pub fn step(pool: &Pool) -> Step {
    match pool {
        Pool::Unreadable(detail) => Step::StandDown(Refusal::ThePoolCouldNotBeRead {
            detail: detail.clone(),
        }),
        Pool::AbsolutelyNothing => Step::StandDown(Refusal::ThePoolSaysNothing),
        Pool::Snapshot(status) if !status.unavailable => {
            Step::StandDown(Refusal::TheInBandIsTheWriter)
        }
        Pool::Snapshot(_) => Step::Ask,
    }
}

/// Whether the term on record is this writer's, for the line that says a
/// handback happened rather than never having held anything.
pub fn was_ours(lease: &Lease, holder: &str) -> bool {
    matches!(lease, Lease::Term { holder: h, .. } if h == holder)
}

// ---------------------------------------------------------------------------
// Reading the cause out of the pool snapshot.
// ---------------------------------------------------------------------------

/// The shape of the evidence decides, not the names in it: an upstream that
/// will come back on its own is one that stated an instant or a window, and one
/// that will answer the same way forever has no instant to state. A name table
/// copied here would silently drop a kind the gateway adds.
const WAITING_IS_THE_REPAIR: &str = "waiting_is_the_repair";
const WAITING_CANNOT_REPAIR: &str = "waiting_cannot_repair";
const WAITING_IS_NOT_THE_WHOLE_REPAIR: &str = "waiting_is_not_the_whole_repair";
const UNKNOWN_REPAIR: &str = "unknown";

/// Did this upstream state a clock of its own?
fn stated_a_clock(entry: &LlmUpstreamEvidence) -> bool {
    entry.quota_reset_unix > 0 || entry.quota_window_secs > 0
}

/// The evidence rows whose cause is today's cause, and how that scope was read.
///
/// The payload carries an entry per upstream with anything on record, including
/// ones whose probe window has expired, while the list of what is unusable
/// *right now* is a separate field. Reading the causes off every entry reports a
/// recovered upstream's old refusal as today's cause — last time's reason read
/// as this time's. So the two are intersected, and when the payload carries no
/// list the scope falls back to everything and says so, because a reading whose
/// scope is unknown is weaker than one whose scope is known.
fn suspect_evidence(status: &LlmPoolStatus) -> (Vec<&LlmUpstreamEvidence>, &'static str) {
    if status.unavailable_upstreams.is_empty() {
        (status.upstream_evidence.iter().collect(), "all_evidence")
    } else {
        let rows = status
            .upstream_evidence
            .iter()
            .filter(|entry| {
                status
                    .unavailable_upstreams
                    .iter()
                    .any(|name| name == &entry.identity)
            })
            .collect();
        (rows, "suspects")
    }
}

/// Reads the cause as a decision: **is waiting the repair?**
fn repair_reading(status: &LlmPoolStatus) -> &'static str {
    let (rows, _) = suspect_evidence(status);
    let named: Vec<&&LlmUpstreamEvidence> = rows
        .iter()
        .filter(|entry| !entry.refusal_kind.trim().is_empty())
        .collect();
    if named.is_empty() {
        // No cause was stated at all: a gateway from before this field, or a
        // body that could not be read. Not ours to answer for it.
        return UNKNOWN_REPAIR;
    }
    let clocked = named.iter().filter(|entry| stated_a_clock(entry)).count();
    let pool_level_clock = status.evidenced_recovery_unix > 0 || status.quota_window_secs > 0;
    if pool_level_clock && clocked == 0 {
        // The pool level says an upstream stated a reset time and not one
        // entry carries one. The two faces disagree, and reading that as
        // "waiting will fix it" reads a broken shape as an answer.
        return UNKNOWN_REPAIR;
    }
    if clocked == named.len() {
        WAITING_IS_THE_REPAIR
    } else if clocked == 0 {
        WAITING_CANNOT_REPAIR
    } else {
        WAITING_IS_NOT_THE_WHOLE_REPAIR
    }
}

/// The distinct causes named by the upstreams that are unusable now. Printed
/// for a human reader; nothing here decides on them.
fn causes(status: &LlmPoolStatus) -> Vec<String> {
    let (rows, _) = suspect_evidence(status);
    let mut out: Vec<String> = rows
        .iter()
        .map(|entry| entry.refusal_kind.trim().to_string())
        .filter(|kind| !kind.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// How far this round can be planned from the cause. This is what the cause
/// decides — not whether the role moves, but how the writer that takes it
/// spends the stretch.
fn plan(status: &LlmPoolStatus) -> &'static str {
    match repair_reading(status) {
        WAITING_IS_THE_REPAIR => "top_up_until_the_stated_reset",
        WAITING_CANNOT_REPAIR => "carry_the_whole_stretch",
        _ => "unknown",
    }
}

// ---------------------------------------------------------------------------
// Reading the two cross-process values.
// ---------------------------------------------------------------------------

async fn read_pool(redis_url: &str) -> Pool {
    let store = match RedisKeyValueStore::on_demand(redis_url) {
        Ok(store) => store,
        Err(e) => return Pool::Unreadable(format!("{e}")),
    };
    match store.get(LLM_POOL_STATUS_KEY).await {
        Ok(None) => Pool::AbsolutelyNothing,
        Ok(Some(raw)) if raw.trim().is_empty() => Pool::AbsolutelyNothing,
        Ok(Some(raw)) => match serde_json::from_str::<LlmPoolStatus>(&raw) {
            Ok(status) => Pool::Snapshot(Box::new(status)),
            Err(e) => Pool::Unreadable(format!("{LLM_POOL_STATUS_KEY} did not parse: {e}")),
        },
        Err(e) => Pool::Unreadable(format!("{e}")),
    }
}

async fn read_lease(broker: &PgOwnerLeaseBroker, role: &str) -> Lease {
    match broker.read(role).await {
        Ok(None) => Lease::NoTerm,
        Ok(Some(LeaseRow {
            holder,
            seconds_left,
        })) => Lease::Term {
            holder,
            seconds_left,
        },
        Err(e) => Lease::Unreadable(format!("{e}")),
    }
}

/// One ask of the arbiter. The row answers.
async fn ask_the_arbiter(
    broker: &PgOwnerLeaseBroker,
    ask: &Ask,
) -> Result<bool, Box<dyn std::error::Error>> {
    let lease = broker.lease(&ask.role, TERM);
    Ok(lease.try_hold().await?)
}

// ---------------------------------------------------------------------------
// The report.
// ---------------------------------------------------------------------------

fn lease_lines(lease: &Lease) -> Vec<String> {
    match lease {
        Lease::NoTerm => vec!["lease_reading=no_term_on_record".into()],
        Lease::Term {
            holder,
            seconds_left,
        } => vec![
            "lease_reading=term_on_record".into(),
            format!("lease_holder={holder}"),
            format!("lease_seconds_left={seconds_left}"),
        ],
        Lease::Unreadable(detail) => vec![
            "lease_reading=unreadable".into(),
            format!("lease_error={detail}"),
        ],
    }
}

fn pool_lines(pool: &Pool) -> Vec<String> {
    let mut lines = Vec::new();
    match pool {
        Pool::Unreadable(detail) => {
            lines.push("pool_status=unreadable".into());
            lines.push(format!("pool_error={detail}"));
        }
        Pool::AbsolutelyNothing => lines.push("pool_status=absent".into()),
        Pool::Snapshot(status) => {
            lines.push("pool_status=present".into());
            lines.push(format!("pool_unavailable={}", status.unavailable));
            lines.push(format!("pool_size={}", status.pool_size));
            lines.push(format!(
                "pool_available_upstreams={}",
                status.available_upstreams()
            ));
            lines.push(format!("pool_min_healthy={}", status.min_healthy()));
            // A reading, not the admission: the in-band's own gate pauses on
            // `unavailable` (nothing left that can serve), so this line is what
            // makes it visible when the declared floor and that gate disagree
            // instead of quietly re-deciding who writes.
            //
            // Three states, not two: `pool_size == 0` is a writer that did not
            // carry the field, and a comparison against it would come out
            // "at or above the floor" — a pool nobody described reported as a
            // healthy one.
            lines.push(format!(
                "pool_below_declared_floor={}",
                if status.pool_size == 0 {
                    "unknown".to_string()
                } else {
                    (status.available_upstreams() < status.min_healthy()).to_string()
                }
            ));
            lines.push(format!(
                "pool_evidenced_recovery_unix={}",
                status.evidenced_recovery_unix
            ));
            lines.push(format!(
                "pool_next_attempt_unix={}",
                status.next_attempt_unix
            ));
            lines.push(format!("pool_repair={}", repair_reading(status)));
            lines.push(format!("pool_cause_scope={}", suspect_evidence(status).1));
            lines.push(format!("pool_causes={}", causes(status).join(",")));
            lines.push(format!("plan={}", plan(status)));
        }
    }
    lines
}

/// The whole report, one `key=value` per line.
///
/// The shape is fixed whatever was found: a reader can tell "this process did
/// not ask" from "it asked and there was nothing", and a missing answer is
/// never printed as an answer — the no-verdict cases carry a `no_verdict=` line
/// instead of a `verdict=` one, so a grep for the verdict cannot pick one up.
pub fn report(
    ask: &Ask,
    ends: &Ends,
    lease: &Lease,
    pool: &Pool,
    verdict: Option<(&str, i32)>,
) -> String {
    let mut lines = vec![
        format!("role={}", ask.role),
        format!("holder={}", ask.holder),
        format!("term_secs={}", TERM.as_secs()),
        format!("ask_period_secs={}", ASK_PERIOD.as_secs()),
        // Which surface each end came from, never the URL: the deployment's
        // carries the passwords.
        format!("database_url_source={}", ends.database_source),
        format!("redis_url_source={}", ends.redis_source),
    ];
    lines.extend(lease_lines(lease));
    lines.extend(pool_lines(pool));
    if let Some((verdict, code)) = verdict {
        lines.push(format!("verdict={verdict}"));
        lines.push(format!("exit_code={code}"));
    }
    lines.join("\n") + "\n"
}

/// The no-verdict cases, whose reason is named rather than turned into a
/// verdict-shaped line.
fn no_verdict(ask: &Ask, ends: &Ends, lease: &Lease, pool: &Pool, why: &str, code: i32) -> i32 {
    print!("{}", report(ask, ends, lease, pool, None));
    println!("no_verdict={why}");
    println!("exit_code={code}");
    code
}

/// The subcommand. Returns the exit code; every failure is a code, not an
/// `Err`, because the codes are the interface.
pub async fn run() -> i32 {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let ask = match parse_args(&args) {
        Ok(ask) => ask,
        Err(message) => {
            eprintln!("write-role: {message}");
            eprintln!("{USAGE}");
            return EXIT_CANNOT_ASK;
        }
    };
    let ends = match resolve_ends(&ask) {
        Ok(ends) => ends,
        Err(message) => {
            eprintln!("write-role: {message}");
            return EXIT_CANNOT_ASK;
        }
    };

    let pool = read_pool(&ends.redis_url).await;

    // The arbiter's own reach is a reading like the other two: failing to open
    // it is not a verdict about the role, and the report goes out with it as
    // such rather than with a bare exit code.
    let broker = match sqlx::PgPool::connect(&ends.database_url).await {
        Ok(pg) => match PgOwnerLeaseBroker::with_holder(pg, ask.holder.clone()).await {
            Ok(broker) => Ok(broker),
            Err(e) => Err(format!("the arbiter could not be opened: {e}")),
        },
        Err(e) => Err(format!("the arbiter could not be reached: {e}")),
    };
    let broker = match broker {
        Ok(broker) => broker,
        Err(detail) => {
            eprintln!("write-role: {detail}");
            let lease = Lease::Unreadable(detail);
            return no_verdict(
                &ask,
                &ends,
                &lease,
                &pool,
                "the_arbiter_answered_nothing",
                EXIT_ARBITER_SILENT,
            );
        }
    };
    let lease = read_lease(&broker, &ask.role).await;

    let (verdict, code) = match step(&pool) {
        Step::StandDown(Refusal::TheInBandIsTheWriter) => {
            ("the_in_band_is_the_writer", EXIT_MUST_NOT_WRITE)
        }
        Step::StandDown(Refusal::ThePoolSaysNothing) => {
            ("the_pool_says_nothing", EXIT_MUST_NOT_WRITE)
        }
        Step::StandDown(Refusal::ThePoolCouldNotBeRead { .. }) => {
            return no_verdict(
                &ask,
                &ends,
                &lease,
                &pool,
                "the_pool_could_not_be_read",
                EXIT_POOL_SILENT,
            )
        }
        Step::Ask => match ask_the_arbiter(&broker, &ask).await {
            Ok(true) => (
                if was_ours(&lease, &ask.holder) {
                    "holds_the_role_renewed"
                } else {
                    "holds_the_role"
                },
                EXIT_HOLDS_THE_ROLE,
            ),
            Ok(false) => ("held_by_another_writer", EXIT_MUST_NOT_WRITE),
            Err(e) => {
                eprintln!("write-role: the arbiter answered nothing: {e}");
                return no_verdict(
                    &ask,
                    &ends,
                    &lease,
                    &pool,
                    "the_arbiter_answered_nothing",
                    EXIT_ARBITER_SILENT,
                );
            }
        },
    };
    print!(
        "{}",
        report(&ask, &ends, &lease, &pool, Some((verdict, code)))
    );
    code
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::LlmUpstreamEvidence;

    fn ask(holder: &str) -> Ask {
        Ask {
            role: DEFAULT_ROLE.to_string(),
            holder: holder.to_string(),
            database_url: None,
            redis_url: None,
        }
    }

    /// Two ends that stand in for the real ones. They carry an obvious
    /// password-shaped part so a test can assert it never reaches the report.
    fn ends() -> Ends {
        Ends {
            database_url: "postgres://user:db-secret@arbiter/db".into(),
            database_source: DATABASE_URL_ENV,
            redis_url: "redis://:redis-secret@pool/0".into(),
            redis_source: REDIS_URL_SOURCE,
        }
    }

    fn free() -> Lease {
        Lease::NoTerm
    }

    fn held_by(holder: &str, seconds_left: i64) -> Lease {
        Lease::Term {
            holder: holder.to_string(),
            seconds_left,
        }
    }

    /// A snapshot of a pool where every upstream is unusable, the way the
    /// gateway reports one it has nothing left that can serve.
    fn down_pool() -> Pool {
        let mut status = LlmPoolStatus {
            unavailable: true,
            pool_size: 4,
            unavailable_upstreams: vec!["a|m".into(), "b|m".into()],
            ..Default::default()
        };
        status.upstream_evidence = vec![
            LlmUpstreamEvidence {
                identity: "a|m".into(),
                consecutive_failures: 3,
                quota_reset_unix: 1_792_339_200,
                refusal_kind: "quota_cycle".into(),
                ..Default::default()
            },
            LlmUpstreamEvidence {
                identity: "b|m".into(),
                consecutive_failures: 3,
                quota_reset_unix: 1_792_339_200,
                refusal_kind: "quota_cycle".into(),
                ..Default::default()
            },
        ];
        Pool::Snapshot(Box::new(status))
    }

    fn up_pool() -> Pool {
        Pool::Snapshot(Box::new(LlmPoolStatus {
            unavailable: false,
            pool_size: 4,
            ..Default::default()
        }))
    }

    /// A live pool needs no ask. The lease reading does not enter it — that is
    /// what the signature says — because a pool that serves means the in-band
    /// can do the work whatever the lease row happens to show.
    #[test]
    fn a_pool_that_carries_the_work_needs_no_ask() {
        assert_eq!(
            step(&up_pool()),
            Step::StandDown(Refusal::TheInBandIsTheWriter)
        );
    }

    /// No snapshot is not a licence. Taking over moves real code, so an outage
    /// nobody stated is not proven, and the out-of-band holds back.
    #[test]
    fn a_snapshot_nobody_published_is_not_a_licence() {
        assert_eq!(
            step(&Pool::AbsolutelyNothing),
            Step::StandDown(Refusal::ThePoolSaysNothing)
        );
    }

    /// A snapshot that could not be read is a third state: not a verdict about
    /// the pool, and no ask leaves this process.
    #[test]
    fn a_snapshot_that_could_not_be_read_is_its_own_state() {
        let pool = Pool::Unreadable("no route to the store".into());
        assert_eq!(
            step(&pool),
            Step::StandDown(Refusal::ThePoolCouldNotBeRead {
                detail: "no route to the store".into()
            })
        );
        // The report says which of the two nothing-states this is.
        let text = report(&ask("out-of-band-70"), &ends(), &free(), &pool, None);
        assert!(text.contains("pool_status=unreadable"), "{text}");
        assert!(text.contains("pool_error=no route to the store"), "{text}");
    }

    /// An unusable pool: the ask goes to the arbiter, whose answer is the
    /// verdict — including when the lease row shows a live term, because only
    /// the row's own answer says whether that term is still there.
    #[test]
    fn an_unusable_pool_leaves_an_ask_to_make() {
        assert_eq!(step(&down_pool()), Step::Ask);
    }

    /// The two readings go out together whatever state each is in, and a lease
    /// that could not be read is a third shape — not a blank field a reader
    /// would take for "nobody holds it".
    #[test]
    fn an_unreadable_lease_is_printed_as_its_own_state() {
        let lease = Lease::Unreadable("no route to the arbiter".into());
        let text = report(&ask("out-of-band-70"), &ends(), &lease, &up_pool(), None);
        assert!(text.contains("lease_reading=unreadable"), "{text}");
        assert!(
            text.contains("lease_error=no route to the arbiter"),
            "{text}"
        );
        assert!(!text.contains("lease_holder="), "{text}");
        assert!(text.contains("pool_unavailable=false"), "{text}");
    }

    /// The cause decides the plan, and it reads the *shape* of the evidence:
    /// an upstream that states an instant or a window will come back on its
    /// own, one that states neither will answer the same way forever.
    #[test]
    fn the_repair_reading_comes_from_the_shape_of_the_evidence() {
        let mut clocked = LlmPoolStatus {
            unavailable: true,
            pool_size: 2,
            unavailable_upstreams: vec!["a|m".into()],
            ..Default::default()
        };
        clocked.upstream_evidence = vec![LlmUpstreamEvidence {
            identity: "a|m".into(),
            quota_reset_unix: 1_792_339_200,
            refusal_kind: "whatever the gateway calls it".into(),
            ..Default::default()
        }];
        assert_eq!(repair_reading(&clocked), WAITING_IS_THE_REPAIR);
        assert_eq!(plan(&clocked), "top_up_until_the_stated_reset");

        // The same entry with no clock of its own: waiting is not the repair,
        // and the name it carries does not move the answer.
        let mut clockless = clocked.clone();
        clockless.upstream_evidence[0].quota_reset_unix = 0;
        assert_eq!(repair_reading(&clockless), WAITING_CANNOT_REPAIR);
        assert_eq!(plan(&clockless), "carry_the_whole_stretch");

        // Mixed: some of the pool comes back on its own and some does not.
        let mut mixed = clockless.clone();
        mixed.unavailable_upstreams = vec!["a|m".into(), "b|m".into()];
        mixed.upstream_evidence.push(LlmUpstreamEvidence {
            identity: "b|m".into(),
            quota_reset_unix: 1_792_339_200,
            refusal_kind: "quota_cycle".into(),
            ..Default::default()
        });
        assert_eq!(repair_reading(&mixed), WAITING_IS_NOT_THE_WHOLE_REPAIR);
        assert_eq!(plan(&mixed), "unknown");
    }

    /// The pool level and the per-upstream entries have to agree, or the
    /// reading is unknown: one of them saying a reset time exists while no
    /// entry carries one is a broken shape, not a reason to wait.
    #[test]
    fn the_two_faces_of_the_snapshot_have_to_agree() {
        let mut status = LlmPoolStatus {
            unavailable: true,
            pool_size: 1,
            evidenced_recovery_unix: 1_792_339_200,
            unavailable_upstreams: vec!["a|m".into()],
            ..Default::default()
        };
        status.upstream_evidence = vec![LlmUpstreamEvidence {
            identity: "a|m".into(),
            refusal_kind: "quota_cycle".into(),
            ..Default::default()
        }];
        assert_eq!(repair_reading(&status), UNKNOWN_REPAIR);
    }

    /// The cause is read off the upstreams that are unusable now, not off every
    /// entry on record: a recovered upstream's old refusal is last time's
    /// reason, and reading it as today's is the mistake this scope prevents.
    #[test]
    fn the_cause_is_scoped_to_what_is_unusable_now() {
        let mut status = LlmPoolStatus {
            unavailable: true,
            pool_size: 2,
            unavailable_upstreams: vec!["b|m".into()],
            ..Default::default()
        };
        status.upstream_evidence = vec![
            LlmUpstreamEvidence {
                identity: "a|m".into(),
                refusal_kind: "entitlement".into(),
                ..Default::default()
            },
            LlmUpstreamEvidence {
                identity: "b|m".into(),
                quota_window_secs: 604_800,
                refusal_kind: "quota_cycle".into(),
                ..Default::default()
            },
        ];
        assert_eq!(suspect_evidence(&status).1, "suspects");
        assert_eq!(causes(&status), vec!["quota_cycle".to_string()]);
        assert_eq!(repair_reading(&status), WAITING_IS_THE_REPAIR);

        // With no list of what is unusable, the scope is every entry — and the
        // report says so rather than passing the wider reading off as the same
        // one.
        let mut unscoped = status.clone();
        unscoped.unavailable_upstreams.clear();
        assert_eq!(suspect_evidence(&unscoped).1, "all_evidence");
        assert_eq!(
            causes(&unscoped),
            vec!["entitlement".to_string(), "quota_cycle".to_string()]
        );
        assert_eq!(repair_reading(&unscoped), WAITING_IS_NOT_THE_WHOLE_REPAIR);
    }

    /// A holder has to be named, and the default role is the contract's, not a
    /// string spelled here.
    #[test]
    fn the_ask_names_a_holder_and_defaults_the_role() {
        let ask = parse_args(&["--holder".into(), "out-of-band-70".into()]).expect("an ask");
        assert_eq!(ask.holder, "out-of-band-70");
        assert_eq!(ask.role, DEFAULT_ROLE);
        assert!(ask.database_url.is_none() && ask.redis_url.is_none());

        let named = parse_args(&[
            "--role".into(),
            "other_role".into(),
            "--holder".into(),
            "me".into(),
        ])
        .expect("an ask");
        assert_eq!(named.role, "other_role");

        let missing = parse_args(&[]).expect_err("a holder is required");
        assert!(missing.contains(HOLDER_FLAG), "{missing}");
        assert!(
            parse_args(&["--holder".into(), "  ".into()]).is_err(),
            "a blank holder is not an identity"
        );
        assert!(parse_args(&["--keeper".into(), "me".into()]).is_err());
    }

    /// The ends come from the surfaces the application itself reads, and the
    /// report says which one each came from — never the URL, because the
    /// deployment's carries the database and Redis passwords.
    #[test]
    fn the_ends_are_named_by_their_source_and_not_printed() {
        let mut named = ask("out-of-band-70");
        named.database_url = Some("postgres://user:flag-secret@other/db".into());
        named.redis_url = Some("redis://:flag-secret@other/1".into());
        let resolved = resolve_ends(&named).expect("both ends were named");
        assert_eq!(resolved.database_source, DATABASE_URL_FLAG);
        assert_eq!(resolved.redis_source, REDIS_URL_FLAG);
        // A named override wins outright: the caller said where to aim.
        assert_eq!(resolved.database_url, named.database_url.clone().unwrap());
        assert_eq!(resolved.redis_url, named.redis_url.clone().unwrap());

        let text = report(&named, &resolved, &free(), &up_pool(), None);
        assert!(
            text.contains(&format!("database_url_source={DATABASE_URL_FLAG}")),
            "{text}"
        );
        assert!(
            text.contains(&format!("redis_url_source={REDIS_URL_FLAG}")),
            "{text}"
        );
        assert!(
            !text.contains("flag-secret"),
            "the report leaked a URL: {text}"
        );

        // The default surfaces are the ones the application reads, and they are
        // printed under their own names.
        let defaults = report(&ask("out-of-band-70"), &ends(), &free(), &up_pool(), None);
        assert!(
            defaults.contains(&format!("database_url_source={DATABASE_URL_ENV}")),
            "{defaults}"
        );
        assert!(
            defaults.contains(&format!("redis_url_source={REDIS_URL_SOURCE}")),
            "{defaults}"
        );
        assert!(!defaults.contains("db-secret"), "{defaults}");
        assert!(!defaults.contains("redis-secret"), "{defaults}");
    }

    /// Every answer has its own code, and the two silent inputs have two codes:
    /// a caller has to be able to tell which end was missing.
    #[test]
    fn the_exit_codes_are_distinct_per_answer() {
        let codes = [
            EXIT_HOLDS_THE_ROLE,
            EXIT_MUST_NOT_WRITE,
            EXIT_CANNOT_ASK,
            EXIT_ARBITER_SILENT,
            EXIT_POOL_SILENT,
        ];
        let mut seen = codes.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), codes.len(), "two answers share a code");
        assert!(
            USAGE.contains("--holder"),
            "the usage names the required flag"
        );
    }

    /// The report carries the verdict and the evidence in one block, and a
    /// missing answer is a different field: a reader grepping for the verdict
    /// cannot pick up a case where none was reached.
    #[test]
    fn a_missing_answer_is_not_printed_as_a_verdict() {
        let text = report(
            &ask("out-of-band-70"),
            &ends(),
            &held_by("out-of-band-70", 240),
            &down_pool(),
            Some(("holds_the_role_renewed", EXIT_HOLDS_THE_ROLE)),
        );
        assert!(text.contains("verdict=holds_the_role_renewed"), "{text}");
        assert!(text.contains("lease_holder=out-of-band-70"), "{text}");
        assert!(text.contains("lease_seconds_left=240"), "{text}");
        assert!(text.contains("pool_repair=waiting_is_the_repair"), "{text}");
        assert!(
            text.contains("plan=top_up_until_the_stated_reset"),
            "{text}"
        );
        // The cadence the caller renews on is the contract's, printed rather
        // than left for each writer to invent.
        assert!(
            text.contains(&format!("ask_period_secs={}", ASK_PERIOD.as_secs())),
            "{text}"
        );
        assert!(
            text.contains(&format!("term_secs={}", TERM.as_secs())),
            "{text}"
        );

        let silent = report(
            &ask("out-of-band-70"),
            &ends(),
            &free(),
            &Pool::AbsolutelyNothing,
            None,
        );
        assert!(!silent.contains("verdict="), "{silent}");
    }

    /// The floor comparison is a reading of the pool, and the name it carries
    /// must not read as permission: the admission is the snapshot's own
    /// verdict on whether anything can serve.
    #[test]
    fn the_declared_floor_is_reported_and_not_decided_on() {
        let mut status = LlmPoolStatus {
            unavailable: false,
            pool_size: 4,
            declared_min_healthy: Some(2),
            unavailable_upstreams: vec!["a|m".into(), "b|m".into(), "c|m".into()],
            ..Default::default()
        };
        status.upstream_evidence = vec![LlmUpstreamEvidence {
            identity: "a|m".into(),
            quota_reset_unix: 1_792_339_200,
            refusal_kind: "quota_cycle".into(),
            ..Default::default()
        }];
        let text = report(
            &ask("out-of-band-70"),
            &ends(),
            &free(),
            &Pool::Snapshot(Box::new(status)),
            None,
        );
        assert!(text.contains("pool_below_declared_floor=true"), "{text}");
        // One upstream of four serves and the floor is two, yet the snapshot's
        // own verdict is that something serves: the out-of-band stays out, and
        // the line above is what makes the disagreement visible.
        assert!(!text.contains("verdict="), "{text}");
    }

    /// A payload that does not carry the pool size is not a pool above its
    /// floor. `0` there is "the writer did not say", and the comparison has to
    /// answer in three states rather than fold that into a clean bill.
    #[test]
    fn an_unreported_pool_size_is_not_a_pool_above_its_floor() {
        let status = LlmPoolStatus {
            unavailable: true,
            ..Default::default()
        };
        assert_eq!(status.pool_size, 0);
        let text = report(
            &ask("out-of-band-70"),
            &ends(),
            &free(),
            &Pool::Snapshot(Box::new(status)),
            None,
        );
        assert!(text.contains("pool_below_declared_floor=unknown"), "{text}");
        assert!(!text.contains("pool_below_declared_floor=false"), "{text}");
    }
}

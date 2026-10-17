//! PostgreSQL-backed LLM token usage ledger.
//!
//! The gateway is the single point all LLM traffic converges on, which makes it
//! the only place per-call token consumption can be metered. Unlike the quota
//! crate's tenant-billing schema (keyed by user/workspace), this ledger records
//! infrastructure-level usage keyed by upstream+model: it answers "how many
//! tokens did we burn, where, and on what" without needing tenant context that
//! proxied calls do not carry.
//!
//! **This table is the single authoritative ledger for "who burned how much".**
//! Every metered call is also copied to two other places, and each exists for a
//! job this table cannot do well — but none of them is the place a spending
//! question is answered:
//!
//! - the Prometheus `llm_tokens_total` series is the realtime face the panels
//!   read. It lives inside the gateway process and is split per pod, so it
//!   starts over at zero on every gateway rollout and is dropped after the
//!   scrape retention; an answer that has to survive a restart cannot come from
//!   it;
//! - the ClickHouse `llm_usage` event is the high-throughput detail face, an
//!   append-only copy of the same row's attributes.
//!
//! A new reading of token spend must resolve to this ledger rather than adding
//! a fourth copy: three copies already disagree in ways a reader has to be told
//! about, and a fourth would only add a further opinion.
//!
//! One near-copy is deliberately *not* this ledger and must not be folded into
//! it: the quota middleware parses the inbound response's `usage.total_tokens`
//! to charge a tenant. It answers "how much should this caller pay", keyed by
//! user/workspace, and its number may be an estimate when the upstream reported
//! nothing at all. This table answers "what did this upstream serve", keyed by
//! upstream+model+actor, and only ever records the upstream's own reading or a
//! known zero — never an estimate. The two are allowed to disagree on the same
//! call, and the disagreement is not a defect on either side: they count at
//! different layers for different owners.
//!
//! Per-call rows are fine-grained but answered only by scanning a table that
//! grows forever, and a question asked repeatedly ("where did last Tuesday's
//! tokens go") should not make every reader scan it. So the ledger is also
//! folded into [`ROLLUP_TABLE`], one row per actor × origin × upstream ×
//! protocol per closed hour. The fold is derived — every row in it can be recomputed from
//! this table — which is what makes it legitimate to run from a process that
//! restarts: a window rolled twice lands the same key with the same values, and
//! a window missed while the process was down is rolled on the next pass. The
//! rollup reads *through* this ledger; it is not a second place a spend reading
//! is decided.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

/// The ledger's per-call table.
pub const LEDGER_TABLE: &str = "gateway_llm_usage";

/// The durable per-window fold of [`LEDGER_TABLE`].
pub const ROLLUP_TABLE: &str = "cog_llm_usage_rollup";

/// Inbound header a caller sets to name the environment a call is made from.
///
/// The ledger's other dimensions say what was called and who called it; this
/// one says where the call was made from, which is the only face that answers
/// "which environment burned these tokens". It rides a header rather than being
/// read off the caller's identity because the same component runs in more than
/// one environment: a table mapping names to environments would go stale the
/// moment a deployment moved, while the caller always knows where it is.
///
/// Defined in the contract layer rather than here, because the crates that set
/// it do not depend on the crate that reads it — spelled out in full at the
/// definition site.
pub use cog_core::LLM_ORIGIN_HEADER;

/// The environment a metered call was made in, as a closed set.
///
/// Closed because it is read as counts and a reader selects rows by it: a value
/// outside the set is not a new environment, it is a caller that did not say.
/// [`Self::declared`] is the only way in, and [`Self::Unknown`] is deliberately
/// outside the four -- an undeclared caller is not attributable to any
/// environment, and folding it into one would credit that environment with
/// tokens it did not burn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LlmOrigin {
    /// The platform's own task pool driving agents (planner, generator, ...).
    GatewayPool,
    /// A long-lived resident worker session.
    ResidentWorker,
    /// An interactive, human-driven session.
    Interactive,
    /// A sandbox executor run.
    Sandbox,
    /// No usable declaration arrived. Not a place a call comes from, so it is
    /// kept out of [`Self::DECLARED`] and out of any environment's total.
    Unknown,
}

impl LlmOrigin {
    /// The four an environment may declare, in the order the ledger reads them.
    /// [`Self::Unknown`] is not one of them.
    pub const DECLARED: [LlmOrigin; 4] = [
        LlmOrigin::GatewayPool,
        LlmOrigin::ResidentWorker,
        LlmOrigin::Interactive,
        LlmOrigin::Sandbox,
    ];

    /// The origin a caller declared, or [`Self::Unknown`] when it declared none
    /// or named one outside the set.
    ///
    /// An unrecognised value reads as "nothing was declared" rather than being
    /// stored as free text: the column is a closed set a reader groups by, and
    /// letting an arbitrary token in would make every new misspelling its own
    /// series. Trimming is the only repair -- a value that names an environment
    /// after trimming is accepted.
    pub fn declared(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some("gateway_pool") => LlmOrigin::GatewayPool,
            Some("resident_worker") => LlmOrigin::ResidentWorker,
            Some("interactive") => LlmOrigin::Interactive,
            Some("sandbox") => LlmOrigin::Sandbox,
            _ => LlmOrigin::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LlmOrigin::GatewayPool => "gateway_pool",
            LlmOrigin::ResidentWorker => "resident_worker",
            LlmOrigin::Interactive => "interactive",
            LlmOrigin::Sandbox => "sandbox",
            LlmOrigin::Unknown => "unknown",
        }
    }
}

/// One metered LLM call.
#[derive(Debug, Clone)]
pub struct LlmUsageRecord {
    /// Pool identity of the upstream that served the call.
    pub upstream: String,
    /// Wire protocol family (`openai` / `anthropic`).
    pub api_style: String,
    /// Real model name as configured on the upstream.
    pub model: String,
    /// `ok`, `error`, or `refused_by_circuit` — the last two carry zero tokens
    /// but keep the attempt visible. The refusal is kept apart from `error`
    /// because they name opposite repairs: an `error` row is an upstream that
    /// was tried and did not serve, a refusal row is one that was never tried
    /// because the whole pool was known unusable, and the second is what makes
    /// a down window readable instead of reading like a window with no traffic.
    /// On a refusal row `upstream` and `model` are `-`: no single upstream is
    /// its subject, so both columns say so rather than naming a candidate.
    pub result: String,
    /// Calling component as normalized by the gateway (`self_review`,
    /// `agent:<role>`, `unknown`); lets per-actor token spend be audited.
    pub actor: String,
    /// Environment the call was made from, as one of [`LlmOrigin`]'s values --
    /// the gateway's reading of the caller's own [`LLM_ORIGIN_HEADER`]. Answers
    /// a question `actor` cannot: the same component runs in more than one
    /// environment, so "who spent this" and "where it was spent" are two axes.
    pub origin: String,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// Input tokens the upstream served from its own prefix cache. Its own
    /// column rather than a fold into `tokens_input`: the ratio of the two is
    /// the hit rate, and that ratio is the only reading a change to prompt
    /// prefix stability can be accepted on, so a durable audit of token spend
    /// that cannot answer it is missing the one number it exists for. How the
    /// two relate follows the wire protocol (on an OpenAI-compatible upstream
    /// cached is a subset of input, on Anthropic the two are disjoint), which
    /// is why they stay apart here instead of being netted out.
    pub tokens_cached: u64,
    /// What this platform's own estimator priced the request at, before any
    /// upstream counted the prompt.
    ///
    /// The row's other input figure is the upstream's truth; this one is the
    /// estimate the window budget is judged against, so one row carries both
    /// rulers and the gap between them is readable. The estimate is a
    /// deliberate lower bound ([`cog_core::estimate_tokens`], the unit the
    /// agent's context window trims by) over the serialized body this gateway
    /// sent, so `estimated_input` below `tokens_input` is expected and its
    /// *size* is the finding: a gap that tracks the content mix is the
    /// estimator's, a request the estimator also called over budget is the
    /// window's. Without both numbers on the same row those two read alike,
    /// which is why this is a column and not a counter of over-budget calls.
    ///
    /// `None` on rows landed by a path that does not price a request (failed
    /// attempts and pooled refusals). Absent is not zero here: a row priced at
    /// zero would read as an empty prompt, and the two have opposite repairs.
    pub estimated_input: Option<u64>,
    /// The unit of work this call was spent for, as the caller declared it --
    /// the same identity the caller reports its own per-run totals under.
    ///
    /// Nullable on purpose, like `estimated_input` and for the same reason:
    /// `NULL` is "this call was not attributed to a job", which is a different
    /// statement from "it was spent for job `unknown`". A NOT NULL default would
    /// put every unattributed call in one group and read as a single very
    /// expensive job, so rows landed by paths that have no job identity keep it
    /// empty rather than inventing one.
    pub task: Option<String>,
    /// Wall time of the call; for streams this is first-byte to last-byte.
    pub latency_ms: u64,
}

/// One `(actor, upstream, api_style)` line of a window's token composition.
///
/// `failed_calls` is not folded into `calls`: an error row carries zero tokens
/// by construction (the upstream never served a completion), so a window whose
/// every call failed and a window with no traffic at all sum to the same token
/// count. Keeping the failure count beside the token totals is what lets a
/// reader tell the two apart instead of reading "this path records nothing".
///
/// A pooled refusal counts here too, and reads off on its own: its rows are the
/// only ones whose `upstream` is `-`, so the group answers "how many calls were
/// turned away before any upstream was tried" without the reader having to
/// subtract anything. Excluding it instead would make a group of refusals read
/// as `calls > 0, failed = 0, tokens = 0` — a path that answered everything and
/// spent nothing, which is the one reading that is certainly false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorSpend {
    pub actor: String,
    /// Environment the calls were made from, one of [`LlmOrigin`]'s values.
    /// Carried beside `actor` rather than folded into it: the same component
    /// can call from more than one environment, and a reader asking "which
    /// environment burned this" needs it as its own axis.
    pub origin: String,
    pub upstream: String,
    /// Wire protocol family the tokens were counted under. Carried because
    /// `tokens_input` means different things per protocol — on an
    /// OpenAI-compatible upstream it already contains `tokens_cached`, on
    /// Anthropic the two are disjoint — so a cache-hit ratio is only meaningful
    /// within one protocol and the column may not be dropped on the way out.
    pub api_style: String,
    pub calls: i64,
    pub failed_calls: i64,
    pub tokens_input: i64,
    pub tokens_output: i64,
    pub tokens_cached: i64,
    pub avg_latency_ms: i64,
}

/// One rolled read: the spend, and the range of windows it was summed over.
#[derive(Debug, Clone)]
pub struct RolledSpend {
    /// Start of the oldest window summed, and end of the newest. Both are
    /// `None` — never one without the other — when no closed window fell inside
    /// the range that was asked for.
    ///
    /// Read out of the rollup rather than echoed from the arguments: only
    /// fully-contained windows are summed, so the range the rows cover is in
    /// general narrower than the range asked for, and a caller given only the
    /// second cannot tell "nothing was spent here" from "the fold has not
    /// reached here yet". Those two are the same shape of answer — an empty
    /// list — which is why the range that was actually read has to travel with
    /// it.
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_until: Option<DateTime<Utc>>,
    pub rows: Vec<ActorSpend>,
}

/// One closed accounting window, `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollupWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// The windows that are closed at `now` and not yet rolled, oldest first.
///
/// A window is rollable once it has ended, so the newest candidate ends at the
/// grid point at or before `now`. `last_end` is the end of the newest window
/// already in the rollup table, or `None` on a first run — in which case the
/// reach is bounded by `lookback_secs` rather than starting at the beginning of
/// the ledger, because the point of catch-up is the recent holes, not replaying
/// all history. Both bounds are on the window grid, so a rerun of an
/// already-rolled window produces the same `(start, end)` key: the fold's
/// idempotence rests on windows being a function of the grid, not of when a
/// process happened to look.
pub fn windows_to_roll(
    last_end: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    window_secs: i64,
    lookback_secs: i64,
) -> Vec<RollupWindow> {
    let w = window_secs.max(1);
    // Boundary of the currently-open window: the end of the newest closed one.
    let newest_end = now.timestamp() - now.timestamp().rem_euclid(w);
    let lowest = newest_end - lookback_secs.max(w);
    let mut start = match last_end {
        Some(end) => {
            let t = end.timestamp();
            let rem = t.rem_euclid(w);
            if rem == 0 {
                t
            } else {
                t + (w - rem)
            }
        }
        // Only a first run is bounded by the lookback; a run that has a starting
        // point folds everything after it, so a gap longer than the lookback is
        // filled rather than skipped.
        None => lowest,
    };
    let mut windows = Vec::new();
    while start + w <= newest_end {
        // `from_timestamp` only fails outside the representable range, which a
        // window derived from "now" is not.
        let (Some(s), Some(e)) = (
            DateTime::from_timestamp(start, 0),
            DateTime::from_timestamp(start + w, 0),
        ) else {
            break;
        };
        windows.push(RollupWindow { start: s, end: e });
        start += w;
    }
    windows
}

/// PostgreSQL usage store.
#[derive(Clone)]
pub struct LlmUsageStore {
    pool: PgPool,
}

impl LlmUsageStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn init_schema(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS gateway_llm_usage (
                id            UUID PRIMARY KEY,
                ts            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                upstream      TEXT NOT NULL,
                api_style     TEXT NOT NULL,
                model         TEXT NOT NULL,
                result        TEXT NOT NULL,
                tokens_input  BIGINT NOT NULL DEFAULT 0,
                tokens_output BIGINT NOT NULL DEFAULT 0,
                tokens_cached BIGINT NOT NULL DEFAULT 0,
                latency_ms    BIGINT NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        // Additive migration for the per-actor dimension; old rows stay
        // attributable as "unknown" without a table rewrite.
        sqlx::query(
            "ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT 'unknown'",
        )
        .execute(&self.pool)
        .await?;
        // Same shape for the cache dimension. Rows written before this column
        // existed default to 0, and 0 is honest for them as long as the reader
        // treats "cached" on a row whose input is also 0 as "not metered"
        // rather than "nothing was cached" -- the two are indistinguishable
        // after the fact, which is exactly why the column is written going
        // forward rather than reconstructed backward.
        sqlx::query(
            "ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS tokens_cached BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await?;
        // Additive migration for the environment dimension, same shape as the
        // actor one above. Rows written before it existed default to `unknown`,
        // which is the honest reading -- they are not attributable to any one
        // environment and must not be counted into one.
        sqlx::query(
            "ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'unknown'",
        )
        .execute(&self.pool)
        .await?;
        // Additive migration for the estimate dimension, and the one column here
        // that is nullable on purpose. "This row was not priced" (older rows, and
        // rows landed by the failure path) is not "this row was priced at zero" --
        // a NOT NULL DEFAULT 0 would make both read as an empty prompt, and a
        // reader grouping by the gap between the two rulers has to handle the
        // absence rather than silently pair it with the truth.
        sqlx::query(
            "ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS estimated_input BIGINT",
        )
        .execute(&self.pool)
        .await?;
        // The per-job dimension. Nullable because "no job identity" is a state
        // of its own: rows landed by paths that cannot name a job (an upstream
        // failure recorded before routing, the gateway's own judgement calls)
        // must not be gathered into one placeholder group that would then read
        // as the most expensive job on the ledger. Text, not UUID: the caller
        // declares whatever identity its own per-run census uses, and the
        // reader's job is to join the two, not to funnel both through a type.
        sqlx::query("ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS task_id TEXT")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_gateway_llm_usage_ts ON gateway_llm_usage (ts)",
        )
        .execute(&self.pool)
        .await?;
        // The window fold. `window_start`/`window_end` are the row's own
        // boundary fields, not a `now()` stamped at insert: a rerun has to key
        // and compare on the window it is recomputing, and a timestamp taken
        // when the row happened to be written would make the same window look
        // like a new one. The primary key is the whole identity of a rollup row,
        // which is what makes a rerun collide with itself and update in place.
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {ROLLUP_TABLE} (
                window_start   TIMESTAMPTZ NOT NULL,
                window_end     TIMESTAMPTZ NOT NULL,
                actor          TEXT NOT NULL,
                origin         TEXT NOT NULL,
                upstream       TEXT NOT NULL,
                api_style      TEXT NOT NULL,
                calls          BIGINT NOT NULL DEFAULT 0,
                failed_calls   BIGINT NOT NULL DEFAULT 0,
                tokens_input   BIGINT NOT NULL DEFAULT 0,
                tokens_output  BIGINT NOT NULL DEFAULT 0,
                tokens_cached  BIGINT NOT NULL DEFAULT 0,
                avg_latency_ms BIGINT NOT NULL DEFAULT 0,
                first_rolled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (window_start, window_end, actor, origin, upstream, api_style)
            )
            "#
        ))
        .execute(&self.pool)
        .await?;
        // The environment dimension reaches the fold as well as the ledger: a
        // fold that dropped an axis the ledger carries would lose it, and the
        // question this axis exists for is read again and again -- which is what
        // the fold is for. Both steps are guarded, so a table already carrying
        // the column and key is left alone. The new column goes **last** in the
        // key: every row written before it existed carries the same default, so
        // the enlarged key is still unique and the swap is safe in place, with
        // no rebuild -- which matters because a first fold is bounded by the
        // lookback and could not re-create history older than it.
        sqlx::query(&format!(
            r#"
            DO $mig$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{ROLLUP_TABLE}' AND column_name = 'origin'
                ) THEN
                    ALTER TABLE {ROLLUP_TABLE}
                        ADD COLUMN origin TEXT NOT NULL DEFAULT 'unknown';
                END IF;
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = '{ROLLUP_TABLE}'::regclass AND contype = 'p'
                      AND pg_get_constraintdef(oid) NOT LIKE '%origin%'
                ) THEN
                    ALTER TABLE {ROLLUP_TABLE} DROP CONSTRAINT {ROLLUP_TABLE}_pkey;
                    ALTER TABLE {ROLLUP_TABLE}
                        ADD PRIMARY KEY
                            (window_start, window_end, actor, origin, upstream, api_style);
                END IF;
            END
            $mig$;
            "#
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record(&self, r: &LlmUsageRecord) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateway_llm_usage
                (id, upstream, api_style, model, result, actor, origin,
                 tokens_input, tokens_output, tokens_cached, estimated_input,
                 task_id, latency_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(&r.upstream)
        .bind(&r.api_style)
        .bind(&r.model)
        .bind(&r.result)
        .bind(&r.actor)
        .bind(&r.origin)
        .bind(r.tokens_input as i64)
        .bind(r.tokens_output as i64)
        .bind(r.tokens_cached as i64)
        .bind(r.estimated_input.map(|v| v as i64))
        .bind(r.task.as_deref())
        .bind(r.latency_ms as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The window's token composition, grouped by `(actor, origin, upstream,
    /// api_style)`.
    ///
    /// This is the reader A1 promises: a spend question is answered from the
    /// ledger rather than from a running process's counters. It aggregates over
    /// `[since, until)` and returns one line per group, ordered so two reads of
    /// the same window are comparable line for line.
    ///
    /// Failed calls are counted but not netted out of the totals, so the caller
    /// can layer on `result` (a window of failures is not a window of silence).
    pub async fn usage_by_actor(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<ActorSpend>> {
        let rows = sqlx::query(&format!(
            r#"
            SELECT actor, origin, upstream, api_style,
                   COUNT(*) AS calls,
                   COUNT(*) FILTER (WHERE result <> 'ok') AS failed_calls,
                   COALESCE(SUM(tokens_input), 0)::BIGINT  AS tokens_input,
                   COALESCE(SUM(tokens_output), 0)::BIGINT AS tokens_output,
                   COALESCE(SUM(tokens_cached), 0)::BIGINT AS tokens_cached,
                   COALESCE(AVG(latency_ms), 0)::BIGINT    AS avg_latency_ms
            FROM {LEDGER_TABLE}
            WHERE ts >= $1 AND ts < $2
            GROUP BY actor, origin, upstream, api_style
            ORDER BY actor, origin, upstream, api_style
            "#
        ))
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| ActorSpend {
                actor: r.get("actor"),
                origin: r.get("origin"),
                upstream: r.get("upstream"),
                api_style: r.get("api_style"),
                calls: r.get("calls"),
                failed_calls: r.get("failed_calls"),
                tokens_input: r.get("tokens_input"),
                tokens_output: r.get("tokens_output"),
                tokens_cached: r.get("tokens_cached"),
                avg_latency_ms: r.get("avg_latency_ms"),
            })
            .collect())
    }

    /// Every `(actor, origin, upstream, api_style)` the ledger has ever recorded.
    ///
    /// This is the set a window is completed against so that "no traffic" still
    /// lands rows instead of vanishing. It is read off the ledger rather than
    /// declared, because the vocabulary is open (`agent:{role}` is a role
    /// string, and origins are whatever callers declare) and any declared list
    /// would go stale the moment a new role ran or a new caller appeared. Any
    /// combination that has ever fired a call gets a row in every later window,
    /// zero when idle, which is what makes a dead upstream period
    /// distinguishable from a period the fold did not run.
    pub async fn known_combos(&self) -> anyhow::Result<Vec<(String, String, String, String)>> {
        let rows = sqlx::query(&format!(
            "SELECT DISTINCT actor, origin, upstream, api_style FROM {LEDGER_TABLE} \
             ORDER BY actor, origin, upstream, api_style"
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("actor"),
                    r.get::<String, _>("origin"),
                    r.get::<String, _>("upstream"),
                    r.get::<String, _>("api_style"),
                )
            })
            .collect())
    }

    /// Fold each closed window into [`ROLLUP_TABLE`], landing a row for every
    /// combination in the window's own composition and a zero row for every
    /// combination the ledger knows but this window did not use.
    ///
    /// Idempotent by primary key: rerunning a window updates the same rows in
    /// place. The token totals are a function of the ledger's rows inside the
    /// window, so a rerun recomputes the same numbers — which is what lets the
    /// fold be legal from a process that restarts, and what makes the fold a
    /// derivation of the ledger rather than a second source of truth.
    ///
    /// Returns how many rows were written (inserted or updated).
    pub async fn rollup_windows(&self, windows: &[RollupWindow]) -> anyhow::Result<u64> {
        if windows.is_empty() {
            return Ok(0);
        }
        // The vocabulary is fetched once per pass rather than once per window:
        // it is the same set for all of them, and reading it per window would
        // multiply a full-table DISTINCT by the number of windows in a catch-up.
        let vocab = self.known_combos().await?;
        let insert = format!(
            r#"
            INSERT INTO {ROLLUP_TABLE}
                (window_start, window_end, actor, origin, upstream, api_style,
                 calls, failed_calls, tokens_input, tokens_output, tokens_cached,
                 avg_latency_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (window_start, window_end, actor, origin, upstream, api_style)
            DO UPDATE SET
                calls          = EXCLUDED.calls,
                failed_calls   = EXCLUDED.failed_calls,
                tokens_input   = EXCLUDED.tokens_input,
                tokens_output  = EXCLUDED.tokens_output,
                tokens_cached  = EXCLUDED.tokens_cached,
                avg_latency_ms = EXCLUDED.avg_latency_ms
            "#
        );
        let mut written = 0u64;
        for w in windows {
            let spend = self.usage_by_actor(w.start, w.end).await?;
            let mut seen: HashSet<(String, String, String, String)> =
                HashSet::with_capacity(spend.len());
            for s in &spend {
                sqlx::query(&insert)
                    .bind(w.start)
                    .bind(w.end)
                    .bind(&s.actor)
                    .bind(&s.origin)
                    .bind(&s.upstream)
                    .bind(&s.api_style)
                    .bind(s.calls)
                    .bind(s.failed_calls)
                    .bind(s.tokens_input)
                    .bind(s.tokens_output)
                    .bind(s.tokens_cached)
                    .bind(s.avg_latency_ms)
                    .execute(&self.pool)
                    .await?;
                seen.insert((
                    s.actor.clone(),
                    s.origin.clone(),
                    s.upstream.clone(),
                    s.api_style.clone(),
                ));
                written += 1;
            }
            for (actor, origin, upstream, api_style) in &vocab {
                if seen.contains(&(
                    actor.clone(),
                    origin.clone(),
                    upstream.clone(),
                    api_style.clone(),
                )) {
                    continue;
                }
                sqlx::query(&insert)
                    .bind(w.start)
                    .bind(w.end)
                    .bind(actor)
                    .bind(origin)
                    .bind(upstream)
                    .bind(api_style)
                    .bind(0i64)
                    .bind(0i64)
                    .bind(0i64)
                    .bind(0i64)
                    .bind(0i64)
                    .bind(0i64)
                    .execute(&self.pool)
                    .await?;
                written += 1;
            }
        }
        Ok(written)
    }

    /// End of the newest window already folded, or `None` when the rollup table
    /// is empty. The fold's catch-up starts here, so a restart resumes at the
    /// first window it has not yet landed rather than replaying history.
    pub async fn last_rolled_window_end(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        let row = sqlx::query(&format!(
            "SELECT MAX(window_end) AS last FROM {ROLLUP_TABLE}"
        ))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.get::<Option<DateTime<Utc>>, _>("last")))
    }

    /// Token spend per `(actor, origin, upstream, api_style)` over the closed windows
    /// inside `[since, until)`, summed across them, together with the endpoints
    /// of the range those windows cover.
    ///
    /// Reads [`ROLLUP_TABLE`], not [`LEDGER_TABLE`]: this is the face a panel
    /// or an ops query reaches, and it must not put a scan of the ever-growing
    /// per-call table behind every refresh. Only fully-contained windows are
    /// summed, so a partially-elapsed hour is left to the next fold rather
    /// than reported as a short window. `avg_latency_ms` is re-weighted by call
    /// count so the result is the mean over the calls, not the mean of the
    /// per-window means.
    ///
    /// The covered range is scanned first and the sums are then taken over
    /// exactly that range, not over the requested one. Both orders read the
    /// same closed windows in a quiet table; under a fold that is still
    /// landing, the ordering is what keeps the two halves of the answer
    /// agreeing. The fold only appends windows newer than the newest it has
    /// already landed, so a window landing between the two statements falls
    /// outside the scanned range and drops out of the sums. The other order
    /// would let the sums cover a window the endpoints do not name — the
    /// half-covered answer this range exists to make visible.
    pub async fn usage_by_actor_rolled(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<RolledSpend> {
        let scanned = sqlx::query(&format!(
            "SELECT MIN(window_start) AS covered_since, MAX(window_end) AS covered_until \
             FROM {ROLLUP_TABLE} \
             WHERE window_start >= $1 AND window_end <= $2"
        ))
        .bind(since)
        .bind(until)
        .fetch_one(&self.pool)
        .await?;
        let covered_since = scanned.get::<Option<DateTime<Utc>>, _>("covered_since");
        let covered_until = scanned.get::<Option<DateTime<Utc>>, _>("covered_until");
        // A minimum and a maximum over the same non-empty set are present or
        // absent together; the pair is destructured rather than the two values
        // checked one by one so that half a range cannot be reported.
        let (Some(covered_since), Some(covered_until)) = (covered_since, covered_until) else {
            return Ok(RolledSpend {
                covered_since: None,
                covered_until: None,
                rows: Vec::new(),
            });
        };
        let rows = sqlx::query(&format!(
            "SELECT actor, origin, upstream, api_style, \
                    SUM(calls)::BIGINT AS calls, \
                    SUM(failed_calls)::BIGINT AS failed_calls, \
                    SUM(tokens_input)::BIGINT AS tokens_input, \
                    SUM(tokens_output)::BIGINT AS tokens_output, \
                    SUM(tokens_cached)::BIGINT AS tokens_cached, \
                    CASE WHEN SUM(calls) > 0 \
                         THEN (SUM(avg_latency_ms::BIGINT * calls) / SUM(calls))::BIGINT \
                         ELSE 0::BIGINT END AS avg_latency_ms \
             FROM {ROLLUP_TABLE} \
             WHERE window_start >= $1 AND window_end <= $2 \
             GROUP BY actor, origin, upstream, api_style \
             ORDER BY (SUM(tokens_input) + SUM(tokens_output)) DESC, \
                      actor, origin, upstream, api_style"
        ))
        .bind(covered_since)
        .bind(covered_until)
        .fetch_all(&self.pool)
        .await?;
        let rows = rows
            .iter()
            .map(|r| ActorSpend {
                actor: r.get::<String, _>("actor"),
                origin: r.get::<String, _>("origin"),
                upstream: r.get::<String, _>("upstream"),
                api_style: r.get::<String, _>("api_style"),
                calls: r.get::<i64, _>("calls"),
                failed_calls: r.get::<i64, _>("failed_calls"),
                tokens_input: r.get::<i64, _>("tokens_input"),
                tokens_output: r.get::<i64, _>("tokens_output"),
                tokens_cached: r.get::<i64, _>("tokens_cached"),
                avg_latency_ms: r.get::<i64, _>("avg_latency_ms"),
            })
            .collect();
        Ok(RolledSpend {
            covered_since: Some(covered_since),
            covered_until: Some(covered_until),
            rows,
        })
    }
}

/// The publishing handle for [`cog_core::LlmUsageReader`]: presents the rolled
/// read face of [`LlmUsageStore`] under the contract the gateway consumes, so
/// that the gateway depends on a reading rather than on this crate's storage
/// internals.
#[derive(Clone)]
pub struct RolledUsageReader {
    store: LlmUsageStore,
}

impl RolledUsageReader {
    pub fn new(store: LlmUsageStore) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl cog_core::LlmUsageReader for RolledUsageReader {
    async fn usage_by_actor_rolled(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> cog_core::SFResult<cog_core::RolledUsage> {
        let spend = self
            .store
            .usage_by_actor_rolled(since, until)
            .await
            .map_err(|e| cog_core::SFError::Database(format!("rolled usage read failed: {e}")))?;
        Ok(cog_core::RolledUsage {
            covered_since: spend.covered_since,
            covered_until: spend.covered_until,
            actors: spend
                .rows
                .into_iter()
                .map(|s| cog_core::ActorUsage {
                    actor: s.actor,
                    origin: s.origin,
                    upstream: s.upstream,
                    api_style: s.api_style,
                    calls: s.calls,
                    failed_calls: s.failed_calls,
                    tokens_input: s.tokens_input,
                    tokens_output: s.tokens_output,
                    tokens_cached: s.tokens_cached,
                    avg_latency_ms: s.avg_latency_ms,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four declared origins are the whole closed set, and `unknown` is not
    /// one of them: it reads as "this environment cannot be named", which must
    /// not be counted into any environment's total.
    #[test]
    fn the_declared_set_is_the_four_and_unknown_is_outside_it() {
        let declared: Vec<&str> = LlmOrigin::DECLARED.iter().map(|o| o.as_str()).collect();
        assert_eq!(
            declared,
            vec!["gateway_pool", "resident_worker", "interactive", "sandbox"]
        );
        assert!(!declared.contains(&LlmOrigin::Unknown.as_str()));
    }

    /// A caller that declares one of the four is read as it declared; anything
    /// else reads as `unknown` rather than being stored as free text, so a
    /// reader that groups by origin never sees a misspelling as its own series.
    #[test]
    fn only_a_declared_name_reads_as_itself() {
        assert_eq!(
            LlmOrigin::declared(Some("gateway_pool")),
            LlmOrigin::GatewayPool
        );
        assert_eq!(
            LlmOrigin::declared(Some("resident_worker")),
            LlmOrigin::ResidentWorker
        );
        assert_eq!(
            LlmOrigin::declared(Some("interactive")),
            LlmOrigin::Interactive
        );
        assert_eq!(LlmOrigin::declared(Some("sandbox")), LlmOrigin::Sandbox);
        // Whitespace is the one repair: a trimmed name still names an origin.
        assert_eq!(LlmOrigin::declared(Some("  sandbox ")), LlmOrigin::Sandbox);
        // Everything else -- absent, empty, case-wrong, or unknown -- is
        // `unknown`, and never a value outside the closed set.
        assert_eq!(LlmOrigin::declared(None), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(Some("")), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(Some("Sandbox")), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(Some("host")), LlmOrigin::Unknown);
    }
}

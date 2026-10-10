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
//! folded into [`ROLLUP_TABLE`], one row per actor × upstream × protocol per
//! closed hour. The fold is derived — every row in it can be recomputed from
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

/// One metered LLM call.
#[derive(Debug, Clone)]
pub struct LlmUsageRecord {
    /// Pool identity of the upstream that served the call.
    pub upstream: String,
    /// Wire protocol family (`openai` / `anthropic`).
    pub api_style: String,
    /// Real model name as configured on the upstream.
    pub model: String,
    /// `ok` or `error` — errors carry zero tokens but keep the attempt visible.
    pub result: String,
    /// Calling component as normalized by the gateway (`self_review`,
    /// `agent:<role>`, `unknown`); lets per-actor token spend be audited.
    pub actor: String,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorSpend {
    pub actor: String,
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
                upstream       TEXT NOT NULL,
                api_style      TEXT NOT NULL,
                calls          BIGINT NOT NULL DEFAULT 0,
                failed_calls   BIGINT NOT NULL DEFAULT 0,
                tokens_input   BIGINT NOT NULL DEFAULT 0,
                tokens_output  BIGINT NOT NULL DEFAULT 0,
                tokens_cached  BIGINT NOT NULL DEFAULT 0,
                avg_latency_ms BIGINT NOT NULL DEFAULT 0,
                first_rolled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (window_start, window_end, actor, upstream, api_style)
            )
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
                (id, upstream, api_style, model, result, actor,
                 tokens_input, tokens_output, tokens_cached, latency_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(&r.upstream)
        .bind(&r.api_style)
        .bind(&r.model)
        .bind(&r.result)
        .bind(&r.actor)
        .bind(r.tokens_input as i64)
        .bind(r.tokens_output as i64)
        .bind(r.tokens_cached as i64)
        .bind(r.latency_ms as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The window's token composition, grouped by `(actor, upstream,
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
            SELECT actor, upstream, api_style,
                   COUNT(*) AS calls,
                   COUNT(*) FILTER (WHERE result <> 'ok') AS failed_calls,
                   COALESCE(SUM(tokens_input), 0)::BIGINT  AS tokens_input,
                   COALESCE(SUM(tokens_output), 0)::BIGINT AS tokens_output,
                   COALESCE(SUM(tokens_cached), 0)::BIGINT AS tokens_cached,
                   COALESCE(AVG(latency_ms), 0)::BIGINT    AS avg_latency_ms
            FROM {LEDGER_TABLE}
            WHERE ts >= $1 AND ts < $2
            GROUP BY actor, upstream, api_style
            ORDER BY actor, upstream, api_style
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

    /// Every `(actor, upstream, api_style)` the ledger has ever recorded.
    ///
    /// This is the set a window is completed against so that "no traffic" still
    /// lands rows instead of vanishing. It is read off the ledger rather than
    /// declared, because the actor vocabulary is open (`agent:{role}` is a role
    /// string, not a fixed enum) and any declared list would go stale the moment
    /// a new role ran. Any combination that has ever fired a call gets a row in
    /// every later window, zero when idle, which is what makes a dead upstream
    /// period distinguishable from a period the fold did not run.
    pub async fn known_combos(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        let rows = sqlx::query(&format!(
            "SELECT DISTINCT actor, upstream, api_style FROM {LEDGER_TABLE} \
             ORDER BY actor, upstream, api_style"
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("actor"),
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
                (window_start, window_end, actor, upstream, api_style,
                 calls, failed_calls, tokens_input, tokens_output, tokens_cached,
                 avg_latency_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (window_start, window_end, actor, upstream, api_style)
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
            let mut seen: HashSet<(String, String, String)> = HashSet::with_capacity(spend.len());
            for s in &spend {
                sqlx::query(&insert)
                    .bind(w.start)
                    .bind(w.end)
                    .bind(&s.actor)
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
                seen.insert((s.actor.clone(), s.upstream.clone(), s.api_style.clone()));
                written += 1;
            }
            for (actor, upstream, api_style) in &vocab {
                if seen.contains(&(actor.clone(), upstream.clone(), api_style.clone())) {
                    continue;
                }
                sqlx::query(&insert)
                    .bind(w.start)
                    .bind(w.end)
                    .bind(actor)
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
}

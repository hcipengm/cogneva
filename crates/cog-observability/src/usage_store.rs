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
use cog_core::{fill_slots, RefusalKind, UsageResult};
use sqlx::{PgPool, Row};

/// The ledger's per-call table.
pub const LEDGER_TABLE: &str = "gateway_llm_usage";

/// The durable per-window fold of [`LEDGER_TABLE`].
pub const ROLLUP_TABLE: &str = "cog_llm_usage_rollup";

/// 任务图的正本表。账上写了一个身份，这个身份是不是平台认得的一件活，只有
/// 这张表能回答——账本自己没有「哪些 id 是任务」的名单，也不该抄一份：抄来的
/// 名单会在第一次删任务的那天开始说谎。
const TASK_GRAPH_TABLE: &str = "cog_dag_tasks";

/// The last measured quota window per upstream.
///
/// A measurement is not a usage row, so it does not belong in the ledger; but
/// it is also not a value the process may keep only in memory. The capacity
/// number here is re-derivable from the ledger whenever the boundary is known,
/// and the boundary is the one thing nothing else remembers once the upstream
/// starts serving again. Kept in its own table so that a restart resumes pacing
/// from the same window instead of silently dropping the bound -- a bound that
/// disappears on the next deploy is worse than no bound, because nothing in the
/// readings says it went away.
pub const WINDOW_TABLE: &str = "gateway_llm_window";

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

/// The ledger's environment vocabulary, its pacing evidence and its row shapes
/// live in the contract layer: the process that fills a row and the process
/// that owns the table are different crates, and neither may name the other.
pub use cog_core::contract::usage_ledger::{
    LlmExchangeRecord, LlmOrigin, LlmUsageRecord, PaceEvidence, PaceUpstreamEvidence, StoredWindow,
    UpstreamWindowReading,
};

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

/// One window of one `(actor, origin, upstream, protocol)` line, unsplit from
/// its window.
///
/// The same columns [`ActorSpend`] carries, plus the window they happened in.
/// Kept as its own type rather than as a tuple so that a caller cannot pair one
/// window's counts with another's timestamps by position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendWindowRow {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub actor: String,
    pub origin: String,
    pub upstream: String,
    pub api_style: String,
    pub calls: i64,
    pub failed_calls: i64,
    pub tokens_input: i64,
    pub tokens_output: i64,
    pub tokens_cached: i64,
}

/// One rolled read left at window granularity: the rows, and the range the
/// windows they came from cover.
#[derive(Debug, Clone)]
pub struct WindowSeries {
    /// Oldest window start and newest window end present in the rows. Both are
    /// `None` — never one without the other — when no closed window fell inside
    /// the requested range, for the reason [`RolledSpend`] gives: an empty list
    /// answers "no spend" and "not covered yet" alike, and only the range tells
    /// them apart.
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_until: Option<DateTime<Utc>>,
    pub rows: Vec<SpendWindowRow>,
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

/// The windows just below `first_folded`, oldest first, down to `floor`.
///
/// The head catch-up only ever lands windows after the newest one folded, so a
/// deployment whose first run was bounded by the lookback has a stretch of
/// ledger that no window has ever summed. That stretch is not merely unread —
/// it makes the rows in it undeletable, because deleting them would destroy the
/// only copy of spend the fold never counted. This is the other direction of
/// the same cursor: at most `max_windows` per call, so a backfill over months
/// of ledger is a series of passes rather than one statement that looks like a
/// stopped loop, and never below `floor`, which is the oldest row the ledger
/// still holds.
///
/// Snapped to the same grid as [`windows_to_roll`]: a window boundary is a
/// function of the epoch, not of when a process happened to look, which is what
/// lets the backfill and the head catch-up meet without overlapping or leaving
/// a hole between them.
pub fn windows_before(
    first_folded: DateTime<Utc>,
    floor: DateTime<Utc>,
    window_secs: i64,
    max_windows: usize,
) -> Vec<RollupWindow> {
    let w = window_secs.max(1);
    let grid = |t: DateTime<Utc>| t.timestamp() - t.timestamp().rem_euclid(w);
    let mut end = grid(first_folded);
    let lowest = grid(floor);
    let mut windows = Vec::new();
    while windows.len() < max_windows && end - w >= lowest {
        let (Some(s), Some(e)) = (
            DateTime::from_timestamp(end - w, 0),
            DateTime::from_timestamp(end, 0),
        ) else {
            break;
        };
        windows.push(RollupWindow { start: s, end: e });
        end -= w;
    }
    windows.reverse();
    windows
}

/// PostgreSQL usage store.
/// 台账的两个闭集，按各自声明的全部取值铺成模板槽位。
///
/// 取 `ALL` 而不是只铺用到的那几个词：谓词写成 `{result:ok}` 这样的槽，词本身
/// 由这里换上。一个模板若不慎写成某个枚举里不存在的词，槽就没人填、语句原样留下
/// `{`，数据库当场拒解析——比一条永远选不中行的谓词好查得多。
fn ledger_slots() -> Vec<(&'static str, &'static str)> {
    UsageResult::ALL
        .iter()
        .map(|outcome| ("result", outcome.as_str()))
        .chain(
            RefusalKind::ALL
                .iter()
                .map(|kind| ("refusal", kind.as_str())),
        )
        .collect()
}

/// This store's schema, and the one a restore replays.
///
/// One text because the list has an order: the ledger's additive columns are
/// meaningless without its table, and the rollup's guarded block inspects a
/// table it must find already there. The table names are spelled inline — a
/// const string cannot fold another const into itself — and the test below
/// keeps those spellings and the [`LEDGER_TABLE`]/[`WINDOW_TABLE`]/
/// [`ROLLUP_TABLE`] constants from drifting apart.
///
/// The two partial indexes name a word from the ledger's closed sets. A partial
/// index on a spelling no row carries is invisible: it is never chosen, so the
/// reads it was added to make cheap fall back to a sequential scan, and the only
/// sign is that a reading got slow again.
const DDL_TEMPLATE: &str = r#"
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
    tokens_cache_created BIGINT NOT NULL DEFAULT 0,
    latency_ms    BIGINT NOT NULL DEFAULT 0
);
-- Additive migration for the per-actor dimension; old rows stay attributable
-- as "unknown" without a table rewrite.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT 'unknown';
-- Same shape for the cache dimension. Rows written before this column existed
-- default to 0, and 0 is honest for them as long as the reader treats "cached"
-- on a row whose input is also 0 as "not metered" rather than "nothing was
-- cached" -- the two are indistinguishable after the fact, which is exactly
-- why the column is written going forward rather than reconstructed backward.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS tokens_cached BIGINT NOT NULL DEFAULT 0;
-- The written half of the same cache. Its own column for the same reason the
-- read half has one: "cached: 0" is two different findings -- a prefix that
-- changes on every call, and a request that never asked the upstream to keep
-- anything -- and only the pair tells them apart. Rows written before this
-- column existed default to 0, which on the OpenAI-compatible face is also the
-- honest reading (that face never reports it); on the Anthropic face it reads
-- as "nothing written", so a reader dating a finding has to know when the
-- column started being written.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS tokens_cache_created BIGINT NOT NULL DEFAULT 0;
-- Additive migration for the environment dimension, same shape as the actor
-- one above. Rows written before it existed default to `unknown`, which is the
-- honest reading -- they are not attributable to any one environment and must
-- not be counted into one.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'unknown';
-- Additive migration for the estimate dimension, and the one column here that
-- is nullable on purpose. "This row was not priced" (older rows, and rows
-- landed by the failure path) is not "this row was priced at zero" -- a NOT
-- NULL DEFAULT 0 would make both read as an empty prompt, and a reader
-- grouping by the gap between the two rulers has to handle the absence rather
-- than silently pair it with the truth.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS estimated_input BIGINT;
-- The per-job dimension. Nullable because "no job identity" is a state of its
-- own: rows landed by paths that cannot name a job (an upstream failure
-- recorded before routing, the gateway's own judgement calls) must not be
-- gathered into one placeholder group that would then read as the most
-- expensive job on the ledger. Text, not UUID: the caller declares whatever
-- identity its own per-run census uses, and the reader's job is to join the
-- two, not to funnel both through a type.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS task_id TEXT;
-- The person dimension, same shape and same reason as the job one: the ceiling
-- a call was judged against is the person's daily allowance, and the row that
-- records a refusal has to name the person it refused. Nullable because "no
-- owner was declared" is a state of its own -- the spend counters cannot stand
-- in for it, since they move only on calls that ran.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS owner_id TEXT;
-- What reading the response found. Nullable with no default: the failure and
-- refusal paths land rows about attempts that never got a body, and rows
-- written before this column existed have no reading to report. A default of
-- `read` would claim a measurement nobody took, and a default of `absent`
-- would blame every upstream for a silence this table never observed.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS usage_outcome TEXT;
-- Why a refused attempt was refused. Additive and nullable for the same reason
-- as the two columns above: rows written before it existed have no kind to
-- report, and inventing one would make a past window's failures read as a
-- cause nobody ever observed.
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS refusal_kind TEXT;
-- 配速拒绝的证据列。加列而不是复用上面三格，理由见 [`PaceEvidence`]。
-- 全部可空、没有默认值：非配速行上 `NULL` 读作"不适用"，而任何
-- `coalesce(…, 0)` 的读法都会把"这一行没有这一拍的数"变成"缺口为零"，
-- 那正是这组列要分开的两件事。
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS pace_measured_upstreams SMALLINT;
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS pace_allowance_tokens BIGINT;
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS pace_spent_tokens BIGINT;
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS pace_reserve_tokens BIGINT;
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS pace_window_start_unix BIGINT;
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS pace_upstreams JSONB;
-- 熔断拒绝让调用方等多久。加这一列而不是复用 `refusal_kind`：那一格答的是
-- "上游为什么拒"（熔断时一个上游都没试，永远是 `NULL`），这一格答的是"熔断器
-- 说下次几点再来"。两种拒绝在账上都叫 `refused_by_circuit`，能把它们分开的只有
-- 这个数的**量级**——窗口翻页黑障是几十秒到几分钟，额度烧穿是几千秒。
-- 可空且无默认值：非熔断行上 `NULL` 读作"不适用"，写 0 会读成"立刻可以重试"。
ALTER TABLE gateway_llm_usage ADD COLUMN IF NOT EXISTS retry_after_secs BIGINT;
CREATE INDEX IF NOT EXISTS idx_gateway_llm_usage_ts ON gateway_llm_usage (ts);
-- 窗口配速那一次读按 `(upstream, result)` 与 `(upstream, refusal_kind)` 各取一
-- 小撮，而表里绝大多数行是熔断拒绝（`upstream = '-'`，与任何一家都不匹配）。没有
-- 这两个部分索引时两次取数都是全表顺序扫描：2026-10-22 实测 148k 行 / 34MB，
-- 一次读数 223ms，且随行数线性变慢。部分索引把谓词写进索引，取数只碰匹配的行。
CREATE INDEX IF NOT EXISTS idx_gateway_llm_usage_ok_upstream
    ON gateway_llm_usage (upstream, ts) WHERE result = '{result:ok}';
CREATE INDEX IF NOT EXISTS idx_gateway_llm_usage_quota_wall
    ON gateway_llm_usage (upstream, ts) WHERE refusal_kind = '{refusal:quota_cycle}';
-- One row per upstream **and window length**, not one per measurement: the
-- pacer only ever acts on the newest window it has an observation for, and a
-- history of stale boundaries would make "which one is in force" a question the
-- reader has to answer. The window length belongs in the key because an
-- upstream can enforce nested cycles at once — a five-hour limit and a weekly
-- one — and both measurements are true at the same time. Keyed by upstream
-- alone the second measurement overwrites the first, and the pacer is left
-- with whichever limit happened to refuse last.
CREATE TABLE IF NOT EXISTS gateway_llm_window (
    upstream        TEXT NOT NULL,
    window_secs     BIGINT NOT NULL,
    boundary_unix   BIGINT NOT NULL,
    capacity_tokens BIGINT NOT NULL,
    source          TEXT NOT NULL,
    measured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (upstream, window_secs)
);
-- The instant the upstream said its quota returns, kept apart from
-- `boundary_unix`: the boundary is what pacing anchored on, and it can be a
-- resume bounded from above rather than anything the upstream stated. NULL is
-- "it named no instant", which is the shape measured on 2026-10-19; filling it
-- with the boundary would turn "nobody said" into "it said exactly this".
ALTER TABLE gateway_llm_window ADD COLUMN IF NOT EXISTS declared_reset_unix BIGINT;
-- Installs that predate the window length being part of the key carry a
-- primary key on `upstream` alone, which silently forbids a second ruler.
DO $mig$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'gateway_llm_window'::regclass AND contype = 'p'
          AND pg_get_constraintdef(oid) NOT LIKE '%window_secs%'
    ) THEN
        ALTER TABLE gateway_llm_window DROP CONSTRAINT gateway_llm_window_pkey;
        ALTER TABLE gateway_llm_window
            ADD PRIMARY KEY (upstream, window_secs);
    END IF;
END
$mig$;
-- The window fold. `window_start`/`window_end` are the row's own boundary
-- fields, not a `now()` stamped at insert: a rerun has to key and compare on
-- the window it is recomputing, and a timestamp taken when the row happened to
-- be written would make the same window look like a new one. The primary key
-- is the whole identity of a rollup row, which is what makes a rerun collide
-- with itself and update in place.
CREATE TABLE IF NOT EXISTS cog_llm_usage_rollup (
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
);
-- The environment dimension reaches the fold as well as the ledger: a fold
-- that dropped an axis the ledger carries would lose it, and the question this
-- axis exists for is read again and again -- which is what the fold is for.
-- Both steps are guarded, so a table already carrying the column and key is
-- left alone. The new column goes **last** in the key: every row written
-- before it existed carries the same default, so the enlarged key is still
-- unique and the swap is safe in place, with no rebuild -- which matters
-- because a first fold is bounded by the lookback and could not re-create
-- history older than it.
DO $mig$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'cog_llm_usage_rollup' AND column_name = 'origin'
    ) THEN
        ALTER TABLE cog_llm_usage_rollup
            ADD COLUMN origin TEXT NOT NULL DEFAULT 'unknown';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'cog_llm_usage_rollup'::regclass AND contype = 'p'
          AND pg_get_constraintdef(oid) NOT LIKE '%origin%'
    ) THEN
        ALTER TABLE cog_llm_usage_rollup DROP CONSTRAINT cog_llm_usage_rollup_pkey;
        ALTER TABLE cog_llm_usage_rollup
            ADD PRIMARY KEY
                (window_start, window_end, actor, origin, upstream, api_style);
    END IF;
END
$mig$;
-- The exchange itself: the bytes we sent and the bytes that came back. The
-- usage row above answers "how much did this call cost"; it cannot answer
-- "what was asked", so a re-run, a duplicate count, a context breakdown or a
-- regression against an old prompt all had to be re-bought from the upstream.
-- Kept in its own table rather than in the ledger row because the ledger is
-- read by the pacing pass on every window boundary and must stay narrow; the
-- bodies are read rarely and only by a person asking a question about a
-- specific call.
CREATE TABLE IF NOT EXISTS gateway_llm_exchange (
    id              UUID PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    upstream        TEXT NOT NULL,
    api_style       TEXT NOT NULL,
    model           TEXT NOT NULL,
    actor           TEXT NOT NULL DEFAULT 'unknown',
    origin          TEXT NOT NULL DEFAULT 'unknown',
    task_id         TEXT,
    result          TEXT NOT NULL,
    latency_ms      BIGINT NOT NULL DEFAULT 0,
    request_sha256  TEXT NOT NULL,
    response_sha256 TEXT,
    request_bytes   BIGINT NOT NULL DEFAULT 0,
    response_bytes  BIGINT NOT NULL DEFAULT 0,
    request_body    TEXT,
    response_body   TEXT,
    truncated       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_gateway_llm_exchange_ts ON gateway_llm_exchange (ts);
-- The hash index is what turns "did we already buy this exact prompt" from a
-- full scan into a lookup; without it the question is only answerable for a
-- window small enough to sit in memory.
CREATE INDEX IF NOT EXISTS idx_gateway_llm_exchange_req ON gateway_llm_exchange (request_sha256);
CREATE INDEX IF NOT EXISTS idx_gateway_llm_exchange_actor ON gateway_llm_exchange (actor, ts DESC);
"#;

/// The schema with the ledger's closed sets filled in.
///
/// A slot nobody fills stays in the text, which the database refuses on the
/// first start rather than accepting as an index that never matches.
pub fn ddl() -> String {
    fill_slots(DDL_TEMPLATE, &ledger_slots())
}

#[derive(Clone)]
pub struct LlmUsageStore {
    pool: PgPool,
}

impl LlmUsageStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    /// 借一个已经连上的池。同一个库上再开一个池只为了读一张表，等于给一次
    /// 读配一套连接预算——调用方手里已经有池时就该用这一条。
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 借出池本身，给同一个库上另一件要写/删的事用（保留期清扫就是一件）：
    /// 再开一个池等于给一件事配一套连接预算。
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Auto-create every table, column and index this store owns.
    ///
    /// The statements themselves are [`ddl`]: a restore replays that same list
    /// instead of keeping a second copy of the schema that only it would run.
    pub async fn init_schema(&self, schema: &dyn cog_core::SchemaRunner) -> anyhow::Result<()> {
        schema
            .apply_schema(&ddl())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    pub async fn record(&self, r: &LlmUsageRecord) -> anyhow::Result<()> {
        let pace_upstreams = r
            .pace
            .as_ref()
            .and_then(|p| serde_json::to_value(&p.upstreams).ok());
        sqlx::query(
            r#"
            INSERT INTO gateway_llm_usage
                (id, upstream, api_style, model, result, actor, origin,
                 tokens_input, tokens_output, tokens_cached, tokens_cache_created,
                 estimated_input, task_id, latency_ms, refusal_kind, retry_after_secs,
                 pace_measured_upstreams, pace_allowance_tokens, pace_spent_tokens,
                 pace_reserve_tokens, pace_window_start_unix, pace_upstreams,
                 usage_outcome, owner_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
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
        .bind(r.tokens_cache_created as i64)
        .bind(r.estimated_input.map(|v| v as i64))
        .bind(r.task.as_deref())
        .bind(r.latency_ms as i64)
        .bind(r.refusal_kind.as_deref())
        .bind(r.retry_after_secs.map(|v| v as i64))
        .bind(r.pace.as_ref().map(|p| p.measured_upstreams as i32))
        .bind(r.pace.as_ref().map(|p| p.allowance_tokens as i64))
        .bind(r.pace.as_ref().map(|p| p.spent_tokens as i64))
        .bind(r.pace.as_ref().map(|p| p.reserve_tokens as i64))
        .bind(r.pace.as_ref().map(|p| p.window_start_unix))
        .bind(pace_upstreams)
        .bind(r.usage_outcome.map(|o| o.as_str()))
        .bind(r.owner.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One exchange: the request bytes that went upstream and the response
    /// bytes that came back.
    ///
    /// The two hashes are over the **raw** bytes, before any redaction, so a
    /// duplicate is a duplicate of what was actually sent. The bodies are the
    /// redacted text, capped by the caller; `truncated` names which side was
    /// cut, because a body that ends early and a body that ended at the
    /// upstream look the same once stored.
    pub async fn record_exchange(&self, r: &LlmExchangeRecord) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO gateway_llm_exchange
                (id, upstream, api_style, model, actor, origin, task_id, result,
                 latency_ms, request_sha256, response_sha256, request_bytes,
                 response_bytes, request_body, response_body, truncated)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(&r.upstream)
        .bind(&r.api_style)
        .bind(&r.model)
        .bind(&r.actor)
        .bind(&r.origin)
        .bind(r.task.as_deref())
        .bind(&r.result)
        .bind(r.latency_ms as i64)
        .bind(&r.request_sha256)
        .bind(r.response_sha256.as_deref())
        .bind(r.request_bytes as i64)
        .bind(r.response_bytes as i64)
        .bind(r.request_body.as_deref())
        .bind(r.response_body.as_deref())
        .bind(r.truncated.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Everything a window pacer needs about one upstream, read in one pass.
    ///
    /// `burned_window_tokens` is the **capacity** reading: what this upstream
    /// served in the window that ended at its wall. An upstream that refuses
    /// with "you have reached this cycle's limit" has just proved that this sum
    /// is at or below what its plan allows — measured, not claimed. What the
    /// current window has used so far is asked separately
    /// ([`Self::spent_since`]) because its starting instant is what this read
    /// computes.
    ///
    /// `last_quota_refusal_unix` is when the allowance ran out, taken from the
    /// row's own classification rather than guessed from a gap in traffic: only
    /// a `quota_cycle` refusal says the allowance is spent, while an
    /// `entitlement` refusal says the capacity never existed — pacing against
    /// the latter would spread a quota that will never arrive. It is `None` for
    /// rows written before the classification column existed, and then there is
    /// no capacity reading either: `burned_window_tokens` is 0 because nothing
    /// was measured, and the newest successful call is not a substitute for a
    /// boundary. It only says where serving stopped, which is equally true of an
    /// upstream that was throttled and of one nobody called.
    ///
    /// `last_resume_unix` is the reset observation for upstreams that name no
    /// instant: serving cannot resume while a window is spent, so the first
    /// success after each wall bounds when that quota returned. The newest of
    /// those is the phase anchor — window starts repeat from it — and it is
    /// still the anchor while the most recent wall has no success after it yet.
    /// It is not the last success: that one may be days old and says nothing
    /// about the boundary.
    pub async fn upstream_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> anyhow::Result<UpstreamWindowReading> {
        let ok_word = UsageResult::Ok.as_str();
        let wall_word = RefusalKind::QuotaCycle.as_str();
        let row = sqlx::query(&format!(
            r#"
            WITH ok AS (
                SELECT ts, tokens_input + tokens_output AS tokens
                FROM {LEDGER_TABLE}
                WHERE upstream = $1 AND result = '{ok_word}'
            ),
            last AS (SELECT MAX(ts) AS ts FROM ok),
            walls AS (
                SELECT ts
                  FROM {LEDGER_TABLE}
                 WHERE upstream = $1 AND refusal_kind = '{wall_word}'
            ),
            wall AS (SELECT MAX(ts) AS ts FROM walls),
            -- The latest resume is the first success after the newest wall
            -- that has a success after it -- not after the newest wall as
            -- such, which keeps the previous cycle's resume available while
            -- the current wall has none. A wall has a success after it
            -- exactly when it sits before the last success, so the wall set
            -- collapses to its newest such row once, and the answer is the
            -- first success above it. Pairing every wall with every success
            -- instead (the obvious way to write "one resume per wall, take
            -- the latest") re-materialised the whole success list per wall:
            -- 223 ms over 13.5k successes x 112 walls on 2026-10-22, and it
            -- grows with each cycle because walls only accumulate.
            last_answerable_wall AS (
                SELECT MAX(w.ts) AS ts
                  FROM walls w
                 WHERE w.ts < (SELECT ts FROM last)
            ),
            resumes AS (
                SELECT MIN(o.ts) AS ts
                  FROM ok o, last_answerable_wall w
                 WHERE o.ts > w.ts
            )
            SELECT
                -- FLOOR, not a bare cast: a cast rounds to the nearest second,
                -- so a row in the second half of a second came back stamped
                -- after itself. A reader that sums from that instant then
                -- starts *after* the row it was told about and loses it —
                -- `spent_since` dropped the newest success half the time,
                -- which reads as a window that served nothing.
                (SELECT FLOOR(EXTRACT(EPOCH FROM ts))::BIGINT FROM last) AS last_ok_unix,
                (SELECT FLOOR(EXTRACT(EPOCH FROM ts))::BIGINT FROM wall) AS last_quota_refusal_unix,
                -- The latest instant serving resumed after a wall bounds when
                -- that window's quota returned. An upstream that names no
                -- reset (kimi's weekly 403 names only the window's length)
                -- leaves no other observation of it, and the wall itself is
                -- not one: the allowance can run out on the window's first
                -- day.
                (SELECT FLOOR(EXTRACT(EPOCH FROM MAX(ts)))::BIGINT
                   FROM resumes) AS last_resume_unix,
                -- Measured around the refusal, and only around it: that row is
                -- the one observation of the allowance running out. There is no
                -- fallback to the last success — that instant only says where
                -- serving stopped, so a sum around it would be a capacity
                -- nobody measured. Without a wall the reading is 0, which the
                -- caller reads as "nothing measured" because the boundary is
                -- NULL in the same row.
                COALESCE(
                    (SELECT SUM(o.tokens) FROM ok o, wall w
                      WHERE w.ts IS NOT NULL
                        AND o.ts > w.ts - make_interval(secs => $2)
                        AND o.ts <= w.ts),
                    0
                )::BIGINT AS burned_window_tokens
            "#
        ))
        .bind(upstream)
        .bind(window_secs as f64)
        .fetch_one(&self.pool)
        .await?;
        Ok(UpstreamWindowReading {
            last_ok_unix: row.get("last_ok_unix"),
            last_quota_refusal_unix: row.get("last_quota_refusal_unix"),
            last_resume_unix: row.get("last_resume_unix"),
            burned_window_tokens: row.get::<i64, _>("burned_window_tokens").max(0) as u64,
        })
    }

    /// Tokens this upstream served in `(since_unix, until_unix]`.
    ///
    /// The bounded form exists because a window's end can be known before the
    /// ledger holds a refusal that names it: an upstream tells us when its
    /// quota returns, and the tokens served before that instant are what the
    /// window allowed. Summing against a fixed upper bound also makes the
    /// number stable -- recomputed after the reset, it still counts the same
    /// window rather than sliding forward into the new one.
    pub async fn burned_between(
        &self,
        upstream: &str,
        since_unix: i64,
        until_unix: i64,
    ) -> anyhow::Result<u64> {
        let ok_word = UsageResult::Ok.as_str();
        let row = sqlx::query(&format!(
            "SELECT COALESCE(SUM(tokens_input + tokens_output), 0)::BIGINT AS tokens
               FROM {LEDGER_TABLE}
              WHERE upstream = $1 AND result = '{ok_word}'
                AND ts > to_timestamp($2::DOUBLE PRECISION)
                AND ts <= to_timestamp($3::DOUBLE PRECISION)"
        ))
        .bind(upstream)
        .bind(since_unix as f64)
        .bind(until_unix as f64)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>("tokens").max(0) as u64)
    }

    /// Durably remember an upstream's measured window.
    ///
    /// Overwrites the previous row: a newer boundary supersedes an older one,
    /// and keeping both would leave the pacing pass to pick. `measured_at` is
    /// refreshed so a reader can tell a window measured today from one measured
    /// two cycles ago.
    pub async fn record_window(&self, w: &StoredWindow) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {WINDOW_TABLE}
                 (upstream, boundary_unix, window_secs, capacity_tokens, source,
                  declared_reset_unix, measured_at)
             VALUES ($1, $2, $3, $4, $5, $6, NOW())
             ON CONFLICT (upstream, window_secs) DO UPDATE SET
                 boundary_unix       = EXCLUDED.boundary_unix,
                 window_secs         = EXCLUDED.window_secs,
                 capacity_tokens     = EXCLUDED.capacity_tokens,
                 source              = EXCLUDED.source,
                 declared_reset_unix = EXCLUDED.declared_reset_unix,
                 measured_at         = NOW()"
        ))
        .bind(&w.upstream)
        .bind(w.boundary_unix)
        .bind(w.window_secs as i64)
        .bind(w.capacity_tokens as i64)
        .bind(&w.source)
        .bind(w.declared_reset_unix)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The window measurement last stored for one upstream and window length.
    ///
    /// Both parts name the row: the same upstream can enforce a five-hour and a
    /// weekly limit at once, and each has its own measured boundary and
    /// capacity. Asking by upstream alone would return whichever ruler was
    /// written last.
    pub async fn stored_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> anyhow::Result<Option<StoredWindow>> {
        let row = sqlx::query(&format!(
            "SELECT upstream, boundary_unix, window_secs, capacity_tokens, source,
                    declared_reset_unix
               FROM {WINDOW_TABLE} WHERE upstream = $1 AND window_secs = $2"
        ))
        .bind(upstream)
        .bind(window_secs as i64)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| StoredWindow {
            upstream: r.get("upstream"),
            boundary_unix: r.get("boundary_unix"),
            window_secs: r.get::<i64, _>("window_secs").max(0) as u64,
            capacity_tokens: r.get::<i64, _>("capacity_tokens").max(0) as u64,
            source: r.get("source"),
            declared_reset_unix: r.get("declared_reset_unix"),
        }))
    }

    /// Tokens this upstream served from `since_unix` onwards.
    ///
    /// Asked separately from [`Self::upstream_window`] because the instant to
    /// start from is only known after the capacity reading names the window's
    /// boundary — the same pass cannot answer both without being told a fact it
    /// is the one computing.
    pub async fn spent_since(&self, upstream: &str, since_unix: i64) -> anyhow::Result<u64> {
        let ok_word = UsageResult::Ok.as_str();
        let row = sqlx::query(&format!(
            "SELECT COALESCE(SUM(tokens_input + tokens_output), 0)::BIGINT AS tokens
               FROM {LEDGER_TABLE}
              WHERE upstream = $1 AND result = '{ok_word}'
                AND ts >= to_timestamp($2::DOUBLE PRECISION)"
        ))
        .bind(upstream)
        .bind(since_unix as f64)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>("tokens").max(0) as u64)
    }

    /// Tokens the whole ledger spent from `since_unix` onwards, in total and
    /// per actor.
    ///
    /// Every row counts, not only `result = 'ok'` ones, and the difference from
    /// [`Self::spent_since`] is the question, not an oversight. Pacing asks what
    /// an upstream *served* out of a window's quota, and there only a completed
    /// answer was served. A spend ceiling asks what was *sent*, and tokens the
    /// upstream answered and then dropped — a stream that died after the first
    /// frame, a call that came back an error carrying usage — were still
    /// produced on our account. Under a ceiling the two rows that carry no
    /// tokens at all are the refusal rows the gateway writes, so counting them
    /// costs nothing and excluding them would be a filter whose only effect is
    /// to under-report money already spent.
    ///
    /// Refusals land as rows with zero tokens, so a caller that only ever sees
    /// refusals reads zero rather than an error — the honest sum.
    pub async fn spend_by_actor_since(
        &self,
        since_unix: i64,
    ) -> anyhow::Result<(u64, std::collections::BTreeMap<String, u64>)> {
        let rows = sqlx::query(&format!(
            "SELECT actor, COALESCE(SUM(tokens_input + tokens_output), 0)::BIGINT AS tokens
               FROM {LEDGER_TABLE}
              WHERE ts >= to_timestamp($1::DOUBLE PRECISION)
              GROUP BY actor
              ORDER BY actor"
        ))
        .bind(since_unix as f64)
        .fetch_all(&self.pool)
        .await?;
        let mut by_actor = std::collections::BTreeMap::new();
        let mut total = 0u64;
        for row in &rows {
            let tokens = row.get::<i64, _>("tokens").max(0) as u64;
            total = total.saturating_add(tokens);
            by_actor.insert(row.get::<String, _>("actor"), tokens);
        }
        Ok((total, by_actor))
    }

    /// 一组任务各自花了多少 token。
    ///
    /// 按任务取而不是按窗口取：挣值的实际成本问的是「这些活一共花了多少」，
    /// 与它们横跨几个窗口无关。没花过钱的任务不出现在结果里（不是 0），调用方
    /// 因此能分开「花了 0」与「这笔账上看不到它」——后者是归因缺口，不是省下来的。
    ///
    /// `result` 不过滤：失败的上游调用照样烧掉了输入 token，把它排除会让实际
    /// 成本偏低，而成本绩效只会在这一侧虚高。
    pub async fn spend_by_task(&self, task_ids: &[String]) -> anyhow::Result<Vec<(String, u64)>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(&format!(
            "SELECT task_id, COALESCE(SUM(tokens_input + tokens_output), 0)::BIGINT AS tokens
               FROM {LEDGER_TABLE}
              WHERE task_id = ANY($1)
              GROUP BY task_id
              ORDER BY task_id"
        ))
        .bind(task_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("task_id"),
                    row.get::<i64, _>("tokens").max(0) as u64,
                )
            })
            .collect())
    }

    /// 同一窗口里**归不到任何一件任务**的用量合计。
    ///
    /// 这个数不属于任何一个项目，所以它不能并进哪个项目的成本；它要报出来，
    /// 是因为它量的是「成本里有多少看不到出处」。折成 0，成本绩效会把归因缺口
    /// 读成省钱。
    ///
    /// 「没有出处」有两种写法，两种都要算：
    ///
    /// - `task_id` 为空：调用方压根没声明在为谁干活；
    /// - `task_id` 写的是一次运行的 id（2026-10-26 之前全是 `ralph-pipeline-*`），
    ///   这个 id 在任务图里找不到对应的活。它看着像有身份，实际指向的是执行
    ///   这件事的那次运行，不是它花掉的钱该记的那件活——按「有 id」就当成已
    ///   归属，这些行会同时从项目成本和缺口里消失。2026-10-26 实测它们占全部
    ///   花销的 27%（1.17e8 / 4.27e8），是缺口里仅次于「没写身份」的那一桶。
    pub async fn unattributed_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<u64> {
        let row = sqlx::query(&format!(
            "SELECT COALESCE(SUM(u.tokens_input + u.tokens_output), 0)::BIGINT AS tokens
               FROM {LEDGER_TABLE} u
              WHERE u.ts >= $1 AND u.ts < $2
                AND (u.task_id IS NULL
                     OR NOT EXISTS (SELECT 1 FROM {TASK_GRAPH_TABLE} t
                                     WHERE t.task_id = u.task_id))"
        ))
        .bind(since)
        .bind(until)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>("tokens").max(0) as u64)
    }

    /// How the calls in `[since, until)` ended, one line per `(result,
    /// refusal_kind)`, ordered so two reads of the same range compare line for
    /// line after the biggest line.
    ///
    /// This is read off the per-call ledger, not the rollup, and that is the
    /// whole point: the fold answers "how many calls failed", the verdict
    /// columns answer "because of what". A range whose every attempt was
    /// refused before leaving the process lands a row here for each kind,
    /// which is what separates a dead pool from an idle one — on every other
    /// face both read as zero tokens.
    ///
    /// `refusal_kind` is coalesced to the empty string: a row that was not
    /// refused has no kind, and a NULL that reaches a reader as `"null"` is
    /// one more refusal reason nobody declared.
    pub async fn outcomes_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<cog_core::OutcomeCount>> {
        let rows = sqlx::query(&format!(
            r#"
            SELECT result,
                   COALESCE(refusal_kind, '') AS refusal_kind,
                   COUNT(*)::BIGINT AS calls
            FROM {LEDGER_TABLE}
            WHERE ts >= $1 AND ts < $2
            GROUP BY result, COALESCE(refusal_kind, '')
            ORDER BY COUNT(*) DESC, result, COALESCE(refusal_kind, '')
            "#
        ))
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| cog_core::OutcomeCount {
                result: r.get("result"),
                refusal_kind: r.get("refusal_kind"),
                calls: r.get("calls"),
            })
            .collect())
    }

    /// How many of the successful calls in `[since, until)` carried a usage
    /// reading, and what that reading found.
    ///
    /// The failure and refusal paths never got a body, so their rows are
    /// excluded rather than counted as unmetered: this read exists to say how
    /// far the token totals beside it can be trusted, and a refused call
    /// spends nothing rather than spending an unknown amount.
    ///
    /// Rows written before the column existed come back with an empty outcome
    /// rather than being dropped or guessed at: the window's denominator is
    /// still the number of successful calls, and a reader that left them out
    /// would report a share higher than the one that is true.
    pub async fn metering_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<cog_core::MeteringCount>> {
        let ok_word = UsageResult::Ok.as_str();
        let rows = sqlx::query(&format!(
            r#"
            SELECT COALESCE(usage_outcome, '') AS outcome,
                   COUNT(*)::BIGINT AS calls
            FROM {LEDGER_TABLE}
            WHERE ts >= $1 AND ts < $2 AND result = '{ok_word}'
            GROUP BY COALESCE(usage_outcome, '')
            ORDER BY COUNT(*) DESC, COALESCE(usage_outcome, '')
            "#
        ))
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| cog_core::MeteringCount {
                outcome: r.get("outcome"),
                calls: r.get("calls"),
            })
            .collect())
    }

    /// The savings the call sites recorded in `[since, until)`.
    ///
    /// Read here rather than from [`crate::skip_store::LlmSkipStore`] because
    /// the reader that needs it already holds this store and asks its three
    /// questions in one place; the tables share one owner (this crate) and one
    /// database, so the join across them is a query, not a boundary crossing.
    pub async fn skips_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<cog_core::SkippedWorkLine>> {
        Ok(crate::skip_store::skips_between(&self.pool, since, until)
            .await?
            .into_iter()
            .map(|row| cog_core::SkippedWorkLine {
                stage: row.stage,
                reason: row.reason,
                skips: row.skips,
            })
            .collect())
    }

    /// The spend verdicts judged for windows inside `[since, until)`.
    ///
    /// One query with the projection's own read: two statements answering
    /// nearly the same question would drift, and the screen would then disagree
    /// with anything else that asked.
    pub async fn verdicts_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<cog_core::SpendVerdictLine>> {
        Ok(
            crate::spend_verdict::verdict_lines(&self.pool, since, until)
                .await?
                .into_iter()
                .map(|row| cog_core::SpendVerdictLine {
                    actor: row.actor,
                    verdict: row.verdict,
                    skip_reason: row.skip_reason,
                    windows: row.cells,
                    skips: row.skips,
                    tokens_input: row.tokens_input,
                    tokens_output: row.tokens_output,
                })
                .collect(),
        )
    }

    /// Whether a job has already put anything on the wire under this identity.
    ///
    /// This is the only question that turns "a task is running" into a reading,
    /// and it is asked before a paced refusal: refusing a job's later calls
    /// strands the tokens it already spent on the work in flight, so the pace
    /// may only hold a job at its start. Rows that name a refusal are not the
    /// job starting — nothing was sent — and counting them would let a paced
    /// job slip through on its own second attempt.
    pub async fn task_has_started(&self, task: &str) -> anyhow::Result<bool> {
        let ok_word = UsageResult::Ok.as_str();
        let error_word = UsageResult::Error.as_str();
        let row = sqlx::query(&format!(
            "SELECT EXISTS(
                 SELECT 1 FROM {LEDGER_TABLE}
                  WHERE task_id = $1 AND result IN ('{ok_word}', '{error_word}')
             ) AS started"
        ))
        .bind(task)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get("started"))
    }

    /// A job's whole cost at a quantile, from `result = 'ok'` rows grouped by
    /// the job identity the caller declared.
    ///
    /// A pace that only compares the spend against a line cut by the clock
    /// starts a job whose remaining allowance is a fraction of what the job
    /// costs and strands that fraction mid-flight. The reserve is therefore a
    /// measured job size, and this is where it is measured — the task axis is
    /// what makes it possible at all. `None` when no job has completed inside
    /// the window (a fresh deployment, or a build that predates the column):
    /// no reading, so no reserve, which is the same fail-open the missing
    /// capacity reading gets.
    ///
    /// The answer is `(cost, jobs, oldest_unix)`. The window handed in is a
    /// parameter, not a measurement: a deployment three hours old asked for the
    /// last thirty days gets a percentile over three hours of work and a number
    /// of exactly the same shape as one over the full month. `jobs` is how many
    /// tasks stand behind it and `oldest_unix` is when the earliest of them ran,
    /// so a reader can see that the span it claims is not the span it has. Both
    /// come from the same subquery as the percentile, so none of the three can
    /// describe a different population than the others.
    pub async fn task_cost_quantile(
        &self,
        days: i64,
        quantile: f64,
    ) -> anyhow::Result<Option<(u64, u64, i64)>> {
        let ok_word = UsageResult::Ok.as_str();
        let row = sqlx::query(&format!(
            r#"
            SELECT PERCENTILE_CONT($2) WITHIN GROUP (ORDER BY total) AS cost,
                   COUNT(*)::BIGINT AS jobs,
                   COALESCE(EXTRACT(EPOCH FROM MIN(oldest))::BIGINT, 0) AS oldest_unix
              FROM (SELECT task_id, SUM(tokens_input + tokens_output)::BIGINT AS total,
                           MIN(ts) AS oldest
                      FROM {LEDGER_TABLE}
                     WHERE result = '{ok_word}' AND task_id IS NOT NULL
                       AND ts > NOW() - make_interval(days => $1)
                     GROUP BY task_id) jobs
            "#
        ))
        .bind(days as i32)
        .bind(quantile)
        .fetch_one(&self.pool)
        .await?;
        // The population travels with the percentile: `PERCENTILE_CONT` answers
        // with a plausible number over three jobs exactly as it does over three
        // hundred, and a reader handed the cost alone cannot tell which one it
        // is looking at. `COUNT(*)` and `MIN(ts)` over the same subquery are the
        // same measurement, taken in the same statement so the three cannot
        // disagree.
        let jobs = row.get::<i64, _>("jobs").max(0) as u64;
        let oldest_unix = row.get::<i64, _>("oldest_unix");
        Ok(row
            .get::<Option<f64>, _>("cost")
            .map(|v| (v.max(0.0) as u64, jobs, oldest_unix)))
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
        let ok_word = UsageResult::Ok.as_str();
        let rows = sqlx::query(&format!(
            r#"
            SELECT actor, origin, upstream, api_style,
                   COUNT(*) AS calls,
                   COUNT(*) FILTER (WHERE result <> '{ok_word}') AS failed_calls,
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

    /// Start of the oldest window already folded, or `None` when the rollup
    /// table is empty.
    ///
    /// This is the lower bound of what the fold has answered for. A ledger row
    /// before it was never summed by any window, so it is the only record of
    /// that spend; the backfill reaches downward from here, and the retention
    /// may not delete below it. One reading serves both because they must never
    /// disagree: a deletion that ran below what the fold covered would remove
    /// the last copy of a number the rollup never had.
    pub async fn first_rolled_window_start(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        let row = sqlx::query(&format!(
            "SELECT MIN(window_start) AS first FROM {ROLLUP_TABLE}"
        ))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.get::<Option<DateTime<Utc>>, _>("first")))
    }

    /// The oldest row the ledger holds, or `None` when it is empty. The
    /// backfill walks down to this: everything the ledger still keeps is what
    /// the fold has to be able to answer for before any of it is retired.
    pub async fn oldest_ledger_ts(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        let row = sqlx::query(&format!("SELECT MIN(ts) AS oldest FROM {LEDGER_TABLE}"))
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.get::<Option<DateTime<Utc>>, _>("oldest")))
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
    ///
    /// Combinations with no call anywhere in the range are left out
    /// (`HAVING SUM(calls) > 0`). The fold lands a zero row for every
    /// combination the ledger has ever seen, so that "folded but idle" stays
    /// distinguishable from "not folded" — a fact about *one window*. Summed
    /// over a range those rows become every caller that has ever run, most of
    /// which did nothing in this range, and a reader that counts the distinct
    /// actors, origins, upstreams or protocols in the answer gets the idle ones
    /// counted as present. A combination that called and failed keeps its row:
    /// `calls` counts attempts, so a refused window still reads as a refusal.
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
             HAVING SUM(calls) > 0 \
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

    /// The same fold, one row per window per `(actor, origin, upstream,
    /// protocol)` instead of one row per combination.
    ///
    /// Ordered by window so that a reader plotting an axis never has to sort:
    /// the fold's primary key already starts with `window_start`, and the one
    /// the reader plots is the one the fold wrote. Ties inside a window are
    /// ordered by the same columns the summed read orders by, so two readers of
    /// the same window see the same rows in the same places.
    pub async fn usage_series(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<WindowSeries> {
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
        let (Some(covered_since), Some(covered_until)) = (covered_since, covered_until) else {
            return Ok(WindowSeries {
                covered_since: None,
                covered_until: None,
                rows: Vec::new(),
            });
        };
        let rows = sqlx::query(&format!(
            "SELECT window_start, window_end, actor, origin, upstream, api_style, \
                    calls, failed_calls, tokens_input, tokens_output, tokens_cached \
             FROM {ROLLUP_TABLE} \
             WHERE window_start >= $1 AND window_end <= $2 \
             ORDER BY window_start, (tokens_input + tokens_output) DESC, \
                      actor, origin, upstream, api_style"
        ))
        .bind(covered_since)
        .bind(covered_until)
        .fetch_all(&self.pool)
        .await?;
        let rows = rows
            .iter()
            .map(|r| SpendWindowRow {
                window_start: r.get::<DateTime<Utc>, _>("window_start"),
                window_end: r.get::<DateTime<Utc>, _>("window_end"),
                actor: r.get::<String, _>("actor"),
                origin: r.get::<String, _>("origin"),
                upstream: r.get::<String, _>("upstream"),
                api_style: r.get::<String, _>("api_style"),
                calls: r.get::<i64, _>("calls"),
                failed_calls: r.get::<i64, _>("failed_calls"),
                tokens_input: r.get::<i64, _>("tokens_input"),
                tokens_output: r.get::<i64, _>("tokens_output"),
                tokens_cached: r.get::<i64, _>("tokens_cached"),
            })
            .collect();
        Ok(WindowSeries {
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

    async fn usage_series(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> cog_core::SFResult<cog_core::UsageSeries> {
        let series =
            self.store.usage_series(since, until).await.map_err(|e| {
                cog_core::SFError::Database(format!("usage series read failed: {e}"))
            })?;
        Ok(cog_core::UsageSeries {
            covered_since: series.covered_since,
            covered_until: series.covered_until,
            rows: series
                .rows
                .into_iter()
                .map(|s| cog_core::WindowSpend {
                    window_start: s.window_start,
                    window_end: s.window_end,
                    actor: s.actor,
                    origin: s.origin,
                    upstream: s.upstream,
                    api_style: s.api_style,
                    calls: s.calls,
                    failed_calls: s.failed_calls,
                    tokens_input: s.tokens_input,
                    tokens_output: s.tokens_output,
                    tokens_cached: s.tokens_cached,
                })
                .collect(),
        })
    }
}

/// The contract face of this store.
///
/// The process that meters calls holds the trait object, not this type: it is
/// not allowed to name the crate that owns the table. The inherent methods stay
/// as they are for the reporting side, which is assembled here and can see both.
#[async_trait::async_trait]
impl cog_core::LlmUsageLedger for LlmUsageStore {
    async fn record(&self, record: &LlmUsageRecord) -> Result<(), String> {
        LlmUsageStore::record(self, record)
            .await
            .map_err(|e| e.to_string())
    }

    async fn record_exchange(&self, record: &LlmExchangeRecord) -> Result<(), String> {
        LlmUsageStore::record_exchange(self, record)
            .await
            .map_err(|e| e.to_string())
    }

    async fn upstream_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> Result<UpstreamWindowReading, String> {
        LlmUsageStore::upstream_window(self, upstream, window_secs)
            .await
            .map_err(|e| e.to_string())
    }

    async fn burned_between(
        &self,
        upstream: &str,
        since_unix: i64,
        until_unix: i64,
    ) -> Result<u64, String> {
        LlmUsageStore::burned_between(self, upstream, since_unix, until_unix)
            .await
            .map_err(|e| e.to_string())
    }

    async fn record_window(&self, window: &StoredWindow) -> Result<(), String> {
        LlmUsageStore::record_window(self, window)
            .await
            .map_err(|e| e.to_string())
    }

    async fn stored_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> Result<Option<StoredWindow>, String> {
        LlmUsageStore::stored_window(self, upstream, window_secs)
            .await
            .map_err(|e| e.to_string())
    }

    async fn spent_since(&self, upstream: &str, since_unix: i64) -> Result<u64, String> {
        LlmUsageStore::spent_since(self, upstream, since_unix)
            .await
            .map_err(|e| e.to_string())
    }

    async fn spend_by_actor_since(
        &self,
        since_unix: i64,
    ) -> Result<(u64, std::collections::BTreeMap<String, u64>), String> {
        LlmUsageStore::spend_by_actor_since(self, since_unix)
            .await
            .map_err(|e| e.to_string())
    }

    async fn task_cost_quantile(
        &self,
        days: i64,
        quantile: f64,
    ) -> Result<Option<(u64, u64, i64)>, String> {
        LlmUsageStore::task_cost_quantile(self, days, quantile)
            .await
            .map_err(|e| e.to_string())
    }

    async fn task_has_started(&self, task: &str) -> Result<bool, String> {
        LlmUsageStore::task_has_started(self, task)
            .await
            .map_err(|e| e.to_string())
    }

    async fn spend_by_task(&self, task_ids: &[String]) -> Result<Vec<(String, u64)>, String> {
        LlmUsageStore::spend_by_task(self, task_ids)
            .await
            .map_err(|e| e.to_string())
    }

    async fn unattributed_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, String> {
        LlmUsageStore::unattributed_between(self, since, until)
            .await
            .map_err(|e| e.to_string())
    }

    async fn outcomes_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::OutcomeCount>, String> {
        LlmUsageStore::outcomes_between(self, since, until)
            .await
            .map_err(|e| e.to_string())
    }

    async fn metering_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::MeteringCount>, String> {
        LlmUsageStore::metering_between(self, since, until)
            .await
            .map_err(|e| e.to_string())
    }

    async fn skips_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::SkippedWorkLine>, String> {
        LlmUsageStore::skips_between(self, since, until)
            .await
            .map_err(|e| e.to_string())
    }

    async fn verdicts_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::SpendVerdictLine>, String> {
        LlmUsageStore::verdicts_between(self, since, until)
            .await
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The declared origins are the whole closed set, and `unknown` is not
    /// one of them: it reads as "this environment cannot be named", which must
    /// not be counted into any environment's total.
    #[test]
    fn the_declared_set_is_closed_and_unknown_is_outside_it() {
        let declared: Vec<&str> = LlmOrigin::DECLARED.iter().map(|o| o.as_str()).collect();
        assert_eq!(
            declared,
            vec![
                "gateway_pool",
                "resident_worker",
                "interactive",
                "sandbox",
                "eval"
            ]
        );
        assert!(!declared.contains(&LlmOrigin::Unknown.as_str()));
    }

    /// A caller that declares one of the set is read as it declared; anything
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
        assert_eq!(LlmOrigin::declared(Some("eval")), LlmOrigin::Eval);
        // Whitespace is the one repair: a trimmed name still names an origin.
        assert_eq!(LlmOrigin::declared(Some("  sandbox ")), LlmOrigin::Sandbox);
        // Everything else -- absent, empty, case-wrong, or unknown -- is
        // `unknown`, and never a value outside the closed set.
        assert_eq!(LlmOrigin::declared(None), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(Some("")), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(Some("Sandbox")), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(Some("host")), LlmOrigin::Unknown);
        // A caller that names a *run* where an environment is asked for is
        // also outside the set: "eval-pod-77" was the value the evaluation job
        // declared, and it read as nothing at all.
        assert_eq!(LlmOrigin::declared(Some("eval-pod-77")), LlmOrigin::Unknown);
    }

    /// 两条部分索引的谓词必须带着枚举声明的词渲染出来。
    ///
    /// 一个没填的槽会让整段 DDL 在第一次启动时被数据库拒解析——那时容器已经起来
    /// 了，失败点在"建表"，而不在这里。所以这里断言渲染后的谓词逐字带着枚举的词，
    /// 而不是只断言"没有花括号"：后者会把任何一段合法带花括号的 schema 误报成漏填，
    /// 也会在槽被换成别的形状时照样放绿。
    #[test]
    fn the_schema_names_the_two_index_predicates_from_their_enums() {
        let schema = ddl();
        assert!(!schema.contains("{result:"), "结果词槽没有填上");
        assert!(!schema.contains("{refusal:"), "拒绝成因槽没有填上");
        assert!(schema.contains(&format!("WHERE result = '{}'", UsageResult::Ok.as_str())));
        assert!(schema.contains(&format!(
            "WHERE refusal_kind = '{}'",
            RefusalKind::QuotaCycle.as_str()
        )));
    }
}

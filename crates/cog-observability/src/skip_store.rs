//! PostgreSQL-backed ledger of LLM calls that were deliberately not made.
//!
//! The token ledger answers "who burned how much" and it can only ever hold the
//! calls that happened. A call site that was skipped and a call site that
//! stopped being reached are therefore the same row on that table — none — and
//! the two want opposite repairs. This is the other half of the account: the
//! saving, written where the decision to save is taken.
//!
//! **It is not a column on the usage ledger, and it must not become one.** A
//! skipped call produces no usage row by construction, so a verdict column
//! there would write "this was saved" onto a row that does not exist, which
//! reads as "this never happened". The two tables are joined by (stage, window)
//! at the reading end, which is what keeps a saving a saving.
//!
//! **It is not the realtime metric either.** `self_review_skipped_total` is the
//! in-process face a panel scrapes; it starts over at zero on every rollout and
//! is dropped with the scrape retention, so a question asked after a restart
//! ("was this review ever skipped last week, or did the call site go away")
//! cannot be answered from it. The two faces carry the same `stage` label on
//! purpose: they name one cell of the account, read on two clocks.
//!
//! Written by whichever process makes the decision (`cog-collaboration`, today
//! the main application), read by the verdict fold, which joins it against the
//! window's spend. Both are readers of the same database, not of each other.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

/// The savings recorded for one `(stage, reason)` over the window asked for.
///
/// `reason` is carried rather than folded into the count: the declared reasons
/// are a closed set that means different things — an upstream that never
/// answered, a stage deliberately outside the review's remit, a gate switched
/// off in this deployment — and a saving whose reason is not readable cannot be
/// told from one nobody decided.
///
/// **`skips` counts decisions, not calls, and the two are not equal.** One
/// skipped review saves the whole loop it did not enter — a critique and the
/// revision it may have asked for — so the calls behind one decision are one or
/// two depending on how the loop would have ended, which nothing here can know.
/// Multiplying by a fixed number would put a figure in the account that no
/// measurement produced; a reader that wants calls has to say how it got there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSkip {
    pub stage: String,
    pub reason: String,
    pub skips: i64,
}

/// This store's schema, and the one a restore replays.
pub const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cog_llm_skip (
                id     UUID PRIMARY KEY,
                ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                stage  TEXT NOT NULL,
                reason TEXT NOT NULL
            )
;
CREATE INDEX IF NOT EXISTS idx_cog_llm_skip_ts ON cog_llm_skip (ts);
"#;

/// PostgreSQL store for the saved-call ledger.
#[derive(Clone)]
pub struct LlmSkipStore {
    pool: PgPool,
}

/// The table. Named in the `cog_` family rather than the `gateway_` one because
/// the writer is not the gateway: the gateway is where every upstream call
/// converges, while a call that is never made has no reason to pass through it.
pub const SKIP_TABLE: &str = "cog_llm_skip";

impl LlmSkipStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn init_schema(&self, schema: &dyn cog_core::SchemaRunner) -> anyhow::Result<()> {
        schema
            .apply_schema(DDL)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// Land one saving. The row's time is the database's `now()`, not the
    /// caller's clock: a window boundary is compared against this column, and a
    /// writer whose clock drifted would land its saving in a window the spend
    /// was never in — the one way to make a skip unreadable against its spend.
    pub async fn record(&self, record: &cog_core::LlmSkipRecord) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {SKIP_TABLE} (id, stage, reason) VALUES ($1, $2, $3)"
        ))
        .bind(uuid::Uuid::new_v4())
        .bind(&record.stage)
        .bind(record.reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The savings recorded over `[since, until)`, one line per
    /// `(stage, reason)`, ordered so two reads of the same window compare line
    /// for line.
    pub async fn skips_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> anyhow::Result<Vec<StageSkip>> {
        let rows = sqlx::query(&format!(
            r#"
            SELECT stage, reason, COUNT(*) AS skips
            FROM {SKIP_TABLE}
            WHERE ts >= $1 AND ts < $2
            GROUP BY stage, reason
            ORDER BY stage, reason
            "#
        ))
        .bind(since)
        .bind(until)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| StageSkip {
                stage: r.get("stage"),
                reason: r.get("reason"),
                skips: r.get("skips"),
            })
            .collect())
    }
}

/// The sink the call sites write through.
///
/// The error is returned rather than logged and swallowed: a saving that
/// failed to land is indistinguishable from a call site that never skipped
/// anything, and the caller is the only place that still knows a skip
/// happened.
#[async_trait::async_trait]
impl cog_core::LlmSkipSink for LlmSkipStore {
    async fn record_skip(&self, record: &cog_core::LlmSkipRecord) -> cog_core::SFResult<()> {
        self.record(record)
            .await
            .map_err(|e| cog_core::SFError::Database(format!("skip record write failed: {e}")))
    }
}

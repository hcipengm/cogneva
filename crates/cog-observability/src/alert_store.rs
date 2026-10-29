//! PostgreSQL-backed alert history.
//!
//! The alert state machine (`firing` → `resolved`) is stateful data: it needs
//! row-level updates and historical queries, which is why it lives in
//! PostgreSQL rather than in the append-only time-series backends. Alerts are
//! keyed by a stable `dedup_key` so repeated evaluations of the same condition
//! update one row instead of piling up duplicates, and a process restart can
//! still see what is currently firing.

use chrono::{DateTime, Utc};
use cog_core::{fill_slots, AlertState};
use serde_json::Value;
use sqlx::{PgPool, Row};

/// Resolve the `{state:...}` slots a statement names from [`AlertState`].
///
/// The alert table's `state` column holds this set and every query over it
/// names one of its words. Spelled here instead of read from the declaration,
/// the two agree only until a variant is renamed -- and then the guard reads
/// "no row is firing", which an operator is least likely to question.
fn alert_sql(template: &str) -> String {
    let mut slots: Vec<(&str, &str)> = Vec::new();
    for state in AlertState::ALL {
        slots.push(("state", state.as_str()));
    }
    fill_slots(template, &slots)
}

/// This store's schema, and the one a restore replays.
///
/// The partial index names the firing state, and the table's `state` column
/// holds a closed set, so the word is a slot here rather than a copy of
/// [`AlertState::as_str`]: a partial index on a spelling no row carries is a
/// valid index that never holds a row, which reads as "nothing is firing".
pub const DDL_TEMPLATE: &str = r#"
CREATE TABLE IF NOT EXISTS alerts (
                id          TEXT PRIMARY KEY,
                rule        TEXT        NOT NULL,
                dedup_key   TEXT        NOT NULL UNIQUE,
                severity    TEXT        NOT NULL,
                state       TEXT        NOT NULL,
                message     TEXT        NOT NULL,
                labels      JSONB       NOT NULL DEFAULT '{}'::jsonb,
                fired_at    TIMESTAMPTZ NOT NULL,
                resolved_at TIMESTAMPTZ,
                updated_at  TIMESTAMPTZ NOT NULL
            )
;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ
;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ
;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_by TEXT
;
-- 站内铃铛的投递标记。NULL 表示这一条 firing 还没人被告知：它同时是"接上通知之前
-- 就已在响的存量"与"写通知失败后待重试"的待办队列，因此不需要另建一张表。
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ
;
-- 这一行最后是谁断言的。同一份规则会被好几个进程各自评一遍（应用与进化 Pod 各起
-- 一个 watcher，共用同一张表），没有这一列时，事后读一行读不出「是谁写的」，
-- 也就分不清「一个写者说的」与「两个写者各自说、最后写入的那句覆盖了前一句」。
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS writer TEXT
;
-- 每个写者对每个键的最新判词，一行一个写者。冲突计数靠它：`alerts` 一行只存
-- 最后写入者的判词，覆盖之后前一个写者说的话就没有了，而分歧恰恰是两个人都还
-- 在断言、结论却相反的那段时间——那段时间在 `alerts` 上不留痕。
--
-- `role` 是「哪一种进程」（应用部署与进化部署各跑一个桥），`writer` 是那一具身体
-- （Pod 名）。分开存是因为它们的寿命不同：一次滚动里两具身体同时活着、属于同一个
-- role，旧的那具说的话与新那具相反——那是滚动的影子，不是两个写者的分歧。判分歧
-- 按 role 取最新的一具，滚动就不算数了。
CREATE TABLE IF NOT EXISTS alert_assertions (
                dedup_key   TEXT        NOT NULL,
                writer      TEXT        NOT NULL,
                role        TEXT        NOT NULL,
                state       TEXT        NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dedup_key, writer)
            )
;
-- `CREATE TABLE IF NOT EXISTS` says nothing to a table that is already there,
-- so a table created before this column existed keeps the old shape for good
-- and every insert that names the column fails -- which reads on the surface as
-- "no writer ever asserted anything", not as a schema that stopped moving.
-- Measured on 2026-10-29: a table written by an earlier revision of this DDL
-- was still three columns wide on the cluster, and the writer's own verdicts
-- were dropped silently while the shared rows kept updating. Added the same way
-- `alerts.writer` is, and the default exists only to backfill the rows that
-- predate it: dropped afterwards so a fresh table and a migrated one have the
-- same shape.
ALTER TABLE alert_assertions ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT ''
;
ALTER TABLE alert_assertions ALTER COLUMN role DROP DEFAULT
;
CREATE INDEX IF NOT EXISTS idx_alert_assertions_observed ON alert_assertions(observed_at DESC)
;
CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts(state, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unnotified ON alerts(fired_at) WHERE state = '{state:firing}' AND notified_at IS NULL;
"#;

/// The schema with the state words filled from [`AlertState`].
pub fn ddl() -> String {
    alert_sql(DDL_TEMPLATE)
}

/// How long a writer's last verdict is kept after that writer stops asserting.
///
/// A day, because nothing reads past five minutes (the divergence surface's
/// window) and a day of history is enough to explain a disagreement someone
/// noticed this morning. The point of the bound is that the table's size has to
/// follow the number of alert keys rather than the number of rollouts.
pub const ASSERTION_RETENTION_SECONDS: i64 = 24 * 60 * 60;

/// A new alert condition to raise (or close).
#[derive(Debug, Clone)]
pub struct NewAlert {
    /// Stable rule name, e.g. `llm_upstream_pool_down`.
    pub rule: String,
    /// Stable identity of this alert instance, e.g. the pool alert rule name.
    /// Re-evaluating the same condition must use the same key.
    pub dedup_key: String,
    pub severity: String,
    pub message: String,
    pub labels: Value,
}

/// What a `set_alert` call changed. Declared in the contract layer, because
/// the process that raises a condition and the one that stores the row are
/// different crates and both have to name the edge.
pub use cog_core::contract::alerts::AlertTransition;

/// The mutable payload of one stored row, read back before deciding what a
/// `set_alert` call has to write.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredAlert {
    pub state: String,
    pub severity: String,
    pub message: String,
    pub labels: Value,
}

impl StoredAlert {
    fn is_firing(&self) -> bool {
        self.state == AlertState::Firing.as_str()
    }
}

/// State transition for one evaluation, given the stored row (if any).
///
/// Pure so the branch table the SQL implements can be asserted directly.
fn decide(condition: bool, stored: Option<&StoredAlert>) -> AlertTransition {
    let firing = stored.is_some_and(StoredAlert::is_firing);
    match (condition, firing) {
        (true, false) => AlertTransition::Fired,
        (false, true) => AlertTransition::Resolved,
        _ => AlertTransition::NoChange,
    }
}

/// Whether an already-firing row's payload describes a different condition
/// reading than the one just evaluated.
///
/// A firing alert is a live projection of a condition that is still true, so
/// its message and labels must track the current evaluation. Leaving them at
/// the firing edge freezes whatever was true at that instant and keeps
/// presenting it as the current reading: a recovery estimate that has since
/// passed, a usage value that has since grown. Consumers cannot tell a stale
/// snapshot from a fresh one, so the refresh happens here rather than at each
/// reader.
fn payload_differs(stored: &StoredAlert, alert: &NewAlert) -> bool {
    stored.severity != alert.severity
        || stored.message != alert.message
        || stored.labels != alert.labels
}

/// What one evaluation writes to an already-firing row.
///
/// Pure, so the clock contract is assertable: the liveness clock advances on
/// every evaluation, the payload clock only when the reading moved. Folding
/// the two into one statement keyed on `payload_differs` is the regression
/// this exists to catch — it is silent at runtime, and its symptom (a stale
/// row that looks exactly like a live one) is the thing the extra clock is
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoChangeWrites {
    /// Whether the sighting clock moves. True for every evaluation that finds
    /// the condition still true.
    pub advance_last_seen: bool,
    /// Whether the payload and its clock are rewritten.
    pub rewrite_payload: bool,
}

fn no_change_writes(stored: &StoredAlert, alert: &NewAlert) -> NoChangeWrites {
    NoChangeWrites {
        advance_last_seen: true,
        rewrite_payload: payload_differs(stored, alert),
    }
}

/// One stored alert row.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRecord {
    pub id: String,
    pub rule: String,
    pub dedup_key: String,
    pub severity: String,
    /// `firing` or `resolved`.
    pub state: String,
    pub message: String,
    pub labels: Value,
    pub fired_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    /// When this row's payload last read differently. A clock of the *reading*.
    pub updated_at: DateTime<Utc>,
    /// When this condition was last evaluated and still found true. A clock of
    /// the *watcher*, which is why it is not the same field: for a rule whose
    /// payload is stable the two diverge, and a reader holding only
    /// `updated_at` cannot tell a condition nobody is looking at any more from
    /// one being confirmed every tick. `None` only for rows written before the
    /// column existed, and only until their next evaluation.
    pub last_seen_at: Option<DateTime<Utc>>,
    /// When a person acknowledged this firing episode, and who. Cleared on the
    /// next firing edge: an acknowledgement covers one episode.
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
}

/// What the alert bridge needs from the table: light or clear a row, and close
/// the rows nothing refreshes any more.
///
/// It is a trait so that "does the bridge keep closing them?" can be measured
/// without a database. That question has no other answer on the surface: the
/// sweep is the only part of the bridge that runs on a clock rather than on an
/// event, so a bridge that stopped sweeping looks exactly like a quiet fleet.
#[async_trait::async_trait]
pub trait AlertRowStore: Send + Sync {
    /// Write `alert` as firing (`condition`) or resolved.
    async fn set_alert(&self, condition: bool, alert: &NewAlert)
        -> anyhow::Result<AlertTransition>;

    /// Close the firing rows of `rule` whose `last_seen_at` is older than
    /// `stale_before`, answering how many were closed.
    async fn resolve_unrefreshed(
        &self,
        rule: &str,
        stale_before: DateTime<Utc>,
    ) -> anyhow::Result<u64>;

    /// Move `dedup_key`'s liveness clock forward if the row is still firing,
    /// answering whether it was.
    ///
    /// For the writer whose verdict cannot change while it lives: a judgement
    /// about the document this process started with is true for the rest of the
    /// process, so re-evaluating it would produce the same `true` every tick —
    /// but `last_seen_at` asks a different question, "is anything still holding
    /// this row up", and a verdict decided once has no second answer to give it.
    /// Measured 2026-10-27: `config_declaration_mismatch` fired once at boot and
    /// sat unread for 67 minutes; the sighting reading named it as a row nobody
    /// was maintaining, which was true and not a defect of the row.
    ///
    /// It touches an already-firing row and nothing else: a row closed by
    /// another process (a newer revision that starts on the corrected document)
    /// stays closed. Re-asserting the verdict instead would re-open it and page
    /// someone for something the next tick of a rollout has already fixed.
    async fn refresh_firing(&self, dedup_key: &str) -> anyhow::Result<bool>;
}

#[async_trait::async_trait]
impl AlertRowStore for PostgresAlertStore {
    async fn set_alert(
        &self,
        condition: bool,
        alert: &NewAlert,
    ) -> anyhow::Result<AlertTransition> {
        PostgresAlertStore::set_alert(self, condition, alert).await
    }

    async fn resolve_unrefreshed(
        &self,
        rule: &str,
        stale_before: DateTime<Utc>,
    ) -> anyhow::Result<u64> {
        PostgresAlertStore::resolve_unrefreshed(self, rule, stale_before).await
    }

    async fn refresh_firing(&self, dedup_key: &str) -> anyhow::Result<bool> {
        PostgresAlertStore::refresh_firing(self, dedup_key).await
    }
}

/// The name this process writes under.
///
/// In a pod `HOSTNAME` is the pod name, which is the identity a reader of the
/// table needs: two writers show up as two names, and a rollout shows up as a
/// new name rather than as a mysterious change of verdict on a row nobody
/// touched. Anywhere else the fallback still names exactly one writer, which is
/// the truth there too.
fn process_identity() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| String::from("unknown-writer"))
}

/// Which kind of process this is: one per deployment.
///
/// The name is the kind, not the body. Two bodies of the same kind overlapping
/// during a rollout are one writer handing over to another, and their differing
/// verdicts in the overlap are the rollout's shadow rather than a disagreement.
/// The application and the evolution deployment are genuinely two writers: both
/// evaluate the same rule set against the same table, and neither can see the
/// other's opinion there.
fn process_role() -> String {
    std::env::var(cog_core::env::DEPLOYMENT_NAME_ENV)
        .unwrap_or_else(|_| String::from("unknown-role"))
}

/// PostgreSQL alert store.
#[derive(Clone)]
pub struct PostgresAlertStore {
    pool: PgPool,
    /// Who this instance is, recorded on every row it writes and on every
    /// verdict it asserts. Several processes evaluate the same rule set against
    /// this one table; without the name, "the row says resolved" cannot be
    /// attributed to a writer, and a disagreement between two live writers is
    /// invisible — whichever wrote last simply wins the row.
    writer: String,
    /// Which kind of process it is; see [`process_role`].
    role: String,
}

impl PostgresAlertStore {
    pub fn new(pool: PgPool) -> Self {
        Self::with_identity(pool, process_identity(), process_role())
    }

    /// Build a store that writes under an explicit identity.
    ///
    /// For measuring what happens when two writers disagree: a test cannot be
    /// two pods, and a single process can hold two stores.
    pub fn with_identity(pool: PgPool, writer: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            pool,
            writer: writer.into(),
            role: role.into(),
        }
    }

    /// The name this store writes under.
    pub fn writer(&self) -> &str {
        &self.writer
    }

    /// The kind of process this store writes for.
    pub fn role(&self) -> &str {
        &self.role
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self::new(pool))
    }

    pub async fn init_schema(&self, schema: &dyn cog_core::SchemaRunner) -> anyhow::Result<()> {
        schema
            .apply_schema(&ddl())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        // The verdict table is keyed by writer, and a writer is a pod name: every
        // rollout leaves a generation of rows nothing will ever overwrite again.
        // Their only reader — the divergence surface — looks at a five-minute
        // window, so what is kept beyond a day is history nothing asks for, and
        // what is not pruned grows with the number of rollouts rather than with
        // the number of alerts. Pruned here because the set of dead writers
        // changes exactly when a process starts: a startup that adds a writer is
        // the moment to drop the ones that are no longer coming back.
        sqlx::query(
            "DELETE FROM alert_assertions \
             WHERE observed_at < now() - ($1::bigint * interval '1 second')",
        )
        .bind(ASSERTION_RETENTION_SECONDS)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Drive one alert condition to the requested state, idempotently.
    ///
    /// `condition = true` guarantees an open `firing` row exists (creating it
    /// on the first call); `false` resolves the open row if any. Callers get
    /// the edge transition so they log/notify once, not every tick.
    ///
    /// A row that is already firing keeps its state and `fired_at` — the edge
    /// is history — but has its payload rewritten whenever the new evaluation
    /// reads differently, so an open alert describes the condition now rather
    /// than the moment it started.
    ///
    /// Three clocks are maintained, and they answer three different questions:
    /// `fired_at` when the condition became true, `updated_at` when the row's
    /// payload last read differently, `last_seen_at` when the condition was
    /// last seen still true. The first two are history and move rarely; the
    /// third is the only one that shows whether anything is still looking. A
    /// single timestamp covering both jobs would leave a stable rule's row
    /// frozen at its firing edge with no way to tell it from an alert whose
    /// producer died — the condition reads exactly the same either way.
    pub async fn set_alert(
        &self,
        condition: bool,
        alert: &NewAlert,
    ) -> anyhow::Result<AlertTransition> {
        let stored =
            sqlx::query("SELECT state, severity, message, labels FROM alerts WHERE dedup_key = $1")
                .bind(&alert.dedup_key)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| StoredAlert {
                    state: row.get("state"),
                    severity: row.get("severity"),
                    message: row.get("message"),
                    labels: row.get("labels"),
                });

        match decide(condition, stored.as_ref()) {
            AlertTransition::Fired => {
                let now = Utc::now();
                sqlx::query(&alert_sql(
                    r#"
                    INSERT INTO alerts
                        (id, rule, dedup_key, severity, state, message, labels,
                         fired_at, resolved_at, updated_at, last_seen_at, writer)
                    VALUES ($1, $2, $3, $4, '{state:firing}', $5, $6, $7, NULL, $7, $7, $8)
                    ON CONFLICT (dedup_key) DO UPDATE SET
                        severity   = EXCLUDED.severity,
                        state      = '{state:firing}',
                        message    = EXCLUDED.message,
                        labels     = EXCLUDED.labels,
                        fired_at   = EXCLUDED.fired_at,
                        resolved_at = NULL,
                        updated_at = EXCLUDED.updated_at,
                        last_seen_at = EXCLUDED.last_seen_at,
                        writer     = EXCLUDED.writer,
                        -- A new firing edge starts a new episode: whoever
                        -- acknowledged the previous one did not acknowledge
                        -- this condition at this time, and carrying the mark
                        -- over would show a fresh alert as already handled.
                        acknowledged_at = NULL,
                        acknowledged_by = NULL,
                        -- A new episode has not been announced yet either: the
                        -- mark belongs to the episode that was notified, and
                        -- carrying it over would leave the bell silent for a
                        -- condition that just started again.
                        notified_at = NULL
                    "#,
                ))
                .bind(uuid::Uuid::new_v4().to_string())
                .bind(&alert.rule)
                .bind(&alert.dedup_key)
                .bind(&alert.severity)
                .bind(&alert.message)
                .bind(&alert.labels)
                .bind(now)
                .bind(&self.writer)
                .execute(&self.pool)
                .await?;
                self.record_assertion_best_effort(&alert.dedup_key, AlertState::Firing)
                    .await;
                Ok(AlertTransition::Fired)
            }
            AlertTransition::Resolved => {
                let now = Utc::now();
                sqlx::query(&alert_sql(
                    "UPDATE alerts SET state = '{state:resolved}', resolved_at = $2, \
                     updated_at = $2, writer = $3 \
                     WHERE dedup_key = $1 AND state = '{state:firing}'",
                ))
                .bind(&alert.dedup_key)
                .bind(now)
                .bind(&self.writer)
                .execute(&self.pool)
                .await?;
                self.record_assertion_best_effort(&alert.dedup_key, AlertState::Resolved)
                    .await;
                Ok(AlertTransition::Resolved)
            }
            AlertTransition::NoChange => {
                if let Some(open) = stored.as_ref().filter(|s| s.is_firing()) {
                    let plan = no_change_writes(open, alert);
                    let now = Utc::now();
                    // Unconditional while firing: this is the liveness clock,
                    // and a clock that only moves when the payload moves is
                    // not one.
                    if plan.advance_last_seen {
                        sqlx::query(&alert_sql(
                            "UPDATE alerts SET last_seen_at = $2, writer = $3 \
                             WHERE dedup_key = $1 AND state = '{state:firing}'",
                        ))
                        .bind(&alert.dedup_key)
                        .bind(now)
                        .bind(&self.writer)
                        .execute(&self.pool)
                        .await?;
                    }
                    if plan.rewrite_payload {
                        sqlx::query(&alert_sql(
                            "UPDATE alerts SET severity = $2, message = $3, labels = $4, \
                             updated_at = $5 WHERE dedup_key = $1 AND state = '{state:firing}'",
                        ))
                        .bind(&alert.dedup_key)
                        .bind(&alert.severity)
                        .bind(&alert.message)
                        .bind(&alert.labels)
                        .bind(now)
                        .execute(&self.pool)
                        .await?;
                    }
                }
                // A resolved row this writer keeps calling resolved is still a
                // verdict, and it is half of what makes "these two writers
                // disagree" a question about the key rather than about who
                // happens to be firing.
                if let Some(seen) = stored.as_ref() {
                    let state = if seen.is_firing() {
                        AlertState::Firing
                    } else {
                        AlertState::Resolved
                    };
                    self.record_assertion_best_effort(&alert.dedup_key, state)
                        .await;
                }
                Ok(AlertTransition::NoChange)
            }
        }
    }

    /// Record the verdict, but never let that bookkeeping decide the outcome.
    ///
    /// The row written above is the alert; this is the record of who said it.
    /// Letting a failure here travel out as `Err` from `set_alert` makes every
    /// caller read "nothing landed" — and they act on that by skipping the
    /// delivery step, so the alert row is written and nobody is told. Measured
    /// on 2026-10-30: a column missing from this table cost seven minutes in
    /// which no outward notification left the process at all while `alerts`
    /// kept moving. Losing the verdict is a real loss and must not be silent,
    /// so it is logged with the key that lost it; it is not, however, a reason
    /// to report the alert itself as unpersisted.
    async fn record_assertion_best_effort(&self, dedup_key: &str, state: AlertState) {
        if let Err(e) = self.record_assertion(dedup_key, state).await {
            tracing::warn!(
                dedup_key = %dedup_key,
                error = %e,
                "alert verdict not recorded; the alert row itself landed"
            );
        }
    }

    /// Record this writer's verdict on `dedup_key`, replacing its previous one.
    ///
    /// The `alerts` row keeps only the last writer's verdict, so a disagreement
    /// between two live writers leaves no trace there — the later write simply
    /// overwrites the earlier one, and the row ends up describing not "what is
    /// true" but "who evaluated last". One row per (key, writer) keeps this
    /// bounded by the alert table's own cardinality, and keeps each writer's
    /// clock separable: a writer that stops evaluating reads as stale here even
    /// while the other writer keeps the shared row fresh.
    async fn record_assertion(&self, dedup_key: &str, state: AlertState) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO alert_assertions (dedup_key, writer, role, state, observed_at) \
             VALUES ($1, $2, $3, $4, now()) \
             ON CONFLICT (dedup_key, writer) DO UPDATE SET \
                 role = EXCLUDED.role, \
                 state = EXCLUDED.state, \
                 observed_at = EXCLUDED.observed_at",
        )
        .bind(dedup_key)
        .bind(&self.writer)
        .bind(&self.role)
        .bind(state.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Close the firing rows of `rule` that nothing has refreshed since `stale_before`.
    ///
    /// Some rules describe a process's own in-memory bookkeeping rather than
    /// the cluster: `agent_unhealthy` is raised per agent id the supervisor's
    /// registry has stopped hearing from, and the only thing that keeps the row
    /// open is the same process saying so on the next pass. When that process
    /// restarts it comes back with an empty registry, so nothing will ever name
    /// those ids again — the rows stay firing for good. Measured on
    /// 2026-10-24: 3992 firing `agent_unhealthy` rows, the oldest untouched
    /// since 08:40 that morning, while sixteen live agents refreshed theirs
    /// every ten seconds. A surface that shows four thousand alerts nobody can
    /// act on is a surface nobody reads.
    ///
    /// The predicate is the liveness clock, not the rule alone: a row whose
    /// `last_seen_at` has not moved for several health intervals describes an
    /// agent the live pass is not looking at. Resolving it is safe in the only
    /// direction that matters — an agent that is genuinely still unhealthy
    /// re-fires on the next pass through the same dedup key, so a wrong guess
    /// repairs itself within one interval.
    ///
    /// Answers how many rows it closed.
    pub async fn resolve_unrefreshed(
        &self,
        rule: &str,
        stale_before: DateTime<Utc>,
    ) -> anyhow::Result<u64> {
        let now = Utc::now();
        let closed = sqlx::query(&alert_sql(
            "UPDATE alerts SET state = '{state:resolved}', resolved_at = $1, updated_at = $1 \
             WHERE rule = $2 AND state = '{state:firing}' \
               AND (last_seen_at IS NULL OR last_seen_at < $3)",
        ))
        .bind(now)
        .bind(rule)
        .bind(stale_before)
        .execute(&self.pool)
        .await?;
        Ok(closed.rows_affected())
    }

    /// Move the row's liveness clock forward, if it is still open.
    ///
    /// The `state = 'firing'` guard is the whole of it: this writer's verdict is
    /// fixed for its lifetime, so writing the verdict again would only re-open a
    /// row someone else has already closed. Answers whether a firing row was
    /// there to refresh — `false` is the caller's signal that this process's
    /// judgement no longer owns a row, and that it has nothing left to sight.
    pub async fn refresh_firing(&self, dedup_key: &str) -> anyhow::Result<bool> {
        let touched = sqlx::query(&alert_sql(
            "UPDATE alerts SET last_seen_at = $2, writer = $3 \
             WHERE dedup_key = $1 AND state = '{state:firing}'",
        ))
        .bind(dedup_key)
        .bind(Utc::now())
        .bind(&self.writer)
        .execute(&self.pool)
        .await?;
        let touched = touched.rows_affected() > 0;
        // Holding a row up is a verdict about it, the same one this writer will
        // reach every tick while it lives. Without this, a writer whose rule set
        // only ever refreshes would read as stopped in the assertions and drop
        // out of the divergence count — the safest direction to be wrong in, but
        // wrong: it would hide a disagreement behind a missing row.
        if touched {
            self.record_assertion_best_effort(dedup_key, AlertState::Firing)
                .await;
        }
        Ok(touched)
    }

    /// Record that `by` has seen the firing alert under `dedup_key`, answering
    /// when — or `None` when there is no firing row to acknowledge.
    ///
    /// Guarded on `state = 'firing'`: acknowledging an episode that already
    /// cleared would put a person's mark on the row the next firing reuses, and
    /// that mark would then read as "someone is on this" for a condition that
    /// person never saw. Nothing is written in that case, so a retry after a
    /// resolution is a no-op rather than a stale write that has to be undone.
    pub async fn acknowledge(
        &self,
        dedup_key: &str,
        by: &str,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        let now = Utc::now();
        let written = sqlx::query(&alert_sql(
            "UPDATE alerts SET acknowledged_at = $2, acknowledged_by = $3 \
             WHERE dedup_key = $1 AND state = '{state:firing}'",
        ))
        .bind(dedup_key)
        .bind(now)
        .bind(by)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok((written > 0).then_some(now))
    }

    /// The `dedup_key` of the row currently firing under `rule`, if any.
    ///
    /// A rule whose identity carries the event it belongs to holds one row per
    /// occurrence, and at most one of them is open at a time. A producer that
    /// only saw the firing edge can hold the key of the row it opened itself,
    /// but a producer that starts up mid-event -- a rolling restart, a crash,
    /// a second process elected to the seat -- never saw that edge and has no
    /// key to resolve. Reading the open key back is what lets it close the row
    /// it did not open, instead of either leaving it firing forever or minting
    /// a second row for an event already in progress.
    pub async fn firing_key_for_rule(&self, rule: &str) -> anyhow::Result<Option<String>> {
        let row = sqlx::query(&alert_sql(
            "SELECT dedup_key FROM alerts WHERE rule = $1 AND state = '{state:firing}' \
             ORDER BY fired_at DESC LIMIT 1",
        ))
        .bind(rule)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("dedup_key")))
    }

    /// Firing alerts nobody has been told about yet, oldest first.
    ///
    /// The queue the notification loop drains. Ordering by `fired_at` means a
    /// long-running alert that predates the loop is announced before one that
    /// started a moment ago — the person opening the bell reads the list the
    /// way it happened.
    pub async fn unnotified_firing(&self, limit: i64) -> anyhow::Result<Vec<NewAlert>> {
        let rows = sqlx::query(&alert_sql(
            "SELECT rule, dedup_key, severity, message, labels FROM alerts \
             WHERE state = '{state:firing}' AND notified_at IS NULL \
             ORDER BY fired_at ASC LIMIT $1",
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NewAlert {
                rule: row.get("rule"),
                dedup_key: row.get("dedup_key"),
                severity: row.get("severity"),
                message: row.get("message"),
                labels: row.get("labels"),
            })
            .collect())
    }

    /// Record that a firing alert has been announced.
    ///
    /// Conditional on both `firing` and `notified_at IS NULL` so the write is
    /// an edge, not a timer: a row resolved in the meantime must not receive a
    /// mark that would suppress the notice of its next episode.
    pub async fn mark_notified(&self, dedup_key: &str) -> anyhow::Result<bool> {
        let result = sqlx::query(&alert_sql(
            "UPDATE alerts SET notified_at = NOW() \
             WHERE dedup_key = $1 AND state = '{state:firing}' AND notified_at IS NULL",
        ))
        .bind(dedup_key)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Dedup keys of the firing rows one producer owns, in no particular order.
    ///
    /// Asked of the store rather than remembered in the process because the
    /// process is not the owner of the answer: a row outlives the writer that
    /// opened it, so a restart, a failed close, or a rule whose label set moved
    /// all leave rows that no in-memory set recalls. A writer that reconciles
    /// against its own memory therefore closes only what it still remembers,
    /// which is exactly the rows that needed no help.
    ///
    /// No limit: the caller is deciding what to close by subtracting what it
    /// just observed, and a truncated list reads as "the rest are not mine" --
    /// every row past the cut would be silently skipped, forever, because the
    /// next pass reads the same truncated list.
    pub async fn firing_keys_from_source(&self, source: &str) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query(&alert_sql(
            "SELECT dedup_key FROM alerts \
             WHERE state = '{state:firing}' AND labels->>'source' = $1",
        ))
        .bind(source)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.get("dedup_key")).collect())
    }

    /// Alerts currently in the `firing` state, newest first.
    pub async fn list_active(&self, limit: i64) -> anyhow::Result<Vec<AlertRecord>> {
        let rows = sqlx::query(&alert_sql(
            "SELECT id, rule, dedup_key, severity, state, message, labels, \
             fired_at, resolved_at, updated_at, last_seen_at, \
             acknowledged_at, acknowledged_by FROM alerts \
             WHERE state = '{state:firing}' ORDER BY fired_at DESC LIMIT $1",
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_record).collect())
    }

    /// Every firing row whose rule is one of `rules`, with no limit.
    ///
    /// The loop that adopts and retires rows on startup asks the store "which
    /// of my rules are still firing", and the answer has to be the whole
    /// answer. `list_active` takes a limit because it feeds a panel; adopting
    /// through it made the list a sample, and a sample is indistinguishable
    /// from the full set: past the limit, a row that is firing reads exactly
    /// like a row that does not exist, so it is neither adopted nor settled and
    /// stays `firing` for good. After a restart nothing else remembers it.
    ///
    /// The rule list bounds the answer instead of a row count: it is as long as
    /// the configuration, and every row a given rule can raise is in scope.
    pub async fn firing_rows_for_rules(
        &self,
        rules: &[String],
    ) -> anyhow::Result<Vec<AlertRecord>> {
        if rules.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(&alert_sql(
            "SELECT id, rule, dedup_key, severity, state, message, labels, \
             fired_at, resolved_at, updated_at, last_seen_at, \
             acknowledged_at, acknowledged_by FROM alerts \
             WHERE state = '{state:firing}' AND rule = ANY($1)",
        ))
        .bind(rules)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_record).collect())
    }

    /// Full alert history (firing and resolved), newest first.
    pub async fn list_history(&self, limit: i64) -> anyhow::Result<Vec<AlertRecord>> {
        let rows = sqlx::query(&alert_sql(
            "SELECT id, rule, dedup_key, severity, state, message, labels, \
             fired_at, resolved_at, updated_at, last_seen_at, \
             acknowledged_at, acknowledged_by FROM alerts \
             ORDER BY fired_at DESC LIMIT $1",
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_record).collect())
    }
}

fn row_to_record(row: sqlx::postgres::PgRow) -> AlertRecord {
    AlertRecord {
        id: row.get("id"),
        rule: row.get("rule"),
        dedup_key: row.get("dedup_key"),
        severity: row.get("severity"),
        state: row.get("state"),
        message: row.get("message"),
        labels: row.get("labels"),
        fired_at: row.get("fired_at"),
        resolved_at: row.get("resolved_at"),
        updated_at: row.get("updated_at"),
        last_seen_at: row.get("last_seen_at"),
        acknowledged_at: row.get("acknowledged_at"),
        acknowledged_by: row.get("acknowledged_by"),
    }
}

/// Self-discovery consumes firing alerts through this core-contract view,
/// keeping cog-reflection free of any storage-crate dependency. A read error
/// is reported as `None` rather than an empty list: a caller that treats
/// absence as a verdict would otherwise read a database hiccup as "nothing is
/// firing" and declare the cluster healthy on the strength of a failed query.
#[async_trait::async_trait]
impl cog_core::ActiveAlertSource for PostgresAlertStore {
    async fn list_active_alerts(&self, limit: i64) -> Option<Vec<cog_core::PersistedAlert>> {
        match self.list_active(limit).await {
            Ok(records) => Some(
                records
                    .into_iter()
                    .map(|r| cog_core::PersistedAlert {
                        rule: r.rule,
                        dedup_key: r.dedup_key,
                        severity: r.severity,
                        state: r.state,
                        message: r.message,
                        labels: r.labels,
                        fired_at: r.fired_at,
                        last_seen_at: r.last_seen_at,
                        acknowledged_at: r.acknowledged_at,
                        acknowledged_by: r.acknowledged_by,
                    })
                    .collect(),
            ),
            Err(e) => {
                tracing::warn!(error = %e, "listing active alerts failed");
                None
            }
        }
    }

    async fn acknowledge_alert(
        &self,
        dedup_key: &str,
        by: &str,
    ) -> Result<Option<DateTime<Utc>>, String> {
        self.acknowledge(dedup_key, by)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Write-side core contract: lets other crates drive durable alert state
/// machines (decomposition failure, stalled DAG tasks) through the same
/// PostgreSQL-backed store as infra alerts.
#[async_trait::async_trait]
impl cog_core::PersistentAlertSink for PostgresAlertStore {
    async fn set_persistent_alert(
        &self,
        condition: bool,
        draft: &cog_core::PersistentAlertDraft,
    ) -> Result<cog_core::AlertTransition, String> {
        let alert = NewAlert {
            rule: draft.rule.clone(),
            dedup_key: draft.dedup_key.clone(),
            severity: draft.severity.clone(),
            message: draft.message.clone(),
            labels: draft.labels.clone(),
        };
        self.set_alert(condition, &alert)
            .await
            .map(|transition| match transition {
                AlertTransition::NoChange => cog_core::AlertTransition::NoChange,
                AlertTransition::Fired => cog_core::AlertTransition::Fired,
                AlertTransition::Resolved => cog_core::AlertTransition::Resolved,
            })
            .map_err(|e| e.to_string())
    }

    async fn firing_key_for_rule(&self, rule: &str) -> Result<Option<String>, String> {
        PostgresAlertStore::firing_key_for_rule(self, rule)
            .await
            .map_err(|e| e.to_string())
    }

    async fn list_active_persistent_alerts(
        &self,
        rule_prefix: &str,
        limit: i64,
    ) -> Vec<cog_core::PersistedAlert> {
        match self.list_active(limit).await {
            Ok(records) => records
                .into_iter()
                .filter(|r| r.rule.starts_with(rule_prefix))
                .map(|r| cog_core::PersistedAlert {
                    rule: r.rule,
                    dedup_key: r.dedup_key,
                    severity: r.severity,
                    state: r.state,
                    message: r.message,
                    labels: r.labels,
                    fired_at: r.fired_at,
                    last_seen_at: r.last_seen_at,
                    acknowledged_at: r.acknowledged_at,
                    acknowledged_by: r.acknowledged_by,
                })
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "listing active alerts for prefix failed");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 建表语句交给的是调用方递进来的那个 runner。
    ///
    /// 这个 crate 曾经自己连库、自己调存储层的执行器：那条依赖让「按什么方式跑建表
    /// 语句」和「跑在哪个后端上」绑在一起，于是这个 store 只能在链着那个驱动栈的
    /// 进程里存在。缝换成参数之后，编译通过只证明「有人可以递进来」——不证明它真被
    /// 用了：把参数丢掉、另找一条路跑，签名照样对。所以这里递一个只记账的 runner，
    /// 读它收到了什么。
    #[tokio::test]
    async fn the_schema_text_runs_through_the_runner_it_is_handed() {
        use std::sync::Mutex;

        struct Recorder {
            seen: Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl cog_core::SchemaRunner for Recorder {
            async fn apply_schema(&self, ddl: &str) -> cog_core::SFResult<()> {
                self.seen.lock().unwrap().push(ddl.to_string());
                Ok(())
            }
        }

        let recorder = Recorder {
            seen: Mutex::new(Vec::new()),
        };
        // 一个永远不会被连上的池：这个用例要证明的是「语句没有走池」，所以池在
        // 这里连不通反而更好——真被用上就会失败。
        let pool = sqlx::PgPool::connect_lazy("postgres://user:pw@127.0.0.1:1/none")
            .expect("a lazy pool parses without connecting");
        let store = PostgresAlertStore::new(pool);
        store
            .init_schema(&recorder)
            .await
            .expect("the recorder answers every attempt");

        let seen = recorder.seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            1,
            "建表语句应当恰好交给递进来的 runner 一次，实得 {} 次",
            seen.len()
        );
        assert!(
            seen[0].contains("CREATE TABLE IF NOT EXISTS alerts"),
            "交出去的应当是这张表自己的语句：{}",
            seen[0]
        );
    }

    /// Every statement this store builds is rendered here, not in the database:
    /// a slot left behind is a parse error at the moment an operator needs the
    /// alert, and a guard that names the wrong state word is a guard that
    /// matches nothing.
    #[test]
    fn the_statements_name_the_states_the_enum_declares() {
        let firing = AlertState::Firing.as_str();
        let resolved = AlertState::Resolved.as_str();
        let schema = ddl();
        // `'{}'::jsonb` 是列默认值，不是槽：只认 `{name:word}` 那种形状。
        assert!(!schema.contains("{state:"), "DDL 还有没填的槽：{schema}");
        assert!(schema.contains(&format!("state = '{firing}'")), "{schema}");
        for state in AlertState::ALL {
            assert!(AlertState::parse(state.as_str()) == Some(state));
        }
        let close = alert_sql(
            "UPDATE alerts SET state = '{state:resolved}' \
             WHERE dedup_key = $1 AND state = '{state:firing}'",
        );
        assert!(!close.contains('{'), "{close}");
        assert!(close.contains(&format!("state = '{resolved}'")), "{close}");
        assert!(close.contains(&format!("state = '{firing}'")), "{close}");
        assert_eq!(AlertState::parse("recovered"), None);
    }

    /// 一张在 `role` 存在之前建出来的表，不会因为建表语句再跑一遍就长出这一列。
    ///
    /// `CREATE TABLE IF NOT EXISTS` 对已经在那儿的表是一句空话：它不校验列，也不
    /// 补列。先跑的旧版本把表留成三列之后，后来每一笔按名字写 `role` 的断言都在
    /// 准备阶段报 `column "role" ... does not exist` —— 而这一行日志读起来不像
    /// 「表停在旧形状」，像「没有任何写者断言过任何东西」。线上就是这个形状：
    /// `alerts` 一直在更新，`alert_assertions` 停在几天前的三行测试残留。
    ///
    /// 所以整条写路径（而不仅是列是否存在）在一个旧形状的表上跑一遍：建表语句要
    /// 补上这一列，补完之后 `set_alert` 的断言要真的落成一行。
    #[tokio::test]
    #[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
    async fn a_table_created_before_the_role_column_takes_it() {
        let url = cog_test_support::database_url();
        let schema = cog_test_support::own_schema(&url, "alert_assert_role").await;
        let pool = cog_test_support::schema_pool(&url, &schema).await;

        // 早一版建表语句留下的三列形状，外加一行比这一列更早的断言行：迁移能不能
        // 跑通是一回事，旧行迁移后读作什么值是另一回事。
        sqlx::query(
            "CREATE TABLE alert_assertions (\
                 dedup_key TEXT NOT NULL, writer TEXT NOT NULL, state TEXT NOT NULL, \
                 observed_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (dedup_key, writer))",
        )
        .execute(&pool)
        .await
        .expect("the older shape is created");
        sqlx::query(
            "INSERT INTO alert_assertions (dedup_key, writer, state, observed_at) \
             VALUES ('older-row', 'older-writer', 'firing', now())",
        )
        .execute(&pool)
        .await
        .expect("a row from before the column exists");

        let store = PostgresAlertStore::with_identity(pool.clone(), "test-writer", "test-role");
        let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
        store
            .init_schema(&schema_runner)
            .await
            .expect("the schema text runs against a table an older revision built");

        let said = alert(POOL_MESSAGE, 1_789_744_595, "critical");
        store
            .set_alert(true, &said)
            .await
            .expect("写断言这条路在一张迁移过的表上必须能走通");

        let backfilled: String =
            sqlx::query_scalar("SELECT role FROM alert_assertions WHERE dedup_key = 'older-row'")
                .fetch_one(&pool)
                .await
                .expect("the column this DDL adds is readable");
        assert_eq!(
            backfilled, "",
            "迁移前就有的行补空角色：它们是这一列出现之前写的，猜一个角色就是把\
             「不知道」写成「知道」"
        );

        let role: String = sqlx::query_scalar(
            "SELECT role FROM alert_assertions WHERE dedup_key = $1 AND writer = 'test-writer'",
        )
        .bind(&said.dedup_key)
        .fetch_one(&pool)
        .await
        .expect("the assertion the store just wrote is on the table");
        assert_eq!(role, "test-role", "新写入的断言带上写者的角色");

        cog_test_support::drop_schema(&url, &schema).await;
    }

    /// The state machine is the part worth pinning down; `set_alert` is pure
    /// decision logic over the stored state. Exercise it against the same
    /// branch table the SQL implements.
    const POOL_MESSAGE: &str =
        "all four upstreams unavailable (no upstream reported a recovery time; \
         next attempt unix 1789744595)";

    fn stored(state: &str) -> StoredAlert {
        StoredAlert {
            state: state.into(),
            severity: "critical".into(),
            message: POOL_MESSAGE.into(),
            labels: serde_json::json!({
                "evidenced_recovery_unix": 0,
                "next_attempt_unix": 1_789_744_595_i64,
            }),
        }
    }

    fn alert(message: &str, next_attempt: i64, severity: &str) -> NewAlert {
        // Both bounds, as the real alert carries them: a fixture that dropped
        // one would make an unchanged reading look like a changed one and hide
        // the very behaviour under test.
        NewAlert {
            rule: "llm_upstream_pool_down".into(),
            dedup_key: "llm_upstream_pool_down".into(),
            severity: severity.into(),
            message: message.into(),
            labels: serde_json::json!({
                "evidenced_recovery_unix": 0,
                "next_attempt_unix": next_attempt,
            }),
        }
    }

    #[test]
    fn alert_state_machine_edges() {
        assert_eq!(decide(true, None), AlertTransition::Fired);
        assert_eq!(
            decide(true, Some(&stored("firing"))),
            AlertTransition::NoChange
        );
        // Re-raising a resolved alert fires again.
        assert_eq!(
            decide(true, Some(&stored("resolved"))),
            AlertTransition::Fired
        );
        assert_eq!(
            decide(false, Some(&stored("firing"))),
            AlertTransition::Resolved
        );
        assert_eq!(decide(false, None), AlertTransition::NoChange);
        assert_eq!(
            decide(false, Some(&stored("resolved"))),
            AlertTransition::NoChange
        );
    }

    /// A firing alert is a projection of a condition that is still true, so a
    /// new evaluation that reads differently has to rewrite the row. The
    /// failure this guards against is silent: the row keeps answering with the
    /// reading taken at the firing edge, and a consumer reading it later cannot
    /// tell that from a fresh one.
    #[test]
    fn firing_row_is_rewritten_when_the_reading_moves() {
        let open = stored("firing");
        // A retry estimate that has since been pushed forward.
        assert!(payload_differs(
            &open,
            &alert(
                "all four upstreams unavailable (no upstream reported a recovery time; \
                 next attempt unix 1789749020)",
                1_789_749_020,
                "critical"
            )
        ));
        // The reading may move in any of the three fields, not just the message.
        assert!(payload_differs(
            &open,
            &alert(POOL_MESSAGE, 1_789_744_595, "warning")
        ));
        assert!(payload_differs(
            &open,
            &alert(POOL_MESSAGE, 1_799_129_599, "critical")
        ));
    }

    /// Re-evaluating an unchanged condition writes nothing: the refresh exists
    /// to keep the payload true, not to churn the row on every tick.
    #[test]
    fn firing_row_is_left_alone_when_the_reading_is_identical() {
        let open = stored("firing");
        assert!(!payload_differs(
            &open,
            &alert(POOL_MESSAGE, 1_789_744_595, "critical")
        ));
    }

    /// An unchanged reading still advances the sighting clock. This is the
    /// whole point of keeping it apart from `updated_at`: for a rule whose
    /// payload is stable — the stall alert's, whose weekly breakdown only moves
    /// when a week does — the payload clock sits at the firing edge forever,
    /// and a reader holding only that cannot tell the row from one whose
    /// producer died.
    #[test]
    fn an_unchanged_reading_still_counts_as_a_sighting() {
        let open = stored("firing");
        let writes = no_change_writes(&open, &alert(POOL_MESSAGE, 1_789_744_595, "critical"));
        assert!(writes.advance_last_seen);
        assert!(!writes.rewrite_payload);
    }

    /// A moved reading advances both. The failure this rules out is the
    /// opposite regression: making the sighting conditional on the payload,
    /// which leaves the clock frozen for exactly the rules that need it most.
    #[test]
    fn a_moved_reading_advances_both_clocks() {
        let open = stored("firing");
        let writes = no_change_writes(&open, &alert(POOL_MESSAGE, 1_789_749_020, "critical"));
        assert!(writes.advance_last_seen);
        assert!(writes.rewrite_payload);
    }

    /// A lookup that failed must not arrive as a lookup that found nothing.
    ///
    /// This is the producer side of the verdict that reads these alerts: the
    /// consumers turn an empty answer into "nothing is wrong" (a healthy
    /// cluster) or into "no recovery work to do", so a query error collapsing
    /// to an empty list would manufacture both conclusions out of a failure.
    ///
    /// A port nothing listens on gives a real connection failure without a
    /// database to run against; the pool reports it well within a second, and
    /// the short acquire timeout keeps the retry from stretching the test.
    #[tokio::test]
    async fn a_lookup_that_failed_answers_none_and_not_an_empty_list() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect_lazy("postgres://nobody@127.0.0.1:1/nothing")
            .expect("building a pool must not require a reachable server");
        let store = PostgresAlertStore::new(pool);

        let answer = cog_core::ActiveAlertSource::list_active_alerts(&store, 1).await;
        assert!(
            answer.is_none(),
            "a failed read reached the caller as an answer: {answer:?}"
        );
    }
}

//! PostgreSQL-backed alert history.
//!
//! The alert state machine (`firing` → `resolved`) is stateful data: it needs
//! row-level updates and historical queries, which is why it lives in
//! PostgreSQL rather than in the append-only time-series backends. Alerts are
//! keyed by a stable `dedup_key` so repeated evaluations of the same condition
//! update one row instead of piling up duplicates, and a process restart can
//! still see what is currently firing.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};

/// This store's schema, and the one a restore replays.
pub const DDL: &str = r#"
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
CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts(state, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unnotified ON alerts(fired_at) WHERE state = 'firing' AND notified_at IS NULL;
"#;

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

/// What a `set_alert` call changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertTransition {
    /// The condition already matched the stored state. The row's payload may
    /// still have been refreshed; callers key their notifications on the
    /// state edge, not on this variant.
    NoChange,
    /// A new firing alert was recorded.
    Fired,
    /// An open alert was marked resolved.
    Resolved,
}

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
        self.state == "firing"
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

/// PostgreSQL alert store.
#[derive(Clone)]
pub struct PostgresAlertStore {
    pool: PgPool,
}

impl PostgresAlertStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self::new(pool))
    }

    pub async fn init_schema(&self) -> anyhow::Result<()> {
        sqlx::raw_sql(DDL).execute(&self.pool).await?;
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
                sqlx::query(
                    r#"
                    INSERT INTO alerts
                        (id, rule, dedup_key, severity, state, message, labels,
                         fired_at, resolved_at, updated_at, last_seen_at)
                    VALUES ($1, $2, $3, $4, 'firing', $5, $6, $7, NULL, $7, $7)
                    ON CONFLICT (dedup_key) DO UPDATE SET
                        severity   = EXCLUDED.severity,
                        state      = 'firing',
                        message    = EXCLUDED.message,
                        labels     = EXCLUDED.labels,
                        fired_at   = EXCLUDED.fired_at,
                        resolved_at = NULL,
                        updated_at = EXCLUDED.updated_at,
                        last_seen_at = EXCLUDED.last_seen_at,
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
                )
                .bind(uuid::Uuid::new_v4().to_string())
                .bind(&alert.rule)
                .bind(&alert.dedup_key)
                .bind(&alert.severity)
                .bind(&alert.message)
                .bind(&alert.labels)
                .bind(now)
                .execute(&self.pool)
                .await?;
                Ok(AlertTransition::Fired)
            }
            AlertTransition::Resolved => {
                let now = Utc::now();
                sqlx::query(
                    "UPDATE alerts SET state = 'resolved', resolved_at = $2, updated_at = $2 \
                     WHERE dedup_key = $1 AND state = 'firing'",
                )
                .bind(&alert.dedup_key)
                .bind(now)
                .execute(&self.pool)
                .await?;
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
                        sqlx::query(
                            "UPDATE alerts SET last_seen_at = $2 \
                             WHERE dedup_key = $1 AND state = 'firing'",
                        )
                        .bind(&alert.dedup_key)
                        .bind(now)
                        .execute(&self.pool)
                        .await?;
                    }
                    if plan.rewrite_payload {
                        sqlx::query(
                            "UPDATE alerts SET severity = $2, message = $3, labels = $4, \
                             updated_at = $5 WHERE dedup_key = $1 AND state = 'firing'",
                        )
                        .bind(&alert.dedup_key)
                        .bind(&alert.severity)
                        .bind(&alert.message)
                        .bind(&alert.labels)
                        .bind(now)
                        .execute(&self.pool)
                        .await?;
                    }
                }
                Ok(AlertTransition::NoChange)
            }
        }
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
        let written = sqlx::query(
            "UPDATE alerts SET acknowledged_at = $2, acknowledged_by = $3 \
             WHERE dedup_key = $1 AND state = 'firing'",
        )
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
        let row = sqlx::query(
            "SELECT dedup_key FROM alerts WHERE rule = $1 AND state = 'firing' \
             ORDER BY fired_at DESC LIMIT 1",
        )
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
        let rows = sqlx::query(
            "SELECT rule, dedup_key, severity, message, labels FROM alerts \
             WHERE state = 'firing' AND notified_at IS NULL \
             ORDER BY fired_at ASC LIMIT $1",
        )
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
        let result = sqlx::query(
            "UPDATE alerts SET notified_at = NOW() \
             WHERE dedup_key = $1 AND state = 'firing' AND notified_at IS NULL",
        )
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
        let rows = sqlx::query(
            "SELECT dedup_key FROM alerts \
             WHERE state = 'firing' AND labels->>'source' = $1",
        )
        .bind(source)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.get("dedup_key")).collect())
    }

    /// Alerts currently in the `firing` state, newest first.
    pub async fn list_active(&self, limit: i64) -> anyhow::Result<Vec<AlertRecord>> {
        let rows = sqlx::query(
            "SELECT id, rule, dedup_key, severity, state, message, labels, \
             fired_at, resolved_at, updated_at, last_seen_at, \
             acknowledged_at, acknowledged_by FROM alerts \
             WHERE state = 'firing' ORDER BY fired_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_record).collect())
    }

    /// Full alert history (firing and resolved), newest first.
    pub async fn list_history(&self, limit: i64) -> anyhow::Result<Vec<AlertRecord>> {
        let rows = sqlx::query(
            "SELECT id, rule, dedup_key, severity, state, message, labels, \
             fired_at, resolved_at, updated_at, last_seen_at, \
             acknowledged_at, acknowledged_by FROM alerts \
             ORDER BY fired_at DESC LIMIT $1",
        )
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
    ) -> Result<(), String> {
        let alert = NewAlert {
            rule: draft.rule.clone(),
            dedup_key: draft.dedup_key.clone(),
            severity: draft.severity.clone(),
            message: draft.message.clone(),
            labels: draft.labels.clone(),
        };
        self.set_alert(condition, &alert)
            .await
            .map(|_| ())
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

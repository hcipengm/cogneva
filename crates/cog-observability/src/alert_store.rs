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
    pub updated_at: DateTime<Utc>,
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
        sqlx::query(
            r#"
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
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts(state, fired_at DESC)")
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
                         fired_at, resolved_at, updated_at)
                    VALUES ($1, $2, $3, $4, 'firing', $5, $6, $7, NULL, $7)
                    ON CONFLICT (dedup_key) DO UPDATE SET
                        severity   = EXCLUDED.severity,
                        state      = 'firing',
                        message    = EXCLUDED.message,
                        labels     = EXCLUDED.labels,
                        fired_at   = EXCLUDED.fired_at,
                        resolved_at = NULL,
                        updated_at = EXCLUDED.updated_at
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
                    if payload_differs(open, alert) {
                        let now = Utc::now();
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

    /// Alerts currently in the `firing` state, newest first.
    pub async fn list_active(&self, limit: i64) -> anyhow::Result<Vec<AlertRecord>> {
        let rows = sqlx::query(
            "SELECT id, rule, dedup_key, severity, state, message, labels, \
             fired_at, resolved_at, updated_at FROM alerts \
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
             fired_at, resolved_at, updated_at FROM alerts \
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
    }
}

/// Self-discovery consumes firing alerts through this core-contract view,
/// keeping cog-reflection free of any storage-crate dependency. Read errors
/// degrade to an empty list: a watcher tick must never panic on a transient
/// database hiccup, and the next tick retries.
#[async_trait::async_trait]
impl cog_core::ActiveAlertSource for PostgresAlertStore {
    async fn list_active_alerts(&self, limit: i64) -> Vec<cog_core::PersistedAlert> {
        match self.list_active(limit).await {
            Ok(records) => records
                .into_iter()
                .map(|r| cog_core::PersistedAlert {
                    rule: r.rule,
                    dedup_key: r.dedup_key,
                    severity: r.severity,
                    state: r.state,
                    message: r.message,
                    labels: r.labels,
                    fired_at: r.fired_at,
                })
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "listing active alerts failed");
                Vec::new()
            }
        }
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
    fn stored(state: &str) -> StoredAlert {
        StoredAlert {
            state: state.into(),
            severity: "critical".into(),
            message: "all four upstreams unavailable (earliest recovery 04:56)".into(),
            labels: serde_json::json!({"earliest_recovery_unix": 1_789_744_595_i64}),
        }
    }

    fn alert(message: &str, recovery: i64, severity: &str) -> NewAlert {
        NewAlert {
            rule: "llm_upstream_pool_down".into(),
            dedup_key: "llm_upstream_pool_down".into(),
            severity: severity.into(),
            message: message.into(),
            labels: serde_json::json!({"earliest_recovery_unix": recovery}),
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
        // A recovery estimate that has since been pushed forward.
        assert!(payload_differs(
            &open,
            &alert(
                "all four upstreams unavailable (earliest recovery 16:30)",
                1_789_749_020,
                "critical"
            )
        ));
        // The reading may move in any of the three fields, not just the message.
        assert!(payload_differs(
            &open,
            &alert(
                "all four upstreams unavailable (earliest recovery 04:56)",
                1_789_744_595,
                "warning"
            )
        ));
        assert!(payload_differs(
            &open,
            &alert(
                "all four upstreams unavailable (earliest recovery 04:56)",
                1_799_129_599,
                "critical"
            )
        ));
    }

    /// Re-evaluating an unchanged condition writes nothing: the refresh exists
    /// to keep the payload true, not to churn the row on every tick.
    #[test]
    fn firing_row_is_left_alone_when_the_reading_is_identical() {
        let open = stored("firing");
        assert!(!payload_differs(
            &open,
            &alert(
                "all four upstreams unavailable (earliest recovery 04:56)",
                1_789_744_595,
                "critical"
            )
        ));
    }
}

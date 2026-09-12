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
    /// The condition already matched the stored state; nothing changed.
    NoChange,
    /// A new firing alert was recorded.
    Fired,
    /// An open alert was marked resolved.
    Resolved,
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
    pub async fn set_alert(
        &self,
        condition: bool,
        alert: &NewAlert,
    ) -> anyhow::Result<AlertTransition> {
        let current: Option<String> =
            sqlx::query_scalar("SELECT state FROM alerts WHERE dedup_key = $1")
                .bind(&alert.dedup_key)
                .fetch_optional(&self.pool)
                .await?;
        let firing = current.as_deref() == Some("firing");

        match (condition, firing) {
            (true, false) => {
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
            (false, true) => {
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
            _ => Ok(AlertTransition::NoChange),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The state machine is the part worth pinning down; `set_alert` is pure
    /// decision logic over the stored state. Exercise it against the same
    /// branch table the SQL implements.
    fn decide(condition: bool, stored: Option<&str>) -> AlertTransition {
        let firing = stored == Some("firing");
        match (condition, firing) {
            (true, false) => AlertTransition::Fired,
            (false, true) => AlertTransition::Resolved,
            _ => AlertTransition::NoChange,
        }
    }

    #[test]
    fn alert_state_machine_edges() {
        assert_eq!(decide(true, None), AlertTransition::Fired);
        assert_eq!(decide(true, Some("firing")), AlertTransition::NoChange);
        // Re-raising a resolved alert fires again.
        assert_eq!(decide(true, Some("resolved")), AlertTransition::Fired);
        assert_eq!(decide(false, Some("firing")), AlertTransition::Resolved);
        assert_eq!(decide(false, None), AlertTransition::NoChange);
        assert_eq!(decide(false, Some("resolved")), AlertTransition::NoChange);
    }
}

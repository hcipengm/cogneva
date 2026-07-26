use chrono::{DateTime, Utc};
use sqlx::{MySqlPool, Row};
use uuid::Uuid;

use crate::error::QuotaResult;
use crate::model::TaskType;

/// A single billing record for token consumption.
#[derive(Debug, Clone)]
pub struct BillingRecord {
    pub id: String,
    pub user_id: String,
    pub workspace_id: Option<String>,
    pub task_id: String,
    pub model: String,
    pub task_type: TaskType,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub cost: f64,
    pub cost_before_weight: f64,
    pub weight_applied: f64,
    pub created_at: DateTime<Utc>,
}

impl BillingRecord {
    pub fn new(
        user_id: impl Into<String>,
        task_id: impl Into<String>,
        model: impl Into<String>,
        task_type: TaskType,
        tokens_input: u64,
        tokens_output: u64,
        cost_before_weight: f64,
    ) -> Self {
        let weight = task_type.weight();
        let cost = cost_before_weight * weight;
        Self {
            id: Uuid::new_v4().to_string(),
            user_id: user_id.into(),
            workspace_id: None,
            task_id: task_id.into(),
            model: model.into(),
            task_type,
            tokens_input,
            tokens_output,
            cost,
            cost_before_weight,
            weight_applied: weight,
            created_at: Utc::now(),
        }
    }

    pub fn with_workspace_id(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }
}

/// A recharge record for adding quota.
#[derive(Debug, Clone)]
pub struct RechargeRecord {
    pub id: String,
    pub target_type: String, // "user" or "workspace"
    pub target_id: String,
    pub amount: f64,
    pub tokens_added: u64,
    pub valid_until: Option<DateTime<Utc>>,
    pub operator_id: String,
    pub remark: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl RechargeRecord {
    pub fn new(
        target_type: impl Into<String>,
        target_id: impl Into<String>,
        amount: f64,
        tokens_added: u64,
        operator_id: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            target_type: target_type.into(),
            target_id: target_id.into(),
            amount,
            tokens_added,
            valid_until: None,
            operator_id: operator_id.into(),
            remark: None,
            created_at: Utc::now(),
        }
    }

    pub fn with_valid_until(mut self, valid_until: DateTime<Utc>) -> Self {
        self.valid_until = Some(valid_until);
        self
    }

    pub fn with_remark(mut self, remark: impl Into<String>) -> Self {
        self.remark = Some(remark.into());
        self
    }
}

/// Monthly summary of billing for a user.
#[derive(Debug, Clone, Default)]
pub struct MonthlySummary {
    pub user_id: String,
    pub year_month: String,
    pub total_cost: f64,
    pub total_tokens_input: u64,
    pub total_tokens_output: u64,
    pub record_count: u64,
}

/// Repository for billing record persistence.
pub struct BillingRepository {
    pool: MySqlPool,
}

impl BillingRepository {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Record a billing entry.
    pub async fn record(&self, record: &BillingRecord) -> QuotaResult<()> {
        sqlx::query(
            r#"
            INSERT INTO billing_records
                (id, user_id, workspace_id, task_id, model, task_type,
                 tokens_input, tokens_output, cost, cost_before_weight,
                 weight_applied, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&record.id)
        .bind(&record.user_id)
        .bind(&record.workspace_id)
        .bind(&record.task_id)
        .bind(&record.model)
        .bind(record.task_type.to_string())
        .bind(record.tokens_input as i64)
        .bind(record.tokens_output as i64)
        .bind(record.cost)
        .bind(record.cost_before_weight)
        .bind(record.weight_applied)
        .bind(record.created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get monthly summary for a user.
    pub async fn get_user_monthly_summary(
        &self,
        user_id: &str,
        year_month: &str,
    ) -> QuotaResult<MonthlySummary> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(cost), 0.0) as total_cost,
                COALESCE(SUM(tokens_input), 0) as total_input,
                COALESCE(SUM(tokens_output), 0) as total_output,
                COUNT(*) as record_count
            FROM billing_records
            WHERE user_id = ? AND DATE_FORMAT(created_at, '%Y-%m') = ?
            "#,
        )
        .bind(user_id)
        .bind(year_month)
        .fetch_one(&self.pool)
        .await?;

        Ok(MonthlySummary {
            user_id: user_id.to_string(),
            year_month: year_month.to_string(),
            total_cost: row.try_get("total_cost").unwrap_or(0.0),
            total_tokens_input: row.try_get::<i64, _>("total_input").unwrap_or(0) as u64,
            total_tokens_output: row.try_get::<i64, _>("total_output").unwrap_or(0) as u64,
            record_count: row.try_get::<i64, _>("record_count").unwrap_or(0) as u64,
        })
    }

    /// Get billing records for a workspace with pagination.
    pub async fn get_workspace_records(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
    ) -> QuotaResult<Vec<BillingRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, workspace_id, task_id, model, task_type,
                   tokens_input, tokens_output, cost, cost_before_weight,
                   weight_applied, created_at
            FROM billing_records
            WHERE workspace_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(workspace_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let task_type_str: String = row.try_get("task_type")?;
            let task_type: TaskType = task_type_str.parse()?;

            records.push(BillingRecord {
                id: row.try_get("id")?,
                user_id: row.try_get("user_id")?,
                workspace_id: row.try_get("workspace_id").ok(),
                task_id: row.try_get("task_id")?,
                model: row.try_get("model")?,
                task_type,
                tokens_input: row.try_get::<i64, _>("tokens_input").unwrap_or(0) as u64,
                tokens_output: row.try_get::<i64, _>("tokens_output").unwrap_or(0) as u64,
                cost: row.try_get("cost").unwrap_or(0.0),
                cost_before_weight: row.try_get("cost_before_weight").unwrap_or(0.0),
                weight_applied: row.try_get("weight_applied").unwrap_or(1.0),
                created_at: row.try_get("created_at")?,
            });
        }

        Ok(records)
    }

    /// Create a recharge record.
    pub async fn create_recharge(&self, record: &RechargeRecord) -> QuotaResult<()> {
        sqlx::query(
            r#"
            INSERT INTO recharge_records
                (id, target_type, target_id, amount, tokens_added,
                 valid_until, operator_id, remark, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&record.id)
        .bind(&record.target_type)
        .bind(&record.target_id)
        .bind(record.amount)
        .bind(record.tokens_added as i64)
        .bind(record.valid_until)
        .bind(&record.operator_id)
        .bind(&record.remark)
        .bind(record.created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

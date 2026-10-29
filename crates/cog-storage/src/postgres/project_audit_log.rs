//! 项目面写操作的审计，落到 `audit_logs` 这张表上。
//!
//! 这张表从 017 迁移建起来就什么都有：DDL、按月分区、按 user/action/resource 三条
//! 索引，还有一个分区维护者每月给它开新分区——但全平台没有一个写者。于是「谁在什么时候
//! 把哪张卡挪到了哪一列」这件事，卖出去之后只在文件版的哈希链里，而那条链是给「有没有
//! 被篡改」用的：它能证明历史没被动过，不能回答「这张任务改过几次、每次改成什么」。
//! 客户的安全评审问的是后者。
//!
//! 两份都写是有意的，不是重复：链**按顺序**追加、每条带前一条的哈希，任何一处被改写
//! 后面全对不上；表**按资源**建索引，「某一张任务的改动」是一次索引扫描。前者查完整性，
//! 后者查人。一份既当链又当索引的数据结构两头都做不好。
//!
//! 落库失败不让写操作失败，但也不许静默：表写不进去时发一条 error 日志，并让调用方拿到
//! `Err`——调用方把它记进指标。写已经发生了（编排侧的状态已经改了），这时候回 500 会让
//! 客户端重试一次、把改动再做一遍；而不出声地丢掉这一行，就是「改过但查不到」的那种洞，
//! 只有下一次审计才会发现。

use async_trait::async_trait;
use sqlx::PgPool;

pub use cog_core::contract::project_audit::{AuditEntry, AuditRow, ProjectAuditLog};

/// `users.id` 是 UUID 列；把 `system` 这类非 UUID 身份绑上去会让整条语句失败，
/// 而失败的是「查人」那一半，不该连「做过什么」一起丢。
fn is_uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

/// 这张表需要的额外索引。
///
/// 表自己的三条索引是给「某个用户做过什么」「某类动作发生过什么」用的；项目面问的是
/// 「这个项目的改动」，而那要看 `details->>'project_id'`。表达式索引是这唯一一种查询
/// 形状的代价；没有它，每打开一个项目的时间线就扫一遍整张审计表。
pub const DDL: &str = r#"
    CREATE INDEX IF NOT EXISTS idx_audit_logs_project
        ON audit_logs ((details->>'project_id'));
"#;

/// Postgres 实现。
pub struct PgProjectAuditLog {
    pool: PgPool,
}

impl PgProjectAuditLog {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 补上这一面需要的索引。幂等，启动时调一次。
    pub async fn init_schema(&self) -> cog_core::SFResult<()> {
        crate::schema_ddl::apply(&self.pool, DDL).await
    }
}

#[async_trait]
impl ProjectAuditLog for PgProjectAuditLog {
    async fn record(&self, entry: AuditEntry) -> cog_core::SFResult<()> {
        // 用户 id 走子查询而不是先查一遍：actor 是 `system`、或者是一个还没落进
        // `users` 的身份（引导期、测试令牌）时，子查询给出 NULL，外键不会拒绝这一行。
        // 「这个人查不到」不该让「他做过什么」整条丢失。
        let user_id = if is_uuid(&entry.actor) {
            Some(entry.actor.as_str())
        } else {
            None
        };
        let details = serde_json::json!({
            "actor": entry.actor,
            "project_id": entry.project_id,
            "before": entry.before,
            "after": entry.after,
        });
        sqlx::query(
            "INSERT INTO audit_logs (user_id, action, resource_type, resource_id, details) \
             VALUES ((SELECT id FROM users WHERE id = $1::uuid), $2, $3, $4, $5)",
        )
        .bind(user_id)
        .bind(&entry.action)
        .bind(&entry.resource_type)
        .bind(&entry.resource_id)
        .bind(&details)
        .execute(&self.pool)
        .await
        .map_err(|e| cog_core::SFError::Database(format!("audit_logs insert failed: {e}")))?;
        Ok(())
    }

    async fn recent(&self, project_id: &str, limit: i64) -> cog_core::SFResult<Vec<AuditRow>> {
        let rows: Vec<(i64, Option<String>, String, String, String, serde_json::Value, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as(
                "SELECT id, details->>'actor', action, resource_type, resource_id, details, created_at \
                   FROM audit_logs \
                  WHERE details->>'project_id' = $1 \
                  ORDER BY created_at DESC, id DESC \
                  LIMIT $2",
            )
            .bind(project_id)
            .bind(limit.clamp(1, 500))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| cog_core::SFError::Database(format!("audit_logs read failed: {e}")))?;
        Ok(rows
            .into_iter()
            .map(
                |(id, actor, action, resource_type, resource_id, details, created_at)| AuditRow {
                    id,
                    actor: actor.unwrap_or_else(|| "unknown".to_string()),
                    action,
                    resource_type,
                    resource_id,
                    before: details
                        .get("before")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    after: details
                        .get("after")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    created_at,
                },
            )
            .collect())
    }
}

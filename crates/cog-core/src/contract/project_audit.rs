//! 项目面写操作的审计：一行记录一次改动，读者按项目一次扫出来。
//!
//! 这张表从迁移建起来就什么都有（DDL、按月分区、三条索引、分区维护者），但曾经
//! 全平台没有一个写者。于是「谁在什么时候把哪张卡挪到了哪一列」这件事，卖出去之后
//! 只在文件版的哈希链里，而那条链是给「有没有被篡改」用的：它能证明历史没被动过，
//! 不能回答「这张任务改过几次、每次改成什么」。客户的安全评审问的是后者。
//!
//! 两份都写是有意的，不是重复：链**按顺序**追加、每条带前一条的哈希，任何一处被
//! 改写后面全对不上；表**按资源**建索引，「某一张任务的改动」是一次索引扫描。前者
//! 查完整性，后者查人。一份既当链又当索引的数据结构两头都做不好。
//!
//! 落库失败不让写操作失败，但也不许静默：表写不进去时发一条 error 日志，并让调用方
//! 拿到 `Err`——调用方把它记进指标。写已经发生了（编排侧的状态已经改了），这时候回
//! 500 会让客户端重试一次、把改动再做一遍；而不出声地丢掉这一行，就是「改过但查不
//! 到」的那种洞，只有下一次审计才会发现。
//!
//! 契约在这里、SQL 在存储 crate：写它的是网关，落库的是 `PgPool`，两者不该为了这一
//! 张表互相认识。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// 一次写操作留下的那一行。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// 谁。`sub`（用户 UUID）或 `system`；不是已知用户时按 NULL 落库，名字仍进 details。
    pub actor: String,
    /// 做了什么，例如 `project.task_updated`。
    pub action: String,
    /// 改的是哪类东西：`task`、`project`、`plan`、`card`。
    pub resource_type: String,
    pub resource_id: String,
    /// 这次改动属于哪个项目——项目时间线按它一次扫出来。
    pub project_id: String,
    /// 改前与改后。新建时 `before` 是 null；只读字段没变也要写进来，否则「改了什么」
    /// 与「现在是什么」分不开。
    pub before: serde_json::Value,
    pub after: serde_json::Value,
}

/// 从表里读回来的一行。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRow {
    pub id: i64,
    pub actor: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// 写者与读者的契约。
#[async_trait]
pub trait ProjectAuditLog: Send + Sync {
    async fn record(&self, entry: AuditEntry) -> SFResult<()>;
    async fn recent(&self, project_id: &str, limit: i64) -> SFResult<Vec<AuditRow>>;
}

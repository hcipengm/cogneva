//! 任务账本的四张表，在用例里怎么摆平。
//!
//! 一件任务是四张表里的行：定义（每改一次多一版）、状态迁移（只追加）、尝试
//! （每次执行一行）、投影（只放还动得了的任务）。所以「清掉一件任务」不是一条
//! `DELETE`，而四条，而且顺序有讲究：先清引用别人的，再清被引用的。
//!
//! 这些动作以前在每个用例里各写一遍，各自对着那张 jsonb 文档表写。抄件之间没有
//! 东西比较：一边漏了一张表，那边只是「看起来清干净了」，下一条用例读到的行里
//! 就多出一件上一条留下的任务。

use sqlx::PgPool;

/// 四张表，按「先清引用方、再清被引用方」的顺序。
const TABLES: [&str; 4] = [
    "task_attempt",
    "task_transition",
    "task_defined",
    "task_current",
];

/// 清掉整个账本。
///
/// 留给那几条要读「全库」读数的用例：状态清点、聚合看板这类，它们数的就是表里的
/// 每一行，留着邻居的行会让期望值随别人跑没跑过而变。
pub async fn clear_task_ledger(pool: &PgPool) -> Result<(), sqlx::Error> {
    for table in TABLES {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// 清掉一件任务在四张表里的全部行。
///
/// 用例里常有「这一行是我刚刚种下的」这种精确收回：清理必须**一件一件**收，而
/// 不是按前缀扫一遍——并行跑的邻居可能正种着同一个前缀下的另一件。
pub async fn purge_task(pool: &PgPool, task_id: &str) -> Result<(), sqlx::Error> {
    for table in ["task_attempt", "task_transition"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE task_id = $1"))
            .bind(task_id)
            .execute(pool)
            .await?;
    }
    sqlx::query("DELETE FROM task_current WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM task_defined WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// 清掉一个工作区的行。
///
/// 绝大多数用例只需要这一条：它们自己写、自己读，关心的行都在自己的 workspace
/// 里，清别人的行只是为了让期望值好看，而那正是共享库上最不该做的事——并行跑的
/// 另一条用例正读着它自己的行。
///
/// 按 `task_id` 反查而不是按 `workspace_id` 列：尝试表和迁移表没有工作区列，
/// 一件任务的工作区在它的定义上。
pub async fn clear_task_ledger_for(pool: &PgPool, workspace: &str) -> Result<(), sqlx::Error> {
    for (table, scoped) in [
        ("task_attempt", false),
        ("task_transition", false),
        ("task_defined", true),
    ] {
        let sql = if scoped {
            format!("DELETE FROM {table} WHERE workspace_id = $1")
        } else {
            format!(
                "DELETE FROM {table} WHERE task_id IN \
                 (SELECT task_id FROM task_defined WHERE workspace_id = $1)"
            )
        };
        sqlx::query(&sql).bind(workspace).execute(pool).await?;
    }
    sqlx::query("DELETE FROM task_current WHERE workspace_id = $1")
        .bind(workspace)
        .execute(pool)
        .await?;
    Ok(())
}

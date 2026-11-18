//! `cogneva export-change-diff <artifact-id>`：把一条变更的 diff 从队列里导出。
//!
//! 盘上的 `<id>.diff` 只是**导出工件**，正本是事件流里的候选事件。所以导出一条变更
//! 要有两条性质：字节逐字来自那条事件，且导出物自己说得出它是从哪一条事件导的
//! （头一行 `# event_id: <ULID>`）。没有水位，一份 `.diff` 是谁、什么时候导的，
//! 事后只能猜。
//!
//! 环境变量：`COGNEVA_DATABASE_URL`（队列落在哪个库）。退出码就是判词：
//! 0 导出到 stdout；1 找不到这条变更或导出不了，原因写 stderr。

use std::sync::Arc;

/// 读环境、建连接、导出、印到 stdout。
pub async fn run_from_args(argv: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let artifact_id = cogneva_arg(argv)
        .ok_or_else(|| "usage: cogneva export-change-diff <artifact-id>".to_string())?;
    let database = std::env::var(cog_core::env::DATABASE_URL).map_err(|_| {
        "COGNEVA_DATABASE_URL 没有设置：不知道队列落在哪个库，这一跑没有去处".to_string()
    })?;

    let pool = sqlx::PgPool::connect(&database).await?;
    let writer = format!(
        "{}@{}",
        cog_core::env::process_identity(|name| std::env::var(name).ok()),
        env!("CARGO_PKG_VERSION")
    );
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let mut seed = [0u8; 10];
    seed.copy_from_slice(&uuid::Uuid::new_v4().into_bytes()[..10]);
    let store: Arc<dyn cog_core::ChangeCandidateStore> = Arc::new(
        cog_storage::postgres::change_candidate_store::PgChangeCandidateStore::new(
            pool,
            "agent:reflection",
            writer,
            "",
            cog_core::EventIdFactory::seeded(now_ms, seed),
        ),
    );

    let candidate = store
        .read(&artifact_id)
        .await
        .map_err(|e| format!("读队列失败：{e}"))?
        .ok_or_else(|| format!("队列里没有 id 为 {artifact_id} 的变更"))?;
    print!(
        "{}",
        cog_reflection::change_pipeline::export_diff(&candidate)
    );
    Ok(())
}

/// 这个子命令的第一个词：命令行里子命令名之后的那个不以 `-` 开头的参数。
fn cogneva_arg(argv: &[String]) -> Option<String> {
    crate::eval_table::operands(argv)
        .into_iter()
        .find(|word| !word.starts_with('-'))
}

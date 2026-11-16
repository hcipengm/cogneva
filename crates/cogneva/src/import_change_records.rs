//! `cogneva import-change-records`：把历史遗留的 `<id>.json` 记录导进队列，然后删掉文件。
//!
//! 这是 S25 顺序里唯一还需要盘上那份字节的一步。旧模型把它当唯一的那份事实，新模型
//! 只认事件；导入之后那份文件就没有读者了，留着只会让下一个人以为它还是正本。
//!
//! 环境变量：
//!
//! * `COGNEVA_DATABASE_URL`：队列落在哪个库；
//! * `COGNEVA_EVOLUTION_CHANGE_DIR`：变更目录，缺省 `./evolution-changes`（与
//!   `self_evolution.change_dir` 的缺省一致）。
//!
//! 退出码就是判词：0 表示每一条读得懂的都进了队、文件已删；非零带一行原因，
//! 读不懂的那几条逐条点名——它们留在原地，因为删掉就是删掉唯一那份它们说过的东西。

use std::sync::Arc;

/// 读环境、建连接、导入、印一行。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var(cog_core::env::DATABASE_URL).map_err(|_| {
        "COGNEVA_DATABASE_URL 没有设置：不知道队列落在哪个库，这一跑没有去处".to_string()
    })?;
    let change_dir = std::env::var("COGNEVA_EVOLUTION_CHANGE_DIR")
        .unwrap_or_else(|_| "./evolution-changes".to_string());

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

    let report = cog_reflection::change_pipeline::import_legacy_records(
        std::path::Path::new(&change_dir),
        &store,
    )
    .await;

    println!(
        "imported: pending={} retired={} unreadable={}",
        report.pending,
        report.retired,
        report.unreadable.len()
    );
    for path in &report.unreadable {
        eprintln!("unreadable, left in place: {}", path.display());
    }
    if !report.unreadable.is_empty() {
        return Err(format!(
            "{} record files could not be imported and were left where they are",
            report.unreadable.len()
        )
        .into());
    }
    Ok(())
}

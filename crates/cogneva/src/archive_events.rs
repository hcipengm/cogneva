//! `cogneva archive-events <older-than-days> [batch] [prefix]`：把可观测事件里超出
//! 保留期的那一段搬进对象存储，读回逐字节核对，核对通过才删库里的行。
//!
//! 这是「搬走再删」的唯一生产入口。删除本身不是目的：容量压力先归档；归档读不回来
//! 就不删，这一批下次再来。

use std::sync::Arc;

use cog_core::ObjectBackend;

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "archive-events")
        .skip(1)
        .collect();
    let days: i64 = args
        .first()
        .ok_or("usage: cogneva archive-events <older-than-days> [batch] [prefix]")?
        .parse()
        .map_err(|_| "older-than-days must be a whole number of days")?;
    if days <= 0 {
        return Err("older-than-days must be positive: a zero would archive every row".into());
    }
    let batch: i64 = args
        .get(1)
        .map(|value| value.parse())
        .transpose()
        .map_err(|_| "batch must be a whole number of rows")?
        .unwrap_or(5_000);
    let prefix = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "audit/observability-events/".to_string());

    let config = crate::config_loader::load();
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the events")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let backend =
        cog_storage::build_object_backend(&config.providers.storage, &config.app.data_dir, None)?;
    archive_all(&pool, &backend, days, batch, &prefix).await?;
    Ok(())
}

/// 一批一批搬，直到没有更老的行。
pub async fn archive_all(
    pool: &sqlx::PgPool,
    backend: &Arc<dyn ObjectBackend>,
    days: i64,
    batch: i64,
    prefix: &str,
) -> Result<(usize, u64), Box<dyn std::error::Error>> {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
    let mut archived = 0usize;
    let mut deleted = 0u64;
    loop {
        let Some(outcome) = cog_storage::observability_event_cap::archive_batch(
            pool,
            "cog_observability_events",
            backend,
            prefix,
            cutoff,
            batch,
        )
        .await?
        else {
            break;
        };
        archived += outcome.archived;
        deleted += outcome.deleted;
        println!(
            "archive-events: {} row(s) -> {} ({} bytes checked), {} deleted",
            outcome.archived, outcome.key, outcome.digest, outcome.deleted
        );
    }
    println!("archive-events: archived {archived}, deleted {deleted} (older than {days} day(s))");
    Ok((archived, deleted))
}

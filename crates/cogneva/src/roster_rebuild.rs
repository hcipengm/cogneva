//! `cogneva roster-rebuild`：照着员工事实把名册投影重写一遍。
//!
//! 名册是投影（`RedisAgentRegistry::rebuild_roster` 从员工事件折，带快照与水位），
//! 这条命令是它的重建入口：投影要有重建命令，否则「删了能不能回来」只能读代码。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let redis_url = std::env::var(cog_core::env::REDIS_URL)
        .map_err(|_| "set COGNEVA_REDIS_URL to the registry's Redis")?;
    let pool = sqlx::PgPool::connect(&database).await?;
    let client = redis::Client::open(redis_url)?;
    let connection = cog_storage::redis::connect(&client).await?;
    let registry = cog_storage::RedisAgentRegistry::new(connection);
    let rows = registry.rebuild_roster(&pool).await?;
    println!("roster-rebuild: {rows} row(s) rebuilt from the employee facts");
    Ok(())
}

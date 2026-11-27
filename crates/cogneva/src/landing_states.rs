//! `cogneva landing-states`：每条变更此刻的落地状态，从流上读。
//!
//! 落地账的文件是缓存；这一条读法证明它可以从流上折回来——文件丢了、机器换了，
//! 「这条变更落到哪、CI 怎么看、为什么退」仍然答得出来。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let states = cog_storage::landing_states(&pool).await?;
    println!("changes\t{}", states.len());
    for state in &states {
        println!(
            "landing\t{}\t{}\t{}\t{}\t{}",
            state.change_id,
            state.base,
            state.landed_rev,
            state.state,
            state
                .retired_reason
                .clone()
                .unwrap_or_else(|| "none".to_string())
        );
    }
    Ok(())
}

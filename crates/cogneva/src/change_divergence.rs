//! `cogneva change-divergence`：评估门的判词与最终结局的分歧，按方向分开数。
//!
//! 判词说收而门拒、判词说拒而门收，是两种相反的病；合成一个数就看不出该修哪边。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let reading = cog_storage::candidate_divergence(&pool).await?;
    println!("total\t{}", reading.total());
    println!("adopt_accepted\t{}", reading.adopt_accepted);
    println!("adopt_rejected\t{}", reading.adopt_rejected);
    println!("reject_accepted\t{}", reading.reject_accepted);
    println!("reject_rejected\t{}", reading.reject_rejected);
    println!("inconclusive_accepted\t{}", reading.inconclusive_accepted);
    println!("inconclusive_rejected\t{}", reading.inconclusive_rejected);
    println!("unjudged\t{}", reading.unjudged);
    println!("divergent\t{}", reading.divergent());
    Ok(())
}

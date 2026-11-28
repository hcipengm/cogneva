//! `cogneva cache-misses [hours]`：窗口里每一次没命中，按**为什么**没命中分类计数。
//!
//! 命中率低是一个数，而两种低法要做的活完全不同：前缀接不上（工具定义变了、system
//! 里注入了每轮都变的东西、历史被改写）与接得上却没有命中（上一轮的字节这次没被
//! 复用）。合成一个数，谁也看不出该修哪一层。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let hours: i64 = std::env::args()
        .skip_while(|arg| arg != "cache-misses")
        .nth(1)
        .map(|raw| raw.parse())
        .transpose()
        .map_err(|_| "hours must be a whole number")?
        .unwrap_or(24);
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let until = chrono::Utc::now();
    let since = until - chrono::Duration::hours(hours);
    let classes = cog_storage::cache_miss_breaks(&pool, since, until).await?;
    let total: i64 = classes.iter().map(|class| class.misses).sum();
    println!("misses\t{total}");
    for class in &classes {
        println!("miss_class\t{}\t{}", class.class, class.misses);
    }
    Ok(())
}

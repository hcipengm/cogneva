//! `cogneva change-ledger-parity`：变更账与流逐格比对。
//!
//! 空输出就是那条断言的全部证据——「表是流的一份投影」。有分歧时逐格印：哪条身份、
//! 哪一格、表里是什么、流上是什么；`none` 是那一侧没有这一格（与「说的是空」分开）。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the stream")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    // `--project` 先把水位之后的事实折进表，再比对：增量重折完立刻看分歧，是同一个
    // 动作的两半——先重折、再问「还差什么」。
    if std::env::args().any(|arg| arg == "--project") {
        let written = cog_storage::project_change_ledger_incremental(&pool).await?;
        println!("projected_rows\t{written}");
    }
    let divergences = cog_storage::change_ledger_divergence(&pool).await?;
    println!("divergences\t{}", divergences.len());
    for divergence in &divergences {
        let cell = |value: &Option<String>| value.clone().unwrap_or_else(|| "none".to_string());
        println!(
            "divergence\t{}\t{}\t{}\t{}",
            divergence.identity,
            divergence.column,
            cell(&divergence.table),
            cell(&divergence.stream)
        );
    }
    Ok(())
}

//! `cogneva db-pressure`：库自己那几格读数。
//!
//! 只给数，不给线：换不换存储、清不清表的判据要由这些数里的一条（或几条）导出，
//! 人拍的阈值不在这里。体积、事务、临时文件、死锁四行都是累计量；两次输出相减就是
//! 这一段的速率。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database to read")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let (size,): (i64,) = sqlx::query_as("SELECT pg_database_size(current_database())::BIGINT")
        .fetch_one(&pool)
        .await?;
    let row: (i64, i64, i64, i64, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT xact_commit, xact_rollback, temp_bytes, deadlocks, stats_reset \
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await?;
    println!("bytes\t{size}");
    println!("xact_commit\t{}", row.0);
    println!("xact_rollback\t{}", row.1);
    println!("temp_bytes\t{}", row.2);
    println!("deadlocks\t{}", row.3);
    // 速率从库自己的 `stats_reset` 导出：经过了多久、这段时间累计了多少事务与临时
    // 字节，相除即这段的平均速率。窗口不是人拍的——它是库上一次重置统计的时刻。
    if let Some(reset) = row.4 {
        let elapsed = (chrono::Utc::now() - reset).num_seconds().max(1) as f64;
        println!("stats_elapsed_secs\t{elapsed:.0}");
        println!("xact_per_sec\t{:.6}", (row.0 + row.1) as f64 / elapsed);
        println!("temp_bytes_per_sec\t{:.3}", row.2 as f64 / elapsed);
    } else {
        println!("rates\tunavailable (statistics were never reset)");
    }
    // 在飞的事务：库自己会掐掉的那几条时间界。它们此刻多老，就是「还差多久被掐」
    // 的直接读数——没有这一格，一次被库掐掉的连接与一次自己断掉的连接在读数上同形。
    // 查询与指标发布在同一处（`cog_storage::db_surface`）：读数只有一份定义。
    for (state, age) in cog_storage::db_surface::inflight_ages(&pool).await? {
        let key = if state == cog_storage::db_surface::XACT_STATE_ACTIVE {
            "xact_active_max_age_secs"
        } else {
            "xact_idle_max_age_secs"
        };
        match age {
            Some(secs) => println!("{key}\t{secs:.3}"),
            None => println!("{key}\tnone (no transaction in this state)"),
        }
    }

    // autovacuum 落后多少：每张表自己的阈值由库的两条设置与本表行数算出——
    // `autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × 活行数`。
    // 阈值不是人拍的数：换一个库、换一张表，它跟着那张表的设置走。
    let vacuum = cog_storage::db_surface::vacuum_lag(&pool).await?;
    let mut lagging = 0usize;
    let mut worst: Option<(String, f64)> = None;
    for (table, dead, threshold) in &vacuum {
        if *dead <= 0 {
            continue;
        }
        let ratio = if *threshold > 0.0 {
            *dead as f64 / *threshold
        } else {
            // 阈值为 0 但已有死行：落后程度无界，按「已经越过」记，不编一个比值。
            f64::INFINITY
        };
        if ratio >= 1.0 {
            lagging += 1;
        }
        if worst.as_ref().map(|(_, w)| ratio > *w).unwrap_or(true) {
            worst = Some((table.clone(), ratio));
        }
        println!("vacuum_dead\t{table}\t{dead}\t{threshold}");
    }
    println!("vacuum_lagging_tables\t{lagging}");
    match worst {
        Some((table, ratio)) if ratio.is_finite() => {
            println!("vacuum_lag_max_ratio\t{ratio:.3}\t{table}")
        }
        Some((table, _)) => println!("vacuum_lag_max_ratio\tunbounded\t{table}"),
        None => println!("vacuum_lag_max_ratio\tnone (no dead tuples)"),
    }

    // 每张事实表最老一行多老：删/搬之前要先知道手里这份历史有多长。表名来自普查
    // （不来自参数），时间列在库的目录里找——没有时间列的表明说「没有」，不印 0。
    for table in cog_storage::table_shapes::FACT_TABLES {
        let column: Option<String> = sqlx::query_scalar(
            "SELECT column_name FROM information_schema.columns \
              WHERE table_name = $1 AND data_type = 'timestamp with time zone' \
                AND column_name IN ('at', 'ts', 'created_at', 'extracted_at') \
              ORDER BY array_position(\
                  ARRAY['at', 'ts', 'created_at', 'extracted_at'], column_name) \
              LIMIT 1",
        )
        .bind(*table)
        .fetch_optional(&pool)
        .await?
        .flatten();
        let Some(column) = column else {
            println!("oldest_row_secs\t{table}\tnone (no time column)");
            continue;
        };
        let age: Option<f64> = sqlx::query_scalar(&format!(
            "SELECT EXTRACT(EPOCH FROM now() - min(\"{column}\"))::float8 FROM \"{table}\""
        ))
        .fetch_one(&pool)
        .await?;
        match age {
            Some(secs) => println!("oldest_row_secs\t{table}\t{secs:.0}"),
            // 空表：没有「最老的一行」，与「刚写进来一行」不是同一件事。
            None => println!("oldest_row_secs\t{table}\tnone (the table is empty)"),
        }
    }

    // 查询延迟：只有装了 pg_stat_statements 才读得到。没有这一格时说「没装」，
    // 不答 0——0 毫秒是一次真实读数。
    let installed: (Option<String>,) =
        sqlx::query_as("SELECT to_regclass('pg_stat_statements')::text")
            .fetch_one(&pool)
            .await?;
    if installed.0.is_some() {
        let slow: Option<(f64, f64, i64, String)> = sqlx::query_as(
            "SELECT mean_exec_time, max_exec_time, calls, left(query, 120) \
               FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 1",
        )
        .fetch_optional(&pool)
        .await?;
        if let Some((mean, max, calls, query)) = slow {
            println!("slow_query_mean_ms\t{mean:.3}");
            println!("slow_query_max_ms\t{max:.3}");
            println!("slow_query_calls\t{calls}");
            println!("slow_query\t{query}");
        }
    } else {
        println!("slow_query\tunavailable (pg_stat_statements not installed)");
    }
    Ok(())
}

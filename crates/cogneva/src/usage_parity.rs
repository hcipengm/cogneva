//! `cogneva usage-parity [hours]`：同一段窗口里，用量行与事件流各读出什么。
//!
//! 停写行之前要盯的那一格：两侧的条数、token、钱逐项相等，才能把行降成投影。
//! 任何一项不等就把差额印出来，不做「差不多」的判断。

/// `delta` 的三项是不是都为零：停写行的闸门只看这一句，不做「差不多」。
fn parity_is_zero(delta: (i64, i64, i64)) -> bool {
    delta == (0, 0, 0)
}

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    // `--require-zero`：把这一格当闸门用（停写行之前那一段要能说「零」而不只是印「零」）。
    // 不带它时行为不变：印出差额、退 0。
    let mut require_zero = false;
    let mut hours_arg: Option<String> = None;
    let mut after = false;
    for arg in std::env::args().skip(1) {
        if !after {
            after = arg == "usage-parity";
            continue;
        }
        if arg == "--require-zero" {
            require_zero = true;
            continue;
        }
        if hours_arg.is_none() {
            hours_arg = Some(arg);
        }
    }
    let hours: i64 = hours_arg
        .map(|raw| raw.parse())
        .transpose()
        .map_err(|_| "hours must be a whole number")?
        .unwrap_or(24);
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding both sides")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let (rows_count, rows_tokens, rows_cost): (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT, \
                COALESCE(SUM(tokens_input + tokens_output), 0)::BIGINT, \
                COALESCE(SUM(cost_micros), 0)::BIGINT \
           FROM gateway_llm_usage WHERE ts >= NOW() - make_interval(hours => $1)",
    )
    .bind(hours as i32)
    .fetch_one(&pool)
    .await?;
    // 事件流那侧的载荷在 `attributes` 里，不是表上的列：`llm_call` 的事实一直只有
    // 那一格（`LlmCall` 序列化后的键）。按列查会报 `column "input_tokens" does not
    // exist`——2026-11-27 在真库上实测到的形状，工具因此一次也没跑成。
    let (stream_count, stream_tokens, stream_cost): (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT, \
                COALESCE(SUM(COALESCE((attributes::jsonb->>'input_tokens')::bigint, 0) \
                           + COALESCE((attributes::jsonb->>'output_tokens')::bigint, 0)), 0)::BIGINT, \
                COALESCE(SUM(COALESCE((attributes::jsonb->>'cost_micros')::bigint, 0)), 0)::BIGINT \
           FROM cogneva_event \
          WHERE kind = 'llm_call' AND at >= NOW() - make_interval(hours => $1)",
    )
    .bind(hours as i32)
    .fetch_one(&pool)
    .await?;
    println!("hours\t{hours}");
    println!("rows\tcalls={rows_count} tokens={rows_tokens} cost={rows_cost}");
    println!("stream\tcalls={stream_count} tokens={stream_tokens} cost={stream_cost}");
    let delta = (
        stream_count - rows_count,
        stream_tokens - rows_tokens,
        stream_cost - rows_cost,
    );
    println!(
        "delta\tcalls={} tokens={} cost={}",
        delta.0, delta.1, delta.2
    );
    if require_zero && !parity_is_zero(delta) {
        std::process::exit(2);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parity_is_zero;

    /// 闸门只认全零：任何一项不同都算没对上，不取「接近」。
    #[test]
    fn only_an_all_zero_delta_passes_the_gate() {
        assert!(parity_is_zero((0, 0, 0)));
        assert!(!parity_is_zero((1, 0, 0)));
        assert!(!parity_is_zero((0, -1, 0)));
        assert!(!parity_is_zero((0, 0, 5)));
    }
}

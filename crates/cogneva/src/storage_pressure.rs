//! `cogneva storage-pressure`：把「存储还跟不跟得上」的四条读数装成一次判词，印出来。
//!
//! 判词本身在 `cog_storage::append_pressure`（四条触发条件、读不到单列成「量不到」）。
//! 这个入口只做一件事：**问它**。此前判词存在而没有调用点，所以「该换存储了」在
//! 平台上不会被提出——一条永远没人问的判据与没有这条判据，在读数上是同一件事。
//!
//! 四条读数各从它自己的来源来：
//!
//! 1. **写入延迟**：本进程刚接上的 `cogneva_event_append_seconds` 直方图，取最近
//!    一段的样本算 p99；与 `UI_REFRESH_MS` 的一半比。
//! 2. **流滞后**：要一个流后端句柄（`MessageBackend`）。这个只读命令没有它，
//!    照实记「量不到」并说清缺什么——不拿 0 冒充没涨。
//! 3. **vacuum 落后**：库自己的 `pg_stat_user_tables`，按每张表自己的触发线判。
//! 4. **体积增长**：宿主 `host_disk_trend` 生产者发布的「还有几天写满」减去提前期，
//!    与平台承诺的保留天数比。生产者不在本仓，读它要 `COGNEVA_PROMETHEUS_URL`。
//!
//! 退出码就是判词，三态分开（「量不到」不许与「没事」同码）：
//!
//! * `0`：四条都读到了，且没有一条触发；
//! * `2`：至少一条触发——这就是「该换存储了」被提出的那一格；
//! * `3`：没有触发，但有读不到的——判了一半，不是没事；
//! * `1`：连库都连不上，这一跑没有判词。

use std::time::Duration;

use chrono::Utc;

use cog_core::{HttpClient as _, MetricsBackend as _};
use cog_storage::append_pressure::{judge, AppendReadings, Verdict};

/// p99 从最近多长一段样本里算。
///
/// 与 Prometheus 的 `rate()`/lookback 默认窗口一致（五分钟）：判词比较的是「此刻
/// 的尾巴」，不是进程启动以来所有写过的平均。
// cap: fixed — Prometheus 的默认 rate/lookback 窗口是 5 分钟；读 p99 用同一段
// keep: 删掉后 p99 会落到一个没有出处的窗口上；由这条查询约定兜住
const P99_WINDOW: Duration = Duration::from_secs(300);

/// 判词的处置词：`keep`（继续用）或 `front`（前移写入口）。闭集之外不猜。
fn decision_word(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "keep" => Some("keep"),
        "front" => Some("front"),
        _ => None,
    }
}

/// `--decide <词> --reason <一句话> [--actor <谁>]` 的参数，缺哪个都说不清这次处置。
struct DecisionArgs {
    decision: &'static str,
    reason: String,
    actor: String,
}

fn parse_decision_args(args: &[String]) -> Result<Option<DecisionArgs>, String> {
    let mut decision = None;
    let mut reason = None;
    let mut actor = None;
    let mut at = 0usize;
    while at < args.len() {
        match args[at].as_str() {
            "--decide" => {
                at += 1;
                let raw = args.get(at).ok_or("--decide 后面要跟 keep 或 front")?;
                decision = Some(
                    decision_word(raw)
                        .ok_or_else(|| format!("--decide 只认 keep 或 front：{raw}"))?,
                );
            }
            "--reason" => {
                at += 1;
                reason = Some(
                    args.get(at)
                        .cloned()
                        .ok_or("--reason 后面要跟一句话：为什么这么定")?,
                );
            }
            "--actor" => {
                at += 1;
                actor = Some(
                    args.get(at)
                        .cloned()
                        .ok_or("--actor 后面要跟做决定的人/进程")?,
                );
            }
            _ => {}
        }
        at += 1;
    }
    let Some(decision) = decision else {
        return Ok(None);
    };
    let reason = reason
        .filter(|reason| !reason.trim().is_empty())
        .ok_or("处置必须带 --reason：没有理由的决定读的人没法复核")?;
    Ok(Some(DecisionArgs {
        decision,
        reason,
        actor: actor.unwrap_or_else(|| "cogneva".into()),
    }))
}

/// 从样本里取 p99。空集是「量不到」，不是 0。
fn p99(samples: &[f64]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let index = ((sorted.len() as f64) * 0.99).ceil() as usize;
    sorted
        .get(index.saturating_sub(1).min(sorted.len() - 1))
        .copied()
}

/// 判词的退出码。三态：没事、该切、判了一半。
fn exit_code(verdict: &Verdict) -> i32 {
    if verdict.switch() {
        2
    } else if !verdict.unread.is_empty() {
        3
    } else {
        0
    }
}

/// 一条读数印成一行：读到了给数，读不到给原因。
fn line(name: &str, value: Option<f64>, reason: &str) -> String {
    match value {
        Some(value) => format!("{name}\t{value}"),
        None => format!("{name}\tunread ({reason})"),
    }
}

fn pressure_name(pressure: cog_storage::append_pressure::Pressure) -> &'static str {
    use cog_storage::append_pressure::Pressure;
    match pressure {
        Pressure::AppendLatency => "append_latency",
        Pressure::StreamLag => "stream_lag",
        Pressure::Vacuum => "vacuum",
        Pressure::Growth => "growth",
    }
}

pub async fn run_from_env() -> Result<i32, Box<dyn std::error::Error>> {
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database to read")?;
    let pool = sqlx::PgPool::connect(&url).await?;

    // 处置模式只落一条决定事实，不再算一遍判词：决定回答的是最近一次提出，
    // 它自己的理由已经由人说出来，不从这里补。
    let args: Vec<String> = std::env::args().collect();
    let decision = parse_decision_args(&args).map_err(|e| format!("storage-pressure: {e}"))?;
    if let Some(decision) = decision {
        use cog_core::EventStore as _;
        let store = cog_storage::postgres::event_log::EventLogStore::new(
            pool.clone(),
            "cogneva-storage-pressure",
        );
        let event = cog_core::FactEvent::new(cog_core::EventPayload::StoragePressureDecided {
            decision: decision.decision.to_string(),
            reason: decision.reason.clone(),
            actor: decision.actor.clone(),
            at_millis: Utc::now().timestamp_millis().max(0) as u64,
        });
        match store.append(event).await {
            Ok(()) => {
                println!("decision\t{}", decision.decision);
                println!("reason\t{}", decision.reason);
                println!("actor\t{}", decision.actor);
                println!("recorded\ttrue");
                return Ok(0);
            }
            Err(error) => {
                println!("decision\t{}", decision.decision);
                println!("recorded\tfalse ({error})");
                return Ok(1);
            }
        }
    }

    // 1. 写入延迟：直方图最近一段的 p99，毫秒。
    let metrics = cog_storage::PostgresMetricsBackend::new(pool.clone());
    let now = Utc::now();
    let samples = match metrics
        .query_histogram_range(
            cog_core::metric_names::EVENT_APPEND_SECONDS.as_str(),
            now - chrono::Duration::from_std(P99_WINDOW).unwrap_or_default(),
            now,
        )
        .await
    {
        Ok(samples) => samples
            .into_iter()
            .map(|sample| sample.value)
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    let append_p99_ms = p99(&samples).map(|secs| secs * 1000.0);
    let append_reason = "最近五分钟没有写入样本";

    // 3. vacuum：每张表越过自己的触发线。
    let vacuum_falling_behind = match cog_storage::db_surface::vacuum_lag(&pool).await {
        Ok(vacuum) => Some(
            vacuum
                .iter()
                .any(|(_, dead, threshold)| *dead as f64 > *threshold),
        ),
        Err(_) => None,
    };
    let vacuum_reason = "库的 pg_stat_user_tables 读不到";

    // 4. 体积增长：宿主生产者发布的「还有几天写满」减去提前期。
    let mut growth_reason = "没有 COGNEVA_PROMETHEUS_URL，读不到宿主磁盘趋势";
    let mut growth_headroom_days = None;
    if let Ok(base) = std::env::var(cog_core::env::PROMETHEUS_URL) {
        match prometheus_scalar(&base, "disk_trend_days_to_full_recent_margin").await {
            Some(margin) => {
                let lead = prometheus_scalar(&base, "disk_trend_full_lead_days")
                    .await
                    .unwrap_or(0.0);
                growth_headroom_days = Some(margin + lead);
            }
            None => {
                growth_reason =
                    "宿主磁盘趋势的生产者不在线（`disk_trend_days_to_full_recent_margin` 缺席）"
            }
        }
    }

    // 2. 流滞后：这个只读命令没有流后端句柄。
    let stream_reason = "这个进程没有流后端（`MessageBackend`），流滞后要由持有那条流的面读";

    let retention_target_days = cog_core::retention::llm_usage_keep_days() as f64;
    let readings = AppendReadings {
        append_p99_ms,
        ui_refresh_ms: Some(cog_core::contract::conversation::UI_REFRESH_MS as f64),
        stream_lag_seconds: None,
        stream_lag_rising: None,
        vacuum_falling_behind,
        growth_headroom_days,
        retention_target_days: Some(retention_target_days),
    };
    let verdict = judge(&readings);

    println!("{}", line("append_p99_ms", append_p99_ms, append_reason));
    println!("{}", line("ui_refresh_ms", readings.ui_refresh_ms, ""));
    println!("{}", line("stream_lag_seconds", None, stream_reason));
    println!(
        "{}",
        line(
            "vacuum_falling_behind",
            vacuum_falling_behind.map(|v| v as u8 as f64),
            vacuum_reason
        )
    );
    println!(
        "{}",
        line("growth_headroom_days", growth_headroom_days, growth_reason)
    );
    println!("retention_target_days\t{retention_target_days}");
    let triggered: Vec<&str> = verdict
        .triggered
        .iter()
        .copied()
        .map(pressure_name)
        .collect();
    let unread: Vec<&str> = verdict.unread.iter().copied().map(pressure_name).collect();
    println!("switch\t{}", verdict.switch());
    println!("triggered\t{}", triggered.join(","));
    println!("unread\t{}", unread.join(","));
    // 「提出」要落成一条事实：退出码只活在这次调用的终端里，而这条判词说的是
    // 「该换存储了」——它必须有人能回头读到，否则「提出过」与「没提出」同形。
    if verdict.switch() {
        use cog_core::EventStore as _;
        let readings_json = serde_json::json!({
            "append_p99_ms": append_p99_ms,
            "ui_refresh_ms": readings.ui_refresh_ms,
            "stream_lag_seconds": serde_json::Value::Null,
            "vacuum_falling_behind": vacuum_falling_behind,
            "growth_headroom_days": growth_headroom_days,
            "retention_target_days": retention_target_days,
        })
        .to_string();
        let store = cog_storage::postgres::event_log::EventLogStore::new(
            pool.clone(),
            "cogneva-storage-pressure",
        );
        let event = cog_core::FactEvent::new(cog_core::EventPayload::StoragePressureRaised {
            triggered: triggered.join(","),
            unread: unread.join(","),
            readings: readings_json,
            at_millis: Utc::now().timestamp_millis().max(0) as u64,
        });
        match store.append(event).await {
            Ok(()) => println!("recorded\ttrue"),
            Err(error) => println!("recorded\tfalse ({error})"),
        }
    }
    Ok(exit_code(&verdict))
}

/// 读 Prometheus 的一个即时标量；读不到或没有样本就是 `None`。
async fn prometheus_scalar(base: &str, name: &str) -> Option<f64> {
    let config = cog_net::HttpClientConfig::load().ok()?;
    let client = cog_net::ReqwestHttpClient::from_config(&config).ok()?;
    let base = base.trim_end_matches('/');
    let request = cog_core::HttpRequest::get(format!("{base}/api/v1/query?query={name}"));
    let response = client.execute(request).await.ok()?;
    let body: serde_json::Value = serde_json::from_slice(&response.body).ok()?;
    let value = body["data"]["result"][0]["value"][1].as_str()?;
    value.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_storage::append_pressure::Pressure;

    /// 处置词只认闭集，且没有理由的处置不算处置。
    #[test]
    fn a_decision_needs_one_of_the_two_words_and_a_reason() {
        assert_eq!(super::decision_word("keep"), Some("keep"));
        assert_eq!(super::decision_word(" FRONT "), Some("front"));
        assert_eq!(super::decision_word("maybe"), None);
        let ok = vec![
            "--decide".to_string(),
            "keep".to_string(),
            "--reason".to_string(),
            "the host still has years of headroom".to_string(),
            "--actor".to_string(),
            "owner".to_string(),
        ];
        let parsed = super::parse_decision_args(&ok).expect("a complete decision parses");
        let parsed = parsed.expect("a decision is present");
        assert_eq!(parsed.decision, "keep");
        assert_eq!(parsed.actor, "owner");
        let no_reason = vec!["--decide".to_string(), "front".to_string()];
        assert!(super::parse_decision_args(&no_reason).is_err());
        let unknown = vec![
            "--decide".to_string(),
            "maybe".to_string(),
            "--reason".to_string(),
            "because".to_string(),
        ];
        assert!(super::parse_decision_args(&unknown).is_err());
        assert!(
            super::parse_decision_args(&[])
                .expect("no decision is fine")
                .is_none(),
            "没写 --decide 时不该编出一个处置"
        );
    }

    #[test]
    fn the_percentile_is_the_tail_not_the_mean() {
        let samples: Vec<f64> = (1..=100).map(|n| n as f64).collect();
        assert_eq!(p99(&samples), Some(99.0), "p99 应当贴着尾巴");
        assert_eq!(p99(&[]), None, "空样本是量不到，不是 0");
    }

    /// 三种结局三个码：「没事」「该切」「判了一半」不许同码。
    #[test]
    fn the_exit_code_separates_quiet_switch_and_partial() {
        let quiet = Verdict {
            triggered: vec![],
            unread: vec![],
        };
        let cut = Verdict {
            triggered: vec![Pressure::AppendLatency],
            unread: vec![],
        };
        let partial = Verdict {
            triggered: vec![],
            unread: vec![Pressure::StreamLag],
        };
        assert_eq!(exit_code(&quiet), 0);
        assert_eq!(exit_code(&cut), 2);
        assert_eq!(exit_code(&partial), 3, "量不到不许与没事同码");
    }
}

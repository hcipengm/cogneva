//! 定期问一次「追加压力」判词，并把**变化**落成一条事实。
//!
//! 判词（[`crate::append_pressure`]）此前只在 `cogneva storage-pressure` 被手工调用时
//! 才存在：没有人问，就没有「该换存储了」这条提出。于是「四条读数都正常」「有一条
//! 越线了没人知道」「判词从没跑过」在读数面上是同一种空白。
//!
//! 这一支是那个「问的人」：每 [`crate::append_pressure::JUDGE_PERIOD`]（判据读的正是
//! 最近五分钟的追加样本，问得再勤也是同一批样本）读四格、判一次，触发时落一条
//! `StoragePressureRaised`。同一句话只落一次——只有触发的条件集合或量不到的条件集合
//! 变了才写（[`crate::append_pressure::verdict_changed`]）：提出是要人处置的，每拍写
//! 一条会把流灌满同一句话，而「提出过几次」也不再说得清。
//!
//! 它**不替人处置**：`keep` / `front` 由 `cogneva storage-pressure --decide` 落
//! `StoragePressureDecided`。两支的读面是 `scripts/host/cogneva-storage-pressure-reading.sh`。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::{PgPool, Row};
use tracing::{info, warn};

use cog_core::{HttpClient, HttpRequest, MetricsBackend, SFError, SFResult};

use crate::append_pressure::{judge, verdict_changed, AppendReadings, Verdict, JUDGE_PERIOD};

/// 日志与事实里的写者名。与手工入口用**同一个**：两条路落下的是同一族事实，读者
/// 不该因为是谁问的而看到两种来源。
const WRITER: &str = "cogneva-storage-pressure";

/// 只读地判「存储还跟不跟得上」，并把变化提出成事实。
pub struct StoragePressureWatch {
    pool: PgPool,
    metrics: Option<Arc<dyn MetricsBackend>>,
    http: Option<Arc<dyn HttpClient>>,
    prometheus_url: Option<String>,
}

impl StoragePressureWatch {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            metrics: None,
            http: None,
            prometheus_url: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_http(mut self, http: Arc<dyn HttpClient>) -> Self {
        self.http = Some(http);
        self
    }

    pub fn with_prometheus_url(mut self, url: Option<String>) -> Self {
        self.prometheus_url = url.filter(|u| !u.trim().is_empty());
        self
    }

    pub fn spawn(
        self: Arc<Self>,
        shutdown: cog_core::ShutdownSignal,
    ) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            "storage_pressure",
            cog_core::loop_health::Cadence::Periodic(JUDGE_PERIOD),
            shutdown.clone(),
            move |beat| {
                let watch = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move {
                    // 一拍一开始就盖章：这一格量的是「这一支还在按节奏问」，不是「问出了什么」。
                    beat.beat();
                    watch.judge_and_record().await;
                    tokio::select! {
                        _ = tokio::time::sleep(JUDGE_PERIOD) => {}
                        _ = shutdown.wait() => {}
                    }
                }
            },
        )
    }

    /// 读四格、判一次；触发且这句话变了才落一条事实。
    pub async fn judge_and_record(&self) -> Option<Verdict> {
        let readings = self.readings().await;
        let verdict = judge(&readings);
        if !verdict.switch() {
            return Some(verdict);
        }
        let last = match self.last_raised().await {
            Ok(last) => last,
            Err(e) => {
                // 读不到上一句时**照样提出**：漏一条提出比重复一条更坏，而重复会在读面
                // 上多一行同样的话，不会把「有人在等回答」抹掉。
                warn!(error = %e, "读不到上一条存储压力提出，按「变了」处理");
                None
            }
        };
        let last_pairs = last.as_ref().map(|(t, u)| (t.as_str(), u.as_str()));
        if !verdict_changed(last_pairs, &verdict) {
            return Some(verdict);
        }
        if let Err(e) = self.record_raised(&verdict, &readings).await {
            warn!(error = %e, "存储压力的提出没落进流；这一拍只在日志里");
        }
        Some(verdict)
    }

    async fn readings(&self) -> AppendReadings {
        let append_p99_ms = match &self.metrics {
            Some(metrics) => {
                let now = Utc::now();
                let start = now
                    - chrono::Duration::from_std(JUDGE_PERIOD)
                        .unwrap_or_else(|_| chrono::Duration::minutes(5));
                match metrics
                    .query_histogram_range(
                        cog_core::metric_names::EVENT_APPEND_SECONDS.as_str(),
                        start,
                        now,
                    )
                    .await
                {
                    Ok(samples) => p99(&samples.iter().map(|s| s.value).collect::<Vec<_>>())
                        .map(|secs| secs * 1000.0),
                    Err(_) => None,
                }
            }
            None => None,
        };
        let vacuum_falling_behind = match crate::db_surface::vacuum_lag(&self.pool).await {
            Ok(vacuum) => Some(
                vacuum
                    .iter()
                    .any(|(_, dead, threshold)| *dead as f64 > *threshold),
            ),
            Err(_) => None,
        };
        let growth_headroom_days = match (&self.http, &self.prometheus_url) {
            (Some(http), Some(base)) => {
                match prometheus_scalar(http, base, "disk_trend_days_to_full_recent_margin").await {
                    Some(margin) => {
                        let lead = prometheus_scalar(http, base, "disk_trend_full_lead_days")
                            .await
                            .unwrap_or(0.0);
                        Some(margin + lead)
                    }
                    None => None,
                }
            }
            _ => None,
        };
        AppendReadings {
            append_p99_ms,
            ui_refresh_ms: Some(cog_core::contract::conversation::UI_REFRESH_MS as f64),
            // 流滞后要持有那条流的面来读；这里没有那个句柄，按「量不到」记，不拿 0 冒充没涨。
            stream_lag_seconds: None,
            stream_lag_rising: None,
            vacuum_falling_behind,
            growth_headroom_days,
            retention_target_days: Some(cog_core::retention::llm_usage_keep_days() as f64),
        }
    }

    /// 流上最近一条 `raised` 的 (triggered, unread)。
    async fn last_raised(&self) -> SFResult<Option<(String, String)>> {
        let row = sqlx::query(
            "SELECT coalesce((attributes::jsonb)->>'triggered', '') AS triggered, \
                    coalesce((attributes::jsonb)->>'unread', '') AS unread \
               FROM cogneva_event \
              WHERE kind = 'storage_pressure' AND step = 'raised' \
              ORDER BY at DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        row.map(|row| {
            Ok((
                row.try_get::<String, _>("triggered")
                    .map_err(|e| SFError::Database(e.to_string()))?,
                row.try_get::<String, _>("unread")
                    .map_err(|e| SFError::Database(e.to_string()))?,
            ))
        })
        .transpose()
    }

    async fn record_raised(&self, verdict: &Verdict, readings: &AppendReadings) -> SFResult<()> {
        use cog_core::EventStore as _;
        let store = crate::postgres::event_log::EventLogStore::new(self.pool.clone(), WRITER);
        let triggered = verdict
            .triggered
            .iter()
            .copied()
            .map(crate::append_pressure::pressure_word)
            .collect::<Vec<_>>()
            .join(",");
        let unread = verdict
            .unread
            .iter()
            .copied()
            .map(crate::append_pressure::pressure_word)
            .collect::<Vec<_>>()
            .join(",");
        let readings_json = serde_json::json!({
            "append_p99_ms": readings.append_p99_ms,
            "ui_refresh_ms": readings.ui_refresh_ms,
            "stream_lag_seconds": readings.stream_lag_seconds,
            "vacuum_falling_behind": readings.vacuum_falling_behind,
            "growth_headroom_days": readings.growth_headroom_days,
            "retention_target_days": readings.retention_target_days,
        })
        .to_string();
        let event = cog_core::FactEvent::new(cog_core::EventPayload::StoragePressureRaised {
            triggered,
            unread,
            readings: readings_json,
            at_millis: Utc::now().timestamp_millis().max(0) as u64,
        });
        store
            .append(event)
            .await
            .map(|_| ())
            .map_err(|e| SFError::Database(e.to_string()))
    }
}

/// 从样本里取 p99；空集是「量不到」，不是 0。
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

/// 读 Prometheus 的一个即时标量；读不到或没有样本就是 `None`。
async fn prometheus_scalar(http: &Arc<dyn HttpClient>, base: &str, name: &str) -> Option<f64> {
    let base = base.trim_end_matches('/');
    let request = HttpRequest::get(format!("{base}/api/v1/query?query={name}"));
    let response = http.execute(request).await.ok()?;
    let body: serde_json::Value = serde_json::from_slice(&response.body).ok()?;
    body["data"]["result"][0]["value"][1]
        .as_str()?
        .parse::<f64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 空样本不许读成 0：0 毫秒是一次真实的追加。
    #[test]
    fn p99_of_nothing_is_not_zero() {
        assert_eq!(p99(&[]), None);
        assert_eq!(p99(&[0.01]), Some(0.01));
    }

    /// 判词周期是 `Duration` 常量，这里钉住它没有退回一个更密或更疏的数——
    /// 改它的人要先改 `append_pressure::JUDGE_PERIOD` 那条理由。
    #[test]
    fn the_watch_asks_on_the_judge_period() {
        assert_eq!(JUDGE_PERIOD, Duration::from_secs(300));
    }
}

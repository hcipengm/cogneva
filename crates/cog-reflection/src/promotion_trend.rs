//! 晋级周报（eval 长期趋势）。
//!
//! 周期任务按 ISO 周聚合晋级台账（近 8 周）：成功率、各级别分布、
//! 回滚量。报告写 `{data_dir}/reports/promotion-trend-latest.json`（机
//! 读）与 `.md`（人读）并保留周期归档；连续 3 周成功率下降且每周有
//! 足够完结对决样本时判定趋势向下——写审计告警（接管台 Audit Trail
//! 可见）并在报告 `alert` 字段留痕，提醒人介入。

use std::sync::Arc;

use chrono::{DateTime, Datelike, Utc};
use cog_core::{
    PromotionLedger, PromotionStatus, PromotionTrendReport, PromotionTrendWeek, SFResult,
};
use tracing::{error, info};

/// 参与聚合的周数。
const WINDOW_WEEKS: usize = 8;
/// 判定趋势向下的连续下降周数。
const DECLINE_RUN: usize = 3;
/// 单周完结对决样本下限（低于则该周不参与趋势判定，避免小样本抖动误报）。
const MIN_WEEK_SAMPLES: u64 = 2;

/// 从台账记录聚合周报。纯函数，便于测试。
pub fn aggregate(
    records: &[cog_core::PromotionRecord],
    now: DateTime<Utc>,
) -> PromotionTrendReport {
    let mut weeks: Vec<PromotionTrendWeek> = (0..WINDOW_WEEKS)
        .map(|back| {
            let t = now - chrono::Duration::weeks((WINDOW_WEEKS - 1 - back) as i64);
            let iso = t.iso_week();
            PromotionTrendWeek {
                week: format!("{}-W{:02}", iso.year(), iso.week()),
                promoted: 0,
                rolled_back: 0,
                failed: 0,
                awaiting_review: 0,
                success_rate: None,
            }
        })
        .collect();

    for rec in records {
        let iso = rec.created_at.iso_week();
        let label = format!("{}-W{:02}", iso.year(), iso.week());
        let Some(bucket) = weeks.iter_mut().find(|w| w.week == label) else {
            continue; // 窗口外
        };
        match rec.status {
            PromotionStatus::Promoted => bucket.promoted += 1,
            PromotionStatus::RolledBack => bucket.rolled_back += 1,
            PromotionStatus::Failed => bucket.failed += 1,
            PromotionStatus::AwaitingApproval => bucket.awaiting_review += 1,
            PromotionStatus::Pending => {}
        }
    }

    for w in &mut weeks {
        let decided = w.promoted + w.rolled_back + w.failed;
        if decided > 0 {
            w.success_rate = Some(w.promoted as f64 / decided as f64);
        }
    }

    // 趋势向下：取窗口尾部有样本的连续周，成功率严格递降达到 DECLINE_RUN。
    let sampled: Vec<&PromotionTrendWeek> = weeks
        .iter()
        .filter(|w| {
            w.success_rate.is_some() && (w.promoted + w.rolled_back + w.failed) >= MIN_WEEK_SAMPLES
        })
        .collect();
    let mut alert = None;
    if sampled.len() >= DECLINE_RUN {
        let tail = &sampled[sampled.len() - DECLINE_RUN..];
        let rates: Vec<f64> = tail.iter().map(|w| w.success_rate.unwrap()).collect();
        if rates.windows(2).all(|p| p[1] < p[0]) {
            let labels: Vec<&str> = tail.iter().map(|w| w.week.as_str()).collect();
            alert = Some(format!(
                "晋级成功率连续 {} 周下降（{}：{:.0}% → {:.0}%），趋势向下，建议人工介入",
                DECLINE_RUN,
                labels.join(" → "),
                rates[0] * 100.0,
                rates[rates.len() - 1] * 100.0
            ));
        }
    }

    PromotionTrendReport {
        generated_at: now,
        weeks,
        alert,
    }
}

/// 把报告写成 markdown（人读）。
pub fn render_markdown(report: &PromotionTrendReport) -> String {
    let mut out = format!(
        "# 晋级周报（生成于 {}）\n\n| 周 | 晋级 | 回滚 | 失败 | 审批中 | 成功率 |\n|---|---|---|---|---|---|\n",
        report.generated_at.format("%Y-%m-%d %H:%M UTC")
    );
    for w in &report.weeks {
        let rate = w
            .success_rate
            .map(|r| format!("{:.0}%", r * 100.0))
            .unwrap_or_else(|| "–".into());
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            w.week, w.promoted, w.rolled_back, w.failed, w.awaiting_review, rate
        ));
    }
    if let Some(ref alert) = report.alert {
        out.push_str(&format!("\n> **趋势告警**：{alert}\n"));
    }
    out
}

/// 周期报表器。
pub struct PromotionTrendReporter {
    ledger: Arc<dyn PromotionLedger>,
    report_dir: std::path::PathBuf,
    interval: std::time::Duration,
    audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
    latest: Arc<tokio::sync::RwLock<Option<PromotionTrendReport>>>,
}

impl PromotionTrendReporter {
    pub fn new(
        ledger: Arc<dyn PromotionLedger>,
        report_dir: std::path::PathBuf,
        interval: std::time::Duration,
        audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ledger,
            report_dir,
            interval,
            audit_stream,
            latest: Arc::new(tokio::sync::RwLock::new(None)),
        })
    }

    /// 最新报告（admin 端点用）；尚未生成过为 None。
    pub fn latest(&self) -> Arc<tokio::sync::RwLock<Option<PromotionTrendReport>>> {
        self.latest.clone()
    }

    /// 后台循环：立即生成一次，之后按间隔周期生成。
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        // 防御配置错误导致的 busy-loop：最小周期 60 秒。
        let interval = self.interval.max(std::time::Duration::from_secs(60));
        loop {
            if let Err(e) = self.generate_once().await {
                error!(error = %e, "Promotion trend report generation failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn generate_once(&self) -> SFResult<()> {
        let records = self.ledger.list(10_000).await?;
        let report = aggregate(&records, Utc::now());

        std::fs::create_dir_all(&self.report_dir)?;
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(self.report_dir.join("promotion-trend-latest.json"), &json)?;
        std::fs::write(
            self.report_dir.join("promotion-trend-latest.md"),
            render_markdown(&report),
        )?;
        let stamp = report.generated_at.format("%Y%m%d-%H%M%S");
        std::fs::write(
            self.report_dir
                .join(format!("promotion-trend-{stamp}.json")),
            &json,
        )?;

        if let Some(ref alert) = report.alert {
            error!(alert = %alert, "Promotion trend alert");
            if let Some(ref stream) = self.audit_stream {
                stream
                    .append(
                        cog_core::AuditKind::Custom("promotion_trend_alert".into()),
                        "promotion-trend",
                        "weekly-report",
                        "trend_down",
                        serde_json::json!({ "alert": alert }),
                    )
                    .await?;
            }
        } else {
            info!(
                weeks = report.weeks.len(),
                "Promotion trend report generated"
            );
        }

        *self.latest.write().await = Some(report);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::PromotionRecord;

    fn rec(weeks_ago: i64, status: PromotionStatus) -> PromotionRecord {
        let t = Utc::now() - chrono::Duration::weeks(weeks_ago);
        PromotionRecord {
            id: format!("id-{weeks_ago}-{}", status.as_str()),
            change_id: "p".into(),
            level: "l1_rollout".into(),
            decision_reason: "test".into(),
            cluster: "publisher".into(),
            status,
            outcome: String::new(),
            eval_summary: None,
            created_at: t,
            updated_at: t,
        }
    }

    #[test]
    fn empty_records_yield_zero_report_without_alert() {
        let report = aggregate(&[], Utc::now());
        assert_eq!(report.weeks.len(), WINDOW_WEEKS);
        assert!(report.alert.is_none());
        assert!(report.weeks.iter().all(|w| w.success_rate.is_none()));
    }

    #[test]
    fn declining_weeks_trigger_alert() {
        // 近三周成功率 100% → 50% → 0%（每周 2 个样本）。
        let mut records = vec![
            rec(2, PromotionStatus::Promoted),
            rec(2, PromotionStatus::Promoted),
            rec(1, PromotionStatus::Promoted),
            rec(1, PromotionStatus::RolledBack),
            rec(0, PromotionStatus::Failed),
            rec(0, PromotionStatus::RolledBack),
        ];
        let report = aggregate(&records, Utc::now());
        assert!(
            report.alert.is_some(),
            "expected trend-down alert: {report:?}"
        );

        // 打乱顺序不影响按时间分桶。
        records.reverse();
        let report2 = aggregate(&records, Utc::now());
        assert!(report2.alert.is_some());
    }

    #[test]
    fn improving_weeks_do_not_alert() {
        let records = vec![
            rec(2, PromotionStatus::Failed),
            rec(2, PromotionStatus::RolledBack),
            rec(1, PromotionStatus::Promoted),
            rec(1, PromotionStatus::Failed),
            rec(0, PromotionStatus::Promoted),
            rec(0, PromotionStatus::Promoted),
        ];
        assert!(aggregate(&records, Utc::now()).alert.is_none());
    }

    #[test]
    fn markdown_contains_table_and_alert() {
        let records = vec![
            rec(2, PromotionStatus::Promoted),
            rec(2, PromotionStatus::Promoted),
            rec(1, PromotionStatus::Promoted),
            rec(1, PromotionStatus::RolledBack),
            rec(0, PromotionStatus::Failed),
            rec(0, PromotionStatus::RolledBack),
        ];
        let md = render_markdown(&aggregate(&records, Utc::now()));
        assert!(md.contains("| 周 |"));
        assert!(md.contains("趋势告警"));
    }
}

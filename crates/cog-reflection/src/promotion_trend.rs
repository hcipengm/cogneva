//! 晋级周报（eval 长期趋势）。
//!
//! 周期任务按 ISO 周聚合晋级台账（近 8 周）：成功率、各级别分布、
//! 回滚量。报告写 `{data_dir}/reports/promotion-trend-latest.json`（机
//! 读）与 `.md`（人读）并保留周期归档。
//!
//! 报告回答两个不同的问题，判据分开写、不合并：
//! - 趋势向下：连续 3 周成功率下降且每周有足够完结对决样本——写审计
//!   告警（接管台 Audit Trail 可见），报告 `alert` 字段留痕。
//! - 停摆：连续 3 个整周一个都没晋级出去——`stall_alert` 字段留痕，
//!   并经 `PersistentAlertSink` 进持久化告警面（恢复时自动解除）。
//!   零晋级时每周样本数都是 0、成功率是空值，趋势判定会把这种周整周
//!   跳过，所以它必须有自己的判据，否则最响的失败读起来像系统空闲。

use std::sync::Arc;

use chrono::{DateTime, Datelike, Utc};
use cog_core::{
    PromotionLedger, PromotionStatus, PromotionTrendReport, PromotionTrendWeek, SFResult,
};
use tracing::{error, info, warn};

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
                awaiting_by_gate: 0,
                awaiting_over_diff_limit: 0,
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
            PromotionStatus::AwaitingApproval => {
                bucket.awaiting_review += 1;
                if let Some(kind) = rec.gate_kind.filter(|k| k.is_gate_diverted()) {
                    bucket.awaiting_by_gate += 1;
                    if kind == cog_core::PromotionGateKind::ApprovalDiffOverLimit {
                        bucket.awaiting_over_diff_limit += 1;
                    }
                }
            }
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

    // 停摆：连续多个整周一个变更都没晋级出去。与趋势判定共用同一个「连续
    // 周数」标尺，因为两者问的是同一个问题——这段时间里系统一直在往坏的方向
    // 走吗——只是症状不同（越来越差 vs 彻底不动）。
    //
    // 判据只数**已完成**的周：窗口末尾那一周是进行中的，刚过零点时零晋级是
    // 常态，把它算进来等于每周一准时误报一次。窗口最短为 8 周，尾部至少还
    // 剩 7 个整周，够 DECLINE_RUN 个连续周判定。
    //
    // 但进行中的这一周要参与**解除**：本周一旦有晋级，停摆就已经结束了，
    // 没道理再挂着告警等到这一周走完——那会让告警在系统恢复之后继续撒谎
    // 最多六天。
    let stall_alert = {
        let completed = &weeks[..weeks.len().saturating_sub(1)];
        let tail = &completed[completed.len().saturating_sub(DECLINE_RUN)..];
        let current_week_is_quiet = weeks.last().is_none_or(|w| w.promoted == 0);
        if tail.len() == DECLINE_RUN
            && current_week_is_quiet
            && tail.iter().all(|w| w.promoted == 0)
        {
            Some(format!(
                "连续 {} 周零晋级（{}），这段时间没有任何变更上线",
                DECLINE_RUN,
                tail.iter()
                    .map(|w| w.week.as_str())
                    .collect::<Vec<_>>()
                    .join(" → ")
            ))
        } else {
            None
        }
    };

    PromotionTrendReport {
        generated_at: now,
        weeks,
        alert,
        stall_alert,
    }
}

/// 把报告写成 markdown（人读）。
pub fn render_markdown(report: &PromotionTrendReport) -> String {
    let mut out = format!(
        "# 晋级周报（生成于 {}）\n\n| 周 | 晋级 | 回滚 | 失败 | 审批中 | 门拦 | 超行数 | 成功率 |\n|---|---|---|---|---|---|---|---|\n",
        report.generated_at.format("%Y-%m-%d %H:%M UTC")
    );
    for w in &report.weeks {
        let rate = w
            .success_rate
            .map(|r| format!("{:.0}%", r * 100.0))
            .unwrap_or_else(|| "–".into());
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
            w.week,
            w.promoted,
            w.rolled_back,
            w.failed,
            w.awaiting_review,
            w.awaiting_by_gate,
            w.awaiting_over_diff_limit,
            rate
        ));
    }
    out.push_str(
        "\n> 门拦 = 分级阈值把变更转入人工审批的数（阈值本身的代价，越多说明阈值在拦，\
         该调的是阈值而不是这些变更）；超行数 = 其中因 diff 行数超上限转入人工的数，\
         直接对应 max_diff_lines 这一个旋钮。\n",
    );
    if let Some(ref alert) = report.alert {
        out.push_str(&format!("\n> **趋势告警**：{alert}\n"));
    }
    if let Some(ref stall) = report.stall_alert {
        out.push_str(&format!("\n> **停摆告警**：{stall}\n"));
    }
    out
}

/// 周期报表器。
pub struct PromotionTrendReporter {
    ledger: Arc<dyn PromotionLedger>,
    report_dir: std::path::PathBuf,
    interval: std::time::Duration,
    audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
    alert_sink: Option<Arc<dyn cog_core::PersistentAlertSink>>,
    latest: Arc<tokio::sync::RwLock<Option<PromotionTrendReport>>>,
}

impl PromotionTrendReporter {
    /// `alert_sink` is the persisted-alert port. Without it the stall condition
    /// still lands in the report file and the audit stream, but stays a
    /// document a human has to go and read rather than a firing alert the rest
    /// of the system can turn into work.
    pub fn new(
        ledger: Arc<dyn PromotionLedger>,
        report_dir: std::path::PathBuf,
        interval: std::time::Duration,
        audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
        alert_sink: Option<Arc<dyn cog_core::PersistentAlertSink>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ledger,
            report_dir,
            interval,
            audit_stream,
            alert_sink,
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

        self.publish_stall(&report).await;

        *self.latest.write().await = Some(report);
        Ok(())
    }

    /// Drive the stall condition into the persisted alert state machine.
    ///
    /// Both directions go through the same call: the sink reconciles by dedup
    /// key, so a week that finally promotes resolves the row instead of leaving
    /// a stale firing alert behind. The labels carry the per-week breakdown, so
    /// someone reading a firing stall can tell a pipeline that generates
    /// nothing from one that generates and fails to land, without opening the
    /// report.
    async fn publish_stall(&self, report: &PromotionTrendReport) {
        let Some(ref sink) = self.alert_sink else {
            return;
        };

        let stall = report.stall_alert.as_deref();
        let weeks: Vec<serde_json::Value> = report
            .weeks
            .iter()
            .map(|w| {
                serde_json::json!({
                    "week": w.week,
                    "promoted": w.promoted,
                    "failed": w.failed,
                    "rolled_back": w.rolled_back,
                    "awaiting_review": w.awaiting_review,
                })
            })
            .collect();
        let draft = cog_core::PersistentAlertDraft {
            rule: cog_core::ALERT_RULE_PROMOTION_STALL.into(),
            dedup_key: cog_core::ALERT_RULE_PROMOTION_STALL.into(),
            // Critical, not Warning: 连续三周零晋级意味着这套系统的主职能已经
            // 停摆，而它恰恰是过去六周谁都没看见的那件事。压成 warning 就等于
            // 把同一件事再藏一次。
            severity: "critical".into(),
            message: stall
                .map(|s| s.to_string())
                .unwrap_or_else(|| "晋级已恢复，停摆告警解除".to_string()),
            labels: serde_json::json!({ "weeks": weeks }),
        };

        match sink.set_persistent_alert(stall.is_some(), &draft).await {
            Err(e) => warn!(error = %e, "promotion stall alert not persisted"),
            Ok(()) => {
                if let Some(stall) = stall {
                    error!(alert = stall, "Promotion stall alert");
                }
            }
        }
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
            gate_kind: None,
            created_at: t,
            updated_at: t,
        }
    }

    fn rec_with_gate(
        weeks_ago: i64,
        status: PromotionStatus,
        gate_kind: cog_core::PromotionGateKind,
    ) -> PromotionRecord {
        PromotionRecord {
            gate_kind: Some(gate_kind),
            ..rec(weeks_ago, status)
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
    fn gate_diverted_approvals_are_counted_apart_from_runtime_downgrades() {
        // 同一周里三条待审批，只有两条是分级阈值自己拦下的；第三条是自动
        // 通道被运行时条件降级，门没参与，不能算进门槛代价。
        let records = vec![
            rec_with_gate(
                0,
                PromotionStatus::AwaitingApproval,
                cog_core::PromotionGateKind::ApprovalDiffOverLimit,
            ),
            rec_with_gate(
                0,
                PromotionStatus::AwaitingApproval,
                cog_core::PromotionGateKind::ApprovalCorePath,
            ),
            rec_with_gate(
                0,
                PromotionStatus::AwaitingApproval,
                cog_core::PromotionGateKind::AutoRollout,
            ),
        ];
        let week = &aggregate(&records, Utc::now()).weeks[WINDOW_WEEKS - 1];
        assert_eq!(week.awaiting_review, 3);
        assert_eq!(week.awaiting_by_gate, 2);
        // 只有超行数那一条落在具体旋钮上。
        assert_eq!(week.awaiting_over_diff_limit, 1);
    }

    #[test]
    fn legacy_approval_without_a_gate_kind_is_not_a_gate_cost() {
        let records = vec![rec(0, PromotionStatus::AwaitingApproval)];
        let week = &aggregate(&records, Utc::now()).weeks[WINDOW_WEEKS - 1];
        assert_eq!(week.awaiting_review, 1);
        assert_eq!(week.awaiting_by_gate, 0);
        assert_eq!(week.awaiting_over_diff_limit, 0);
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

    /// 零晋级是这套系统能给出的最响的失败，但它每周的样本数都是 0，成功率
    /// 是空值——趋势判定会把这种周整周跳过。判据必须独立于「有对决胜出」
    /// 这个前提，否则最该被看见的状态和「系统空闲」长得一模一样。
    #[test]
    fn weeks_without_a_single_promotion_read_as_a_stall_not_as_silence() {
        let report = aggregate(&[], Utc::now());
        assert!(
            report.alert.is_none(),
            "没有样本谈不上成功率下降：{report:?}"
        );
        let stall = report
            .stall_alert
            .expect("连续零晋级必须被报出来，不能静默");
        assert!(stall.contains("零晋级"), "告警要说明是什么状态：{stall}");
    }

    #[test]
    fn a_promotion_in_the_completed_tail_clears_the_stall() {
        // 两天前刚晋级过一次，完成周的尾部里有产出，就不算停摆。
        let report = aggregate(&[rec(2, PromotionStatus::Promoted)], Utc::now());
        assert!(report.stall_alert.is_none(), "{report:?}");
    }

    /// 本周刚开始时零晋级是常态，不能构成停摆；本周一旦有晋级，停摆就已经
    /// 结束，也不必等这一周走完才解除——否则告警会在系统恢复后继续撒谎。
    #[test]
    fn an_in_progress_week_neither_causes_nor_holds_the_stall() {
        let report = aggregate(&[rec(0, PromotionStatus::Promoted)], Utc::now());
        assert!(
            report.stall_alert.is_none(),
            "本周已晋级就说明停摆结束了：{report:?}"
        );
    }

    #[test]
    fn markdown_surfaces_the_stall_separately_from_the_trend() {
        let md = render_markdown(&aggregate(&[], Utc::now()));
        assert!(md.contains("停摆告警"), "{md}");
        assert!(!md.contains("趋势告警"), "两条判据不能混成一条：{md}");
    }

    struct FixedLedger(Vec<PromotionRecord>);

    #[async_trait::async_trait]
    impl cog_core::PromotionLedger for FixedLedger {
        async fn record(&self, _rec: PromotionRecord) -> SFResult<()> {
            Ok(())
        }
        async fn update_status(
            &self,
            _id: &str,
            _status: PromotionStatus,
            _outcome: &str,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn count_promoted_since(&self, _since: DateTime<Utc>) -> SFResult<u64> {
            Ok(0)
        }
        async fn recent(&self, _limit: usize) -> SFResult<Vec<PromotionRecord>> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        calls: std::sync::Mutex<Vec<(bool, String, String)>>,
    }

    #[async_trait::async_trait]
    impl cog_core::PersistentAlertSink for RecordingSink {
        async fn set_persistent_alert(
            &self,
            condition: bool,
            draft: &cog_core::PersistentAlertDraft,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push((
                condition,
                draft.rule.clone(),
                draft.severity.clone(),
            ));
            Ok(())
        }
        async fn list_active_persistent_alerts(
            &self,
            _rule_prefix: &str,
            _limit: i64,
        ) -> Vec<cog_core::PersistedAlert> {
            Vec::new()
        }
    }

    fn temp_report_dir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("promotion-trend-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// 报告文件是给人看的产物，不是判据：只有条件真的进了持久化告警面，
    /// 系统才可能自己发现「六周零晋级」。恢复时同一条 dedup key 必须被解除，
    /// 否则告警会在系统恢复之后永久挂着。
    #[tokio::test]
    async fn the_stall_reaches_the_alert_sink_and_is_resolved_on_recovery() {
        let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
        let stalled = PromotionTrendReporter::new(
            Arc::new(FixedLedger(Vec::new())),
            temp_report_dir("stalled"),
            std::time::Duration::from_secs(60),
            None,
            Some(sink.clone()),
        );
        stalled.generate_once().await.unwrap();

        let recovered = PromotionTrendReporter::new(
            Arc::new(FixedLedger(vec![rec(2, PromotionStatus::Promoted)])),
            temp_report_dir("recovered"),
            std::time::Duration::from_secs(60),
            None,
            Some(sink.clone()),
        );
        recovered.generate_once().await.unwrap();

        let calls = sink.calls.lock().unwrap().clone();
        let rule = cog_core::ALERT_RULE_PROMOTION_STALL.to_string();
        assert_eq!(
            calls,
            vec![
                (true, rule.clone(), "critical".to_string()),
                (false, rule, "critical".to_string()),
            ],
            "停摆要报出来，恢复要解除，且用同一条 rule：{calls:?}"
        );
    }
}

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
                pending: 0,
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
            PromotionStatus::Pending => bucket.pending += 1,
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
                "连续 {} 周零晋级（{}），这段时间这张台账里没有一条变更被记成上线。{}{}",
                DECLINE_RUN,
                tail.iter()
                    .map(|w| w.week.as_str())
                    .collect::<Vec<_>>()
                    .join(" → "),
                stall_reason(tail),
                waiting_in_the_open_week(weeks.last())
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

/// 停摆停下的是哪一侧，从**这几周自己的台账数**里读出来。
///
/// 判词能说的只有这张台账里的数。**这条台账是一条通道的账，不是系统的账**：
/// 它由 `hand_off` → `AutoPromoter` → `PromotionChannel` 写，出口推的是
/// `gitops.repo_url`（生产 rollout）；而变更还有另一条上线路——landing 直接
/// 提交到 main、由 `mainline_deployer` 滚集群，那条路**一个字节都不写这张台账**。
/// 于是「台账里零晋级」对三件处置完全不同的事同时成立：系统从来没生成出变更
/// （去看生成侧）、生成出来了但全卡在这条通道（去看这条通道）、以及**变更从另
/// 一条通道上线了**（去看 landing 侧的落地计数）。判词的读者往往只有那一行
/// 文字，看不到旁边的 labels，所以原因必须长在判词里；也不能替读者断言
/// 「没有任何变更上线」——那句话量的是系统，而这面读数量的是通道。
///
/// 原因只能从**同一批周**里取。拿一个别的窗口的计数（比如自本进程启动以来的
/// 信号计数）拼在这里是不行的：两个窗口不一样长，读起来就是一句没有依据的话，
/// 而且进程一重启那个数就归零，而停摆压根没变。看清这一点与上面那条并不冲突：
/// 另一条通道的计数**可以**当旁证，但必须先解决「两个计数的窗口怎么对齐」
/// （落地按提交时刻、台账按行 `created_at`，两者不同源），在那之前只点名
/// 该去读哪条序列，不把它的数搬进这句话。
///
/// 「台账里一条都没有」**不是**「一条都没有生成出来」的读数：它还有一个
/// 来源——台账自己没被写（写者掉了/结构上不可达）。那时前面几件事在台账上
/// 与本情形同形，而判词如果把「没生成」说成事实，读者会去查一个没坏的地方。
/// 所以零记录这一支只报它量到的东西（台账空）和几种可能，不替读者选一种。
fn stall_reason(tail: &[PromotionTrendWeek]) -> String {
    let pending: u64 = tail.iter().map(|w| w.pending).sum();
    let awaiting: u64 = tail.iter().map(|w| w.awaiting_review).sum();
    let failed: u64 = tail.iter().map(|w| w.failed).sum();
    let rolled_back: u64 = tail.iter().map(|w| w.rolled_back).sum();
    let generated = pending + awaiting + failed + rolled_back;
    if generated == 0 {
        "这几周台账里没有任何变更记录（待执行/审批中/失败/回滚全为 0）：要么没有变更被生成出来（去看生成侧），要么台账自己没被写（去看台账的写者），要么变更走了另一条不写这张台账的通道（去看 landing 侧的落地计数）——这几种情形在这份读数上同形，判词判不出是哪一种".to_string()
    } else {
        format!(
            "这几周生成了 {generated} 条却一条都没从这条通道落地（待执行 {pending}、审批中 {awaiting}、失败 {failed}、回滚 {rolled_back}）——停的是这条落地通道，不是生成侧"
        )
    }
}

/// 进行中这一周里排到审批台上的变更，单独起一句说。
///
/// 停摆判词只数**已完成**的周（见 `aggregate` 里的理由），代价是它看不见窗口
/// 末尾那一周正在排队的东西。而在这一格里等着的那条变更，恰好是判词给的两种解释
/// （没生成 / 台账自己没被写）都盖不住的一种状态：变更生成出来了、台账也写了、
/// 没有任何东西坏掉，动的只是**读者自己的手**。判词把读者支去生成侧或台账写者，
/// 他查完两处都是好的，告警却仍在响。
///
/// 这一格不属于上面那句话的窗口（它讲的是已完成的那几周），所以不并进去，而是
/// 单独起一句、自己带星期号。两个窗口的计数拼在同一句话里，读起来就是一句没有
/// 依据的话——判词里每个数都要能指回自己那个窗口。
///
/// 只说审批中这一档：待执行的那些在飞、轮不到读者动手，写进来只会让这句变钝。
fn waiting_in_the_open_week(week: Option<&PromotionTrendWeek>) -> String {
    match week {
        Some(w) if w.awaiting_review > 0 => format!(
            "另外，本周（{}）已有 {} 条变更停在审批台等待人工批准——停摆的另一头可能就在那里，既不是生成侧也不是台账写者。",
            w.week, w.awaiting_review
        ),
        _ => String::new(),
    }
}

/// 把报告写成 markdown（人读）。
pub fn render_markdown(report: &PromotionTrendReport) -> String {
    let mut out = format!(
        "# 晋级周报（生成于 {}）\n\n| 周 | 晋级 | 回滚 | 失败 | 待执行 | 审批中 | 门拦 | 超行数 | 成功率 |\n|---|---|---|---|---|---|---|---|---|\n",
        report.generated_at.format("%Y-%m-%d %H:%M UTC")
    );
    for w in &report.weeks {
        let rate = w
            .success_rate
            .map(|r| format!("{:.0}%", r * 100.0))
            .unwrap_or_else(|| "–".into());
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            w.week,
            w.promoted,
            w.rolled_back,
            w.failed,
            w.pending,
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
                    "pending": w.pending,
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

    /// 停摆的判词要说清停的是哪一侧。同样是「连续几周零晋级」，「一条变更都没
    /// 生成出来」与「生成了却一条都没落地」要去看的地方相反，而判词的读者往往
    /// 只有那一行文字。
    ///
    /// 台账零记录这一支还多一层：它只证明台账是空的，不证明没生成过——台账自己
    /// 没被写的时候，这两件事在台账上同形。所以那一支要点名两种可能，不能替
    /// 读者挑一种，否则读者会去查一个没坏的地方。
    #[test]
    fn the_stall_verdict_says_which_side_stopped() {
        let quiet = aggregate(&[], Utc::now())
            .stall_alert
            .expect("一条记录都没有也是停摆");
        assert!(
            quiet.contains("生成侧"),
            "空台账的第一种可能是没生成，判词要点到生成侧：{quiet}"
        );
        assert!(
            quiet.contains("写者"),
            "台账自己没被写是同形的第二种可能，不点名就只查一头：{quiet}"
        );
        assert!(
            !quiet.contains("一条变更都没有生成出来"),
            "台账空推不出「没生成」，判词不能把可能说成事实：{quiet}"
        );

        // 同样的零晋级，但这几周里变更是存在的，只是每条都停在落地那一段。
        let records = vec![
            rec(1, PromotionStatus::Failed),
            rec(2, PromotionStatus::RolledBack),
            rec(3, PromotionStatus::AwaitingApproval),
            rec(3, PromotionStatus::Pending),
        ];
        let stalled = aggregate(&records, Utc::now())
            .stall_alert
            .expect("零晋级就是停摆");
        assert!(
            stalled.contains("落地通道"),
            "有生成没落地就该指向落地通道：{stalled}"
        );
        assert!(
            !stalled.contains("一条变更都没有生成出来"),
            "判词不能把「生成了但没落地」说成「没生成」：{stalled}"
        );
    }

    /// 这张台账是**一条通道**的账，不是系统的账。这条通道静默的同时，变更完全
    /// 可能从另一条通道上线——landing 提交到 main、由部署器滚集群，那条路一个
    /// 字节都不写这张台账（实测 2026-10-05：判词点名的 W39／W40 里各有 6／3 笔
    /// 走的是那条路）。所以判词的措辞只能说自己数了哪张台账，不能替系统下
    /// 「没有任何变更上线」这种结论，并且要把这条通道之外的可能点出来。
    #[test]
    fn the_stall_verdict_speaks_for_its_own_ledger_not_for_the_system() {
        let quiet = aggregate(&[], Utc::now())
            .stall_alert
            .expect("一条记录都没有也是停摆");
        assert!(
            !quiet.contains("没有任何变更上线"),
            "这句话量的是系统、读的却是通道，判词不能替系统下结论：{quiet}"
        );
        assert!(
            quiet.contains("没有一条变更被记成上线"),
            "判词要说清它数的是这张台账里的记录：{quiet}"
        );
        assert!(
            quiet.contains("landing"),
            "变更从别的通道上线是第四种可能，不点名该去看哪条序列，读者只会查台账这一头：{quiet}"
        );

        // 同一句话在有记录的那一支上也要成立：断言与分支无关。
        let records = vec![rec(0, PromotionStatus::Failed)];
        let stalled = aggregate(&records, Utc::now())
            .stall_alert
            .expect("零晋级就是停摆");
        assert!(
            !stalled.contains("没有任何变更上线"),
            "有记录这一支同样不能替系统下结论：{stalled}"
        );
    }

    /// 停摆判词只看已完成的那几周，所以它看不见进行中这一周里已经排到审批台上的
    /// 变更——而那正好是判词的两种解释都盖不住的一种状态，也是唯一一种**读者自己
    /// 动手就能解掉**的状态。这一格必须单独说出来。
    #[test]
    fn a_change_waiting_for_approval_in_the_open_week_is_named_though_the_verdict_ignores_it() {
        let records = vec![rec(0, PromotionStatus::AwaitingApproval)];
        let stall = aggregate(&records, Utc::now())
            .stall_alert
            .expect("已完成的那几周零晋级，仍是停摆");
        assert!(
            stall.contains("审批台"),
            "本周有变更在等人批准就必须点名，否则读者会去查生成侧与台账写者：{stall}"
        );
        assert!(stall.contains("1 条"), "{stall}");
        assert!(
            stall.contains("生成侧") && stall.contains("写者"),
            "原有那两种解释不能因为多了一句就被顶掉：{stall}"
        );

        // 没有在等审批的变更时，这一句不能凭空长出来。
        let quiet = aggregate(&[], Utc::now())
            .stall_alert
            .expect("一条记录都没有也是停摆");
        assert!(!quiet.contains("审批台"), "{quiet}");
    }

    /// 待执行那一档必须真的进桶。少了它，「生成了但还在飞」的三周在判词里
    /// 与「什么都没生成」一模一样——这正是这一档存在的理由。
    #[test]
    fn changes_that_are_still_in_flight_are_counted_as_generated() {
        let records = vec![rec(1, PromotionStatus::Pending)];
        let week = &aggregate(&records, Utc::now()).weeks[6];
        assert_eq!(week.pending, 1);
        assert_eq!(week.promoted, 0);
        assert_eq!(
            week.success_rate, None,
            "还在飞的变更不是胜负样本，不能进成功率分母"
        );
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

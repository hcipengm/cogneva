//! 绩效越界 ⇒ 一条重规划任务。
//!
//! 面板上的红字不改变任何事：一个只把指数画出来的项目面，读到越界的人要么恰好
//! 在那儿，要么什么都没发生。所以越界要落成任务图里的一条活——它会被调度、被
//! 认领、有验收标准、有终态。图上多一条活，读者才知道这件事有人在管；一条只在
//! 通知里说过的越界，第二天没人记得它还在。
//!
//! 这里只做造任务这一侧才知道的两件判断：
//!
//! 1. **这一版基线是不是已经有过重规划**。判定侧每轮都会把同一处越界再报一遍
//!    （读数没变），所以「同一条越界只造一条活」必须落在这里。去重键是基线版本，
//!    不是日期：重规划要交的交付物就是新一版基线，版本没动说明上一次还没交货，
//!    再报一次不会多出新东西；版本动了而还在落后，那是新的事实，值得再问一次。
//!    按日期去重会让同一个停滞的项目每天多一条一模一样的活，最后没人看队列。
//! 2. **这条活有没有验收标准**。重规划也是一份工作包，工作包没有验收标准，做完
//!    与没做完就没有判据——这一条在分解门里已经判过一次，这里在生成处就判：
//!    入图之后再没有读面能把它补出来。

use chrono::{DateTime, Utc};
use cog_core::{BreachReport, PerformanceIndex, ReplanOutcome, Task, TaskType};

/// 重规划任务的类型拼写。图上按它认出「这一条是回注出来的活」。
pub const REPLAN_TASK_KIND: &str = "replan";

/// 这条任务在输入里记自己那一格的键。判定去重、读面认出它都读这一格。
pub const REPLAN_INPUT_KEY: &str = "replan";

/// id 的可读前缀。后面跟指数名与一段摘要。
const ID_PREFIX: &str = "replan";

/// 造任务这一侧的判断结果。
///
/// 与交给判定侧的 [`ReplanOutcome`] 是两个东西：那一个是「这件事落定了没有」，
/// 这一个是「要不要把这一行塞进图里」，所以它把行本身带着——行不交给调用方，
/// 调用方就得自己再造一遍，两遍必然长得不一样。
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// 造出来了，直接入图。
    Mint(Box<Task>),
    /// 这一版基线已经有过重规划，不再造。
    AlreadyMinted { task_id: String, open: bool },
    /// 生成处拒绝。判词要说清缺哪一件。
    Refused { reason: String },
}

impl From<Decision> for ReplanOutcome {
    fn from(decision: Decision) -> Self {
        match decision {
            Decision::Mint(task) => ReplanOutcome::Minted {
                task_id: task.id.clone(),
            },
            Decision::AlreadyMinted { task_id, open } => {
                ReplanOutcome::AlreadyMinted { task_id, open }
            }
            Decision::Refused { reason } => ReplanOutcome::Refused { reason },
        }
    }
}

/// 一条重规划任务的 id：同一（项目、指数、基线版本）永远同一个。
///
/// 可读的那几个事实写在任务的输入里，不塞进 id：id 会进 URL 与日志，而基线版本
/// 是一个带 `:` 与 `+` 的时间戳，塞进去只会让每个调用点各自转义一次。
pub fn replan_id(project: &str, index: PerformanceIndex, revision: &str) -> String {
    let digest = blake3::hash(format!("{project}\u{0}{revision}").as_bytes());
    let hex = digest.to_hex();
    format!("{ID_PREFIX}-{}-{}", index.code(), &hex.as_str()[..16])
}

/// 这一行是不是给（项目、指数、基线版本）这处越界造的重规划。
///
/// 按输入里记的三格认，不按 id 认：id 里只有一段摘要，读不出它说的是哪一版；
/// 而将来如果有人手工补一条同形状的行，按输入认也能认出重复。
pub fn is_replan_for(task: &Task, project: &str, index: PerformanceIndex, revision: &str) -> bool {
    if !matches!(&task.task_type, TaskType::Custom(kind) if kind == REPLAN_TASK_KIND) {
        return false;
    }
    let Some(facts) = task.input.get(REPLAN_INPUT_KEY) else {
        return false;
    };
    facts.get("project").and_then(|v| v.as_str()) == Some(project)
        && facts.get("index").and_then(|v| v.as_str()) == Some(index.code())
        && facts.get("baseline_rev").and_then(|v| v.as_str()) == Some(revision)
}

/// 造一条重规划任务，或者说明为什么不造。
///
/// `existing` 是这个部署里的任务行（至少包含这个项目的）：去重要读它们。
/// `revision` 是这次越界发生时项目所处的基线版本——验收标准与去重都按它。
pub fn replan_task(
    breach: &BreachReport,
    revision: &str,
    existing: &[Task],
    now: DateTime<Utc>,
) -> Decision {
    // 交上来的读数先自己站得住：三个都能在不成立的读数上造出一条看着很像的活，
    // 而那条活要花的 token 是真的。
    if !breach.measured.is_finite() {
        return rejected(format!(
            "指数读数是 {}：一个非有限的比值不是读数，按它造的活没人知道该干什么",
            breach.measured
        ));
    }
    if breach.against == 0 {
        return rejected(format!(
            "{}的分母是 0（分子 {}）：分母为零时比值不是 0 而是没有定义，差多少也说不出来",
            breach.index.denominator(),
            breach.earned
        ));
    }
    if !breach.index.breaching(breach.measured) {
        return rejected(format!(
            "{}量到 {:.3}，没有越界（判据是低于 1.0）",
            breach.index.what(),
            breach.measured
        ));
    }

    if let Some(previous) = existing
        .iter()
        .find(|t| is_replan_for(t, &breach.project, breach.index, revision))
    {
        return Decision::AlreadyMinted {
            task_id: previous.id.clone(),
            // 还没走完说的是「它的结论还在路上」；已经走完说的是「去看它的结论，
            // 那一版基线就是它的交付物」。两件事读者要做的事不同。
            open: !previous.status.is_terminal(),
        };
    }

    let task_id = replan_id(&breach.project, breach.index, revision);
    let mut task = Task::new(
        task_id,
        TaskType::Custom(REPLAN_TASK_KIND.to_string()),
        serde_json::json!({
            "goal": goal_sentence(breach, revision),
            REPLAN_INPUT_KEY: {
                "project": breach.project,
                "index": breach.index.code(),
                "measured": breach.measured,
                "earned": breach.earned,
                "against": breach.against,
                "gap_tokens": breach.gap_tokens(),
                "baseline_rev": revision,
                "observed_at": now.to_rfc3339(),
            },
        }),
    );
    // 挂在这个项目的根下：项目就是 goal 根连同它的后代，挂在别处它就不是这个
    // 项目的活——看板、范围树、挣值三面都不会把它算进来。
    task.goal_id = Some(breach.project.clone());
    task.parent_task_id = Some(breach.project.clone());
    task.acceptance = acceptance_for(breach, revision);
    task.created_at = now;
    task.updated_at = now;

    // 生成处过一遍分解门：这是一份没有人挂在它下面的工作包，与分解出来的叶子读
    // 的是同一条判据。判不过就不出这条行——「产物是什么」由造它的地方保证，
    // 不能指望下游再拦一次，因为下游那条路上它已经是图里的一条活了。
    let defects = crate::wbs::check_work_package(&task.id, &task.acceptance);
    if !defects.is_empty() {
        return rejected(format!(
            "这条活过不了分解门：{}",
            crate::wbs::summary(&defects)
        ));
    }

    Decision::Mint(Box::new(task))
}

fn rejected(reason: String) -> Decision {
    Decision::Refused { reason }
}

/// 这条活要做的事，一句自己站得住的话：越界的是哪一项、差多少、按哪一版。
///
/// 不写成「请重规划」——执行它的那套 pipeline 与后来读它的那个人，都要能从这
/// 一句里看出判据：量到多少、线在哪、差多少 token。
fn goal_sentence(breach: &BreachReport, revision: &str) -> String {
    format!(
        "重规划项目 {}：{}量到 {:.3}（挣值 {} token / {} {} token，差 {} token），\
         基线版本 {revision}",
        breach.project,
        breach.index.what(),
        breach.measured,
        breach.earned,
        breach.index.denominator(),
        breach.against,
        breach.gap_tokens(),
    )
}

/// 怎样算做完。两个指数不一样，因为能收回来与收不回来的东西不一样。
fn acceptance_for(breach: &BreachReport, revision: &str) -> Vec<String> {
    let mut acceptance = vec![format!(
        "`task_plan` 上出现一个比 {revision} 新的 baseline_rev，且覆盖本项目全部还没完成的任务\
         ——只重排一部分，落后只是被挪到没重排的那几条上"
    )];
    match breach.index {
        PerformanceIndex::Schedule => {
            acceptance.push(format!(
                "按新基线重算的进度绩效 ≥ 1.0（`GET /api/v1/projects/{}/evm` 直接给这个数）；\
                 重排之后仍然追不平时，在结果里写明残余落后多少 token、为什么追不平",
                breach.project
            ));
        }
        PerformanceIndex::Cost => {
            acceptance.push(
                "在结果里写明这次超支怎么处置：砍掉哪些任务（列出 id）、还是把哪几条的 \
                 `budget_tokens` 上调到多少、还是照原计划继续——三者之一，都带 token 数"
                    .to_string(),
            );
            // 已经烧掉的 token 收不回来：要求成本绩效回到 1.0，等于要求一条做不到的
            // 验收标准，那样的活只会失败到重试用光。
            acceptance.push(
                "不要求成本绩效回到 1.0（已经花掉的收不回来）；要求的是上面那一条处置\
                 连同它的 token 数被写下来，下一次读这个项目的人不必再猜"
                    .to_string(),
            );
        }
    }
    acceptance
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    const PROJECT: &str = "goal-root-1";
    const REV: &str = "2026-10-26T09:00:00+00:00";

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_760_000_000 + secs, 0).unwrap()
    }

    fn breach(index: PerformanceIndex, measured: f64, earned: u64, against: u64) -> BreachReport {
        BreachReport {
            project: PROJECT.to_string(),
            index,
            measured,
            earned,
            against,
        }
    }

    fn minted(decision: Decision) -> Task {
        match decision {
            Decision::Mint(task) => *task,
            other => panic!("expected a task, got {other:?}"),
        }
    }

    /// 一处越界造出一条活，它自己把所有该说的话都说了。
    #[test]
    fn a_breach_mints_one_task_under_the_project_root_naming_the_index_and_the_gap() {
        let found = breach(PerformanceIndex::Schedule, 0.62, 6_200, 10_000);
        let task = minted(replan_task(&found, REV, &[], at(0)));

        assert_eq!(task.goal_id.as_deref(), Some(PROJECT), "挂在项目根下");
        assert_eq!(task.parent_task_id.as_deref(), Some(PROJECT));
        assert!(task.is_executable, "这是一条要被跑的活，不是登记行");
        assert!(!task.acceptance.is_empty(), "工作包必须带验收标准");

        let goal = task.input["goal"].as_str().expect("goal sentence");
        assert!(goal.contains("进度绩效"), "点名越界的是哪一项：{goal}");
        assert!(goal.contains("0.620"), "写出量到多少：{goal}");
        assert!(goal.contains("3800"), "写出差多少 token：{goal}");
        assert!(goal.contains(REV), "写出按哪一版基线：{goal}");

        let facts = &task.input[REPLAN_INPUT_KEY];
        assert_eq!(facts["project"], PROJECT);
        assert_eq!(facts["index"], "schedule");
        assert_eq!(facts["gap_tokens"], 3_800);
        assert_eq!(facts["baseline_rev"], REV);
    }

    /// 同一处越界重问一次不产生第二条活：判定侧每一轮都会再报一遍。
    #[test]
    fn the_same_breach_on_the_same_revision_mints_once_and_says_whether_it_is_open() {
        let found = breach(PerformanceIndex::Cost, 0.5, 1_000, 2_000);
        let first = minted(replan_task(&found, REV, &[], at(0)));

        let again = replan_task(&found, REV, &[first.clone()], at(60));
        assert_eq!(
            again,
            Decision::AlreadyMinted {
                task_id: first.id.clone(),
                open: true,
            },
            "同一个 id 的活还在待办里，就是还没交货"
        );

        let mut done = first.clone();
        done.status = cog_core::TaskStatus::Completed;
        assert_eq!(
            replan_task(&found, REV, &[done], at(120)),
            Decision::AlreadyMinted {
                task_id: first.id.clone(),
                open: false,
            },
            "它走完了：该去看它的结论，而不是再造一条一样的问题"
        );
    }

    /// 版本动了而还在落后，那是新的事实：id 换一个，值得再问一次。
    #[test]
    fn a_new_baseline_revision_is_a_new_breach() {
        let found = breach(PerformanceIndex::Schedule, 0.7, 7_000, 10_000);
        let before = minted(replan_task(&found, REV, &[], at(0)));
        let after = minted(replan_task(
            &found,
            "2026-10-27T09:00:00+00:00",
            &[before.clone()],
            at(86_400),
        ));
        assert_ne!(before.id, after.id);
        // 两个 id 都只由（项目、指数、版本）决定，换个时刻问同一个问题答案一样。
        assert_eq!(
            after.id,
            replan_id(
                PROJECT,
                PerformanceIndex::Schedule,
                "2026-10-27T09:00:00+00:00"
            )
        );
    }

    /// 三条红线：造出来的每一份工作包都过得了分解门。
    ///
    /// 这一条与下面那条是一对——一条说「造出来的能过」，一条说「门真的会拦」。
    /// 只留前者，验收标准整段删空也照样绿：空的东西过一道看不见东西的门。
    #[test]
    fn every_minted_package_passes_the_same_gate_the_decomposition_uses() {
        for index in PerformanceIndex::ALL {
            let task = minted(replan_task(&breach(index, 0.5, 1, 2), REV, &[], at(0)));
            assert!(!task.acceptance.is_empty(), "{index:?} 造出了空验收标准");
            let defects = crate::wbs::check_work_package(&task.id, &task.acceptance);
            assert_eq!(
                defects,
                Vec::new(),
                "{index:?} 造出的工作包过不了分解门：{defects:?}"
            );
        }
    }

    /// 门对同一形状确实会拦：没有验收标准的工作包是 `NoAcceptance`。
    #[test]
    fn a_package_without_acceptance_is_what_that_gate_refuses() {
        let defects = crate::wbs::check_work_package("replan-x", &[]);
        assert_eq!(defects.len(), 1);
        assert_eq!(defects[0].kind, crate::wbs::WbsDefectKind::NoAcceptance);
    }

    /// 不成立的读数不造活：造出来的那条要花的 token 是真的。
    #[test]
    fn a_reading_that_is_not_a_breach_is_refused_with_the_reason() {
        for (found, pinch) in [
            (breach(PerformanceIndex::Schedule, 1.0, 10, 10), "没有越界"),
            (breach(PerformanceIndex::Cost, 1.4, 14, 10), "没有越界"),
            (breach(PerformanceIndex::Schedule, 0.0, 0, 0), "分母是 0"),
            (breach(PerformanceIndex::Cost, f64::NAN, 1, 2), "非有限"),
        ] {
            match replan_task(&found, REV, &[], at(0)) {
                Decision::Refused { reason } => assert!(
                    reason.contains(pinch),
                    "判词要说清是哪一件不成立（找 {pinch}）：{reason}"
                ),
                other => panic!("expected a refusal for {found:?}, got {other:?}"),
            }
        }
    }
}

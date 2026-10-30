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

/// 同一版基线上，同一条越界最多造几条重规划。
///
/// 三条：一条没跑起来、一条跑死在半路，都还留一次机会。三条都走完了而基线
/// 还是那一版，缺的就不是第四次尝试，是有人去看这三条为什么都没交货——再往下
/// 造就只是每轮往队列里添一条注定失败的活，而队列上那一堆长得一样的行会把真
/// 正在等的那条埋掉。
// cap: bound — 三次都没有交货，缺的不是第四次尝试，是有人去看这三条；再往下就是每轮往队列里添一条注定失败的活
pub const MAX_ATTEMPTS: usize = 3;

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

/// 一条重规划任务的 id：同一（项目、指数、基线版本、第几次尝试）永远同一个。
///
/// 可读的那几个事实写在任务的输入里，不塞进 id：id 会进 URL 与日志，而基线版本
/// 是一个带 `:` 与 `+` 的时间戳，塞进去只会让每个调用点各自转义一次。
///
/// 尝试次数进摘要：一次尝试死掉之后，下一次要造的是一条**新**活——同一个 id 再
/// 造一遍会被入图那一侧按「已经有过」顶掉，而它顶掉的那一行是死的。
pub fn replan_id(project: &str, index: PerformanceIndex, revision: &str, attempt: u32) -> String {
    let digest = blake3::hash(format!("{project}\u{0}{revision}\u{0}{attempt}").as_bytes());
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

    // 这一版基线上已经有过的重规划。去重键留在 `is_replan_for` 里：项目、指数、
    // 基线版本三格都在那一处认。
    //
    // 走到这里说明当前基线版本还是 `revision`——一个交了货的重规划会把基线推
    // 到新的一版，那么下一次比较的就不是这一版了，这些行也不会再被认出来。
    // 换句话说：这些行里没有一条交付过，它们的终态只说明「这一次尝试结束了」。
    let attempts: Vec<&Task> = existing
        .iter()
        .filter(|t| is_replan_for(t, &breach.project, breach.index, revision))
        .collect();

    // 还有一条没走完：它的结论还在路上，等它，别再添一条问同一个问题的活。
    if let Some(open) = attempts.iter().find(|t| !t.status.is_terminal()) {
        return Decision::AlreadyMinted {
            task_id: open.id.clone(),
            open: true,
        };
    }

    // 全走完了。造下一条是因为这条越界还在，不是因为它值得再问一遍：同一条
    // 越界在队列上堆到第四条的时候，缺的是有人去看前三条为什么没交货。
    if attempts.len() >= MAX_ATTEMPTS {
        let died: Vec<String> = attempts
            .iter()
            .map(|t| format!("{}={:?}", t.id, t.status))
            .collect();
        return rejected(format!(
            "这一版基线（{revision}）上的重规划已经走过 {} 条都没交货（{}）：\
             再造一条只是把同一个问题再问一遍，先看它们为什么都走完了而基线没动",
            attempts.len(),
            died.join("、")
        ));
    }
    let attempt = attempts.len() as u32 + 1;

    // 挂在哪一行下面：这一组的根，而不是项目 id。
    //
    // 项目 id 是 goal 的标识，不是任何一条任务的 id——把它写进 parent_task_id，
    // 每条重规划都是一条父指针悬空的孤儿：范围树按父指针成树，读出来就是「父不在
    // 图里」，而那个标记是为分解缺陷准备的，从第一天起恒亮之后没人再看它。
    //
    // 根在这一组里恰好该有一条：`parent_task_id` 为空的那一行。零条说明这一组
    // 没有根（分解没交上来或者已被删），两条以上说明挑哪条都是猜——两种都拒绝，
    // 不造一条挂在空处的活。
    let root = match project_root(&breach.project, existing) {
        Ok(root) => root,
        Err(reason) => return rejected(reason),
    };

    let task_id = replan_id(&breach.project, breach.index, revision, attempt);
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
                "attempt": attempt,
                "observed_at": now.to_rfc3339(),
            },
        }),
    );
    // 归组读 `goal_id`，成树读 `parent_task_id`：前者让它进这个项目，后者让它
    // 挂在根下面。两格答的是两个问题，一个都不能省。
    task.goal_id = Some(breach.project.clone());
    task.parent_task_id = Some(root.to_string());
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

/// 这个项目组的根行：`goal_id` 是它、`parent_task_id` 为空的那一条。
///
/// 返回 `Err` 而不是猜一条：一个组的根是用来挂活的锚点，锚点有两个的时候，
/// 「挂在其中一个下面」与「挂在另一个下面」在范围树里是两棵不同的树。
fn project_root<'a>(project: &str, tasks: &'a [Task]) -> Result<&'a str, String> {
    let roots: Vec<&Task> = tasks
        .iter()
        .filter(|task| task.goal_id.as_deref() == Some(project) && task.parent_task_id.is_none())
        .collect();
    match roots.as_slice() {
        [root] => Ok(root.id.as_str()),
        [] => Err(format!(
            "项目 {project} 在任务图里没有根行（goal_id 指向它、parent_task_id 为空的那一条），\
             重规划无处可挂"
        )),
        many => Err(format!(
            "项目 {project} 在任务图里有 {} 条根行，挑哪一条都是猜，不造挂在其中一个下面的活",
            many.len()
        )),
    }
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

    /// 项目组的根行：`goal_id` 指向这个项目、自己头上没有父。重规划的父指针
    /// 必须落在它身上——项目 id 不是任何一条任务的 id。
    const ROOT_TASK: &str = "task-root-1";

    fn root_row() -> Task {
        let mut task = Task::new(
            ROOT_TASK,
            TaskType::Custom("goal".into()),
            serde_json::json!({}),
        );
        task.goal_id = Some(PROJECT.to_string());
        task
    }

    /// 一个项目组：根行，加上调用方给的那些行。
    fn group(rows: Vec<Task>) -> Vec<Task> {
        let mut all = vec![root_row()];
        all.extend(rows);
        all
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
        let task = minted(replan_task(&found, REV, &group(vec![]), at(0)));

        assert_eq!(task.goal_id.as_deref(), Some(PROJECT), "归进这个项目");
        assert_eq!(
            task.parent_task_id.as_deref(),
            Some(ROOT_TASK),
            "父指针落在这一组的根行上"
        );
        assert_ne!(
            task.parent_task_id.as_deref(),
            Some(PROJECT),
            "项目 id 是 goal 的标识，不是任何一条任务的 id——写进父指针就是一条孤儿"
        );
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
    fn the_same_breach_on_the_same_revision_mints_once_while_the_first_is_still_open() {
        let found = breach(PerformanceIndex::Cost, 0.5, 1_000, 2_000);
        let first = minted(replan_task(&found, REV, &group(vec![]), at(0)));

        let again = replan_task(&found, REV, &group(vec![first.clone()]), at(60));
        assert_eq!(
            again,
            Decision::AlreadyMinted {
                task_id: first.id.clone(),
                open: true,
            },
            "同一条还在待办里，就是还没交货——再问一遍不会多出新东西"
        );
    }

    /// 尝试走完了而基线还是那一版，说明它没交货：要造的是一条**新**活，而不是
    /// 把「它走完了」读成「这件事有人管了」。
    ///
    /// 两种终态都算没交货，因为判据不读它的状态而读基线：交了货的重规划会把基线
    /// 推到新的一版，那么下一次比较的根本不是这一版，这条重规划也就不会再被认
    /// 出来。还认得出它，就说明基线没动。
    #[test]
    fn an_attempt_that_ended_without_moving_the_baseline_gets_a_successor() {
        let found = breach(PerformanceIndex::Schedule, 0.5, 1_000, 2_000);
        let first = minted(replan_task(&found, REV, &group(vec![]), at(0)));

        for ended in [
            cog_core::TaskStatus::Completed,
            cog_core::TaskStatus::Failed,
        ] {
            let mut previous = first.clone();
            previous.status = ended;
            let successor = minted(replan_task(
                &found,
                REV,
                &group(vec![previous.clone()]),
                at(120),
            ));
            assert_ne!(
                successor.id, previous.id,
                "{ended:?} 之后再造还得是一条新活"
            );
            assert_eq!(
                successor.input[REPLAN_INPUT_KEY]["attempt"], 2,
                "第几次尝试写在输入里：读面要分得开这两条是同一处越界的两次追问"
            );
            assert_eq!(
                successor.input[REPLAN_INPUT_KEY]["baseline_rev"], REV,
                "还是那一版基线：去重键没变，变的只是尝试次数"
            );
        }
    }

    /// 三条都走完还是那一版基线，就不再造第四条：这时候缺的是有人去看前三条，
    /// 而不是队列上多一条注定一样的活。
    #[test]
    fn a_breach_that_outlived_its_attempts_is_refused_with_all_of_them_named() {
        let found = breach(PerformanceIndex::Cost, 0.5, 1_000, 2_000);
        let mut rows: Vec<Task> = Vec::new();
        // 依次造出三条：每一条都按「前一条已死」走到下一个尝试号。
        for attempt in 1..=MAX_ATTEMPTS {
            let next = minted(replan_task(
                &found,
                REV,
                &group(rows.clone()),
                at(attempt as i64),
            ));
            assert_eq!(next.input[REPLAN_INPUT_KEY]["attempt"], attempt as u64);
            let mut dead = next;
            dead.status = cog_core::TaskStatus::Failed;
            rows.push(dead);
        }
        match replan_task(&found, REV, &group(rows), at(600)) {
            Decision::Refused { reason } => {
                assert!(
                    reason.contains(&MAX_ATTEMPTS.to_string()),
                    "判词要说清走过几条：{reason}"
                );
                assert!(
                    reason.contains("Failed"),
                    "判词要点名它们各自的结局：{reason}"
                );
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// 版本动了而还在落后，那是新的事实：id 换一个，值得再问一次。
    #[test]
    fn a_new_baseline_revision_is_a_new_breach() {
        let found = breach(PerformanceIndex::Schedule, 0.7, 7_000, 10_000);
        let before = minted(replan_task(&found, REV, &group(vec![]), at(0)));
        let after = minted(replan_task(
            &found,
            "2026-10-27T09:00:00+00:00",
            &group(vec![before.clone()]),
            at(86_400),
        ));
        assert_ne!(before.id, after.id);
        // 两个 id 都只由（项目、指数、版本）决定，换个时刻问同一个问题答案一样。
        assert_eq!(
            after.id,
            replan_id(
                PROJECT,
                PerformanceIndex::Schedule,
                "2026-10-27T09:00:00+00:00",
                1
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
            let task = minted(replan_task(
                &breach(index, 0.5, 1, 2),
                REV,
                &group(vec![]),
                at(0),
            ));
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

    /// 一个组没有根行时不造活。造出来会是什么样：一条父指针指向空处的孤儿，
    /// 而范围树里「父不在图里」那个标记是为分解缺陷准备的——它从部署第一天起
    /// 恒亮之后，真正的分解缺陷就没人看得见了。
    #[test]
    fn a_group_without_a_root_row_is_refused_instead_of_minted_as_an_orphan() {
        let found = breach(PerformanceIndex::Schedule, 0.5, 1_000, 2_000);
        // 一条属于这个组、却也是一条后代的行：没有根，就找不到该挂的地方。
        let mut child = Task::new(
            "task-child",
            TaskType::Custom("work".into()),
            serde_json::json!({}),
        );
        child.goal_id = Some(PROJECT.to_string());
        child.parent_task_id = Some("task-gone".into());
        match replan_task(&found, REV, &[child], at(0)) {
            Decision::Refused { reason } => {
                assert!(reason.contains("没有根行"), "判词要说清是缺根：{reason}")
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// 两条根行也不造：挑哪一条都是猜，而两种挑法在范围树里是两棵不同的树。
    #[test]
    fn a_group_with_two_roots_is_refused_rather_than_guessing_one() {
        let found = breach(PerformanceIndex::Schedule, 0.5, 1_000, 2_000);
        let mut second = root_row();
        second.id = "task-root-2".into();
        match replan_task(&found, REV, &group(vec![second]), at(0)) {
            Decision::Refused { reason } => {
                assert!(reason.contains("2 条根行"), "判词要数出来有几条：{reason}")
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
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
            match replan_task(&found, REV, &group(vec![]), at(0)) {
                Decision::Refused { reason } => assert!(
                    reason.contains(pinch),
                    "判词要说清是哪一件不成立（找 {pinch}）：{reason}"
                ),
                other => panic!("expected a refusal for {found:?}, got {other:?}"),
            }
        }
    }
}

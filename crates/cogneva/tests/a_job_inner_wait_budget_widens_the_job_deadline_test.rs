//! principle: feedback_a_wait_budget_inside_the_job_must_also_widen_the_job_deadline
//! Gate: every `CI_*_BUDGET_SECS` / `CI_*_WAIT_SECS` bound in the rollout module is added into the dispatcher's wait budget, and the rollout Job carries no deadline of its own.
//!
//! 一次带墙钟上界的等待跑在 Job 里，这个界有两个头：里层是等待本身，外层是**派发方
//! 愿意等多久**（`judgement_wait_budget`）。只封里层的那一刻没有症状——代码照常跑，
//! 测试照常绿，只有慢上游出现时才出事：派发方先走，判词被推到下一轮，而下一轮是
//! 十分钟之后。
//!
//! 2026-10-23 实测：部署器给每个目标补第三次 CI 提问，`CI_ASK_BUDGET_SECS = 180` 包住
//! 了整段提问，而等待预算原本只按「每目标 startup + rollout」算。
//!
//! 那时外层还有第二头：Job 的 `activeDeadlineSeconds`。它已经拆掉了——一个按总和
//! 算出来的钟分不出「滚动慢了」与「滚动卡住」，而它开枪的后果是集群停在半滚状态、
//! Job 自己的回滚也走不到。现在判停是「还在不在推进」：进程每一步写
//! `COGNEVA_PROGRESS_FILE`，容器上的存活探针读文件年龄与文件里那个窗口。所以这条门
//! 现在判两件事：
//!
//! 1. 每个这样的常量都出现在派发方的等待预算（`judgement_wait_budget`）里；
//! 2. Job 清单里没有 `activeDeadlineSeconds`，并且带着读进度文件的存活探针——
//!    期限回来了、或者探针不见了，都是「按钟杀掉还在干活的容器」这个形状重新长回来。
//!
//! 为什么按**名字的形式**取人口：`CI_*_BUDGET_SECS` / `CI_*_WAIT_SECS` 是这类界的命名
//! 约定，新加一段等待时照着它命名就自动进这一格。今天匹配上的是 `CI_ASK_BUDGET_SECS`
//! 与 `CI_PENDING_WAIT_SECS`；`CI_PENDING_REASK_SECS` 不匹配，因为它是复问的间隔，不是
//! 一整段预算——它由 `CI_PENDING_WAIT_SECS` 那个总数封顶，本身不该进预算公式。
//!
//! 这条门判不到的地方：它读的是源码文本，界若通过函数参数、宏或配置结构从别处传进来，
//! 这里没有名字可找。所以每个常量都要求**被找到**——名字改了、常量删了都会红（那是
//! 「什么都没判」，不是「没有问题」），需要人回来把这一格改成新的形状。
//!
//! 这扇门跑在 `cargo test` 里，所以它在带内也生效：集群自己提交的变更同样要过它。

use crate::common;

use common::routes::{read, repo_root};
use common::rust_production::production_lines;

const ROLLOUT: &str = "crates/cog-reflection/src/mainline_rollout.rs";
const DEPLOYER: &str = "crates/cog-reflection/src/mainline_deployer.rs";

/// 这一族界的名字形式。两段而不是一个前缀：`BUDGET`（一次动作愿意等多久）与 `WAIT`
/// （等一个结果出来），两种都跑在 Job 里，都会被外层的等待预算挤掉。
fn is_job_inner_budget(name: &str) -> bool {
    name.starts_with("CI_") && (name.ends_with("_BUDGET_SECS") || name.ends_with("_WAIT_SECS"))
}

/// 声明在给定源文本里的这类常量名。
fn declared_budgets(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in production_lines(text) {
        let Some(pos) = line.find("const ") else {
            continue;
        };
        let rest = &line[pos + "const ".len()..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if is_job_inner_budget(&name) {
            out.push(name);
        }
    }
    out
}

/// 派发方那一段期限计算的正文（注释已剔掉：那段注释里就写着这些常量名，按裸子串
/// 判会让「从这里删掉」看起来像「还在这里」）。
fn wait_budget_body(text: &str) -> Option<String> {
    let lines: Vec<&str> = production_lines(text);
    let start = lines
        .iter()
        .position(|l| l.contains("fn judgement_wait_budget("))?;
    let mut body = String::new();
    for line in &lines[start..] {
        body.push_str(line);
        body.push('\n');
        if line.trim_end() == "}" {
            break;
        }
    }
    Some(body)
}

/// 哪些常量没有出现在这一段计算里。
fn missing_from(body: &str, budgets: &[String]) -> Vec<String> {
    budgets
        .iter()
        .filter(|name| !body.contains(name.as_str()))
        .cloned()
        .collect()
}

#[test]
fn every_job_inner_budget_is_added_into_the_dispatchers_wait_budget() {
    let root = repo_root();
    let budgets = declared_budgets(&read(&root.join(ROLLOUT)));
    assert!(
        !budgets.is_empty(),
        "{ROLLOUT} 里一个 CI_*_BUDGET_SECS / CI_*_WAIT_SECS 都没读到——这是「什么都没判」，不是「都在期限里」"
    );

    let body = wait_budget_body(&read(&root.join(DEPLOYER))).unwrap_or_else(|| {
        panic!("{DEPLOYER} 里找不到 judgement_wait_budget 那一处期限计算——判据的锚变了，这一格要跟着改")
    });

    let missing = missing_from(&body, &budgets);
    assert!(
        missing.is_empty(),
        "这些 Job 内的等待界没有进派发方的等待预算：{missing:?}\n\
         只封里层时，慢上游会让判词被推到下一轮，而下一轮是十分钟之后"
    );
}

/// Job 不再带一个到点杀容器的钟，判停由容器自己的存活探针按进度文件做。
///
/// 这条测试盯住的正是那个替换：一个 `activeDeadlineSeconds` 回来了、或者探针
/// 不见了，都是「按钟杀掉还在干活的容器」这个形状重新长回来。它读生产行，不读
/// 注释——清单上方那段注释里写着这个名字，按裸子串判会让「删掉了」读成「还在」。
#[test]
fn the_rollout_job_is_stopped_by_its_own_progress_not_by_a_deadline() {
    let root = repo_root();
    let text = read(&root.join(DEPLOYER));
    let production = production_lines(&text).join("\n");
    assert!(
        !production.contains("\"activeDeadlineSeconds\""),
        "{DEPLOYER} 里又出现了 activeDeadlineSeconds：它到点杀容器，不问那容器还在不在推进"
    );
    assert!(
        production.contains("cog_core::progress::liveness_probe()"),
        "{DEPLOYER} 的滚动 Job 没有读进度文件的存活探针——没有它，Job 上也没有期限，一个真卡住的滚动就没人收尾了"
    );
    assert!(
        production.contains("cog_core::progress::PROGRESS_FILE_ENV"),
        "{DEPLOYER} 的滚动 Job 没有把进度文件路径交给容器，探针读的是一条空路径"
    );
}

#[test]
fn the_scan_names_the_bound_it_compares() {
    // 人口不是空的，而且每个名字都真的在那一份源里：名字改掉而不改这一格时，
    // 上面那条会从「没有违例」变成「什么都没判」，两者在外面同形。
    let root = repo_root();
    let text = read(&root.join(ROLLOUT));
    let budgets = declared_budgets(&text);
    assert!(
        budgets.iter().any(|b| b == "CI_ASK_BUDGET_SECS"),
        "这一族里没有 CI_ASK_BUDGET_SECS：读到的是 {budgets:?}"
    );
    for name in &budgets {
        assert!(
            text.contains(name.as_str()),
            "{name} 被读出来却不在源里，扫描器读错了"
        );
    }
}

#[test]
fn a_budget_missing_from_the_computation_is_reported() {
    let budgets = vec!["CI_ASK_BUDGET_SECS".to_string()];
    assert!(
        missing_from("fn judgement_wait_budget() {\n  1 + CI_ASK_BUDGET_SECS\n}\n", &budgets)
            .is_empty(),
        "在里面时不该报"
    );
    assert_eq!(
        missing_from("fn judgement_wait_budget() {\n  1\n}\n", &budgets),
        budgets,
        "不见时必须点名"
    );
}

#[test]
fn a_comment_that_names_the_budget_does_not_count_as_using_it() {
    // 那段代码上方有一段注释点名这些常量。判据读的是代码行，注释里的名字
    // 不该把「删掉了」读成「还在」。
    let text = "\
// 这段注释里写着 CI_ASK_BUDGET_SECS，但它只是一行说明
fn judgement_wait_budget() {
    1
}
";
    let body = wait_budget_body(text).expect("切成一段");
    assert_eq!(
        missing_from(&body, &["CI_ASK_BUDGET_SECS".to_string()]),
        vec!["CI_ASK_BUDGET_SECS".to_string()],
        "注释里的名字被当成了使用"
    );
}

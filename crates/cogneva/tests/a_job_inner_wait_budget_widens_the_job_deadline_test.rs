//! principle: feedback_a_wait_budget_inside_the_job_must_also_widen_the_job_deadline
//! Gate: every `CI_*_BUDGET_SECS` / `CI_*_WAIT_SECS` bound in the rollout module is added into both deadline computations in the deployer.
//!
//! 一次带墙钟上界的等待跑在 Job 里，这个界就有两个头：里层是等待本身，外层是 Job 的
//! `activeDeadlineSeconds`。只封里层的那一刻没有症状——代码照常跑，测试照常绿，只有慢
//! 上游出现时才出事，而那时的形状是 Job 被杀（`Killed`），读起来像「判定进程自己没跑完」，
//! 没人会想到是新增的那一段吃掉了滚动预算。
//!
//! 2026-10-23 实测：部署器给每个目标补第三次 CI 提问，`CI_ASK_BUDGET_SECS = 180` 包住了
//! 整段提问（超时按读不到放行），而 Job 的期限原本只按「每目标 startup + rollout」算。
//!
//! 判据是一条**包含**关系，两条都判：
//!
//! 1. 每个这样的常量都出现在「这一轮等多久」（`judgement_wait_budget`）里；
//! 2. 也都出现在 Job 期限那一段表达式里（`activeDeadlineSeconds` 到
//!    `ttlSecondsAfterFinished` 之间）。
//!
//! 为什么是两个地方而不是一个：两处算的是不同的东西——一处是部署器自己愿意等多久，
//! 一处是 Job 活多久。第一处漏了会让判词被推到下一轮，第二处漏了会让 Job 中途被杀。
//! 两种失败在读数面上长得完全不同，所以不合并成一个「出现了吗」。
//!
//! 为什么按**名字的形式**取人口：`CI_*_BUDGET_SECS` / `CI_*_WAIT_SECS` 是这类界的命名
//! 约定，新加一段等待时照着它命名就自动进这一格。今天匹配上的是 `CI_ASK_BUDGET_SECS`
//! 与 `CI_PENDING_WAIT_SECS`；`CI_PENDING_REASK_SECS` 不匹配，因为它是复问的间隔，不是
//! 一整段预算——它由 `CI_PENDING_WAIT_SECS` 那个总数封顶，本身不该进期限公式。
//!
//! 这条门判不到的地方：它读的是源码文本，界若通过函数参数、宏或配置结构从别处传进来，
//! 这里没有名字可找。所以每个常量都要求**被找到**——名字改了、常量删了都会红（那是
//! 「什么都没判」，不是「没有问题」），需要人回来把这一格改成新的形状。
//!
//! 这扇门跑在 `cargo test` 里，所以它在带内也生效：集群自己提交的变更同样要过它。

use std::path::Path;

use crate::common;

use common::routes::repo_root;
use common::rust_production::production_lines;

const ROLLOUT: &str = "crates/cog-reflection/src/mainline_rollout.rs";
const DEPLOYER: &str = "crates/cog-reflection/src/mainline_deployer.rs";

/// 这一族界的名字形式。两段而不是一个前缀：`BUDGET`（一次动作愿意等多久）与 `WAIT`
/// （等一个结果出来），两种都跑在 Job 里，都会被外层期限挤掉。
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

/// 部署器里两处期限计算各自的正文（注释已剔掉：这两段的注释里就写着这几个常量名，
/// 按裸子串判会让「从这里删掉」看起来像「还在这里」）。
fn deadline_terms(text: &str) -> Vec<(&'static str, String)> {
    let lines: Vec<&str> = production_lines(text);
    let mut terms = Vec::new();

    // 一处：`fn judgement_wait_budget(...)` 到下一个顶格的 `}`。
    if let Some(start) = lines
        .iter()
        .position(|l| l.contains("fn judgement_wait_budget("))
    {
        let mut body = String::new();
        for line in &lines[start..] {
            body.push_str(line);
            body.push('\n');
            if line.trim_end() == "}" {
                break;
            }
        }
        terms.push(("judgement_wait_budget", body));
    }

    // 另一处：Job 清单里 `"activeDeadlineSeconds":` 那一项，到 `"ttlSecondsAfterFinished"`。
    if let Some(start) = lines
        .iter()
        .position(|l| l.contains("\"activeDeadlineSeconds\":"))
    {
        let mut body = String::new();
        for line in &lines[start..] {
            body.push_str(line);
            body.push('\n');
            if line.contains("\"ttlSecondsAfterFinished\"") {
                break;
            }
        }
        terms.push(("activeDeadlineSeconds", body));
    }

    terms
}

/// 哪些常量没有出现在哪些计算里。
fn missing_from(terms: &[(&'static str, String)], budgets: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (label, body) in terms {
        for name in budgets {
            if !body.contains(name.as_str()) {
                out.push((name.clone(), (*label).to_string()));
            }
        }
    }
    out
}

fn source(root: &Path, rel: &str) -> String {
    let path = root.join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读不到 {}：{e}", path.display()))
}

#[test]
fn every_job_inner_budget_is_added_into_both_deadline_computations() {
    let root = repo_root();
    let budgets = declared_budgets(&source(&root, ROLLOUT));
    assert!(
        !budgets.is_empty(),
        "{ROLLOUT} 里一个 CI_*_BUDGET_SECS / CI_*_WAIT_SECS 都没读到——这是「什么都没判」，不是「都在期限里」"
    );

    let terms = deadline_terms(&source(&root, DEPLOYER));
    let labels: Vec<&str> = terms.iter().map(|(l, _)| *l).collect();
    for want in ["judgement_wait_budget", "activeDeadlineSeconds"] {
        assert!(
            labels.contains(&want),
            "{DEPLOYER} 里找不到 {want} 那一处期限计算（读到的是 {labels:?}）——判据的锚变了，这一格要跟着改"
        );
    }

    let missing = missing_from(&terms, &budgets);
    assert!(
        missing.is_empty(),
        "这些 Job 内的等待界没有进期限计算：{missing:?}\n\
         只封里层时，慢上游会把滚动预算挤掉，Job 到点被杀——而那在部署器那边读起来\n\
         像「判定进程没跑完」，看不出是哪一段"
    );
}

#[test]
fn the_scan_names_the_bound_it_compares() {
    // 人口不是空的，而且每个名字都真的在那一份源里：名字改掉而不改这一格时，
    // 上面那条会从「没有违例」变成「什么都没判」，两者在外面同形。
    let root = repo_root();
    let text = source(&root, ROLLOUT);
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
fn a_budget_missing_from_one_computation_is_reported() {
    let budgets = vec!["CI_ASK_BUDGET_SECS".to_string()];
    let both = vec![
        (
            "judgement_wait_budget",
            "fn judgement_wait_budget() {\n  1 + CI_ASK_BUDGET_SECS\n}\n".to_string(),
        ),
        (
            "activeDeadlineSeconds",
            "    \"activeDeadlineSeconds\": CI_ASK_BUDGET_SECS,\n".to_string(),
        ),
    ];
    assert!(missing_from(&both, &budgets).is_empty(), "两处都有时不该报");

    let only_inner = vec![
        (
            "judgement_wait_budget",
            "fn judgement_wait_budget() {\n  1 + CI_ASK_BUDGET_SECS\n}\n".to_string(),
        ),
        (
            "activeDeadlineSeconds",
            "    \"activeDeadlineSeconds\": 600,\n".to_string(),
        ),
    ];
    assert_eq!(
        missing_from(&only_inner, &budgets),
        vec![(
            "CI_ASK_BUDGET_SECS".to_string(),
            "activeDeadlineSeconds".to_string()
        )],
        "只封了里层时必须点名外层那一处"
    );
}

#[test]
fn a_comment_that_names_the_budget_does_not_count_as_using_it() {
    // 部署器那两处上方各有一段注释点名这些常量。判据读的是代码行，注释里的名字
    // 不该把「删掉了」读成「还在」。
    let text = "\
// 这段注释里写着 CI_ASK_BUDGET_SECS，但它只是一行说明
fn judgement_wait_budget() {
    1
}
";
    assert!(
        missing_from(&deadline_terms(text), &["CI_ASK_BUDGET_SECS".to_string()])
            .iter()
            .any(|(name, label)| name == "CI_ASK_BUDGET_SECS" && label == "judgement_wait_budget"),
        "注释里的名字被当成了使用"
    );
}

#[test]
fn the_reader_takes_the_whole_expression_and_not_the_first_literal() {
    let text = "\
        \"activeDeadlineSeconds\": (a + b) * n
            + crate::mainline_rollout::CI_ASK_BUDGET_SECS
            + crate::mainline_rollout::CI_PENDING_WAIT_SECS,
        \"ttlSecondsAfterFinished\": 86400,
";
    let terms = deadline_terms(text);
    assert_eq!(terms.len(), 1, "应当只切出一处");
    let budgets = vec![
        "CI_ASK_BUDGET_SECS".to_string(),
        "CI_PENDING_WAIT_SECS".to_string(),
    ];
    assert!(
        missing_from(&terms, &budgets).is_empty(),
        "整段表达式里的两个常量都要认出来：{:?}",
        terms[0].1
    );
}

//! principle: feedback_a_fixture_that_runs_the_expression_but_not_the_comparison_tests_the_wrong_evaluator
//! Gate: every `AlertCondition` variant has an operator the promtool fixture runner folds into the expression, and the runner has no operator the enum lacks.
//!
//! 进程内的规则是两段：`promql` 取数，`condition` 再对每个序列的值比一次。Prometheus 的告警
//! 规则只有一段——表达式返回元素就点火。把 `promql` 单独交给 promtool，测到的是**另一台求值器**：
//! 阈值改成十亿它照样绿。修法是把条件折进表达式（`greater_than` → `(expr) > th`，`equal` 那种
//! 还要按进程的 `EPSILON` 语义逐字对齐），于是 promtool 的「响不响」与进程的「响不响」才同一个
//! 判据。
//!
//! 这条门守的是那个别名表的**人口**：`AlertCondition` 加一个变体、而夹具跑者不认识它时，今天
//! 只有等某条规则真的用了那个算子才会红（跑者遇不认识的行子是 `sys.exit`）。在那之前，一份
//! 「全绿」的夹具读数会被读成「这一族都证明过了」。
//!
//! 两个方向都判：枚举有、别名表没有（新算子没接上），别名表有、枚举没有（表停在旧名字上，
//! 多出来的那几行永远不会被执行，也就永远不会被证伪）。另外每一格都要求**读到了东西**——
//! 解析读空与「两边都齐」在输出上同形，所以空人口自己先红。
//!
//! 这扇门判不到「折得对不对」：把 `>` 写成 `>=` 它看不出来。那一半在跑者自己的夹具里
//! （`deploy/scripts/tests/cogneva-json-alert-rules.test.sh` 的「只把阈值抬到十亿」那一格，
//! 以及不认识的算子必须非零那一格）。

use crate::common;

use common::routes::{read, repo_root};
use common::rust_production::production_text;

const ALERTS_RS: &str = "crates/cog-core/src/contract/alerts.rs";
const RUNNER: &str = "deploy/scripts/test-acceptance-census-alert-rules.sh";

/// `GreaterThanOrEqual` → `greater_than_or_equal`：跑者那份别名表用的就是这个名字。
fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.char_indices() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// 枚举变体的名字。只看 `pub enum AlertCondition { … }` 那一段，行尾带 `(f64)` 或裸名都认。
fn condition_variants(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut inside = false;
    let mut depth = 0i32;
    for line in text.lines() {
        if !inside {
            if line.contains("pub enum AlertCondition") {
                inside = true;
                depth += line.matches('{').count() as i32 - line.matches('}').count() as i32;
            }
            continue;
        }
        let t = line.trim();
        if t.starts_with('}') && depth <= 1 {
            break;
        }
        let name: String = t
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() && name.chars().next().is_some_and(|c| c.is_uppercase()) {
            out.push(name);
        }
    }
    out
}

/// 跑者那份 `OPERATORS = { … }` 里的键。
fn operator_keys(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if !inside {
            if line.trim_start().starts_with("OPERATORS = {") {
                inside = true;
            }
            continue;
        }
        let t = line.trim();
        if t.starts_with('}') {
            break;
        }
        if t.starts_with('#') {
            continue;
        }
        if let Some(start) = t.find('"') {
            if let Some(end) = t[start + 1..].find('"') {
                out.push(t[start + 1..start + 1 + end].to_string());
            }
        }
    }
    out
}

/// 枚举有、别名表没有的那些变体名。
fn unshaped_variants(variants: &[String], keys: &[String]) -> Vec<String> {
    variants
        .iter()
        .map(|v| snake_case(v))
        .filter(|v| !keys.iter().any(|k| k == v))
        .collect()
}

/// 别名表有、枚举没有的那些键。
fn orphan_keys(variants: &[String], keys: &[String]) -> Vec<String> {
    let shaped: Vec<String> = variants.iter().map(|v| snake_case(v)).collect();
    keys.iter()
        .filter(|k| !shaped.iter().any(|v| v == *k))
        .cloned()
        .collect()
}

#[test]
fn every_condition_variant_has_a_shape_in_the_fixture_runner() {
    let root = repo_root();
    let variants = condition_variants(&production_text(&read(&root.join(ALERTS_RS))));
    assert!(
        !variants.is_empty(),
        "{ALERTS_RS} 里一个 AlertCondition 变体都没读到——这是「什么都没判」，不是「都接上了」"
    );
    let keys = operator_keys(&read(&root.join(RUNNER)));
    assert!(
        !keys.is_empty(),
        "{RUNNER} 里一个算子别名都没读到——判据的锚变了（表名或写法改了），这一格要跟着改"
    );

    let missing = unshaped_variants(&variants, &keys);
    assert!(
        missing.is_empty(),
        "这些 AlertCondition 变体在夹具跑者里没有 PromQL 形状：{missing:?}\n\
         加一个变体而跑者不认识它时，夹具要等某条规则真的用了那个算子才会红——在那之前\n\
         它的「全绿」会被读成「这一族都证明过了」"
    );

    let orphans = orphan_keys(&variants, &keys);
    assert!(
        orphans.is_empty(),
        "夹具跑者里有枚举中不存在的算子：{orphans:?}（表停在旧名字上，那几行永远不会被执行）"
    );
}

#[test]
fn the_readers_see_the_population_they_compare() {
    let root = repo_root();
    let variants = condition_variants(&production_text(&read(&root.join(ALERTS_RS))));
    for want in [
        "GreaterThan",
        "LessThan",
        "Equal",
        "NotEqual",
        "GreaterThanOrEqual",
        "LessThanOrEqual",
    ] {
        assert!(
            variants.iter().any(|v| v == want),
            "读不出变体 {want}（读到的是 {variants:?}）"
        );
    }
    assert_eq!(snake_case("GreaterThanOrEqual"), "greater_than_or_equal");
}

#[test]
fn a_variant_without_a_shape_is_reported() {
    let variants = vec!["GreaterThan".to_string(), "Within".to_string()];
    let keys = vec!["greater_than".to_string()];
    assert_eq!(
        unshaped_variants(&variants, &keys),
        vec!["within".to_string()]
    );
    assert!(orphan_keys(&variants, &keys).is_empty());
}

#[test]
fn a_shape_without_a_variant_is_reported() {
    let variants = vec!["GreaterThan".to_string()];
    let keys = vec!["greater_than".to_string(), "occasionally".to_string()];
    assert!(unshaped_variants(&variants, &keys).is_empty());
    assert_eq!(
        orphan_keys(&variants, &keys),
        vec!["occasionally".to_string()]
    );
}

#[test]
fn a_commented_out_shape_does_not_count() {
    let runner = "\
OPERATORS = {
    \"greater_than\": (\"{p} > {th}\"),
    # \"less_than\": (\"{p} < {th}\"),
}
";
    assert_eq!(operator_keys(runner), vec!["greater_than".to_string()]);
}

#[test]
fn the_enum_reader_stops_at_the_end_of_the_enum() {
    let src = "\
pub enum AlertCondition {
    GreaterThan(f64),
    LessThan(f64),
}

pub enum SomethingElse {
    NotACondition,
}
";
    assert_eq!(
        condition_variants(src),
        vec!["GreaterThan".to_string(), "LessThan".to_string()]
    );
}

//! principle: feedback_no_hardcoding_config_surface
//! The alert rules select our own series down to exact label values:
//! `outcome="unrepairable"`, `state="granted"`, `readout="runs_failed"`,
//! `kind="quota_cycle"`. Each of those values is a string in the rule document
//! and a string in the code that emits the series, and nothing compared the
//! two.
//!
//! The failure is silent, and it is the empty reading again: a value renamed on
//! the emitter leaves a selector that matches no series. PromQL does not fail on
//! that — the vector comes back empty, the rule evaluates over nothing and
//! reports nothing, and "this rule is not firing" is the same reading as "none
//! of these happened". A rule pinned to last quarter's spelling of a word keeps
//! its name, its summary and its place in the alert list while it watches a
//! series that no longer exists.
//!
//! The sibling gate `deployed_rule_labels_are_labels_the_code_emits_test.rs`
//! holds the label *names*; this one holds the *values*. The alert rules are
//! what it reads: the board no longer spells a query of its own — it is
//! computed from the closed set of series and the reading registry, so its
//! panels draw series that exist by construction rather than by a label value
//! somebody typed into a manifest.
//! What it judges:
//!
//! * every exact value a query selects on one of our own series is a string that
//!   still exists as a literal somewhere in `crates/*/src`.
//!
//! What it cannot judge:
//!
//! * which label the value is written under. The value and the label name are
//!   often assembled in different files (the value in the module that knows the
//!   vocabulary, the label name in the metric helper), so requiring them side by
//!   side would redden on correct code. A value moved to another label passes
//!   here and is caught by nothing;
//! * a value that survives only in a test fixture. Fixtures are separated out:
//!   the census reads the production half of each file, so a value that a rule
//!   and no shipped code still spell is reported. The split is per item rather
//!   than "everything before the first `#[cfg(test)]`", which would leave the
//!   production tail of every file that carries an inline test module unread;
//! * a query that selects a value this test's selector parser fails to see. That
//!   is why the extracted set is asserted to be large and to contain values
//!   known to be in the documents.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, sources_of_tree};

/// The document an operator reads our series through, and the JSON key it holds
/// a query under.
///
/// The hand-written dashboard used to be the second document. It is gone: the
/// board is computed from the closed set of series and the reading registry, so
/// its panels draw series that exist by construction and hold no label value of
/// their own for this gate to compare. What decides on a label value now is a
/// rule, and the rule file is what is left to read.
const DOCUMENTS: &[(&str, &str)] = &[("deploy/helm/cogneva/files/cogneva.json", "promql")];

/// Metric prefixes that belong to another exporter. A selector on `kube_pod_*`
/// names a vocabulary this tree does not write, and holding it to this tree
/// would be a census of somebody else's words.
/// `helm_` is the upgrade-path reading: a host script outside this repository
/// publishes it through the aggregator, so its label values are not this
/// tree's to hold. The alert-rule contract test owns the inventory of series
/// this workspace does not publish.
const FOREIGN_METRIC_PREFIXES: &[&str] = &[
    "kube_",
    "node_",
    "kubelet_",
    "container_",
    "process_",
    "helm_",
];

/// The whole-metric exceptions to the prefixes above.
const FOREIGN_METRICS: &[&str] = &["up"];

/// One `label="value"` inside a series selector.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Selection {
    metric: String,
    label: String,
    value: String,
}

/// Every query string held under `key` in the document's text.
///
/// Read as text rather than as a parsed document: the queries are JSON strings
/// with escaped inner quotes (`\"`), and scanning for the key keeps the whole
/// rule file's shape out of this gate.
fn expressions_under(text: &str, key: &str) -> Vec<String> {
    let needle = format!("\"{key}\"");
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(&needle) {
        let mut i = from + at + needle.len();
        from = i;
        while i < bytes.len() && (bytes[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            continue;
        }
        i += 1;
        while i < bytes.len() && (bytes[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'"' {
            continue;
        }
        let start = i + 1;
        let mut end = start;
        while end < bytes.len() {
            if bytes[end] == b'\\' {
                end += 2;
                continue;
            }
            if bytes[end] == b'"' {
                break;
            }
            end += 1;
        }
        if end >= bytes.len() {
            break;
        }
        out.push(text[start..end].replace("\\\"", "\""));
        from = end + 1;
    }
    out
}

fn is_metric_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

/// The exact values an expression selects, one entry per `(metric, label)`.
///
/// A regex alternation (`outcome=~"read|absent"`) is a set of exact values and
/// is split; an alternative carrying regex structure (`.*`, `.+`, a character
/// class) names a pattern rather than a word and is skipped — it cannot be
/// compared to a literal.
fn selections(expression: &str) -> Vec<Selection> {
    let chars: Vec<char> = expression.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '{' {
            i += 1;
            continue;
        }
        // The metric name is what runs back from the brace.
        let mut start = i;
        while start > 0 && is_metric_char(chars[start - 1]) {
            start -= 1;
        }
        let metric: String = chars[start..i].iter().collect();
        let mut end = i + 1;
        while end < chars.len() && chars[end] != '}' {
            end += 1;
        }
        if metric.is_empty() || end >= chars.len() {
            i += 1;
            continue;
        }
        let body: String = chars[i + 1..end].iter().collect();
        for item in body.split(',') {
            let item = item.trim();
            // `!=` / `!~` exclude a value; a value that no series carries is
            // what they want, so they are not part of this comparison.
            if item.contains("!=") || item.contains("!~") {
                continue;
            }
            let Some(at) = item.find('=') else {
                continue;
            };
            let label = item[..at].trim();
            let rest = item[at + 1..].trim_start();
            let (rest, regex) = match rest.strip_prefix('~') {
                Some(after) => (after.trim_start(), true),
                None => (rest, false),
            };
            let Some(rest) = rest.strip_prefix('"').and_then(|r| r.strip_suffix('"')) else {
                continue;
            };
            let alternatives: Vec<&str> = if regex {
                rest.split('|').collect()
            } else {
                vec![rest]
            };
            for value in alternatives {
                if value.is_empty()
                    || value.chars().all(|c| c.is_ascii_digit())
                    || value.chars().any(|c| {
                        matches!(
                            c,
                            '.' | '*'
                                | '+'
                                | '['
                                | ']'
                                | '('
                                | ')'
                                | '{'
                                | '}'
                                | '\\'
                                | '^'
                                | '$'
                                | '?'
                        )
                    })
                {
                    continue;
                }
                out.push(Selection {
                    metric: metric.clone(),
                    label: label.to_string(),
                    value: value.to_string(),
                });
            }
        }
        i = end + 1;
    }
    out
}

fn ours(selection: &Selection) -> bool {
    !FOREIGN_METRICS.contains(&selection.metric.as_str())
        && !FOREIGN_METRIC_PREFIXES
            .iter()
            .any(|prefix| selection.metric.starts_with(prefix))
}

/// The values the rules select on, and the ones no file under `crates/*/src`
/// spells any more.
fn wanted_and_missing(root: &Path, files: &[PathBuf]) -> (BTreeSet<Selection>, Vec<Selection>) {
    let mut wanted: BTreeSet<Selection> = BTreeSet::new();
    for (file, key) in DOCUMENTS {
        let path = root.join(file);
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let expressions = expressions_under(&text, key);
        assert!(
            !expressions.is_empty(),
            "{file} 里一个 `{key}` 都没读到：文档搬了家或键改了名，这一条会在空集上假装绿"
        );
        for expression in &expressions {
            wanted.extend(selections(expression).into_iter().filter(ours));
        }
    }

    // The production half of each file: a word that survives only in a fixture
    // is not a word the series is written with, and reading the fixtures as
    // well would let a renamed emitter pass on the strength of its own tests.
    let sources: Vec<String> = files
        .iter()
        .map(|path| {
            let text =
                std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            production_lines(&text).join("\n")
        })
        .collect();

    let missing: Vec<Selection> = wanted
        .iter()
        .filter(|selection| {
            let needle = format!("\"{}\"", selection.value);
            !sources.iter().any(|text| text.contains(&needle))
        })
        .cloned()
        .collect();
    (wanted, missing)
}

#[test]
fn every_value_a_rule_selects_on_is_a_value_the_tree_writes() {
    let root = repo_root();
    let (wanted, missing) = wanted_and_missing(&root, &production_sources(&root));
    // 抽取面自检：解析读空了的时候，下面那条判据会在空集上假装绿。
    assert!(
        wanted.len() >= 150,
        "只从规则里读到 {} 个 (series, label, value) 三元组，抽取面本身坏了：{wanted:?}",
        wanted.len()
    );
    for known in [
        (
            "cogneva_volume_capacity_outcome_total",
            "outcome",
            "refused",
        ),
        ("cogneva_volume_capacity_bytes", "state", "granted"),
        ("cogneva_task_checkpoint_total", "outcome", "unsuperseded"),
    ] {
        let selection = Selection {
            metric: known.0.to_string(),
            label: known.1.to_string(),
            value: known.2.to_string(),
        };
        assert!(
            wanted.contains(&selection),
            "规则里读不到 {known:?}：选择器解析坏了"
        );
    }

    assert!(
        missing.is_empty(),
        "部署里的规则按这些标签值挑选我们自己的 series，树里却已经找不到这些字面量：\n{}\n\
         值在产出端改了名，选择器就落在一个不存在的 series 上：PromQL 不报错，\
         向量为空、规则在空集上求值，报出来的是「没有告警」——与「没有发生」同形",
        missing
            .iter()
            .map(|s| format!("  {}{{{}=\"{}\"}}", s.metric, s.label, s.value))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// 判据自己也要有判据：一棵小树里写了一个值、没写另一个，判出来的必须正是
/// 没写的那个。少了这条，「永远返回空」的实现与「树是干净的」不可分。
#[test]
fn a_value_no_source_spells_is_reported_missing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    let rules = root.join("deploy/helm/cogneva/files");
    std::fs::create_dir_all(&rules).expect("rules dir");
    std::fs::write(
        rules.join("cogneva.json"),
        serde_json::json!({
            "groups": [{
                "rules": [
                    { "promql": "max(example_thing{outcome=\"written\"}) > 0" },
                    { "promql": "max(example_thing{outcome=\"renamed\"}) > 0" }
                ]
            }]
        })
        .to_string(),
    )
    .expect("rules");
    let src = root.join("crates/cog-example/src");
    std::fs::create_dir_all(&src).expect("src dir");
    // `renamed` appears only inside a test module: the emitter was renamed, the
    // fixture still says the old word. Reading fixtures as production would
    // call this clean on the strength of the very test that outlived the code.
    std::fs::write(
        src.join("lib.rs"),
        "fn label() -> &'static str { \"written\" }\n\
         \n\
         #[cfg(test)]\n\
         mod tests {\n\
             fn old_word() -> &'static str { \"renamed\" }\n\
         }\n",
    )
    .expect("source");

    let (wanted, missing) = wanted_and_missing(root, &sources_of_tree(root));
    // 两个选择器、两个取值，`written` 只出现一次。
    assert_eq!(wanted.len(), 2, "{wanted:?}");
    assert_eq!(
        missing,
        vec![Selection {
            metric: "example_thing".into(),
            label: "outcome".into(),
            value: "renamed".into(),
        }]
    );
}

/// 解析面自己也要有判据，否则「读到空集」与「没有选择器」不可分。
#[test]
fn the_selector_parser_reads_exact_values_and_skips_patterns() {
    let exact = selections("max(cogneva_volume_capacity_bytes{state=\"granted\"}) > 0");
    assert_eq!(
        exact,
        vec![Selection {
            metric: "cogneva_volume_capacity_bytes".into(),
            label: "state".into(),
            value: "granted".into(),
        }]
    );

    // 正则的每个分支都是一个确切值；带正则结构的那个不是词，跳过。
    let alternation =
        selections("sum(llm_usage_readings_total{outcome=~\"read|absent|not_asked\"})");
    let values: Vec<&str> = alternation.iter().map(|s| s.value.as_str()).collect();
    assert_eq!(values, vec!["read", "absent", "not_asked"]);
    assert!(selections("x{a=~\".*cogneva.*\"}").is_empty());

    // 别人的 series 不是本仓库的词汇。
    let foreign = selections("kube_pod_container_status_restarts_total{container=\"redis\"}");
    assert!(foreign.iter().all(|s| !ours(s)));
    assert!(!ours(&Selection {
        metric: "up".into(),
        label: "namespace".into(),
        value: "cogneva".into(),
    }));
    // 带外前缀的序列不是本仓库的词汇：判据按前缀认，不看它有没有在树里出现过。
    assert!(!ours(&Selection {
        metric: "kube_pod_container_status_restarts_total".into(),
        label: "scope".into(),
        value: "selected".into(),
    }));

    // 扫描器读的是 `"key": "value"`，值里的引号是转义过的 `\"`。
    let plain = expressions_under(
        r#"{"promql": "max by (dir) (cogneva_build_gate_slots)", "other": "x"}"#,
        "promql",
    );
    assert_eq!(plain, vec!["max by (dir) (cogneva_build_gate_slots)"]);
    let embedded = expressions_under(
        "{\"promql\": \"sum(rate(cogneva_x_total{outcome=\\\"ok\\\"}[5m]))\"}\n",
        "promql",
    );
    assert_eq!(
        embedded,
        vec!["sum(rate(cogneva_x_total{outcome=\"ok\"}[5m]))"]
    );
}

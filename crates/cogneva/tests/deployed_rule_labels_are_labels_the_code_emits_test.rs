//! principle: feedback_no_hardcoding_config_surface
//! The alert rules group our own series by label name: `max by (category)
//! (cogneva_landing_failures_total)`, `by (dir, role)`, `by (loop)`. Every one
//! of those names is a string in the rule document and a string in the code
//! that emits the series, and nothing compared the two.
//!
//! The failure is silent: a label renamed on the emitter side leaves the rule
//! grouping by a name no series carries. PromQL does not fail on that — the
//! grouping comes back empty, the rule evaluates over nothing and reports
//! nothing, and "the alert did not fire" is the same reading as "there was
//! nothing to alert on". The face goes quiet instead of wrong.
//!
//! These names are mostly too short for the shared-string census (`dir`,
//! `role`, `tool`, `side` are under the length it counts as structural), so
//! this is where they are held. What it judges, and on which side:
//!
//! * every name the rules group our series by is either written by a file in
//!   this tree, listed below, or belongs to the scrape surface;
//! * every line of that inventory still names something a rule groups by, so a
//!   rule that stops reading a label does not leave a line behind that claims
//!   it does;
//! * every file the inventory names still publishes the label -- as a literal, or
//!   through a constant whose value is that label. Either way a rename on the
//!   emitter side breaks it, which is what this line is for.
//!
//! What it cannot judge: a rule that groups by a name this test's expression
//! parser fails to see. That is why the extracted set is asserted to be large
//! and to contain names known to be in the document — a parser that reads
//! nothing would otherwise pass every gate below it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common;

use common::routes::repo_root;
use common::rules::promql_expressions;

use common::rust_production::{production_sources, relative_name};

/// Labels the scrape surface attaches, keyed to the reason this tree does not
/// write them.
const FOREIGN: &[(&str, &str)] = &[
    ("pod", "Pod 名由 kubelet 的服务发现打上，代码不写它"),
    ("container", "同上：容器名由采集面打"),
    (
        "instance",
        "Prometheus 按抓取目标打的地址，不是我们写进 series 的",
    ),
    (
        "job",
        "Prometheus 按抓取任务打的任务名；代码里那个 `job` 是日志标签，不是这行的来源",
    ),
    ("endpoint", "服务发现按端点打的标签"),
    (
        "persistentvolume",
        "kube-state-metrics 的 PV 名：这里的分组加在它的 series 上，\
         同一段表达式的另一支才是我们的读数",
    ),
];

/// The label names the rules group our series by, and the file that writes each
/// one.
///
/// Written out rather than derived: the point is that adding a label to a rule
/// is a decision someone makes here, with the emitter named, instead of
/// something that happens in one file and breaks in another.
const WRITTEN_BY_CODE: &[(&str, &str)] = &[
    ("asker", "crates/cog-reflection/src/build_gate/mod.rs"),
    ("category", "crates/cog-github/src/landing.rs"),
    ("cause", "crates/cog-observability/src/observable.rs"),
    ("clause", "crates/cog-reflection/src/mainline_deployer.rs"),
    ("consumer", "crates/cog-supervisor/src/consumer_lag.rs"),
    ("data_dir", "crates/cog-reflection/src/store_readouts.rs"),
    ("dir", "crates/cog-reflection/src/build_gate/mod.rs"),
    ("face", "crates/cog-orchestrator/src/face_drift.rs"),
    ("intent", "crates/cog-github/src/landing.rs"),
    ("kind", "crates/cog-orchestrator/src/resource_ledger.rs"),
    ("layer", "crates/cog-core/src/contract/knowledge.rs"),
    ("loop", "crates/cog-core/src/contract/loop_health.rs"),
    ("mirror", "crates/cog-github/src/landing.rs"),
    ("namespace", "crates/cog-observability/src/infra_watch.rs"),
    ("node", "crates/cog-observability/src/host_ledger.rs"),
    ("operation", "crates/cog-memory/src/metrics_instrumented.rs"),
    ("outcome", "crates/cog-observability/src/logs.rs"),
    ("outlet", "crates/cog-notification/src/delivery.rs"),
    (
        "persistentvolumeclaim",
        "crates/cog-core/src/claim_footprint.rs",
    ),
    ("phase", "crates/cog-observability/src/analytics.rs"),
    ("pool", "crates/cog-storage/src/plugin.rs"),
    ("reason", "crates/cog-observability/src/plugin.rs"),
    ("resource", "crates/cog-reflection/src/build_gate/mod.rs"),
    ("result", "crates/cog-orchestrator/src/sop/dispatch.rs"),
    ("role", "crates/cog-reflection/src/build_gate/mod.rs"),
    ("service", "crates/cog-observability/src/logs.rs"),
    ("side", "crates/cog-github/src/landing.rs"),
    ("slug", "crates/cog-orchestrator/src/principle_ledger.rs"),
    ("slot", "crates/cog-reflection/src/build_gate/mod.rs"),
    (
        "source",
        "crates/cog-observability/src/producer_heartbeat.rs",
    ),
    ("stage", "crates/cog-collaboration/src/observable.rs"),
    ("state", "crates/cog-observability/src/config_delivery.rs"),
    (
        "storage_class",
        "crates/cog-reflection/src/volume_capacity.rs",
    ),
    ("store", "crates/cog-reflection/src/store_readouts.rs"),
    ("stream", "crates/cog-stream/src/observable.rs"),
    ("tier", "crates/cog-reflection/src/change_pipeline.rs"),
    ("tool", "crates/cog-agent/src/archive.rs"),
    ("verb", "crates/cog-reflection/src/mainline_deployer.rs"),
    ("verdict", "crates/cog-collaboration/src/actors/mod.rs"),
    ("what", "crates/cog-reflection/src/build_gate/mod.rs"),
    ("worktree", "crates/cog-reflection/src/workspace.rs"),
];

/// The end of the parenthesised group that starts at `open`, inclusive of the
/// closing paren.
fn group_end(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The label names an expression groups *our own* series by.
///
/// Only `by` / `without` clauses whose operand mentions a `cogneva_` metric
/// count. A grouping over `node_` or `kube_` series names labels that belong to
/// another exporter, and holding those to this tree would be a census of
/// somebody else's vocabulary.
fn grouping_labels(expression: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for clause in ["by", "without"] {
        let mut from = 0;
        while let Some(at) = expression[from..].find(clause) {
            let at = from + at;
            from = at + clause.len();
            let after = expression[from..].trim_start();
            let Some(after) = after.strip_prefix('(') else {
                continue;
            };
            let open = expression.len() - after.len() - 1;
            let Some(close) = expression[open..].find(')').map(|i| open + i) else {
                continue;
            };
            let names = &expression[open + 1..close];
            let rest = expression[close + 1..].trim_start();
            let operand = match rest.strip_prefix('(') {
                Some(_) => {
                    let operand_open = expression.len() - rest.len();
                    match group_end(expression, operand_open) {
                        Some(end) => &expression[operand_open..=end],
                        None => continue,
                    }
                }
                None => continue,
            };
            if !operand.contains("cogneva_") {
                continue;
            }
            for name in names.split(',') {
                let name = name.trim();
                if !name.is_empty() && name != "__name__" {
                    out.insert(name.to_string());
                }
            }
        }
    }
    out
}

#[test]
fn every_label_the_rules_group_by_is_one_the_tree_writes() {
    let root = repo_root();
    let path = root.join("deploy/helm/cogneva/files/cogneva.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let document: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let mut expressions = Vec::new();
    promql_expressions(&document, &mut expressions);
    let mut used = BTreeSet::new();
    for expression in &expressions {
        used.extend(grouping_labels(expression));
    }
    // 抽取面自检：解析读空了的时候，下面每一条判据都会在空集上假装绿。
    assert!(
        used.len() >= 30,
        "只从规则里读到 {} 个分组标签，抽取面本身坏了：{used:?}",
        used.len()
    );
    for known in ["dir", "role", "stream", "outcome"] {
        assert!(used.contains(known), "规则里读不到 `{known}`：分组解析坏了");
    }

    let mut accounted: BTreeSet<&str> = BTreeSet::new();
    accounted.extend(WRITTEN_BY_CODE.iter().map(|(name, _)| *name));
    accounted.extend(FOREIGN.iter().map(|(name, _)| *name));

    let missing: Vec<&String> = used
        .iter()
        .filter(|name| !accounted.contains(name.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "部署里的规则按这些标签名分组，本表里却没有一处说它是谁写的：{missing:?}。\
         名字对不上时 PromQL 不报错，分组结果为空、规则在空集上求值，报出来的是\
         「没有告警」——与「一切正常」同形"
    );

    let stale: Vec<&&str> = accounted
        .iter()
        .filter(|name| !used.contains(**name))
        .collect();
    assert!(
        stale.is_empty(),
        "这些标签已经没有任何规则分组了，本表还留着：{stale:?}"
    );

    for (name, file) in WRITTEN_BY_CODE {
        let source = root.join(file);
        let body = std::fs::read_to_string(&source)
            .unwrap_or_else(|e| panic!("{}: {e}", source.display()));
        assert!(
            body.contains(&format!("\"{name}\"")) || references_a_declaration_of(&root, file, name),
            "{file} 里已经找不到 `{name}` 这个标签名了：规则还在按它分组，\
             代码却不再按这个名字发读数，分组结果会是空的"
        );
    }
}

/// 这个文件发读数时用的标签名，是否已经改由常量给出。
///
/// 判据从「文件里还写着这个字面量」放宽到「文件引用了一个值等于它的常量」：
/// 标签名收进常量之后，字面量只剩下那一处声明，发读数的地方是 `crate::LABEL_X`。
/// 两种写法都要认，否则把字面量换成常量这一件事本身就会把这个门判红——而它想
/// 拦的是标签从发读数的一侧消失，不是标签的写法。
fn references_a_declaration_of(root: &Path, file: &str, label: &str) -> bool {
    let Some(krate) = file
        .strip_prefix("crates/")
        .and_then(|rest| rest.split('/').next())
    else {
        return false;
    };
    let prefix = format!("crates/{krate}/src/");
    let files: Vec<PathBuf> = production_sources(root)
        .into_iter()
        .filter(|path| relative_name(root, path).starts_with(&prefix))
        .collect();
    let mut names = Vec::new();
    for path in &files {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines() {
            let Some(at) = line.find("const ") else {
                continue;
            };
            let rest = &line[at + "const ".len()..];
            let Some((name, tail)) = rest.split_once(':') else {
                continue;
            };
            if tail
                .trim_start()
                .starts_with(&format!("&str = \"{label}\""))
            {
                names.push(name.trim().to_string());
            }
        }
    }
    if names.is_empty() {
        return false;
    }
    let body = std::fs::read_to_string(root.join(file)).unwrap_or_default();
    names.iter().any(|name| body.contains(&format!("::{name}")))
}

/// 解析面自己也要有判据，否则「读到空集」与「没有标签」不可分。
#[test]
fn the_expression_parser_reads_a_grouping_and_ignores_foreign_series() {
    let grouped = grouping_labels(
        "max by (category, face) (increase(cogneva_landing_failures_total[6h])) > 0",
    );
    assert_eq!(
        grouped.into_iter().collect::<Vec<_>>(),
        vec!["category".to_string(), "face".to_string()]
    );

    // 按别人的 series 分组不是本仓库的词汇：不统计它，表格就不用为它负责。
    let foreign = grouping_labels("max by (mountpoint) (node_filesystem_avail_bytes)");
    assert!(foreign.is_empty(), "别人的 series 的分组标签不该算进来");

    // 子查询与嵌套括号也要读全。
    let nested = grouping_labels(
        "(min_over_time((max by (dir, role) (cogneva_build_gate_slots))[5m:1m]) < 1)",
    );
    assert_eq!(
        nested.into_iter().collect::<Vec<_>>(),
        vec!["dir".to_string(), "role".to_string()]
    );
}

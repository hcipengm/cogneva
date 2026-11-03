//! Gate: the opt-out label and the node-level collector's drop rule are the same
//! statement, and both are true of the pods that carry it.
//!
//! `cogneva.io/logs: self` on a pod template means "every container here already
//! pushes its own lines to Loki". The node-level collector (Grafana Alloy) reads
//! that label off the pod and skips the whole pod, because writing the same line
//! twice is how a log bill doubles without anyone noticing.
//!
//! Both halves fail silently. If a pod self-ships but is not labelled, its lines
//! arrive twice — visible only as a bigger bill. If a pod is labelled but some
//! container in it does not self-ship, that container's lines arrive nowhere:
//! the process runs, the exit code is whatever it is, and the one thing a reader
//! needs is gone. Neither shows up in any health check, so it is judged here.
//!
//! Three things are checked, and each can be wrong on its own:
//!
//!   1. the label value the pod templates use is the value the collector's drop
//!      rule matches (rename one and the rule stops matching every pod);
//!   2. a labelled pod has no non-init container that fails to self-ship;
//!   3. an unlabelled pod that runs a self-shipping container also runs a
//!      container that cannot self-ship — otherwise the label was simply
//!      forgotten, and that pod's lines are written twice.
//!
//! A pod may be unlabelled and still duplicate a container (the image registry
//! shares a pod with the volume-walker sidecar, and the registry's own lines are
//! worth more than the duplicate they cost). Check 3 asks for the *reason*, not
//! for the label: the duplicate is only allowed when dropping the pod would lose
//! a container that has nowhere else to go.
//!
//! Init containers are outside all three checks: they run shell scripts and one
//! shot binaries, never the self-pushing assembly, so the collector must always
//! keep them — which is exactly what the `false` half of the drop rule says.

use crate::common;

use common::routes::read;
use common::routes::repo_root;
use common::routes::walk;
use common::rust_text;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde_yaml::Value;

/// The pod label the chart puts on templates whose containers ship their own
/// logs. Written once here so the checks below cannot disagree about it.
const OPT_OUT_LABEL: &str = "cogneva.io/logs";
const OPT_OUT_VALUE: &str = "self";

/// The pods allowed to duplicate a self-shipping container, and why.
///
/// Labelling skips the whole pod, so a pod that mixes a self-shipping container
/// with one that cannot ship has to choose: label it and lose the second
/// container's lines, or leave it unlabelled and pay for the duplicate. The
/// registry is the one pod where the duplicate is the cheaper mistake — its own
/// lines are worth more than the second copy costs, while the sidecar beside it
/// has nowhere else to go.
///
/// This is a list, not a count. A count would have let a fourth profile's copy
/// of the same pod, or a second pod with the same problem, slide in without
/// anyone deciding it was acceptable.
const DUPLICATE_IS_ACCEPTED: &[(&str, &str)] = &[(
    "cogneva-registry",
    "同 Pod 的卷搬移 sidecar 不推日志，贴标签会把它那半边也丢掉",
)];

/// The subcommands that write log lines to Loki from inside the process.
///
/// Two paths install a subscriber: the table in `main.rs`, and the standalone
/// security gateway, whose assembly (`gateway_observability::assemble`) installs
/// one for the pool, alert and metering faces it wires up. The gateway is spelled
/// out here rather than read out of the dispatch arm — an arm-shaped search is
/// what the table in `main.rs` exists to avoid.
fn self_shipping_tokens(root: &Path) -> BTreeSet<String> {
    let argv = rust_text::cli_argv_tokens(&read(&root.join("crates/cogneva/src/cli.rs")));
    let mut out = BTreeSet::new();
    let wired = rust_text::standalone_log_services(&read(&root.join("crates/cogneva/src/main.rs")));
    for variant in wired.into_keys() {
        let token = argv.get(&variant).unwrap_or_else(|| {
            panic!(
                "STANDALONE_LOG_SERVICES names Command::{variant}, which the \
                 argument table in cli.rs does not spell"
            )
        });
        out.insert(token.clone());
    }
    out.insert(
        argv.get("SecurityGateway")
            .expect("cli.rs no longer spells the security gateway's argv token")
            .clone(),
    );
    assert!(
        out.len() >= 6,
        "only {} log-wired subcommands were derived; this gate is judging an empty set",
        out.len()
    );
    out
}

/// One pod template: its labels and its containers, with init containers marked.
struct PodTemplate {
    /// The workload's name, which is the same pod in every tree that renders it.
    /// The trees carry three copies of the chart's output plus the static
    /// baseline; judging "how many pods duplicate their lines" by file path
    /// makes that count scale with the number of profiles instead of with the
    /// number of workloads.
    name: String,
    origin: String,
    labels: BTreeMap<String, String>,
    /// (is_init, container)
    containers: Vec<(bool, Value)>,
}

fn pod_templates(value: &Value, origin: &str, out: &mut Vec<PodTemplate>) {
    let Some(kind) = value.get("kind").and_then(|k| k.as_str()) else {
        return;
    };
    if !matches!(
        kind,
        "Deployment" | "StatefulSet" | "DaemonSet" | "CronJob" | "Job"
    ) {
        return;
    }
    let mut spec = value.get("spec");
    if kind == "CronJob" {
        spec = spec
            .and_then(|s| s.get("jobTemplate"))
            .and_then(|s| s.get("spec"));
    }
    let Some(template) = spec.and_then(|s| s.get("template")) else {
        return;
    };
    let name = value
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("<unnamed>");
    let mut labels = BTreeMap::new();
    if let Some(map) = template
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_mapping())
    {
        for (key, value) in map {
            if let (Some(key), Some(value)) = (key.as_str(), value.as_str()) {
                labels.insert(key.to_owned(), value.to_owned());
            }
        }
    }
    let mut containers = Vec::new();
    if let Some(spec) = template.get("spec") {
        for (is_init, field) in [(true, "initContainers"), (false, "containers")] {
            if let Some(items) = spec.get(field).and_then(|c| c.as_sequence()) {
                for item in items {
                    containers.push((is_init, item.clone()));
                }
            }
        }
    }
    out.push(PodTemplate {
        name: name.to_owned(),
        origin: format!("{origin} [{name}]"),
        labels,
        containers,
    });
}

fn image_of(container: &Value) -> String {
    container
        .get("image")
        .and_then(|i| i.as_str())
        .unwrap_or("")
        .to_owned()
}

fn strings_of(container: &Value, key: &str) -> Vec<String> {
    container
        .get(key)
        .and_then(|v| v.as_sequence())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Does this container run the platform's own binary in a mode that writes its
/// lines to Loki itself?
///
/// The image has to be ours: a `registry:2` container is started with no argv at
/// all, which is the same shape as the assembled application. The command has to
/// be absent or the binary itself: the seed and migrate init containers run shell
/// scripts out of the same image and never reach an assembly. And the subcommand
/// has to be one that installs a subscriber — a stdout-protocol subcommand
/// (`eval-table`) is not one, and labelling it would send its output nowhere.
fn self_ships(container: &Value, tokens: &BTreeSet<String>) -> bool {
    let image = image_of(container);
    let ours = image
        .rsplit('/')
        .next()
        .map(|segment| segment.starts_with("cogneva:"))
        .unwrap_or(false);
    if !ours {
        return false;
    }
    let command = strings_of(container, "command");
    if !command.is_empty() && command != ["/opt/cogneva/cogneva"] {
        return false;
    }
    let args = strings_of(container, "args");
    match args.first() {
        None => true,
        Some(token) => tokens.contains(token),
    }
}

/// The relabel program of the collector's pod-scrape job, read out of the
/// manifest that ships it.
struct CollectorRule {
    namespaces: Vec<String>,
    /// The drop rule's `source_labels`, `separator` and `regex`. Alloy spells
    /// relabel rules the way Prometheus does, so these read the same as they did
    /// when the collector was promtail.
    drop_source_labels: Vec<String>,
    drop_separator: String,
    drop_regex: String,
}

/// Byte span of the first block whose header starts with `header`: `(body, after)`
/// — the inside, and the offset just past its closing brace. Alloy's config is
/// HCL-shaped (`component "name" { … }`, nested blocks), so a field is only safe
/// to read once the enclosing block is delimited.
fn block_span(text: &str, header: &str) -> Option<(std::ops::Range<usize>, usize)> {
    let start = text.find(header)?;
    let open = start + text[start..].find('{')?;
    let mut depth = 0usize;
    for (offset, c) in text[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some((open + 1..open + offset, open + offset + 1));
                }
            }
            _ => {}
        }
    }
    None
}

fn block<'a>(text: &'a str, header: &str) -> Option<&'a str> {
    block_span(text, header).map(|(body, _)| &text[body])
}

/// Every `key = value` line directly in `body` — comments skipped, nested blocks
/// not descended into. Only the first occurrence of a key is returned: an HCL
/// block assigns each attribute once.
fn attribute(body: &str, key: &str) -> Option<String> {
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with("//") || line.starts_with('#') {
            continue;
        }
        let rest = match line.strip_prefix(key) {
            Some(rest) if rest.starts_with(|c: char| c.is_whitespace()) => rest.trim_start(),
            _ => continue,
        };
        let Some(value) = rest.strip_prefix('=') else {
            continue;
        };
        return Some(value.split("//").next().unwrap_or("").trim().to_owned());
    }
    None
}

/// The quoted strings in an HCL value: `"a"` yields one, `["a", "b"]` yields two.
fn quoted(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = value.char_indices();
    while let Some((at, c)) = chars.next() {
        if c != '"' {
            continue;
        }
        let rest = &value[at + 1..];
        let Some(len) = rest.find('"') else {
            break;
        };
        out.push(rest[..len].to_owned());
        for _ in 0..len + 1 {
            chars.next();
        }
    }
    out
}

fn collector_rule(root: &Path) -> CollectorRule {
    let path = root.join("deploy/k3s/observability/manifests/27-log-collector.yaml");
    let text = read(&path);
    let mut config_text = None;
    for document in text.split("\n---") {
        let Ok(value) = serde_yaml::from_str::<Value>(document) else {
            continue;
        };
        if value.get("kind").and_then(|k| k.as_str()) != Some("ConfigMap") {
            continue;
        }
        if let Some(config) = value.get("data").and_then(|d| d.get("config.alloy")) {
            config_text = config.as_str().map(str::to_owned);
        }
    }
    let config = config_text.unwrap_or_else(|| {
        panic!(
            "{} has no ConfigMap carrying `config.alloy`: either the collector moved \
             or its scrape program is no longer readable here",
            path.display()
        )
    });

    let discovery = block(&config, "discovery.kubernetes")
        .unwrap_or_else(|| panic!("{}: no `discovery.kubernetes` block", path.display()));
    let namespaces = block(discovery, "namespaces")
        .and_then(|n| attribute(n, "names"))
        .map(|names| quoted(&names))
        .unwrap_or_default();

    // Every `rule { … }` inside the relabel component; the drop rule is the one
    // carrying `action = "drop"`.
    let relabel = block(&config, "discovery.relabel")
        .unwrap_or_else(|| panic!("{}: no `discovery.relabel` block", path.display()));
    let mut rest = relabel;
    let mut drop = None;
    while let Some(at) = rest.find("rule") {
        let Some((body, after)) = block_span(&rest[at..], "rule") else {
            break;
        };
        if attribute(&rest[at..][body.clone()], "action").as_deref() == Some("\"drop\"") {
            drop = Some(&rest[at..][body]);
            break;
        }
        // Step past this block; a find over the whole body would re-find it.
        rest = &rest[at + after..];
    }
    let drop = drop.unwrap_or_else(|| {
        panic!(
            "{}: the collector has no `action = \"drop\"` rule, so every pod's lines \
             are shipped — including the ones that already pushed themselves",
            path.display()
        )
    });

    CollectorRule {
        namespaces,
        drop_source_labels: attribute(drop, "source_labels")
            .map(|labels| quoted(&labels))
            .unwrap_or_default(),
        drop_separator: attribute(drop, "separator")
            .map(|s| quoted(&s).join(""))
            .unwrap_or_default(),
        drop_regex: attribute(drop, "regex")
            .map(|r| quoted(&r).join(""))
            .unwrap_or_default(),
    }
}

/// How the Kubernetes service discovery spells a pod label as a meta label:
/// characters outside `[a-z0-9_]` become `_`, and the whole thing is lowercased.
fn meta_label_of(label: &str) -> String {
    let mut out = String::from("__meta_kubernetes_pod_label_");
    for c in label.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out
}

#[test]
fn a_pod_that_ships_its_own_logs_is_the_one_the_collector_skips() {
    let root = repo_root();
    let tokens = self_shipping_tokens(&root);
    let rule = collector_rule(&root);

    // 1. The collector only ever sees the namespace the workloads live in, and
    //    its drop rule is the one keyed on this label.
    assert!(
        rule.namespaces.iter().any(|n| n == "cogneva"),
        "collector 只采 {:?}，其中没有 `cogneva`：这批工作负载的标准输出一条也进不了 Loki",
        rule.namespaces
    );
    let label_meta = meta_label_of(OPT_OUT_LABEL);
    assert!(
        rule.drop_source_labels.contains(&label_meta),
        "采集器的丢弃规则读的是 {:?}，而不是 `{label_meta}`（`{OPT_OUT_LABEL}` 展开后的\n\
         元标签名）。改标签名时两边必须一起改，否则每个自推的 Pod 都会被再采一遍",
        rule.drop_source_labels
    );
    let init_meta = "__meta_kubernetes_pod_container_init";
    assert!(
        rule.drop_source_labels.contains(&init_meta.to_owned()),
        "丢弃规则没有考虑 `{init_meta}`：init 容器永远不自推（它们跑的是 sh 脚本），\n\
         按 Pod 一概丢弃会把种子/迁移/GC 那几段日志一起丢掉——而那正是「Pod 为什么卡在\n\
         Init」唯一的第一现场"
    );

    // 2. The value the templates use is the value the rule matches.
    //
    // 两条投递面都读：`deploy/k3s` 是 apply 路径的静态清单，`deploy/rendered` 是
    // 三种 profile 渲染出来的那一份。此前这里把渲染面当成「一棵树」再走一遍，而
    // 走树收的是文件——渲染面的每一份都被当成目录去读，读不到就悄悄退出，于是
    // 这条判据一直只扫到 `deploy/k3s`：少读一块与那一块都合规在判词上同形。
    let mut files = Vec::new();
    walk(&root.join("deploy/k3s"), &["yaml"], &mut files);
    walk(&root.join("deploy/rendered"), &["yaml"], &mut files);
    let mut pods = Vec::new();
    for file in &files {
        let text = read(file);
        let origin = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .display()
            .to_string();
        for document in text.split("\n---") {
            let Ok(value) = serde_yaml::from_str::<Value>(document) else {
                continue;
            };
            pod_templates(&value, &origin, &mut pods);
        }
    }
    assert!(
        pods.len() > 15,
        "只在树里认出 {} 个 Pod 模板：遍历没看见工作负载，这条判据今天几乎在空集上通过",
        pods.len()
    );

    let values: BTreeSet<String> = pods
        .iter()
        .filter_map(|p| p.labels.get(OPT_OUT_LABEL).cloned())
        .collect();
    assert_eq!(
        values,
        BTreeSet::from([OPT_OUT_VALUE.to_owned()]),
        "Pod 模板上的 `{OPT_OUT_LABEL}` 取值为 {values:?}，而采集器的丢弃规则写成 \
         `{}`；值不一致时规则一条也匹配不上，每个自推的 Pod 都会被再采一遍",
        rule.drop_regex
    );
    let expected = format!("{OPT_OUT_VALUE}{}", rule.drop_separator);
    assert!(
        rule.drop_regex.starts_with(&expected) && rule.drop_regex.contains("false"),
        "丢弃规则写的是 `{}`，它应当把「标签是 {OPT_OUT_VALUE}」与「不是 init 容器」\
         一起判：以 `{expected}` 开头并含 `false`",
        rule.drop_regex
    );

    // 3. A labelled pod may only contain containers that self-ship; an unlabelled
    //    pod may only contain a self-shipping container when another container
    //    there has nowhere else to send its lines.
    let mut labelled = 0;
    let mut unlabelled_with_self_shipper: BTreeSet<String> = BTreeSet::new();
    // Keyed by workload name: the same pod is rendered into three profiles plus
    // the static baseline, and one defect listed four times reads like four.
    let mut problems: BTreeMap<String, String> = BTreeMap::new();
    for pod in &pods {
        let has_label = pod
            .labels
            .get(OPT_OUT_LABEL)
            .map(|v| v == OPT_OUT_VALUE)
            .unwrap_or(false);
        if has_label {
            labelled += 1;
        }
        let mains: Vec<&Value> = pod
            .containers
            .iter()
            .filter(|(is_init, _)| !is_init)
            .map(|(_, c)| c)
            .collect();
        let shippers: Vec<&Value> = mains
            .iter()
            .copied()
            .filter(|c| self_ships(c, &tokens))
            .collect();
        let silent: Vec<&Value> = mains
            .iter()
            .copied()
            .filter(|c| !self_ships(c, &tokens))
            .collect();
        let named = |c: &Value| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unnamed>")
                .to_owned()
        };

        if has_label && !silent.is_empty() {
            problems.insert(
                pod.name.clone(),
                format!(
                    "{} 贴着 `{OPT_OUT_LABEL}: {OPT_OUT_VALUE}`，但里面的 {} 不自己推日志——\n\
                     采集器按 Pod 跳过，这些容器的标准输出从此没有任何去处",
                    pod.origin,
                    silent
                        .iter()
                        .map(|c| format!("`{}`", named(c)))
                        .collect::<Vec<_>>()
                        .join("、")
                ),
            );
        }
        if !has_label && !shippers.is_empty() {
            if silent.is_empty() {
                problems.insert(
                    pod.name.clone(),
                    format!(
                        "{} 跑着会自己推日志的 {}，却没贴 `{OPT_OUT_LABEL}: {OPT_OUT_VALUE}`：\n\
                         采集器会把这些行再送一遍",
                        pod.origin,
                        shippers
                            .iter()
                            .map(|c| format!("`{}`", named(c)))
                            .collect::<Vec<_>>()
                            .join("、")
                    ),
                );
            } else {
                unlabelled_with_self_shipper.insert(pod.name.clone());
            }
        }
    }

    assert!(
        problems.is_empty(),
        "日志投递面两边的说法对不上：\n  {}\n\n\
         贴标签的判据是「这个 Pod 里每个非 init 容器都在自己推日志」；采集器按 Pod 跳过。\n\
         自推的 Pod 没贴标签会被采两遍，贴了标签却有容器不自推则那些行一条也留不下。",
        problems
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    // Both sides of the line have to be populated for the two checks above to
    // mean anything.
    assert!(
        labelled >= 8,
        "只有 {labelled} 个 Pod 贴了 `{OPT_OUT_LABEL}`；这个数掉了的时候，第 3 条判据\n\
         对绝大多数工作负载其实是空转"
    );
    let accepted: BTreeSet<String> = DUPLICATE_IS_ACCEPTED
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect();
    let undeclared: Vec<&String> = unlabelled_with_self_shipper.difference(&accepted).collect();
    assert!(
        undeclared.is_empty(),
        "这些 Pod 不贴标签、却把自推容器的日志又采了一遍：{undeclared:?}。\n\
         它们要么漏贴了 `{OPT_OUT_LABEL}: {OPT_OUT_VALUE}`，要么得在\n\
         `DUPLICATE_IS_ACCEPTED` 里写明「同 Pod 有采不了的容器」这个理由——\n\
         不写理由的多采一遍是账单上的一笔，不是判据里的一个例外"
    );
    for (name, why) in DUPLICATE_IS_ACCEPTED {
        assert!(
            unlabelled_with_self_shipper.contains(*name),
            "`DUPLICATE_IS_ACCEPTED` 里留着 `{name}`（{why}），但树里已经没有这个\n\
             「不贴标签又混着自推容器」的 Pod 了：豁免不是永久免检，它得跟着理由一起消失"
        );
    }
}

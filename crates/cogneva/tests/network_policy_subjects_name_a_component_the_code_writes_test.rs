//! principle: feedback_no_hardcoding_config_surface
//! A NetworkPolicy's subject selector names the Pods the document governs. In this
//! tree one of those selectors names a Pod that has no manifest at all: the
//! mainline rollout Job is built at runtime by `mainline_deployer.rs`, so its
//! component label is spelled in the policy and again as a constant in that file,
//! and nothing compared the two.
//!
//! The failure is silent in both directions. Rename the constant and the policy
//! governs the empty set -- every rule it carries stops applying, so the egress
//! it used to allow is dropped and the reading is a connection timeout, the same
//! reading as a peer that never answered. Rename the selector value and the same
//! thing happens with the blame on the other side. `kubectl apply` reports neither.
//!
//! What is judged: a subject selector that selects no Pod template in the tree it
//! is applied with must name a component value some constant in `crates/*/src`
//! spells. That constant is the readable statement of "the code stamps Pods with
//! this name", so the selector and the code are compared through it. A selector
//! that does select a Pod in the tree is left alone: its Pod is right there, and
//! the drift between a manifest selector and a manifest label is judged by
//! `deploy/scripts/check-control-plane-reachability.sh`.
//!
//! What it cannot judge: that a constant is actually used where the Pod labels
//! are built (the association is "a constant the code declares", not "the
//! constant this template uses"), so a policy naming a component the code
//! declares but never stamps still reads clean. The census floors below are what
//! keeps that from turning into a silent no-op.
//!
//! The subtree `deploy/k3s/observability/` is skipped, on the same grounds the
//! reachability script skips it: the Pods its selectors name (prometheus,
//! grafana) come from kube-prometheus-stack and have no manifest here, so there
//! is nothing in the tree to compare against. The skip is counted, not implied.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::rust_production::{production_lines, production_sources};
use common::service_topology::{
    network_policies, pod_labels, selector_matches, NetworkPolicySubject,
};

/// 子树：它的 Pod 由别的栈提供，树里没有可比的清单。
const FOREIGN_STACK: &str = "deploy/k3s/observability/";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The component values a crate declares as a string constant. The name has to say
/// COMPONENT: the value of `const TIMEOUT_SECS` is not a component, and a scan that
/// collects every string constant answers a different question than the one asked.
fn constants_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in production_lines(text) {
        let trimmed = line.trim();
        if !(trimmed.starts_with("const ") || trimmed.starts_with("pub const ")) {
            continue;
        }
        if !trimmed.contains("COMPONENT") || !trimmed.contains(": &str =") {
            continue;
        }
        let Some(start) = trimmed.find("= \"") else {
            continue;
        };
        let rest = &trimmed[start + 3..];
        let Some(end) = rest.find('"') else {
            continue;
        };
        out.insert(rest[..end].to_string());
    }
    out
}

fn component_constants(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        out.extend(constants_in(&text));
    }
    out
}

/// 一份清单属于哪一条交付路径（`deploy/k3s` 或某个 profile）。
fn tree_of(manifest: &str) -> String {
    if let Some(rest) = manifest.strip_prefix("deploy/rendered/") {
        return format!(
            "deploy/rendered/{}",
            rest.split('/').next().unwrap_or_default()
        );
    }
    "deploy/k3s".to_string()
}

#[derive(Default)]
struct Census {
    offences: Vec<String>,
    skipped: Vec<String>,
    /// 每条交付路径上「选不中树里任何 Pod」的 subject 有几份。
    inert_per_tree: BTreeMap<String, usize>,
    judged: usize,
}

fn census(
    policies: &[NetworkPolicySubject],
    pods: &[(String, BTreeMap<String, String>)],
    constants: &BTreeSet<String>,
) -> Census {
    let namespaces_with_pods: BTreeSet<&str> = pods
        .iter()
        .map(|(namespace, _)| namespace.as_str())
        .collect();
    let mut out = Census::default();
    for policy in policies {
        if policy.manifest.starts_with(FOREIGN_STACK) {
            out.skipped.push(format!(
                "{}：{} 的 Pod 由别的栈提供（树里没有它的清单），选择器无从比对",
                policy.manifest, policy.name
            ));
            continue;
        }
        if policy.uses_expressions {
            out.skipped.push(format!(
                "{}：{} 的 subject 用了 matchExpressions，这个读取器只读 matchLabels",
                policy.manifest, policy.name
            ));
            continue;
        }
        if !namespaces_with_pods.contains(policy.namespace.as_str()) {
            out.skipped.push(format!(
                "{}：{} 落在命名空间 {:?}，树里这个命名空间一个 Pod 模板都没有",
                policy.manifest, policy.name, policy.namespace
            ));
            continue;
        }
        out.judged += 1;
        let live = pods.iter().any(|(namespace, labels)| {
            *namespace == policy.namespace && selector_matches(labels, &policy.selector)
        });
        if live {
            continue;
        }
        *out.inert_per_tree
            .entry(tree_of(&policy.manifest))
            .or_default() += 1;
        let Some(component) = policy.selector.get(cog_core::topology::LABEL_COMPONENT) else {
            out.offences.push(format!(
                "{}：NetworkPolicy {} 的 subject 选择器 {:?} 选不中树里任何 Pod，而它没有按 {} \
                 指名组件——读不出这份策略到底管谁",
                policy.manifest,
                policy.name,
                policy.selector,
                cog_core::topology::LABEL_COMPONENT
            ));
            continue;
        };
        if !constants.contains(component) {
            out.offences.push(format!(
                "{}：NetworkPolicy {} 的 subject 选不中树里任何 Pod，它指的组件 {component:?} \
                 也不是代码里任何组件常量的值——代码不会把 Pod 打成这个名字，这份策略的作用\
                 范围是空集，它写的每一条放行与拒绝都不作用于任何 Pod",
                policy.manifest, policy.name
            ));
        }
    }
    out
}

fn real_tree_census() -> (Census, usize) {
    let root = repo_root();
    let constants = component_constants(&root);
    assert!(
        constants.contains("mainline-rollout"),
        "从 crates/*/src 里一个组件常量都没读到滚动 Job 的那个值：读取器坏了，后面每条判据都会绿得没有意义"
    );
    let policies = network_policies(&root);
    let pods = pod_labels(&root);
    assert!(!policies.is_empty(), "交付树里一份 NetworkPolicy 都没读到");
    assert!(!pods.is_empty(), "交付树里一个 Pod 模板都没读到");
    let census = census(&policies, &pods, &constants);
    (census, policies.len())
}

#[test]
fn every_subject_that_selects_no_pod_names_a_component_the_code_writes() {
    let (census, _) = real_tree_census();
    assert!(
        census.offences.is_empty(),
        "这些策略的选择器选不中树里的 Pod，而选择器里的组件值也不是代码写的：\n{}",
        census.offences.join("\n")
    );
}

#[test]
fn every_tree_has_exactly_one_subject_that_selects_no_pod() {
    let (census, policies) = real_tree_census();
    assert!(
        !census.inert_per_tree.is_empty(),
        "没有一条交付路径上有选不中的 subject"
    );
    for (tree, count) in &census.inert_per_tree {
        assert_eq!(
            *count, 1,
            "{tree} 里有 {count} 份策略的 subject 选不中树里任何 Pod（读到 {policies} 份策略、判了 \
             {} 份）。今天这个数是 1，指的是滚动 Job——它的 Pod 由部署器在运行时创建，树里没有\
             清单。多出来的那一份要么是拼错了组件值，要么是又一份策略在管运行时创建的 Pod；\
             两种情况都得看一眼，不能让它就这么静默地成为空范围。",
            census.judged
        );
    }
}

#[test]
fn the_census_skips_only_the_foreign_observability_stack() {
    let (census, _) = real_tree_census();
    for skipped in &census.skipped {
        assert!(
            skipped.contains("别的栈提供") || skipped.contains("kube-prometheus-stack"),
            "有一条策略被跳过，理由却不是「Pod 由别的栈提供」：\n{skipped}"
        );
    }
    assert!(
        census.skipped.iter().any(|s| s.contains("别的栈提供")),
        "observability 那棵子树的策略没有被读到：跳过名单是空的，说明读取范围变了，\
         这条排除需要重新确认"
    );
}

#[test]
fn a_renamed_component_constant_leaves_the_subject_naming_nothing() {
    let policy = NetworkPolicySubject {
        manifest: "deploy/k3s/network-policy.yaml".to_string(),
        name: "cogneva-mainline-rollout-egress".to_string(),
        namespace: "cogneva".to_string(),
        selector: BTreeMap::from([(
            cog_core::topology::LABEL_COMPONENT.to_string(),
            "mainline-rollout".to_string(),
        )]),
        uses_expressions: false,
    };
    // 常量集合里没有它指的那个组件：等同于「代码把这个名字改掉了，清单没跟上」。
    let renamed = BTreeSet::from(["gateway".to_string()]);
    let pods = vec![(
        "cogneva".to_string(),
        BTreeMap::from([("app.kubernetes.io/name".to_string(), "x".to_string())]),
    )];
    let census = census(&[policy], &pods, &renamed);
    assert_eq!(census.inert_per_tree.get("deploy/k3s"), Some(&1));
    assert!(
        census
            .offences
            .iter()
            .any(|line| line.contains("mainline-rollout")),
        "改掉常量之后选择器仍然被判绿：{:?}",
        census.offences
    );
}

#[test]
fn a_subject_that_selects_a_pod_in_the_tree_is_not_held_to_a_constant() {
    let policy = NetworkPolicySubject {
        manifest: "deploy/k3s/network-policy.yaml".to_string(),
        name: "example".to_string(),
        namespace: "cogneva".to_string(),
        selector: BTreeMap::from([(
            cog_core::topology::LABEL_COMPONENT.to_string(),
            "made-up".to_string(),
        )]),
        uses_expressions: false,
    };
    let pods = vec![(
        "cogneva".to_string(),
        BTreeMap::from([(
            cog_core::topology::LABEL_COMPONENT.to_string(),
            "made-up".to_string(),
        )]),
    )];
    let census = census(&[policy], &pods, &BTreeSet::new());
    assert_eq!(census.judged, 1);
    assert!(
        census.offences.is_empty(),
        "树里真的选了 Pod 的选择器不该被要求指到常量上：{:?}",
        census.offences
    );
}

#[test]
fn a_subject_naming_no_component_is_reported() {
    let policy = NetworkPolicySubject {
        manifest: "deploy/k3s/network-policy.yaml".to_string(),
        name: "example".to_string(),
        namespace: "cogneva".to_string(),
        selector: BTreeMap::from([("app.kubernetes.io/name".to_string(), "ghost".to_string())]),
        uses_expressions: false,
    };
    let pods = vec![("cogneva".to_string(), BTreeMap::new())];
    let census = census(&[policy], &pods, &BTreeSet::new());
    assert!(
        census
            .offences
            .iter()
            .any(|line| line.contains("读不出这份策略到底管谁")),
        "选择器选不中 Pod 又没指组件，却没被报出来：{:?}",
        census.offences
    );
}

#[test]
fn the_constant_scan_reads_production_and_ignores_comments_and_test_blocks() {
    let text = "/// 说明里写 const COMPONENT_DOC: &str = \"documented\"; 不算\n\
                pub const COMPONENT_REAL: &str = \"real\";\n\
                // const COMPONENT_COMMENT: &str = \"commented\";\n\
                const TIMEOUT_SECS: &str = \"not-a-component\";\n\
                #[cfg(test)]\n\
                mod tests {\n    const COMPONENT_TEST: &str = \"in-test\";\n}\n";
    let found = constants_in(text);
    assert!(found.contains("real"), "没读到真的常量：{found:?}");
    for absent in ["documented", "commented", "not-a-component", "in-test"] {
        assert!(
            !found.contains(absent),
            "{absent:?} 不该被读成组件常量：{found:?}"
        );
    }
}

#[test]
fn the_policy_reader_sees_both_delivery_paths() {
    let root = repo_root();
    let policies = network_policies(&root);
    assert!(
        policies
            .iter()
            .any(|policy| policy.manifest.starts_with("deploy/k3s/")),
        "交付树 deploy/k3s 里一份策略都没读到"
    );
    assert!(
        policies
            .iter()
            .any(|policy| policy.manifest.starts_with("deploy/rendered/")),
        "预渲染 profile 里一份策略都没读到"
    );
}

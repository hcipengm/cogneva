//! principle: feedback_an_admission_cap_makes_a_knob_a_coupled_pair
//! 关停面只有**一个**数：Pod 宽限期。排空预算是它算出来的，不是另一个旋钮。
//!
//! 进程收到 SIGTERM 之后的时间轴有两段，两段的主人不同：
//!
//! - 宽限期（`terminationGracePeriodSeconds`）：kubelet 从 SIGTERM 起算，到点
//!   SIGKILL。集群读的是这一格，它是这条链上唯一被强制执行的时间。
//! - 排空预算（`core.system.shutdown_timeout_ms`）：进程自己等在飞的运行跑到回合
//!   边界，到点就不等了。它必须短于宽限期，否则进程还在排空就被 SIGKILL——那一次
//!   排空只发生在日志里，事后看到的只有「这次尝试没有续跑点」。
//!
//! 两个数分两处写时，宽限期一改，预算不会跟着走；而这种不一致在清单上读不出来，
//! 只能靠人记得同步（此前就靠一条契约测试手抄着守）。所以预算是算出来的：模板把
//! 宽限期减去收尾余量（`shutdown.drainTeardownMarginMs`）写进进程环境
//! `COGNEVA_SHUTDOWN_TIMEOUT_MS`，字段与进程环境同源，三处不可能分家。
//!
//! 宽限期要盖住**每一个跑 agent 运行的工作负载**，不是只盖主应用：实测 2026-11-09
//! 11:43 两个 Pod 同时在跑 —— 主应用的 `DSC-*` generator 与进化 Pod 的
//! `self-signal-alert-*` planner。只给主应用注入预算，被滚动的进化 Pod 照样按
//! cogneva.json 里那份**脱清单默认值**排空，而清单上看起来「已经做了」。
//!
//! 所以这条契约要判的是：values 只有那两个叶子、没有第三个预算；模板从它们算出
//! 预算而不是写字面量；每个工作负载（chart 渲染与 `deploy/k3s` 静态清单两条路径）
//! 都带着这个环境变量与同一个宽限；环变量名在交付的配置里有映射（否则注入了也
//! 不写任何字段）。只改一处而不改另一处会红在这里，而不是在集群上被 SIGKILL 的
//! 那一次。

use crate::common;

use common::routes::{read, walk_all};

use common::routes::repo_root;

/// 进程读到的那份配置。
const JSON: &str = "deploy/helm/cogneva/files/cogneva.json";

/// 部署能改的那份值。
const VALUES: &str = "deploy/helm/cogneva/values.yaml";

/// 渲染产物：每个 profile 一份 Deployment。
const RENDERED: &str = "deploy/rendered";

/// 元启动自建集群时 `kubectl apply -k` 的那份清单：它不是渲染产物，是手维护的
/// parity 基线，所以它有没有这一格不会被「渲染是否最新」那条门看见。
const STATIC_DEPLOYMENTS: &[(&str, &str)] = &[
    ("cogneva", "deploy/k3s/deployment.yaml"),
    ("cogneva-evolution", "deploy/k3s/evolution-deployment.yaml"),
];

/// 跑 agent 运行的工作负载：名字、渲染出的清单名、写它的模板。
///
/// 这份名单只收**会执行模型回合**的那些 Pod。别的 Pod 拿到宽限期没有坏处，但
/// 它们不排空，列进来只会让这份名单随时间变长、与本条契约无关。
const WORKLOADS: &[(&str, &str, &str)] = &[
    (
        "cogneva",
        "41-deployment-cogneva.yaml",
        "templates/gateway.yaml",
    ),
    (
        "cogneva-evolution",
        "41-deployment-cogneva-evolution.yaml",
        "templates/evolution.yaml",
    ),
];

/// 把算出来的预算交给进程的那个环境变量。
const BUDGET_ENV: &str = "COGNEVA_SHUTDOWN_TIMEOUT_MS";

/// 进程读预算的配置路径。
const BUDGET_PATH: &str = "system.shutdown_timeout_ms";

fn json_document() -> serde_json::Value {
    let text = read(&repo_root().join(JSON));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{JSON} does not parse: {e}"))
}

fn values_shutdown() -> serde_yaml::Value {
    let text = read(&repo_root().join(VALUES));
    let values: serde_yaml::Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    values.get("shutdown").cloned().unwrap_or_else(|| {
        panic!(
            "{VALUES} no longer has a shutdown section; the grace period and the teardown \
             margin it is split by are one decision"
        )
    })
}

fn leaf(section: &serde_yaml::Value, key: &str) -> u64 {
    section
        .get(key)
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| panic!("{VALUES}: shutdown.{key} is missing or not a number"))
}

/// 关停面的叶子就是这两个：宽限期与收尾余量。
///
/// 预算不在其中。它曾经是第三个叶子（`drainBudgetMs`），与 cogneva.json 里那份
/// 手抄同步；现在它是前两个算出来的，正因为算出来才不需要谁记得改。
#[test]
fn the_shutdown_face_has_no_third_number() {
    let shutdown = values_shutdown();
    let actual: Vec<String> = shutdown
        .as_mapping()
        .unwrap_or_else(|| panic!("{VALUES}: shutdown is not a mapping"))
        .keys()
        .filter_map(|k| k.as_str().map(str::to_string))
        .collect();
    assert!(
        !actual.iter().any(|k| k == "drainBudgetMs"),
        "shutdown.drainBudgetMs is back in {VALUES}: the budget is computed from the grace \
         minus the teardown margin, and a number written here is a second one that will drift \
         from the field kubelet enforces"
    );
    let mut sorted = actual.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            "drainTeardownMarginMs".to_string(),
            "terminationGraceSeconds".to_string()
        ],
        "the shutdown section in {VALUES} carries {actual:?}: only the grace period and the \
         teardown margin belong here, and every other number in this face is supposed to be \
         computed from them"
    );
}

/// 余量必须留在宽限期之内：算出来的预算要是零或负数，进程根本没时间排空。
#[test]
fn the_teardown_margin_fits_inside_the_grace_period() {
    let shutdown = values_shutdown();
    let grace_ms = leaf(&shutdown, "terminationGraceSeconds") * 1000;
    let margin_ms = leaf(&shutdown, "drainTeardownMarginMs");
    assert!(
        margin_ms < grace_ms,
        "shutdown.drainTeardownMarginMs ({margin_ms} ms) does not fit inside \
         shutdown.terminationGraceSeconds ({grace_ms} ms): the computed drain budget would be \
         zero or less, and the process would exit on SIGTERM without letting a single run \
         reach a turn boundary"
    );
}

/// 每个跑 agent 回合的工作负载都从 values 取宽限，并注入算出来的预算。
#[test]
fn every_workload_template_derives_its_budget_from_the_grace() {
    let grace = "terminationGracePeriodSeconds: {{ .Values.shutdown.terminationGraceSeconds }}";
    for (name, _, template) in WORKLOADS {
        let text = read(&repo_root().join("deploy/helm/cogneva").join(template));
        assert!(
            text.contains(grace),
            "{template} ({name}) must take the grace period from \
             values.shutdown.terminationGraceSeconds, not from a number written in the \
             template -- a pod that runs agent turns and is killed before its drain budget \
             spends the budget on nothing"
        );
        assert!(
            text.contains("cogneva.drainBudgetEnv"),
            "{template} ({name}) does not inject the drain budget: without it the process \
             drains under the standalone default in {JSON}, which no field enforces and no \
             test compares with the grace"
        );
    }
}

/// 那个环境变量在交付的配置里有映射，否则注入了也不写任何字段。
#[test]
fn the_budget_environment_variable_is_mapped_to_the_field_the_process_reads() {
    let document = json_document();
    let mapped = document
        .get("env")
        .and_then(|e| e.get(BUDGET_ENV))
        .and_then(|v| v.as_str());
    assert_eq!(
        mapped,
        Some(BUDGET_PATH),
        "{JSON} no longer maps {BUDGET_ENV} to {BUDGET_PATH}: the manifests set the variable, \
         the loader drops it, and the process drains under the file's default while every \
         rendered manifest reads as if it had been told"
    );
}

/// 渲染产物里，预算等于宽限期减余量，且两个工作负载都有。
#[test]
fn every_rendered_profile_carries_the_derived_budget() {
    let shutdown = values_shutdown();
    let grace = leaf(&shutdown, "terminationGraceSeconds");
    let margin = leaf(&shutdown, "drainTeardownMarginMs");
    let want = grace * 1000 - margin;
    let root = repo_root().join(RENDERED);
    let mut paths = Vec::new();
    walk_all(&root, &mut paths);
    let mut seen: Vec<String> = Vec::new();
    for (name, file, _) in WORKLOADS {
        let mut found = false;
        for path in &paths {
            if path.file_name().and_then(|n| n.to_str()) != Some(*file) {
                continue;
            }
            let text = read(path);
            assert_eq!(
                rendered_field(&text, "terminationGracePeriodSeconds"),
                Some(grace),
                "{} ({name}) renders a grace period other than values' {grace}: the rendered \
                 profile is stale -- re-run deploy/scripts/render-deploy.sh",
                path.display()
            );
            assert_eq!(
                rendered_env(&text, BUDGET_ENV),
                Some(want.to_string()),
                "{} ({name}) does not render {BUDGET_ENV} as {grace} s - {margin} ms = {want}: \
                 either the template stopped deriving it or the rendered profile is stale -- \
                 re-run deploy/scripts/render-deploy.sh",
                path.display()
            );
            seen.push(path.display().to_string());
            found = true;
        }
        assert!(
            found,
            "{RENDERED} has no {file}; this test would pass by checking nothing for {name}"
        );
    }
    assert!(
        !seen.is_empty(),
        "{RENDERED} is empty; the budget on the rendered path is unjudged"
    );
}

/// `deploy/k3s` 那两份静态清单带着同一个宽限与同一个算出来的预算：那条路径不经过
/// 渲染，渲染新鲜度那条门看不见它。
#[test]
fn the_static_manifests_carry_the_derived_budget() {
    let shutdown = values_shutdown();
    let grace = leaf(&shutdown, "terminationGraceSeconds");
    let want = grace * 1000 - leaf(&shutdown, "drainTeardownMarginMs");
    for (name, relative) in STATIC_DEPLOYMENTS {
        let text = read(&repo_root().join(relative));
        assert_eq!(
            rendered_field(&text, "terminationGracePeriodSeconds"),
            Some(grace),
            "{relative} ({name}) carries a grace period other than values' {grace}: a cluster \
             installed by `kubectl apply -k` runs the agent workload under the wrong period"
        );
        assert_eq!(
            rendered_env(&text, BUDGET_ENV),
            Some(want.to_string()),
            "{relative} ({name}) does not carry {BUDGET_ENV} = {want}: on this path the process \
             falls back to the standalone default and the manifest reads as if it had been told"
        );
    }
}

/// 一份清单里的一个数字字段。
///
/// 这不是一个 YAML 解析器：渲染产物由 chart 自己写形状，要判的是那个数与 values
/// 同不同，而不是这份文件语法是否合法。
fn rendered_field(text: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    text.lines().find_map(|line| {
        line.trim()
            .strip_prefix(prefix.as_str())
            .and_then(|rest| rest.trim().trim_matches('"').parse::<u64>().ok())
    })
}

/// 一份清单里某个环境变量的值。
///
/// 只认 `- name: <VAR>` 之后紧跟的那一行 `value:`：清单由 chart 渲染成这个形状，
/// 这里判的是它与算出来的预算同不同，不是在解析 YAML。
fn rendered_env(text: &str, name: &str) -> Option<String> {
    let needle = format!("- name: {name}");
    let lines: Vec<&str> = text.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        if line.trim() != needle {
            continue;
        }
        for follower in lines.iter().skip(index + 1).take(3) {
            if let Some(rest) = follower.trim().strip_prefix("value:") {
                return Some(rest.trim().trim_matches('"').to_string());
            }
        }
    }
    None
}

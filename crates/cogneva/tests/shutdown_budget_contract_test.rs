//! principle: feedback_an_admission_cap_makes_a_knob_a_coupled_pair
//! 关停预算是**一对**数，不是两个各自能调的旋钮。
//!
//! 进程收到 SIGTERM 之后的时间轴有四段，每段有自己的主人：
//!
//! - 钩子预算（`core.system.shutdown_timeout_ms`）：排空等在飞的运行跑到回合边界，
//!   到点就不等了。写在这里的值，进程读的是 `files/cogneva.json` 那一份。
//! - Pod 宽限期（`terminationGracePeriodSeconds`）：kubelet 从 SIGTERM 起算，
//!   到点 SIGKILL。写在这里的值，集群读的是 `values.yaml` 那一份。
//!
//! 宽限短于预算是这一对唯一会坏的方向，而它的坏法读不出来：排空照跑、日志照写
//! 「等着让路」，只是宽限期先到，SIGKILL 把还没走到回合边界的那一段连进程一起
//! 收走。事后看得到的是「这次尝试没有续跑点」——与「日志没装」同形。
//!
//! 宽限期要盖住**每一个跑 agent 运行的工作负载**，不是只盖主应用：实测 2026-11-09
//! 11:43 两个 Pod 同时在跑 —— 主应用的 `DSC-*` generator 与进化 Pod 的
//! `self-signal-alert-*` planner。只给主应用加宽限，被滚动的进化 Pod 照样在预算
//! 走完之前被 SIGKILL，而清单上看起来「已经做了」。
//!
//! 所以三件事必须在同一次读里对上：JSON 的预算、values 的预算、values 的宽限；
//! 外加两条路径上的落点（chart 渲染 / `deploy/k3s` 静态清单）与两个工作负载。
//! 只改一处而不改另一处会红在这里，而不是在集群上被 SIGKILL 的那一次。

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

/// 宽限比预算多出来的余量（毫秒）：钩子返回之后进程还要刷日志、收 tokio 运行时、
/// 让 kubelet 观测到退出。这一段没有读数可依，所以它是一个明确的覆盖值，写在这里
/// 而不是散在模板里。
// cap: override — 排空耗时还没有读数；15 秒是让日志与运行时收尾够用的手留余量
const HANDOVER_MARGIN_MS: u64 = 15_000;

fn json_budget_ms() -> u64 {
    let text = read(&repo_root().join(JSON));
    let parsed: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{JSON} does not parse: {e}"));
    parsed
        .get("system")
        .and_then(|s| s.get("shutdown_timeout_ms"))
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| {
            panic!(
                "{JSON} no longer carries system.shutdown_timeout_ms (core.system in the \
                 process); the drain budget has no reader"
            )
        })
}

fn values_shutdown() -> serde_yaml::Value {
    let text = read(&repo_root().join(VALUES));
    let values: serde_yaml::Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    values.get("shutdown").cloned().unwrap_or_else(|| {
        panic!(
            "{VALUES} no longer has a shutdown section; the budget and the grace period \
                 stopped being one decision"
        )
    })
}

fn leaf(section: &serde_yaml::Value, key: &str) -> u64 {
    section
        .get(key)
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| panic!("{VALUES}: shutdown.{key} is missing or not a number"))
}

/// 预算的两个落点是同一个数。
#[test]
fn the_drain_budget_in_the_chart_is_the_one_the_process_reads() {
    let shutdown = values_shutdown();
    assert_eq!(
        leaf(&shutdown, "drainBudgetMs"),
        json_budget_ms(),
        "shutdown.drainBudgetMs in {VALUES} and system.shutdown_timeout_ms in {JSON} \
         disagree: the drain would run on one budget while the chart documents another"
    );
}

/// 宽限必须罩住预算加余量。
#[test]
fn the_pod_grace_period_covers_the_drain_budget() {
    let shutdown = values_shutdown();
    let budget_ms = json_budget_ms();
    let grace_ms = leaf(&shutdown, "terminationGraceSeconds") * 1000;
    assert!(
        grace_ms >= budget_ms + HANDOVER_MARGIN_MS,
        "shutdown.terminationGraceSeconds ({} ms) does not cover the drain budget ({budget_ms} ms) \
         plus {HANDOVER_MARGIN_MS} ms of handover: kubelet would SIGKILL a process that is still \
         waiting for its runs to reach a turn boundary, and the drain would be silently partial",
        grace_ms
    );
}

/// 每个工作负载的模板都从 values 取值，而不是自己写一个数——写死的那个数不会
/// 跟着预算动，而且只改一个模板时另一个会安静地留在默认值上。
#[test]
fn every_workload_template_reads_the_grace_period_from_values() {
    let want = "terminationGracePeriodSeconds: {{ .Values.shutdown.terminationGraceSeconds }}";
    for (name, _, template) in WORKLOADS {
        let text = read(&repo_root().join("deploy/helm/cogneva").join(template));
        assert!(
            text.contains(want),
            "{template} ({name}) must take the grace period from \
             values.shutdown.terminationGraceSeconds, not from a number written in the \
             template -- a pod that runs agent turns and is killed before its drain budget \
             spends the budget on nothing"
        );
    }
}

/// 每个 profile 的渲染产物里，两个工作负载都带上同一个宽限。
#[test]
fn every_rendered_profile_carries_the_same_grace_period() {
    let shutdown = values_shutdown();
    let grace = leaf(&shutdown, "terminationGraceSeconds");
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
            let rendered = rendered_grace(&text).unwrap_or_else(|| {
                panic!(
                    "{} ({name}) renders no terminationGracePeriodSeconds: the pod that runs \
                     agent turns gets the 30 s default and is SIGKILLed mid-drain",
                    path.display()
                )
            });
            assert_eq!(
                rendered,
                grace,
                "{} renders terminationGracePeriodSeconds as {rendered}, values says {grace}: \
                 the rendered profile is stale -- re-run deploy/scripts/render-deploy.sh",
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
        "{RENDERED} is empty; the grace period on the rendered path is unjudged"
    );
}

/// `deploy/k3s` 那两份静态清单带着同一个宽限：那条路径不经过渲染，渲染新鲜度
/// 那条门看不见它。
#[test]
fn the_static_manifests_carry_the_same_grace_period() {
    let shutdown = values_shutdown();
    let grace = leaf(&shutdown, "terminationGraceSeconds");
    for (name, relative) in STATIC_DEPLOYMENTS {
        let text = read(&repo_root().join(relative));
        let found = rendered_grace(&text).unwrap_or_else(|| {
            panic!(
                "{relative} ({name}) carries no terminationGracePeriodSeconds: a cluster \
                 installed by `kubectl apply -k` runs the agent workload without a grace \
                 period wider than its drain budget"
            )
        });
        assert_eq!(found, grace, "{relative} says {found}, values says {grace}");
    }
}

/// 一份 Deployment 清单里进程容器的宽限期。
///
/// 只认 `kind: Deployment` 的清单（调用方已筛过），取第一条
/// `terminationGracePeriodSeconds:`。这不是一个 YAML 解析器：渲染产物由 chart
/// 自己写形状，要判的是那个数与 values 同不同，而不是这份文件语法是否合法。
fn rendered_grace(text: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        line.trim()
            .strip_prefix("terminationGracePeriodSeconds:")
            .and_then(|rest| rest.trim().parse::<u64>().ok())
    })
}

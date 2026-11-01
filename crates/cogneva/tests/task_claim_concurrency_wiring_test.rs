//! 在途执行上限这条通路：代码里读的那个名字，必须能从 values 走到 Pod 的 env。
//!
//! 要修的坏法是「开关在代码里、部署面没有它的名字」。这个数原本是写死在
//! `task_executor_router` 里的 `4`，于是「一个进程同时跑几道任务」这件事没有
//! 运维入口——而它决定的是**同时花 token 额度的条数**。实测 2026-11-01：两个
//! 执行进程各 4 条在途（`cogneva_task_router_claim_slots` / `_in_flight` 都报 4），
//! 看板上恰好 8 条 `running` 分属 8 个 goal，k3 的 5 小时窗口（1,670,569 token）
//! 在开窗 10 分钟里被花掉 1,105,275，八道变更各走一半、一道也没走完。
//!
//! 这条通路有三环，缺任何一环断在不同地方：
//!
//! 1. chart 的 ConfigMap 把这个名字送进 Pod，且取值来自 `.Values`——写死的名字送
//!    不进配置，只有名字没有值等于没有；
//! 2. `values.yaml` 里真的有那个键：引用一个不存在的 `.Values` 键会渲染成空串而
//!    不报错，parity 也因此照常绿（两侧都空）；
//! 3. `deploy/k3s/` 的静态清单（parity 基线兼 GitOps 拉取端）也带着这个名字——
//!    k3s 那条路径不经 helm，少了它，同一份 values 在两种投递方式下是两个行为。
//!
//! 名字从代码里的常量取，不在这里另抄一份：改名的正确做法是改常量并让这条测试
//! 把部署面必须跟的三处一起点名。

use crate::common;

use common::routes::read_repo;

const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const K3S_CONFIGMAP: &str = "deploy/k3s/configmap.yaml";
const VALUES_BLOCK: &str = "taskRouter:";
const VALUES_KEY: &str = "claimConcurrency:";

/// `values.yaml` 里 `taskRouter:` 段的 `claimConcurrency` 值（去掉引号与行尾注释）。
fn values_claim_concurrency() -> Option<String> {
    let values = read_repo(VALUES_YAML);
    let mut inside = false;
    for line in values.lines() {
        if line.starts_with(VALUES_BLOCK) {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        // 段的下一条顶格键就是出口：values 顶层键都不缩进。
        if !line.starts_with(' ') && !line.trim().is_empty() {
            return None;
        }
        let Some(rest) = line.trim().strip_prefix(VALUES_KEY) else {
            continue;
        };
        let value = rest.split('#').next().unwrap_or("").trim();
        return Some(value.trim_matches('"').to_string());
    }
    None
}

/// 把 env 名送到 Pod 的那一行：名字在左，取值来自 `.Values`。
#[test]
fn the_chart_delivers_the_env_name_from_values() {
    let name = cog_orchestrator::task_executor_router::CLAIM_CONCURRENCY_ENV;
    let template = read_repo(CHART_CONFIGMAP);
    let line = template
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{name}:")));
    let Some(line) = line else {
        panic!("chart 的 ConfigMap 没有把 {name} 送进 Pod：进程读到的永远是写死的默认值");
    };
    assert!(
        line.contains(".Values.taskRouter.claimConcurrency"),
        "ConfigMap 里的 {name} 不是从 values 取的（这一行是 {line:?}）：写死的名字送不进配置"
    );
}

/// 引用的那个 values 键必须真的存在，否则渲染成空串而不报错。
#[test]
fn the_values_key_exists_and_reads_as_a_count() {
    let value = values_claim_concurrency().unwrap_or_else(|| {
        panic!("values.yaml 的 {VALUES_BLOCK} 段里没有 {VALUES_KEY}：引用不存在的键会渲染成空串")
    });
    let parsed = cog_orchestrator::task_executor_router::parse_claim_concurrency(&value)
        .unwrap_or_else(|problem| panic!("values.yaml 里的 {VALUES_KEY} 进程读不动：{problem}"));
    assert!(parsed > 0, "解析成功却得到 0 个许可：所有执行都会停摆");
}

/// 静态清单那条路径也要有这个名字：k3s 投递不经 helm，少了它就是两种投递两个行为。
#[test]
fn the_k3s_static_configmap_carries_the_same_name() {
    let name = cog_orchestrator::task_executor_router::CLAIM_CONCURRENCY_ENV;
    let static_manifest = read_repo(K3S_CONFIGMAP);
    assert!(
        static_manifest
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{name}:"))),
        "deploy/k3s/configmap.yaml 里没有 {name}：同一个部署经静态清单投递时这个开关不存在"
    );
}

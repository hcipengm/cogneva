//! 看板列上限这条通路：代码里读的那个名字，必须能从 values 走到 Pod 的 env。
//!
//! 要修的坏法是「开关在代码里、部署面没有它的名字」：进程读
//! `COGNEVA_BOARD_WIP_LIMITS`，而 chart 的 ConfigMap 没有这一行——每个 Pod 读到的
//! 都是「没配」，运维在 values 里写下的上限一条也拦不住，而且**没有任何读数会说
//! 这件事**：没配与配了没生效在发布侧是同一个行为（照发），在看板读面上是同一个
//! 空白（`wip_limit: null`）。
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
//! 名字从代码里的常量取，不在这里另抄一份：改名的正确做法是改常量并让这条测试把
//! 部署面必须跟的三处一起点名，而不是让两个地方各写一个字符串再各自漂移。
//!
//! 默认值也在这里判：`wipLimits: ""` 就是「没配」。写成一个具体数等于替所有人选
//! 了一条准入闸门，而这条闸门会真的拦住派发。

use std::path::PathBuf;

const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const K3S_CONFIGMAP: &str = "deploy/k3s/configmap.yaml";
const VALUES_BLOCK: &str = "board:";
const VALUES_KEY: &str = "wipLimits:";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// `values.yaml` 里 `board:` 段的 `wipLimits` 值（去掉引号与行尾注释）。
fn values_wip_limits() -> Option<String> {
    let values = read(VALUES_YAML);
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
    let name = cog_core::config::BOARD_WIP_LIMITS_ENV;
    let template = read(CHART_CONFIGMAP);
    let line = template
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{name}:")));
    let Some(line) = line else {
        panic!("chart 的 ConfigMap 没有把 {name} 送进 Pod：进程读到的永远是「没配」");
    };
    assert!(
        line.contains(".Values.board.wipLimits"),
        "ConfigMap 里的 {name} 不是从 values 取的（这一行是 {line:?}）：写死的名字送不进配置"
    );
}

/// 引用的那个 values 键必须真的存在，否则渲染成空串而不报错。
#[test]
fn the_values_key_exists_and_defaults_to_no_limit() {
    let value = values_wip_limits().unwrap_or_else(|| {
        panic!("values.yaml 的 {VALUES_BLOCK} 段里没有 {VALUES_KEY}：引用不存在的键会渲染成空串")
    });
    assert_eq!(
        value, "",
        "看板列上限的默认值必须是空串（没配＝不限）。写成一个数就是替所有人选了一条会真的拦下派发的闸门"
    );
}

/// 静态清单那条路径也要有这个名字：k3s 投递不经 helm，少了它就是两种投递两个行为。
#[test]
fn the_k3s_static_configmap_carries_the_same_name() {
    let name = cog_core::config::BOARD_WIP_LIMITS_ENV;
    let static_manifest = read(K3S_CONFIGMAP);
    assert!(
        static_manifest
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{name}:"))),
        "deploy/k3s/configmap.yaml 里没有 {name}：同一个部署经静态清单投递时这个开关不存在"
    );
}

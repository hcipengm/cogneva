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

use crate::common;

use common::routes::read_repo;

const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const K3S_CONFIGMAP: &str = "deploy/k3s/configmap.yaml";
const VALUES_BLOCK: &str = "board:";
const VALUES_KEY: &str = "wipLimits:";

/// `values.yaml` 里 `board:` 段的 `wipLimits` 值（去掉引号与行尾注释）。
fn values_wip_limits() -> Option<String> {
    common::yaml::values_entry(VALUES_YAML, VALUES_BLOCK, VALUES_KEY)
}

/// 把 env 名送到 Pod 的那一行：名字在左，取值来自 `.Values`。
#[test]
fn the_chart_delivers_the_env_name_from_values() {
    let name = cog_core::config::BOARD_WIP_LIMITS_ENV;
    let template = read_repo(CHART_CONFIGMAP);
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

/// 引用的那个 values 键必须真的存在，而且它的值必须是一条**发布侧真能用的**规格。
///
/// 这一条此前判的是「必须是空串」，因为它怕的是有人凭空写下一个数、替所有人
/// 选了一条会真的拦下派发的闸门。属主 2026-11-01 明确选了 `running=1`（「需求再大
/// 也是串行一个个解决」），于是那个断言变成了「谁按属主的决定配一次就红」——它拦住
/// 的不再是凭空写下的数，而是这条配置本身。
///
/// 判据换成型而不是换成「随便」：值必须能被发布侧那个解析器读成合法的上限（拼错
/// 的列名、`0`、只增不减的终态列都会被它拒并给出原因），空串仍然合法——那正是
/// 「没配＝不限」。另外只许配 `running`：`scheduled` 压的是派发而不是 token，会把
/// 长任务后面所有短任务一起堵死，这条决定写在 values 的注释里，也在这里判住。
#[test]
fn the_values_key_holds_a_spec_the_publisher_can_use() {
    let value = values_wip_limits().unwrap_or_else(|| {
        panic!("values.yaml 的 {VALUES_BLOCK} 段里没有 {VALUES_KEY}：引用不存在的键会渲染成空串")
    });
    let (limits, problems) = cog_core::BoardWipLimits::from_spec(&value);
    assert!(
        problems.is_empty(),
        "values.yaml 里的 {VALUES_KEY} 发布侧读不动（{problems:?}）：写错的条目会被丢掉且不生效"
    );
    assert!(
        limits.limit_for("scheduled").is_none(),
        "配了 scheduled 的列上限：压下去的是派发而不是 token，长任务会把后面的短任务一起堵死"
    );
}

/// 静态清单那条路径也要有这个名字：k3s 投递不经 helm，少了它就是两种投递两个行为。
#[test]
fn the_k3s_static_configmap_carries_the_same_name() {
    let name = cog_core::config::BOARD_WIP_LIMITS_ENV;
    let static_manifest = read_repo(K3S_CONFIGMAP);
    assert!(
        static_manifest
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{name}:"))),
        "deploy/k3s/configmap.yaml 里没有 {name}：同一个部署经静态清单投递时这个开关不存在"
    );
}

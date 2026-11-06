//! 变更账的批大小这条通路：代码里读的那个名字，必须能从 values 走到 Pod 的 env。
//!
//! 要修的坏法是「开关在代码里、部署面没有它的名字」。一笔任务尝试实测约 102 次
//! LLM 调用、约 ¥1，而这笔钱的大头是固定部分——一次规划、一轮上下文、一轮沙盒
//! 验证——不随任务承载几条增长。账上 1,957 条待办、其中 1,694 条同一形状，一条
//! 一道任务就是把这同一份固定价格付了 1,957 次。于是折行成了一件事，而「一次折
//! 几条」必须能改：它由生成侧一次写得完多少决定，那是一个会随模型变的数，不是一
//! 个写死在代码里的界。
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
//! 名字与解析都从代码里取，不在这里另抄一份：改名的正确做法是改常量并让这条测试
//! 把部署面必须跟的三处一起点名。

use crate::common;

use common::routes::read_repo;

const CHART_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";
const VALUES_YAML: &str = "deploy/helm/cogneva/values.yaml";
const K3S_CONFIGMAP: &str = "deploy/k3s/configmap.yaml";
const VALUES_BLOCK: &str = "changeLedger:";
const VALUES_KEY: &str = "rowsPerHandover:";

/// 把 env 名送到 Pod 的那一行：名字在左，取值来自 `.Values`。
#[test]
fn the_chart_delivers_the_batch_size_from_values() {
    let name = cog_orchestrator::change_ledger::ROWS_PER_HANDOVER_ENV;
    let template = read_repo(CHART_CONFIGMAP);
    let line = template
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{name}:")));
    let Some(line) = line else {
        panic!("chart 的 ConfigMap 没有把 {name} 送进 Pod：折行就只能改代码才能改");
    };
    assert!(
        line.contains(".Values.changeLedger.rowsPerHandover"),
        "ConfigMap 里的 {name} 不是从 values 取的（这一行是 {line:?}）：写死的名字送不进配置"
    );
}

/// 引用的那个 values 键必须真的存在，否则渲染成空串而不报错。
#[test]
fn the_values_key_exists_and_reads_as_a_batch_size() {
    let value =
        common::yaml::values_entry(VALUES_YAML, VALUES_BLOCK, VALUES_KEY).unwrap_or_else(|| {
            panic!(
                "values.yaml 的 {VALUES_BLOCK} 段里没有 {VALUES_KEY}：引用不存在的键会渲染成空串"
            )
        });
    let parsed = cog_orchestrator::change_ledger::parse_rows_per_handover(&value)
        .unwrap_or_else(|problem| panic!("values.yaml 里的 {VALUES_KEY} 进程读不动：{problem}"));
    assert!(
        parsed > 0,
        "解析成功却得到 0 行一批：一拍会一直攒、一条也派不出去"
    );
}

/// 静态清单那条路径也要有这个名字：k3s 投递不经 helm，少了它就是两种投递两个行为。
#[test]
fn the_k3s_static_configmap_carries_the_same_name() {
    let name = cog_orchestrator::change_ledger::ROWS_PER_HANDOVER_ENV;
    let static_manifest = read_repo(K3S_CONFIGMAP);
    assert!(
        static_manifest
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{name}:"))),
        "deploy/k3s/configmap.yaml 里没有 {name}：同一个部署经静态清单投递时这个开关不存在"
    );
}

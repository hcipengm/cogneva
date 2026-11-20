//! 切到远端对象后端之后，本地那棵树里还剩多少东西。
//!
//! 换 provider 是搬数据，不是改配置：远端后端读的是桶，`local-fs` 时期写在
//! `base_path` 上的对象一个都看不见，而这条路上没有任何地方会报错——部署照常
//! 起来，只是面对一个空库。启动日志会数一次（`plugin.rs`），但日志只有当场
//! 看着进程起来的人读得到；重启一次、换个进程，那行字就没了。所以同一个数也
//! 从进程自己的抓取面送出去：谁在看板、谁在告警上，问的是同一个问题。
//!
//! 读数只有一条、在启动那一刻定下：数到 0 是一条 0 的 gauge，数到 N 是 N，
//! 读不出来是**另一条**序列上的 1。后两者不能并成一条——把「读不出来」写成
//! 0，卷没挂上就会被读成「已经搬完了」，而这两件事要做的动作正相反。
//!
//! 它走的是抓取面（`Observable`）而不是共享表：这条读数只写一次，落进共享表
//! 之后这个进程再也不会更新它，进程没了那行还在——「写者停了它不会缺席，只会
//! 把最后一笔值永久送出去」正是共享表 gauge 的读法。抓取面渲染的是本进程此刻
//! 的那一笔，进程走了序列跟着走，不存在冻结的值。

use std::sync::OnceLock;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// 非 `local-fs` 后端下，`base_path` 里还留着的对象数。
pub const LEGACY_OBJECTS_METRIC: &str = "cogneva_object_store_legacy_objects";

/// 那次清点没读成：不是零个，是不知道。
pub const LEGACY_SCAN_UNREADABLE_METRIC: &str = "cogneva_object_store_legacy_scan_unreadable";

/// 读 [`LEGACY_OBJECTS_METRIC`] 的告警规则名（交付文档里的那一条）。
pub const LEGACY_OBJECTS_RULE: &str = "object_store_legacy_objects_left_behind";

/// 读 [`LEGACY_SCAN_UNREADABLE_METRIC`] 的告警规则名。
pub const LEGACY_SCAN_UNREADABLE_RULE: &str = "object_store_legacy_tree_unreadable";

/// 哪个后端在跑——决定这棵树里的东西是不是「看不见的」。
pub const LABEL_PROVIDER: &str = "provider";

/// 要搬的那棵树。标签值就是这个路径：一条告警要能点名人去搬哪个目录。
pub const LABEL_PATH: &str = "path";

/// 启动时那一次清点的结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyObjectTree {
    /// 正在跑的后端。
    pub provider: String,
    /// 清点的是哪棵树。
    pub path: String,
    /// 数到的对象数；`None` 是「没读成」，不是零。
    pub objects: Option<usize>,
}

static SCAN: OnceLock<LegacyObjectTree> = OnceLock::new();

/// 记下这次清点。每个进程只会清点一次，第二次调用不覆盖第一次的结果——
/// 同一个进程里换 provider 不是运行期会发生的事，而让后来的调用悄悄改掉
/// 已经答过的问题，会让「这个进程启动时数到了什么」变成没人答得上来。
pub fn record_scan(tree: LegacyObjectTree) {
    let _ = SCAN.set(tree);
}

/// 本次进程记下的那次清点。
pub fn recorded_scan() -> Option<&'static LegacyObjectTree> {
    SCAN.get()
}

/// 一次清点该渲染成哪一条序列。
///
/// 分成两条序列而不是给同一条加 `outcome` 标签：值本身说的是两件事
/// （多少个 / 读没读成），混在一个名字下，读的人得先看标签才知道数字怎么读。
pub fn readings_for(tree: &LegacyObjectTree) -> Vec<RawMetric> {
    let (name, value) = match tree.objects {
        Some(objects) => (LEGACY_OBJECTS_METRIC, objects as f64),
        None => (LEGACY_SCAN_UNREADABLE_METRIC, 1.0),
    };
    vec![RawMetric::new(name, value)
        .with_label(LABEL_PROVIDER, tree.provider.as_str())
        .with_label(LABEL_PATH, tree.path.as_str())]
}

/// 把那次清点送进本进程的抓取面。
///
/// 没有清点过（`local-fs`，或者后端与库共用 `data_dir` 而 `base_path` 没说
/// 另指一处）时什么都不渲染：这棵树没有「看不见的对象」这个问题，静就是答案。
pub struct LegacyObjectTreeObservable;

#[async_trait]
impl Observable for LegacyObjectTreeObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(recorded_scan().map(readings_for).unwrap_or_default())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// 它说的是本进程启动时那一次清点，与谁在问无关。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(objects: Option<usize>) -> LegacyObjectTree {
        LegacyObjectTree {
            provider: "seaweedfs".to_string(),
            path: "/var/lib/cogneva-data/storage".to_string(),
            objects,
        }
    }

    /// 数到 N：一条序列，值是 N，两个标签都在——告警要把路径念给去搬的人听。
    #[test]
    fn the_count_is_published_with_the_tree_it_was_found_under() {
        let readings = readings_for(&tree(Some(12)));
        assert_eq!(readings.len(), 1);
        let reading = &readings[0];
        assert_eq!(reading.name, LEGACY_OBJECTS_METRIC);
        assert_eq!(reading.value, 12.0);
        assert_eq!(
            reading.labels.get(LABEL_PROVIDER).map(String::as_str),
            Some("seaweedfs")
        );
        assert_eq!(
            reading.labels.get(LABEL_PATH).map(String::as_str),
            Some("/var/lib/cogneva-data/storage")
        );
    }

    /// 数到 0 与「没读成」是两条序列上的两个答案，形状上分得开：
    /// 前者是 0 的计数，后者根本不用计数那条名字。
    #[test]
    fn an_empty_tree_and_an_unreadable_one_are_not_the_same_reading() {
        let empty = readings_for(&tree(Some(0)));
        let unreadable = readings_for(&tree(None));
        assert_eq!(empty[0].name, LEGACY_OBJECTS_METRIC);
        assert_eq!(empty[0].value, 0.0);
        assert_eq!(unreadable[0].name, LEGACY_SCAN_UNREADABLE_METRIC);
        assert_ne!(empty[0].name, unreadable[0].name);
        // 读不出时不许同时报一个计数：那会让人以为数过。
        assert!(!unreadable.iter().any(|m| m.name == LEGACY_OBJECTS_METRIC));
    }

    /// 没有清点时一条都不渲染：静不是 0，也不是坏消息。
    #[tokio::test]
    async fn nothing_is_rendered_before_a_scan_was_recorded() {
        if recorded_scan().is_some() {
            // 同一测试进程里的另一个用例先记过一次；这条用例判的是「没清点」
            // 那条路，它只好让开，而不是伪造一个别的答案。
            return;
        }
        let readings = LegacyObjectTreeObservable
            .collect_metrics("")
            .await
            .unwrap();
        assert!(readings.is_empty());
    }

    /// 这个读数不随维度变：它说的是本进程，谁在问都一样。
    #[test]
    fn the_reading_does_not_branch_on_a_dimension() {
        assert!(LegacyObjectTreeObservable.available_dimensions().is_empty());
    }

    /// 两条规则名与它们读的序列名都写在这一处，规则文件里的名字才不会各写各的。
    #[test]
    fn each_reading_names_the_rule_that_reads_it() {
        assert!(LEGACY_OBJECTS_RULE.starts_with("object_store_legacy_"));
        assert!(LEGACY_SCAN_UNREADABLE_RULE.starts_with("object_store_legacy_"));
        assert_ne!(LEGACY_OBJECTS_RULE, LEGACY_SCAN_UNREADABLE_RULE);
    }
}

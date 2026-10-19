//! 进程自报：它从哪个数据根加载了哪些策略库、读到哪个 ACTIVE 版本、有多少决策证据。
//!
//! 备份包写回成功与「新进程真的把它们加载了」是两件事。前者的证据在包里（演练逐库
//! 读回），后者的证据只可能来自进程自己：一个挂错了卷、或 `data_dir` 指到别处的新
//! 进程，会安静地跑在一个空库上，而恢复端、包、回执全都写着成功。所以这里把「我加载
//! 到的是什么」发布成读数，读数是按 `data_dir` 与库名标注的——同一块 PVC 上两个不同
//! 的 `data_dir` 因此是两个库，而不是同一个库被读了两遍。
//!
//! 恢复端还会写一张交接单（[`RestoreExpectation`]）：包里的 ACTIVE 版本与试验数。
//! 进程启动时对一次，把期望与实读各发布一格，并把它消费掉。两格分开而不是发布一个
//! 布尔：判词「不一致」要能指出哪一侧是多少，否则只有结论没有现场。
//!
//! 为什么不清空数据就重来的那次不用这张单：单子在核对成功后删除，之后重启不再对——
//! 策略库会随进化往前走，拿一份旧包里的版本当标准，每次重启都会红，而它红得没有
//! 意义。核对失败时单子留着，下一次启动仍然红。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cog_core::{metric_names, MetricsBackend};
use serde::{Deserialize, Serialize};

use crate::{MetaLearningEngine, PolicyStore};

/// 元学习策略库的名字。恢复端的交接单、进程的读数和引擎挂载用的是同一个名字，
/// 三处各写一份字面量就会在改名那天分成两半。
pub const META_LEARNING_POLICY: &str = "meta_learning.mode";

/// 恢复端写进数据根的交接单：包里的 ACTIVE 版本与决策证据规模。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreExpectation {
    pub version: u32,
    /// 包是什么时候做的、记的哪个源码 rev：排障时用来判断「这份单子是不是这一次的」。
    pub package_created_at: String,
    pub package_revision: String,
    pub policy_stores: Vec<ExpectedStore>,
    #[serde(default)]
    pub decision_trials: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedStore {
    pub name: String,
    pub active_version: u64,
}

impl RestoreExpectation {
    pub const VERSION: u32 = 1;

    /// 交接单在数据根里的位置。放在 `meta_learning/` 而不是数据根顶层：那一层已经
    /// 被「每个一级子目录都要分类」的门管着，为一张一次性单子新开一格，等于让每个
    /// 部署都得为它写一条分类。
    pub fn path_in(data_dir: &Path) -> PathBuf {
        data_dir
            .join("meta_learning")
            .join("restore_expectation.json")
    }

    /// 取走交接单：读到就消费，读不出来就当作没有。
    ///
    /// 删除失败只告警不失败：单子留着的最坏后果是下一次启动再对一遍，而对不上的那次
    /// 本来就该继续红。反过来，因为删不掉而拒绝启动，会把「核对」这件事变成恢复路上
    /// 新的单点。
    pub fn take(data_dir: &Path) -> Option<Self> {
        let path = Self::path_in(data_dir);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "restore expectation unreadable");
                return None;
            }
        };
        let expectation: Self = match serde_json::from_str(&text) {
            Ok(expectation) => expectation,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "restore expectation is not the shape a restore writes"
                );
                return None;
            }
        };
        Some(expectation)
    }

    /// 核对通过后把它拿掉。没有单子留下来，下一次启动就不会拿一份过期的版本当标准。
    pub fn discard(data_dir: &Path) {
        let path = Self::path_in(data_dir);
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "restore expectation could not be removed after a successful check; the next \
                     start will check the same package again, and a policy that has moved on \
                     since then will be reported as a mismatch"
                );
            }
        }
    }
}

/// 一个进程加载的策略库集合，加上它自己的决策证据。
pub struct StoreReadouts {
    data_dir: PathBuf,
    stores: Vec<(String, PolicyStore)>,
    engine: Arc<MetaLearningEngine>,
    metrics: Option<Arc<dyn MetricsBackend>>,
}

impl StoreReadouts {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        stores: Vec<(String, PolicyStore)>,
        engine: Arc<MetaLearningEngine>,
        metrics: Option<Arc<dyn MetricsBackend>>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            stores,
            engine,
            metrics,
        }
    }

    /// 发布「这个进程现在加载的是什么」。
    ///
    /// 每次抓取前都重算，而不是启动时算一次：ACTIVE 会随进化移动、证据会随决策增长，
    /// 一份只写一次的读数在第一次进化之后就开始说旧事。
    pub async fn publish(&self) {
        // 没有指标后端就不发布：它不该拦住启动，而「发布了但没人收」与「没发布」
        // 在读数面上同形，所以一行日志说清是后者。
        if self.metrics.is_none() {
            tracing::debug!(
                data_dir = %self.data_dir.display(),
                "no metrics backend published; this process's loaded stores are not on the face"
            );
            return;
        }
        let data_dir = self.data_dir.display().to_string();
        for (name, store) in &self.stores {
            let labels = |readout: &str| {
                let mut labels = HashMap::new();
                labels.insert("readout".to_string(), readout.to_string());
                labels.insert("data_dir".to_string(), data_dir.clone());
                labels.insert("store".to_string(), name.clone());
                labels
            };
            // 这一格不是「1 = 正常」，而是「这个进程在这里、从这条路径装载」的声明。
            // 它的价值在它的缺席：一条 active_version 读数若没有同一组标签的声明，
            // 读者就分不清那是「加载了但版本读不出来」还是「另一个进程报的」。
            self.record(labels("data_dir"), 1.0).await;
            // ACTIVE 读不出来（目录不在、文件坏了）报 -1，而不是 0：版本号从 1 起，
            // 0 会被读成「有一个第 0 版」，-1 在任何比较里都不等于一个真版本。
            let active = match store.load_active(name).await {
                Ok(Some(artifact)) => artifact.version as f64,
                Ok(None) => 0.0,
                Err(e) => {
                    tracing::warn!(store = %name, error = %e, "policy store active version unreadable");
                    -1.0
                }
            };
            self.record(labels("active_version"), active).await;
        }

        let groups = self.engine.decision_groups().await;
        let trials: u64 = groups
            .iter()
            .flat_map(|g| g.values())
            .map(|(attempts, _)| u64::from(*attempts))
            .sum();
        for (readout, value) in [
            ("decision_groups", groups.len() as f64),
            ("decision_trials", trials as f64),
        ] {
            let mut labels = HashMap::new();
            labels.insert("readout".to_string(), readout.to_string());
            labels.insert("data_dir".to_string(), data_dir.clone());
            labels.insert("store".to_string(), META_LEARNING_POLICY.to_string());
            self.record(labels, value).await;
        }
    }

    /// 对一次恢复交接单：把包里的期望与本进程实读各发布一格，核对通过就把单子消费掉。
    ///
    /// 期望那一格是**这个进程启动时**读到的包事实，实读那一格是同一刻的自己；两者
    /// 都只在这一次启动发布，之后不再刷新——策略库随后会移动，刷新其中一侧就成了
    /// 拿两个时刻的数比大小。
    pub async fn check_restore(&self, expectation: &RestoreExpectation) {
        if self.metrics.is_none() {
            return;
        }
        let data_dir = self.data_dir.display().to_string();
        let mut agreed = true;
        for expected in &expectation.policy_stores {
            let Some((name, store)) = self.stores.iter().find(|(n, _)| n == &expected.name) else {
                // 包里有的库这个进程没有加载：这本身就是不一致，而且它没有实读可发布。
                tracing::warn!(
                    store = %expected.name,
                    "the package restored a policy store this process does not load"
                );
                agreed = false;
                continue;
            };
            let loaded = match store.load_active(name).await {
                Ok(Some(artifact)) => artifact.version as f64,
                Ok(None) => 0.0,
                Err(e) => {
                    tracing::warn!(store = %name, error = %e, "policy store active version unreadable");
                    -1.0
                }
            };
            let labels = |readout: &str| {
                let mut labels = HashMap::new();
                labels.insert("readout".to_string(), readout.to_string());
                labels.insert("data_dir".to_string(), data_dir.clone());
                labels.insert("store".to_string(), name.clone());
                labels
            };
            self.record(
                labels("restore_expected_active_version"),
                expected.active_version as f64,
            )
            .await;
            self.record(labels("restore_loaded_active_version"), loaded)
                .await;
            if (expected.active_version as f64) != loaded {
                agreed = false;
                tracing::warn!(
                    store = %name,
                    expected = expected.active_version,
                    loaded,
                    "the restored package holds one ACTIVE version and this process loaded another"
                );
            }
        }
        if let Some(expected_trials) = expectation.decision_trials {
            let loaded_trials: f64 = self
                .engine
                .decision_groups()
                .await
                .iter()
                .flat_map(|g| g.values())
                .map(|(attempts, _)| f64::from(*attempts))
                .sum();
            let labels = |readout: &str| {
                let mut labels = HashMap::new();
                labels.insert("readout".to_string(), readout.to_string());
                labels.insert("data_dir".to_string(), data_dir.clone());
                labels.insert("store".to_string(), META_LEARNING_POLICY.to_string());
                labels
            };
            self.record(
                labels("restore_expected_decision_trials"),
                expected_trials as f64,
            )
            .await;
            self.record(labels("restore_loaded_decision_trials"), loaded_trials)
                .await;
            // 只判「少了」：包之后演化出来的证据会让实读更大，那是正常的；包里的
            // 一条证据不见了才是恢复没落到这个进程上。
            if loaded_trials < expected_trials as f64 {
                agreed = false;
                tracing::warn!(
                    expected = expected_trials,
                    loaded = loaded_trials,
                    "the restored package holds more decision evidence than this process loaded"
                );
            }
        }
        if agreed {
            RestoreExpectation::discard(&self.data_dir);
        }
    }

    async fn record(&self, labels: HashMap<String, String>, value: f64) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        if let Err(e) = metrics
            .record_gauge(metric_names::POLICY_STORE_READINGS, value, labels.clone())
            .await
        {
            tracing::warn!(
                readout = labels.get("readout").map(String::as_str).unwrap_or(""),
                error = %e,
                "policy store reading could not be published"
            );
        }
    }
}

/// 这条循环在活跃登记表里的名字，也是 `cogneva_loop_tick_age_seconds{loop=...}` 的值。
pub const STORE_READOUTS_LOOP: &str = "reflection_store_readouts";

/// 读数多久刷新一次。
///
/// 要短于读它的告警规则的写者窗口（规则那边用 1800 秒判「写者还在写」），否则一条
/// 正常的循环会因为自己的节奏被判成停了；也不能太短，策略库的 ACTIVE 一天动不了
/// 几次，太快只是让同一份值反复落库。
pub const STORE_READOUTS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// 挂上刷新循环。
///
/// 为什么要有这条循环而不是只在启动时发布一次：ACTIVE 会随进化移动、决策证据会增长，
/// 一份只写一次的读数在第一次进化之后说的就是旧事；而读它的规则读的是「此刻」。循环
/// 自己的存活由 `loop_health` 报，不由它发布的读数报——读数停了与值没变在面上同形。
pub fn spawn_store_readouts_loop(
    readouts: Arc<StoreReadouts>,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let interval = STORE_READOUTS_INTERVAL;
    cog_core::loop_health::spawn(
        STORE_READOUTS_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // 每次重试都重建，所以循环体要用的东西在这里克隆。
        move |beat| {
            let readouts = Arc::clone(&readouts);
            let shutdown = shutdown.clone();
            async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => return,
                    }
                    readouts.publish().await;
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{DecisionCategory, DecisionGroupKey, DecisionOutcome};

    fn labels_of(sample: &cog_core::MetricSample) -> (String, String, String) {
        (
            sample.labels.get("readout").cloned().unwrap_or_default(),
            sample.labels.get("data_dir").cloned().unwrap_or_default(),
            sample.labels.get("store").cloned().unwrap_or_default(),
        )
    }

    async fn fixture(
        dir: &std::path::Path,
    ) -> (
        PolicyStore,
        Arc<MetaLearningEngine>,
        Arc<cog_storage::MemoryMetricsBackend>,
    ) {
        let store = PolicyStore::new(dir.join("policies"));
        store
            .save_new_version(
                META_LEARNING_POLICY,
                serde_json::json!({"min_samples": 40}),
                "t",
            )
            .await
            .unwrap();
        store.activate(META_LEARNING_POLICY, 1).await.unwrap();
        let engine = Arc::new(
            MetaLearningEngine::with_durable_state(
                Arc::new(crate::InMemoryRecorder::new()),
                crate::DecisionStatsSnapshot::new(dir.join("meta_learning/decision_stats.json")),
            )
            .await,
        );
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        (store, engine, metrics)
    }

    /// 读数按 `data_dir` 与库名分开：同一块卷上两个数据根是两个库，合成一条就没有
    /// 任何一个数字能说明是哪个进程加载了什么。
    #[tokio::test]
    async fn publish_reports_the_store_by_data_dir_and_version() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, metrics) = fixture(dir.path()).await;
        let readouts = StoreReadouts::new(
            dir.path(),
            vec![(META_LEARNING_POLICY.to_string(), store)],
            engine.clone(),
            Some(metrics.clone()),
        );

        readouts.publish().await;
        let samples = metrics
            .query_gauge_latest(metric_names::POLICY_STORE_READINGS.as_str())
            .await
            .unwrap();
        let by_readout: HashMap<String, f64> =
            samples.iter().map(|s| (labels_of(s).0, s.value)).collect();
        let data_dir = dir.path().display().to_string();
        for sample in &samples {
            let (_, got_dir, got_store) = labels_of(sample);
            assert_eq!(got_dir, data_dir, "每条都带着它来自哪个数据根");
            assert_eq!(got_store, META_LEARNING_POLICY);
        }
        assert_eq!(by_readout["data_dir"], 1.0);
        assert_eq!(by_readout["active_version"], 1.0);
        assert_eq!(by_readout["decision_trials"], 0.0);
        assert_eq!(by_readout["decision_groups"], 0.0);

        // 决策记进去之后同一轮发布要跟上：只写一次的读数在第一次决策之后就说旧事。
        engine
            .record(
                DecisionCategory::PgeMode,
                &DecisionGroupKey::by_task_type("bugfix"),
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();
        readouts.publish().await;
        let samples = metrics
            .query_gauge_latest(metric_names::POLICY_STORE_READINGS.as_str())
            .await
            .unwrap();
        let by_readout: HashMap<String, f64> =
            samples.iter().map(|s| (labels_of(s).0, s.value)).collect();
        assert_eq!(by_readout["decision_trials"], 1.0);
        assert_eq!(by_readout["decision_groups"], 1.0);
    }

    /// 恢复核对：期望与实读各发布一格；对上了就把交接单消费掉。
    #[tokio::test]
    async fn check_restore_reports_both_sides_and_consumes_the_hand_over() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, metrics) = fixture(dir.path()).await;
        let readouts = StoreReadouts::new(
            dir.path(),
            vec![(META_LEARNING_POLICY.to_string(), store)],
            engine.clone(),
            Some(metrics.clone()),
        );
        engine
            .record(
                DecisionCategory::PgeMode,
                &DecisionGroupKey::by_task_type("bugfix"),
                "pipeline",
                DecisionOutcome::Success,
            )
            .await
            .unwrap();
        let path = RestoreExpectation::path_in(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec(&RestoreExpectation {
                version: RestoreExpectation::VERSION,
                package_created_at: "2026-10-19T00:00:00Z".into(),
                package_revision: "abc".into(),
                policy_stores: vec![ExpectedStore {
                    name: META_LEARNING_POLICY.into(),
                    active_version: 1,
                }],
                decision_trials: Some(1),
            })
            .unwrap(),
        )
        .unwrap();

        let expectation = RestoreExpectation::take(dir.path()).unwrap();
        readouts.check_restore(&expectation).await;

        let samples = metrics
            .query_gauge_latest(metric_names::POLICY_STORE_READINGS.as_str())
            .await
            .unwrap();
        let by_readout: HashMap<String, f64> =
            samples.iter().map(|s| (labels_of(s).0, s.value)).collect();
        assert_eq!(by_readout["restore_expected_active_version"], 1.0);
        assert_eq!(by_readout["restore_loaded_active_version"], 1.0);
        assert_eq!(by_readout["restore_expected_decision_trials"], 1.0);
        assert_eq!(by_readout["restore_loaded_decision_trials"], 1.0);
        assert!(
            !path.exists(),
            "对上之后交接单要消费掉，否则下次启动拿旧包当标准"
        );
    }

    /// 对不上：两格的数各自留下，交接单不消费——下一次启动仍然要报同一个差。
    #[tokio::test]
    async fn a_mismatch_keeps_the_hand_over_for_the_next_start() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, metrics) = fixture(dir.path()).await;
        let readouts = StoreReadouts::new(
            dir.path(),
            vec![(META_LEARNING_POLICY.to_string(), store)],
            engine,
            Some(metrics.clone()),
        );
        let path = RestoreExpectation::path_in(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec(&RestoreExpectation {
                version: RestoreExpectation::VERSION,
                package_created_at: "2026-10-19T00:00:00Z".into(),
                package_revision: "abc".into(),
                policy_stores: vec![ExpectedStore {
                    name: META_LEARNING_POLICY.into(),
                    active_version: 7,
                }],
                decision_trials: Some(4),
            })
            .unwrap(),
        )
        .unwrap();

        let expectation = RestoreExpectation::take(dir.path()).unwrap();
        readouts.check_restore(&expectation).await;

        let samples = metrics
            .query_gauge_latest(metric_names::POLICY_STORE_READINGS.as_str())
            .await
            .unwrap();
        let by_readout: HashMap<String, f64> =
            samples.iter().map(|s| (labels_of(s).0, s.value)).collect();
        assert_eq!(by_readout["restore_expected_active_version"], 7.0);
        assert_eq!(by_readout["restore_loaded_active_version"], 1.0);
        assert_eq!(by_readout["restore_expected_decision_trials"], 4.0);
        assert_eq!(by_readout["restore_loaded_decision_trials"], 0.0);
        assert!(path.exists(), "没核对上就不能把单子拿掉");
    }

    /// 没有交接单是正常启动：不发布、不删除、不失败。
    #[tokio::test]
    async fn no_hand_over_is_an_ordinary_start() {
        let dir = tempfile::tempdir().unwrap();
        assert!(RestoreExpectation::take(dir.path()).is_none());
    }
}

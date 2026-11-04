//! 安全网关的可观测面装配。
//!
//! 网关只认 `cog_core` 的契约（`ScrapableMetrics`、`PersistentAlertSink`、
//! `AnalyticsSink`、`LlmUsageLedger`、`Observable`），实现都在 `cog-observability`
//! 与 `cog-storage` 里。由哪一份实现、连哪个库、日志推不推 Loki，是这个进程的
//! 事实——所以装配留在这个 crate，网关那层拿到的都是已经造好的 `Arc<dyn …>`。
//!
//! 同一条降级约定贯穿全部几块：连不上只 WARN、不阻止网关启动。观测是附加能力，
//! 代理与凭证代持才是网关的本职；一个连不上 Loki 的网关仍要把上游请求代理过去。

use std::sync::Arc;

use cog_gateway::security_gateway::{PoolObservability, SecurityGatewayConfig, VolumeFootprint};
use cog_observability::alert_store::PostgresAlertStore;
use cog_observability::analytics::{ClickHouseAnalyticsBackend, ClickHouseEventBuffer};
use cog_observability::data_volume;
use cog_observability::logs::{LokiBackgroundPusher, LokiPushClient};
use cog_observability::metrics::PrometheusMetricsBackend;
use cog_observability::usage_store::LlmUsageStore;

/// 交给网关的观测面：已经造好的实现 + 这次要渲染的卷占用。
pub struct GatewayObservability {
    pub pool: Arc<PoolObservability>,
    pub volume_footprint: Vec<VolumeFootprint>,
}

/// 造出安全网关的全部观测面，并把本进程的日志与卷走查一起装上。
pub async fn assemble(
    config: &SecurityGatewayConfig,
    http_client: &Arc<dyn cog_core::HttpClient>,
) -> GatewayObservability {
    let exporters = match cog_observability::ObservabilityExportersConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            // 配置读不出来不是"没配"：后者是默认的关，前者意味着这份部署说了话
            // 而进程没听见。降级为全关，但把原因说出来。
            eprintln!("cogneva: observability exporters config unreadable: {e}");
            cog_observability::ObservabilityExportersConfig::default()
        }
    };
    let loki_pusher = init_gateway_logging(&exporters, http_client);

    // 卷占用：声明读得出来但走查建不起来时，那一格在抓取面上就是缺席。声明本身
    // 出问题要喊出来——配了变量与没配变量，在"卷没在长"这个读数上同形。
    let (volume_observables, volume_declaration_problems) = data_volume::observables_from_env();
    for problem in &volume_declaration_problems {
        tracing::error!(problem = %problem, "volume footprint declaration became no reading");
    }
    let volume_footprint: Vec<VolumeFootprint> = volume_observables
        .iter()
        .map(|observable| VolumeFootprint {
            claim: observable.claim().to_string(),
            observable: Arc::clone(observable) as Arc<dyn cog_core::Observable>,
        })
        .collect();

    let pool = build_pool_observability(config, http_client, &exporters).await;

    // 刷写循环的结局读数要等后端建起来才接得上：在构造点取手柄会拿到 `None`。
    if let Some(pusher) = loki_pusher {
        let sink: Arc<dyn cog_core::MetricsBackend> = pool.metrics.clone();
        pusher.set_metrics(Some(sink));
        drop(pusher.run_loop());
    }

    data_volume::spawn_watchers(
        &volume_observables,
        data_volume::scan_interval_from_env(),
        cog_core::ShutdownSignal::default(),
    );

    GatewayObservability {
        pool,
        volume_footprint,
    }
}

fn init_gateway_logging(
    obs: &cog_observability::ObservabilityExportersConfig,
    http_client: &Arc<dyn cog_core::HttpClient>,
) -> Option<Arc<LokiBackgroundPusher>> {
    // 与节点侧守护进程同一份读法：两处各写一遍时，改名只会让其中一处去读一个
    // 再没人设置的名字，而读不到与「本来就没配」在调用点上同形。
    let (level, format) = cog_observability::logs::level_and_format();
    let pusher = if obs.loki.enabled {
        let client = Arc::new(
            LokiPushClient::new(&obs.loki.endpoint)
                .with_max_retries(obs.loki.max_retries.max(1))
                .with_timeout(obs.loki.timeout_secs)
                .with_label(
                    cog_gateway::LABEL_SERVICE,
                    cog_core::topology::SECURITY_GATEWAY,
                )
                .with_client(http_client.clone()),
        );
        // 循环由调用方在后端建好后起（见 `assemble`）：在构造点起，后端之前
        // 每一轮的结局就无处记。
        Some(Arc::new(LokiBackgroundPusher::new(
            client,
            std::time::Duration::from_secs(obs.loki.flush_interval_sec.max(1)),
            obs.loki.max_batch_size.max(1),
        )))
    } else {
        None
    };
    cog_observability::logs::init_subscriber_with_pusher(
        &level,
        format,
        pusher.clone(),
        cog_core::topology::SECURITY_GATEWAY,
    );
    pusher
}

/// 一个启动时没接上的出口，与它「以后还会接上」的可能。
///
/// 建表这一步在滚动升级的重叠期会撞锁：上一代 Pod 还在每拍读写同一张表，
/// `ALTER TABLE` 要的 ACCESS EXCLUSIVE 排在它后面，schema runner 那十来秒的重试
/// 用完就放弃。放弃只发生在启动那一秒，代价却按进程寿命计——这一代进程剩下的
/// 整个生命周期里一条告警都落不了库，而读数面上唯一的痕迹是「开着的行不再被
/// 刷新」，要等 `alert_sighting` 那十五分钟的窗口才看得见。
///
/// 所以「没接上」只是一个当下的状态，不是终点：后台任务按退避一直重试，接上了
/// 就换进这个槽，此后每一拍与没有出过事时完全一样。槽用同步锁而不是异步锁：
/// 读的那一步只是克隆一个 `Arc`，锁不会跨 `await` 持有。
pub struct DeferredSink<T: ?Sized> {
    /// 这一格在日志里叫什么，让「哪个出口没接上」可归因。
    name: &'static str,
    slot: std::sync::RwLock<Option<Arc<T>>>,
}

impl<T: ?Sized> DeferredSink<T> {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            slot: std::sync::RwLock::new(None),
        }
    }

    /// 现在接上的那个；没有就是没有，不拿别的东西顶上。
    fn ready(&self) -> Option<Arc<T>> {
        // 毒化不改变这一格里存的事实：另一个线程在安装时 panic 了，槽里要么是
        // 接上的出口要么是空的，两种都还能照原样读。
        self.slot
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn install(&self, sink: Arc<T>) {
        *self
            .slot
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(sink);
    }
}

/// 退避重试的封顶。
///
/// 第一档由调用方给（生产是 `SINK_RETRY_FIRST`）：它要长过一次正常滚动的重叠期
/// ——撞锁的那一瞬旧 Pod 还在写，几秒之后就没了，重试太密只是把已经撞过的那把锁
/// 再撞一遍。封顶一分钟：库真的不通时，每多试一次就多一条连接与一行日志，而一分
/// 钟一次足够在恢复后的第一分钟里接上。
// cap: bound — 要长过一次正常滚动的重叠期（撞锁的那一瞬旧 Pod 还在写、几秒后就走），又要短到恢复后第一分钟里接得上；没有一处读数能把它变成分位数
const SINK_RETRY_FIRST: std::time::Duration = std::time::Duration::from_secs(5);
// cap: bound — 库真的不通时每多试一次就多一条连接与一行日志；一分钟一次已经够在恢复后的第一分钟里接上，再密只是把不通的库再敲一遍
const SINK_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(60);

/// 一直重试到接上为止，然后停下。
///
/// 不设次数上限：放弃一次就是「这一代进程直到下次滚动都没有这个出口」，而那个
/// 状态没有任何读数能把它与「出口在、只是这一刻没话说」分开。接上之后不再试——
/// 出口自己会报错，重连是它的事，这一层只负责「启动时那一秒没造出来」。
fn spawn_retry<T, F, Fut>(sink: Arc<DeferredSink<T>>, first: std::time::Duration, build: F)
where
    T: ?Sized + Send + Sync + 'static,
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Arc<T>, String>> + Send + 'static,
{
    let max = SINK_RETRY_MAX.max(first);
    tokio::spawn(async move {
        let mut wait = first;
        loop {
            tokio::time::sleep(wait).await;
            match build().await {
                Ok(inner) => {
                    sink.install(inner);
                    tracing::info!(
                        sink = sink.name,
                        "启动时没接上的出口接上了，此后按正常节拍写"
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        sink = sink.name,
                        error = %e,
                        retry_in_secs = wait.as_secs(),
                        "出口仍接不上，继续退避重试"
                    );
                    wait = (wait * 2).min(max);
                }
            }
        }
    });
}

/// 没接上时对调用方说的是「读不到」，不是「没有」。
///
/// 两条返回 `Result` 的方法都如实退错。`firing_key_for_rule` 若答 `Ok(None)`，
/// 调用方会以为没有开着的行、当场按当下时钟新铸一个身份：一次事件被写成两行，
/// 恢复又只关得掉其中一行——那正是「身份里带上事件」这条设计要避免的事。
/// `list_active_persistent_alerts` 没有错误通道，只能答空：读它的人把「接上之前
/// 的这一拍」读成「没有要认领的行」，代价是重启后晚一拍接上，不会多开一行。
#[async_trait::async_trait]
impl cog_core::PersistentAlertSink for DeferredSink<dyn cog_core::PersistentAlertSink> {
    async fn set_persistent_alert(
        &self,
        condition: bool,
        draft: &cog_core::PersistentAlertDraft,
    ) -> Result<cog_core::AlertTransition, String> {
        match self.ready() {
            Some(sink) => sink.set_persistent_alert(condition, draft).await,
            None => Err(format!("{} 出口还没接上：后台在退避重试", self.name)),
        }
    }

    async fn firing_key_for_rule(&self, rule: &str) -> Result<Option<String>, String> {
        match self.ready() {
            Some(sink) => sink.firing_key_for_rule(rule).await,
            None => Err(format!("{} 出口还没接上：开着的行读不回来", self.name)),
        }
    }

    async fn list_active_persistent_alerts(
        &self,
        rule_prefix: &str,
        limit: i64,
    ) -> Vec<cog_core::PersistedAlert> {
        match self.ready() {
            Some(sink) => sink.list_active_persistent_alerts(rule_prefix, limit).await,
            None => Vec::new(),
        }
    }
}

/// 账本这一格没接上时，每个调用都如实退错，而不是答一个「没有」。
///
/// 二十一条方法全都返回 `Result`，所以接不上这件事可以对每一条都说出来——
/// 而这正是它和告警出口不同的地方：那边 `list_active_persistent_alerts` 没有
/// 错误通道，只能答空。账本没有这个缺口：一次没落成的调用要能被计成一次失败，
/// 不能读成「账本里本来就没有这一笔」。
#[async_trait::async_trait]
impl cog_core::LlmUsageLedger for DeferredSink<dyn cog_core::LlmUsageLedger> {
    async fn record(&self, record: &cog_core::LlmUsageRecord) -> Result<(), String> {
        self.ledger()?.record(record).await
    }

    async fn record_exchange(&self, record: &cog_core::LlmExchangeRecord) -> Result<(), String> {
        self.ledger()?.record_exchange(record).await
    }

    async fn upstream_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> Result<cog_core::UpstreamWindowReading, String> {
        self.ledger()?.upstream_window(upstream, window_secs).await
    }

    async fn burned_between(
        &self,
        upstream: &str,
        since_unix: i64,
        until_unix: i64,
    ) -> Result<u64, String> {
        self.ledger()?
            .burned_between(upstream, since_unix, until_unix)
            .await
    }

    async fn record_window(&self, window: &cog_core::StoredWindow) -> Result<(), String> {
        self.ledger()?.record_window(window).await
    }

    async fn stored_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> Result<Option<cog_core::StoredWindow>, String> {
        self.ledger()?.stored_window(upstream, window_secs).await
    }

    async fn stored_windows(&self, upstream: &str) -> Result<Vec<cog_core::StoredWindow>, String> {
        self.ledger()?.stored_windows(upstream).await
    }

    async fn spent_since(&self, upstream: &str, since_unix: i64) -> Result<u64, String> {
        self.ledger()?.spent_since(upstream, since_unix).await
    }

    async fn spend_by_actor_since(
        &self,
        since_unix: i64,
    ) -> Result<(u64, std::collections::BTreeMap<String, u64>), String> {
        self.ledger()?.spend_by_actor_since(since_unix).await
    }

    async fn spend_by_upstream_and_actor(
        &self,
    ) -> Result<Vec<cog_core::UpstreamActorSpend>, String> {
        self.ledger()?.spend_by_upstream_and_actor().await
    }

    async fn task_cost_quantile(
        &self,
        days: i64,
        quantile: f64,
    ) -> Result<Option<(u64, u64, i64)>, String> {
        self.ledger()?.task_cost_quantile(days, quantile).await
    }

    async fn task_has_started(&self, task: &str) -> Result<bool, String> {
        self.ledger()?.task_has_started(task).await
    }

    async fn spend_by_task(&self, task_ids: &[String]) -> Result<Vec<(String, u64)>, String> {
        self.ledger()?.spend_by_task(task_ids).await
    }

    async fn spend_by_person_since(&self, since_unix: i64) -> Result<Vec<(String, u64)>, String> {
        self.ledger()?.spend_by_person_since(since_unix).await
    }

    async fn spend_by_task_since(
        &self,
        task_ids: &[String],
        since_unix: i64,
    ) -> Result<Vec<(String, u64)>, String> {
        self.ledger()?
            .spend_by_task_since(task_ids, since_unix)
            .await
    }

    async fn task_submitter(&self, task_id: &str) -> Result<Option<String>, String> {
        self.ledger()?.task_submitter(task_id).await
    }

    async fn unattributed_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, String> {
        self.ledger()?.unattributed_between(since, until).await
    }

    async fn outcomes_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::OutcomeCount>, String> {
        self.ledger()?.outcomes_between(since, until).await
    }

    async fn metering_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::MeteringCount>, String> {
        self.ledger()?.metering_between(since, until).await
    }

    async fn skips_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::SkippedWorkLine>, String> {
        self.ledger()?.skips_between(since, until).await
    }

    async fn verdicts_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<cog_core::SpendVerdictLine>, String> {
        self.ledger()?.verdicts_between(since, until).await
    }
}

impl DeferredSink<dyn cog_core::LlmUsageLedger> {
    /// 现在接上的账本；没接上就如实说「还没接上」。
    ///
    /// 这句话与「账本说没有」是两件事：前者是这一代进程还没造出账本，后者是一个
    /// 真的读过了的答案。调用方按 `Result` 分开，正是为了不让两者同形。
    fn ledger(&self) -> Result<Arc<dyn cog_core::LlmUsageLedger>, String> {
        self.ready()
            .ok_or_else(|| format!("{} 出口还没接上：后台在退避重试", self.name))
    }
}

/// 接一次计量账本：连接、建表。
///
/// 与 [`connect_alert_sink`] 同一个形状，理由也一样：它同时是启动那一次与后台
/// 每一次重试调用的函数，两条路只要有一处不一样，「重试接上的账本」就与「启动
/// 就接上的账本」不是同一个东西。
async fn connect_usage_ledger(url: &str) -> Result<Arc<dyn cog_core::LlmUsageLedger>, String> {
    let runner = cog_storage::schema_ddl::PgSchemaRunner::connect(url)
        .await
        .map_err(|e| format!("schema runner 连接失败: {e}"))?;
    let store = LlmUsageStore::connect(url)
        .await
        .map_err(|e| format!("PostgreSQL 连接失败: {e}"))?;
    // 留底表（含正文）的保留期不在这里管：它由主应用里的
    // `observability_exchange_retention` 循环按同一个库定期清，并把"最老一行多老 /
    // 保留期是多少 / 占多少字节"发布成读数。放在启动时清一次等于把"一行能活多久"
    // 绑在网关的滚动频率上。
    store
        .init_schema(&runner)
        .await
        .map_err(|e| format!("gateway_llm_usage 建表失败: {e}"))?;
    Ok(Arc::new(store))
}

/// 计量账本：现在就接一次，接不上就给一个会自己接上的槽。
///
/// 不接上的代价与告警出口不同：告警出口关掉是「这一面没人喊」，账本关掉是
/// **每一笔花销都不进任何窗口，而且没有回填**。所以这里不留一个直到下次滚动
/// 才可能改变的 `None`。
async fn usage_ledger(url: &str) -> Arc<dyn cog_core::LlmUsageLedger> {
    match connect_usage_ledger(url).await {
        Ok(store) => {
            tracing::info!("LLM token 计量明细已落 PostgreSQL");
            store
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "计量账本启动时没接上，先降级为「还没接上」；后台按退避一直重试到接上为止"
            );
            let deferred = Arc::new(DeferredSink::<dyn cog_core::LlmUsageLedger>::new("usage"));
            let url = url.to_string();
            spawn_retry(Arc::clone(&deferred), SINK_RETRY_FIRST, move || {
                let url = url.clone();
                async move { connect_usage_ledger(&url).await }
            });
            deferred
        }
    }
}

/// 接一次告警库：连接、建表、挂上指标。
///
/// 成功与失败完全由这一次的结局决定、不留半截状态，因为它同时是启动那一次与
/// 后台每一次重试调用的函数——两条路只要有一处不一样，「重试接上的那个」就与
/// 「启动就接上的那个」不是同一个出口。
async fn connect_alert_sink(
    url: &str,
    metrics: Arc<dyn cog_core::MetricsBackend>,
) -> Result<Arc<dyn cog_core::PersistentAlertSink>, String> {
    let runner = cog_storage::schema_ddl::PgSchemaRunner::connect(url)
        .await
        .map_err(|e| format!("schema runner 连接失败: {e}"))?;
    let store = PostgresAlertStore::connect(url)
        .await
        .map_err(|e| format!("PostgreSQL 连接失败: {e}"))?;
    store
        .init_schema(&runner)
        .await
        .map_err(|e| format!("alerts 建表失败: {e}"))?;
    Ok(Arc::new(store.with_metrics(metrics)))
}

/// 告警出口：现在就接一次，接不上就给一个会自己接上的槽。
async fn alert_sink(
    url: &str,
    metrics: Arc<dyn cog_core::MetricsBackend>,
) -> Arc<dyn cog_core::PersistentAlertSink> {
    match connect_alert_sink(url, metrics.clone()).await {
        Ok(store) => {
            tracing::info!("告警状态机已落 PostgreSQL");
            store
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "告警出口启动时没接上，先降级为「还没接上」；后台按退避一直重试到接上为止"
            );
            let deferred = Arc::new(DeferredSink::<dyn cog_core::PersistentAlertSink>::new(
                "alerts",
            ));
            let url = url.to_string();
            spawn_retry(Arc::clone(&deferred), SINK_RETRY_FIRST, move || {
                let url = url.clone();
                let metrics = metrics.clone();
                async move { connect_alert_sink(&url, metrics).await }
            });
            deferred
        }
    }
}

async fn build_pool_observability(
    config: &SecurityGatewayConfig,
    http_client: &Arc<dyn cog_core::HttpClient>,
    obs: &cog_observability::ObservabilityExportersConfig,
) -> Arc<PoolObservability> {
    let metrics = Arc::new(PrometheusMetricsBackend::new(""));

    let analytics = if obs.clickhouse.enabled {
        let backend = Arc::new(
            ClickHouseAnalyticsBackend::new(&obs.clickhouse.base_url, &obs.clickhouse.database)
                .with_table(&obs.clickhouse.table)
                .with_auth(&obs.clickhouse.username, &obs.clickhouse.password)
                .with_client(http_client.clone()),
        );
        if let Err(e) = backend.init_table().await {
            tracing::warn!(error = %e, "ClickHouse 建表失败，时序明细降级为关闭");
            None
        } else {
            tracing::info!(table = %obs.clickhouse.table, "ClickHouse 时序明细已接入");
            let flush_metrics: Arc<dyn cog_core::MetricsBackend> = metrics.clone();
            Some(Arc::new(ClickHouseEventBuffer::new(
                backend,
                std::time::Duration::from_secs(obs.clickhouse.flush_interval_sec.max(1)),
                obs.clickhouse.max_batch_size.max(1),
                Some(flush_metrics),
            )))
        }
    } else {
        None
    };

    // 建表文本归各 store 自己，跑表的是存储 crate 的 runner：把锁等待限死是
    // 数据库的性质，不是某个 store 的。
    //
    // 接不上的结局是「还没接上」而不是「这一代进程没有告警面」：出口是一个会自己
    // 接上的槽（见 `alert_sink`）。2026-11-03 实测：滚动重叠期旧 Pod 还占着那张表，
    // 新 Pod 的 `ALTER TABLE` 等不到锁、十六次重试用完就放弃，此后一小时五十分钟
    // 里 `llm_upstream_pool_down` 那一行没人刷新，而池一直在断供——引擎没有停，
    // 是它的告警出口在启动那一秒被关掉了，再没打开过。
    let alerts: Option<Arc<dyn cog_core::PersistentAlertSink>> =
        match config.database_url.as_deref() {
            Some(url) => {
                let backend: Arc<dyn cog_core::MetricsBackend> = metrics.clone();
                Some(alert_sink(url, backend).await)
            }
            None => None,
        };

    // 账本与告警出口同一条处置：接不上是「还没接上」，不是「这一代进程没有
    // 账本」。区别在于账本丢的是钱：一次装配失败（库没起来、滚动重叠期等不到
    // 建表的锁）会让这一代进程此后每一笔花销都不进任何窗口，而账本没有回填——
    // 直到 Pod 被换掉为止。后台按退避一直重试到接上，接上之后落库按正常节拍走。
    let usage: Option<Arc<dyn cog_core::LlmUsageLedger>> = match config.database_url.as_deref() {
        Some(url) => Some(usage_ledger(url).await),
        None => None,
    };

    // 池读数的耐久面：与 alerts / usage 同一个 database_url。它是加法而不是
    // 替代：进程内注册表照旧每拍都写，那条永远先发生、也始终可用。
    let durable: Option<Arc<dyn cog_core::MetricsBackend>> = match config.database_url.as_deref() {
        Some(url) => match sqlx::PgPool::connect(url).await {
            Ok(pool) => {
                let backend = cog_storage::PostgresMetricsBackend::new(pool);
                match backend.init_schema().await {
                    Ok(()) => {
                        tracing::info!("池读数耐久面已接到共享 metrics store");
                        Some(Arc::new(backend))
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "metrics 建表失败，池读数耐久面降级为关闭");
                        None
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "PostgreSQL 连接失败，池读数耐久面降级为关闭");
                None
            }
        },
        None => None,
    };

    let analytics: Option<Arc<dyn cog_core::AnalyticsSink>> =
        analytics.map(|sink| sink as Arc<dyn cog_core::AnalyticsSink>);
    let metrics: Arc<dyn cog_core::ScrapableMetrics> = metrics;

    Arc::new(PoolObservability::new(
        metrics, analytics, alerts, usage, durable,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{LlmUsageLedger, PersistentAlertSink};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 一个记下自己被调了几次的出口，用来证明「接上之后的调用落到谁身上」。
    #[derive(Default)]
    struct RecordingSink {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::PersistentAlertSink for RecordingSink {
        async fn set_persistent_alert(
            &self,
            _condition: bool,
            _draft: &cog_core::PersistentAlertDraft,
        ) -> Result<cog_core::AlertTransition, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(cog_core::AlertTransition::NoChange)
        }

        async fn firing_key_for_rule(&self, _rule: &str) -> Result<Option<String>, String> {
            Ok(Some("llm_upstream_pool_down@1793609822".into()))
        }

        async fn list_active_persistent_alerts(
            &self,
            _rule_prefix: &str,
            _limit: i64,
        ) -> Vec<cog_core::PersistedAlert> {
            Vec::new()
        }
    }

    fn draft() -> cog_core::PersistentAlertDraft {
        cog_core::PersistentAlertDraft {
            rule: "llm_upstream_pool_down".into(),
            dedup_key: "llm_upstream_pool_down@1793609822".into(),
            severity: "critical".into(),
            message: "池不可用".into(),
            labels: serde_json::json!({}),
        }
    }

    /// 启动时接不上的出口仍然是一个出口：调用得到的是「还没接上」这句话本身，
    /// 而不是被静默吞掉。改造前它在这里是 `None`——调用方拿到的是「没有出口」，
    /// 于是一整代进程里没有一行告警被写出去，而没有任何一处在问这件事。
    #[tokio::test]
    async fn a_sink_that_could_not_start_still_answers_the_caller() {
        let metrics: Arc<dyn cog_core::MetricsBackend> =
            Arc::new(PrometheusMetricsBackend::new(""));
        // 不是一个数据库 URL：这一次连接立刻失败，走的就是降级那一支（不挑一个真实
        // 但没人听的端口，sqlx 会在那里把 acquire_timeout 等满）。
        let sink = alert_sink("not a database url", metrics).await;

        let error = sink
            .set_persistent_alert(true, &draft())
            .await
            .expect_err("没接上就不能答「已落库」");
        assert!(
            error.contains("还没接上"),
            "调用方要能读出这是「还没接上」而不是存储坏了：{error}"
        );

        // 「读不到」不许答成「没有开着的行」：那会让调用方每拍新铸一个身份，
        // 一次事件被写成几十行，而恢复只关得掉最后一行。
        assert!(
            sink.firing_key_for_rule("llm_upstream_pool_down")
                .await
                .is_err(),
            "没有出口时，没读到开着的行与「没有开着的行」必须分得开"
        );
    }

    /// 槽会自己接上：重试失败几次之后接上了，调用就落到真的出口上。
    #[tokio::test]
    async fn the_slot_installs_the_sink_as_soon_as_a_retry_succeeds() {
        let slot = Arc::new(DeferredSink::<dyn cog_core::PersistentAlertSink>::new(
            "alerts",
        ));
        let attempts = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&attempts);
        spawn_retry(
            Arc::clone(&slot),
            std::time::Duration::from_millis(1),
            move || {
                let seen = Arc::clone(&seen);
                async move {
                    if seen.fetch_add(1, Ordering::SeqCst) < 2 {
                        return Err("schema did not get its locks".to_string());
                    }
                    let sink = Arc::new(RecordingSink::default());
                    Ok(sink as Arc<dyn cog_core::PersistentAlertSink>)
                }
            },
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while slot.ready().is_none() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert!(
            slot.ready().is_some(),
            "重试成功之后槽里要有人；没有的话这一代进程的告警面就是永久关着的"
        );
        assert!(
            attempts.load(Ordering::SeqCst) >= 3,
            "前两次失败之后还要再试，而不是试一次就放弃"
        );
        assert!(
            slot.set_persistent_alert(true, &draft()).await.is_ok(),
            "接上之后的调用要落到真的出口上"
        );
    }

    /// 只回答「谁花了多少」的账本替身：这一格证明的是调用落到了真账本上。
    struct CountingLedger {
        reads: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmUsageLedger for CountingLedger {
        async fn record(&self, _: &cog_core::LlmUsageRecord) -> Result<(), String> {
            unimplemented!("这一格只读不写")
        }
        async fn record_exchange(&self, _: &cog_core::LlmExchangeRecord) -> Result<(), String> {
            unimplemented!("这一格只读不写")
        }
        async fn upstream_window(
            &self,
            _: &str,
            _: u64,
        ) -> Result<cog_core::UpstreamWindowReading, String> {
            unimplemented!("这一格只读不写")
        }
        async fn burned_between(&self, _: &str, _: i64, _: i64) -> Result<u64, String> {
            unimplemented!("这一格只读不写")
        }
        async fn record_window(&self, _: &cog_core::StoredWindow) -> Result<(), String> {
            unimplemented!("这一格只读不写")
        }
        async fn stored_window(
            &self,
            _: &str,
            _: u64,
        ) -> Result<Option<cog_core::StoredWindow>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn stored_windows(&self, _: &str) -> Result<Vec<cog_core::StoredWindow>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spent_since(&self, _: &str, _: i64) -> Result<u64, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_actor_since(
            &self,
            _: i64,
        ) -> Result<(u64, std::collections::BTreeMap<String, u64>), String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_upstream_and_actor(
            &self,
        ) -> Result<Vec<cog_core::UpstreamActorSpend>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn task_cost_quantile(
            &self,
            _: i64,
            _: f64,
        ) -> Result<Option<(u64, u64, i64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn task_has_started(&self, _: &str) -> Result<bool, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_task(&self, _: &[String]) -> Result<Vec<(String, u64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn spend_by_person_since(&self, _: i64) -> Result<Vec<(String, u64)>, String> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(vec![("someone".to_string(), 7)])
        }
        async fn spend_by_task_since(
            &self,
            _: &[String],
            _: i64,
        ) -> Result<Vec<(String, u64)>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn task_submitter(&self, _: &str) -> Result<Option<String>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn unattributed_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<u64, String> {
            unimplemented!("这一格只读不写")
        }
        async fn outcomes_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::OutcomeCount>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn metering_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::MeteringCount>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn skips_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::SkippedWorkLine>, String> {
            unimplemented!("这一格只读不写")
        }
        async fn verdicts_between(
            &self,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<cog_core::SpendVerdictLine>, String> {
            unimplemented!("这一格只读不写")
        }
    }

    /// 账本启动时没接上：先如实退错，后台接上之后同一个调用落到真账本上。
    ///
    /// 没有这一格，账本接不上的那一代进程就是「直到滚动都没有账本」：每笔花销
    /// 都不进任何窗口，而账本没有回填。退错也不能退成「没有账目」——那会让读者
    /// 把「读不到」读成一个真的答案，一天的花费于是显示为零。
    #[tokio::test]
    async fn a_ledger_that_starts_down_takes_reads_once_it_connects() {
        let slot = Arc::new(DeferredSink::<dyn cog_core::LlmUsageLedger>::new("usage"));

        let error = slot
            .spend_by_person_since(0)
            .await
            .expect_err("没接上时不能答「没有账目」");
        assert!(
            error.contains("还没接上"),
            "调用方要能读出这是「还没接上」而不是「账本说没有人花过钱」：{error}"
        );

        let attempts = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&attempts);
        let counted = Arc::clone(&reads);
        spawn_retry(
            Arc::clone(&slot),
            std::time::Duration::from_millis(1),
            move || {
                let seen = Arc::clone(&seen);
                let counted = Arc::clone(&counted);
                async move {
                    if seen.fetch_add(1, Ordering::SeqCst) < 2 {
                        return Err("PostgreSQL 还没起来".to_string());
                    }
                    Ok(Arc::new(CountingLedger { reads: counted })
                        as Arc<dyn cog_core::LlmUsageLedger>)
                }
            },
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while slot.ready().is_none() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert!(
            slot.ready().is_some(),
            "重试成功之后槽里要有人；没有的话这一代进程的每一笔花销都会丢到下次滚动"
        );
        assert_eq!(
            slot.spend_by_person_since(0)
                .await
                .expect("接上之后要落到真账本"),
            vec![("someone".to_string(), 7)],
            "接上之后的读要来自账本本身"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "那一次读要真的经过账本，而不是被槽答了"
        );
    }
}

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
    let level = std::env::var(cog_core::env::LOG_LEVEL)
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".into());
    let format = if std::env::var("COGNEVA_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
    {
        cog_observability::LogFormat::Json
    } else {
        cog_observability::LogFormat::Pretty
    };
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
const SINK_RETRY_FIRST: std::time::Duration = std::time::Duration::from_secs(5);
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

    let usage = match config.database_url.as_deref() {
        Some(url) => match cog_storage::schema_ddl::PgSchemaRunner::connect(url).await {
            Ok(schema_runner) => match LlmUsageStore::connect(url).await {
                Ok(store) => match store.init_schema(&schema_runner).await {
                    Ok(()) => {
                        tracing::info!("LLM token 计量明细已落 PostgreSQL");
                        // 留底表（含正文）的保留期不在这里管：它由主应用里的
                        // `observability_exchange_retention` 循环按同一个库定期清，
                        // 并把"最老一行多老 / 保留期是多少 / 占多少字节"发布成读数。
                        // 放在启动时清一次等于把"一行能活多久"绑在网关的滚动频率上。
                        Some(Arc::new(store))
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "gateway_llm_usage 建表失败，计量明细降级为关闭");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "PostgreSQL 连接失败，计量明细降级为关闭");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "schema runner 连接失败，计量明细降级为关闭");
                None
            }
        },
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
    let usage: Option<Arc<dyn cog_core::LlmUsageLedger>> =
        usage.map(|store| store as Arc<dyn cog_core::LlmUsageLedger>);
    let metrics: Arc<dyn cog_core::ScrapableMetrics> = metrics;

    Arc::new(PoolObservability::new(
        metrics, analytics, alerts, usage, durable,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::PersistentAlertSink;
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
}

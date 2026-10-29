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
    let alerts = match config.database_url.as_deref() {
        Some(url) => match cog_storage::schema_ddl::PgSchemaRunner::connect(url).await {
            Ok(schema_runner) => match PostgresAlertStore::connect(url).await {
                Ok(store) => match store.init_schema(&schema_runner).await {
                    Ok(()) => {
                        tracing::info!("告警状态机已落 PostgreSQL");
                        Some(Arc::new(store))
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "alerts 建表失败，告警落库降级为关闭");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "PostgreSQL 连接失败，告警落库降级为关闭");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "schema runner 连接失败，告警落库降级为关闭");
                None
            }
        },
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
    let alerts: Option<Arc<dyn cog_core::PersistentAlertSink>> =
        alerts.map(|store| store as Arc<dyn cog_core::PersistentAlertSink>);
    let usage: Option<Arc<dyn cog_core::LlmUsageLedger>> =
        usage.map(|store| store as Arc<dyn cog_core::LlmUsageLedger>);
    let metrics: Arc<dyn cog_core::ScrapableMetrics> = metrics;

    Arc::new(PoolObservability::new(
        metrics, analytics, alerts, usage, durable,
    ))
}

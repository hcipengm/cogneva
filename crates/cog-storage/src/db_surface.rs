//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//! 库自己也有界：把「会在飞的活杀掉的库级参数」和「vacuum 追不上的表」变成读数。
//!
//! 代码里的界拆干净，不等于现场没有界。数据库自己带着几条会在半路杀掉一条活语句或
//! 一段事务的时间界（`statement_timeout`、`idle_in_transaction_session_timeout`、
//! `lock_timeout`），以及一套由死元组触发的回收节奏（`autovacuum_vacuum_scale_factor`
//! 与阈值）。它们不是这份仓库里的常量，所以任何扫常量的门都看不见它们；而它们杀掉的
//! 活与代码里一个硬编码的界杀掉的活，在读数上是同一件事。
//!
//! 这里不做任何调整：只读。三条时间界按秒发布（`0` 表示那一条没开——「没开」与
//! 「开成 0 秒」在读数上必须分得开，所以 0 只表示没开，PostgreSQL 本身也不接受 0 秒的
//! 那种写法），另外发布一张「死元组已经越过 autovacuum 自己的触发线」的表有多少张。
//!
//! 为什么是「越过触发线」而不是「死元组最多的前 N 张」：前 N 是一个没有出处的数，而
//! 触发线是库自己的判据——`autovacuum_vacuum_threshold + scale_factor × live_tuples`。
//! 一张表越过它的那一刻，就是 vacuum 本该已经来了却没来的那一刻；把它们数出来，这个
//! 读数与库的行为是同一个判据，而不是一份并行的猜测。
//!
//! 每张表的「最老一行年龄」不在这里：一张表的时间列叫什么，只有写它的模块知道（大部分
//! 表甚至没有），一个泛化的查询只能靠猜。哪些表的年龄重要，由各自的面按自己的列名发布
//! ——这里发布的是「库的时间界」与「vacuum 追不上」，那两样才是这张表之外的东西。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, Row};
use tracing::warn;

use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "storage_database_surface";

/// The label naming which setting a value is about.
pub const SETTING_LABEL: &str = "setting";

/// The label naming the deployment a reading came from. Same word as the other
/// capacity loops': a reader selects on the label it was told about, and a
/// second spelling of it reads as a series with no deployment at all.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// The settings this loop reads, in the order the reading prints them.
///
/// All three are durations in milliseconds and all three end work that is in
/// flight. The list is written out rather than discovered from `pg_settings`
/// because "every duration setting" would include dozens of internals nothing
/// here can act on, and a reading whose population is decided by the server's
/// build is not comparable across deployments.
pub const SETTINGS: [&str; 3] = [
    "statement_timeout",
    "idle_in_transaction_session_timeout",
    "lock_timeout",
];

/// 一分钟：Prometheus 的默认 lookback delta 是五分钟，读数须在其内至少刷新一次。
// cap: fixed — Prometheus 的默认 lookback delta = 5 分钟，读数须在其内至少刷新一次；这里取 1/5
pub const PERIOD: Duration = Duration::from_secs(60);

/// 一次读到的库面。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbSurfaceReading {
    /// 每条时间界，按 [`SETTINGS`] 的顺序，**秒**；`0` 表示那一条没开。
    pub settings_secs: Vec<(&'static str, i64)>,
    /// 死元组已经越过 autovacuum 自己的触发线的表。
    pub vacuum_behind: Vec<(String, i64)>,
}

/// 只读地测量库面。不调整任何参数、不删任何行。
pub struct DatabaseSurface {
    pool: PgPool,
    metrics: Option<Arc<dyn MetricsBackend>>,
    deployment: Option<String>,
}

impl DatabaseSurface {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            metrics: None,
            deployment: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
        self
    }

    pub fn spawn(self: Arc<Self>, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            LOOP,
            cog_core::loop_health::Cadence::Periodic(PERIOD),
            shutdown.clone(),
            move |beat| {
                let surface = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move { surface.run(beat, shutdown).await }
            },
        )
    }

    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        loop {
            beat.beat();
            match self.measure_once().await {
                Ok(reading) => self.report(&reading).await,
                Err(e) => warn!(error = %e, "database surface measurement failed"),
            }
            tokio::select! {
                _ = tokio::time::sleep(PERIOD) => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// 读一次：三条时间界 + 越过触发线的表。
    pub async fn measure_once(&self) -> SFResult<DbSurfaceReading> {
        let rows =
            sqlx::query("SELECT name, setting::bigint AS ms FROM pg_settings WHERE name = ANY($1)")
                .bind(SETTINGS.map(str::to_string).to_vec())
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?;

        let mut by_name: HashMap<&str, i64> = HashMap::new();
        for row in &rows {
            let name: String = row
                .try_get("name")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let ms: i64 = row
                .try_get("ms")
                .map_err(|e| SFError::Database(e.to_string()))?;
            // 只认名单里的名字：`name = ANY($1)` 已经限定了，这里再收一次是为了让
            // 一个改过名的设置在读数里出现成「缺一格」而不是「换了个名字」。
            if let Some(known) = SETTINGS.iter().find(|known| **known == name) {
                by_name.insert(known, ms);
            }
        }
        let settings_secs: Vec<(&'static str, i64)> = SETTINGS
            .iter()
            .map(|name| {
                let secs = by_name.get(name).map(|ms| ms / 1000).unwrap_or(0);
                (*name, secs)
            })
            .collect();

        // 库自己的触发线：`threshold + scale_factor × live_tuples`。两个参数都从
        // `pg_settings` 读，因为它们是部署改过的界，不是这里写死的两个数。
        let behind = sqlx::query(
            "WITH trigger AS (
                 SELECT
                   (SELECT setting::bigint FROM pg_settings
                     WHERE name = 'autovacuum_vacuum_threshold') AS threshold,
                   (SELECT setting::float8 FROM pg_settings
                     WHERE name = 'autovacuum_vacuum_scale_factor') AS scale
             )
             SELECT s.relname AS name, s.n_dead_tup::bigint AS dead
               FROM pg_stat_user_tables s, trigger t
              WHERE s.n_dead_tup > t.threshold + t.scale * s.n_live_tup
              ORDER BY s.n_dead_tup DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let vacuum_behind = behind
            .iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("name")
                        .map_err(|e| SFError::Database(e.to_string()))?,
                    row.try_get::<i64, _>("dead")
                        .map_err(|e| SFError::Database(e.to_string()))?,
                ))
            })
            .collect::<SFResult<Vec<_>>>()?;

        Ok(DbSurfaceReading {
            settings_secs,
            vacuum_behind,
        })
    }

    async fn report(&self, reading: &DbSurfaceReading) {
        let Some(ref mb) = self.metrics else { return };
        let base = self.labels();
        for (name, secs) in &reading.settings_secs {
            let mut labels = base.clone();
            labels.insert(SETTING_LABEL.to_string(), (*name).to_string());
            self.emit(
                mb,
                cog_core::metric_names::DB_SETTING_SECONDS,
                *secs as f64,
                labels,
            )
            .await;
        }
        self.emit(
            mb,
            cog_core::metric_names::DB_VACUUM_BEHIND_TABLES,
            reading.vacuum_behind.len() as f64,
            base,
        )
        .await;
    }

    fn labels(&self) -> HashMap<String, String> {
        match self.deployment {
            Some(ref deployment) => {
                HashMap::from([(DEPLOYMENT_LABEL.to_string(), deployment.clone())])
            }
            None => HashMap::new(),
        }
    }

    async fn emit(
        &self,
        mb: &Arc<dyn MetricsBackend>,
        name: cog_core::MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) {
        if let Err(e) = mb.record_gauge(name, value, labels).await {
            warn!(error = %e, metric = %name, "database surface gauge emit failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three settings are the ones that end work in flight, and the list is
    /// the population the reading prints -- a setting added here without a
    /// reader, or read without being listed, is the drift this pins.
    #[test]
    fn the_reading_names_the_settings_that_end_work_in_flight() {
        for name in SETTINGS {
            assert!(
                matches!(
                    name,
                    "statement_timeout" | "idle_in_transaction_session_timeout" | "lock_timeout"
                ),
                "`{name}` is not one of the time bounds that kills work in flight: the reading \
                 would then publish a number nothing can act on"
            );
        }
        assert_eq!(
            SETTINGS.len(),
            3,
            "a fourth setting needs its own reason here"
        );
    }

    /// 真库上读一次：三条时间界都在读数里，vacuum 那张表读得出来。
    #[tokio::test]
    #[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
    async fn the_reading_comes_back_from_a_real_server() {
        let url = cog_test_support::database_url();
        let pool = PgPool::connect(&url).await.expect("a live pool");
        let reading = DatabaseSurface::new(pool)
            .measure_once()
            .await
            .expect("measure the surface");
        assert_eq!(
            reading.settings_secs.len(),
            SETTINGS.len(),
            "every setting in the list has to come back, present or zero: {reading:?}"
        );
        for (name, secs) in &reading.settings_secs {
            assert!(*secs >= 0, "{name} came back negative: {reading:?}");
        }
        // 触发线的查询本身要能跑：空结果与「跑不动」不是一件事，这里要的是前者。
        let _ = reading.vacuum_behind.len();
    }

    /// 这个模块不删、不调参数：它只读。形状门，和另外三个测量循环同一形状。
    #[test]
    fn the_module_only_reads() {
        let source = include_str!("db_surface.rs");
        for forbidden in [
            ["DE", "LETE FROM"].join(""),
            ["AL", "TER DATABASE"].join(" "),
            ["AL", "TER SYSTEM"].join(" "),
        ] {
            assert!(
                !source.to_uppercase().contains(&forbidden.to_uppercase()),
                "a measuring loop grew the power to {forbidden}"
            );
        }
    }
}

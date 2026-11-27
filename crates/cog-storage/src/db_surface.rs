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

/// 在飞事务那一格按状态分的取值：库的状态名原样写出来，
/// 因为掐它的设置按这个名字认人。
pub const XACT_STATE_ACTIVE: &str = "active";
pub const XACT_STATE_IDLE: &str = "idle in transaction";

/// The label naming which transaction state an age belongs to.
pub const DB_STATE_LABEL: &str = "state";

/// The label naming which table a per-table reading is about.
pub const DB_TABLE_LABEL: &str = "table";

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
///
/// 这三个是**界**，而且界不在树里——它们在数据库的设置里。所以普查器扫不到它们，
/// 这一支（连同它的读数与下面每一条 `db-bound:`）就是它们在树上的人口：每一条都要
/// 先回答「它为什么存在」，再回答「值该由哪条实测/约定给出」。按第一问，删得掉的是
/// 这个界对**读数的保护**（读数现在不等待），删不掉的是它对**共用库**的保护
/// （一条跑飞的语句/一条空闲事务占着快照与锁，受害的是平台其余部分）；所以它们由库
/// 这一面持有，值不由这个仓拍。今天部署里三条都是 0（未设），读数面把「未设」发成
/// NaN，而不是 0 秒。
// db-bound: statement_timeout — 界：单条语句的最长执行，到点由库杀掉 — 值来源：derived，若设应由平台的请求时限或实测查询 p99 导出（两条读数今天都没有）；当前未设（读数面发 NaN，不发 0——PG 内部用 0 表示关掉，那不是零秒）
// db-bound: idle_in_transaction_session_timeout — 界：一条空闲事务能持快照多久，0＝一条空闲事务可永远挡住 vacuum — 值来源：derived，应由最长合法事务的实测时长导出；当前未设（读数面发 NaN，不发 0——PG 内部用 0 表示关掉，那不是零秒）
// db-bound: lock_timeout — 界：等锁到什么时候放弃，0＝一直等 — 值来源：derived，应由请求时限或实测锁等待 p99 导出；当前未设（读数面发 NaN，不发 0——PG 内部用 0 表示关掉，那不是零秒）
pub const SETTINGS: [&str; 3] = [
    "statement_timeout",
    "idle_in_transaction_session_timeout",
    "lock_timeout",
];

/// 一分钟：Prometheus 的默认 lookback delta 是五分钟，读数须在其内至少刷新一次。
// cap: fixed — Prometheus 的默认 lookback delta = 5 分钟，读数须在其内至少刷新一次；这里取 1/5
// keep: 删掉后 db 读数在 lookback 内不刷新，序列消失；由「lookback delta 的 1/5」兜住
pub const PERIOD: Duration = Duration::from_secs(60);

/// 一次读到的库面。
#[derive(Debug, Clone, PartialEq)]
pub struct DbSurfaceReading {
    /// 每条时间界，按 [`SETTINGS`] 的顺序，**秒**；`0` 表示那一条没开。
    pub settings_secs: Vec<(&'static str, i64)>,
    /// 死元组已经越过 autovacuum 自己的触发线的表。
    pub vacuum_behind: Vec<(String, i64)>,
    /// 在飞的事务有多老，按状态分：`None` 表示这个状态此刻没有事务——
    /// 「没有」与「刚开、0 秒」在读数上必须分开，所以缺席而不是 0。
    pub xact_ages: Vec<(&'static str, Option<f64>)>,
    /// 每张表的死元组与该表自己的触发线：两者的商就是「越过触发线多远」，
    /// 商在读到的地方算，阈值为 0 时不会编出一个比值。
    pub vacuum: Vec<(String, i64, f64)>,
}

/// 在飞事务的两条年龄：`active` 与 `idle in transaction`。
///
/// 库自己会掐掉它们（`statement_timeout`／`idle_in_transaction_session_timeout`／
/// `lock_timeout`），所以年龄就是「还差多久被掐」。两态分开读：它们被不同的设置掐，
/// 一个最大值说不清是哪条界在逼近。
pub async fn inflight_ages(pool: &PgPool) -> SFResult<Vec<(&'static str, Option<f64>)>> {
    let (active, idle): (Option<f64>, Option<f64>) = sqlx::query_as(
        "SELECT EXTRACT(EPOCH FROM max(now() - xact_start))::float8 \
                FILTER (WHERE state = 'active'), \
                EXTRACT(EPOCH FROM max(now() - xact_start))::float8 \
                FILTER (WHERE state = 'idle in transaction') \
           FROM pg_stat_activity \
          WHERE xact_start IS NOT NULL AND pid <> pg_backend_pid()",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(vec![(XACT_STATE_ACTIVE, active), (XACT_STATE_IDLE, idle)])
}

/// 每张表的死元组与它自己的触发线
/// （`autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × 活行数`，两边都从库读）。
pub async fn vacuum_lag(pool: &PgPool) -> SFResult<Vec<(String, i64, f64)>> {
    let rows: Vec<(String, i64, f64)> = sqlx::query_as(
        "SELECT relname, n_dead_tup::BIGINT, \
                (current_setting('autovacuum_vacuum_threshold')::float8 \
                 + current_setting('autovacuum_vacuum_scale_factor')::float8 \
                   * GREATEST(n_live_tup, 0))::float8 AS threshold \
           FROM pg_stat_user_tables \
          ORDER BY n_dead_tup DESC",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(rows)
}

/// 一条设置值该发布成什么数：设了就发秒数，没设（PostgreSQL 用 0 表示「关掉」）
/// 就发 **NaN**，而不是 0。
///
/// 0 在容器的语义里是「这条界不存在」，不是「允许零秒」。把它当 0 秒发出去，读的人
/// 与规则都要各自记得打一个 `> 0` 的补丁（现有的 idle-transaction 规则正是这么写的），
/// 而任何忘了补丁的读者会把「没有界」读成「界是零、每条语句都该被杀」。NaN 让
/// 「这条界没有值」与「这条界是 0 秒」在读数面上分得开：序列仍在（说明库面跑到了），
/// 值不是数（PromQL 的比较全为假，面板画成断点）。
pub fn setting_seconds_value(secs: i64) -> f64 {
    if secs > 0 {
        secs as f64
    } else {
        f64::NAN
    }
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

        // 每张表的死元组与它自己的触发线都从库读：两个参数是部署改过的界，
        // 不是这里写死的数。「越过」由两者现比，不另写一条 SQL——两条问法会漂。
        let vacuum = vacuum_lag(&self.pool).await?;
        let vacuum_behind: Vec<(String, i64)> = vacuum
            .iter()
            .filter(|(_, dead, threshold)| *dead as f64 > *threshold)
            .map(|(table, dead, _)| (table.clone(), *dead))
            .collect();
        let xact_ages = inflight_ages(&self.pool).await?;

        Ok(DbSurfaceReading {
            settings_secs,
            vacuum_behind,
            xact_ages,
            vacuum,
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
                setting_seconds_value(*secs),
                labels,
            )
            .await;
        }
        self.emit(
            mb,
            cog_core::metric_names::DB_VACUUM_BEHIND_TABLES,
            reading.vacuum_behind.len() as f64,
            base.clone(),
        )
        .await;
        for (state, age) in &reading.xact_ages {
            // 这个状态此刻没有事务就**不发**：0 秒是一次刚开的事务，
            // 发一个 0 会让「没有」与「刚好在跑」在面板上同形。
            let Some(age) = age else { continue };
            let mut labels = base.clone();
            labels.insert(DB_STATE_LABEL.to_string(), (*state).to_string());
            self.emit(
                mb,
                cog_core::metric_names::DB_XACT_AGE_SECONDS,
                *age,
                labels,
            )
            .await;
        }
        for (table, dead, threshold) in &reading.vacuum {
            let mut labels = base.clone();
            labels.insert(DB_TABLE_LABEL.to_string(), table.clone());
            self.emit(
                mb,
                cog_core::metric_names::DB_DEAD_TUPLES,
                *dead as f64,
                labels.clone(),
            )
            .await;
            self.emit(
                mb,
                cog_core::metric_names::DB_VACUUM_THRESHOLD_TUPLES,
                *threshold,
                labels,
            )
            .await;
        }
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

    /// 库侧那三个界不在树里，普查器扫不到：它们的「人口」就是这一份清单加每条一行
    /// `// db-bound:` 判词。少一行、或哪一行没说「为什么存在／值从哪来」，就红——
    /// 否则往 SETTINGS 里加一条新的界，既不在树的普查里，也没人答过第一问。
    #[test]
    fn every_database_bound_names_why_it_exists_and_where_its_value_comes_from() {
        let source = include_str!("db_surface.rs");
        let dispositions: Vec<&str> = source
            .lines()
            .filter_map(|line| line.trim().strip_prefix("// db-bound:"))
            .collect();
        assert_eq!(
            dispositions.len(),
            SETTINGS.len(),
            "SETTINGS 有 {} 条，`// db-bound:` 判词有 {} 条：新加的界没人答第一问",
            SETTINGS.len(),
            dispositions.len()
        );
        for (setting, line) in SETTINGS.iter().zip(dispositions) {
            assert!(
                line.contains(setting),
                "判词与 SETTINGS 的顺序对不上：{line}"
            );
            assert!(
                line.contains("界：") && line.contains("值来源："),
                "{setting} 的判词没写「它是什么界／值从哪来」：{line}"
            );
            assert!(
                line.contains("derived") || line.contains("fixed"),
                "{setting} 的判词没说值是 derived 还是 fixed：{line}"
            );
        }
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

//! Removes the rows of metric names nothing produces any more.
//!
//! A retired name is not a reading: no producer will write it again, so whatever
//! is stored under it will never be updated. Where that matters is every place a
//! series is stored, and the metric store has four:
//!
//! - `cog_metrics_samples`, the append-only log — for a gauge this *is* the
//!   value, read as the newest sample per label set.
//! - `cog_metric_counter_totals`, one row per label set, rewritten in place —
//!   the value of a counter.
//! - `cog_metric_histogram_buckets` and `cog_metric_histogram_sums`, the
//!   accumulation of a histogram.
//!
//! The sweep in [`crate::SampleLogCap`] cannot reach the last three: they have
//! no rows to rank and no overshoot to trim, because they hold current state
//! rather than history. An unreachable store is a series that is not merely held
//! but never deleted at all — a retired counter keeps being listed and served at
//! its frozen total, and a frozen total reads downstream as "no traffic" rather
//! than as "this metric is gone", which is the more misleading of the two.
//!
//! So the release is its own pass, not a variant of pruning. Pruning is driven
//! by how much is held and only runs when the store is over its budget; a
//! retirement is a fact about a name and has to take effect whether or not the
//! store happens to be full. Riding the capacity sweep would mean a quiet
//! deployment never releasing anything, and the store is at its quietest exactly
//! where a rename would otherwise sit unfixed.
//!
//! What it never does is decide that a name is dead on its own. That judgement
//! belongs where the names are known — [`cog_core::RETIRED_METRIC_NAMES`] — and
//! a guess made here from age or from rank would delete a live series' current
//! value whenever its producer was slow.
//!
//! A name is only one way a series falls out of use. The other is a shape: a
//! producer gains a label, so every label set written before that gain is
//! orphaned while the name itself stays alive. Those are declared too, in
//! [`SUPERSEDED_SHAPES`], and released by this same pass under the same rule —
//! a person counted the writes and said so, the pass does not guess. The
//! difference is what makes the claim safe. A retired name is safe because the
//! list is curated and nothing contradicts it; a retired shape is safe because
//! the name has a newer row carrying the label — the orphan's own replacement.
//! Age is the third leg, and the sentence above is about *that* leg: it decides
//! when the release may act, not whether the row is dead. That makes it a
//! statistical promise rather than a structural one — a producer that pauses
//! longer than any pause this shape has ever been seen to take is still
//! released — so its value is a measured maximum times a stated multiplier, and
//! never a median or a round number picked to look safe.

use std::collections::HashMap;

use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::{MetricsBackend, SFError, SFResult};

use crate::metrics_sample_cap::DEPLOYMENT_LABEL;

/// The accumulation tables, named for the kind of value each holds.
pub const COUNTER_TOTALS_TABLE: &str = "cog_metric_counter_totals";
pub const HISTOGRAM_BUCKETS_TABLE: &str = "cog_metric_histogram_buckets";
pub const HISTOGRAM_SUMS_TABLE: &str = "cog_metric_histogram_sums";

/// Rows one statement may delete from the sample log.
///
/// Not a policy and not a cadence: it bounds how long a single `DELETE` holds
/// locks, the same way the sweep's batch does. A retired name's history can be
/// the larger part of the log, and deleting it in one statement would block
/// every writer for as long as that takes.
// cap: bound — 与清扫那批同源地界单句的锁：一个退役名字的历史可能是日志的大头，一句删完会让所有写者等那么久
const RELEASE_BATCH: i64 = 5_000;

/// 申报：一个名字**后来长出来的标签**，与长出之前那批标签集。
///
/// 每项是一对 `(name, mandatory label)`：这个名字还在被生产，而它的生产者现在
/// **必须**带这个标签，于是这个名字下所有不带该标签的行都不会再被写了。
///
/// 它与 [`cog_core::RETIRED_METRIC_NAMES`] 是同一条纪律，理由也一样：删除会清掉一个
/// 序列的当前值，只有**数过写入**的人能说「它的生产者已经停了」。区别在于这里要读的
/// 是**计数**不是名字——每一项都在这里，是因为它那几行不带标签的账停了、而带标签的
/// 兄弟还在走（2026-10-16 实测：`cogneva_eval_gate_outcomes_total` 的 `outcome` 那批
/// 停在 10-14 15:07Z、值全 0；`cogneva_mainline_ci_no_verdict_reason_total` 的 `reason`
/// 那批停在 10-15 04:37Z、值 3/364/367；同刻带标签的行一分钟内刚写过）。
///
/// 一项管这个名字所在的**每一张表**：计数器的孤儿与它自己在样本日志里的历史行是同
/// 一个形状。标签本身正是守卫要测的那个键，所以一个名字若日后又把标签丢了回去，它
/// 就没有带标签的兄弟、守卫找不到可删的行——不会反过来删活行。
pub const SUPERSEDED_SHAPES: &[(&str, &str)] = &[
    ("cogneva_eval_gate_outcomes_total", "site"),
    ("cogneva_mainline_ci_no_verdict_reason_total", "platform"),
    // 2026-10-29 现量：这两格的孤儿停下之后，带标签的兄弟一直在走，而孤儿本身仍是
    // 一条活序列——两者的值相差一个数量级，读的人先看到的是那个不动的大数。
    // `cogneva_mainline_ci_verdict_total` 的 `{verdict}` 那三行停在 10-28 18:18Z
    // （fail 850 / no_evidence 1126 / pass 518），带 `ask` 的九行同一刻正在写
    // （合计 fail 28）；`cogneva_process_report_ingest_total` 的 `{outcome}` 那两行
    // 停在 10-27 08:26Z，带 `node` 的三行 08:00 刚写过。
    //
    // 为什么这两个能申报而 `cogneva_cluster_read_total` 不能：申报要求「生产者现在
    // **必须**带这个标签」。两处写入点分别是 `record_ci_verdict_reading` 与
    // `publish_ingest`，都在构造标签的表里无条件插入；而 `cogneva_cluster_read_total`
    // 的 `deployment` 是 `if let Some(...)`，`deployment_label()` 解析不到时会整批不写
    // 这个标签——那批行今天安静两天只是没走到那条路上，不是写者停了，所以它**不能**
    // 申报。要先让那个标签变成无条件，再说这一格。
    ("cogneva_mainline_ci_verdict_total", "ask"),
    ("cogneva_process_report_ingest_total", "node"),
    ("metrics_retired_rows_removed", DEPLOYMENT_LABEL),
    ("metrics_samples_rows", DEPLOYMENT_LABEL),
    ("metrics_samples_bytes", DEPLOYMENT_LABEL),
    ("metrics_samples_budget_rows", DEPLOYMENT_LABEL),
    ("metrics_samples_over_capacity", DEPLOYMENT_LABEL),
];

/// 一个被判为孤儿的标签集要**安静多久**，release 才允许动它。
///
/// 这是形状守卫唯一一条**统计**腿；另两条是结构性的（这行不带那个标签、且同名有个
/// 带那个标签的更年轻的行）。它是**覆盖位**、不是导出的值：`source=` 2026-10-16 从
/// 样本日志逐标签集量**活形状**两次写入之间的最大空档 —— 16416 s（4.56 h，
/// `{site="port",outcome=…}`，40 条样本），`{reason="no_runs",platform="gitee"}`
/// 13902 s（83 条）。取 21600 s ＝ 6 h，是那个最大值 × 约 1.32。**运行期没有任何东西
/// 重算它**：把它做成一条实时查询只会让删除的判据随现网抖动，而这里要的恰恰是一个
/// 不动的界。
///
/// 覆盖位会过期，而这个量过期的方向**只有一边**（「最大空档」只随样本积累而涨）：
/// 真值涨过 6 h 的那天，表现是**静默删掉一个只是罕见的写者**（计数从增量重起、
/// Prometheus 读成 reset），没有任何红灯。所以重测触发写在这里、由人执行：
///   1. `SUPERSEDED_SHAPES` 里任一形状的活形状写入被观测到**比实测最大值还安静**
///      （6 h 的余量被吃光）时；
///   2. 任一形状的**生产者周期**变更时（本常数假定孤儿与活形状同一节奏）。
///
/// **缺判据**：这**不是「量不出来」**——「某个活形状安静了多久」一条
/// `lag(timestamp)` 就量得出（样本日志逐标签集取最大）。缺的是**读者**：没有载体在
/// 跑这条查询。而且读者**不能是一条 `warn!`**——本仓一条告警规则的 summary 自己写着，
/// 那个失败从前只是一条随 Pod 一起死的日志线；日志线不是载体。要真有人管，得是一条
/// 随进度动的读数或一条规则，两者都要动 core 的登记表；在那之前机检还欠一份
/// 「可调量登记表」（哪些是 tunable、每个 tunable 的读数带 `derived`/`override`
/// 来源标签、标 `derived` 的要有一条「换输入 ⇒ 值跟着变」的测试）。所以它是**写下来的
/// 触发**，不是一扇会红的门。这条覆盖位**退回闭环**也还有第二条阻碍：平台的既有先例
/// 把出口写成「标了来源的覆盖位，等这个词自己有一周分布之后换成 `max_over_time(…[7d])`」，
/// 而「形状的最大空档」今天**没有被发布成任何序列**，连那条退路的起点都还没有。
///
/// 取最大值是因为失败方向**不对称**：取小了会删一个**还在被写**的计数器行，而
/// `increment_counter_total` 是 `SET value = t.value + EXCLUDED.value`，下一笔从增量
/// 重起、Prometheus 读成 reset ⇒ 比不删更坏。取大了只是把一个没有截止期的删除推后。
/// 中位数落在被保护的形状自己正常的节奏里，压根不是候选。
// cap: bound — increment_counter_total 是 SET value = t.value + EXCLUDED.value，删后再写会被读成 reset，比不删更坏；取大了只是把一个没有截止期的删除推后
pub const SUPERSEDED_QUIET_SECS: i64 = 6 * 3600;

/// 一张表把时钟放在哪——形状守卫要按它判年龄。
///
/// 四张表里两张自带钟列、日志每行一次观测自带时刻，而 `cog_metric_histogram_buckets`
/// **没有钟列**（只有 `name, labels, bucket, observations`）：它的钟在同名同标签的
/// `cog_metric_histogram_sums` 行上。两者由**两条无事务的语句**写
/// （`increment_histogram_bucket` 先写 bucket 行、再写 sums 行并盖 `updated_at`），
/// 中间失败会让伙伴的钟消失。**没有钟不是钟很旧**：下面那个标量子查询取不到伙伴时是
/// NULL，与它比较的谓词全为 NULL ⇒ 那行不删。
#[derive(Debug, Clone, Copy)]
enum ShapeClock {
    /// 钟就是本表这一列。
    Column(&'static str),
    /// 本表没有钟列：取同名同标签的 `histogram_sums` 行的 `updated_at`。
    ViaSums,
}

/// Gauge reporting the rows this pass removed, per table.
///
/// One series per table and per deployment rather than one total: a table that
/// keeps reporting a non-zero removal is the reading that says a release is not
/// draining, and a merged scalar would hide which of the four it is — while a
/// series shared by two deployments hides which of *them* it is.
use cog_core::metric_names::METRICS_RETIRED_ROWS_REMOVED as RETIRED_ROWS_REMOVED_METRIC;
use cog_core::metric_names::METRICS_RETIREMENT_RELEASE_FAILED as RETIREMENT_RELEASE_FAILED_METRIC;

/// What one release pass removed, per table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetirementOutcome {
    pub samples: u64,
    pub counter_totals: u64,
    pub histogram_buckets: u64,
    pub histogram_sums: u64,
}

impl RetirementOutcome {
    pub fn total(&self) -> u64 {
        self.samples + self.counter_totals + self.histogram_buckets + self.histogram_sums
    }

    /// The per-table counts, named for the table each came from.
    pub fn per_table(&self) -> [(&'static str, u64); 4] {
        [
            ("samples", self.samples),
            ("counter_totals", self.counter_totals),
            ("histogram_buckets", self.histogram_buckets),
            ("histogram_sums", self.histogram_sums),
        ]
    }
}

/// Deletes the rows of retired metric names from every table that stores one.
pub struct MetricsRetirement {
    pool: PgPool,
    samples: String,
    counter_totals: String,
    histogram_buckets: String,
    histogram_sums: String,
    retired_names: Vec<&'static str>,
}

impl MetricsRetirement {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            samples: crate::metrics_sample_cap::SAMPLES_TABLE.to_string(),
            counter_totals: COUNTER_TOTALS_TABLE.to_string(),
            histogram_buckets: HISTOGRAM_BUCKETS_TABLE.to_string(),
            histogram_sums: HISTOGRAM_SUMS_TABLE.to_string(),
            retired_names: cog_core::RETIRED_METRIC_NAMES.to_vec(),
        }
    }

    /// Point the pass at other tables. Exists so a test can use throwaway
    /// tables instead of the live ones.
    pub fn with_tables(
        mut self,
        samples: impl Into<String>,
        counter_totals: impl Into<String>,
        histogram_buckets: impl Into<String>,
        histogram_sums: impl Into<String>,
    ) -> Self {
        self.samples = samples.into();
        self.counter_totals = counter_totals.into();
        self.histogram_buckets = histogram_buckets.into();
        self.histogram_sums = histogram_sums.into();
        self
    }

    /// Replace the retired-name set. Exists so a test can name a series of its
    /// own instead of depending on which names the codebase happens to have
    /// retired.
    pub fn with_retired_names(mut self, names: Vec<&'static str>) -> Self {
        self.retired_names = names;
        self
    }

    /// Run the pass until nothing is left to remove, and say what went.
    ///
    /// Idempotent: a second pass over the same names finds nothing. That is what
    /// lets any deployment holding the store run it on its own cadence without
    /// agreeing with anyone else about who may run it — every deployment ships
    /// the same list, and two of them deleting the same rows is two statements
    /// where the second finds none.
    pub async fn release(&self) -> SFResult<RetirementOutcome> {
        // 两条通道：按名字（申报的退役名）与按形状（申报的标签集）。名字集为空
        // **不是**整体早退——形状那一步不依赖它，而「一个名字都没退役、只有形状要
        // 清」正是一趟必须照样推计数的输入。
        let mut counter_totals = 0u64;
        let mut histogram_buckets = 0u64;
        let mut histogram_sums = 0u64;
        let mut samples = 0u64;

        if !self.retired_names.is_empty() {
            // The accumulation tables hold one row per series rather than one per
            // observation, so their share of a retirement is bounded by how many
            // label sets the name ever carried and one statement is enough. The log
            // is the append-only one and gets the batched loop.
            counter_totals += self.delete_by_name(&self.counter_totals).await?;
            histogram_buckets += self.delete_by_name(&self.histogram_buckets).await?;
            histogram_sums += self.delete_by_name(&self.histogram_sums).await?;
            loop {
                let deleted = self.delete_log_batch().await?;
                samples += deleted;
                if deleted < RELEASE_BATCH as u64 {
                    break;
                }
            }
        }

        // 形状那一步的删除数**折回它所属的那张表**，不开第五格：这张表读数的读者
        // 问的是「哪张表没在排水」，而「从那张表删掉了多少」正是答案的一部分；
        // 「哪个名字退休完了」是另一个问题，读的也是同一张表。开第五格会造出一个
        // 既非前者也非后者的对角项，两个问题都答不了。
        counter_totals += self
            .delete_superseded_shapes(&self.counter_totals, ShapeClock::Column("updated_at"), None)
            .await?;
        // 桶行拿 sums 行当自己的钟（[`ShapeClock::ViaSums`]），所以这一步**必须先于**
        // 下面 sums 那一步：反过来的话，同一个形状的 sums 孤儿先被删掉，桶行就取不到
        // 伙伴、钟变 NULL ⇒ 它按「量不了」被留下，那个形状的桶行于是永远清不掉。
        histogram_buckets += self
            .delete_superseded_shapes(&self.histogram_buckets, ShapeClock::ViaSums, None)
            .await?;
        histogram_sums += self
            .delete_superseded_shapes(&self.histogram_sums, ShapeClock::Column("updated_at"), None)
            .await?;
        samples += self
            .delete_superseded_shapes(&self.samples, ShapeClock::Column("timestamp"), Some("id"))
            .await?;

        Ok(RetirementOutcome {
            samples,
            counter_totals,
            histogram_buckets,
            histogram_sums,
        })
    }

    /// `DELETE ... WHERE name = ANY($1)` against a table that holds one row per
    /// series.
    async fn delete_by_name(&self, table: &str) -> SFResult<u64> {
        let sql = format!(
            "DELETE FROM {} WHERE name = ANY($1)",
            cog_core::sql::quote_relation(table)
        );
        Ok(sqlx::query(&sql)
            .bind(self.names())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected())
    }

    /// One batch of the sample log's retired rows, oldest first.
    async fn delete_log_batch(&self) -> SFResult<u64> {
        let sql = format!(
            "DELETE FROM {table} WHERE id IN (
                 SELECT id FROM {table}
                 WHERE name = ANY($1)
                 ORDER BY id
                 LIMIT $2
             )",
            table = cog_core::sql::quote_relation(&self.samples),
        );
        Ok(sqlx::query(&sql)
            .bind(self.names())
            .bind(RELEASE_BATCH)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected())
    }

    /// 读某一行时刻的 SQL 表达式（`row` 是那一行在语句里的别名）。
    ///
    /// `ViaSums` 那一支在取不到伙伴行时是 NULL —— 与它比较的谓词全为 NULL，于是那行
    /// 不删（见 [`ShapeClock::ViaSums`]）。
    fn clock_expr(&self, row: &str, clock: ShapeClock) -> String {
        match clock {
            ShapeClock::Column(column) => format!("{row}.{column}"),
            ShapeClock::ViaSums => format!(
                "(SELECT _clock.updated_at FROM {sums} _clock \
                 WHERE _clock.name = {row}.name AND _clock.labels = {row}.labels)",
                sums = cog_core::sql::quote_relation(&self.histogram_sums),
            ),
        }
    }

    /// 形状那一步：把申报形状的孤儿标签集从一张表里删掉。
    ///
    /// 三条腿缺一不可：这行不带申报的那个标签、同名有个带该标签的**更年轻**的行、
    /// 这行已安静超过 [`SUPERSEDED_QUIET_SECS`]。这里没有任何一步在**自己判定一行
    /// 死了**——[`SUPERSEDED_SHAPES`] 里的那一对是申报，三条腿只是「可以安全动手」
    /// 的条件。
    ///
    /// `batch_by`：本表一行一次观测时传它的主键列（样本日志是 `id`），删除按
    /// [`RELEASE_BATCH`] 分批——理由与 [`Self::delete_log_batch`] 相同，一个标签集的
    /// 历史无上界；一行一个序列的累加表传 `None`，一条语句即可。
    async fn delete_superseded_shapes(
        &self,
        table: &str,
        clock: ShapeClock,
        batch_by: Option<&'static str>,
    ) -> SFResult<u64> {
        if SUPERSEDED_SHAPES.is_empty() {
            return Ok(0);
        }
        let names: Vec<String> = SUPERSEDED_SHAPES
            .iter()
            .map(|(n, _)| (*n).to_string())
            .collect();
        let labels: Vec<String> = SUPERSEDED_SHAPES
            .iter()
            .map(|(_, l)| (*l).to_string())
            .collect();
        let table = cog_core::sql::quote_relation(table);
        let mine = self.clock_expr("t", clock);
        let sibling = self.clock_expr("s", clock);
        let legs = format!(
            "t.name = d.name
               AND NOT (t.labels ? d.label)
               AND {mine} < NOW() - ($3::double precision * INTERVAL '1 second')
               AND EXISTS (
                     SELECT 1 FROM {table} s
                      WHERE s.name = t.name
                        AND s.labels ? d.label
                        AND {sibling} > {mine}
                   )"
        );

        match batch_by {
            None => {
                let sql = format!(
                    "DELETE FROM {table} t \
                     USING unnest($1::text[], $2::text[]) AS d(name, label) \
                     WHERE {legs}"
                );
                Ok(sqlx::query(&sql)
                    .bind(names)
                    .bind(labels)
                    .bind(SUPERSEDED_QUIET_SECS as f64)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| SFError::Database(e.to_string()))?
                    .rows_affected())
            }
            Some(id) => {
                let sql = format!(
                    "DELETE FROM {table} WHERE {id} IN (
                         SELECT t.{id} FROM {table} t, unnest($1::text[], $2::text[]) AS d(name, label)
                          WHERE {legs}
                          LIMIT $4
                     )"
                );
                let mut total = 0u64;
                loop {
                    let deleted = sqlx::query(&sql)
                        .bind(&names)
                        .bind(&labels)
                        .bind(SUPERSEDED_QUIET_SECS as f64)
                        .bind(RELEASE_BATCH)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| SFError::Database(e.to_string()))?
                        .rows_affected();
                    total += deleted;
                    if deleted < RELEASE_BATCH as u64 {
                        break;
                    }
                }
                Ok(total)
            }
        }
    }

    fn names(&self) -> Vec<String> {
        self.retired_names
            .iter()
            .map(|n| (*n).to_string())
            .collect()
    }
}

/// One release pass, with whatever it is allowed to report through.
///
/// It is driven from the sweep's loop rather than given a timer of its own:
/// the sweep's period is derived from how fast the log fills and capped at
/// [`crate::metrics_sample_cap`]'s longest wait, which makes it the one
/// periodic visitor the metric store has. Hanging the release off it means a
/// retirement takes effect within one period without inventing a second
/// constant to be wrong about. What it must not inherit from the sweep is the
/// capacity gate: the release runs every cycle, whether or not the store is
/// over budget.
pub struct RetirementPass {
    retirement: std::sync::Arc<MetricsRetirement>,
    metrics: Option<std::sync::Arc<dyn MetricsBackend>>,
    /// The deployment this process is, for the reading below. `None` publishes
    /// unlabelled, which is what a process whose platform never told it which
    /// deployment it belongs to can honestly do.
    deployment: Option<String>,
}

impl RetirementPass {
    pub fn new(retirement: std::sync::Arc<MetricsRetirement>) -> Self {
        Self {
            retirement,
            metrics: None,
            deployment: None,
        }
    }

    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Name this deployment on the reading this pass publishes.
    ///
    /// Every deployment ships the release and runs it on its own cadence, and
    /// they all write into one shared store where a series *is* its label set.
    /// Without this name the passes of two deployments land in the same series,
    /// and the reader's `min_over_time` is then the minimum across both streams
    /// rather than over one deployment's passes. Those are not the same claim:
    /// the release clears a table in one pass however large its backlog, so of
    /// any two passes that straddle a deletion the later one finds the rows
    /// already gone and writes a zero. The merged minimum is therefore zero in
    /// every window holding such a pair, and "some deployment's passes keep
    /// finding rows" — the condition the rule exists for — becomes unreadable on
    /// a series that carries neither deployment's passes.
    ///
    /// The value has to be bounded: a deployment name, not a pod name. Every
    /// series' newest row is kept forever, so an identity that changes per
    /// rollout buys a permanent floor row per rollout.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
        self
    }

    /// One pass. Failures are logged, not propagated: a store that cannot be
    /// reached must not stop the sweep that runs beside it.
    pub async fn run_once(&self) {
        match self.retirement.release().await {
            Ok(outcome) => {
                if outcome.total() > 0 {
                    let counts: HashMap<&'static str, u64> =
                        outcome.per_table().into_iter().collect();
                    info!(
                        removed = outcome.total(),
                        ?counts,
                        "Released retired metric rows"
                    );
                }
                self.report(&outcome).await;
                self.report_outcome(false).await;
            }
            Err(e) => {
                // release 失败时那一格不盖，读它的规则会按伴生钟年龄把「没在
                // 排空」当成「写者停了」而对一个冻住的值静默。这一格是那件事
                // 自己的读数：每一趟都盖，成功 0、失败 1。
                self.report_outcome(true).await;
                warn!(error = %e, "Retired metric release failed")
            }
        }
    }

    /// 发布「这一趟 release 有没有拿到结果」。
    ///
    /// `metrics_retired_rows_removed` 只在 `Ok(outcome)` 分支盖章，所以一次
    /// 失败的 release 会让它停在上一笔值上，而 `metrics_retired_rows_still_arriving`
    /// 的年龄守卫在界内还信它——release 跑不动与表已经排空于是在读数上同形。
    /// 带 deployment 标签，理由与那一格相同：每台都在跑这个循环，不带就会
    /// 把两台合进同一条序列。
    async fn report_outcome(&self, failed: bool) {
        let Some(ref mb) = self.metrics else { return };
        publish_release_outcome(mb, failed, self.deployment.as_deref()).await;
    }

    /// Publish what the pass removed from each table, every cycle.
    ///
    /// Every cycle, cycles that removed nothing included, because one of this
    /// series' readers counts samples instead of reading the value: the rule
    /// that says a release is not draining is `min_over_time(...[1h]) > 0`,
    /// which is "every sample in the last hour was positive". One sample per
    /// pass is what makes that the same claim as "every pass in the last hour
    /// found rows". Gating the write would keep the value honest and break the
    /// claim — the gate that writes only on change, or only on a non-zero
    /// removal, leaves the hour holding change points rather than passes, and
    /// the rule fires the moment the first removal lands instead of after an
    /// hour of them.
    ///
    /// The worry a gate is reached for is real and is answered by this same
    /// write: a pass that removed nothing writes a zero, so a removal that
    /// stops reads as stopped rather than as its last non-zero value held
    /// forever. What that needs is the zero, not a row every pass — the row
    /// every pass is for the counting reader above.
    ///
    /// Whose passes they are has to be on the row as well, for the same reason
    /// the count is: this loop runs in every deployment, so a series identified
    /// only by its table carries the passes of both, and the minimum across two
    /// streams is not the minimum over either one.
    async fn report(&self, outcome: &RetirementOutcome) {
        let Some(ref mb) = self.metrics else { return };
        publish_removals(mb, outcome, self.deployment.as_deref()).await;
    }
}

/// One row per table, whatever the pass removed.
///
/// Split out of the pass so the shape the counting reader depends on can be
/// read and tested without a live store: the pass itself is welded to a pool,
/// and the claim under test is about rows, not about deletion.
///
/// The labels are the table and the deployment that ran the pass. The deployment
/// is not decoration on a reading the reader already scopes itself: it is what
/// makes the series *this deployment's* pass stream, and the reader asks a
/// question about one stream.
async fn publish_removals(
    metrics: &std::sync::Arc<dyn MetricsBackend>,
    outcome: &RetirementOutcome,
    deployment: Option<&str>,
) {
    for (table, removed) in outcome.per_table() {
        let mut labels = HashMap::from([(crate::LABEL_TABLE.to_string(), table.to_string())]);
        if let Some(deployment) = deployment {
            labels.insert(DEPLOYMENT_LABEL.to_string(), deployment.to_string());
        }
        if let Err(e) = metrics
            .record_gauge(RETIRED_ROWS_REMOVED_METRIC, removed as f64, labels)
            .await
        {
            warn!(
                error = %e,
                metric = %RETIRED_ROWS_REMOVED_METRIC,
                table,
                "retirement removal gauge emit failed"
            );
        }
    }
}

/// One row per pass, saying whether the release returned.
///
/// The removal rows above are stamped only once the release returns, so a
/// release that fails leaves the reader of those rows trusting the last value
/// until its own age bound closes the rule over it — a release that cannot run
/// and a table that is drained read the same. This is the reading that answers
/// for the pass itself: 0 when it returned, 1 when it did not, written every
/// pass. It carries the deployment for the same reason the removal rows do —
/// one store is shared, and a series without the name would carry two
/// deployments' passes.
///
/// Split out of the pass so the row it writes can be read and tested without a
/// live store, exactly as [`publish_removals`] is.
async fn publish_release_outcome(
    metrics: &std::sync::Arc<dyn MetricsBackend>,
    failed: bool,
    deployment: Option<&str>,
) {
    let mut labels = HashMap::new();
    if let Some(deployment) = deployment {
        labels.insert(DEPLOYMENT_LABEL.to_string(), deployment.to_string());
    }
    if let Err(e) = metrics
        .record_gauge(
            RETIREMENT_RELEASE_FAILED_METRIC,
            if failed { 1.0 } else { 0.0 },
            labels,
        )
        .await
    {
        warn!(
            error = %e,
            metric = %RETIREMENT_RELEASE_FAILED_METRIC,
            "retirement release outcome gauge emit failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryMetricsBackend;

    /// Every row this pass has published, oldest first. The reading under test
    /// is how many there are and what the last one says, so the rows are the
    /// reading and not the values.
    async fn published(metrics: &MemoryMetricsBackend) -> Vec<(String, f64)> {
        metrics
            .query_gauge_range(
                RETIRED_ROWS_REMOVED_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|sample| {
                (
                    sample
                        .labels
                        .get(crate::LABEL_TABLE)
                        .cloned()
                        .unwrap_or_default(),
                    sample.value,
                )
            })
            .collect()
    }

    fn backend() -> (
        std::sync::Arc<MemoryMetricsBackend>,
        std::sync::Arc<dyn MetricsBackend>,
    ) {
        let concrete = std::sync::Arc::new(MemoryMetricsBackend::new());
        let erased: std::sync::Arc<dyn MetricsBackend> =
            std::sync::Arc::clone(&concrete) as std::sync::Arc<dyn MetricsBackend>;
        (concrete, erased)
    }

    /// The label set of every row this pass published, oldest first. Read beside
    /// [`published`], which drops the labels to read the values.
    async fn published_labels(metrics: &MemoryMetricsBackend) -> Vec<HashMap<String, String>> {
        metrics
            .query_gauge_range(
                RETIRED_ROWS_REMOVED_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|sample| sample.labels)
            .collect()
    }

    /// Two passes that removed nothing leave two rows per table, and the newer
    /// of each pair says zero.
    ///
    /// The rule reading this series asks whether every sample in the last hour
    /// was positive, so a pass that writes nothing is a pass the rule cannot
    /// see: gated on the value, or on "only when it found something", the hour
    /// would hold change points instead of passes and the rule would fire on
    /// the first removal rather than on an hour of them. The second assertion
    /// is what keeps the gate from being reached for the other way round — a
    /// count that goes quiet has to read as quiet, which is the zero, not the
    /// absence of a row.
    #[tokio::test]
    async fn a_pass_that_removed_nothing_still_reports() {
        let (concrete, metrics) = backend();
        let nothing = RetirementOutcome::default();

        publish_removals(&metrics, &nothing, None).await;
        let after_first = published(&concrete).await;
        assert_eq!(
            after_first.len(),
            nothing.per_table().len(),
            "the first pass must report one row per table: {after_first:?}"
        );
        assert!(
            after_first.iter().all(|(_, value)| *value == 0.0),
            "a pass that removed nothing reports zero: {after_first:?}"
        );

        publish_removals(&metrics, &nothing, None).await;
        let after_second = published(&concrete).await;
        assert_eq!(
            after_second.len(),
            2 * nothing.per_table().len(),
            "a second pass that removed nothing was not reported: the reader that \
             counts samples per pass cannot see it: {after_second:?}"
        );
    }

    /// And a pass that did remove something moves the value, under the same
    /// label set as the zeros it replaces.
    ///
    /// Counting rows alone would pass an implementation that only ever wrote
    /// zeros, so the value face is judged beside the row face.
    #[tokio::test]
    async fn a_pass_that_removed_rows_reports_them_per_table() {
        let (concrete, metrics) = backend();
        publish_removals(&metrics, &RetirementOutcome::default(), None).await;

        let outcome = RetirementOutcome {
            samples: 7,
            counter_totals: 0,
            histogram_buckets: 3,
            histogram_sums: 0,
        };
        publish_removals(&metrics, &outcome, None).await;

        let rows = published(&concrete).await;
        let latest: HashMap<String, f64> = rows
            .iter()
            .rev()
            .take(outcome.per_table().len())
            .map(|(table, value)| (table.clone(), *value))
            .collect();
        assert_eq!(
            latest.get("samples").copied(),
            Some(7.0),
            "the table that lost rows must report how many: {rows:?}"
        );
        assert_eq!(
            latest.get("histogram_buckets").copied(),
            Some(3.0),
            "every table reports its own count rather than a shared one: {rows:?}"
        );
        assert_eq!(
            latest.get("counter_totals").copied(),
            Some(0.0),
            "a table that lost nothing is reported as zero, not omitted: {rows:?}"
        );
    }

    /// A reading that is one deployment's own pass stream says which deployment
    /// ran it.
    ///
    /// Every deployment ships this loop and they share one store, where a series
    /// *is* its label set. Two deployments' passes in one series make the
    /// reader's `min_over_time` a minimum across both streams, and since the
    /// release clears a table in one pass however large its backlog the second
    /// pass of any pair finds nothing and writes zero — so the merged minimum is
    /// zero exactly when a deletion happened, and the rule for "a release that
    /// is not draining" could never fire. The label is what keeps the question
    /// about one stream askable.
    ///
    /// The two assertions are the two ways to get it wrong: a missing name (the
    /// series is shared again) and a name that grows per restart (a pod name
    /// buys a permanent floor row per rollout, since every series' newest row is
    /// kept forever).
    #[tokio::test]
    async fn the_reading_names_the_deployment_that_ran_the_pass() {
        let (concrete, metrics) = backend();
        let outcome = RetirementOutcome {
            samples: 7,
            ..RetirementOutcome::default()
        };

        publish_removals(&metrics, &outcome, Some("probe-deployment")).await;

        for labels in published_labels(&concrete).await {
            assert_eq!(
                labels.get(DEPLOYMENT_LABEL).map(String::as_str),
                Some("probe-deployment"),
                "a reading that is one deployment's passes must name it: {labels:?}"
            );
            assert!(
                labels.contains_key("table"),
                "the table stays on the reading: a merged scalar would hide which \
                 of the four stores is not draining: {labels:?}"
            );
            assert_eq!(
                labels.len(),
                2,
                "the deployment and the table are the whole identity: {labels:?}"
            );
        }
    }

    /// And a process the platform never told which deployment it is publishes
    /// unlabelled rather than under an empty name.
    ///
    /// A blank label value is a name every such process would answer with, which
    /// is the same collision the label exists to end — under a value that reads
    /// like an answer.
    #[tokio::test]
    async fn a_pass_with_no_deployment_available_publishes_no_name() {
        let (concrete, metrics) = backend();

        publish_removals(&metrics, &RetirementOutcome::default(), None).await;
        let named = RetirementPass::new(std::sync::Arc::new(MetricsRetirement::new(
            PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/nothing").expect("a lazy pool"),
        )))
        .with_deployment("   ");
        assert_eq!(
            named.deployment, None,
            "a blank deployment name is no name, not an empty one"
        );

        for labels in published_labels(&concrete).await {
            assert!(
                !labels.contains_key(DEPLOYMENT_LABEL),
                "a deployment nobody named must publish no name at all: {labels:?}"
            );
        }
    }

    /// The reading that answers for the pass itself moves off zero when the
    /// release does not return, which is the shape the removal rows cannot show:
    /// they are simply not written, and the store goes on serving their last
    /// value.
    ///
    /// The row is judged on both faces — a row exists for a returning pass and
    /// it says zero, and a failing pass writes a newer row that says one — so an
    /// implementation that only ever wrote zeros, or only ever wrote on failure,
    /// is caught rather than passed.
    #[tokio::test]
    async fn the_release_outcome_says_whether_the_pass_returned() {
        let (concrete, metrics) = backend();

        publish_release_outcome(&metrics, false, Some("probe-deployment")).await;
        publish_release_outcome(&metrics, true, Some("probe-deployment")).await;

        let rows = concrete
            .query_gauge_range(
                RETIREMENT_RELEASE_FAILED_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "both passes are reported: {rows:?}");
        assert_eq!(
            rows[0].value, 0.0,
            "a pass that returned reports zero, not silence: {rows:?}"
        );
        assert_eq!(
            rows[1].value, 1.0,
            "a pass that did not return is the reading the removal rows are \
             missing: {rows:?}"
        );
        for row in &rows {
            assert_eq!(
                row.labels.get(DEPLOYMENT_LABEL).map(String::as_str),
                Some("probe-deployment"),
                "the outcome names who ran the pass, for the same reason the \
                 removal rows do: {row:?}"
            );
        }
    }

    /// The counters whose label set was widened while the name stayed, and
    /// whose orphans were still being served when this was written.
    ///
    /// Not a restatement of the registry for its own sake: each pair is here
    /// because someone counted the writes on 2026-10-29 and the alternative
    /// reading of the same store rows is wrong. The orphans outvalue their
    /// live siblings by an order of magnitude, so a reader that sees the
    /// family without this release sees the frozen total and not the traffic.
    ///
    /// A name leaves this list only with a measurement that says its producer
    /// stopped carrying the label, which is the opposite of why it was added.
    #[test]
    fn the_shapes_whose_label_became_mandatory_stay_declared() {
        for (name, label) in [
            ("cogneva_mainline_ci_verdict_total", "ask"),
            ("cogneva_process_report_ingest_total", "node"),
        ] {
            assert!(
                SUPERSEDED_SHAPES.contains(&(name, label)),
                "{name} without {label} is a frozen total that reads as traffic: \
                 its live sibling is written every minute and the orphan has been \
                 quiet since the day the label was added"
            );
        }
    }
}

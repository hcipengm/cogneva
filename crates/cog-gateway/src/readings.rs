//! The readings that answer "is anything about to break".
//!
//! Each one is a decision that has to be made now — a disk that will not fit the
//! next build, a database whose oldest transaction has stopped moving, an
//! ingress returning 5xx, a vector store that got slower, a dependency that is
//! no longer reachable. The WebUI reads them here rather than from Grafana
//! because the value is what an operator acts on; Grafana is where they go to
//! ask the follow-up question, and every reading carries the panel it belongs to.
//!
//! Every reading names the series it reads and the file that produces it. A
//! reading whose series nobody produces is not "zero": it is unanswerable, and
//! the shape below keeps that case distinct from a real zero.
//!
//! Two groups, because the two answer different questions about different
//! machinery. `infra` is what the platform stands on — disk, database, vector
//! store, ingress, probes — every series foreign to this repository. `delivery`
//! is the platform's own chain, and it runs in one direction: an upstream CI
//! gate decides whether a revision may enter, the model pool decides whether the
//! work inside can run at all, and the change queue decides whether finished
//! work can leave. Until this group existed the whole chain was invisible on
//! the product's own board: on 2026-10-31 the mainline stood still for 2.7 hours
//! while every row here read "正常", because no reading asked about it.

use std::collections::HashMap;

/// Which question a reading belongs to.
///
/// The board draws one section per group. A reading filed under the wrong group
/// is not a wrong number, but it is read in the wrong minute: an operator
/// scanning "基础设施" for a stalled rollout will not find it, and nothing about
/// the row says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadingGroup {
    /// The platform's surroundings: machine, database, vector store, ingress,
    /// reachability. Producers are foreign to this repository.
    Infra,
    /// The platform's own delivery chain. Producers are files in this repository,
    /// which is what makes the sourcing gate able to check them.
    Delivery,
}

impl ReadingGroup {
    /// The word the group travels as, and the key the board's labels use.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadingGroup::Infra => "infra",
            ReadingGroup::Delivery => "delivery",
        }
    }

    /// Every group this vocabulary has, for readers that must cover all of them
    /// rather than listing the variants a second time.
    pub const ALL: [ReadingGroup; 2] = [ReadingGroup::Infra, ReadingGroup::Delivery];
}

impl serde::Serialize for ReadingGroup {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadingSpec {
    pub id: &'static str,
    pub title: &'static str,
    /// Which section of the board this row belongs to.
    pub group: ReadingGroup,
    /// PromQL. One scalar per reading: a reading that needs two numbers is two
    /// readings, because a card can only show one verdict.
    pub query: &'static str,
    pub unit: &'static str,
    /// Thresholds, read according to `higher_is_worse`. `None` is "this reading
    /// has no threshold" — a count that is only meaningful as a trend must not
    /// borrow a number somebody invented, or it will start firing on its own.
    pub warn: Option<f64>,
    pub crit: Option<f64>,
    pub higher_is_worse: bool,
    /// The series this reads, spelled the way it appears in Prometheus. Written
    /// out so a reader can paste it into the Grafana explore view.
    pub series: &'static str,
    /// The file in this repository that produces it. Foreign series name their
    /// owner instead ("postgres-exporter", "traefik"), because that is who has
    /// to be looked at when the series disappears.
    pub produced_by: &'static str,
}

/// Disk headroom, as a fraction of the filesystem the root lives on.
pub const DISK_FREE: ReadingSpec = ReadingSpec {
    id: "disk_free_ratio",
    title: "根盘剩余占比",
    group: ReadingGroup::Infra,
    query: r#"min(1 - (node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay|squashfs"} / node_filesystem_size_bytes{mountpoint="/",fstype!~"tmpfs|overlay|squashfs"}))"#,
    unit: "%",
    warn: Some(0.15),
    crit: Some(0.07),
    higher_is_worse: false,
    series: "node_filesystem_avail_bytes / node_filesystem_size_bytes",
    produced_by: "node-exporter",
};

/// How much of postgres' connection ceiling is in use.
pub const PG_CONNECTIONS: ReadingSpec = ReadingSpec {
    id: "pg_connections",
    title: "数据库连接数",
    group: ReadingGroup::Infra,
    query: r#"sum(pg_stat_database_numbackends{datname="cogneva"})"#,
    unit: "条",
    warn: Some(80.0),
    crit: Some(110.0),
    higher_is_worse: true,
    series: "pg_stat_database_numbackends",
    produced_by: "postgres-exporter（数据库 Pod 内的边车）",
};

/// The oldest transaction still open. A number that only grows means something
/// is holding a snapshot and vacuum cannot reclaim behind it.
pub const PG_OLDEST_TX: ReadingSpec = ReadingSpec {
    id: "pg_oldest_tx_seconds",
    title: "最老事务年龄",
    group: ReadingGroup::Infra,
    query: r#"max(pg_stat_activity_max_tx_duration{datname="cogneva"})"#,
    unit: "秒",
    warn: Some(300.0),
    crit: Some(1800.0),
    higher_is_worse: true,
    series: "pg_stat_activity_max_tx_duration",
    produced_by: "postgres-exporter（数据库 Pod 内的边车）",
};

/// Vectors held by the store. Read as a trend, not a limit: a jump means an
/// ingest nobody announced.
pub const QDRANT_VECTORS: ReadingSpec = ReadingSpec {
    id: "qdrant_vectors",
    title: "向量总数",
    group: ReadingGroup::Infra,
    query: "sum(collections_vector_total)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "collections_vector_total",
    produced_by: "qdrant",
};

/// Ingress 5xx per second. The ratio would be nicer, but a rate of zero when
/// there is no traffic and a rate of zero when everything is fine are the same
/// number here; the panel beside it carries the request rate for that reason.
pub const INGRESS_5XX: ReadingSpec = ReadingSpec {
    id: "ingress_5xx_rate",
    title: "入口 5xx 速率",
    group: ReadingGroup::Infra,
    query: r#"sum(rate(traefik_service_requests_total{code=~"5.."}[5m])) or sum(rate(nginx_ingress_controller_requests{status=~"5.."}[5m]))"#,
    unit: "/s",
    warn: Some(0.1),
    crit: Some(1.0),
    higher_is_worse: true,
    series: "traefik_service_requests_total{code=~\"5..\"}",
    produced_by: "traefik（k3s 形态）/ ingress-nginx（标准 k8s 形态）",
};

/// Probe targets that are currently failing, including the certificate check.
pub const UNREACHABLE: ReadingSpec = ReadingSpec {
    id: "unreachable_targets",
    title: "不通的目标数",
    group: ReadingGroup::Infra,
    query: "sum(probe_success == 0)",
    unit: "个",
    warn: Some(1.0),
    crit: Some(2.0),
    higher_is_worse: true,
    series: "probe_success",
    produced_by: "blackbox-exporter（`19-blackbox-exporter.yaml` + 可达性值域文件）",
};

/// How long the upstream CI gate has been calling this tree red, in one
/// continuous stretch.
///
/// The chain's first joint, but read as an age and not as a state: the stretch
/// survives a revision change and survives a round in which the upstream has
/// not answered yet, so a non-zero value does not mean a hold is on. On
/// 2026-11-02 this row read 4802 seconds while the state file said
/// `ci_hold=None` and the verdict was `no_evidence` — the rollout was moving,
/// only the upstream was still running. `mainline_ci_hold_active` is the row
/// that answers "is a hold on"; this one answers "how long has it been red",
/// which is what the four-hour bound is about.
///
/// The critical bound is the alert rule's own number — `mainline_ci_hold_seconds`
/// fires past four hours — so the board and the pager cannot drift apart. The
/// warning bound is a stated override rather than a derived one: the longest
/// segment observed before this reading existed was 43 minutes (released on a
/// `no_evidence` reading), one upstream round spans about 11 minutes, and the
/// deployer's own beat is 10; an hour sits above every segment that has ended on
/// its own. It will be replaced by this series' own distribution once the store
/// holds a week of it.
pub const MAINLINE_CI_HOLD: ReadingSpec = ReadingSpec {
    id: "mainline_ci_hold_seconds",
    title: "主线被 CI 扣住的时长",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_mainline_ci_hold_seconds)",
    unit: "秒",
    warn: Some(3600.0),
    crit: Some(14400.0),
    higher_is_worse: true,
    series: "cogneva_mainline_ci_hold_seconds",
    produced_by: "crates/cog-reflection/src/mainline_deployer.rs",
};

/// Whether anything is working on the rollout the CI gate is holding.
///
/// The row beside `mainline_ci_hold_seconds`, and the one that decides how to
/// read it. A hold that has a goal behind it is the gate working; a hold whose
/// every round failed to hand that goal to the planner is the mainline standing
/// still with nobody told — the same row until 2026-10-31, when every upstream
/// answered 503 and each round's refusal lived only in a log the next rollout
/// took with it.
///
/// One is critical and there is no warning step: the deployer already waits out
/// three of its own beats inside the alert rule before this is a fault, and a
/// hold with no owner does not get better by being watched longer. The bound is
/// that rule's `== 1`, so the board and the pager read the same number.
pub const MAINLINE_CI_HOLD_UNOWNED: ReadingSpec = ReadingSpec {
    id: "mainline_ci_hold_unowned",
    title: "主线被扣住却无人接手",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_mainline_ci_hold_unowned)",
    unit: "0/1",
    warn: None,
    crit: Some(1.0),
    higher_is_worse: true,
    series: "cogneva_mainline_ci_hold_unowned",
    produced_by: "crates/cog-reflection/src/mainline_deployer.rs",
};

/// Whether a hold is on right now.
///
/// The third row of the same joint, and the only one of the three that answers
/// this question. The duration is a stretch that outlives the hold and the
/// unowned flag is 0 both when a hold is owned and when there is no hold at
/// all, so a reader with just those two takes 4802 seconds on a round with
/// `ci_hold=None` for a hold that is on — which is what happened on 2026-11-02.
///
/// There is no threshold: 1 is not a fault (the gate holding a red revision is
/// the gate working) and the two rows beside it decide when that hold is a
/// problem. The row is on the board so the duration can be read as an age.
pub const MAINLINE_CI_HOLD_ACTIVE: ReadingSpec = ReadingSpec {
    id: "mainline_ci_hold_active",
    title: "主线此刻被扣住",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_mainline_ci_hold_active)",
    unit: "0/1",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_mainline_ci_hold_active",
    produced_by: "crates/cog-reflection/src/mainline_deployer.rs",
};

/// How long the oldest change has been blocked by the model pool.
///
/// The chain's second joint. A change that cannot call a model never becomes a
/// build, so this age grows while every worker is idle and every queue looks
/// busy. The bound is the one the `change_pool_block_stranded` rule already
/// carries, for the same reason as above.
///
/// The count beside it is not a second reading on purpose: the number of
/// blocked rows says how wide the jam is, and the age says whether it is moving.
/// Only the second one is a decision, and a row showing four thousand blocked
/// changes with no threshold would read "正常" while the pipeline is stopped.
pub const CHANGE_POOL_BLOCKED_OLDEST: ReadingSpec = ReadingSpec {
    id: "change_pool_blocked_oldest_seconds",
    title: "被模型池挡住的最老变更",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_change_dispatch_pool_blocked_oldest_seconds)",
    unit: "秒",
    warn: None,
    crit: Some(1800.0),
    higher_is_worse: true,
    series: "cogneva_change_dispatch_pool_blocked_oldest_seconds",
    produced_by: "crates/cog-orchestrator/src/change_ledger.rs",
};

/// How many model upstreams the pool reports healthy.
///
/// The chain's third joint, and the one that decides whether the other two can
/// move at all: with zero healthy upstreams nothing runs, and `higher_is_worse`
/// is false because fewer is worse. Zero is critical, one is a warning — one
/// answering upstream is a pool with no redundancy left, which is the state that
/// made 2026-10-31 look like an idle system rather than a broken one.
///
/// `max by (upstream)` is not decoration: several pods publish this gauge for
/// the same upstream, and summing the raw series would multiply the population
/// by the replica count and report health that does not exist.
pub const LLM_UPSTREAMS_HEALTHY: ReadingSpec = ReadingSpec {
    id: "llm_upstreams_healthy",
    title: "健康的模型上游数",
    group: ReadingGroup::Delivery,
    query: r#"sum(max by (upstream) (llm_upstream_healthy{upstream=~".+"}))"#,
    unit: "个",
    warn: Some(1.0),
    crit: Some(0.0),
    higher_is_worse: false,
    series: "llm_upstream_healthy",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// How far the build cache sits above the cap it was given.
///
/// The cap is derived from the volume and the reclaim pass trims to it; this is
/// the gap the pass has not closed yet. A cap that is merely being approached
/// and a cap that is exceeded by more than the pass can remove in one turn are
/// different problems, and only the second one means the volume keeps filling
/// between passes — so the reading is the excess, in bytes, and zero is quiet.
pub const BUILD_CACHE_OVER_CAP: ReadingSpec = ReadingSpec {
    id: "build_cache_over_cap_bytes",
    title: "构建缓存超出上限的量",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_build_target_over_limit_bytes)",
    unit: "字节",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: cog_core::build_cache::BUILD_TARGET_OVER_LIMIT_METRIC,
    produced_by: "crates/cog-core/src/build_cache.rs",
};

/// Discovery probes that ended without a usable answer.
///
/// A probe answers one question about a candidate and is supposed to end in a
/// verdict. Expired, rejected, inconclusive and unanswered are the four ways it
/// ends with nothing, and each was a silent outcome: the loop keeps starting
/// probes, the face keeps showing the candidates, and the only reader was a
/// panel. Summed over the four because the decision is the same for all of
/// them — the probe machinery is failing to answer — while the split between
/// them stays on the per-cause series for whoever asks why.
pub const DISCOVERY_PROBES_UNANSWERED: ReadingSpec = ReadingSpec {
    id: "discovery_probes_unanswered",
    title: "没有给出结论的探测",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_discovery_probe_expired[30m])) + sum(increase(cogneva_discovery_probe_rejected[30m])) + sum(increase(cogneva_discovery_probe_inconclusive[30m])) + sum(increase(cogneva_discovery_probe_unanswered[30m]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_discovery_probe_rejected / expired / inconclusive / unanswered",
    produced_by: "crates/cog-reflection/src/discovery_outcome_readings.rs",
};

/// How long until the pool probes again, and when an upstream said it would be back.
///
/// Backoff is the pool's own answer to a bad window, and it is invisible from
/// every other number: `llm_upstreams_healthy` reads zero both while the pool is
/// waiting out a backoff and while every upstream is broken, and the two call
/// for different actions. These two timestamps are what separates them -- the
/// probe's next attempt says the pool is still trying on a schedule, and an
/// upstream that reported its own recovery instant says somebody told us when.
///
/// `consecutive_failures` rides along because it is the count behind the wait:
/// a backoff that grows while failures stop is a pool that never got a probe
/// through.
pub const LLM_POOL_BACKOFF: ReadingSpec = ReadingSpec {
    id: "llm_pool_backoff_seconds",
    title: "池子下一次探测还要等多久",
    group: ReadingGroup::Delivery,
    query: "max(llm_pool_next_attempt_unix) - time()",
    unit: "秒",
    warn: Some(600.0),
    crit: Some(3600.0),
    higher_is_worse: true,
    series: "llm_pool_next_attempt_unix / llm_pool_evidenced_recovery_unix / llm_upstream_consecutive_failures",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// How much of the pool is reserve: upstreams known but not in the rotation.
///
/// A pool whose reserve population is empty has no headroom left: the next
/// upstream failure is a full outage rather than a reorder. `reserve_since_unix`
/// is the age of the newest reserve entry, so a reserve that stopped being
/// refilled reads the same as one that was never filled unless this is read
/// beside it.
pub const LLM_POOL_RESERVE: ReadingSpec = ReadingSpec {
    id: "llm_pool_reserve",
    title: "池子的储备上游",
    group: ReadingGroup::Delivery,
    query: "max(llm_pool_reserve_population)",
    unit: "个",
    warn: Some(1.0),
    crit: Some(0.0),
    higher_is_worse: false,
    series: "llm_pool_reserve_population / llm_pool_reserve_since_unix / llm_pool_quota_window_secs / llm_upstream_position / llm_upstream_quota_window_secs / llm_upstream_shape_errored / llm_request_param_clamped_total",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// Attempts a shutdown had to leave behind, and the ones it resumed later.
///
/// Two numbers that only mean something together: an abandoned attempt says the
/// drain ran out of budget with work in flight, and a resumed one says the
/// journal brought it back. Reading only the first makes every rollout look
/// lossy; reading only the second makes the loss look free.
pub const SHUTDOWN_DRAIN_LOSS: ReadingSpec = ReadingSpec {
    id: "shutdown_drain_loss",
    title: "关停时被硬切的尝试",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_shutdown_drain_undeclared_total[30m])) + sum(increase(cogneva_attempt_resume_total[30m]))",
    unit: "条",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "cogneva_shutdown_drain_undeclared_total / cogneva_attempt_resume_total / cogneva_shutdown_drain_milliseconds",
    produced_by: "crates/cog-agent/src/plugin.rs",
};

/// Images and layers the registry's reclaim removed, and what it kept.
///
/// The registry is the one volume that grows without a reader noticing: a prune
/// that stops running and a registry with nothing to prune look identical until
/// the bytes are read. These four are the pass's own account -- what it freed,
/// what it kept, and how much it pruned.
pub const REGISTRY_RECLAIM: ReadingSpec = ReadingSpec {
    id: "registry_reclaim",
    title: "镜像仓库回收掉的镜像与层",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_buildah_store_freed_bytes_total[24h]))",
    unit: "字节",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_buildah_store_freed_bytes_total / cogneva_buildah_store_kept_images / cogneva_buildah_store_pruned_images_total / cogneva_buildah_store_pruned_layers_total",
    produced_by: "crates/cog-reflection/src/mainline_rollout.rs",
};

/// Tool call records that reached the permanent Raw layer.
///
/// Every call is written whole when the tool returns, so this is the volume the
/// archive is taking and, read against `cogneva_tool_output_archive_failed_total`,
/// the share of calls whose permanent copy landed. It has no threshold of its
/// own: a record landed is the expected outcome, so a number moving here says
/// how much traffic the layer is carrying, not whether anything is wrong. The
/// decision lives on the failure counter's alert rule, where one increment is
/// already a loss.
pub const TOOL_CALL_RECORDS_ARCHIVED: ReadingSpec = ReadingSpec {
    id: "tool_call_records_archived",
    title: "工具调用记录落地",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_tool_call_archived_total[30m]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_tool_call_archived_total / cogneva_tool_output_archive_failed_total",
    produced_by: "crates/cog-agent/src/archive.rs",
};

/// Tool output that had to be truncated before an agent could read it.
///
/// A truncation is not an error: it is the budget doing its job. What is worth
/// a reading is the rate -- an agent whose every call returns truncated output
/// is working from summaries it did not ask for, and the character count says
/// how much of the evidence never reached the model.
pub const TOOL_OUTPUT_TRUNCATED: ReadingSpec = ReadingSpec {
    id: "tool_output_truncated",
    title: "被截断的工具输出",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_tool_output_truncated_total[30m])) + sum(increase(cogneva_tool_output_truncated_chars_total[30m]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_tool_output_truncated_total / cogneva_tool_output_truncated_chars_total / cogneva_tool_output_fetch_budget_exhausted_total",
    produced_by: "crates/cog-agent/src/archive.rs",
};

/// Rows and bytes the two append-only stores are holding.
///
/// The storage face's own size: every one of these tables keeps rows that no
/// retention pass has removed, and "the table is big" is not a reading until it
/// is compared with the retention bound that is supposed to cap it. Read
/// together so a table that stopped being trimmed is visible next to the ones
/// that are still moving.
pub const STORE_GROWTH: ReadingSpec = ReadingSpec {
    id: "store_growth",
    title: "追加表的行与字节",
    group: ReadingGroup::Infra,
    query: "sum(metrics_samples_bytes) + sum(cogneva_checkpoints_bytes) + sum(cogneva_observability_events_bytes)",
    unit: "字节",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "metrics_samples_bytes / cogneva_checkpoints_bytes / cogneva_observability_events_bytes / cogneva_task_events_rows / cogneva_task_events_tasks / cogneva_llm_usage_rows",
    produced_by: "crates/cog-storage/src/plugin.rs",
};

/// Mainline work that was deferred, superseded or skipped instead of landing.
///
/// The delivery chain's counterweight: every number that says work is moving has
/// a sibling here that says why it did not. A deferral is the gate telling a
/// change to come back, a superseded rollout is a second change overtaking the
/// first, and a skipped rev is the deployer deciding not to build at all --
/// three different reasons the same change never arrived, and none of them
/// visible from the counts of what did.
pub const MAINLINE_SETBACKS: ReadingSpec = ReadingSpec {
    id: "mainline_setbacks",
    title: "主线被打回与越过的次数",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_mainline_advance_deferrals[24h])) + sum(increase(cogneva_mainline_superseded_rollout_total[24h])) + sum(increase(cogneva_deployer_revs_skipped_total[24h])) + sum(increase(cogneva_mainline_ci_check_excluded_total[24h])) + sum(increase(cogneva_landing_ci_failure_inherited_total[24h]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_mainline_advance_deferrals / cogneva_mainline_superseded_rollout_total / cogneva_deployer_revs_skipped_total / cogneva_mainline_ci_check_excluded_total / cogneva_landing_ci_failure_inherited_total",
    produced_by: "crates/cog-reflection/src/mainline_rollout.rs",
};

/// Resource events the bus could not deliver.
///
/// Dropping one is the resource face going blind for that change: the pending
/// queue keeps its depth and nothing on any board says an event never arrived.
pub const RESOURCE_EVENTS_DROPPED: ReadingSpec = ReadingSpec {
    id: "resource_events_dropped",
    title: "被丢弃的资源事件",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_resource_events_dropped_total[30m]))",
    unit: "条",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "cogneva_resource_events_dropped_total / cogneva_resource_events_pending / cogneva_resource_events",
    produced_by: "crates/cog-orchestrator/src/resource_ledger.rs",
};

/// How hard the judge Job is being throttled against its own limits.
///
/// The bound the size law sets is only right if the Job is not being throttled
/// underneath it: a ratio near one means the container spent its period on the
/// run queue, and the reading it reports is then about the host, not the work.
pub const ROLLOUT_JOB_THROTTLED: ReadingSpec = ReadingSpec {
    id: "rollout_job_throttled_ratio",
    title: "判据 Job 被限流的比例",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_rollout_job_cpu_throttled_ratio)",
    unit: "0-1",
    warn: Some(0.2),
    crit: Some(0.5),
    higher_is_worse: true,
    series: "cogneva_rollout_job_cpu_throttled_ratio / cogneva_rollout_job_memory_peak_ratio / cogneva_rollout_job_reading_unix",
    produced_by: "crates/cog-reflection/src/mainline_rollout.rs",
};

/// Memory the extraction and dead-letter paths left behind.
///
/// Two populations that both mean "the loop is behind": raw sources whose
/// summary is still owed, and entries that failed for their own content and
/// will not be retried. A backlog that only grows is the extraction loop
/// failing quietly, because every other memory number keeps moving.
pub const MEMORY_BACKLOG: ReadingSpec = ReadingSpec {
    id: "memory_backlog",
    title: "记忆萃取积压与死信",
    group: ReadingGroup::Delivery,
    query: "max(memory_unextracted_raw)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series:
        "memory_unextracted_raw / memory_dead_letter_raw / cogneva_memory_dropped_entries_total",
    produced_by: "crates/cog-memory/src/plugin.rs",
};

/// How long a drain actually took, at its slow tail.
///
/// The budget is a policy; this is what the policy is being spent on. Read at
/// p95 rather than the mean because one drain that waited out the whole budget
/// is the case that sizes it, and a mean over many fast ones hides exactly那一条.
pub const SHUTDOWN_DRAIN_DURATION: ReadingSpec = ReadingSpec {
    id: "shutdown_drain_duration_seconds",
    title: "排空耗时（p95）",
    group: ReadingGroup::Delivery,
    query: "histogram_quantile(0.95, sum by (le) (rate(cogneva_shutdown_drain_milliseconds_bucket[30m]))) / 1000",
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_shutdown_drain_milliseconds",
    produced_by: "crates/cog-agent/src/plugin.rs",
};

/// Event-bus handoffs that had to be rebuilt rather than delivered directly.
///
/// A handoff is the bus telling a consumer to catch up from the store. It is
/// the recovery path, so a rate that climbs while the direct path looks quiet
/// says the bus is dropping frames that nobody counts elsewhere.
pub const EVENT_BUS_HANDOFFS: ReadingSpec = ReadingSpec {
    id: "event_bus_handoffs",
    title: "事件总线的交接次数",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_event_bus_handoff_total[30m]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_event_bus_handoff_total",
    produced_by: "crates/cog-supervisor/src/consumer_lag.rs",
};

/// Rows, streams and the age of the oldest row in the event store.
///
/// Nothing retires this table any more: a stream is replayed from offset zero by
/// both readers, so a retired one answers with a gap that looks like the start of
/// the run. Growth is therefore a reading rather than a policy, and this is where
/// it is read — rows, distinct streams, and how far back the oldest row reaches.
/// A store whose growth changed shape shows up here first, as an age that stops
/// moving while the row count keeps climbing.
pub const TASK_EVENT_STORE: ReadingSpec = ReadingSpec {
    id: "task_event_store_rows",
    title: "事件表的行数",
    group: ReadingGroup::Infra,
    query: "max(cogneva_task_events_rows) or on() max(cogneva_task_events_tasks) or on() max(cogneva_llm_usage_rows)",
    unit: "行",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_task_events_rows / cogneva_task_events_tasks",
    produced_by: "crates/cog-storage/src/task_event_retention.rs",
};

/// How far back the event archive reaches.
///
/// The archive is never retired (a stream replayed from offset zero would answer
/// with a gap shaped like the start of the run), so its depth is a reading rather
/// than a policy: an age that keeps climbing says the archive is growing, an age
/// that stops moving while the row count climbs says the oldest stream is being
/// appended to and nothing new is starting. Without this panel the depth would
/// have no reader at all, and a growing archive would only be visible as a disk
/// number nobody attributes.
pub const TASK_EVENT_ARCHIVE_AGE: ReadingSpec = ReadingSpec {
    id: "task_event_archive_age_seconds",
    title: "事件档案有多深",
    group: ReadingGroup::Infra,
    query: "max(cogneva_task_events_oldest_age_seconds)",
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_task_events_oldest_age_seconds",
    produced_by: "crates/cog-storage/src/task_event_retention.rs",
};

/// How far the running build sits behind the newest release.
///
/// The version contract compares the binary's declared version with the tag it
/// was built from; a commit count that keeps climbing means releases stopped
/// while the tree kept moving, and every other delivery number keeps looking
/// healthy because the loop that would notice is the one that stopped.
pub const VERSION_LAG: ReadingSpec = ReadingSpec {
    id: "version_lag_commits",
    title: "离最近一次发布还有多少提交",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_version_commits_since_release) or on() max(cogneva_version_declared_info)",
    unit: "提交",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_version_commits_since_release / cogneva_version_declared_info / cogneva_promotion_week_changes",
    produced_by: "crates/cog-reflection/src/version_contract.rs",
};

/// Requests the gateway served, and how slow its slow tail was.
///
/// The one pair that answers "is the platform itself healthy" without any
/// judgement layered on it: the rate says traffic, the p95 says whether the
/// process is keeping up. Neither has a threshold here -- the bound belongs to
/// whoever sets the SLO, and inventing one would be a second copy of it.
pub const HTTP_LATENCY: ReadingSpec = ReadingSpec {
    id: "http_latency_p95_ms",
    title: "网关请求 p95",
    group: ReadingGroup::Infra,
    query: "histogram_quantile(0.95, sum by (le) (rate(http_request_duration_ms_bucket[5m]))) or on() sum(rate(http_requests_total[5m]))",
    unit: "毫秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "http_request_duration_ms / http_requests_total",
    produced_by: "crates/cog-gateway/src/lib.rs",
};

/// Exchanges and the latency the model calls inside them took.
///
/// The LLM account's own volume next to its own latency: an exchange count that
/// climbs while latency does too is the pool serving more work per second than
/// it can answer, which is the state right before the gate starts deferring.
pub const LLM_EXCHANGE_VOLUME: ReadingSpec = ReadingSpec {
    id: "llm_exchange_volume",
    title: "LLM 交换量与调用延迟",
    group: ReadingGroup::Delivery,
    query: "max(llm_call_latency_ms) or on() max(gateway_llm_exchange_rows) or on() max(gateway_llm_exchange_bytes)",
    unit: "毫秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "llm_call_latency_ms / gateway_llm_exchange_rows / gateway_llm_exchange_bytes",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// Tasks the SOP dispatcher injected, and roles it could not map.
///
/// The dispatcher's own account: an unmapped role is a task that was looked at
/// and not handed to anybody, and it is invisible from the injection count
/// alone because the loop keeps running either way.
pub const SOP_DISPATCH: ReadingSpec = ReadingSpec {
    id: "sop_unmapped_roles",
    title: "SOP 派发时对不上的角色",
    group: ReadingGroup::Delivery,
    query: "sum(increase(sop_unmapped_role_total[24h])) or on() sum(increase(sop_dispatch_total[24h])) or on() sum(increase(sop_tasks_injected_total[24h]))",
    unit: "次",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "sop_unmapped_role_total / sop_dispatch_total / sop_tasks_injected_total",
    produced_by: "crates/cog-orchestrator/src/sop/dispatch.rs",
};

/// How many times in a row the pool's probes for an upstream have failed.
///
/// The backoff is computed from this number: a count that keeps climbing while
/// `llm_upstreams_healthy` reads one says the last healthy upstream is failing
/// probes and the pool is holding it up with retries rather than with a working
/// upstream. Read as the maximum over upstreams -- the worst one is the one that
/// decides when the pool reports itself unavailable.
pub const LLM_UPSTREAM_FAILURE_RUN: ReadingSpec = ReadingSpec {
    id: "llm_upstream_failure_run",
    title: "上游连续失败次数",
    group: ReadingGroup::Delivery,
    query: "max(llm_upstream_consecutive_failures)",
    unit: "次",
    warn: Some(3.0),
    crit: None,
    higher_is_worse: true,
    series: "llm_upstream_consecutive_failures / llm_upstream_position / llm_upstream_quota_window_secs / llm_upstream_shape_errored / llm_pool_quota_window_secs / llm_pool_reserve_since_unix / llm_pool_evidenced_recovery_unix / llm_request_param_clamped_total",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// Resource events waiting for the loop that carries them.
///
/// A depth that keeps rising is the fan-out loop falling behind the build gate;
/// the events are still there, so nothing is lost yet, but every reading that
/// depends on them is describing a moment that has already passed.
pub const RESOURCE_EVENTS_BACKLOG: ReadingSpec = ReadingSpec {
    id: "resource_events_pending",
    title: "等待转发的资源事件",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_resource_events_pending) or on() max(cogneva_resource_events)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_resource_events_pending / cogneva_resource_events / cogneva_resource_events_dropped_total",
    produced_by: "crates/cog-orchestrator/src/resource_ledger.rs",
};

/// Raw sources parked in the dead-letter store.
///
/// A dead letter is a source that failed for its own content: re-driving it
/// would produce the same failure, so it sits there until a person decides.
/// The backlog above is work the loop still owes; this one is work it has
/// given up on, and the two must not be added together.
pub const MEMORY_DEAD_LETTERS: ReadingSpec = ReadingSpec {
    id: "memory_dead_letters",
    title: "记忆死信",
    group: ReadingGroup::Delivery,
    query: "max(memory_dead_letter_raw) or on() max(memory_unextracted_raw) or on() max(cogneva_memory_dropped_entries_total)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "memory_dead_letter_raw / memory_unextracted_raw",
    produced_by: "crates/cog-memory/src/plugin.rs",
};

/// How close the judge Job came to its memory limit.
///
/// The size law sets the limit from the peak it measured; a ratio that reaches
/// one means the next run was killed by the kernel before it could report, and
/// the reading that would have raised the limit never arrives. Read beside the
/// throttling ratio: throttled is slow, near-the-limit is about to be killed.
pub const ROLLOUT_JOB_NEAR_LIMIT: ReadingSpec = ReadingSpec {
    id: "rollout_job_memory_ratio",
    title: "判据 Job 内存接近上限的比例",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_rollout_job_memory_peak_ratio) or on() max(cogneva_rollout_job_reading_unix)",
    unit: "0-1",
    warn: Some(0.85),
    crit: Some(1.0),
    higher_is_worse: true,
    series: "cogneva_rollout_job_memory_peak_ratio / cogneva_rollout_job_reading_unix / cogneva_rollout_job_cpu_throttled_ratio",
    produced_by: "crates/cog-reflection/src/mainline_rollout.rs",
};

/// Tool fetches that ran out of budget before they finished.
///
/// Different from a truncation: a truncation hands the model what it could read,
/// this hands it a failure. A rate that climbs means the budget is sizing the
/// wrong thing -- a fetch that needs two more seconds or a few more kilobytes --
/// and the agent is working around missing evidence instead of reading it.
pub const TOOL_FETCH_BUDGET_EXHAUSTED: ReadingSpec = ReadingSpec {
    id: "tool_fetch_budget_exhausted",
    title: "取数预算用尽的工具调用",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_tool_output_fetch_budget_exhausted_total[30m]))",
    unit: "次",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "cogneva_tool_output_fetch_budget_exhausted_total / cogneva_tool_output_truncated_chars_total",
    produced_by: "crates/cog-agent/src/archive.rs",
};

/// Changes dispatched by the built-in recipe instead of a declared one.
///
/// The landing recipe can come from the project's own configuration or from
/// the built-in fallback, and the two dispatch identically. What separates them
/// is this label: a project that never declared a kind and one that declared
/// the built-in look the same in every other number, and only the second is a
/// declaration doing its job.
pub const PROJECT_KIND_DECLARED: ReadingSpec = ReadingSpec {
    id: "project_kind_declared_share",
    title: "走声明 recipe 的变更",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_project_kind_total{source=\"declared\"}[24h]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_project_kind_total",
    produced_by: "crates/cog-reflection/src/change_execution.rs",
};

/// Tasks held back because their project's board column was at its limit.
///
/// The only number that separates "the column limit is doing its job" from "the
/// publisher had nothing to do": both leave the same trace, no dispatch. Read
/// per project because a single project's column being full is a stall for that
/// goal and is invisible in a fleet-wide total.
pub const BOARD_COLUMN_HELD: ReadingSpec = ReadingSpec {
    id: "board_column_held",
    title: "被看板列上限挡住的任务",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_board_column_held_total[24h]))",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_board_column_held_total",
    produced_by: "crates/cog-orchestrator/src/dag_executor/mod.rs",
};

/// How each run of the eval gate ended, at both sites.
///
/// The gate blocks a change whose eval suite regressed; a run that ends in the
/// same outcome every time says the gate is not discriminating, and a site that
/// never reports says the gate is not running there at all. Both look like "no
/// regressions" from any other number.
pub const EVAL_GATE_OUTCOMES: ReadingSpec = ReadingSpec {
    id: "eval_gate_outcomes",
    title: "eval 门的判定结果",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_eval_gate_outcomes_total[24h]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_eval_gate_outcomes_total",
    produced_by: "crates/cog-eval/src/plugin.rs",
};

/// Storage tier migrations the background pass has run.
///
/// A migration pass that stops shows nothing: the rows it should have moved sit
/// where they were, every read still answers, and only the absence of this
/// counter's growth says the tiering stopped doing anything.
pub const TIER_MIGRATIONS: ReadingSpec = ReadingSpec {
    id: "tier_migrations",
    title: "存储分层迁移的次数",
    group: ReadingGroup::Infra,
    query: "sum(increase(tier_migration_total[24h]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "tier_migration_total",
    produced_by: "crates/cog-storage/src/plugin.rs",
};

/// Where the pool currently sits in its rotation.
///
/// Position is how far down the candidate list the pool has walked; a position
/// that stays high while health is fine says the earlier upstreams are being
/// skipped for a reason nobody has written down.
pub const LLM_UPSTREAM_POSITION: ReadingSpec = ReadingSpec {
    id: "llm_upstream_position",
    title: "上游在轮换里的位置",
    group: ReadingGroup::Delivery,
    query: "max(llm_upstream_position)",
    unit: "位",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "llm_upstream_position",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// How long the quota window is, as the pace measured it.
///
/// The window is what every token bound is a fraction of: a window that shrinks
/// makes the same spend look like an overspend, and a window nobody measured
/// reads as zero, which is the state that turns "nobody paced" into "the budget
/// is spent". Both the pool's and the upstream's own declaration are read here
/// because a pool that reports one and an upstream that reports another is a
/// disagreement the operator has to settle.
pub const LLM_QUOTA_WINDOW: ReadingSpec = ReadingSpec {
    id: "llm_quota_window_seconds",
    title: "配额窗口长度",
    group: ReadingGroup::Delivery,
    query: "max(llm_pool_quota_window_secs or llm_upstream_quota_window_secs)",
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "llm_pool_quota_window_secs / llm_upstream_quota_window_secs",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// When an upstream said it would be back, as an age.
///
/// An upstream that reports its own recovery instant is telling us something no
/// probe can: the wait is not ours to guess. An age that runs past it means the
/// upstream said it was back and the pool has not tried again.
pub const LLM_EVIDENCED_RECOVERY: ReadingSpec = ReadingSpec {
    id: "llm_evidenced_recovery_age_seconds",
    title: "上游自报的恢复时刻（距今）",
    group: ReadingGroup::Delivery,
    query: "time() - max(llm_pool_evidenced_recovery_unix)",
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "llm_pool_evidenced_recovery_unix",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// How long the newest reserve entry has been sitting there.
///
/// A reserve that was filled once and never again looks the same as a full one
/// from the population alone; the age says which. Beyond a window it is a
/// reserve nobody is refreshing, which is a pool that cannot absorb a failure.
pub const LLM_RESERVE_AGE: ReadingSpec = ReadingSpec {
    id: "llm_reserve_age_seconds",
    title: "储备上游存在的时长",
    group: ReadingGroup::Delivery,
    query: "time() - max(llm_pool_reserve_since_unix)",
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "llm_pool_reserve_since_unix",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// Upstream answers whose shape the pool could not read, and parameters it had
/// to clamp before sending.
///
/// Both are the pool meeting a contract it does not fully match: a shape error
/// is an upstream answering something the parser did not expect, a clamp is our
/// own request corrected before it left. Either rate climbing is the pool and
/// an upstream disagreeing, which no health count shows because the call
/// eventually answers.
pub const LLM_CONTRACT_MISMATCH: ReadingSpec = ReadingSpec {
    id: "llm_contract_mismatch",
    title: "上游响应形态错误与参数夹紧",
    group: ReadingGroup::Delivery,
    query: "sum(increase(llm_upstream_shape_errored[24h])) + sum(increase(llm_request_param_clamped_total[24h]))",
    unit: "次",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "llm_upstream_shape_errored / llm_request_param_clamped_total",
    produced_by: "crates/cog-gateway/src/security_gateway.rs",
};

/// Registry reclaim, counted rather than weighed.
///
/// The byte reading says how much was freed; this says how many objects went.
/// A pass that frees kilobytes from one huge image and a pass that frees the
/// same kilobytes from two hundred small ones are different problems, and the
/// counters are also where a prune that stopped entirely shows up first.
pub const REGISTRY_PRUNE_COUNTS: ReadingSpec = ReadingSpec {
    id: "registry_prune_counts",
    title: "镜像仓库回收的对象数",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_buildah_store_pruned_images_total[24h])) + sum(increase(cogneva_buildah_store_pruned_layers_total[24h])) + sum(increase(cogneva_registry_pruned_tags_total[24h])) + max(cogneva_buildah_store_kept_images) + sum(increase(cogneva_registry_rebuild_hold_secs_total[24h]))",
    unit: "个",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_buildah_store_pruned_images_total / cogneva_buildah_store_pruned_layers_total / cogneva_buildah_store_kept_images / cogneva_registry_pruned_tags_total / cogneva_registry_rebuild_hold_secs_total",
    produced_by: "crates/cog-reflection/src/mainline_rollout.rs",
};

/// How deep the change queue is stuck, and for what reason.
///
/// The ledger's own account of a jam: a block census by cause, rows waiting on
/// the model pool, remeasures owed, and the windows the writer had to wait for.
/// Read together because "the queue is long" is not actionable and "the queue is
/// long because these forty rows are blocked on the pool" is.
pub const CHANGE_QUEUE_JAM: ReadingSpec = ReadingSpec {
    id: "change_queue_jam",
    title: "变更队列卡住的行与原因",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_change_dispatch_pool_blocked_rows) or on() max(cogneva_change_block_census) or on() max(cogneva_change_ledger_remeasure_pending) or on() max(cogneva_change_write_windows)",
    unit: "行",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_change_dispatch_pool_blocked_rows / cogneva_change_block_census / cogneva_change_ledger_remeasure_pending / cogneva_change_write_windows",
    produced_by: "crates/cog-orchestrator/src/change_ledger.rs",
};

/// Ready work the dispatcher deferred because a backlog was already in flight.
///
/// A deferral is not a failure; a deferral rate that keeps climbing is the lane
/// running behind by design and never catching up, which every "in flight" count
/// reads as healthy activity.
pub const DISPATCH_DEFERRED: ReadingSpec = ReadingSpec {
    id: "dispatch_deferred",
    title: "因积压被推迟的派发",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_dag_stalled_scheduled_deferred_for_backlog_total[24h]))",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_dag_stalled_scheduled_deferred_for_backlog_total",
    produced_by: "crates/cog-orchestrator/src/plugin.rs",
};

/// Promotion candidates the week's window produced.
///
/// The promotion loop's own count: a week with none is a loop that either had
/// nothing to promote or stopped looking, and the difference is whether the
/// count is zero because the tree is quiet or because the pass did not run.
pub const PROMOTION_CANDIDATES: ReadingSpec = ReadingSpec {
    id: "promotion_candidates",
    title: "本周的晋升候选",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_promotion_week_changes)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_promotion_week_changes",
    produced_by: "crates/cog-reflection/src/promotion_trend.rs",
};

/// Tasks held at ready-admission because the lane was full.
///
/// The admission gate's own count: work that passed every check and then waited
/// for capacity. It is the number that separates "the queue is empty because
/// there is nothing to do" from "the queue is empty because nothing could get
/// in".
pub const READY_ADMISSION_HELD: ReadingSpec = ReadingSpec {
    id: "ready_admission_held",
    title: "在准入处被挡住的就绪任务",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_ready_admission_held_total[24h]))",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_ready_admission_held_total",
    produced_by: "crates/cog-orchestrator/src/plugin.rs",
};

/// Alert channels the deployment configured.
///
/// Zero channels is a deployment where every alert fires into nothing: the
/// rules evaluate, the states change, and no message leaves the process. That
/// is indistinguishable from "nothing is wrong" on every alert board.
pub const ALERT_CHANNELS: ReadingSpec = ReadingSpec {
    id: "alert_channels_configured",
    title: "配置的告警通道数",
    group: ReadingGroup::Infra,
    query: "max(cogneva_alert_channels_configured)",
    unit: "条",
    warn: Some(1.0),
    crit: Some(0.0),
    higher_is_worse: false,
    series: "cogneva_alert_channels_configured",
    produced_by: "crates/cog-observability/src/plugin.rs",
};

/// Observability events the store refused to persist.
///
/// The event board reads this store, so a write failure is a hole in the
/// history that the board draws as a quiet period rather than as a gap.
pub const OBSERVABILITY_EVENT_WRITE_FAILURES: ReadingSpec = ReadingSpec {
    id: "observability_event_write_failures",
    title: "观测事件写失败的次数",
    group: ReadingGroup::Infra,
    query: "sum(increase(cogneva_observability_event_persist_failed_total[30m]))",
    unit: "次",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "cogneva_observability_event_persist_failed_total",
    produced_by: "crates/cog-supervisor/src/consumer_lag.rs",
};

/// Rows the principle ledger holds, by check kind.
///
/// The register's own census: a kind that disappears is a check whose rows were
/// retired, which is a decision the tree should be able to show rather than a
/// silent shrink.
pub const PRINCIPLE_LEDGER_ROWS: ReadingSpec = ReadingSpec {
    id: "principle_ledger_rows",
    title: "原则账的登记行数",
    group: ReadingGroup::Infra,
    query: "sum(cogneva_principle_ledger_check_kind_rows)",
    unit: "行",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_principle_ledger_check_kind_rows",
    produced_by: "crates/cog-orchestrator/src/principle_ledger.rs",
};

/// What the size law set the CPU request to for the busiest workload.
///
/// The law computes a request per container from the measured p95; this is the
/// largest one it wrote. A number that jumps is a workload whose usage changed
/// shape, and a number that sits still while the workload's usage climbs is the
/// law not running -- both are invisible from the objects themselves.
pub const WORKLOAD_CPU_DERIVED: ReadingSpec = ReadingSpec {
    id: "workload_cpu_request_millicores",
    title: "尺寸律定下的 CPU 请求（最大）",
    group: ReadingGroup::Infra,
    query: "max(cogneva_workload_request_cpu_millicores) or on() max(cogneva_workload_limit_cpu_millicores) or on() max(cogneva_workload_replicas)",
    unit: "毫核",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_workload_request_cpu_millicores / cogneva_workload_limit_cpu_millicores / cogneva_workload_replicas",
    produced_by: "crates/cog-reflection/src/workload_resources.rs",
};

/// What the size law set the memory request to for the busiest workload.
///
/// Memory is the half that kills rather than throttles: a request below what a
/// workload actually uses is an eviction waiting for a busy minute, and the
/// derivation is the only thing that can see it coming.
pub const WORKLOAD_MEMORY_DERIVED: ReadingSpec = ReadingSpec {
    id: "workload_memory_request_bytes",
    title: "尺寸律定下的内存请求（最大）",
    group: ReadingGroup::Infra,
    query: "max(cogneva_workload_request_memory_bytes) or on() max(cogneva_workload_limit_memory_bytes) or on() max(cogneva_workload_resources_evaluations_total) or on() max(cogneva_workload_replicas_evaluations_total)",
    unit: "字节",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_workload_request_memory_bytes / cogneva_workload_limit_memory_bytes / cogneva_workload_resources_evaluations_total / cogneva_workload_replicas_evaluations_total",
    produced_by: "crates/cog-reflection/src/workload_resources.rs",
};

/// The default resources the namespace's LimitRange hands to containers that
/// declare none.
///
/// B1 removed every pinned resource, so this default is what runs the workloads
/// that declare nothing -- a hook Job, an init container. A default that is too
/// small shows up as a Job killed mid-run, and this is the only place the number
/// it was given is visible.
pub const LIMIT_RANGE_DEFAULTS: ReadingSpec = ReadingSpec {
    id: "limit_range_default_memory_bytes",
    title: "LimitRange 默认内存",
    group: ReadingGroup::Infra,
    query: "max(cogneva_limit_range_default_memory_bytes) or on() max(cogneva_limit_range_default_request_memory_bytes) or on() max(cogneva_limit_range_default_cpu_millicores) or on() max(cogneva_limit_range_default_request_cpu_millicores) or on() max(cogneva_limit_range_defaults_evaluations_total)",
    unit: "字节",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_limit_range_default_memory_bytes / cogneva_limit_range_default_request_memory_bytes / cogneva_limit_range_default_cpu_millicores / cogneva_limit_range_default_request_cpu_millicores / cogneva_limit_range_defaults_evaluations_total",
    produced_by: "crates/cog-reflection/src/workload_resources.rs",
};

/// The wait the pacing law computed for the build-cache scan.
///
/// The declared interval is the floor; a target that sits at the floor means the
/// scan is finishing faster than the floor allows and the law is doing nothing.
/// A target that climbs past it is a scan getting slower -- which is the reading
/// that justifies the cadence moving.
pub const PACING_TARGET: ReadingSpec = ReadingSpec {
    id: "pacing_target_seconds",
    title: "节拍律定下的扫描间隔",
    group: ReadingGroup::Infra,
    query: "max(cogneva_pacing_target_seconds) or on() max(cogneva_pacing_evaluations_total)",
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_pacing_target_seconds / cogneva_pacing_evaluations_total",
    produced_by: "crates/cog-core/src/pacing.rs",
};

/// The retention target the loop computed from the measured write rate.
///
/// The rows a table is allowed to keep is derived from how fast it is written
/// and how many bytes a row costs; a target that keeps climbing while writes
/// stay flat means rows got bigger, and a target that stops moving means the
/// measurement stopped.
pub const RETENTION_TARGET_ROWS: ReadingSpec = ReadingSpec {
    id: "retention_target_rows",
    title: "保留目标行数",
    group: ReadingGroup::Infra,
    query: "max(cogneva_retention_target) or on() max(cogneva_retention_evaluations_total)",
    unit: "行",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_retention_target / cogneva_retention_evaluations_total",
    produced_by: "crates/cog-core/src/retention.rs",
};

/// Obeservables pinned by more than one plugin, or read before init finished.
///
/// These are the startup audit's findings: a handle published twice means two
/// plugins believe they own it, a handle nobody consumed is a service built for
/// no reader, and an unordered read is a plugin reading a pin another plugin has
/// not published yet. All three are silent at runtime and fatal in the right
/// order of events.
pub const PLUGIN_PIN_AUDIT: ReadingSpec = ReadingSpec {
    id: "plugin_pin_audit",
    title: "启动期发布审计",
    group: ReadingGroup::Infra,
    query: "max(cogneva_plugin_pin_multi_published) + max(cogneva_plugin_pin_unconsumed) + max(cogneva_plugin_pin_unordered_init_read) + max(cogneva_plugin_pin_unsatisfied) + max(cogneva_plugin_publishes)",
    unit: "个",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: "cogneva_plugin_pin_multi_published / cogneva_plugin_pin_multi_published_count / cogneva_plugin_pin_unconsumed / cogneva_plugin_pin_unconsumed_count / cogneva_plugin_pin_unordered_init_read / cogneva_plugin_pin_unordered_init_read_count / cogneva_plugin_pin_unsatisfied / cogneva_plugin_pin_unsatisfied_count / cogneva_plugin_publishes / cogneva_plugin_publishes_count",
    produced_by: "crates/cogneva/src/pin_census.rs",
};

/// The outward loop's passes, by source.
///
/// An outward pass is the platform telling another system what changed. A source
/// that stops producing passes is a system that silently stopped being told --
/// and unlike a failed call, it leaves no error anywhere.
pub const OUTWARD_PASSES: ReadingSpec = ReadingSpec {
    id: "outward_passes",
    title: "外发回路的轮次",
    group: ReadingGroup::Delivery,
    query: "sum(increase(cogneva_outward_passes_total[24h])) or on() sum(increase(cogneva_outward_items_total[24h])) or on() max(cogneva_outward_sources_configured) or on() max(cogneva_outward_config)",
    unit: "次",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_outward_passes_total / cogneva_outward_items_total / cogneva_outward_sources_configured / cogneva_outward_config",
    produced_by: "crates/cog-orchestrator/src/outward.rs",
};

/// How many builds are queued for a slot right now.
///
/// Every other number on the build gate describes what it did with the slots it
/// handed out -- waits, holds, refusals -- and all of them read healthy while
/// the queue in front of the gate grows. A queued build is the host saying the
/// bound is the constraint at this instant, so zero is the only quiet value.
pub const BUILD_QUEUE_DEPTH: ReadingSpec = ReadingSpec {
    id: "build_queue_depth",
    title: "排在建槽外的构建",
    group: ReadingGroup::Delivery,
    query: "sum(cogneva_build_gate_waiting)",
    unit: "条",
    warn: Some(0.0),
    crit: None,
    higher_is_worse: true,
    series: cog_core::contract::build_gate::BUILD_GATE_WAITING,
    produced_by: "crates/cog-core/src/contract/build_gate.rs",
};

/// The longest a single build has waited for a slot, in this process's life.
///
/// The queue's depth says the bound is tight now; this says how tight it has
/// been. No threshold: the number it should be judged against is the gate's own
/// wait budget, and a bound written here would be a second copy of that.
pub const BUILD_GATE_WAIT_MAX: ReadingSpec = ReadingSpec {
    id: "build_gate_wait_max_ms",
    title: "最长的一次等槽",
    group: ReadingGroup::Delivery,
    query: "max(cogneva_build_gate_wait_ms_max)",
    unit: "毫秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: cog_core::contract::build_gate::BUILD_GATE_WAIT_MS_MAX,
    produced_by: "crates/cog-core/src/contract/build_gate.rs",
};

/// 库自己带的那三条时间界里最紧的一条：任何一条开了，就有一条活会在半路被杀。
///
/// 这三个数是部署改出来的，不是这份仓库写下的常量，所以扫常量的门看不见它们；
/// 一条被库杀在半路的语句，与一条被代码里的界杀在半路的语句，损失是同一件事。
/// 取三者最大值：读的是「这里有没有一条界会动手」，不是「哪一条最常触发」。
pub const DB_TIMEOUT_BOUND_SECONDS: ReadingSpec = ReadingSpec {
    id: "db_timeout_bound_seconds",
    title: "数据库自带的时间界（三条里最紧的一条）",
    group: ReadingGroup::Infra,
    query: r#"max(cogneva_db_setting_seconds)"#,
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_db_setting_seconds",
    produced_by: "crates/cog-storage/src/db_surface.rs",
};

/// 空闲事务最老的那一条有多老。
///
/// 库会在 `idle_in_transaction_session_timeout` 到点时掐掉它，所以这条年龄就是
/// 「那条界已经花掉多少」。与 `pg_stat_activity_max_tx_duration` 不同：那个是全部
/// 在飞事务的最大值，分不清是有一条语句在跑，还是一段事务开了口不再说话——
/// 后者会一直按住快照，让 vacuum 追不上。该状态此刻没有事务时这一格缺席，不是 0。
pub const DB_IDLE_TRANSACTION_AGE: ReadingSpec = ReadingSpec {
    id: "db_idle_transaction_age_seconds",
    title: "最老的空闲事务年龄",
    group: ReadingGroup::Infra,
    query: r#"max(cogneva_db_xact_age_seconds{state="idle in transaction"})"#,
    unit: "秒",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_db_xact_age_seconds",
    produced_by: "crates/cog-storage/src/db_surface.rs",
};

/// 每张表离自己的 autovacuum 触发线有多远：死元组 ÷ 触发线，取最大的一张。
///
/// 比值在读到的地方算，不发布成一个数：阈值为 0 的表（触发关掉）会除出无界，
/// 那正是「已有死行就早已越过」的读法；先除好再发布会把这一情形编成一个比值。
/// 1.0 不是人拍的线：它就是 autovacuum 自己的触发线，越过即「本该来而没来」。
pub const DB_VACUUM_LAG_RATIO: ReadingSpec = ReadingSpec {
    id: "db_vacuum_lag_max_ratio",
    title: "离 autovacuum 触发线最远的那张表（比值）",
    group: ReadingGroup::Infra,
    query: r#"max(cogneva_db_dead_tuples / cogneva_db_vacuum_threshold_tuples)"#,
    unit: "倍",
    // 1.0 是库自己的触发线，不是这里挑的：越过它的那一刻就是 vacuum 本该来过而没来。
    // 再往上没有第二条由库给定的线，所以不给 crit——那会是一个拍出来的数。
    warn: Some(1.0),
    crit: None,
    higher_is_worse: true,
    series: "cogneva_db_dead_tuples",
    produced_by: "crates/cog-storage/src/db_surface.rs",
};

/// autovacuum 已经追不上的表有多少张。
///
/// 越过触发线的那一刻，就是 vacuum 本该来过而没来的那一刻；死元组是「删了却还占着」
/// 的空间，它让备份变慢、让规划器把表读得比活数据大。
pub const DB_VACUUM_BEHIND_TABLES: ReadingSpec = ReadingSpec {
    id: "db_vacuum_behind_tables",
    title: "autovacuum 追不上的表",
    group: ReadingGroup::Infra,
    query: r#"max(cogneva_db_vacuum_behind_tables)"#,
    unit: "张",
    warn: Some(1.0),
    crit: Some(5.0),
    higher_is_worse: true,
    series: "cogneva_db_vacuum_behind_tables",
    produced_by: "crates/cog-storage/src/db_surface.rs",
};

/// How many rows the event stream took in over the last day, all kinds together.
///
/// The per-kind breakdown is the census; this row answers the one question that
/// has no threshold by itself: is the stream moving at all. A stream that
/// stopped and a stream nothing writes to look the same on every other row, and
/// they call for opposite work, so the volume is drawn beside the oldest row's
/// age rather than judged against a number somebody would have to pick.
pub const EVENT_STREAM_VOLUME: ReadingSpec = ReadingSpec {
    id: "event_stream_rows_per_day",
    title: "事件流每天的写入行数",
    group: ReadingGroup::Delivery,
    query: "sum(cogneva_event_stream_rows_per_day) or on() vector(0)",
    unit: "行/天",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "cogneva_event_stream_rows_per_day",
    produced_by: "crates/cog-storage/src/event_stream_census.rs",
};

/// Payloads a reader met that it could not decode, counted at the writer.
///
/// The writer refuses a version it does not know, so this stays at zero by a
/// gate and not by a rule; the row is here because "the gate is stricter now"
/// and "an older writer is still publishing" both move it, and the second one
/// is invisible everywhere else.
pub const EVENT_UNDECODABLE: ReadingSpec = ReadingSpec {
    id: "event_undecodable_total",
    title: "读不出来的事件载荷",
    group: ReadingGroup::Delivery,
    query: "sum(cogneva_event_undecodable_total) or on() vector(0)",
    unit: "条",
    warn: None,
    crit: None,
    higher_is_worse: true,
    series: "cogneva_event_undecodable_total",
    produced_by: "crates/cog-storage/src/postgres/change_candidate_store.rs",
};

/// How many LLM exchange rows the gateway took in over the last day.
///
/// The retention loop derives the keep window from this rate; the row is what a
/// person reads when the window and the volume disagree. The rate is published
/// only once two readings have been compared, so an absent series means "not
/// measured yet", not "no growth" — which is why this is a reading and not a
/// zero.
pub const LLM_EXCHANGE_RATE: ReadingSpec = ReadingSpec {
    id: "llm_exchange_rows_per_day",
    title: "LLM 交换表每天的新增行数",
    group: ReadingGroup::Delivery,
    query: "sum(gateway_llm_exchange_rows_per_day) or on() vector(0)",
    unit: "行/天",
    warn: None,
    crit: None,
    higher_is_worse: false,
    series: "gateway_llm_exchange_rows_per_day",
    produced_by: "crates/cog-observability/src/exchange_retention.rs",
};

pub const READINGS: [&ReadingSpec; 70] = [
    &DISK_FREE,
    &EVENT_STREAM_VOLUME,
    &EVENT_UNDECODABLE,
    &LLM_EXCHANGE_RATE,
    &PG_CONNECTIONS,
    &DB_TIMEOUT_BOUND_SECONDS,
    &DB_VACUUM_BEHIND_TABLES,
    &DB_IDLE_TRANSACTION_AGE,
    &DB_VACUUM_LAG_RATIO,
    &PG_OLDEST_TX,
    &QDRANT_VECTORS,
    &INGRESS_5XX,
    &UNREACHABLE,
    &MAINLINE_CI_HOLD,
    &MAINLINE_CI_HOLD_UNOWNED,
    &MAINLINE_CI_HOLD_ACTIVE,
    &CHANGE_POOL_BLOCKED_OLDEST,
    &LLM_UPSTREAMS_HEALTHY,
    &BUILD_CACHE_OVER_CAP,
    &DISCOVERY_PROBES_UNANSWERED,
    &LLM_POOL_BACKOFF,
    &LLM_POOL_RESERVE,
    &SHUTDOWN_DRAIN_LOSS,
    &REGISTRY_RECLAIM,
    &TOOL_CALL_RECORDS_ARCHIVED,
    &TOOL_OUTPUT_TRUNCATED,
    &STORE_GROWTH,
    &MAINLINE_SETBACKS,
    &RESOURCE_EVENTS_DROPPED,
    &ROLLOUT_JOB_THROTTLED,
    &MEMORY_BACKLOG,
    &SHUTDOWN_DRAIN_DURATION,
    &EVENT_BUS_HANDOFFS,
    &TASK_EVENT_STORE,
    &TASK_EVENT_ARCHIVE_AGE,
    &VERSION_LAG,
    &HTTP_LATENCY,
    &LLM_EXCHANGE_VOLUME,
    &SOP_DISPATCH,
    &LLM_UPSTREAM_FAILURE_RUN,
    &RESOURCE_EVENTS_BACKLOG,
    &MEMORY_DEAD_LETTERS,
    &ROLLOUT_JOB_NEAR_LIMIT,
    &TOOL_FETCH_BUDGET_EXHAUSTED,
    &PROJECT_KIND_DECLARED,
    &BOARD_COLUMN_HELD,
    &EVAL_GATE_OUTCOMES,
    &TIER_MIGRATIONS,
    &LLM_UPSTREAM_POSITION,
    &LLM_QUOTA_WINDOW,
    &LLM_EVIDENCED_RECOVERY,
    &LLM_RESERVE_AGE,
    &LLM_CONTRACT_MISMATCH,
    &REGISTRY_PRUNE_COUNTS,
    &CHANGE_QUEUE_JAM,
    &DISPATCH_DEFERRED,
    &PROMOTION_CANDIDATES,
    &READY_ADMISSION_HELD,
    &ALERT_CHANNELS,
    &OBSERVABILITY_EVENT_WRITE_FAILURES,
    &PRINCIPLE_LEDGER_ROWS,
    &WORKLOAD_CPU_DERIVED,
    &WORKLOAD_MEMORY_DERIVED,
    &LIMIT_RANGE_DEFAULTS,
    &PACING_TARGET,
    &RETENTION_TARGET_ROWS,
    &PLUGIN_PIN_AUDIT,
    &OUTWARD_PASSES,
    &BUILD_QUEUE_DEPTH,
    &BUILD_GATE_WAIT_MAX,
];

/// The word a reading carries about its own value.
///
/// The board colours a row by this word and prints a Chinese name beside it
/// through a map keyed by the same word. Both of those fall back when the word
/// is not one they know — the neutral colour, the word itself — so a verdict
/// spelled somewhere else, or added here without telling the board, would show
/// up as a value nobody acts on rather than as a disagreement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadingVerdict {
    /// Within the warning threshold, or below every threshold this reading has.
    Ok,
    /// Past `warn`, not yet past `crit`.
    Warning,
    /// Past `crit`.
    Critical,
    /// No value: no series, no answer, no threshold to judge by. Kept apart
    /// from `Ok` on purpose — one is a quiet system, the other is a blind spot.
    Unknown,
}

impl ReadingVerdict {
    /// Every word this vocabulary has, for readers that must cover all of them
    /// rather than listing the variants a second time.
    pub const ALL: [ReadingVerdict; 4] = [
        ReadingVerdict::Ok,
        ReadingVerdict::Warning,
        ReadingVerdict::Critical,
        ReadingVerdict::Unknown,
    ];

    /// The word this verdict travels as, and the key the board's maps use.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadingVerdict::Ok => "ok",
            ReadingVerdict::Warning => "warning",
            ReadingVerdict::Critical => "critical",
            ReadingVerdict::Unknown => "unknown",
        }
    }

    /// Read a word back. The wire is the only place these words are not typed.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|verdict| verdict.as_str().eq_ignore_ascii_case(word))
    }
}

/// Serialised through [`ReadingVerdict::as_str`] rather than a per-variant
/// `#[serde(rename)]`, which would be a second place the word is written.
impl serde::Serialize for ReadingVerdict {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Reading {
    #[serde(flatten)]
    pub spec: ReadingSpec,
    /// `None` is "no answer", which is not the same as zero. A missing series
    /// and a series reading zero lead to different actions — one is a broken
    /// exporter, the other is a quiet system.
    pub value: Option<f64>,
    pub verdict: ReadingVerdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_because: Option<String>,
    /// Where the follow-up question is answered: the board and the cell.
    ///
    /// Computed from the registry rather than written per reading. The board is
    /// the one the generator puts every reading on, and the cell is the
    /// reading's position in the registry — so a reading cannot point at a
    /// panel nobody draws, which is what a written uid allowed.
    pub panel: ReadingPanel,
}

/// The board and cell a reading's panel occupies.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadingPanel {
    pub uid: &'static str,
    /// Position in [`READINGS`], which is the order the board is built in.
    pub id: usize,
}

/// Judge one value against its spec.
pub fn verdict_of(spec: &ReadingSpec, value: Option<f64>) -> ReadingVerdict {
    let Some(value) = value else {
        return ReadingVerdict::Unknown;
    };
    let at_or_past = |threshold: Option<f64>| match threshold {
        Some(threshold) if spec.higher_is_worse => value >= threshold,
        Some(threshold) => value <= threshold,
        // 没有阈值的读数只能报值，不能报警：编一个数出来，它就会在自己选的时刻响。
        None => false,
    };
    if at_or_past(spec.crit) {
        ReadingVerdict::Critical
    } else if at_or_past(spec.warn) {
        ReadingVerdict::Warning
    } else {
        ReadingVerdict::Ok
    }
}

/// Pull the single scalar out of a Prometheus `/api/v1/query` answer.
///
/// An empty result and a malformed body both mean "no answer"; neither means
/// zero. Prometheus answers `200` with an empty `result` when nothing matches,
/// so status code is not a verdict either.
pub fn parse_scalar(body: &serde_json::Value) -> Option<f64> {
    let result = body.get("data")?.get("result")?.as_array()?;
    let first = result.first()?;
    let value = first.get("value")?.as_array()?;
    let raw = value.get(1)?.as_str()?;
    raw.parse::<f64>().ok()
}

/// Where Prometheus is, as the deployment declares it.
///
/// The chart fills this from the same values leaf that fills the config's
/// `infra_watch.prometheus_url`, because both ask the same Prometheus: the rule
/// evaluator and this board must not be the two readers of one fact that can
/// disagree.
///
/// Nothing declared is an error, not a default. A guessed in-cluster address is
/// the one kind of wrong answer this surface cannot show: a query against a
/// closed port and a query that matched no series both come back as "no
/// reading", so a guess that moved with the release reads exactly like a quiet
/// week.
pub fn prometheus_base() -> Result<String, String> {
    let declared = std::env::var(cog_core::env::PROMETHEUS_URL).ok();
    prometheus_base_from(declared.as_deref())
}

/// [`prometheus_base`] with the declaration handed in, so the rule about what
/// counts as declared can be read without an environment to set.
fn prometheus_base_from(declared: Option<&str>) -> Result<String, String> {
    declared
        .map(str::trim)
        .map(|s| s.trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "没有声明 Prometheus 地址（COGNEVA_PROMETHEUS_URL）：读数面不猜集群内的地址，\
             猜错与「这条序列没有样本」在读数面上同形"
                .to_string()
        })
}

/// The client these queries go through.
///
/// The process-wide one the assembly layer injected, not one built here: this
/// crate does not own the implementation — timeouts, pool and proxy have one
/// definition and it lives where the client is constructed. A second client
/// built inside a handler would carry its own numbers and be discovered only by
/// a query that hangs in one place and not another. When nothing was injected
/// the answer says so instead of quietly falling back, because a self-built
/// client reads exactly like the configured one from here.
fn query_client() -> Result<std::sync::Arc<dyn cog_core::HttpClient>, String> {
    crate::contribution_admin::http_client()
}

/// The deadline for one query. A reading that cannot answer in five seconds is
/// already the answer; a failed query is reported as "no reading", never as 0.
// cap: fixed — 一次查询的等待上限；超时报「没有读数」，绝不报 0
// keep: 删掉后一次慢查询把面板拖住，超时也不报「没有读数」；由「超时报没有读数，绝不报 0」兜住
// wall: 等=一次读数查询返回 / 等不到=这条读数报「没有读数」，**绝不报 0**——0 是一次真实读数，与「没量到」必须分得开 / 续跑=能，下一拍再查
const QUERY_TIMEOUT_SECS: u64 = 5;

/// Ask Prometheus for one query.
async fn ask(
    client: &std::sync::Arc<dyn cog_core::HttpClient>,
    base: &str,
    query: &str,
) -> Result<Option<f64>, String> {
    let url = format!("{base}/api/v1/query?query={}", percent_encode(query));
    let request = cog_core::HttpRequest::get(url).timeout(QUERY_TIMEOUT_SECS);
    let response = client
        .execute(request)
        .await
        .map_err(|e| format!("prometheus unreachable: {e}"))?;
    if !response.is_success() {
        return Err(format!("prometheus answered {}", response.status));
    }
    let body: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("prometheus response is not JSON: {e}"))?;
    Ok(parse_scalar(&body))
}

/// Percent-encode a PromQL expression for a query string.
///
/// PromQL is mostly punctuation — braces, quotes, `=~`, brackets — so a query
/// pasted into a URL unencoded arrives as something else entirely. Only the
/// unreserved set passes through; everything else becomes `%XX`.
fn percent_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() * 3);
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Every reading, with its verdict. Queries run concurrently: six serial round
/// trips at a 5s timeout each would let one slow series blank the whole board.
pub async fn observability_readings_handler() -> axum::response::Response {
    use axum::response::IntoResponse;
    use axum::Json;

    let base = match prometheus_base() {
        Ok(base) => base,
        Err(e) => {
            // 地址没声明与 Prometheus 不可达是两件事：一个要改清单，一个要查
            // 那台 Prometheus。混成一句「取不到读数」，运维会去查一台根本没被
            // 指到的机器。
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "prometheus_not_configured", "message": e})),
            )
                .into_response();
        }
    };
    let client = match query_client() {
        Ok(c) => c,
        Err(e) => {
            // 装配层没交进客户端：说出来，而不是临时造一个让「漏了装配」看起来像
            // 「装配好了」。
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "http_client_not_wired", "message": e})),
            )
                .into_response();
        }
    };

    let mut handles = Vec::with_capacity(READINGS.len());
    for spec in READINGS {
        let client = client.clone();
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            ask(&client, &base, spec.query).await
        }));
    }
    let mut by_id: HashMap<&'static str, Result<Option<f64>, String>> = HashMap::new();
    for (spec, handle) in READINGS.iter().zip(handles) {
        let outcome = handle
            .await
            .unwrap_or_else(|e| Err(format!("query task failed: {e}")));
        by_id.insert(spec.id, outcome);
    }

    let readings: Vec<Reading> = READINGS
        .iter()
        .enumerate()
        .map(|(cell, spec)| {
            let (value, why) = match by_id.remove(spec.id) {
                Some(Ok(Some(v))) => (Some(v), None),
                Some(Ok(None)) => (
                    None,
                    Some(format!(
                        "没有序列：`{}` 当前一条样本都没有（产出方：{}）",
                        spec.series, spec.produced_by
                    )),
                ),
                Some(Err(e)) => (None, Some(e)),
                None => (None, Some("查询没有回来".to_string())),
            };
            Reading {
                spec: (*spec).clone(),
                value,
                verdict: verdict_of(spec, value),
                unavailable_because: why,
                panel: ReadingPanel {
                    uid: crate::grafana_export::READINGS_BOARD_UID,
                    id: cell,
                },
            }
        })
        .collect();

    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({
            "prometheus": base,
            "readings": readings,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_undeclared_prometheus_is_an_error_not_an_address() {
        // 猜一个集群内地址的坏处不是「猜错」本身，是猜错后的症状与「这条序列
        // 没有样本」同形：面板上两格都写着没有读数，一种要改清单、一种要查
        // Prometheus。
        assert!(prometheus_base_from(None).is_err());
        assert!(prometheus_base_from(Some("")).is_err());
        assert!(prometheus_base_from(Some("   ")).is_err());
        // 声明了就用它，只做去空白的收尾（末尾斜杠会拼出 `//api/v1/query`）。
        assert_eq!(
            prometheus_base_from(Some(" http://prom.monitoring:9090/ ")).unwrap(),
            "http://prom.monitoring:9090"
        );
    }

    #[test]
    fn an_empty_prometheus_answer_is_not_zero() {
        // Prometheus 的 200 + 空 result 是"没有这条序列"，读成 0 会把一个坏掉的
        // exporter 显示成一切安静。
        assert_eq!(
            parse_scalar(&serde_json::json!({"data": {"result": []}})),
            None
        );
        assert_eq!(parse_scalar(&serde_json::json!({"data": {}})), None);
        assert_eq!(
            parse_scalar(&serde_json::json!({
                "data": {"result": [{"value": [1792936073, "0.930000"]}]}
            })),
            Some(0.93)
        );
    }

    #[test]
    fn a_reading_with_no_value_is_unknown_rather_than_ok() {
        assert_eq!(verdict_of(&DISK_FREE, None), ReadingVerdict::Unknown);
        // 0 是一个真实的读数：剩余 0% 是 critical，不是 unknown。
        assert_eq!(verdict_of(&DISK_FREE, Some(0.0)), ReadingVerdict::Critical);
    }

    #[test]
    fn thresholds_point_the_way_the_reading_gets_worse() {
        // 剩余量越低越坏。
        assert_eq!(verdict_of(&DISK_FREE, Some(0.5)), ReadingVerdict::Ok);
        assert_eq!(verdict_of(&DISK_FREE, Some(0.10)), ReadingVerdict::Warning);
        assert_eq!(verdict_of(&DISK_FREE, Some(0.05)), ReadingVerdict::Critical);
        // 连接数越高越坏——同一组阈值、相反的方向。
        assert_eq!(verdict_of(&PG_CONNECTIONS, Some(10.0)), ReadingVerdict::Ok);
        assert_eq!(
            verdict_of(&PG_CONNECTIONS, Some(90.0)),
            ReadingVerdict::Warning
        );
        assert_eq!(
            verdict_of(&PG_CONNECTIONS, Some(120.0)),
            ReadingVerdict::Critical
        );
    }

    /// 交付链那三条的判词方向：两个方向都在这一族里，写反了不会报错，只会安静地
    /// 把「停了三小时」读成正常、把「一切正常」读成告警。
    #[test]
    fn the_delivery_readings_point_both_ways() {
        // 扣得越久越坏：一小时才到警告，四小时才到严重——两个都是「有出处」的界，
        // 不是这一格自己编的。
        assert_eq!(verdict_of(&MAINLINE_CI_HOLD, Some(0.0)), ReadingVerdict::Ok);
        assert_eq!(
            verdict_of(&MAINLINE_CI_HOLD, Some(3599.0)),
            ReadingVerdict::Ok
        );
        assert_eq!(
            verdict_of(&MAINLINE_CI_HOLD, Some(3600.0)),
            ReadingVerdict::Warning
        );
        assert_eq!(
            verdict_of(&MAINLINE_CI_HOLD, Some(14400.0)),
            ReadingVerdict::Critical
        );
        // 被挡住的最老变更只有一个界：等不到半小时就只是等着。
        assert_eq!(
            verdict_of(&CHANGE_POOL_BLOCKED_OLDEST, Some(1799.0)),
            ReadingVerdict::Ok
        );
        assert_eq!(
            verdict_of(&CHANGE_POOL_BLOCKED_OLDEST, Some(1800.0)),
            ReadingVerdict::Critical
        );
        // 可用上游越少越坏：0 个是什么也调不到，1 个是没有任何冗余。
        assert_eq!(
            verdict_of(&LLM_UPSTREAMS_HEALTHY, Some(0.0)),
            ReadingVerdict::Critical
        );
        assert_eq!(
            verdict_of(&LLM_UPSTREAMS_HEALTHY, Some(1.0)),
            ReadingVerdict::Warning
        );
        assert_eq!(
            verdict_of(&LLM_UPSTREAMS_HEALTHY, Some(3.0)),
            ReadingVerdict::Ok
        );
        // 问不到不是 0：没有样本时三条都必须说「不知道」，而不是「没问题」。
        for spec in [
            &MAINLINE_CI_HOLD,
            &CHANGE_POOL_BLOCKED_OLDEST,
            &LLM_UPSTREAMS_HEALTHY,
        ] {
            assert_eq!(
                verdict_of(spec, None),
                ReadingVerdict::Unknown,
                "{}",
                spec.id
            );
        }
    }

    #[test]
    fn a_reading_without_a_threshold_reports_a_value_but_never_fires() {
        // 向量总数只有趋势意义：给它编一个上限，它就会在没人约定的时刻响。
        assert_eq!(
            verdict_of(&QDRANT_VECTORS, Some(9.9e12)),
            ReadingVerdict::Ok
        );
        assert_eq!(verdict_of(&QDRANT_VECTORS, None), ReadingVerdict::Unknown);
    }

    /// Every declared verdict travels as its own word, and no two share one.
    ///
    /// The board looks each word up in a colour map and a Chinese-name map; a
    /// duplicate spelling would make one of the four unreachable on screen
    /// while this side still counts it as covered.
    #[test]
    fn every_declared_verdict_reads_back_as_itself() {
        let mut words = std::collections::BTreeSet::new();
        for verdict in ReadingVerdict::ALL {
            assert!(
                words.insert(verdict.as_str()),
                "two variants share the word {:?}; the board's maps would only ever reach one",
                verdict.as_str()
            );
            assert_eq!(ReadingVerdict::parse(verdict.as_str()), Some(verdict));
            assert_eq!(
                serde_json::to_string(&verdict).unwrap(),
                format!("\"{}\"", verdict.as_str())
            );
        }
        assert_eq!(ReadingVerdict::parse("OK"), Some(ReadingVerdict::Ok));
        assert_eq!(ReadingVerdict::parse("fine"), None);
    }

    #[test]
    fn a_threshold_is_serialised_as_null_not_as_a_made_up_number() {
        // serde_json 把非有限的 f64 写成 null；接口里绝不能出现一个读的人以为
        // 有意义的阈值。
        let wire = serde_json::to_value(Reading {
            spec: QDRANT_VECTORS.clone(),
            value: None,
            verdict: ReadingVerdict::Unknown,
            unavailable_because: None,
            panel: ReadingPanel {
                uid: crate::grafana_export::READINGS_BOARD_UID,
                id: 0,
            },
        })
        .unwrap();
        assert!(wire["warn"].is_null());
        assert!(wire["crit"].is_null());
    }

    #[test]
    fn a_promql_expression_survives_being_put_in_a_url() {
        // 大括号、引号、`=~` 原样塞进查询串会被读成别的表达式；未保留字符以外的
        // 一律转义，运算符本身必须原样到达 Prometheus。
        assert_eq!(percent_encode("up"), "up");
        assert_eq!(
            percent_encode(r#"sum(x{a="b"})"#),
            "sum%28x%7Ba%3D%22b%22%7D%29"
        );
        assert_eq!(percent_encode(r#"a=~"5..""#), "a%3D~%225..%22");
        // 空格是 PromQL 的分隔符，绝不能变成 `+`。
        assert_eq!(percent_encode("a b"), "a%20b");
    }

    #[test]
    fn every_reading_names_a_producer_and_a_series() {
        // 一条读数说不出谁产出它，就等于没有人对它的消失负责。
        for spec in READINGS {
            assert!(!spec.series.is_empty(), "{} has no series", spec.id);
            assert!(!spec.produced_by.is_empty(), "{} has no producer", spec.id);
            assert!(!spec.query.is_empty(), "{} has no query", spec.id);
        }
    }
}

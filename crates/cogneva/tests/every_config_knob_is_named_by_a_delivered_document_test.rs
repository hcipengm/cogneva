//! principle: feedback_no_hardcoding_config_surface
//!
//! 「禁止硬编码」在配置面上有两个半场。`config_section_reader_contract_test`
//! 判的是第一个半场的一侧：文档写出来的每个键都得有字段接着，否则运维设了一个
//! 没人读的旋钮。另一半是反过来的——类型能读的每个旋钮，至少得有一份**交付**
//! 文档把它写出来。一个只活在 `Default` 里的字段，运维在交付文档里看不到它，
//! 也就没有发现它的路径；它实际上等价于写死在代码里，只是某一天恰好有人能改。
//!
//! 判据是「名字出现过」，不是「每份文档都写全」：样例、chart、静态清单与渲染
//! 出来的 profile 各承担一部分，合起来才是运维手上那一叠文档。一份文档漏掉某个
//! 键、另一份写了，就有人能发现它。
//!
//! 「写过」有两种形态，都算：「文档树里写着这个键」，与「文档自己的 `env` 表把
//! 某个环境变量指向这个点分路径」——后者由加载器真的应用，运维有一条不碰文件的
//! 改法。只认前一种时，一个值由代码默认给、环境变量可调的旋钮会被读成「文档没
//! 写」，于是有人把那个默认值抄进样例来让这道门变绿（2026-11-19 实际发生过：
//! 69 个叶子是默认值的逐字副本）。
//!
//! 所有交付文档都没写的键，才落在 [`CODE_ONLY`]：
//! 每一条都要写明它为什么只能由代码给值。登记了却已经被某份文档写出来的，按过期
//! 报出来——许可留在已经没人走的路口，下一个真把旋钮写死的人就没有判据拦。
//!
//! 节类型、声明文档的位置、走节的骨架都只有一份，在 `common::config_surface`；
//! 本门与 `config_section_reader_contract_test` 共用它。
//!
//! `produced` 来自两处之和：读回该节得到的键，与读取器类型的字段声明。只靠读回
//! 时会漏掉 `skip_serializing_if` 在默认值上省掉的字段（`dag_executor.nats_url`、
//! `system.enabled_plugins`），它们在文档里看不见，却真的能被类型读进来；声明把
//! 这一半补上。字段声明同时给出 `#[serde(skip)]`（不算旋钮）与 `#[serde(alias)]`
//! （同一个旋钮的另一个拼写）的答案。
//!
//! 字段声明跟着字段类型往下走：`Option` / `Vec` / `Box` / `Arc` 剥壳，本地结构体
//! 递归，`HashMap` / `Value` 是动态键。所以 `observability.loki.*`、
//! `infra_watch.rules[]`、`providers.*` 这些内层旋钮与顶层在同一条判据下，而不是
//! 只对顶层成立。
//!
//! 判不了什么：
//!
//! * 键与消费者的连接：字段有、文档写了、代码没读，本门看不见。那是阅读器与
//!   消费者之间的连接，`config_section_reader_contract_test` 的文件头已说明。
//! * 只活在 `src/` 里的常量式旋钮（超时、上限、名字）：它们根本不进配置面，由
//!   各道「声明只写一次」的墙按全树生产源判。

use std::collections::BTreeSet;

use crate::common;

use common::config_surface::{readers, section_shapes, shipped_configs, TypeGraph};
use common::routes::repo_root;

/// At least this many knobs must be produced by the reader types. A floor,
/// because an empty set satisfies every "nothing is missing" statement.
const PRODUCED_FLOOR: usize = 300;

/// At least this many documents must be shipped: the chart's file, the static
/// manifest and the rendered profiles.
const DOCUMENT_FLOOR: usize = 5;

/// A knob no delivered document names, and why the code has to supply it.
struct CodeOnly {
    /// `section.key`, or `section.nested.key` for a pointer-read section.
    path: &'static str,
    why: &'static str,
}

const fn code_only(path: &'static str, why: &'static str) -> CodeOnly {
    CodeOnly { path, why }
}

/// 摄取面的周期与批量：没有环境入口，也没有哪条实测导出过它们（分诊归 D5）。
/// 代码默认值是这一族今天唯一的声明；样例里那份是同一个数的逐字副本，2026-11-19 删除。
/// 要改就写配置文件同名键。
const INGEST_ONLY: &str = "摄取面的周期与批量：没有环境入口，也没有哪条实测导出过它\
                          （分诊归 D5）。代码默认值是唯一的声明，样例副本 2026-11-19 删除";

/// GitHub 集成面的周期与上限：随部署的仓库规模与落地节奏走。
const GITHUB_ONLY: &str = "GitHub 集成面的周期与上限：随部署的仓库规模与落地节奏走；\
                           没有环境入口，代码默认是唯一的声明，样例副本 2026-11-19 删除";

/// Gitee 集成面：与 `GITHUB_ONLY` 同形。
const GITEE_ONLY: &str = "Gitee 集成面的周期与上限：同 `GITHUB_ONLY`，代码默认是唯一的\
                          声明，样例副本 2026-11-19 删除";

/// 配置投放判据的两段等待：窗口由「换版时 Pod 被替换」的实际时长定（见该节的
/// `_comment`，2026-10-22 实测一次滚动 73 秒）。写死秒数会把一次观察钉成常量。
const DECLARATION_WAIT_ONLY: &str = "两段等待由「换版时 Pod 被替换」的窗口定（见该节\
                                      `_comment` 的实测）；样例副本 2026-11-19 删除";

/// loki 推送的批与退避：随日志量与导出端走（见该节 `_comment_batching`）。
const LOKI_BATCH_ONLY: &str = "loki 推送的批与退避：随日志量与导出端走（见\
                              `_comment_batching`）；代码默认是唯一的声明，样例副本删除";

/// Knobs no delivered document writes.
///
/// An entry is a claim someone looked at. Three kinds are legitimate, and each
/// entry says which of them it is:
///
/// * the knob has an environment entry in its owning crate (`AGENT_LOOP_ENV`,
///   `OBS_ENV`, `MEMORY_ENV`), so the sample is not its only delivery surface.
///   The document's own `env` table needs no entry here: [`surfaces`] reads it
///   as a naming form, and an entry for one of its paths is stale by design;
/// * the default *is* the decision — an empty optional (`None` means this
///   family has no executable condition), a block that is off by default and
///   would read as a recommendation if a sample carried it, or a value derived
///   from another bound (a reclaim window, a tool-return budget);
/// * the knob names a per-entity default rather than a deployment setting (the
///   agent pool's default employee identity, overwritten per employee).
const CODE_ONLY: &[CodeOnly] = &[
    code_only("gitee_integration.max_issues_per_scan", GITEE_ONLY),
    code_only("gitee_integration.poll_interval_secs", GITEE_ONLY),
    code_only("github_integration.conversation.awaiting_reply_timeout_hours", GITHUB_ONLY),
    code_only("github_integration.conversation.max_clarification_rounds", GITHUB_ONLY),
    code_only("github_integration.landing_policy.ci_poll_interval_secs", GITHUB_ONLY),
    code_only("github_integration.landing_policy.ci_watch_timeout_secs", GITHUB_ONLY),
    code_only("github_integration.landing_policy.max_land_attempts", GITHUB_ONLY),
    code_only("github_integration.landing_policy.redrive_cause_window_secs", GITHUB_ONLY),
    code_only("github_integration.landing_policy.redrive_max_rounds_per_cause", GITHUB_ONLY),
    code_only("github_integration.max_issues_per_scan", GITHUB_ONLY),
    code_only("github_integration.poll_interval_secs", GITHUB_ONLY),
    code_only(
        "llm_routing.backends[].weight",
        "每个 backend 的默认权重是 1：样例里每一项 `weight: 1` 都只是把默认值抄了一遍\n         （2026-11-19 删除）。要改某个 backend 的权重就写那一项；不写的那项由类型给 1",
    ),
    code_only("memory.ingest.aged_out_redrive_batch", INGEST_ONLY),
    code_only("memory.ingest.backlog_warn_at", INGEST_ONLY),
    code_only("memory.ingest.bus_claim_batch", INGEST_ONLY),
    code_only("memory.ingest.bus_claim_interval_secs", INGEST_ONLY),
    code_only("memory.ingest.bus_claim_min_idle_ms", INGEST_ONLY),
    code_only("memory.ingest.extraction_concurrency", INGEST_ONLY),
    code_only(
        "memory.ingest.extraction_input_budget_tokens",
        "摄取抽取的输入预算：随部署的模型上下文与记忆条目长度走；见 `INGEST_ONLY`，样例副本 2026-11-19 删除",
    ),
    code_only("memory.ingest.max_retries", INGEST_ONLY),
    code_only("memory.ingest.pool_check_secs", INGEST_ONLY),
    code_only("memory.ingest.pull_pause_after_failures", INGEST_ONLY),
    code_only("memory.ingest.pull_pause_initial_secs", INGEST_ONLY),
    code_only("memory.ingest.pull_pause_max_secs", INGEST_ONLY),
    code_only("memory.ingest.pull_resume_observation_secs", INGEST_ONLY),
    code_only("memory.ingest.reconcile_interval_secs", INGEST_ONLY),
    code_only("memory.ingest.reconcile_lookback_hours", INGEST_ONLY),
    code_only("memory.ingest.retry_base_delay_ms", INGEST_ONLY),
    code_only("observability.config_declaration.hold_secs", DECLARATION_WAIT_ONLY),
    code_only("observability.config_declaration.settle_secs", DECLARATION_WAIT_ONLY),
    code_only(
        "observability.infra_watch.eval_failure_alert_after",
        "连续几轮求值失败才报：判据是「一条规则坏了多久会被发现」，随求值周期走；\n         样例副本 2026-11-19 删除",
    ),
    code_only("observability.loki.flush_interval_sec", LOKI_BATCH_ONLY),
    code_only("observability.loki.max_batch_size", LOKI_BATCH_ONLY),
    code_only("observability.loki.max_retries", LOKI_BATCH_ONLY),
    code_only("observability.loki.timeout_secs", LOKI_BATCH_ONLY),
    code_only(
        "observability.trace_collector.buffer_max_bytes",
        "trace 事件的内存缓冲预算，按部署的内存上限定（见该节 `_comment`）；样例里的数\n         只是默认值的副本",
    ),
    code_only(
        "pge.local_repair_max",
        "本地修复的每轮上限：随部署的分区规模走；代码默认是唯一的声明，样例副本\n         2026-11-19 删除",
    ),
    code_only(
        "ralph.max_iterations",
        "Ralph Loop 的迭代上限与停滞窗口由同一条预算决定（见该节 `_comment`）：两条\n         都按次计、不跨执行累计；样例副本 2026-11-19 删除",
    ),
    code_only(
        "ralph.stagnation_window",
        "同上：停滞窗口与迭代上限是一对，写死一个数会把另一半藏起来",
    ),
    code_only(
        "self_evolution.artifact_evolution.interval_secs",
        "策略进化的试验节奏：由闭环上一轮的耗时决定（与 `poll_interval_secs` 同形），\n         样例写死一个周期会读成部署的选择",
    ),
    code_only(
        "self_evolution.artifact_evolution.min_trials",
        "一个候选策略要被判所必需的试验数：统计判据的样本量，随噪声与效应量走；\n         样例副本 2026-11-19 删除",
    ),
    code_only(
        "self_review.max_iterations",
        "自审的最大轮数：随部署的产出规模走；代码默认是唯一的声明，样例副本\n         2026-11-19 删除",
    ),
    code_only(
        "agent_loop.agent_id",
        "池子的缺省参数而不是身份：每位员工的名字在它被造出来时由管理器逐位覆盖；\
         把某个名字写进样例，读配置的人会以为平台上有这么一位",
    ),
    code_only(
        "agent_loop.think_stall_timeout_secs",
        "由 `COGNEVA_AGENT_LOOP_THINK_STALL_TIMEOUT_SECS` 覆盖；默认值是「多久没动静\
         算停了」的判据，随部署的模型延迟走",
    ),
    code_only(
        "agent_loop.external_archive_threshold_chars",
        "由 `COGNEVA_AGENT_LOOP_EXTERNAL_ARCHIVE_THRESHOLD_CHARS` 覆盖；默认值受工具\
         返回预算约束（见字段注释），样例不复制一个推导出来的数",
    ),
    code_only(
        "agent_loop.web_search_fallback_base",
        "由 `COGNEVA_WEB_SEARCH_FALLBACK_BASE` 覆盖；不设就是让查询不出自己这棵树——\
         那不是一个缺省值，而是部署的选择，样例不替部署写一个后备端点",
    ),
    code_only(
        "dag_executor.redis_read_block_ms",
        "没有环境变量入口：默认值由 Redis 读阻塞语义与部署的往返时间决定（见\
         `default_redis_read_block_ms`），样例写一个数等于替部署做调优决定；配置文件\
         里写入即生效",
    ),
    code_only(
        "dag_executor.redis_read_resume_block_ms",
        "同上：从已见 id 续读的那一侧更短，默认值与阻塞读一起推导；样例不复制推导值",
    ),
    code_only(
        "dag_executor.scheduled_task_stall_secs",
        "默认值必须不短于传输层自己的回收窗口（见字段注释）；两者是一起推导的，\
         样例写死一个数会把耦合关系藏起来",
    ),
    code_only(
        "gateway.access_token_ttl_minutes",
        "`0` = 用内置缺省（`DEFAULT_ACCESS_TOKEN_TTL_MINUTES`）；把 0 写进样例是把\
         「没选」写成一个选择。键由 `GatewayConfig` 声明，配置文件写入即生效，样例\
         不替部署预设会话寿命",
    ),
    code_only(
        "gateway.demo_login_enabled",
        "由 `COGNEVA_DEMO_LOGIN_ENABLED` 覆盖；样例把它写成 false 会把一个一次性\
         演示开关当成部署建议",
    ),
    code_only(
        "gitee_integration.api_base",
        "由 `GITEE_API_BASE_ENV` 指向的环境变量覆盖；自建 Gitee 的地址随部署走",
    ),
    code_only(
        "gitee_integration.token_env",
        "它本身是一个环境变量名的指针；部署的 Secret 名字由部署自己定，样例写死一个\
         名字会把别人的 Secret 当成约定",
    ),
    code_only(
        "memory.maintenance",
        "整块是周期维护（低价值记忆自动衰减）的运行参数，由 `MEMORY_ENV` 的\
         `COGNEVA_MEMORY_DECAY_*` 覆盖；样例只写摄取面，写一块衰减参数会把\
         「默认在衰减」读成部署的选择",
    ),
    code_only(
        "observability.jaeger",
        "整块默认关闭；由 `OBS_ENV` 的 `COGNEVA_JAEGER_*` 覆盖。样例写一块关闭的\
         exporter 会读成「这个部署有它」",
    ),
    code_only(
        "observability.clickhouse",
        "整块默认关闭；由 `OBS_ENV` 的 `COGNEVA_CLICKHOUSE_*` 覆盖",
    ),
    code_only(
        "observability.alertmanager",
        "整块默认关闭；由 `OBS_ENV` 的 `COGNEVA_ALERTMANAGER_*` 覆盖",
    ),
    code_only(
        "observability.elasticsearch",
        "整块默认关闭，且它没有环境变量入口：只有配置文件能启用它。样例不写一块关闭的\
         导出器——写它会读成「这个部署有它」，而部署要启用时直接写这个键",
    ),
    code_only(
        "self_evolution.executor_enabled",
        "由 `COGNEVA_SELF_EVOLUTION_EXECUTOR_ENABLED` 覆盖；默认关闭是安全方向，\
         样例不替部署开启进化执行",
    ),
    code_only(
        "self_evolution.fmt_check_command",
        "`None` = 这一族没有可执行条件（见字段注释）；空值不是「没配置」，而是一个\
         部署断言，样例不替它断言自己有格式化检查",
    ),
    code_only(
        "self_evolution.lint_command",
        "`None` = 这个项目没有 linter（见字段注释）；写进样例等于替部署断言它有",
    ),
    code_only(
        "self_evolution.image_rollout",
        "整块默认关闭；由部署按需写。样例写一块关闭的滚动路径会读成「这个部署有它」",
    ),
    code_only(
        "self_evolution.microvm",
        "整块默认关闭；路径与内核镜像随宿主走（见默认值），样例不替部署写宿主路径",
    ),
    code_only(
        "self_evolution.workspaces",
        "由 `COGNEVA_SELF_EVOLUTION_WORKSPACES_*` 覆盖；默认值即生产路径（见字段注释），\
         样例不复制路径推导",
    ),
    code_only(
        "system.partition_maintenance_interval_secs",
        "由 `COGNEVA_PARTITION_MAINTENANCE_INTERVAL_SECS` 覆盖；维护周期随部署的\
         分区规模走",
    ),
    code_only(
        "dag_executor.nats_url",
        "旧的单 URL 拼写，载入时升级并入 `dag_executor.nats.urls`；交付文档只写升级\
         后的形状，两处都写会让同一个端点有两个来源",
    ),
    code_only(
        "github_integration.bot_identity.persona",
        "实例自治身份：首次进化时生成并持久化。样例写一个名字等于替每个部署定下同\
         一位人格",
    ),
    code_only(
        "github_integration.bot_identity.fingerprint",
        "实例机器指纹，缺失时首次使用自动生成；样例写一个值会让两个部署共用同一\
         指纹，把各自的记录认成对方的",
    ),
    code_only(
        "github_integration.bot_identity.app_slug",
        "GitHub App 的公开 slug，随实际安装的 App 走；未配置时只认 username。样例\
         没有 App 可指，写它会读成「这个部署以 App 身份发言」",
    ),
    code_only(
        "observability.data_volume_watch.claim",
        "由 `COGNEVA_DATA_VOLUME_CLAIM` 给出（`cog-core/src/claim_footprint.rs`）；\
         空 = 不启动观测，而 PVC 名是部署拓扑事实，不是平台能替它写的",
    ),
    code_only(
        "observability.data_volume_watch.volumes",
        "由 `COGNEVA_DATA_VOLUME_MOUNTS` 给出；每个额外挂载的 claim/path 配对只有\
         该部署自己知道（见字段注释），样例不替它声明自己挂了哪些卷",
    ),
    code_only(
        "self_evolution.deploy_admission.server_dry_run",
        "`Option<Vec<String>>`：省略就是这个部署没有开启集群侧 dry-run。样例写一个\
         argv 会读成检查已开启，而判据进程可能根本连不上自己的 API server",
    ),
    code_only(
        "dag_executor.archive_after_secs",
        "48 h = 两次最长信号冷却（24 h）加一次重启余量：重试前提要读得到那行失败\
         记录，行被清掉，重试就静默变成一次新提交（见字段注释）。样例写 172800 是\
         把推导结果钉成一个数，改冷却的人看不到另一半",
    ),
    code_only(
        "dag_executor.archive_poll_interval_secs",
        "归档节奏由保留期（`archive_after_secs`）决定，本值只是多久看一次；样例写\
         一个扫描周期会把「多久看一次」读成部署的选择",
    ),
    code_only(
        "dag_executor.max_retries",
        "重试次数是部署对任务成功的容忍度；平台默认 3 是没人决定时的答案，样例不\
         替部署定（与 `decomposition_max_attempts` 同理，后者有环境入口，这个没有）",
    ),
    code_only(
        "dag_executor.nats.consumer_ack_wait_secs",
        "必须大于最坏单条处理时延（归档 + 两次 LLM + 重试，分钟级），900 秒覆盖\
         它；样例写死一个数会让改处理链路的人看不到这个下界",
    ),
    code_only(
        "dag_executor.nats.consumer_max_deliver",
        "毒药消息的投递上限，是写 DLQ 的判据：协议消费语义，不是部署的选择",
    ),
    code_only(
        "dag_executor.nats.consumer_max_ack_pending",
        "背压闸门：积压留在流里而不是堆进进程内存（见字段注释）；样例写 1024 会\
         读成部署对消费者容量的决定",
    ),
    code_only(
        "hook_engine.default_rate_limit",
        "整块是内建 hook 的缺省令牌桶；每个部署的 hook 有它自己的限速，样例写一块\
         默认桶会读成部署替所有 hook 定了速率",
    ),
    code_only(
        "hook_engine.hook_timeout_secs",
        "hook 延迟的容忍度随部署的 hook 实现走；样例不替部署定一个超时",
    ),
    code_only(
        "multi_backend_consumer.publish_buffer_capacity",
        "总线故障窗的异步缓冲上限，排满即丢事件（见字段注释）：是「宁可丢也不无\
         界」的语义，不是容量选择",
    ),
    code_only(
        "multi_backend_consumer.publish_retry_base_delay_ms",
        "指数退避基数，随总线故障的恢复时间走；样例写 1000 会把它读成一个调过的值",
    ),
    code_only(
        "multi_backend_consumer.retry_interval_secs",
        "消费重试周期，随部署的消息面负载走",
    ),
    code_only(
        "self_evolution.build_gate.build_demand_cores",
        "一次构建实测要几个核；由 `cogneva_build_gate_tuning{input,source}` 以\
         `calibrated` 发布，运行期实测到新值才转 `derived`。样例写一个数会把它变\
         成 `configured`——读的人会以为有人选过它",
    ),
    code_only(
        "self_evolution.build_gate.build_peak_rss_mib",
        "实测构建峰值 RSS；同上由读数发布，运行期跟着实测走，样例不复制一次测量",
    ),
    code_only(
        "self_evolution.build_gate.mem_floor_mib",
        "这个进程要留住的内存地板，按宿主付得起的内存量裁过（`fitted`）；样例写\
         24576 会读成有人选过",
    ),
    code_only(
        "self_evolution.build_gate.cpu_calm_pct",
        "「安静」的 CPU 判据，本机标定；样例写 5.0 会把它读成一个调好的阈值",
    ),
    code_only(
        "self_evolution.build_gate.io_calm_pct",
        "「安静」的 IO 判据，与 CPU 判据一起标定；样例不复制标定值",
    ),
    code_only(
        "self_evolution.build_gate.mem_calm_pct",
        "「安静」的内存判据，同上",
    ),
    code_only(
        "self_evolution.build_gate.io_stall_budget_pct",
        "每次构建可分到的 IO 停顿预算，按构建峰值与磁盘实测推出；样例不复制推导值",
    ),
    code_only(
        "self_evolution.build_gate.bound_window_secs",
        "构建门回看多久算「忙」：判据的时间尺度，随本机负载标定；`Tuning::shipped()`\
         从配置默认读、不再写第二份（见该处注释），样例也不写",
    ),
    code_only(
        "self_evolution.build_gate.cpu_sample_gap_ms",
        "两次 CPU 采样的间隔：与 `bound_window` 一起标定的时间尺度",
    ),
    code_only(
        "self_evolution.build_gate.reserve_fresh_secs",
        "构建槽预约的新鲜期，过期即丢弃（见 `reserve_fresh` 的注释），随一轮构建\
         的时长走",
    ),
    code_only(
        "self_evolution.health_check_grace_period_secs",
        "滚动替换后给新进程的启动宽限期，随部署的启动耗时走；样例不替部署定",
    ),
    code_only(
        "self_evolution.health_check_interval_secs",
        "宽限期内的探测间隔，与宽限期一起定",
    ),
    code_only(
        "self_evolution.health_check_max_retries",
        "宽限期内允许的不健康探测次数",
    ),
    code_only(
        "supervisor.alert_history_max",
        "进程内告警历史的保留条数：内存预算的一部分，随部署的内存上限走",
    ),
    code_only(
        "supervisor.behavior_history_max",
        "每位 agent 的行为历史条数，随部署的 agent 数走",
    ),
    code_only(
        "supervisor.broadcast_capacity",
        "监督面广播通道容量：内存预算的一部分",
    ),
    code_only(
        "supervisor.event_window_secs",
        "监督面滑动统计窗口：判据的时间尺度",
    ),
    code_only(
        "supervisor.health_checker",
        "整块是三类判据（suspect / dead / stuck）的时长；阈值随部署的 agent 心跳\
         节奏走，样例写一块默认值会读成部署的判断题",
    ),
    code_only(
        "supervisor.heartbeat_history_max",
        "心跳历史保留条数，与 agent 数一起定",
    ),
    code_only(
        "supervisor.quota_threshold",
        "配额告警阈值：量纲随部署的配额面走，样例不替部署定",
    ),
    code_only(
        "supervisor.rebalance_interval_secs",
        "任务再平衡周期，随部署的任务积压节奏走",
    ),
    code_only(
        "supervisor.task_rebalancer",
        "整块是每轮再平衡的上限（每位 agent 几个任务、每轮几次分配），随部署的\
         agent 池规模走",
    ),
    code_only(
        "system.event_channel_capacity",
        "`AgentEvent` 广播通道容量：内存预算的一部分，随部署的事件速率走",
    ),
    code_only(
        "system.task_event_channel_capacity",
        "`TaskEvent` 广播通道容量：同上",
    ),
    code_only(
        "system.timeout_checker_interval_secs",
        "超时回收的检查周期：一条 `Running` 任务能被发现停摆的延迟上界就是这个值",
    ),
    code_only(
        "tier_migrator.cold_compression_level",
        "zstd 级别 9 是「冷层才值得多花 CPU」的约定值（3 是同一条约定的另一半）；\
         样例写它会读成部署的压缩选择",
    ),
    code_only(
        "tier_migrator.warm_compression_level",
        "同上：3 是热→温的默认代价，与冷层的 9 成对",
    ),
    code_only(
        "system.enabled_plugins",
        "`None` = 加载全部插件；写进样例即变成白名单，把插件面收窄成一个选择。\
         黑名单是 `disabled_plugins`，样例写的是它",
    ),
];

fn registered(path: &str) -> Option<&'static CodeOnly> {
    CODE_ONLY.iter().find(|entry| entry.path == path)
}

/// The dot paths a delivered document's own `env` table points at.
///
/// The table is `env var name -> dot path inside this document`, and the loader
/// applies it: a name written here is a way for an operator to set that knob
/// without touching the file. So the table is a naming surface, and a knob it
/// points at is delivered even when the document writes no value for that leaf.
///
/// `_comment` keys are prose. Values that are not strings are not mappings.
fn env_named_paths(doc: &serde_json::Value) -> Vec<String> {
    let Some(env) = doc.get("env").and_then(|env| env.as_object()) else {
        return Vec::new();
    };
    env.iter()
        .filter(|(name, _)| !name.starts_with("_comment"))
        .filter_map(|(_, target)| target.as_str().map(str::to_string))
        .collect()
}

/// Every knob path a reader produces and every one a delivered document names,
/// both as `section.key` / `section.nested.key`, plus the document count.
///
/// Two naming forms count, because both are in the delivered document:
/// the leaf written in the document's tree, and the `env` table entry that
/// points at it (see [`env_named_paths`]).
fn surfaces(root: &std::path::Path) -> (BTreeSet<String>, BTreeSet<String>, usize) {
    let mut named = BTreeSet::new();
    let mut produced = BTreeSet::new();
    let documents = shipped_configs(root);
    let docs = documents.len();
    let graph = TypeGraph::new(root);
    for (file, doc) in documents {
        named.extend(env_named_paths(&doc));
        for reader in readers() {
            if reader.section == "_comment" {
                continue;
            }
            // A section the document does not carry is still a section the
            // reader type produces: asking the type what it can read does not
            // depend on whether this one document happens to write it. Skipping
            // absent sections made `produced` a function of the document, so an
            // emptied-out section (`"hook_engine": {}`) fell out of the census
            // and every knob in it stopped being checked — the empty object was
            // there only to keep the gate looking at it.
            let empty = serde_json::json!({});
            let section = doc.get(reader.section).unwrap_or(&empty);
            let mut shapes = Vec::new();
            section_shapes(&graph, &file, &reader, section, reader.section, &mut shapes);
            for shape in shapes {
                for key in shape.produced {
                    produced.insert(format!("{}.{key}", shape.path));
                }
                for key in shape.written {
                    named.insert(format!("{}.{key}", shape.path));
                }
            }
        }
    }
    (named, produced, docs)
}

#[test]
fn every_knob_a_reader_produces_is_named_by_a_delivered_document() {
    let root = repo_root();
    let (named, produced, docs) = surfaces(&root);
    assert!(
        docs >= DOCUMENT_FLOOR,
        "只读到 {docs} 份交付文档（下限 {DOCUMENT_FLOOR}）：chart、静态清单或\
         渲染 profile 少了一份，「文档没写」是在一叠缺页的文档上说的"
    );
    assert!(
        produced.len() >= PRODUCED_FLOOR,
        "只读出 {} 个旋钮（下限 {PRODUCED_FLOOR}）：走节的那一段没读到题，\
         「交付文档都写了」是在一个空集上说的",
        produced.len()
    );
    assert!(
        named.len() >= PRODUCED_FLOOR,
        "交付文档只写出 {} 个旋钮（下限 {PRODUCED_FLOOR}）：读文档那一段停了",
        named.len()
    );
    assert!(
        named.intersection(&produced).count() >= PRODUCED_FLOOR,
        "交付文档写出的键与类型产生的键交集只有 {} 个（下限 {PRODUCED_FLOOR}）：\
         两边读的不是同一份旋钮",
        named.intersection(&produced).count()
    );

    let missing: Vec<&String> = produced
        .difference(&named)
        .filter(|path| registered(path).is_none())
        .collect();
    assert!(
        missing.is_empty(),
        "这些旋钮没有任何一份交付文档写出来。值由代码默认给、运维在文档里看不见，\
         等于写死在代码里，只是某一天恰好有人能改。要么把它写进至少一份交付文档，\
         要么在 CODE_ONLY 里写明为什么只能由代码给值：\n{}",
        missing
            .iter()
            .map(|path| format!("  {path}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// 文档里有没有在这个路径上写着一个值。
fn the_tree_holds(doc: &serde_json::Value, path: &str) -> bool {
    let mut cur = doc;
    for step in path.split('.') {
        match cur.get(step) {
            Some(child) => cur = child,
            None => return false,
        }
    }
    true
}

/// `env` 表那一条分支得真的在起作用，而且它带来的不止重复。
///
/// 两个断言：表里点到的每个旋钮都算「被点名」；至少有一个旋钮**只**被 env 表
/// 点名——文档树里没有写它的值。后者是这条分支存在的理由：少了它，环境入口就
/// 不算交付面，那些只能用环境变量调的旋钮会被读成「文档没写」，而它们的值本来
/// 就该由代码默认给，写进样例反而成了第二个声明。
#[test]
fn an_env_entry_names_the_knob_it_points_at() {
    let root = repo_root();
    let (named, produced, _) = surfaces(&root);
    let documents = shipped_configs(&root);
    let mut only_by_env = BTreeSet::new();
    let mut checked = 0;
    for (file, doc) in &documents {
        for path in env_named_paths(doc) {
            if !produced.contains(&path) {
                continue; // 指向的键不归这些读取器：别的门判它
            }
            checked += 1;
            assert!(
                named.contains(&path),
                "{file}: env 表指向 {path}，但「交付文档点名」的集合里没有它——\
                 读 env 那一段坏了"
            );
            if !the_tree_holds(doc, &path) {
                only_by_env.insert(format!("{path}（{file}）"));
            }
        }
    }
    assert!(
        checked >= 1,
        "env 表一条都没点到一个真旋钮：这张表的读法坏了，上面那条判据会因此\
         把只能用环境变量调的旋钮全读成「文档没写」"
    );
    assert!(
        !only_by_env.is_empty(),
        "每个 env 指向的旋钮在文档树里都写了值——这条分支没有带来任何一条新的\
         点名，它要么退化成重复，要么读 env 那一段停了"
    );
    println!(
        "env 表点名 {checked} 次（跨 {} 份文档），其中 {} 个旋钮只在 env 表里被点名",
        documents.len(),
        only_by_env.len()
    );
}

#[test]
fn a_code_only_entry_that_is_named_or_gone_is_stale() {
    let (named, produced, _) = surfaces(&repo_root());
    let stale: Vec<&CodeOnly> = CODE_ONLY
        .iter()
        .filter(|entry| named.contains(entry.path) || !produced.contains(entry.path))
        .collect();
    assert!(
        stale.is_empty(),
        "这些 CODE_ONLY 登记已经不指向「类型产生、文档不写」的旋钮：{}\n\
         登记是给一处现场发的许可，现场没了许可就该收回——留着它，\
         下一次有人真的把旋钮只写在代码里时，这条登记会替他放行。",
        stale
            .iter()
            .map(|entry| format!("\n  {}（{}）", entry.path, entry.why))
            .collect::<Vec<_>>()
            .join("")
    );
}

/// 判据得先证明自己会红：把一份文档改名后的旋钮与类型产生的旋钮并起来比较，
/// 差集就是「没被写的那些」。这里用合成集合问同一段减法，免得某天两边都读成
/// 空集时判词仍然全绿。
#[test]
fn the_subtraction_reddens_on_a_knob_no_document_names() {
    let produced: BTreeSet<String> = ["gateway.port", "gateway.host", "system.data_root"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let named: BTreeSet<String> = ["gateway.port", "gateway.host"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let missing: Vec<&String> = produced.difference(&named).collect();
    assert_eq!(missing, vec!["system.data_root"]);
}

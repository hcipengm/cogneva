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
//! 键、另一份写了，就有人能发现它。所有交付文档都没写的键，才落在 [`CODE_ONLY`]：
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

/// At least this many documents must be shipped: example, chart, static
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

/// Knobs no delivered document writes.
///
/// An entry is a claim someone looked at. Three kinds are legitimate, and each
/// entry says which one it is:
///
/// * the knob has an environment entry of its own, so the document sample is
///   not its only delivery surface (`AGENT_LOOP_ENV`, `OBS_ENV`, the
///   `COGNEVA_*` dot-path table);
/// * the default *is* the decision — an empty optional (`None` means this
///   family has no executable condition), a block that is off by default and
///   would read as a recommendation if a sample carried it, or a value derived
///   from another bound (a reclaim window, a tool-return budget);
/// * the knob names a per-entity default rather than a deployment setting (the
///   agent pool's default employee identity, overwritten per employee).
const CODE_ONLY: &[CodeOnly] = &[
    code_only(
        "agent_loop.agent_id",
        "池子的缺省参数而不是身份：每位员工的名字在它被造出来时由管理器逐位覆盖；\
         把某个名字写进样例，读配置的人会以为平台上有这么一位",
    ),
    code_only(
        "agent_loop.max_iterations",
        "`None` = 不设轮数天花板；由 `COGNEVA_AGENT_LOOP_MAX_ITERATIONS` 覆盖，\
         写进样例等于替部署下一个轮数断言",
    ),
    code_only(
        "agent_loop.eval_max_iterations",
        "同上：`None` = 不设天花板；由 `COGNEVA_AGENT_LOOP_EVAL_MAX_ITERATIONS` 覆盖",
    ),
    code_only(
        "agent_loop.think_stall_timeout_secs",
        "由 `COGNEVA_AGENT_LOOP_THINK_STALL_TIMEOUT_SECS` 覆盖；默认值是「多久没动静\
         算停了」的判据，随部署的模型延迟走",
    ),
    code_only(
        "agent_loop.final_draft_timeout_secs",
        "由 `COGNEVA_AGENT_LOOP_FINAL_DRAFT_TIMEOUT_SECS` 覆盖；它界的是预算花完之后\
         那一次收尾调用，默认值随上游尾延迟走",
    ),
    code_only(
        "agent_loop.memory_api_base",
        "由 `COGNEVA_MEMORY_API_BASE` 覆盖；指向平台自己的 HTTP 面，地址随部署走",
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
        "dag_executor.decomposition_max_attempts",
        "由 `COGNEVA_DECOMPOSITION_MAX_ATTEMPTS` 覆盖；重试次数是部署对分解质量的\
         容忍度，不是平台该替它定的值",
    ),
    code_only(
        "dag_executor.decomposition_orphan_watch_enabled",
        "由 `COGNEVA_DECOMPOSITION_ORPHAN_WATCH_ENABLED` 覆盖；回收孤儿占位任务是\
         一项运维决定（开着会终止别人眼里还在排队的行）",
    ),
    code_only(
        "dag_executor.decomposition_orphan_poll_interval_secs",
        "由 `COGNEVA_DECOMPOSITION_ORPHAN_POLL_INTERVAL_SECS` 覆盖；扫描周期随部署\
         的规模走",
    ),
    code_only(
        "dag_executor.decomposition_orphan_stall_after_secs",
        "由 `COGNEVA_DECOMPOSITION_ORPHAN_STALL_AFTER_SECS` 覆盖；多久算卡住是部署\
         对自身负载的判断",
    ),
    code_only(
        "dag_executor.decomposition_orphan_alert_dwell_secs",
        "由 `COGNEVA_DECOMPOSITION_ORPHAN_ALERT_DWELL_SECS` 覆盖；默认值是扫描/观察\
         周期的两倍（见字段注释），推导关系写在代码里比抄进样例更可靠",
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
        "system.enabled_plugins",
        "`None` = 加载全部插件；写进样例即变成白名单，把插件面收窄成一个选择。\
         黑名单是 `disabled_plugins`，样例写的是它",
    ),
];

fn registered(path: &str) -> Option<&'static CodeOnly> {
    CODE_ONLY.iter().find(|entry| entry.path == path)
}

/// Every knob path a reader produces and every one a delivered document names,
/// both as `section.key` / `section.nested.key`, plus the document count.
fn surfaces(root: &std::path::Path) -> (BTreeSet<String>, BTreeSet<String>, usize) {
    let mut named = BTreeSet::new();
    let mut produced = BTreeSet::new();
    let documents = shipped_configs(root);
    let docs = documents.len();
    let graph = TypeGraph::new(root);
    for (file, doc) in documents {
        for reader in readers() {
            if reader.section == "_comment" {
                continue;
            }
            let Some(section) = doc.get(reader.section) else {
                continue;
            };
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
        "只读到 {docs} 份交付文档（下限 {DOCUMENT_FLOOR}）：样例、chart、静态清单或\
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

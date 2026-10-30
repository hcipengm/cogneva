//! principle: feedback_a_named_sample_that_falls_back_to_the_default_answers_a_different_question
//! principle: feedback_a_rotation_ranked_by_never_proven_must_exclude_the_always_picked_one
//! `cogneva backup` / `cogneva restore`：把权威数据面打成可搬运备份包。
//!
//! 权威面 = 关系数据（逻辑导出）+ 向量索引（服务端一致性快照）+ 对象 raw 层
//! （文件级复制）+ 实例身份（Secret 打成 apply-able manifest）。其余状态
//! （NATS/Redis/Meilisearch/registry/进化克隆）全部可重建，不进包。
//!
//! 那两份 Secret 清单**只在包本身是密文时才在**：清单里的值只是 base64，放进明文包
//! 就等于把凭证抄一份到备份卷与卷外副本上。没有包级口令时这一段缺席，manifest 的
//! `lossy_surfaces` 点名它。
//!
//! 包布局：
//! ```text
//! <pkg>/manifest.json                 版本/rev/实例指纹/时间/各段内容清单
//! <pkg>/secrets/cogneva-secrets.yaml      apply-able Secret（data 为 base64；仅密文包）
//! <pkg>/secrets/cogneva-secrets-oob.yaml  同上，带外凭证那一份（只被网关读；仅密文包）
//! <pkg>/pg/public.<table>.csv         COPY ... TO STDOUT CSV（分区父表含全分区）
//! <pkg>/pg/sequences.sql              全部序列的 setval 语句
//! <pkg>/qdrant/<collection>.snapshot  服务端一致性快照
//! <pkg>/objects/storage/...           cogneva-data-pvc 的 storage 子树原样
//! <pkg>/artifacts/<root>/policies/... 每个数据根的产物级进化策略库
//! <pkg>/artifacts/<root>/meta_learning/decision_stats.json  调参证据
//! <pkg>/artifacts/<root>/workspace/... 编辑器工作区整树
//! ```
//!
//! `artifacts/` 只收 `policies/`、`meta_learning/decision_stats.json` 与 `workspace/`，
//! 不收数据根的其余子树（工作树、数仓、原始日志）：那些要么可重建，要么体积以百 GiB
//! 计，且不属于「换机后必须逐字节相同」的权威面。`workspace/` 是例外里的例外——它
//! 没有第二条重建路径，用户在编辑器里新建的文件只有这一份。
//! 整包打成 `cogneva-backup-<UTC 时间戳>.tar.zst`，时间戳字典序即新旧序。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use cog_core::{
    BackupDrillRecord, BackupOutcome, BackupRunClosing, BackupRunOpening, BackupRunStore,
};
use cog_storage::{PostgresBackupRunStore, PostgresDecisionStatsStore};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// 包名前缀与时间戳格式：字典序排序即时间序，保留策略依赖这一点。
///
/// 前缀与后缀声明在 cog-core：网关按同一组名字列举、下载、删除包，两处各写一份
/// 时改一处不报错，另处的清单读成「一个包也没有」——而那正是第一次备份还没
/// 跑完时的读数，保留策略读着它什么也不删，看着像闲、不像坏。
pub const PACKAGE_PREFIX: &str = cog_core::BACKUP_PACKAGE_PREFIX;
pub const PACKAGE_SUFFIX: &str = cog_core::BACKUP_PACKAGE_SUFFIX;
const TIMESTAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

/// 打包与还原两侧都读的同几个环境变量。名字在这里写一次：写两遍的时候，改
/// 一处不会报错，另一处会读到一个再没人设置的名字，而「读不到」在调用点上
/// 与「没配」同形。
const BACKUP_DIR_ENV: &str = "COGNEVA_BACKUP_DIR";
const BACKUP_OFFSITE_DIR_ENV: &str = "COGNEVA_BACKUP_OFFSITE_DIR";
/// 演练**单趟**的预算（秒）。一趟 = 一份包从解包到对数完。
///
/// 界必须由进程自己拿着，而不是只由 Job 的 `activeDeadlineSeconds` 兜着：Job 的界
/// 到点是把 Pod 杀掉，进程没有机会写那一行，`backup_drills` 里于是只剩上一晚的记录
/// ——「这一趟被杀了」与「这一趟没跑过」在账上同形。2026-10-27 实测正是这样：第一趟
/// 2271 s 之后，第二趟在 3600 s 的 Job 界上被杀，一行都没留下。
const DRILL_PASS_BUDGET_ENV: &str = "COGNEVA_BACKUP_DRILL_PASS_BUDGET_SECS";
/// 没配 env 时单趟的预算。数与 values.yaml 的 `backup.drillPassBudgetSeconds`
/// 同源，由 `deploy/scripts/tests/backup-drill-budget-covers-both-passes.test.sh`
/// 盯着两边与 Job 界的关系。
// cap: bound — 与 Job 的界同源，由 backup-drill-budget-covers-both-passes 那个测试盯着两边的关系
const DRILL_PASS_BUDGET_DEFAULT_SECS: u64 = 3600;
const QDRANT_HTTP_URL_ENV: &str = "COGNEVA_QDRANT_HTTP_URL";
const IMAGE_REF_ENV: &str = "COGNEVA_IMAGE_REF";

/// 落库的错误文本上限。错误本身通常只有一行，这个界是为了让一条失控的长错误
/// （整段响应体、整张表名清单）不至于把行撑成不可读的东西。
// cap: bound — 错误本身通常只有一行；这个界让一条失控的长错误（整段响应体、整张表名清单）不至于把行撑成不可读的东西
const ERROR_TEXT_MAX_CHARS: usize = 2000;

/// 一次运行默认的期限，也是集群清单里那个 `activeDeadlineSeconds` 的默认值。
///
/// 实测一次运行 41 秒（含导出、快照、打包、裁剪），一小时的界是它的一百多倍：
/// 落在里面的运行照跑，落在外面的那个不是慢，是卡死了——而它卡着的时候，
/// 调度器的 `Forbid` 会让后面每一次都不再开始。没有这个界，一次挂住的运行
/// 就能把备份永久停掉，顺带把「谁来给它收尾」也一并停掉。
// cap: bound — 调度器的 Forbid 会让后面每一次都不再开始；没有这个界，一次挂住的运行就能把备份永久停掉
const DEFAULT_RUN_DEADLINE_SECONDS: i32 = 3600;

/// 内部实例密钥对象名：业务 Pod 读的那一份（数据库/缓存/内部签名/S3/邮件）。
pub const SECRET_NAME_INTERNAL: &str = cog_core::topology::SECRET_INTERNAL;
/// 带外凭证对象名：只被安全网关读的那一份（LLM 上游、平台 token、git 身份私钥…）。
/// 两片是权限边界，不是分类：内部那份是整对象 get，谁持它就读得到里面每一个键。
pub const SECRET_NAME_OOB: &str = cog_core::topology::SECRET_OOB;

/// 表发现查询：public schema 的普通表（relkind=r）与分区父表（p），
/// 排除分区子表（pg_inherits 有登记的就是子表——父子都导会双份）与
/// `_sqlx_migrations`（schema 版本由迁移器自己管，恢复目标先跑迁移，
/// 恢复旧行只会把版本记录拧回过去）。
const DISCOVER_TABLES_SQL: &str = r#"
    SELECT c.relname::text
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p')
      AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)
      AND c.relname <> '_sqlx_migrations'
    ORDER BY c.relname
"#;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableReport {
    pub name: String,
    pub rows: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectionReport {
    pub name: String,
    pub snapshot: String,
    pub points: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostgresReport {
    pub tables: Vec<TableReport>,
    pub sequences: usize,
}

/// 一个策略库的打包读数：版本数、链头、当前激活版本。
///
/// `active_version` 与 `head_version` 分开记，因为 ACTIVE 指旧版本是合法的回滚态，
/// 不是损坏。恢复端要判的是「ACTIVE 指向的那个版本在包里、且 hash 对得上」，不是
/// 「ACTIVE 等于最大版本」。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyStoreReport {
    pub name: String,
    pub versions: u64,
    pub head_version: u64,
    pub head_hash: String,
    /// ACTIVE 读不出来时为 0、`active_hash` 为空，原因进 `problem`。
    pub active_version: u64,
    pub active_hash: String,
    #[serde(default)]
    pub problem: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecisionStatsReport {
    pub groups: u64,
    pub trials: u64,
    pub bytes: u64,
}

/// 一个数据根下的产物层：策略库与调参证据。
///
/// 身份是「根名 + 库名」，不是库名：主应用与进化 Pod 可能各有一套同名库，
/// 它们是两份独立状态，不允许在清单里合成一条。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArtifactRootReport {
    pub root: String,
    /// 备份 Pod 里看到的挂载点，只作现场记录；恢复端按 `root` 选择目标根。
    pub data_dir: String,
    /// `policies/` 是否存在。空目录与「从来没有过」不是同一件事。
    pub policies_present: bool,
    #[serde(default)]
    pub policy_stores: Vec<PolicyStoreReport>,
    #[serde(default)]
    pub decision_stats: Option<DecisionStatsReport>,
    /// 编辑器工作区树（`{data_dir}/workspace`）的读数。`None` = 这个根没有工作区，
    /// 或者这个包是按更早的格式打出来的——两种情况都表示「包里没有这一棵树」，
    /// 恢复端不做任何事，而不是把它当成「有一个空工作区」。
    ///
    /// 它进包的理由与策略库不同：策略库能说「谁带它」，工作区不能——用户在那里面
    /// 写下的东西没有第二个副本，换一台机器就是没了。
    #[serde(default)]
    pub workspace: Option<WorkspaceReport>,
}

/// 工作区整棵树的读数：文件数与总字节。
///
/// 恢复端拿这两项去核**将要落地的那一份**，而不是「目录存在」——一个空目录与
/// 一棵真的抄过去的树，在 `is_dir()` 面前是同一件事。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceReport {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectsReport {
    pub files: u64,
    pub bytes: u64,
}

/// 备份包的清单：备份端写下的「这个包该有什么」。
///
/// 它是恢复端**唯一**能拿包内实测去比对的声明——`run_restore` 除了把这几个字段
/// 打进日志，还要拿它逐段核包（见 `verify_package_against_manifest`）。
///
/// 每个内容段都带 `#[serde(default)]`：旧版本打出来的包可能整段没有，那种包
/// 按原设计仍然可恢复（缺段不是错，是当时的格式里没有这一段），所以读不出来时
/// 退成默认值、在核包时按「这一段当年没声明」跳过，而不是拿「没声明」当
/// 「声明为零」去拦一次合法的恢复。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: String,
    pub revision: String,
    pub instance_fingerprint: String,
    pub namespace: String,
    pub created_at: String,
    #[serde(default)]
    pub secrets_keys: Vec<String>,
    #[serde(default)]
    pub postgres: PostgresReport,
    #[serde(default)]
    pub qdrant: Vec<CollectionReport>,
    #[serde(default)]
    pub objects: ObjectsReport,
    /// 每个数据根的产物层。旧包没有这一段，读出来是空表——「当年没声明」不是
    /// 「声明为零」，演练按「没有可读的产物段」跳过，不判失败。
    #[serde(default)]
    pub artifacts: Vec<ArtifactRootReport>,
    /// 出包进程跑的镜像引用（chart 注入的同一个值）。空 = 那个部署没注入，
    /// 不是「没有镜像」。恢复端拿它与自己要跑的那一份比对，不比字符串相等就
    /// 拒绝——这是唯一能把「这个包的字节是哪份代码产出的」落到读数上的字段。
    #[serde(default)]
    pub image: String,
    /// 出包时这份二进制里最新的 PG 迁移号。0 = 没读出来，不是「没有迁移」。
    /// 数据结构只能往前走：恢复端拒绝比它自己更新的包，因为那意味着包里带着
    /// 本二进制没有的列或表。
    #[serde(default)]
    pub schema_version: i64,
    /// 这份包**没有**带走的东西，逐条写清丢了什么、为什么可以丢。清单之外
    /// 的缺失不允许「默认可以接受」——有损面必须是一条条被点名过的。
    #[serde(default)]
    pub lossy_surfaces: Vec<LossySurface>,
    /// 每一段数据被取走的时刻。包是几段在不同时刻取的东西拼起来的：PG 是一致性
    /// 快照，对象层是逐文件拷贝，产物层是逐库读。相隔太久，包里这几段就已经不是
    /// 「同一份数据」了——声明出来，恢复端才有一条能判红的读数。空 = 旧格式，
    /// 当年没声明，不是「相隔 0 秒」。
    #[serde(default)]
    pub segments: Vec<SegmentStamp>,
    /// 出包时允许的跨段偏差，与 `segments` 成对出现。0 = 没声明。
    #[serde(default)]
    pub max_segment_skew_seconds: u64,
    /// 进化 bare 仓的完整历史 bundle。`None` = 这个包没带它（旧包，或出包时仓库
    /// 还没 seed / 打包失败），此时 `lossy_surfaces` 里必须点名这一格。
    #[serde(default)]
    pub evolution_bundle: Option<EvolutionBundleReport>,
}

/// 一段数据被取走的时刻。名字用段的身份（`postgres`／`qdrant`／`objects`／
/// `artifacts`），不用文件路径：恢复端按段名说「是哪两段相隔太久」。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SegmentStamp {
    pub name: String,
    pub at: String,
}

/// 进化 bare 仓以 git bundle 形式进包时的读数。
///
/// 没有这一段（旧包，或出包时仓库还没 seed / 打包失败）＝「这个包没带」；带了就
/// 必须能回答「带了多少、HEAD 在哪」，恢复端才能核包内实测并把它种回去。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EvolutionBundleReport {
    /// bundle 文件字节数；恢复端按它核包内实测。
    pub bytes: u64,
    /// bundle 记录的 ref 数（`git bundle list-heads`，不含 HEAD 那一行）。
    pub refs: u64,
    /// 出包时 bare 仓 HEAD 指向的符号引用（如 `refs/heads/main`）。
    pub head: String,
    /// HEAD 解引用后的提交。
    pub commit: String,
}

/// 一份有损面：命名 + 丢了什么 + 为什么可丢。
///
/// 三个字段都要有值，判据是「读了这一条，能不能回答『换机后少了什么、能不能
/// 接受』」。只写名字的条目等于把问题留给下一个人。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LossySurface {
    pub name: String,
    pub lost: String,
    pub acceptable_because: String,
    /// 换机之后这一面靠谁长回来。恢复端按它决定收尾那句要点谁的名：全部照列
    /// （nats / redis / meilisearch 每次恢复都缺席）会让那句话退化成每次都亮的
    /// 告警；全部不列则「对象层没带」与「什么都带回来了」又是同一句话。
    ///
    /// 老包没有这一格，缺省按 [`SurfaceRecovery::NeedsFollowUp`] 算——没声明的
    /// 面默认要人去看，宁可按错报也不静默放过。
    #[serde(default)]
    pub recovery: SurfaceRecovery,
}

/// 一份有损面换机后的去向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceRecovery {
    /// 平台自己长回来：消费者重连、缓存重建、镜像重拉、从公开远端重取。
    SelfRebuilding,
    /// 得有人另外去做：从别的备份恢复、或按向导重新签发。
    #[default]
    NeedsFollowUp,
}

/// 备份配置。集群 CronJob 全部由 env 注入；测试直接构造。
pub struct BackupConfig {
    pub database_url: String,
    /// Qdrant HTTP 端点（快照 API 只在 REST 6333 上，gRPC 端口不行）。
    pub qdrant_http_url: String,
    pub backup_dir: PathBuf,
    pub secrets_dir: PathBuf,
    /// 带外凭证那一份 Secret 的挂载点（只被安全网关读的键）。两片分开导出，
    /// 恢复时按对象名逐个 apply——合并成一份会让恢复把一个对象写回成两个的
    /// 内容，而权限面正是按对象划的。
    pub oob_secrets_dir: PathBuf,
    /// cogneva-data-pvc 挂载根；对象子树按约定取其下 `storage/`。
    ///
    /// 「按约定」是因为这一侧只拿得到数据根：`providers.storage.options.base_path`
    /// 在应用进程的配置里，备份进程读不到。部署把对象放到别处时（chart 默认的
    /// seaweedfs 有自己那块卷），`storage/` 不存在，包里就不带对象——这件事由
    /// 清单里那张 `objects` 有损面点名，见 [`local_objects_dir`]。
    pub data_dir: PathBuf,
    /// 主数据根之外、也要取产物层的数据根，形如 `evolution=/var/lib/...`。
    ///
    /// 备份 Pod 把进化卷**只读**挂进来，并靠同节点亲和避开 RWO 跨节点 attach
    /// 失败。名字是清单里的身份，不能重复、不能叫 `app`（主根的名字）。
    pub extra_data_roots: Vec<(String, PathBuf)>,
    /// 保留几个包：**覆盖位**（`COGNEVA_BACKUP_KEEP`，构造时 `unwrap_or(7)`）。
    ///
    /// 7 的来历量过、不是拍的：备份卷 `cogneva-backup-pvc` 声明 10 GiB，而
    /// 2026-10-09…10-15 那七个包实测 42–195 MB、合计 788 MB ⇒ 按**最大的那个**
    /// 外推 7 个包 ≈ 1.4 GiB，约占声明容量的 14%。取「一周」是因为 CronJob 一天
    /// 一趟，一周覆盖「发现坏了到修好」的窗口。
    ///
    /// 它**不能**建成闭环：留几个包是策略不是测量，按剩余空间算只会让「留多久的
    /// 账」随磁盘水位漂。生效值由 `cogneva_backup_run_readings{readout="keep_target"}`
    /// 发出去。**缺判据**：那条读数没有 `source` 标签（界读数该标来源），也没有
    /// 东西在包体量长期贴住卷容量时报。
    pub keep_count: usize,
    /// 这一次运行最长能跑多久。集群侧同一个值同时是 Job 的
    /// `activeDeadlineSeconds`（到点由 kubelet 杀容器）和这里的 env——一个值
    /// 两处承载，不许各自写一份。
    ///
    /// 它落进账本那一行的 `run_deadline_seconds`，因为那个数要跟着这一行走：
    /// 一行运行中的记录没人在跑，只有拿**它自己被开出来时**的期限才能判，用
    /// 今天的旋钮去判昨天那一次是两回事。
    pub run_deadline_seconds: i32,
    /// 备份卷之外的第二个落点：每跑完一次，新包再往这里复制一份。
    ///
    /// **它换掉的是什么，写清楚**：备份卷是一块 local-path PV，删除 PVC、卷被
    /// 回收、`helm uninstall` 之后换个名字重装，都会让那个卷上的包一起消失。
    /// 复制到卷外的一条路径上，救的是这些；**不救宿主丢失**——同一台机器上的
    /// 另一条路径和备份卷在同一个故障域里，「跨路径」不等于「跨机器」。真正的
    /// 异地落点要跨机器或对象存储，需要凭证，不属于这个进程能自己闭合的事。
    ///
    /// `None` = 没有配置落点。这时不复制，演练侧也量不到副本，于是「备份卷之外
    /// 有没有一份」这件事会由规则报红，而不是悄悄当作已覆盖。
    pub offsite_dir: Option<PathBuf>,
    /// 进化 bare 仓的挂载点（`gitRemote` 那一卷）。`None` = 这次部署没有进化，
    /// 包里也就不可能有它的 bundle。
    ///
    /// 它不在数据根下，是一个独立的卷：`DATA_ROOT_SUBDIRS` 里 `git-remote` 那一行
    /// 的权威面就是它。已推的 rev 能从公开远端重取，未推的不能——那些才是进化
    /// 自己产出的状态，必须逐字进入包。
    pub git_remote_dir: Option<PathBuf>,
    /// Where a package written on somebody's request is put so it can be handed
    /// back to them. `None` = the nightly run, which nobody is waiting on.
    ///
    /// It is a copy, not a move: the backup directory's retention and the drill
    /// that opens the newest package there both keep working on the run's own
    /// output. Copies here are never pruned by the run -- the caller that asked
    /// for this package decides when it is done with it.
    pub publish_dir: Option<PathBuf>,
    pub namespace: String,
}

impl BackupConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let database_url = std::env::var(cog_core::env::DATABASE_URL)
            .map_err(|_| "COGNEVA_DATABASE_URL is required for backup")?;
        let qdrant_http_url = std::env::var(QDRANT_HTTP_URL_ENV)
            .map_err(|_| "COGNEVA_QDRANT_HTTP_URL is required for backup")?;
        Ok(Self {
            database_url,
            qdrant_http_url,
            backup_dir: PathBuf::from(
                std::env::var(BACKUP_DIR_ENV).unwrap_or_else(|_| "/backups".into()),
            ),
            secrets_dir: PathBuf::from(
                std::env::var("COGNEVA_SECRETS_DIR").unwrap_or_else(|_| "/secrets".into()),
            ),
            oob_secrets_dir: PathBuf::from(
                std::env::var("COGNEVA_OOB_SECRETS_DIR").unwrap_or_else(|_| "/secrets-oob".into()),
            ),
            data_dir: PathBuf::from(cog_core::paths::data_dir()),
            extra_data_roots: parse_extra_data_roots(
                BACKUP_EXTRA_DATA_ROOTS_ENV,
                std::env::var(BACKUP_EXTRA_DATA_ROOTS_ENV).ok(),
            )?,
            keep_count: std::env::var("COGNEVA_BACKUP_KEEP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(7),
            run_deadline_seconds: std::env::var("COGNEVA_BACKUP_RUN_DEADLINE_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_RUN_DEADLINE_SECONDS),
            offsite_dir: std::env::var(BACKUP_OFFSITE_DIR_ENV)
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            git_remote_dir: std::env::var("COGNEVA_BACKUP_GIT_REMOTE_DIR")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            publish_dir: std::env::var(cog_core::env::BACKUP_PUBLISH_DIR)
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            namespace: cog_core::topology::namespace(),
        })
    }

    /// 主根在前、追加根在后。顺序只影响清单的书写顺序，不影响身份。
    pub fn data_roots(&self) -> Vec<(String, PathBuf)> {
        let mut roots = vec![("app".to_string(), self.data_dir.clone())];
        roots.extend(self.extra_data_roots.iter().cloned());
        roots
    }
}

/// 解析 `name=path[,name=path]`。
///
/// 出包端与恢复端各读一个 env，名字只在这里各写一遍。
const BACKUP_EXTRA_DATA_ROOTS_ENV: &str = "COGNEVA_BACKUP_EXTRA_DATA_ROOTS";
const RESTORE_EXTRA_DATA_ROOTS_ENV: &str = "COGNEVA_RESTORE_EXTRA_DATA_ROOTS";

/// 备份与恢复各读一个 env（[`BACKUP_EXTRA_DATA_ROOTS_ENV`] /
/// [`RESTORE_EXTRA_DATA_ROOTS_ENV`]），形状同一条：恢复端面对的是一个包里已经
/// 写下的根名，能不能给出目标路径是另一件事，所以两份表必须用同一个口径解析——
/// 两份各写一遍解析只会在某一天分叉。`var` 只用于让错误点名是哪一份表坏了。
///
/// 坏掉的条目让整次运行**失败**，不跳过：少一个数据根而包照样「成功」，正是换机后
/// 才发现某一份状态从没进过包的形状。空表 = 没有追加根，是合法值。
fn parse_extra_data_roots(
    var: &str,
    raw: Option<String>,
) -> Result<Vec<(String, PathBuf)>, Box<dyn std::error::Error>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mut roots: Vec<(String, PathBuf)> = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (name, path) = part
            .split_once('=')
            .ok_or_else(|| format!("{var}: {part:?} 不是 name=path"))?;
        let name = name.trim();
        let path = path.trim();
        if name.is_empty() || path.is_empty() {
            return Err(format!("{var}: {part:?} 的 name 或 path 为空").into());
        }
        if name == "app" {
            return Err(format!("{var}: `app` 是主数据根的名字，不能重复").into());
        }
        if roots.iter().any(|(n, _)| n == name) {
            return Err(format!("{var}: 根名 {name:?} 重复").into());
        }
        roots.push((name.to_string(), PathBuf::from(path)));
    }
    Ok(roots)
}

pub fn package_name(at: chrono::DateTime<Utc>) -> String {
    format!(
        "{PACKAGE_PREFIX}{}{PACKAGE_SUFFIX}",
        at.format(TIMESTAMP_FORMAT)
    )
}

/// 表名只许小写字母/数字/下划线——标识符要拼进 COPY/TRUNCATE 语句，
/// 白名单校验挡住畸形包里的注入面。
fn validate_table_name(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(format!("unsafe table name in package: {name:?}").into());
    }
    Ok(())
}

/// Columns the package's own header names, in the order the rows carry them.
///
/// `COPY ... FROM STDIN WITH (HEADER true)` skips the header row and then maps
/// the remaining values **by position**, not by name. A package written before
/// a column was added therefore fails against today's schema with
/// `missing data for column "..."` -- which rejects every package older than
/// the newest column, the ordinary case for a restore. Naming the columns the
/// package carries inserts exactly what it has and lets the newer columns take
/// their defaults.
fn csv_columns(header: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let columns: Vec<String> = header
        .trim_end_matches(['\r', '\n'])
        .split(',')
        .map(|column| column.trim_matches('"').to_string())
        .collect();
    for (index, column) in columns.iter().enumerate() {
        // 表头来自包内文件，是外部输入：先按普通标识符白名单判，
        // 不合格就不回显原文（错误信息会进 backup_drills.error 并随备份保存）。
        let mut chars = column.chars();
        let starts_well = matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c == '_');
        let rest_well = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !starts_well || !rest_well {
            return Err(
                format!("column {index} of the package header is not a plain identifier").into(),
            );
        }
    }
    Ok(columns)
}

/// 包里有、当前 schema 里已经没有的列。
///
/// 这类列的数据恢复时无处可放：显式点名会被 Postgres 判未知列，而不点名
/// （按位置对齐）会静默把值错位到别的列上——两种都不能接受。恢复因此要
/// 拒绝整包并把名字报出来，由人决定是找回旧 schema 还是放弃那段数据；
/// 悄悄少恢复一列的话，行数比对完全看不出来。
fn columns_without_a_home<'a>(package: &'a [String], current: &[String]) -> Vec<&'a String> {
    package
        .iter()
        .filter(|column| !current.contains(*column))
        .collect()
}

/// 包里有、当前 schema 里已经没有的表。
///
/// 迁移可以删表，而保留窗口里的包会跨过那一次迁移。删掉的表在新结构里没有名字，
/// 它的行恢复时无处可放；但**拒绝整包**同样不行——同一个包里其余的表此刻正是
/// 要恢复的那批数据，为一张已经作废的表把它们一起退回去，等于让一次删表把整个
/// 保留窗口变成「不可恢复」。所以跳过它们，把名字带回去记账：沉默地跳过是另一
/// 种谎，装回来的表数与行数会全部显得与清单一致。
fn tables_without_a_home<'a>(package: &'a [String], current: &[String]) -> Vec<&'a String> {
    package
        .iter()
        .filter(|table| !current.contains(*table))
        .collect()
}

/// 包里的列中，目标表接受写入的那些：生成列由目标自己算，不在这份名单里。
/// `SELECT setval('name', …)` 里那个序列名。认不出返回 `None`，调用方按原样执行。
///
/// 只认导出端自己写的那种形状（`setval('{seq}', {last_value}, {is_called})`）；
/// 认不出就不猜——猜错的代价是把一句无关的 SQL 当成序列语句跳过。
fn sequence_named_in_setval(line: &str) -> Option<&str> {
    let start = line.find("setval('")? + "setval('".len();
    let rest = &line[start..];
    let end = rest.find('\'')?;
    let name = &rest[..end];
    (!name.is_empty()).then_some(name)
}

fn writable_columns<'a>(package: &'a [String], computed: &[String]) -> Vec<&'a String> {
    package
        .iter()
        .filter(|column| !computed.iter().any(|c| c == *column))
        .collect()
}

/// 把包内这一张表的 CSV 按块直送进 `target`，列由包自己的表头点名。
///
/// 分块流式灌入：整表读进内存会在大表上把容器打 OOM
/// （metrics 类表 CSV 可达数百 MB 并随时间增长）。
async fn copy_package_table_into(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &str,
    path: &Path,
    columns: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::File::open(path)?;
    let mut copy_in = tx
        .copy_in_raw(&copy_from_statement(target, columns))
        .await?;
    use std::io::Read;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        copy_in.send(buf[..n].to_vec()).await?;
    }
    copy_in.finish().await?;
    Ok(())
}

fn copy_from_statement(table: &str, columns: &[String]) -> String {
    let list: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    format!(
        "COPY \"{table}\" ({}) FROM STDIN WITH (FORMAT csv, HEADER true)",
        list.join(", ")
    )
}

/// The columns a package may carry: everything the table stores itself.
///
/// A generated column is refused by `COPY ... FROM STDIN` the moment it is
/// named — `Generated columns cannot be used in COPY.` — so a package that
/// carries one is a package of a whole table that cannot be restored. The value
/// is not lost by leaving it out: PostgreSQL computes it from the other columns
/// on every insert. The header and the rows are written from this same list, so
/// naming the columns on one side and the other stays one decision.
const MOVABLE_COLUMNS_SQL: &str = "SELECT column_name::text FROM information_schema.columns \
     WHERE table_schema = 'public' AND table_name = $1 \
       AND (is_generated IS NULL OR is_generated = 'NEVER') \
     ORDER BY ordinal_position";

/// The columns a table computes rather than stores, which a restore cannot name.
const GENERATED_COLUMNS_SQL: &str = "SELECT column_name::text FROM information_schema.columns \
     WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'ALWAYS'";

fn copy_to_statement(table: &str, columns: &[String]) -> String {
    let list: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    format!(
        "COPY (SELECT {} FROM \"{table}\") TO STDOUT WITH (FORMAT csv, HEADER true)",
        list.join(", ")
    )
}

/// 运行时发现表清单：新表随迁移自动进备份，不维护手工列表。
pub async fn discover_tables(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let tables: Vec<String> = sqlx::query_scalar(DISCOVER_TABLES_SQL)
        .fetch_all(pool)
        .await?;
    Ok(tables)
}

/// PG 逻辑导出：逐表 COPY TO CSV + 全部序列的 setval 语句。
/// 运行中实例的数据目录文件级拷贝不保证一致，逻辑导出是唯一安全形态。
/// 导出一律用 COPY (SELECT …) 形式：PG 不允许 COPY <分区父表> TO，
/// 而 SELECT 形式对普通表与分区父表都成立。列名逐个点出而不是 `SELECT *`：
/// 生成列进得了包、却进不了 `COPY FROM`，一个包只要带上它就整表恢复不了。
pub async fn backup_postgres(
    pool: &PgPool,
    out_dir: &Path,
) -> Result<PostgresReport, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(out_dir)?;
    // 逐表 COPY 只有在同一个一致性快照里才是一份数据：默认 READ COMMITTED
    // 下每张表各看各的时刻，导出期间提交的事务会让包里的表互相矛盾。
    // 表清单也在这条事务里取，避免发现表之后又冒出新表。
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;

    let tables: Vec<String> = sqlx::query_scalar(DISCOVER_TABLES_SQL)
        .fetch_all(&mut *tx)
        .await?;
    let mut report = PostgresReport {
        tables: Vec::with_capacity(tables.len()),
        sequences: 0,
    };

    for table in &tables {
        validate_table_name(table)?;
        let rows: i64 = sqlx::query_scalar(&cog_core::sql::relation_rows_sql(table))
            .fetch_one(&mut *tx)
            .await?;
        let path = out_dir.join(format!("public.{table}.csv"));
        // 只搬表自己存的列：生成列在这里点名会在恢复时被 Postgres 拒掉，
        // 而它的值是别的列算出来的，不搬不丢信息。
        let movable: Vec<String> = sqlx::query_scalar(MOVABLE_COLUMNS_SQL)
            .bind(table)
            .fetch_all(&mut *tx)
            .await?;
        let mut file = std::fs::File::create(&path)?;
        let mut copy_out = tx.copy_out_raw(&copy_to_statement(table, &movable)).await?;
        use futures::StreamExt;
        use std::io::Write;
        while let Some(chunk) = copy_out.next().await {
            file.write_all(&chunk?)?;
        }
        drop(copy_out);
        report.tables.push(TableReport {
            name: table.clone(),
            rows,
        });
    }

    // 序列不属于任何表的数据行，TRUNCATE RESTART IDENTITY 会清零，
    // 必须单独导出当前值，恢复时 setval 回位。
    let sequences: Vec<String> = sqlx::query_scalar(
        "SELECT sequencename::text FROM pg_sequences WHERE schemaname = 'public' \
         ORDER BY sequencename",
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut sql = String::new();
    for seq in &sequences {
        validate_table_name(seq)?;
        let (last_value, is_called): (i64, bool) =
            sqlx::query_as(&format!("SELECT last_value, is_called FROM \"{seq}\""))
                .fetch_one(&mut *tx)
                .await?;
        sql.push_str(&format!(
            "SELECT setval('{seq}', {last_value}, {is_called});\n"
        ));
    }
    std::fs::write(out_dir.join(SEQUENCES_FILE), &sql)?;
    report.sequences = sequences.len();
    // 出错时 tx 在 drop 时回滚；成功路径显式提交，把快照释放掉。
    tx.commit().await?;
    Ok(report)
}

/// 恢复做了什么。
///
/// `tables` 是装回去的表；`recomputed` 是包里有值、装载时却由目标自己算回来的
/// 列，记作 `表.列`；`tables_without_a_home` 是包里有、当前 schema 已经没有的
/// 表。后两者都不进成败判决——生成列的值由同一行的其它列算出，不搬不丢数据；
/// 被迁移删掉的表在新结构里没有任何读者——但都必须被写下来：生成表达式一旦改动，
/// 旧包会被装成「当时不曾有过」的值，而表数、行数、源库比对三项全都照旧是全绿；
/// 被跳过的表同理，若不留名字，一次删表会让旧包读起来像「完整恢复了」。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreReport {
    pub tables: Vec<String>,
    pub recomputed: Vec<String>,
    pub tables_without_a_home: Vec<String>,
    /// 包里的 `setval` 指向、而目标结构已经没有的序列。它们与被跳过的表是同一件事
    /// 的两种落点：表没了，它的序列也一起被迁移删掉；包里的 `sequences.sql` 却还
    /// 按旧导出点名它。跳过它必须留名字——静默跳过会让「装回去的序列数」看起来与
    /// 清单一致，而实际上少的那一条正是被删表的自增计数器。
    pub sequences_without_a_home: Vec<String>,
}

/// PG 恢复：整包表一次性 TRUNCATE ... RESTART IDENTITY CASCADE，
/// 同一事务内关掉触发器（含外键检查）逐表 COPY FROM，最后序列归位。
/// 目标必须是跑完迁移的库（fresh install 天然是）；session_replication_role
/// 需要超级用户，集群的 POSTGRES_USER 与 CI 的 postgres 都满足。
pub async fn restore_postgres(
    pool: &PgPool,
    pkg_pg_dir: &Path,
) -> Result<RestoreReport, Box<dyn std::error::Error>> {
    let mut tables = Vec::new();
    let mut recomputed = Vec::new();
    for entry in std::fs::read_dir(pkg_pg_dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix("public.") {
            if let Some(table) = rest.strip_suffix(".csv") {
                validate_table_name(table)?;
                tables.push(table.to_string());
            }
        }
    }
    tables.sort_unstable();

    // 包里这批表在当前 schema 里还剩几张：一次删表的迁移会把保留窗口里的旧包
    // 推到「装不回去」的那一侧，而它丢掉的只是已经没人读的那张表。整包拒绝等于
    // 让那一次迁移顺手作废整个窗口，所以按名字跳过并带回去记账；名字是必须的，
    // 否则跳过之后表数与行数会与清单完全一致，读起来像一份完整的恢复。
    let current_tables: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND c.relname = ANY($1)",
    )
    .bind(&tables)
    .fetch_all(pool)
    .await?;
    let tables_without_a_home: Vec<String> = tables_without_a_home(&tables, &current_tables)
        .into_iter()
        .cloned()
        .collect();
    if !tables_without_a_home.is_empty() {
        tracing::warn!(
            tables = %tables_without_a_home.join(","),
            "restore: package carries tables the current schema no longer has; their rows are not restored"
        );
        tables.retain(|table| !tables_without_a_home.contains(table));
    }

    if !tables.is_empty() {
        let mut tx = pool.begin().await?;
        sqlx::query("SET LOCAL session_replication_role = 'replica'")
            .execute(&mut *tx)
            .await?;
        let quoted: Vec<String> = tables.iter().map(|t| format!("\"{t}\"")).collect();
        sqlx::query(&format!(
            "TRUNCATE {} RESTART IDENTITY CASCADE",
            quoted.join(", ")
        ))
        .execute(&mut *tx)
        .await?;
        for table in &tables {
            // 分块流式灌入：整表读进内存会在大表上把容器打 OOM
            // （metrics 类表 CSV 可达数百 MB 并随时间增长）。
            let path = pkg_pg_dir.join(format!("public.{table}.csv"));
            let columns = csv_columns(&{
                use std::io::BufRead;
                let mut header = String::new();
                std::io::BufReader::new(std::fs::File::open(&path)?).read_line(&mut header)?;
                header
            })?;
            let current: Vec<String> = sqlx::query_scalar(
                "SELECT column_name::text FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = $1",
            )
            .bind(table)
            .fetch_all(&mut *tx)
            .await?;
            let vanished = columns_without_a_home(&columns, &current);
            if !vanished.is_empty() {
                return Err(format!(
                    "package for {table} carries columns the current schema no longer has: {}",
                    vanished
                        .iter()
                        .map(|c| c.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .into());
            }
            // 生成列在包里有、在目标表里也有，`columns_without_a_home` 因此判它
            // 「有家」；但它的家不接受外人：`COPY FROM` 一点名就整表失败。包是旧
            // 导出写的（那时导出还带 `SELECT *`），而这类列的值由同行其它列算出，
            // 不搬并不丢数据：把包按原样灌进一张 `LIKE` 目标表的临时表（`LIKE`
            // 默认不复制生成表达式，生成列在那里退化成普通列），再把非生成列搬进
            // 目标表。直接 COPY 会少一遍写入，但那份 CSV 的行里夹着生成列的值，
            // 按块直送的形状改不动它——拒绝整包则等于让保留窗口里的每一份旧包都
            // 不可恢复。
            let computed: Vec<String> = sqlx::query_scalar(GENERATED_COLUMNS_SQL)
                .bind(table)
                .fetch_all(&mut *tx)
                .await?;
            let writable = writable_columns(&columns, &computed);
            if writable.len() == columns.len() {
                copy_package_table_into(&mut tx, table, &path, &columns).await?;
            } else {
                recomputed.extend(
                    computed
                        .iter()
                        .filter(|column| columns.iter().any(|c| c == *column))
                        .map(|column| format!("{table}.{column}")),
                );
                // 临时表固定名字足够：逐表用、用完即丢，同一时刻只有一张。
                const STAGING: &str = "_drill_stage";
                sqlx::query(&format!(
                    "CREATE TEMP TABLE \"{STAGING}\" (LIKE public.\"{table}\")"
                ))
                .execute(&mut *tx)
                .await?;
                copy_package_table_into(&mut tx, STAGING, &path, &columns).await?;
                let list: Vec<String> = writable.iter().map(|c| format!("\"{c}\"")).collect();
                // `OVERRIDING SYSTEM VALUE` 保住 identity 列的包内取值；没有
                // identity 的表上它也合法。
                sqlx::query(&format!(
                    "INSERT INTO public.\"{table}\" ({list}) OVERRIDING SYSTEM VALUE \
                     SELECT {list} FROM \"{STAGING}\"",
                    list = list.join(", ")
                ))
                .execute(&mut *tx)
                .await?;
                sqlx::query(&format!("DROP TABLE \"{STAGING}\""))
                    .execute(&mut *tx)
                    .await?;
            }
        }
        tx.commit().await?;
    }

    let seq_path = pkg_pg_dir.join(SEQUENCES_FILE);
    let mut sequences_without_a_home = Vec::new();
    if seq_path.exists() {
        let sql = std::fs::read_to_string(seq_path)?;
        // 序列跟着表一起被迁移删掉，而 `sequences.sql` 是按出包那一刻的
        // `pg_sequences` 写的：里面还留着已删表的自增计数器。整句执行会在
        // 「关系不存在」上失败，把整个包判成不可恢复——它丢掉的只是一条已经没有
        // 列在用的计数器。所以先问目标哪些序列还在，再只跑活着的那些。
        let mut names = Vec::new();
        for line in sql.lines().filter(|l| !l.trim().is_empty()) {
            if let Some(name) = sequence_named_in_setval(line) {
                names.push(name.to_string());
            }
        }
        let living: Vec<String> = if names.is_empty() {
            Vec::new()
        } else {
            sqlx::query_scalar(
                "SELECT c.relname::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'public' AND c.relkind = 'S' AND c.relname = ANY($1)",
            )
            .bind(&names)
            .fetch_all(pool)
            .await?
        };
        for line in sql.lines().filter(|l| !l.trim().is_empty()) {
            match sequence_named_in_setval(line) {
                Some(name) if !living.iter().any(|l| l == name) => {
                    sequences_without_a_home.push(name.to_string());
                }
                _ => {
                    sqlx::query(line).execute(pool).await?;
                }
            }
        }
        if !sequences_without_a_home.is_empty() {
            tracing::warn!(
                sequences = %sequences_without_a_home.join(","),
                "restore: package carries sequences the current schema no longer has; their values are not restored"
            );
        }
    }
    Ok(RestoreReport {
        tables,
        recomputed,
        tables_without_a_home,
        sequences_without_a_home,
    })
}

/// Secret 卷（只读挂载，key 一文件）打成 apply-able manifest。
/// BTreeMap 让 key 序确定，包内容可 diff。
///
/// The object name is a parameter because the instance keeps two of them: the
/// business-readable one and the out-of-band one only the gateway reads. A
/// single manifest would have to invent a merge that the cluster does not have,
/// and the restore side applies whole objects — so the packaging keeps the same
/// two objects it read.
pub fn build_secret_manifest(
    secrets_dir: &Path,
    namespace: &str,
    object_name: &str,
) -> Result<(String, Vec<String>), Box<dyn std::error::Error>> {
    let mut data = BTreeMap::new();
    for entry in std::fs::read_dir(secrets_dir)? {
        let entry = entry?;
        let path = entry.path();
        // K8s Secret 卷的每个 key 都是指向 ..data/<key> 的符号链接，is_file
        // 跟随链接为真；要排除的是 ..data / ..2024_xxx 这类点开头的内部条目，
        // 不是链接本身。
        if !path.is_file() {
            continue;
        }
        let key = entry.file_name().to_string_lossy().into_owned();
        if key.starts_with('.') {
            continue;
        }
        let value = std::fs::read(&path)?;
        data.insert(key, base64::engine::general_purpose::STANDARD.encode(value));
    }
    if data.is_empty() {
        return Err(format!("no secret keys found under {}", secrets_dir.display()).into());
    }
    let keys: Vec<String> = data.keys().cloned().collect();
    let mut yaml = format!(
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: {object_name}\n  \
         namespace: {namespace}\ntype: Opaque\ndata:\n"
    );
    for (k, v) in &data {
        yaml.push_str(&format!("  {k}: {v}\n"));
    }
    Ok((yaml, keys))
}

/// 把两片 Secret 卷导出成 apply-able 清单，返回进包键名的全表。
///
/// **只在包本身是密文时才导**。包里的 Secret 清单只是 base64，能读文件就等于持有
/// 全部凭证：卷上一份、卷外副本一份、宿主状态目录一份，0600 挡住同机别的账号，挡不住
/// 拿到盘的人，也挡不住把 PVC 快照拷走的人。没口令时把凭证写进明文包，等于把「凭证
/// 只住在安全网关里」这句话当场作废——那两份清单躺的每一个位置都是新的凭证载体。
///
/// 不导的代价要写清楚，所以它不是静默跳过：manifest 的 `lossy_surfaces` 会点名这一格
/// （换机后内部那片由安装脚本重新生成、带外那片由向导重新签发）。拦整趟备份换不到更好
/// 的东西：没凭证的包仍然救得回全部业务状态。
fn export_secret_manifests(
    internal_dir: &Path,
    oob_dir: &Path,
    namespace: &str,
    out_dir: &Path,
    sealed: bool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if !sealed {
        tracing::warn!(
            env = crate::backup_crypto::KEY_ENV,
            "没有配置包级加密口令：这一份包以明文落盘，两片 Secret 清单**不进包**"
        );
        return Ok(Vec::new());
    }
    std::fs::create_dir_all(out_dir)?;
    // 两片各出一份 apply-able manifest。键名带上对象前缀：只列裸键名的话，
    // 包清单读起来像"这些键都在一个对象里"，而恢复端是按对象 apply 的——
    // 那句误读会在换机时才暴露。
    let mut keys = Vec::new();
    for (dir, object_name) in [
        (internal_dir, SECRET_NAME_INTERNAL),
        (oob_dir, SECRET_NAME_OOB),
    ] {
        let (yaml, object_keys) = match build_secret_manifest(dir, namespace, object_name) {
            Ok(v) => v,
            // 带外那份可能还没被创建过（全新安装、向导还没跑）：那不是备份失败，
            // 包里少一份空对象而已。内部那份缺失是真问题（含实例指纹）。
            Err(e) if object_name == SECRET_NAME_OOB => {
                tracing::warn!(dir = %dir.display(), error = %e, "备份：带外凭证对象读不到，跳过该片");
                continue;
            }
            Err(e) => return Err(e),
        };
        let secret_file = out_dir.join(format!("{object_name}.yaml"));
        std::fs::write(&secret_file, yaml)?;
        // 打包沿用的是这个文件的权限，所以这里定成 0600：解包出来的那份清单
        // 也就不会对别人可读，而不是只靠外面那层目录挡着。
        restrict_to_owner(&secret_file, 0o600)?;
        keys.extend(
            object_keys
                .into_iter()
                .map(|k| format!("{object_name}/{k}")),
        );
    }
    Ok(keys)
}

/// 递归复制目录树，返回（文件数， 总字节）。源不存在时记 0——新装实例
/// 可能还没有任何 raw 对象，这不是错误。
pub fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<(u64, u64)> {
    if !src.exists() {
        return Ok((0, 0));
    }
    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut stack = vec![(src.to_path_buf(), dst.to_path_buf())];
    while let Some((from, to)) = stack.pop() {
        std::fs::create_dir_all(&to)?;
        for entry in std::fs::read_dir(&from)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            let target = to.join(entry.file_name());
            if ty.is_dir() {
                stack.push((entry.path(), target));
            } else if ty.is_file() {
                bytes += entry.metadata()?.len();
                std::fs::copy(entry.path(), target)?;
                files += 1;
            }
        }
    }
    Ok((files, bytes))
}

struct QdrantHttp {
    client: reqwest::Client,
    base: String,
}

impl QdrantHttp {
    fn new(base: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base: base.trim_end_matches('/').to_string(),
        }
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!("qdrant GET {path} -> {}", resp.status()).into());
        }
        Ok(resp.json().await?)
    }

    async fn collection_names(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let body = self.get_json("/collections").await?;
        let names = body["result"]["collections"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|c| c["name"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        Ok(names)
    }

    async fn points_count(&self, collection: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let body = self.get_json(&format!("/collections/{collection}")).await?;
        Ok(body["result"]["points_count"].as_u64().unwrap_or(0))
    }

    /// 服务端一致性快照：创建（wait=true 阻塞到落盘）→ 下载 → 删服务端副本
    /// （快照累积占磁盘，下载成功后服务端那份没有保留价值）。
    async fn snapshot_collection(
        &self,
        collection: &str,
        out_dir: &Path,
    ) -> Result<CollectionReport, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .post(format!(
                "{}/collections/{collection}/snapshots?wait=true",
                self.base
            ))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!("qdrant snapshot {collection} -> {}", resp.status()).into());
        }
        let body: serde_json::Value = resp.json().await?;
        let snap_name = body["result"]["name"]
            .as_str()
            .ok_or("qdrant snapshot response missing result.name")?
            .to_string();

        let resp = self
            .client
            .get(format!(
                "{}/collections/{collection}/snapshots/{snap_name}",
                self.base
            ))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!("qdrant download {snap_name} -> {}", resp.status()).into());
        }
        let bytes = resp.bytes().await?;
        std::fs::write(out_dir.join(format!("{collection}.snapshot")), &bytes)?;

        let resp = self
            .client
            .delete(format!(
                "{}/collections/{collection}/snapshots/{snap_name}",
                self.base
            ))
            .send()
            .await?;
        if !resp.status().is_success() {
            tracing::warn!(
                collection,
                snap_name,
                status = %resp.status(),
                "qdrant server-side snapshot cleanup failed"
            );
        }

        Ok(CollectionReport {
            name: collection.to_string(),
            snapshot: snap_name,
            points: self.points_count(collection).await.unwrap_or(0),
        })
    }

    async fn upload_snapshot(
        &self,
        collection: &str,
        file: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bytes = std::fs::read(file)?;
        let part =
            reqwest::multipart::Part::bytes(bytes).file_name(format!("{collection}.snapshot"));
        let form = reqwest::multipart::Form::new().part("snapshot", part);
        let resp = self
            .client
            .post(format!(
                "{}/collections/{collection}/snapshots/upload?priority=snapshot",
                self.base
            ))
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("qdrant upload {collection} -> {status}: {body}").into());
        }
        Ok(())
    }
}

/// 目录里现存的备份包，按包名排序即按时间排序。
///
/// 单独一个函数是因为它有两个读者：保留策略要按它裁，账要按它报「留下了几个、
/// 最老的那个是哪一刻」。两处各自 `read_dir` 一遍会走出两个目录快照，而它们
/// 说的是同一件事。
pub fn packages_in(dir: &Path) -> Vec<PathBuf> {
    let mut packages: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(PACKAGE_PREFIX) && n.ends_with(PACKAGE_SUFFIX))
            })
            .collect(),
        Err(_) => return Vec::new(),
    };
    packages.sort();
    packages
}

/// 一个装着凭证材料的目录，有多少条权限违例。
///
/// 判据只有两条，都取「恰为」而不是「至少」：目录必须恰为 0700，顶层的
/// `*.tar.zst` 必须恰为 0600。多一位组或其他人的权限，就意味着一份装着整库
/// 内容的包能被别的账号读走——那不是修饰差异，那是没有钥匙也读得到。
///
/// 计数按条目数：目录本身不合记一条，每个不合的包文件各记一条。子目录不递归，
/// 旧包里的 tar 条目不打开——这条读数量的是落盘的现场，不是包里的历史。
///
/// 返回 `None` 是三件不同的事：目录不存在（这个安装没有这个落点）、不是目录、
/// 或者列不出来。三种都不该印成 0——0 说的是「看了，干净」。
fn credential_violations_in(dir: &Path) -> Option<i32> {
    let meta = match std::fs::symlink_metadata(dir) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(path = %dir.display(), error = %e, "凭证目录 stat 不出来，这一类按没量过记");
            return None;
        }
    };
    use std::os::unix::fs::PermissionsExt;
    if !meta.is_dir() {
        tracing::warn!(path = %dir.display(), "凭证目录的位置上不是目录，这一类按没量过记");
        return None;
    }

    let mut violations = i32::from(meta.permissions().mode() & 0o777 != 0o700);
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(path = %dir.display(), error = %e, "凭证目录列不出来，这一类按没量过记");
            return None;
        }
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let name_is_package = entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.ends_with(PACKAGE_SUFFIX));
        if !name_is_package {
            continue;
        }
        let Ok(file_meta) = entry.metadata() else {
            continue;
        };
        if !file_meta.is_file() {
            continue;
        }
        if file_meta.permissions().mode() & 0o777 != 0o600 {
            violations += 1;
        }
    }
    Some(violations)
}

/// 卷上这些包里，有多少份在卷外目录里没有同名的那一份。
///
/// 「卷外那份是不是最新」只问一份文件，副本可以完全跟得上而窗口只覆盖了一半：
/// 复制是在更早的几份写出来之后才打开的，那些包从头到尾只有一份。这里数的是
/// 窗口而不是一份，两个读数的读者与修法都不同。
fn missing_from_window(volume: &[PathBuf], copies: &[PathBuf]) -> i32 {
    let names: std::collections::HashSet<&str> = copies
        .iter()
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()))
        .collect();
    volume
        .iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_none_or(|n| !names.contains(n))
        })
        .count() as i32
}

/// 包名里的时刻。保留策略靠「名字字典序即时间序」，账上要判「最老的包有多老」
/// 也只能从这个时刻来：mtime 会被拷贝、rsync、恢复 drill 改写，而包名就是产生
/// 它的那次运行写在行上的 `started_at` 的来源——两边同一个钟读出来的数才可比。
pub fn package_instant(name: &str) -> Option<chrono::DateTime<Utc>> {
    let stem = name
        .strip_prefix(PACKAGE_PREFIX)?
        .strip_suffix(PACKAGE_SUFFIX)?;
    chrono::NaiveDateTime::parse_from_str(stem, TIMESTAMP_FORMAT)
        .ok()
        .map(|naive| naive.and_utc())
}

/// 保留策略：包名时间戳字典序即新旧序，留最新 N 份。删除旧包失败只告警——
/// 保留失败绝不能反过来弄丢新包。
pub fn enforce_retention(dir: &Path, keep: usize) -> Vec<PathBuf> {
    let mut packages = packages_in(dir);
    let mut deleted = Vec::new();
    while packages.len() > keep {
        let victim = packages.remove(0);
        match std::fs::remove_file(&victim) {
            Ok(()) => deleted.push(victim),
            Err(e) => {
                tracing::warn!(path = %victim.display(), error = %e, "retention delete failed");
                break;
            }
        }
    }
    deleted
}

/// 包里带着 Secret 对象（恢复时要 apply 回去），而它们只是 base64，等于明文。
/// 于是凡是装这些字节的路径，从落盘那一刻起就只对属主可见：默认 umask 给的是
/// 0644，宿主机上任何用户都能把整份凭证读走，事后再补加密也补不回这一次泄漏。
#[cfg(unix)]
fn restrict_to_owner(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn restrict_to_owner(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}

/// 打包主体。口令从参数进来而不是在函数里读环境变量：同一趟里「凭证要不要进包」
/// 与「包是不是密文」必须由同一个答案决定，而两次读环境变量之间插进来的差异，会让
/// 凭证躺进一个明文包里；测试也才能在不改进程环境的前提下同时验证两条路。
fn tar_package_with_key(
    staging: &Path,
    staging_name: &str,
    out: &Path,
    key: Option<&crate::backup_crypto::PackageKey>,
) -> std::io::Result<()> {
    let tmp = out.with_extension("tmp");
    {
        let file = std::fs::File::create(&tmp)?;
        // 临时名也可能从上一趟没清掉的残留来，所以不放宽成「只在新建时设」。
        restrict_to_owner(&tmp, 0o600)?;
        match key {
            Some(key) => {
                // 链条：tar → zstd → 加密帧 → 文件。加密放在压缩之后，压缩率不受影响；
                // 放在 zstd 之前会让密文不可压，包会大出一大截。
                let sealing = key.seal(file)?;
                let mut encoder = zstd::Encoder::new(sealing, 3)?;
                encoder.include_checksum(true)?;
                let mut builder = tar::Builder::new(encoder);
                builder.append_dir_all(staging_name, staging)?;
                let encoder = builder.into_inner()?;
                let sealing = encoder.finish()?;
                sealing.finish()?;
            }
            None => {
                let mut encoder = zstd::Encoder::new(file, 3)?;
                // zstd 默认不写内容校验和（frame header 的 `Content_Checksum_flag` 为 0），
                // 于是 `zstd -t` 只解不校，一个位数翻转的文件仍会被判通过。打开它，让
                // 压缩帧自带的 xxhash 在校验时被真正算到——只对**这行之后**打出来的包生效，
                // 已经落盘的旧包没有校验和，改变不了，也不该假装它们有。
                encoder.include_checksum(true)?;
                let mut builder = tar::Builder::new(encoder);
                builder.append_dir_all(staging_name, staging)?;
                let encoder = builder.into_inner()?;
                encoder.finish()?;
            }
        }
    }
    std::fs::rename(&tmp, out)
}

fn untar_package(pkg: &Path, dest: &Path) -> std::io::Result<PathBuf> {
    let key = crate::backup_crypto::PackageKey::from_env();
    untar_package_with_key(pkg, dest, key.as_ref())
}

/// 解包主体。新旧两种包靠文件头分辨：加密包必须有钥匙，旧包在轮换窗口里照常能解。
/// 「分不出来就当旧包」不成立——那会把一个解密失败伪装成 zstd 损坏。
fn untar_package_with_key(
    pkg: &Path,
    dest: &Path,
    key: Option<&crate::backup_crypto::PackageKey>,
) -> std::io::Result<PathBuf> {
    let encrypted = crate::backup_crypto::package_is_encrypted(pkg)?;
    let file = std::fs::File::open(pkg)?;
    let reader: Box<dyn std::io::Read> = if encrypted {
        let key = key.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} 是加密包，但没有 {}：没有口令解不开",
                    pkg.display(),
                    crate::backup_crypto::KEY_ENV
                ),
            )
        })?;
        Box::new(key.open(file)?)
    } else {
        Box::new(file)
    };
    let decoder = zstd::Decoder::new(reader)?;
    let mut archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(dest)?;
    // 解出来的树里有 Secret 清单，包内的条目权限沿用了打包时的 0644；
    // 把根目录收成 0700，让别人连往下走的路径都没有。
    restrict_to_owner(dest, 0o700)?;
    archive.unpack(dest)?;
    let name = pkg
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(PACKAGE_SUFFIX))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("package name must end with {PACKAGE_SUFFIX}"),
            )
        })?;
    Ok(dest.join(name))
}

/// 一次运行收尾时留下的东西，就是要记进账的那几列。它们一起算、一起交出去，
/// 免得「留下了几个」和「最老的是哪一刻」分别去读两次目录。
struct LeftBehind {
    package: PathBuf,
    bytes: Option<i64>,
    checksum: Option<String>,
    coverage: String,
    kept_after: Option<i32>,
    oldest_kept_at: Option<chrono::DateTime<Utc>>,
}

/// 这一次运行跑完之后，目录里还剩什么。两个数出自同一次枚举。
fn retained_state(dir: &Path) -> (Option<i32>, Option<chrono::DateTime<Utc>>) {
    let packages = packages_in(dir);
    let oldest = packages
        .first()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .and_then(package_instant);
    (Some(packages.len() as i32), oldest)
}

/// 账本库的连接。连不上就返回 None，**绝不因此判备份失败**。
///
/// 理由是不对称的：一次已经跑完的备份，被「记录写不进去」判失败，等于拿被保护
/// 的东西换了那张说明事情的纸。所以这一路的每个失败都只留一条 warn，而 warn 是
/// 有声音的——账上少一行这件事必须在日志里看得见，否则「没写」和「没跑」同形。
async fn open_run_store(cfg: &BackupConfig) -> Option<PostgresBackupRunStore> {
    match PgPool::connect(&cfg.database_url).await {
        Ok(pool) => Some(PostgresBackupRunStore::new(pool)),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "备份：账本库连不上，这一次运行不会在账上留痕（备份本身继续）"
            );
            None
        }
    }
}

/// 落库前把错误文本洗干净：常见的密钥形态交给公共脱敏，连接串里的口令另外按
/// 值整段换掉——错误多半来自 PG 客户端，而它会把整条 URL 连同口令一起抄进
/// 错误消息里，行一旦落库，口令就跟着备份包走到恢复端去了。
fn recordable_error(err: &dyn std::error::Error, cfg: &BackupConfig) -> String {
    scrub_error(&err.to_string(), &cfg.database_url)
}

/// 连接串里的口令，取「scheme://user:password@host」的这一段。取不到就没有要
/// 换的东西——那也说明这条 URL 里本来就没有口令。
fn url_password(url: &str) -> Option<String> {
    let authority = url.split_once("://")?.1.split(['/', '?']).next()?;
    let userinfo = authority.rsplit_once('@')?.0;
    let password = userinfo.split_once(':')?.1;
    (!password.is_empty()).then(|| password.to_string())
}

/// 把开口与收尾都记进账，然后按结局返回。
///
/// 开口在任何工作之前写：这个进程可能活不到收尾（OOM／驱逐／被 kill），而那一行
/// 的开口就是它来过的唯一见证——只记终局的表恰恰在最需要它的时候是空的。
async fn record_run(
    store: Option<&PostgresBackupRunStore>,
    run_id: &str,
    closing: BackupRunClosing,
) {
    let Some(store) = store else {
        return;
    };
    match store.close_run(run_id, &closing).await {
        Ok(true) => {}
        Ok(false) => tracing::warn!(
            run = %run_id,
            "备份：账上没有这一笔还开着的运行，收尾没写进去（开工那行要么没落，要么已经结过）"
        ),
        Err(e) => tracing::warn!(error = %e, run = %run_id, "备份：收尾那一行没写进去"),
    }
}

/// 上一次演练量到的凭证面是否已经拦得住下一份备份。
///
/// 这条判据是**读者侧的**：读数由演练写进账本，写者在这里读它，读到开放权限
/// 就拒绝再往这个面上写一份新包。理由不是「演练说这份包装不回去」——那是另一
/// 个结论——而是：一个别人读得到的目录里再落一份装着整库内容的包，等于把暴露
/// 面继续做大。修复方式在返回的话里写清楚：收紧权限，跑一次演练让读数归零，
/// 下一晚的备份照常。
///
/// `None`（没有演练行、或三列都为空）不拦：没有量过不是量到干净，但也不能拿
/// 「没量过」当「有违例」去停掉备份——拦的依据必须是量到的数。
fn credential_surface_refusal(drill: Option<&cog_core::BackupDrillRecord>) -> Option<String> {
    let drill = drill?;
    let counts = [
        ("备份卷", drill.credential_violations_backup_pvc),
        ("卷外副本", drill.credential_violations_offsite),
        ("状态目录", drill.credential_violations_state_dir),
    ];
    let open: Vec<(&str, i32)> = counts
        .into_iter()
        .filter_map(|(name, count)| match count {
            Some(count) if count > 0 => Some((name, count)),
            _ => None,
        })
        .collect();
    if open.is_empty() {
        return None;
    }
    let detail = open
        .iter()
        .map(|(name, count)| format!("{name} {count} 条"))
        .collect::<Vec<_>>()
        .join("，");
    Some(format!(
        "凭证面有开放权限（上一次演练 {} 量到：{detail}），拒绝再写一份包：先用 0700 收紧目录、\
         用 0600 收紧顶层 *.tar.zst，再跑一次 backup-drill 让读数归零，下一晚的备份会照常进行",
        drill.drill_id
    ))
}

pub async fn run_backup(cfg: &BackupConfig) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&cfg.backup_dir)?;
    let now = Utc::now();
    let staging_name = package_name(now)
        .trim_end_matches(PACKAGE_SUFFIX)
        .to_string();
    let staging = cfg.backup_dir.join(&staging_name);
    std::fs::create_dir_all(&staging)?;
    // staging 是**打包之前**的明文树，里面有 Secret 清单；一趟跑被中途杀掉时
    // 它不会被清理，会一直躺在卷上。收成 0700 让外人连目录都进不去。
    restrict_to_owner(&staging, 0o700)?;

    let run_store = open_run_store(cfg).await;
    if let Some(store) = &run_store {
        match store
            .open_run(&BackupRunOpening {
                run_id: staging_name.clone(),
                started_at: now,
                keep_target: cfg.keep_count as i32,
                run_deadline_seconds: cfg.run_deadline_seconds,
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                run = %staging_name,
                "备份：这个标识已经在账上了，开工的行保持原样——同一个标识不会开两次"
            ),
            Err(e) => tracing::warn!(error = %e, "备份：开工那一行没写进去"),
        }

        // 读者侧的执行点：上一次演练量到的凭证面有开放权限，这一趟不写。
        // 读不出来（库不可达）不拦：那一趟本来就会在开工行上失败，拿一个读不
        // 到的读数去停备份，是把「不知道」当成了「有违例」。
        let refusal = match store.read_latest_drill().await {
            Ok(latest) => credential_surface_refusal(latest.as_ref()),
            Err(e) => {
                tracing::warn!(error = %e, "备份：上一次演练的读数读不出来，权限这一面这一趟不拦");
                None
            }
        };
        if let Some(reason) = refusal {
            tracing::error!(run = %staging_name, reason = %reason, "备份被凭证面拦住");
            let _ = std::fs::remove_dir_all(&staging);
            record_run(
                run_store.as_ref(),
                &staging_name,
                BackupRunClosing {
                    outcome: BackupOutcome::Failed,
                    finished_at: Utc::now(),
                    package: None,
                    bytes: None,
                    package_checksum: None,
                    package_coverage: None,
                    kept_after: None,
                    oldest_kept_at: None,
                    error: Some(reason.clone()),
                },
            )
            .await;
            return Err(reason.into());
        }
    }

    match finish_backup(cfg, now, &staging, &staging_name).await {
        Ok(left) => {
            record_run(
                run_store.as_ref(),
                &staging_name,
                BackupRunClosing {
                    outcome: BackupOutcome::Ok,
                    // 收尾的时刻在包写完、保留裁完之后取：先取的钟会让「结」早于
                    // 「产」，而这两列的差就是这次运行花了多久。
                    finished_at: Utc::now(),
                    package: left
                        .package
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(str::to_string),
                    bytes: left.bytes,
                    package_checksum: left.checksum,
                    package_coverage: Some(left.coverage),
                    kept_after: left.kept_after,
                    oldest_kept_at: left.oldest_kept_at,
                    error: None,
                },
            )
            .await;
            Ok(left.package)
        }
        Err(e) => {
            record_run(
                run_store.as_ref(),
                &staging_name,
                BackupRunClosing {
                    outcome: BackupOutcome::Failed,
                    finished_at: Utc::now(),
                    package: None,
                    bytes: None,
                    package_checksum: None,
                    package_coverage: None,
                    kept_after: None,
                    oldest_kept_at: None,
                    error: Some(recordable_error(e.as_ref(), cfg)),
                },
            )
            .await;
            Err(e)
        }
    }
}

/// 把刚打好的包再复制一份到卷外，并按同样的份数裁掉旧的。
///
/// 复制走临时名再改名：演练是**遍历目录**找最新包的，一个写到一半的包出现在
/// 目录里会被它读成「最新那份」，于是下一次演练证明的是半份文件。临时名的后缀
/// 不匹配包名形状，遍历看不到它。
///
/// 复制失败只留日志，不改这次运行的结局：卷上那份是完整的，第二份没落成是第二
/// 份自己的事——它由演练那一跑的读数报出来，不让它把一次成功的备份改判成失败。
fn copy_offsite(dir: &Path, package: &Path, keep: usize) {
    let Some(name) = package.file_name() else {
        tracing::warn!(package = %package.display(), "offsite: 包没有文件名，无法复制");
        return;
    };
    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::warn!(dir = %dir.display(), error = %e, "offsite: 落点建不出来，第二份没写");
        return;
    }
    let dest = dir.join(name);
    let partial = dir.join(format!("{}.partial", name.to_string_lossy()));
    let copied = std::fs::copy(package, &partial).and_then(|bytes| {
        // 卷外这一份离开 PVC 之后照样装着 Secret 清单：复制件也必须只对属主可见，
        // 不能指望源文件的权限被原样带过来。
        restrict_to_owner(&partial, 0o600)?;
        let want = std::fs::metadata(package)?.len();
        if bytes != want {
            return Err(std::io::Error::other(format!(
                "复制了 {bytes} 字节，源是 {want} 字节"
            )));
        }
        std::fs::rename(&partial, &dest)
    });
    match copied {
        Ok(()) => {}
        Err(e) => {
            let _ = std::fs::remove_file(&partial);
            tracing::warn!(
                package = %package.display(),
                dir = %dir.display(),
                error = %e,
                "offsite: 第二份没写进去"
            );
            return;
        }
    }
    let pruned = enforce_retention(dir, keep);
    tracing::info!(
        package = %dest.display(),
        pruned = pruned.len(),
        "offsite copy complete"
    );
}

/// 把保留窗口里缺的卷外副本补上，返回补成了几份。
///
/// 上面那份只复制「这一跑刚打出来的包」，于是写的那一刻没落成的那份以后就没有机会
/// 补：盘满、卷外挂载不在、或者在复制打开之前就已经写下的包。演练的
/// `offsite_missing_packages` 会在整个保留窗口里一直读到那个缺口，而窗口一过，源也没有
/// 了——缺口就永久留在账上，只有人能去宿主上手工补。所以每一跑都顺手把缺口补掉：
/// 判据与演练是同一个（按名字比窗口里的包），补的就是演练会数的那几份。
///
/// 从新往旧补：补到一半没空间时，落下的必须是最近的那几份。补没补成看落点，不看
/// 返回值——`copy_offsite` 把失败写成日志而不改这一跑的结局，那份日志不该被这里
/// 读成「补好了」。
fn backfill_offsite(volume: &Path, dir: &Path, keep: usize) -> usize {
    let present: std::collections::HashSet<std::ffi::OsString> = packages_in(dir)
        .into_iter()
        .filter_map(|p| p.file_name().map(|n| n.to_os_string()))
        .collect();
    let mut copied = 0;
    for package in packages_in(volume).into_iter().rev() {
        let Some(name) = package.file_name().map(|n| n.to_os_string()) else {
            continue;
        };
        if present.contains(&name) {
            continue;
        }
        copy_offsite(dir, &package, keep);
        if dir.join(&name).exists() {
            copied += 1;
        }
    }
    copied
}

/// Copy the finished package where the party that asked for it can fetch it.
///
/// A failure here fails the run. The package already exists in the backup
/// directory, so nothing is lost -- but the run was asked for by somebody
/// waiting on this file, and a package left only there is pruned by the backup
/// directory's own retention, on a schedule that belongs to the nightly job.
/// "It went somewhere" and "it went where it was asked to go" are different
/// facts and only one of them is the one that was requested.
///
/// The copy is written under a dot-name and renamed into place: a download
/// that starts while this runs must not see half a package, and the listing
/// must not offer one either.
fn publish_package(dir: &Path, package: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(dir)?;
    let name = package
        .file_name()
        .ok_or("发布：包没有文件名，不知道该复制成什么")?;
    let target = dir.join(name);
    let size = package.metadata()?.len();
    // 同一份包被要了两次是同一份文件，不是两份。
    if target.exists() {
        if target.metadata()?.len() == size {
            return Ok(target);
        }
        std::fs::remove_file(&target)?;
    }
    let partial = dir.join(format!(".{}.partial", name.to_string_lossy()));
    let _ = std::fs::remove_file(&partial);
    std::fs::copy(package, &partial)?;
    restrict_to_owner(&partial, 0o600)?;
    std::fs::rename(&partial, &target)?;
    tracing::info!(
        package = %target.display(),
        bytes = size,
        "backup package published for download"
    );
    Ok(target)
}

/// 真正跑一次备份并收尾：导出、写清单、打包、清 staging、裁保留。
async fn finish_backup(
    cfg: &BackupConfig,
    now: chrono::DateTime<Utc>,
    staging: &Path,
    staging_name: &str,
) -> Result<LeftBehind, Box<dyn std::error::Error>> {
    // 口令读**一次**，这一趟的两个决定都由它回答：凭证进不进包、包是不是密文。
    // 分两处各读一次环境变量，两次之间插进来的差异会让凭证躺进一个明文包。
    let key = crate::backup_crypto::PackageKey::from_env();
    match run_backup_into(cfg, staging, key.is_some()).await {
        Ok(manifest) => {
            // 覆盖清单在打包之前算：它说的是「这个包**该有**什么」，而清单是包内
            // 唯一那份声明。打包之后就算不出来了——staging 已经删掉。
            let coverage = package_coverage(&manifest);
            let manifest_json = serde_json::to_string_pretty(&manifest)?;
            std::fs::write(staging.join(MANIFEST_FILE), format!("{manifest_json}\n"))?;
            let package = cfg.backup_dir.join(package_name(now));
            tar_package_with_key(staging, staging_name, &package, key.as_ref())?;
            std::fs::remove_dir_all(staging)?;
            let deleted = enforce_retention(&cfg.backup_dir, cfg.keep_count);
            if let Some(dir) = &cfg.offsite_dir {
                copy_offsite(dir, &package, cfg.keep_count);
                let backfilled = backfill_offsite(&cfg.backup_dir, dir, cfg.keep_count);
                if backfilled > 0 {
                    tracing::info!(
                        dir = %dir.display(),
                        backfilled,
                        "offsite backfill caught up on packages written before the copy existed"
                    );
                }
            }
            if let Some(dir) = &cfg.publish_dir {
                publish_package(dir, &package)?;
            }
            let (kept_after, oldest_kept_at) = retained_state(&cfg.backup_dir);
            tracing::info!(
                package = %package.display(),
                kept = cfg.keep_count,
                pruned = deleted.len(),
                "backup package complete"
            );
            let bytes = std::fs::metadata(&package).ok().map(|m| m.len() as i64);
            // 摘要读失败不判这一趟失败：包已经落地了，拿「算不出摘要」把它判成没有备份，
            // 是拿被保护的东西换那张说明它的纸。留一条 warn，因为「没记」和「记了又对上」
            // 在账上是两种不同的缺席，而只有日志能说出这一趟属于哪种。
            let checksum = match package_digest(&package) {
                Ok(digest) => Some(digest),
                Err(e) => {
                    tracing::warn!(
                        package = %package.display(),
                        error = %e,
                        "备份：包写完了，但摘要算不出来，这一趟在账上不会有校验和"
                    );
                    None
                }
            };
            Ok(LeftBehind {
                package,
                bytes,
                checksum,
                coverage,
                kept_after,
                oldest_kept_at,
            })
        }
        Err(e) => {
            // 半成品 staging 必须清掉：留在原地会被当成可恢复包误用。
            let _ = std::fs::remove_dir_all(staging);
            Err(e)
        }
    }
}

async fn run_backup_into(
    cfg: &BackupConfig,
    staging: &Path,
    sealed: bool,
) -> Result<BackupManifest, Box<dyn std::error::Error>> {
    let keys = export_secret_manifests(
        &cfg.secrets_dir,
        &cfg.oob_secrets_dir,
        &cfg.namespace,
        &staging.join(SECRETS_DIR),
        sealed,
    )?;

    // 每一段取完就盖一个时刻。写在每段之后而不是统一在开头取：要量的是「这段
    // 数据是什么时候被取走的」，不是「这一跑什么时候开始」。
    let mut segments = Vec::new();
    let pool = PgPool::connect(&cfg.database_url).await?;
    let postgres = backup_postgres(&pool, &staging.join(PACKAGE_PG_DIR)).await?;
    segments.push(segment_stamp("postgres"));

    let qdrant = QdrantHttp::new(&cfg.qdrant_http_url);
    let qdrant_dir = staging.join(QDRANT_DIR);
    std::fs::create_dir_all(&qdrant_dir)?;
    let mut collections = Vec::new();
    for name in qdrant.collection_names().await? {
        collections.push(qdrant.snapshot_collection(&name, &qdrant_dir).await?);
    }
    segments.push(segment_stamp("qdrant"));

    // 对象层：备份进程只拿到 `data_dir`，读不到 `providers.storage.options.base_path`，
    // 所以按约定找 `<data_dir>/storage`。约定之外的部署——chart 默认的 seaweedfs 把对象
    // 放在它自己的卷里——这一棵根本不存在，而 `copy_tree` 对不存在的源返回 `(0, 0)`：
    // 「一个对象都没带」与「本来就没有对象」在清单上同形。所以缺席要落进
    // `lossy_surfaces` 点名，不能安静地记成 0 个文件。
    let objects_dir = local_objects_dir(&cfg.data_dir);
    let (files, bytes) = match &objects_dir {
        Some(src) => copy_tree(src, &staging.join(PACKAGE_OBJECTS_DIR).join(OBJECTS_SUBDIR))?,
        None => (0, 0),
    };
    segments.push(segment_stamp("objects"));
    let artifacts = collect_artifacts(cfg, staging, Some(&pool)).await?;
    segments.push(segment_stamp("artifacts"));

    // 进化 bare 仓：它不在数据根里，是独立的一卷，所以单独打成 git bundle。
    // 打包失败不让整次备份失败——没有 bundle 的包仍然救得回其余全部状态，而
    // 这一格的损失会在 manifest 的 `lossy_surfaces` 里被点名。相反，成功就必须
    // 在 manifest 里留下读数，恢复端才知道该把哪一份种回去。
    let evolution_bundle = match bundle_evolution(
        cfg.git_remote_dir.as_deref(),
        &staging.join(GIT_DIR),
    ) {
        Ok(report) => report,
        Err(e) => {
            tracing::warn!(error = %e, "备份：进化 bare 仓打不成 git bundle，这一格按有损面点名");
            None
        }
    };
    if evolution_bundle.is_some() {
        segments.push(segment_stamp("git-bundle"));
    }
    let bound = max_segment_skew();
    if let Ok((first, last, skew)) = segment_span(&segments) {
        if skew > bound as i64 {
            // 包照打：一份偏差超界的包仍然比没有包强（PG 那一段本身是自洽的），
            // 而恢复端会拒绝它。这一行是给看日志的人的第一句信号。
            tracing::warn!(
                skew_seconds = skew,
                bound,
                first = %first,
                last = %last,
                "备份：几段数据相隔超过声明的界，恢复端会拒绝这个包"
            );
        }
    }

    // 实例指纹的规范来源是 Secret（装入期生成）；env 是同一值的注入副本，
    // 两边都读不到时记 unknown，manifest 仍然落盘（备份本身不失败）。
    let fingerprint = std::fs::read_to_string(cfg.secrets_dir.join("instance-fingerprint"))
        .ok()
        .or_else(|| std::env::var(cog_core::env::INSTANCE_FINGERPRINT).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());

    // 进包了没有，由导出的结果说，不由「有没有口令」再推一遍：带外那片可能还没
    // 建过，那一趟虽然带着钥匙，包里也确实没有它的键。
    let credentials_present = !keys.is_empty();

    Ok(BackupManifest {
        version: env!("CARGO_PKG_VERSION").to_string(),
        revision: crate::build_info::GIT_REVISION.to_string(),
        instance_fingerprint: fingerprint,
        namespace: cfg.namespace.clone(),
        created_at: now_rfc3339(),
        secrets_keys: keys,
        postgres,
        qdrant: collections,
        objects: ObjectsReport { files, bytes },
        artifacts,
        image: std::env::var(IMAGE_REF_ENV).unwrap_or_default(),
        schema_version: current_schema_version(),
        lossy_surfaces: lossy_surfaces(
            evolution_bundle.is_some(),
            credentials_present,
            objects_dir.is_none(),
        ),
        segments,
        max_segment_skew_seconds: bound,
        evolution_bundle,
    })
}

/// 备份包的目录布局。出包、核包、恢复与演练各读一遍这些名字；各写一份字面量时，
/// 改名只会让其中一侧找不到东西，而「找不到」在几处读起来就是「这个包没有这一段」。
const MANIFEST_FILE: &str = "manifest.json";
const SECRETS_DIR: &str = "secrets";
const QDRANT_DIR: &str = "qdrant";
const ARTIFACTS_DIR: &str = "artifacts";
const GIT_DIR: &str = "git";
const EVOLUTION_BUNDLE_FILE: &str = "evolution.bundle";

/// 包里的 PG 段：一份 `pg_dump` 与全部序列的 `setval` 语句。
const PACKAGE_PG_DIR: &str = "pg";

/// PG 段里那串序列语句的文件名。写包的一侧拼它，核包与恢复的一侧再拼一遍。
const SEQUENCES_FILE: &str = "sequences.sql";

/// 数据根下装对象的那一级。
pub const OBJECTS_SUBDIR: &str = "storage";

/// 包里对象层的那一层。与 [`OBJECTS_SUBDIR`] 同名同构：恢复端按同一个相对布局
/// 把整棵抄回去，两边各写一份字面量只会在改名之后让恢复把对象放到别处——那边
/// 没有读者会报错，只是从此读不到对象。
const PACKAGE_OBJECTS_DIR: &str = "objects";

/// 对象层的本地子树，**只有它真在盘上**才算这个包该带走的东西。
///
/// 备份进程只拿得到数据根，读不到 `providers.storage.options.base_path`，所以按
/// 约定找 `<data_dir>/storage`。约定之外的部署里这一棵不存在，而 `copy_tree` 对
/// 不存在的源返回 `(0, 0)`——「一个对象都没带」与「本来就没有对象」在清单上同形，
/// 所以缺席要进 `lossy_surfaces`，不在这里安静地返回一个空结果。
fn local_objects_dir(data_dir: &Path) -> Option<PathBuf> {
    let dir = data_dir.join(OBJECTS_SUBDIR);
    dir.is_dir().then_some(dir)
}

/// 数据根下一级子目录在「换机」这件事上的待遇。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubdirClass {
    /// 包必须带走。带不走是这次备份的缺陷，不是可以接受的损失。
    Authoritative,
    /// 包不带，且换机后不重建也行——行则行在 `why` 上，不写在别处。
    Discardable,
}

/// 数据根下一级子目录的一条判词。
///
/// `name` 是子目录名本身，不是给人看的标题：判断面（`crates/cogneva/tests/
/// data_root_subdirs.rs`）拿它跟普查面逐字比对。`roots` 是它会出现的那些数据根
/// （主根、进化根）——同一个名字在两块卷上都出现时两边都得算。
///
/// `why` 回答的是「换机后少了它，为什么可以不补」。`Discardable` 的行没有 `why`
/// 就是一张「谁都没想过」的空分类，测试判红。
#[derive(Debug)]
pub struct DataRootSubdir {
    pub name: &'static str,
    pub roots: &'static [&'static str],
    pub class: SubdirClass,
    pub what: &'static str,
    pub why: &'static str,
}

/// 数据根下每一个一级子目录的判词，**闭集**。
///
/// 这张表存在的理由不是「给人查阅」，而是：数据根是「换机有没有丢东西」的实际
/// 边界，而它此前没有任何一处列过——新增一个子目录时，备份 Job 照旧绿、恢复演练
/// 照旧绿，包里就是少了一棵树。`warehouse/` 就是这么进来的：它是学习数仓的导出
/// 面，代码里写 `{data_dir}/warehouse`，在它出现之前没有任何一份清单提过它。
///
/// 所以判词的判据不是这张表自己：普查面从代码、应用配置、渲染清单三条来源把
/// 子目录**数出来**，再回这里找判词；找不着就是「有人新开了一个子目录而没人说
/// 过它换机怎么办」，红。反过来，表里有一行而普查面没有（`git-workdir`、
/// `pending-changes` 这类只在运行期出现、代码里由变量拼出来的），只是提示，不是红。
///
/// 这张表同时是 `lossy_surfaces()` 里「数据根下丢了什么」那一族的来源：
/// `Discardable` 的行会进包的损失清单，理由取这一行的 `why`。
pub const DATA_ROOT_SUBDIRS: &[DataRootSubdir] = &[
    DataRootSubdir {
        name: OBJECTS_SUBDIR,
        roots: &["main"],
        class: SubdirClass::Authoritative,
        what: "对象层（providers.storage.options.base_path）",
        why: "包里的 objects 段逐文件带走并计数",
    },
    DataRootSubdir {
        name: "policies",
        roots: &["main", "evolution"],
        class: SubdirClass::Authoritative,
        what: "策略库版本文件与 ACTIVE",
        why: "包里的 artifacts 段按「数据根 + 库名」逐库带走",
    },
    DataRootSubdir {
        name: "meta_learning",
        roots: &["main", "evolution"],
        class: SubdirClass::Authoritative,
        what: "决策统计快照",
        why: "包里的 artifacts 段带走，运行时正本是 PG 表，二者按格取大",
    },
    DataRootSubdir {
        name: "git-remote",
        roots: &["evolution"],
        class: SubdirClass::Authoritative,
        what: "进化的 bare 仓库",
        why: "整仓打成 git bundle 进包（`git/evolution.bundle`，恢复时种回）；\
              出包时打不成才退回 lossy_surfaces 点名",
    },
    DataRootSubdir {
        name: "workspace",
        roots: &["main"],
        class: SubdirClass::Authoritative,
        what: "WebUI 编辑器的用户工作区（{data_dir}/workspace/<user uuid>）",
        why: "包里的 artifacts/<root>/workspace 段整树带走，恢复端按文件数与字节数\
              核对后换回原位；用户在里面新建的文件没有第二个副本",
    },
    DataRootSubdir {
        name: "raw-logs",
        roots: &["main"],
        class: SubdirClass::Discardable,
        what: "原始来源归档（raw_logger.base_dir）",
        why: "抽取结果已落 PG 的权威面；重抽取要从上游重抓，代价是时间不是数据",
    },
    DataRootSubdir {
        name: "warehouse",
        roots: &["main"],
        class: SubdirClass::Discardable,
        what: "学习数仓的 JSONL 导出（{data_dir}/warehouse）",
        why: "它是 PG 与策略库的导出面，不是权威面；数据在来源与 PG 里",
    },
    DataRootSubdir {
        name: "landing",
        roots: &["main"],
        class: SubdirClass::Discardable,
        what: "落地通道的每次落地记录与工作副本",
        why: "落地的结果以 git 提交与服务状态为准；这些文件是过程记录",
    },
    DataRootSubdir {
        name: "git-workdir",
        roots: &["main"],
        class: SubdirClass::Discardable,
        what: "落地通道的共享 git 工作副本",
        why: "它是一次克隆的工作区，从远端仓库重新克隆即得",
    },
    DataRootSubdir {
        name: "pending-changes",
        roots: &["main"],
        class: SubdirClass::Discardable,
        what: "还没投递出去的已生成变更（数据根下 pending-changes/，每条一个 json）",
        why: "已投递的以 change_ledger 与 git 上的提交为准；没投递的那次生成换机后\
              重跑一遍生成，代价是一次生成而不是系统缺一块",
    },
    DataRootSubdir {
        name: "eval",
        roots: &["main"],
        class: SubdirClass::Discardable,
        what: "评测数据集（{data_dir}/eval/datasets）",
        why: "`deploy/scripts/fetch-benchmark-data.sh` 可从公开来源重取",
    },
];

/// 按子目录名取判词。
pub fn subdir_policy(name: &str) -> Option<&'static DataRootSubdir> {
    DATA_ROOT_SUBDIRS.iter().find(|row| row.name == name)
}

/// 本二进制带的最新 PG 迁移号。读不出来记 0（没声明），不让备份失败：
/// 迁移目录缺席是打包问题，应当由别处的门去红，不是让这一晚没有备份。
fn current_schema_version() -> i64 {
    let app_dir = cog_core::paths::app_dir();
    let dir = cog_storage::migrate::migrations_dir(&app_dir);
    match cog_storage::migrate::discover_up_migrations(&dir, cog_storage::migrate::Driver::Postgres)
    {
        Ok(migrations) => migrations.iter().map(|m| m.version).max().unwrap_or(0),
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "迁移目录读不出来，schema_version 记 0");
            0
        }
    }
}

/// 跨段偏差的默认界：五分钟。超过它，包里几段就已经不是同一份数据了。可由
/// `COGNEVA_BACKUP_MAX_SEGMENT_SKEW_SECONDS` 覆盖——大对象层在小机器上本来就慢，
/// 把界写死会让那类环境每一晚都产出一个恢复端拒收的包。
// cap: bound — 大对象层在小机器上本来就慢，写死会让那类环境每一晚都产出一个恢复端拒收的包，所以它可覆盖
const DEFAULT_MAX_SEGMENT_SKEW_SECONDS: u64 = 300;

fn max_segment_skew() -> u64 {
    std::env::var("COGNEVA_BACKUP_MAX_SEGMENT_SKEW_SECONDS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_MAX_SEGMENT_SKEW_SECONDS)
}

fn segment_stamp(name: &str) -> SegmentStamp {
    SegmentStamp {
        name: name.to_string(),
        at: now_rfc3339(),
    }
}

/// 首段、末段的段名与两者相距多少秒。段表为空时返回 None（旧格式）；时刻读不
/// 出来时返回 Err——一条读不出来的声明等于没声明，不能当成「没有偏差」。
fn segment_span(segments: &[SegmentStamp]) -> Result<(String, String, i64), String> {
    let mut first: Option<(String, DateTime<Utc>)> = None;
    let mut last: Option<(String, DateTime<Utc>)> = None;
    for s in segments {
        let at = DateTime::parse_from_rfc3339(&s.at)
            .map_err(|e| format!("分段 {} 的时刻读不出来：{e}", s.name))?
            .with_timezone(&Utc);
        if first.as_ref().is_none_or(|(_, t)| at < *t) {
            first = Some((s.name.clone(), at));
        }
        if last.as_ref().is_none_or(|(_, t)| at > *t) {
            last = Some((s.name.clone(), at));
        }
    }
    match (first, last) {
        (Some((fname, ft)), Some((lname, lt))) => Ok((fname, lname, (lt - ft).num_seconds())),
        _ => Err("分段表是空的".into()),
    }
}

/// 恢复前判跨段偏差。包是几段在不同时刻取的东西拼起来的，恢复端拿包自己声明的
/// 界去判：超过界就拒绝，而不是把不同时刻的段拼成一个「成功」。
///
/// 三条边界：旧包没有分段表（不判，留一条日志）；声明了分段却没声明界（拒绝——
/// 成对的声明只出现一半，读不出该按什么判）；时刻读不出来（拒绝）。
fn check_segment_skew(manifest: &BackupManifest) -> Result<(), String> {
    if manifest.segments.is_empty() {
        tracing::info!("包没有分段时刻声明（旧格式）：跨段偏差无从判");
        return Ok(());
    }
    if manifest.max_segment_skew_seconds == 0 {
        return Err("包声明了分段时刻却没有声明允许的跨段偏差".into());
    }
    let (first, last, skew) = segment_span(&manifest.segments)?;
    if skew > manifest.max_segment_skew_seconds as i64 {
        return Err(format!(
            "包的几段数据相隔 {skew}s，超过声明的界 {}s（{first} → {last}）",
            manifest.max_segment_skew_seconds
        ));
    }
    tracing::info!(
        skew_seconds = skew,
        bound = manifest.max_segment_skew_seconds,
        "跨段偏差在界内"
    );
    Ok(())
}

/// 这份包**没有**带走的东西。清单外的一律不许「默认可丢」。
///
/// 每一条都要能回答「换机后少了什么、为什么能接受」。加了新的外部依赖而这里
/// 没有对应的行，换机后那个面就是静默消失的——所以这里是一张按依赖来源写的
/// 闭集，不是一份事后的说明。
///
/// `bundle_present` 是包里到底有没有那份进化 bare 仓的 git bundle：带了就不该
/// 再把它列成损失，没带才是。这一格由出包时实测决定，不由这张表自己声称。
///
/// `credentials_present` 同理：没有包级口令时两片 Secret 清单根本不进包（凭证
/// 不往明文里抄），那就必须有一行说清换机后少了什么、怎么补回来——否则恢复端
/// 只会看到 `secrets_keys` 是空的，而那和「老包当年没声明」是两种事。
fn lossy_surfaces(
    bundle_present: bool,
    credentials_present: bool,
    objects_absent: bool,
) -> Vec<LossySurface> {
    let mut surfaces: Vec<LossySurface> = [
        (
            "nats",
            "在途事件与 JetStream 里尚未被消费的消息",
            "事件是通知面，权威状态在 PG 与对象层；消费者重连后从权威状态重建",
            SurfaceRecovery::SelfRebuilding,
        ),
        (
            "redis",
            "缓存、租约与去重计数",
            "全部是可重建的派生状态；租约在重启后按既有规则重新竞争",
            SurfaceRecovery::SelfRebuilding,
        ),
        (
            "meilisearch",
            "搜索索引",
            "索引由权威数据重建，恢复后按索引任务重跑",
            SurfaceRecovery::SelfRebuilding,
        ),
        (
            "cluster-registry",
            "containerd 里已拉取的镜像层",
            "镜像是构建产物，不是权威面；由源码 rev 重建或从 release 重新拉取",
            SurfaceRecovery::SelfRebuilding,
        ),
    ]
    .into_iter()
    .map(|(name, lost, why, recovery)| LossySurface {
        name: name.to_string(),
        lost: lost.to_string(),
        acceptable_because: why.to_string(),
        recovery,
    })
    .collect();
    // 数据根下不带走的子目录，判词本来就和 `DATA_ROOT_SUBDIRS` 里那一行是同一句话。
    // 两处各写一份的结果已经出现过一次：同一条损失在两份清单里措辞不同，读者没法
    // 判断它们是同一件事还是两件。这里只取表里的那一行。
    for subdir in DATA_ROOT_SUBDIRS
        .iter()
        .filter(|row| row.class == SubdirClass::Discardable)
    {
        surfaces.push(LossySurface {
            name: subdir.name.to_string(),
            lost: subdir.what.to_string(),
            acceptable_because: subdir.why.to_string(),
            recovery: SurfaceRecovery::SelfRebuilding,
        });
    }
    if !bundle_present {
        surfaces.push(LossySurface {
            name: "evolution-bare-repo".to_string(),
            lost: "未推送到远端的进化 rev（这个包没有带 git bundle）".to_string(),
            acceptable_because:
                "已推的 rev 可从公开远端重取；未推的那部分确实是损失——出包时仓库还没 seed \
                 或 bundle 没打成，出包日志里有那一行告警"
                    .to_string(),
            recovery: SurfaceRecovery::SelfRebuilding,
        });
    }
    if objects_absent {
        surfaces.push(LossySurface {
            name: "objects".to_string(),
            lost: "对象层的全部对象（数据根下没有 `storage/` 这一棵子树）".to_string(),
            acceptable_because:
                "备份进程只拿到数据根，读不到 `providers.storage.options.base_path`，所以它按\
                 约定找 `<data_dir>/storage`。部署把对象放在别处时（chart 默认的 seaweedfs 把\
                 它们放在自己那块卷上），这份包从来不带对象——换机后要由那项服务自己的备份\
                 恢复，或按 `change_ledger` 与工件重新生成。点名它是为了不让「一份都没带」\
                 读成「本来就一个对象都没有」"
                    .to_string(),
            recovery: SurfaceRecovery::NeedsFollowUp,
        });
    }
    if !credentials_present {
        surfaces.push(LossySurface {
            name: "credentials".to_string(),
            lost: format!(
                "两片 Secret 对象清单（`{}` / `{}`）",
                cog_core::topology::SECRET_INTERNAL,
                cog_core::topology::SECRET_OOB
            ),
            acceptable_because:
                "包不是密文时不把凭证抄进包：一份能读文件就拿走全部 token 的明文包，本身就是\
                 新的凭证载体。换机后内部那片由安装脚本重新生成，带外那片（平台 token、\
                 LLM 上游钥匙、git 私钥）由向导重新写入网关"
                    .to_string(),
            recovery: SurfaceRecovery::NeedsFollowUp,
        });
    }
    surfaces
}

/// 把进化 bare 仓的完整历史打成一个 git bundle 放进 staging。
///
/// `None` 有三种正当来源：部署里没有进化、仓库还没 seed、仓库还没有提交。三种
/// 都让 manifest 不带这一格，`lossy_surfaces` 会据此点名损失，恢复端也就知道
/// 包里没有可种的东西。打包失败走 `Err`，调用方记一条告警后按「没带」处理——
/// 没有 bundle 的包仍然救得回其余全部状态，让整晚的备份因此失败反而更糟。
fn bundle_evolution(
    dir: Option<&Path>,
    out_dir: &Path,
) -> Result<Option<EvolutionBundleReport>, Box<dyn std::error::Error>> {
    let Some(dir) = dir else {
        return Ok(None);
    };
    if !dir.join("HEAD").is_file() {
        tracing::info!(dir = %dir.display(), "进化 bare 仓还没 seed（没有 HEAD），这个包不带它");
        return Ok(None);
    }
    // 空仓（init 过但还没有提交）没有可打包的历史；`rev-parse --verify HEAD` 会
    // 非零退出，那不是错误，只是「还没有东西可带」。
    if !git_run(dir, &["rev-parse", "--verify", "HEAD"])?
        .status
        .success()
    {
        tracing::info!(dir = %dir.display(), "进化 bare 仓还没有提交，这个包不带它");
        return Ok(None);
    }
    std::fs::create_dir_all(out_dir)?;
    let bundle = out_dir.join(EVOLUTION_BUNDLE_FILE);
    let partial = out_dir.join("evolution.bundle.partial");
    let _ = std::fs::remove_file(&partial);
    let partial_s = partial
        .to_str()
        .ok_or("bundle 路径不是 UTF-8，git 命令行给不出来")?;
    let bundle_s = bundle
        .to_str()
        .ok_or("bundle 路径不是 UTF-8，git 命令行给不出来")?;

    git_ok(dir, &["bundle", "create", partial_s, "--all"])?;
    // 自己先验一遍：一个恢复端打不开的 bundle 比没有更坏——manifest 会说自己带了。
    git_ok(dir, &["bundle", "verify", partial_s])?;
    std::fs::rename(&partial, &bundle)?;

    let heads = git_ok(dir, &["bundle", "list-heads", bundle_s])?;
    let refs = heads
        .lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter(|refname| *refname != "HEAD")
        .count() as u64;
    let head = git_ok(dir, &["symbolic-ref", "-q", "HEAD"])
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let commit = git_ok(dir, &["rev-parse", "HEAD"])?.trim().to_string();
    let bytes = std::fs::metadata(&bundle)?.len();
    tracing::info!(
        dir = %dir.display(),
        bytes,
        refs,
        head = %head,
        commit = %commit,
        "进化 bare 仓已打成 git bundle"
    );
    Ok(Some(EvolutionBundleReport {
        bytes,
        refs,
        head,
        commit,
    }))
}

fn git_run(dir: &Path, args: &[&str]) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    Ok(std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()?)
}

fn git_ok(dir: &Path, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let output = git_run(dir, args)?;
    if !output.status.success() {
        return Err(format!(
            "git {} 失败（{}）：{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// 把每个数据根的产物层抄进包，并留下逐库读数。
///
/// 抄 `policies/`、`meta_learning/decision_stats.json` 与 `workspace/`。写点已经是
/// tmp+fsync+rename，读者要么看到旧文件要么看到新文件；版本文件与 ACTIVE 之间
/// 仍可能隔一拍，清单因此把 head 与 active 分开记，恢复端按「active 指向的版本
/// 存在且 hash 对得上」判，不按「active 必须等于 head」。
async fn collect_artifacts(
    cfg: &BackupConfig,
    staging: &Path,
    pool: Option<&PgPool>,
) -> Result<Vec<ArtifactRootReport>, Box<dyn std::error::Error>> {
    let mut reports = Vec::new();
    for (root, dir) in cfg.data_roots() {
        let mut report = collect_artifact_root(&root, &dir, staging).await?;
        // 决策统计的运行时作者是库里的表；主数据根上的那份文件只是导出面。
        // 进包前由表重写一遍，停机期间停在卷上的旧文件才不会冒充当前证据。
        if root == "app" {
            if let Some(pool) = pool {
                export_decision_stats(pool, staging, &mut report).await;
            }
        }
        reports.push(report);
    }
    Ok(reports)
}

/// 用表里的行重写包内那份 `decision_stats.json`，并把读数换成本次写出的文件。
///
/// 卷上可能也有一份（没有库的进程写的，或者换成表之前留下的）。两边按
/// 「同一格取较大计数」合并，而不是二选一：计数只增不减，较大的一侧就是看得更多
/// 的那条历史，丢哪一边都等于把已经记下的证据删掉。
///
/// 表读不出来只告警、不判这次备份失败——同一个不对称：一份已经跑完的包不该
/// 因为它的一个说明面读不到而被扔掉。代价写在日志里：这时包内那份是卷上的文件，
/// 比表旧。
async fn export_decision_stats(pool: &PgPool, staging: &Path, report: &mut ArtifactRootReport) {
    use cog_core::DecisionStatsStore;

    let store = PostgresDecisionStatsStore::new(pool.clone());
    let cells = match store.load_all().await {
        Ok(cells) => cells,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "备份：决策统计表读不出来，包内那份沿用卷上的文件（可能比表旧）"
            );
            return;
        }
    };

    let staged = staging
        .join(ARTIFACTS_DIR)
        .join("app")
        .join(cog_reflection::DATA_SUBDIR)
        .join(cog_reflection::DECISION_STATS_FILE);
    let file = std::fs::read_to_string(&staged).ok();
    let from_file = file.as_deref().map(file_group_count).unwrap_or(0);
    let Some(body) = merge_decision_stats(file.as_deref(), &cells) else {
        // 表和卷上都没有任何一格：包里不带这份文件，清单也不记它。
        return;
    };

    if let Some(parent) = staged.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(error = %e, path = %parent.display(), "备份：决策统计导出目录建不出来");
            return;
        }
    }
    if let Err(e) = std::fs::write(&staged, body.as_bytes()) {
        tracing::warn!(error = %e, path = %staged.display(), "备份：决策统计导出写不进去");
        return;
    }
    match decision_stats_counts(&body) {
        Ok((groups, trials)) => {
            report.decision_stats = Some(DecisionStatsReport {
                groups,
                trials,
                bytes: body.len() as u64,
            });
            tracing::info!(
                groups,
                trials,
                table_cells = cells.len(),
                file_groups = from_file,
                "备份：包内决策统计由表导出"
            );
        }
        Err(e) => tracing::warn!(error = %e, "备份：导出的决策统计读不回来"),
    }
}

/// (category, group) -> decision -> (attempts, successes)，按字符串有序：同一份
/// 证据必须导出同一个文件，否则排障时会把「什么都没变」读成「状态变了」。
type StatsTree = BTreeMap<String, BTreeMap<String, BTreeMap<String, (i64, i64)>>>;

/// 把卷上的旧文件与表里的行合成一份导出，空则回答 `None`。
///
/// 两边按「同一格取较大计数」合并，而不是二选一或相加：计数只增不减，较大的
/// 一侧就是看得更多的那条历史；相加则会在表与文件本来描述同一段历史时把它们
/// 数两遍。任何一边解析不了的格被跳过，不让一格坏数据带走整份导出。
fn merge_decision_stats(
    file: Option<&str>,
    cells: &[cog_core::DecisionStatCell],
) -> Option<String> {
    let mut merged: StatsTree = BTreeMap::new();
    if let Some(text) = file {
        if let Ok(doc) = serde_json::from_str::<serde_json::Value>(text) {
            if let Some(groups) = doc.get("groups").and_then(|g| g.as_array()) {
                for group in groups {
                    let Some(category) = group.get("category").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let Some(key) = group.get("key").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let Some(counts) = group.get("counts").and_then(|c| c.as_object()) else {
                        continue;
                    };
                    for (decision, pair) in counts {
                        let Some((a, s)) = pair
                            .as_array()
                            .and_then(|p| Some((p.first()?.as_i64()?, p.get(1)?.as_i64()?)))
                        else {
                            continue;
                        };
                        raise_cell(&mut merged, category, key, decision, a, s);
                    }
                }
            }
        }
    }
    for cell in cells {
        raise_cell(
            &mut merged,
            &cell.category,
            &cell.group_key,
            &cell.decision,
            cell.attempts,
            cell.successes,
        );
    }
    if merged.is_empty() {
        return None;
    }
    let groups: Vec<serde_json::Value> = merged
        .iter()
        .flat_map(|(category, by_key)| {
            by_key.iter().map(move |(key, decisions)| {
                let counts: serde_json::Map<String, serde_json::Value> = decisions
                    .iter()
                    .map(|(d, (a, s))| (d.clone(), serde_json::json!([a, s])))
                    .collect();
                serde_json::json!({"category": category, "key": key, "counts": counts})
            })
        })
        .collect();
    Some(serde_json::json!({"version": 1, "groups": groups}).to_string())
}

/// 文件里有几组。只用于日志里对照「表带来了几格、文件原来有几组」。
fn file_group_count(text: &str) -> usize {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|doc| doc.get("groups").and_then(|g| g.as_array().map(Vec::len)))
        .unwrap_or(0)
}

fn raise_cell(
    merged: &mut StatsTree,
    category: &str,
    key: &str,
    decision: &str,
    attempts: i64,
    successes: i64,
) {
    let entry = merged
        .entry(category.to_string())
        .or_default()
        .entry(key.to_string())
        .or_default()
        .entry(decision.to_string())
        .or_insert((attempts, successes));
    entry.0 = entry.0.max(attempts);
    entry.1 = entry.1.max(successes);
}

async fn collect_artifact_root(
    root: &str,
    dir: &Path,
    staging: &Path,
) -> Result<ArtifactRootReport, Box<dyn std::error::Error>> {
    let mut report = ArtifactRootReport {
        root: root.to_string(),
        data_dir: dir.display().to_string(),
        ..Default::default()
    };

    let policies_src = cog_reflection::policies_path(dir);
    if policies_src.is_dir() {
        report.policies_present = true;
        let dst = staging
            .join(ARTIFACTS_DIR)
            .join(root)
            .join(cog_reflection::POLICIES_SUBDIR);
        copy_policy_tree(&policies_src, &dst)?;
        report.policy_stores = describe_policy_stores(&policies_src).await?;
    }

    let stats_src = cog_reflection::decision_stats_path(dir);
    if stats_src.is_file() {
        let dst = staging
            .join(ARTIFACTS_DIR)
            .join(root)
            .join(cog_reflection::DATA_SUBDIR);
        std::fs::create_dir_all(&dst)?;
        std::fs::copy(&stats_src, dst.join(cog_reflection::DECISION_STATS_FILE))?;
        let bytes = std::fs::metadata(&stats_src)?.len();
        let text = std::fs::read_to_string(&stats_src)?;
        let (groups, trials) = decision_stats_counts(&text)?;
        report.decision_stats = Some(DecisionStatsReport {
            groups,
            trials,
            bytes,
        });
    }

    // 编辑器工作区整树进包。它不像策略库那样有「从哪重建」的第二条路：用户在
    // 里面新建的文件只有这一份，而机器换掉时数据根就是全部。逐文件复制，清单记
    // 下文件数与字节数，恢复端据此判「抄过去的这一份是不是当年那一份」。
    let workspace_src = cog_gateway::ide::workspace_root(dir);
    if workspace_src.is_dir() {
        let dst = staging
            .join(ARTIFACTS_DIR)
            .join(root)
            .join(cog_gateway::ide::WORKSPACE_SUBDIR);
        let (files, bytes) = copy_tree(&workspace_src, &dst)?;
        report.workspace = Some(WorkspaceReport { files, bytes });
    }

    Ok(report)
}

/// 只抄策略库里的持久件：`ACTIVE` 与 `*.json`。`*.tmp` 是写到一半的临时文件，
/// 打进包里既没有读者，还会让恢复端把一次崩溃的残骸当成一个版本。
fn copy_policy_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    for store in std::fs::read_dir(src)? {
        let store = store?;
        if !store.file_type()?.is_dir() {
            continue;
        }
        let to = dst.join(store.file_name());
        std::fs::create_dir_all(&to)?;
        for file in std::fs::read_dir(store.path())? {
            let file = file?;
            if !file.file_type()?.is_file() {
                continue;
            }
            let name = file.file_name();
            let name_str = name.to_string_lossy();
            if name_str == "ACTIVE" || name_str.ends_with(".json") {
                std::fs::copy(file.path(), to.join(&name))?;
            }
        }
    }
    Ok(())
}

/// 逐库读出「有几个版本、链头是谁、ACTIVE 是谁」。读不出来的原因进 `problem`，
/// 让备份照样完成、把破损留在清单里；演练会拿这份声明去包里复核，破损在那一跑
/// 变成红，而不是在这里静默丢掉。
async fn describe_policy_stores(
    policies_dir: &Path,
) -> Result<Vec<PolicyStoreReport>, Box<dyn std::error::Error>> {
    let store = cog_reflection::PolicyStore::new(policies_dir);
    let mut names = Vec::new();
    for entry in std::fs::read_dir(policies_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
    }
    names.sort();

    let mut reports = Vec::new();
    for name in names {
        let versions = store.list_versions(&name).await?;
        let mut row = PolicyStoreReport {
            name: name.clone(),
            versions: versions.len() as u64,
            ..Default::default()
        };
        match versions.last().copied() {
            Some(head) => match store.load_version(&name, head).await? {
                Some(artifact) => {
                    row.head_version = head;
                    row.head_hash = artifact.hash;
                }
                None => row.problem = Some(format!("链头版本 {head:06} 读不出来")),
            },
            None => row.problem = Some("策略库里没有任何版本文件".into()),
        }
        let active_path = policies_dir.join(&name).join("ACTIVE");
        match std::fs::read_to_string(&active_path) {
            Ok(text) => match text.trim().parse::<u64>() {
                Ok(version) => {
                    row.active_version = version;
                    match store.load_version(&name, version).await? {
                        Some(artifact) => row.active_hash = artifact.hash,
                        None => {
                            row.problem =
                                Some(format!("ACTIVE 指向版本 {version:06}，但包里没有这个版本"))
                        }
                    }
                }
                Err(e) => row.problem = Some(format!("ACTIVE 不是版本号: {e}")),
            },
            Err(e) => row.problem = Some(format!("ACTIVE 读不出来: {e}")),
        }
        reports.push(row);
    }
    Ok(reports)
}

/// `decision_stats.json` 的形状：`{version, groups:[{category,key,counts:{"x":[attempts, successes]}}]}`。
/// 试验数取每条计数的第一格之和；第二格是成功数，不在这里用。
fn decision_stats_counts(text: &str) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(text)?;
    let groups = doc
        .get("groups")
        .and_then(|g| g.as_array())
        .ok_or("decision_stats.json 没有 groups 数组")?;
    let mut trials = 0u64;
    for group in groups {
        if let Some(counts) = group.get("counts").and_then(|c| c.as_object()) {
            for pair in counts.values() {
                if let Some(first) = pair.as_array().and_then(|p| p.first()) {
                    trials += first.as_u64().unwrap_or(0);
                }
            }
        }
    }
    Ok((groups.len() as u64, trials))
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// 只数不搬：与 `copy_tree` 同一套口径（只算普通文件、只累加 `metadata().len()`），
/// 用在核包时量包内实测的 objects 段。
fn measure_tree(dir: &Path) -> std::io::Result<(u64, u64)> {
    if !dir.exists() {
        return Ok((0, 0));
    }
    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(from) = stack.pop() {
        for entry in std::fs::read_dir(&from)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_dir() {
                stack.push(entry.path());
            } else if ty.is_file() {
                bytes += entry.metadata()?.len();
                files += 1;
            }
        }
    }
    Ok((files, bytes))
}

/// 数一个 `COPY ... WITH (FORMAT csv, HEADER true)` 导出文件里有几**条数据行**
/// （表头那一条不算）。
///
/// 不能直接数换行：CSV 字段里可以嵌换行（文本列里带 `\n` 很常见），Postgres 会
/// 把这种字段整体加上引号，于是「文件里的换行数」比「记录数」多，拿它当行数会
/// 在一批合法包上误报。这里按 RFC4180 的状态机走一遍：引号里的换行不算记录界，
/// 引号里成对的 `""` 是一个转义引号。流式读，因为 metrics 表的 CSV 可达数百 MB。
fn count_csv_rows(path: &Path) -> std::io::Result<i64> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut in_quotes = false;
    // 引号里刚读到一个 `"`，它到底是「转义引号的前半个」还是「字段结束」要看
    // 下一个字节，跨缓冲区也不能丢，所以它是持久的。
    let mut saw_quote = false;
    let mut newlines: i64 = 0;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        for &b in &buf[..n] {
            if in_quotes {
                if saw_quote {
                    saw_quote = false;
                    if b == b'"' {
                        continue; // "" → 一个转义引号，仍在引号里
                    }
                    in_quotes = false; // 字段在这里收尾，这一字节按引号外处理
                } else if b == b'"' {
                    saw_quote = true;
                    continue;
                } else {
                    continue;
                }
            }
            if b == b'"' {
                in_quotes = true;
            } else if b == b'\n' {
                newlines += 1;
            }
        }
    }
    // 表头也占一行；COPY TO STDOUT 每行都以换行收尾，故数据行 = 换行数 - 1。
    Ok((newlines - 1).max(0))
}

/// 核包：把包里**实测**到的东西与 manifest **声明**的逐段比。不一致就退非零，
/// 恢复一个内容与清单对不上的包比拒绝它更危险——`restore_postgres` 第一步是
/// `TRUNCATE`，被删掉的是当前库；而一份截断的 CSV 会让一张表只灌回一半。
///
/// 只核 manifest 声明到的段；某段在 manifest 里是空的（旧格式整段没写），按
/// 「当年没声明这一段」跳过——缺段不是错。声明了却不符才是错。
fn verify_package_against_manifest(
    pkg_dir: &Path,
    manifest: &BackupManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut problems: Vec<String> = Vec::new();

    // postgres：声明的表集与包内 `pg/public.<表>.csv` 集相等，且每张表的行数与
    // 声明一致；`sequences.sql` 的语句数等于声明的序列数。
    if !manifest.postgres.tables.is_empty() {
        let pg_dir = pkg_dir.join(PACKAGE_PG_DIR);
        let declared: BTreeMap<&str, i64> = manifest
            .postgres
            .tables
            .iter()
            .map(|t| (t.name.as_str(), t.rows))
            .collect();
        let mut found: BTreeMap<String, i64> = BTreeMap::new();
        if pg_dir.is_dir() {
            for entry in std::fs::read_dir(&pg_dir)? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if let Some(table) = name
                    .strip_prefix("public.")
                    .and_then(|rest| rest.strip_suffix(".csv"))
                {
                    let rows = count_csv_rows(&pg_dir.join(&name))?;
                    found.insert(table.to_string(), rows);
                }
            }
        }
        for (table, rows) in &declared {
            match found.get(*table) {
                None => problems.push(format!(
                    "pg: declared table {table:?} is not in the package"
                )),
                Some(f) if f != rows => problems.push(format!(
                    "pg: table {table:?} declared {rows} row(s), package holds {f}"
                )),
                Some(_) => {}
            }
        }
        for table in found.keys() {
            if !declared.contains_key(table.as_str()) {
                problems.push(format!(
                    "pg: package table {table:?} is not in the manifest"
                ));
            }
        }
        let seq_path = pg_dir.join(SEQUENCES_FILE);
        let seqs = if seq_path.exists() {
            std::fs::read_to_string(&seq_path)?
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count()
        } else {
            0
        };
        if seqs != manifest.postgres.sequences {
            problems.push(format!(
                "pg: manifest declares {} sequence(s), sequences.sql holds {seqs}",
                manifest.postgres.sequences
            ));
        }
    }

    // qdrant：声明的集合集与包内 `qdrant/<集合>.snapshot` 集相等。快照的
    // 服务端名字（`snapshot`）落进文件后无从复算，只核文件在不在。
    if !manifest.qdrant.is_empty() {
        let qdrant_dir = pkg_dir.join(QDRANT_DIR);
        let mut found: BTreeMap<String, ()> = BTreeMap::new();
        if qdrant_dir.is_dir() {
            for entry in std::fs::read_dir(&qdrant_dir)? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if let Some(collection) = name.strip_suffix(".snapshot") {
                    found.insert(collection.to_string(), ());
                }
            }
        }
        for report in &manifest.qdrant {
            if !found.contains_key(&report.name) {
                problems.push(format!(
                    "qdrant: declared collection {:?} has no {}.snapshot in the package",
                    report.name, report.name
                ));
            }
        }
        for collection in found.keys() {
            if !manifest.qdrant.iter().any(|r| &r.name == collection) {
                problems.push(format!(
                    "qdrant: package holds a {collection}.snapshot the manifest does not declare"
                ));
            }
        }
    }

    // objects：声明的文件数与字节数与包内实测一致。两个数都为 0 视作没有这一段
    // （旧格式），跳过——不然一个真有对象的包会拿默认值去撞一次误报。
    if manifest.objects.files != 0 || manifest.objects.bytes != 0 {
        let (files, bytes) = measure_tree(&pkg_dir.join(PACKAGE_OBJECTS_DIR).join(OBJECTS_SUBDIR))?;
        if files != manifest.objects.files || bytes != manifest.objects.bytes {
            problems.push(format!(
                "objects: manifest declares {} file(s)/{} byte(s), package holds {files}/{bytes}",
                manifest.objects.files, manifest.objects.bytes
            ));
        }
    }

    // secrets：声明里每个键都属于某个对象，包内该对象的 `<对象名>.yaml` 必须在。
    // 只核对象粒度——键的内容是 base64，写进 manifest 的只有键名清单。
    if !manifest.secrets_keys.is_empty() {
        let secrets_dir = pkg_dir.join(SECRETS_DIR);
        let mut declared_objects: BTreeMap<String, ()> = BTreeMap::new();
        for key in &manifest.secrets_keys {
            if let Some((object, _)) = key.split_once('/') {
                declared_objects.insert(object.to_string(), ());
            }
        }
        for object in declared_objects.keys() {
            if !secrets_dir.join(format!("{object}.yaml")).is_file() {
                problems.push(format!(
                    "secrets: manifest lists keys of object {object:?} but the package has no {object}.yaml"
                ));
            }
        }
    }

    // 进化 bare 仓的 bundle：清单说带了，包里就必须是那一份；字节数对不上
    // 与「文件不在」是两种不同的坏，分开说。内容能不能打开由种子那一步用
    // `git bundle verify` 判——那是唯一会真正消费它的地方。
    if let Some(bundle) = &manifest.evolution_bundle {
        let path = pkg_dir.join(GIT_DIR).join(EVOLUTION_BUNDLE_FILE);
        match std::fs::metadata(&path) {
            Ok(meta) if meta.len() == bundle.bytes => {}
            Ok(meta) => problems.push(format!(
                "git: manifest declares {} byte(s) for evolution.bundle, package holds {}",
                bundle.bytes,
                meta.len()
            )),
            Err(_) => problems.push(
                "git: manifest reports an evolution bundle but the package has no \
                 git/evolution.bundle"
                    .to_string(),
            ),
        }
    }

    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "package does not match its manifest ({}):\n  - {}",
            pkg_dir.display(),
            problems.join("\n  - ")
        )
        .into())
    }
}

/// 恢复收尾的那一句。
///
/// `restore complete` 对两份完全不同的包是同一句话：一份把该带的都带回来了，另一份
/// 按清单就没带对象层、没带凭证。清单里的 `lossy_surfaces` 恰恰是出包那一刻对「这个
/// 包不带走什么」的声明，而恢复端此前从不读它——于是换机之后要单独补的那几样，只能
/// 靠操作者自己去翻包里的 manifest.json 才看得见。所以收尾把要人动手的面跟在后面。
///
/// 只点 [`SurfaceRecovery::NeedsFollowUp`] 那些：nats / redis / meilisearch 每次恢复
/// 都缺席，把它们也列上会让这句话每次都亮，读的人很快就不再读它。
fn restore_summary(manifest: &BackupManifest) -> String {
    let follow_up: Vec<&str> = manifest
        .lossy_surfaces
        .iter()
        .filter(|s| s.recovery == SurfaceRecovery::NeedsFollowUp)
        .map(|s| s.name.as_str())
        .collect();
    if follow_up.is_empty() {
        return "restore complete".to_string();
    }
    format!(
        "restore complete, but these surfaces still need someone to act: {} (the reasons were logged per surface before the restore started)",
        follow_up.join(", ")
    )
}

/// 恢复入口：`pkg` 可为 .tar.zst 或已解包目录。逐段恢复存在的部分，
/// 缺段记日志不判负——包可能来自只备份了部分段的旧版本。
///
/// 动手恢复之前先拿 manifest 逐段核包（`verify_package_against_manifest`）：
/// 内容与清单对不上就退非零，绝不带着分歧走到 `TRUNCATE` 那一步。
pub async fn run_restore(pkg: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "COGNEVA_DATABASE_URL is required for restore")?;
    let data_dir = PathBuf::from(cog_core::paths::data_dir());

    let extract_root = if pkg.is_dir() {
        None
    } else {
        let root = std::env::temp_dir().join(format!("cogneva-restore-{}", uuid::Uuid::new_v4()));
        Some((root.clone(), untar_package(pkg, &root)?))
    };
    let pkg_dir = match &extract_root {
        Some((_, dir)) => dir.clone(),
        None => pkg.to_path_buf(),
    };

    let manifest_path = pkg_dir.join(MANIFEST_FILE);
    if !manifest_path.exists() {
        return Err(format!("{} missing manifest.json", pkg_dir.display()).into());
    }
    let manifest: BackupManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
    tracing::info!(
        version = %manifest.version,
        revision = %manifest.revision,
        instance = %manifest.instance_fingerprint,
        created_at = %manifest.created_at,
        "restoring package"
    );
    // 清单说这个包里有什么，此刻盘上是事实——两者对不上就停在这里。
    verify_package_against_manifest(&pkg_dir, &manifest)?;
    // 再判代次：包里的字节装得下，不代表本二进制认得它们。
    check_provenance(&manifest)?;
    check_segment_skew(&manifest)?;
    // 清单点名过的损失，在动手之前逐条说清。恢复完再报就晚了：那几样是换机之后
    // 要另外补的东西，操作者得在恢复还没开始时就看见。
    for surface in &manifest.lossy_surfaces {
        tracing::warn!(
            surface = %surface.name,
            lost = %surface.lost,
            acceptable_because = %surface.acceptable_because,
            recovery = ?surface.recovery,
            "this package did not carry this surface"
        );
    }

    let pg_dir = pkg_dir.join(PACKAGE_PG_DIR);
    if pg_dir.exists() {
        let pool = PgPool::connect(&database_url).await?;
        let report = restore_postgres(&pool, &pg_dir).await?;
        tracing::info!(
            tables = report.tables.len(),
            recomputed = report.recomputed.len(),
            "postgres restored"
        );
        if !report.tables_without_a_home.is_empty() {
            // 恢复进程的日志是操作者唯一的现场：这些表的数据没有回来，说清是哪几张。
            tracing::warn!(
                tables = %report.tables_without_a_home.join(","),
                "these package tables no longer exist in this schema and their rows were not restored"
            );
        }
    }

    let qdrant_dir = pkg_dir.join(QDRANT_DIR);
    if qdrant_dir.exists() {
        let qdrant_http_url = std::env::var(QDRANT_HTTP_URL_ENV)
            .map_err(|_| "package has qdrant snapshots but COGNEVA_QDRANT_HTTP_URL is unset")?;
        let qdrant = QdrantHttp::new(&qdrant_http_url);
        for entry in std::fs::read_dir(&qdrant_dir)? {
            let path = entry?.path();
            if let Some(name) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".snapshot"))
            {
                qdrant.upload_snapshot(name, &path).await?;
                tracing::info!(collection = name, "qdrant collection restored");
            }
        }
    }

    let objects_src = pkg_dir.join(PACKAGE_OBJECTS_DIR).join(OBJECTS_SUBDIR);
    if objects_src.exists() {
        let (files, bytes) = copy_tree(&objects_src, &data_dir.join(OBJECTS_SUBDIR))?;
        tracing::info!(files, bytes, "objects restored");
    }

    // 产物层最后恢复，恢复不了就让整次恢复失败：包里的策略库与决策统计是
    // 进化状态的权威面，缺了它们「恢复成功」只是一句结论。
    let artifact_readings = restore_artifacts(&pkg_dir, &manifest, &data_dir).await?;
    for reading in &artifact_readings {
        tracing::info!(reading = %reading, "artifact restored");
    }

    // 进化 bare 仓的未推 rev 只在这个包里有：种回去必须发生在部署器重新启动之前
    // （恢复脚本此刻正把写者停在 0 副本上），否则它先从一个只有公开 rev 的基线
    // 起步，而那之后没有任何读数会说少过东西。
    seed_evolution_bundle(&pkg_dir, &manifest)?;

    if let Some((root, _)) = &extract_root {
        let _ = std::fs::remove_dir_all(root);
    }
    tracing::info!("{}", restore_summary(&manifest));
    Ok(())
}

/// 把包里的进化 bare 仓 bundle 种回目标卷。
///
/// 目标可能是空卷（空白机刚 bootstrap，进化 init 还没跑），也可能是已经由公开
/// 远端 seed 过的仓。后者不能直接 `fetch --force`：公开 main 可能比出包那一刻
/// 更新，把 ref 拨回去会让部署器看到一次倒退。所以逐个 ref 合：目标没有的建出来，
/// 能快进的快进，已经分叉的原样不动并点名——包里的那份仍在包内，不会因为这一步
/// 被抹掉。
fn seed_evolution_bundle(
    pkg_dir: &Path,
    manifest: &BackupManifest,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(report) = &manifest.evolution_bundle else {
        return Ok(());
    };
    let bundle = pkg_dir.join(GIT_DIR).join(EVOLUTION_BUNDLE_FILE);
    let target = match std::env::var("COGNEVA_RESTORE_GIT_REMOTE_DIR") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => {
            if std::env::var("COGNEVA_RESTORE_SKIP_GIT_BUNDLE").as_deref() == Ok("1") {
                let tree = cog_core::paths::TREE_REPO;
                tracing::warn!(
                    bundle_bytes = report.bytes,
                    refs = report.refs,
                    tree,
                    "包里有进化 bare 仓的 bundle，但恢复端没有落点且被显式跳过：未推的 rev 留在包里，\
                     没有种回 {tree}"
                );
                return Ok(());
            }
            return Err(format!(
                "包里有进化 bare 仓的 git bundle，但恢复端没有落点：\
                 设 COGNEVA_RESTORE_GIT_REMOTE_DIR 指向 {} 那一卷；\
                 确认目标不需要进化时显式设 COGNEVA_RESTORE_SKIP_GIT_BUNDLE=1",
                cog_core::paths::TREE_REPO
            )
            .into());
        }
    };
    seed_evolution_bundle_into(&bundle, report, &target)
}

/// 种子本体，落点由调用方给出：恢复入口从 env 取，测试直接给临时目录。
fn seed_evolution_bundle_into(
    bundle: &Path,
    report: &EvolutionBundleReport,
    target: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let meta = std::fs::metadata(bundle).map_err(|e| {
        format!(
            "包里有进化 bare 仓的 bundle 声明，但 {} 打不开：{e}",
            bundle.display()
        )
    })?;
    if meta.len() != report.bytes {
        return Err(format!(
            "包里的 evolution.bundle 是 {} 字节，manifest 声明 {} 字节：这一份不是出包时那一份",
            meta.len(),
            report.bytes
        )
        .into());
    }

    std::fs::create_dir_all(target)?;
    if !target.join("HEAD").is_file() {
        git_ok(target, &["init", "--bare", "."])?;
    }
    let bundle_s = bundle
        .to_str()
        .ok_or("bundle 路径不是 UTF-8，git 命令行给不出来")?;
    git_ok(target, &["bundle", "verify", bundle_s])?;

    // 先把对象与 ref 名一起抓进临时命名空间，再逐个决定目标 ref 动不动。
    git_ok(
        target,
        &[
            "fetch",
            bundle_s,
            "+refs/heads/*:refs/restore/heads/*",
            "+refs/tags/*:refs/restore/tags/*",
        ],
    )?;
    let mut created = 0usize;
    let mut fast_forward = 0usize;
    let mut diverged = Vec::new();
    let heads = git_ok(target, &["bundle", "list-heads", bundle_s])?;
    for line in heads.lines() {
        let mut fields = line.split_whitespace();
        let Some(_sha) = fields.next() else { continue };
        let Some(refname) = fields.next() else {
            continue;
        };
        if refname == "HEAD" {
            continue;
        }
        let (tmp, target_ref) = if let Some(name) = refname.strip_prefix("refs/heads/") {
            (format!("refs/restore/heads/{name}"), refname.to_string())
        } else if let Some(name) = refname.strip_prefix("refs/tags/") {
            (format!("refs/restore/tags/{name}"), refname.to_string())
        } else {
            // 远端跟踪之类不属于权威面的 ref：包里带了也不种。
            continue;
        };
        match merge_bundle_ref(target, &tmp, &target_ref)? {
            BundleRefMerge::Created => created += 1,
            BundleRefMerge::FastForwarded => fast_forward += 1,
            BundleRefMerge::Same => {}
            BundleRefMerge::Diverged => diverged.push(target_ref),
        }
    }
    // 临时命名空间不留在目标仓里：下一轮恢复再建一份，名字不会越攒越多。
    let leftovers = git_ok(
        target,
        &["for-each-ref", "--format=%(refname)", "refs/restore/"],
    )?;
    for refname in leftovers.lines().map(str::trim).filter(|s| !s.is_empty()) {
        let _ = git_run(target, &["update-ref", "-d", refname]);
    }

    // 空白卷上 `git init --bare` 的 HEAD 指向 master；包里是 main 时那个符号引用
    // 指着不存在的 ref，部署器会把它读成「main 不见了」。只在 HEAD 悬空时改。
    let head_ok = git_ok(target, &["symbolic-ref", "-q", "HEAD"])
        .ok()
        .map(|head| {
            git_run(target, &["rev-parse", "--verify", head.trim()])
                .map(|out| out.status.success())
                .unwrap_or(false)
        })
        .unwrap_or(false);
    if !head_ok
        && !report.head.is_empty()
        && git_run(target, &["rev-parse", "--verify", &report.head])
            .map(|out| out.status.success())
            .unwrap_or(false)
    {
        git_ok(target, &["symbolic-ref", "HEAD", &report.head])?;
    }

    tracing::info!(
        target = %target.display(),
        bytes = report.bytes,
        refs = report.refs,
        created,
        fast_forward,
        diverged = diverged.len(),
        head = %report.head,
        commit = %report.commit,
        "进化 bare 仓已从包里的 git bundle 种回"
    );
    if !diverged.is_empty() {
        tracing::warn!(
            refs = %diverged.join(","),
            "这些 ref 在目标仓里已有不同的历史，没有强推覆盖：包里的那份仍在包内"
        );
    }
    Ok(())
}

enum BundleRefMerge {
    Created,
    FastForwarded,
    Same,
    Diverged,
}

fn merge_bundle_ref(
    dir: &Path,
    source: &str,
    target: &str,
) -> Result<BundleRefMerge, Box<dyn std::error::Error>> {
    let source_sha = git_ok(dir, &["rev-parse", "--verify", source])?;
    let source_sha = source_sha.trim();
    let current = git_run(dir, &["rev-parse", "--verify", target])?;
    if !current.status.success() {
        git_ok(dir, &["update-ref", target, source_sha])?;
        return Ok(BundleRefMerge::Created);
    }
    let current_sha = String::from_utf8_lossy(&current.stdout).trim().to_string();
    if current_sha == source_sha {
        return Ok(BundleRefMerge::Same);
    }
    let ancestor = git_run(
        dir,
        &["merge-base", "--is-ancestor", &current_sha, source_sha],
    )?;
    if ancestor.status.success() {
        git_ok(dir, &["update-ref", target, source_sha])?;
        return Ok(BundleRefMerge::FastForwarded);
    }
    // 非零里 1 = 不是祖先（分叉），其余是 git 自己出错。
    if ancestor.status.code() == Some(1) {
        Ok(BundleRefMerge::Diverged)
    } else {
        Err(format!(
            "git merge-base --is-ancestor {current_sha} {source_sha} 失败：{}",
            String::from_utf8_lossy(&ancestor.stderr).trim()
        )
        .into())
    }
}

/// 包与本二进制是不是同一代。
///
/// 三条判据，方向不同：
/// - **数据结构只能向前**：包里的 schema 版本比本二进制新就拒绝——装回去会缺列，
///   而缺列在恢复日志里没有任何症状。包更旧是正常升级路径，放行并记一条。
/// - **源码 rev 要包含包 rev**：不是字符串相等。空白机重装装的是公开仓库当前
///   这一版，而包可能来自更早的一版，那正是正常的升级路径。恢复脚本在有仓库的
///   宿主上用 `git merge-base --is-ancestor` 证明「包 rev 是本源 rev 的祖先」，
///   并把两个 rev 一起交给这里核对。证明不了（源码比包旧、或宿主上没有历史）
///   才拒绝，退路是人显式接受（`COGNEVA_RESTORE_ACCEPT_REV_MISMATCH=1`）。
/// - **镜像引用不同只记不拦**：同一个 rev 可以有多个构建产物，把它当身份会拦住
///   合法恢复。它是排障时的对照，不是判据。
fn check_provenance(manifest: &BackupManifest) -> Result<(), Box<dyn std::error::Error>> {
    check_provenance_against(
        manifest,
        crate::build_info::GIT_REVISION,
        current_schema_version(),
        &RevAcceptance::from_env(),
    )
}

/// 跨代恢复的接受依据。
///
/// 两种来源必须分开记：`ancestry` 是恢复脚本在**有仓库的地方**（宿主上的源码
/// 检出）用 `git merge-base --is-ancestor` 算出来的，绑定了包 rev 与恢复镜像的
/// rev；`explicit` 是人明确说「我知道，装吧」。二进制在 Pod 里没有历史可查，能做
/// 的只有核对这份证明点名的两个 rev 与眼前的包、与自己一致——这至少保证证明不是
/// 从另一次恢复上抄来的。
#[derive(Debug, Default)]
struct RevAcceptance {
    explicit: bool,
    ancestry_package: Option<String>,
    ancestry_source: Option<String>,
}

impl RevAcceptance {
    fn from_env() -> Self {
        Self {
            explicit: std::env::var("COGNEVA_RESTORE_ACCEPT_REV_MISMATCH").as_deref() == Ok("1"),
            ancestry_package: std::env::var("COGNEVA_RESTORE_ANCESTRY_PACKAGE_REV")
                .ok()
                .filter(|v| !v.is_empty()),
            ancestry_source: std::env::var("COGNEVA_RESTORE_ANCESTRY_SOURCE_REV")
                .ok()
                .filter(|v| !v.is_empty()),
        }
    }

    /// 证明只对「它当时量的那两个 rev」有效：包 rev 必须正是 manifest 里那个，
    /// 源 rev 必须正是本二进制自己。换成别的 rev 就退回「没有证明」。
    fn proves_ancestry(&self, package_rev: &str, source_rev: &str) -> bool {
        self.ancestry_package.as_deref() == Some(package_rev)
            && self.ancestry_source.as_deref() == Some(source_rev)
    }
}

/// 判据本体，代次由调用方给出：恢复入口给的是本二进制自己的值，测试给的是
/// 写死的一组——否则「读不到迁移目录」的那台机器上这条判据会悄悄退化成空跑。
fn check_provenance_against(
    manifest: &BackupManifest,
    current_rev: &str,
    current_schema: i64,
    acceptance: &RevAcceptance,
) -> Result<(), Box<dyn std::error::Error>> {
    if manifest.schema_version > 0 && current_schema > 0 {
        if manifest.schema_version > current_schema {
            return Err(format!(
                "包的 schema 版本 {} 比本二进制的 {} 新：本二进制没有那些迁移，装回去会缺列。\
                 用出包那一代的镜像恢复，或先升级引擎",
                manifest.schema_version, current_schema
            )
            .into());
        }
        if manifest.schema_version < current_schema {
            tracing::info!(
                package = manifest.schema_version,
                binary = current_schema,
                "包是更早的 schema：按迁移往前装"
            );
        }
    } else {
        tracing::info!("包或二进制没有声明 schema 版本（旧格式），不判代次");
    }

    if !manifest.revision.is_empty() && manifest.revision != current_rev {
        // 前进方向的恢复是常态：空白机装的是公开仓库当前这一版，而包可能来自
        // 几天前。判据不是「rev 相等」，而是「包那一版被本源包含」——这个事实
        // 只有仓库能回答，所以由恢复脚本在宿主上算好，二进制核对它点名的对象。
        let ancestry = acceptance.proves_ancestry(&manifest.revision, current_rev);
        if !ancestry && !acceptance.explicit {
            return Err(format!(
                "包来自源码 rev {}，本二进制是 {current_rev}；跨代恢复要么由恢复脚本\
                 证明包 rev 是本源的一个祖先（用宿主上的源码检出跑 git merge-base，\
                 脚本会自动带上证明），要么显式接受：设 COGNEVA_RESTORE_ACCEPT_REV_MISMATCH=1 后重跑",
                manifest.revision
            )
            .into());
        }
        if ancestry {
            tracing::info!(
                package = %manifest.revision,
                binary = current_rev,
                "包 rev 是本二进制 rev 的祖先（恢复脚本在源码仓库里核实过），按前进方向安装"
            );
        } else {
            tracing::warn!(
                package = %manifest.revision,
                binary = current_rev,
                "跨代恢复已被显式接受"
            );
        }
    }

    if !manifest.image.is_empty() {
        tracing::info!(
            package_image = %manifest.image,
            current_image = %std::env::var(IMAGE_REF_ENV).unwrap_or_default(),
            "包的镜像引用（只作对照，不拦恢复）"
        );
    }
    Ok(())
}

/// 把包里的产物层写回每个数据根，并在落地的字节上逐库验一遍。
///
/// 演练那一侧证明的是「包里的字节读得回来」；写回这一侧要回答的是「换一台机器
/// 装得上，装上去的那一份还是完好的」。两件事不能互相代替：只备份不写回，
/// 换机后策略库是空的、进程照样起得来；写回了不验，ACTIVE 指向一个不存在的
/// 版本也照样能放进目录。
///
/// 目标根的取法与备份侧同一条：主根是 `COGNEVA_DATA_DIR`，追加根由
/// `COGNEVA_RESTORE_EXTRA_DATA_ROOTS` 给出。包里有一个根而恢复端给不出目标路径
/// 时**整次失败**——静默跳过就是换机后少了那份状态而回执说成功。
async fn restore_artifacts(
    pkg_dir: &Path,
    manifest: &BackupManifest,
    data_dir: &Path,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if manifest.artifacts.is_empty() {
        // 旧格式的包没有这一段：缺段不是错，是当时没有这个段。
        return Ok(Vec::new());
    }
    let mut targets: Vec<(String, PathBuf)> = vec![("app".to_string(), data_dir.to_path_buf())];
    targets.extend(parse_extra_data_roots(
        RESTORE_EXTRA_DATA_ROOTS_ENV,
        std::env::var(RESTORE_EXTRA_DATA_ROOTS_ENV).ok(),
    )?);
    restore_artifacts_into(pkg_dir, manifest, &targets).await
}

/// 目标表由调用方给出，这一半不读 env——`run_restore` 从 env 装配，测试直接给表。
async fn restore_artifacts_into(
    pkg_dir: &Path,
    manifest: &BackupManifest,
    targets: &[(String, PathBuf)],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut readings = Vec::new();
    for root in &manifest.artifacts {
        let target = targets
            .iter()
            .find(|(name, _)| name == &root.root)
            .map(|(_, path)| path.clone())
            .ok_or_else(|| {
                format!(
                    "包里有数据根 {:?}（备份时挂在 {}），恢复端没有它的目标路径；\
                     用 COGNEVA_RESTORE_EXTRA_DATA_ROOTS 给出 name=path",
                    root.root, root.data_dir
                )
            })?;
        restore_artifact_root(pkg_dir, root, &target, &mut readings).await?;
        write_restore_expectation(manifest, root, &target)?;
    }
    Ok(readings)
}

/// 把「这个数据根里刚写回了什么」留在数据根里，等新进程开机时对。
///
/// 恢复端的成功与「新进程真的加载了这份字节」不是一件事：卷换错、`data_dir` 指到
/// 别处、Pod 挂在另一块 PVC 上，恢复 Job 全都报成功。唯一能把两件事接起来的地方是
/// 进程自己——而它开机时只有数据根可读，所以期望值必须写在这里。
///
/// 放在 `meta_learning/` 下：那一层已经被「一级子目录都要分类」的门管着，为一张
/// 一次性单子在数据根顶层新开一格，等于让每个部署都为它写一条分类。进程核对通过
/// 后会把它删掉，它不会进下一次的备份包（产物层只收 `policies/` 与
/// `meta_learning/decision_stats.json`）。
fn write_restore_expectation(
    manifest: &BackupManifest,
    root: &ArtifactRootReport,
    target: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let expectation = cog_reflection::RestoreExpectation {
        version: cog_reflection::RestoreExpectation::VERSION,
        package_created_at: manifest.created_at.clone(),
        package_revision: manifest.revision.clone(),
        policy_stores: root
            .policy_stores
            .iter()
            .map(|store| cog_reflection::ExpectedStore {
                name: store.name.clone(),
                active_version: store.active_version,
            })
            .collect(),
        decision_trials: root.decision_stats.as_ref().map(|stats| stats.trials),
    };
    // 包里这一根什么都没带：没有可对的对象，也就不该留一张空单子让进程去核对。
    if expectation.policy_stores.is_empty() && expectation.decision_trials.is_none() {
        return Ok(());
    }
    // 目录由交接单自己的位置决定：它落在哪一层，那一层就是要建的。
    let path = cog_reflection::RestoreExpectation::path_in(target);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&expectation)?)?;
    Ok(())
}

/// 一个数据根：先落到目标根下的临时目录，验过再换到正式位置。
///
/// 直接往正式位置上写，会把「验不过」留在现网那一份里；先写暂存盘再换位，
/// 坏包只会让一个 `.restore-*` 目录被删掉，正式位置仍是换位前的那一份。
async fn restore_artifact_root(
    pkg_dir: &Path,
    root: &ArtifactRootReport,
    target: &Path,
    readings: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let base = pkg_dir.join(ARTIFACTS_DIR).join(&root.root);
    std::fs::create_dir_all(target)?;
    let scratch = target.join(format!(".restore-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&scratch)?;
    let outcome = stage_artifact_root(&base, root, target, &scratch, readings).await;
    let _ = std::fs::remove_dir_all(&scratch);
    outcome
}

async fn stage_artifact_root(
    base: &Path,
    root: &ArtifactRootReport,
    target: &Path,
    scratch: &Path,
    readings: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    if root.policies_present {
        let src = base.join(cog_reflection::POLICIES_SUBDIR);
        if !src.is_dir() {
            return Err(format!("{} 不在包里，但清单说这个根有策略库", src.display()).into());
        }
        let staged = scratch.join(cog_reflection::POLICIES_SUBDIR);
        copy_policy_tree(&src, &staged)?;
        // 清单是一条一条写的，多出来的库没有声明就没有可比的对象——不许静默
        // 写进去，也不许静默丢掉。两边都数，对不上就停。
        let packaged = count_store_dirs(&src)?;
        if packaged != root.policy_stores.len() as u64 {
            return Err(format!(
                "{} 里有 {packaged} 个策略库，清单只声明了 {}",
                src.display(),
                root.policy_stores.len()
            )
            .into());
        }
        // 验的是**将要落地的那一份**，不是包里的原件：拷贝本身就是一次可能出错
        // 的搬运，验原件会把这一步的错误放过。
        let store = cog_reflection::PolicyStore::new(staged.clone());
        for reported in &root.policy_stores {
            let line = verify_one_store(&store, &root.root, reported)
                .await
                .map_err(|e| format!("{}/policies/{}: {e}", root.root, reported.name))?;
            readings.push(line);
        }
        sync_tree(&staged)?;
        replace_dir(&staged, &cog_reflection::policies_path(target))?;
    }

    if let Some(stats) = &root.decision_stats {
        let src = base
            .join(cog_reflection::DATA_SUBDIR)
            .join(cog_reflection::DECISION_STATS_FILE);
        let staged = scratch.join(cog_reflection::DECISION_STATS_FILE);
        std::fs::copy(&src, &staged)?;
        std::fs::File::open(&staged)?.sync_all()?;
        let line = verify_one_stats(&staged, &root.root, stats)
            .map_err(|e| format!("{}/meta_learning/decision_stats.json: {e}", root.root))?;
        readings.push(line);
        let dir = target.join(cog_reflection::DATA_SUBDIR);
        std::fs::create_dir_all(&dir)?;
        std::fs::rename(&staged, dir.join(cog_reflection::DECISION_STATS_FILE))?;
        std::fs::File::open(&dir)?.sync_all()?;
    }

    if let Some(workspace) = &root.workspace {
        let src = base.join(cog_gateway::ide::WORKSPACE_SUBDIR);
        if !src.is_dir() {
            return Err(format!("{} 不在包里，但清单说这个根有工作区", src.display()).into());
        }
        let staged = scratch.join(cog_gateway::ide::WORKSPACE_SUBDIR);
        let (files, bytes) = copy_tree(&src, &staged)?;
        if files != workspace.files || bytes != workspace.bytes {
            return Err(format!(
                "{}: 抄过去的是 {files} 个文件 / {bytes} 字节，清单声明 {} 个 / {} 字节",
                src.display(),
                workspace.files,
                workspace.bytes
            )
            .into());
        }
        sync_tree(&staged)?;
        replace_dir(&staged, &cog_gateway::ide::workspace_root(target))?;
        readings.push(format!(
            "{}: 工作区 {files} 个文件 / {bytes} 字节（清单一致）",
            root.root
        ));
    }
    Ok(())
}

/// 子目录数，口径与 `describe_policy_stores` 一致：只数名字能读成 UTF-8 的目录。
fn count_store_dirs(policies_dir: &Path) -> std::io::Result<u64> {
    let mut count = 0;
    for entry in std::fs::read_dir(policies_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.file_name().to_str().is_some() {
            count += 1;
        }
    }
    Ok(count)
}

/// 每个文件与目录都 sync 一遍，最后 sync 父目录。
///
/// 恢复完就可能断电：没有这一步，rename 成功而目录项还在页缓存里，机器起来
/// 看到的是一个空目录——「恢复成功」变成了「什么都没恢复」。顺序是子先父后，
/// 父目录的 sync 必须发生在子项都落盘之后才有意义。
fn sync_tree(dir: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            std::fs::File::open(entry.path())?.sync_all()?;
        }
    }
    std::fs::File::open(dir)?.sync_all()
}

/// 把一个目录换到正式位置：旧目录先挪到旁边，新目录再挪进来，挪失败就把旧的
/// 挪回去。
///
/// 不做「先删旧的再 rename 新的」：那中间有一段时间正式位置是空的，此刻崩掉就
/// 只剩下一个要人工拼的半成品。旧目录改名到 `.old-*` 之后立刻可回滚；仅当两步
/// rename 之间断电才会留下「只有 `.old-*`、没有正式目录」的现场——恢复没有完成，
/// 重跑一次从包里再落一份即可，原有字节还在盘上。
fn replace_dir(staged: &Path, live: &Path) -> std::io::Result<()> {
    let parked = live.with_file_name(format!(
        ".{}.old-{}",
        live.file_name().unwrap_or_default().to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let had_live = live.exists();
    if had_live {
        std::fs::rename(live, &parked)?;
    }
    if let Err(e) = std::fs::rename(staged, live) {
        if had_live {
            let _ = std::fs::rename(&parked, live);
        }
        return Err(e);
    }
    if had_live {
        let _ = std::fs::remove_dir_all(&parked);
    }
    Ok(())
}

/// 演练用的临时库名：固定一个，不按次取。
///
/// 固定的名字让「上一次崩掉的演练留下了什么」有确定的答案——这一次开工先把它
/// 删掉。按次取名字会让每一次崩溃都在集群的 PG 里留下一个 1 GB 的残库，而没人
/// 会去数有几个。
pub const DRILL_DATABASE: &str = "cog_drill";

/// 演练互斥锁的键。取自 `cog_dril` 的 ASCII，只要求集群内不与人撞车，不承担含义。
// cap: bound — 取自 cog_dril 的 ASCII，只要求集群内不与人撞车，不承担含义
const DRILL_LOCK_KEY: i64 = 0x636f_675f_6472_696c;

/// 演练配置。只要库、卷与迁移目录。
///
/// 不复用 [`BackupConfig`]：它要求 qdrant 端点，而演练不碰向量与对象层——那两段
/// 要真恢复就得有一个空白目标环境，这一趟要证明的是「包里的关系数据与它自己的账
/// 能装回一个跑完迁移的空库」。让演练 Job 为此背上一个 qdrant 地址，是把一个能
/// 跑的进程挡在一个它用不上的依赖后面。
#[derive(Clone)]
pub struct DrillConfig {
    pub database_url: String,
    pub backup_dir: PathBuf,
    /// 备份卷之外的第二个落点，与写者读同一个 env。`None` = 这个安装没有第二
    /// 个落点，演练会把这件事实记成空列；读那一面的规则由「从来没有过一份」来判。
    pub offsite_dir: Option<PathBuf>,
    /// 宿主那一跑从 restic 仓库里观察到的时刻写在哪里（`observation.json`）。
    ///
    /// 这是「另有一份在别处」与「钥匙多久没换」唯一不靠自报的来源：写者说
    /// 自己上传成功，不等于仓库里有快照；凭证文件的 mtime 是上次重封的时刻，
    /// 重封不等于换钥匙。`None` = 这个安装没有这条观察通道，账本两列留空，
    /// 读作「没量过」，不是「刚换过」。
    pub offsite_state_dir: Option<PathBuf>,
    /// 迁移目录：演练要在一个空库上把表结构建出来，用的必须是**这个镜像**里的
    /// 迁移，而不是别处的。镜像把迁移装在 `/opt/cogneva/crates/cog-storage/migrations`。
    pub migrations_dir: PathBuf,
    /// 这一趟开哪一份包；`None` 是**最新那一份**。
    ///
    /// 保留窗口里的旧包只能从这里走到：包与读端不同步的问题只在它们身上出现
    /// （旧格式的列、包里带着生成列），而每天只开最新一份时，这些包要到真正
    /// 需要恢复的那天才第一次被打开。
    pub package: Option<String>,
    /// 这一趟除了最新那份，还开一份**窗口里最久没有被成功打开过**的包。
    ///
    /// 只开最新那份证明不了保留窗口的其余成员：旧包与读端不同步的问题只在它们
    /// 身上出现，而它们要到真正需要恢复的那天才第一次被打开。
    pub also_unproven: bool,
    /// 单趟预算：一份包从解包到对数完最多花多久。到点由这一跑自己收尾并落一行
    /// `failed`，而不是等 Job 的界把进程杀掉——被杀掉的那一趟写不出任何东西。
    pub pass_budget: Duration,
}

impl DrillConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let database_url = std::env::var(cog_core::env::DATABASE_URL)
            .map_err(|_| "COGNEVA_DATABASE_URL is required for backup-drill")?;
        let app_dir = cog_core::paths::app_dir();
        let (package, also_unproven) = requested_package(std::env::args().skip(1))?;
        Ok(Self {
            database_url,
            backup_dir: PathBuf::from(
                std::env::var(BACKUP_DIR_ENV).unwrap_or_else(|_| "/backups".into()),
            ),
            offsite_dir: std::env::var(BACKUP_OFFSITE_DIR_ENV)
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            offsite_state_dir: std::env::var("COGNEVA_BACKUP_OFFSITE_STATE_DIR")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            migrations_dir: cog_storage::migrate::migrations_dir(&app_dir),
            package,
            also_unproven,
            pass_budget: pass_budget_from_env(),
        })
    }
}

/// 单趟预算：`COGNEVA_BACKUP_DRILL_PASS_BUDGET_SECS`，没配或读不成数就用默认值。
///
/// 读不成数不报错：一个拼错的 env 让演练整场起不来，比让它按默认值跑一趟更坏——
/// 起不来的那一晚，账上没有任何一行说为什么。
fn pass_budget_from_env() -> Duration {
    pass_budget_from(std::env::var(DRILL_PASS_BUDGET_ENV).ok().as_deref())
}

/// 解析那一格。`None`、空串、零、非数一律落到默认值：这四种写法在这里是同一件事
/// （没给出可用的界），而它们各自的来历留给读 env 的人，不该让演练从此起不来。
fn pass_budget_from(raw: Option<&str>) -> Duration {
    let secs = raw
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(DRILL_PASS_BUDGET_DEFAULT_SECS);
    Duration::from_secs(secs)
}

/// `backup-drill` 自己在子命令之后认的那个操作数。
///
/// cli 把子命令之后的一切原样留给它（与 `restore <package>` 同一形状），所以
/// 这里不认得的写法要自己报错：一个拼错的 `--package` 会静默落回「开最新那份」，
/// 而那一趟的结论看起来和点名开旧包的成功一模一样。
fn requested_package(args: impl Iterator<Item = String>) -> Result<(Option<String>, bool), String> {
    let mut args = args;
    // 第一个词是子命令名本身。
    let _ = args.next();
    let mut package = None;
    let mut also_unproven = false;
    while let Some(arg) = args.next() {
        if arg == "--package" {
            let name = args
                .next()
                .ok_or_else(|| "--package 后面要跟一个包名".to_string())?;
            package = Some(name);
        } else if arg == "--also-unproven" {
            also_unproven = true;
        } else {
            return Err(format!("backup-drill 不认这个参数：{arg}"));
        }
    }
    if package.is_some() && also_unproven {
        return Err("--package 与 --also-unproven 指的不是同一件事，只能用一个".into());
    }
    Ok((package, also_unproven))
}

/// 卷上最新那份包的属主与「此刻读它的进程」不是同一个人，就说清是谁不对。
///
/// 比较的是**包的属主**与**读包的进程**，两边任一变动都会让这一格红：把 Job 换成
/// 非 root 之后属主没跟上、或者有人以 root 往卷里补了一份包。它不是配置项——一个
/// 可配的「期望 uid」既能配错，也能在两边一起漂走时继续绿。
fn package_owner_violation(package: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(package).ok()?;
    let reader = process_uid()?;
    owner_violation(meta.uid(), reader, &package.display().to_string())
}

/// 判词本身，与文件系统和进程无关，单独可测。
fn owner_violation(owner: u32, reader: u32, package: &str) -> Option<String> {
    if owner == reader && reader != 0 {
        return None;
    }
    Some(format!(
        "卷上最新那份包 {package} 的属主是 uid {owner}，读它的进程是 uid {reader}：\
         备份与演练必须跑在同一个专用非 root uid 上。uid 0 时「包只有属主可读」不是隔离，\
         两个 uid 不同则是有人以另一个身份往卷里写了包——两种都要人来处理，不是包的问题"
    ))
}

/// 读本进程的 uid。Linux 上是 `/proc/self/status` 的 `Uid:` 行（真实、有效、保存、
/// 文件系统四个值，取第一个即真实 uid），不为一件事引一个依赖。
fn process_uid() -> Option<u32> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|uid| uid.parse().ok())
}

/// 打开的这一份包，是不是它名字里那一跑写出来的。
///
/// 路径与字节数回答不了这件事：把一份旧包的字节复制成一个新名字，名字对、大小对、
/// 卷上「最新那份」指向它，演练会照常 ok，而它考的是几个月前那一晚。包内
/// `manifest.created_at` 是写出这份包的那一跑落盘前读的钟，账本里同一个 run_id 的
/// `started_at`/`finished_at` 是那一跑的起止：包内时刻落在窗外，就说明名字与内容
/// 不是同一跑。账本里没有这一跑（包比账本早）时不判——那是另一件事，不该在这里
/// 借「开错包」的名义红。
async fn package_belongs_to_its_run(
    live: &PgPool,
    name: &str,
    manifest_created_at: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let Ok(created_at) = DateTime::parse_from_rfc3339(manifest_created_at) else {
        return Ok(Some(format!(
            "包 {name} 的清单里 created_at 读不出时刻（{manifest_created_at}）：\
             这一份是不是它名字说的那一跑写的，没有可对照的数"
        )));
    };
    let created_at = created_at.with_timezone(&Utc);
    let run_id = name.strip_suffix(PACKAGE_SUFFIX).unwrap_or(name);
    let row: Option<(DateTime<Utc>, Option<DateTime<Utc>>)> =
        sqlx::query_as("SELECT started_at, finished_at FROM backup_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(live)
            .await?;
    let Some((started_at, finished_at)) = row else {
        return Ok(None);
    };
    let Some(finished_at) = finished_at else {
        // 账本里这一跑还没收尾：不是这一份包的问题，收尾那一行有它自己的读者。
        return Ok(None);
    };
    Ok(run_window_violation(
        name,
        created_at,
        started_at,
        finished_at,
    ))
}

/// 盘上这一份包的字节，是不是写它那一跑记下的那一份。
///
/// 问的是账上新开的那一列，不是重新打一遍包：写端算摘要的时刻在包落盘之后、
/// 交给卷外复制之前，所以读者算出来的不同就只能是「这之后字节变了」。
async fn package_checksum_mismatch(
    live: &PgPool,
    name: &str,
    package: &Path,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let run_id = name.strip_suffix(PACKAGE_SUFFIX).unwrap_or(name);
    let recorded: Option<Option<String>> =
        sqlx::query_scalar("SELECT package_checksum FROM backup_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(live)
            .await?;
    let Some(recorded) = recorded else {
        return Ok(None);
    };
    if recorded.is_none() {
        // 这一次改动之前写的包没有这一列。不是缺陷，但必须有人看得见「这一格比不了」，
        // 否则一次写入侧的回归（摘要再也记不进去）会以同样的形状沉默。
        tracing::warn!(
            package = %name,
            "账上没有这一份包的摘要，校验和这一格比不了"
        );
        return Ok(None);
    }
    let actual = package_digest(package)?;
    Ok(checksum_violation(name, recorded.as_deref(), &actual))
}

/// 包内时刻该落在这一跑的起止之间，判词单独可测。
fn run_window_violation(
    name: &str,
    created_at: DateTime<Utc>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> Option<String> {
    // 一分钟的宽容只用来吸收时钟粒度，不用来吸收「差了几天」。
    let slack = chrono::Duration::seconds(60);
    if created_at >= started_at - slack && created_at <= finished_at + slack {
        return None;
    }
    let run_id = name.strip_suffix(PACKAGE_SUFFIX).unwrap_or(name);
    Some(format!(
        "包 {name} 里的时刻 {created_at} 不在账本里 {run_id} 那一跑的起止之间\
         （{started_at} … {finished_at}）：名字与内容是两跑的东西，这份字节不是它名字说的那一份"
    ))
}

/// 没点名时，默认那一趟必须打开卷上最新那份。
fn default_pass_must_open_the_newest(
    chosen: &Path,
    packages: &[PathBuf],
    named: bool,
) -> Option<String> {
    if named {
        return None;
    }
    let newest = packages.last()?;
    if chosen == newest {
        return None;
    }
    Some(format!(
        "演练默认该打开卷上最新那份 {}，实际打开的是 {}：这是选择规则的问题，不是包的问题",
        newest.display(),
        chosen.display()
    ))
}

/// 这一趟开哪一份包。
///
/// 默认最新那一份，与本次改动之前的行为一致；点名时接受带不带 `.tar.zst` 两种写法。
/// 名字不在卷上就报错并列出可选的名字——演练点名开一份不存在的包，若悄悄退回最新，
/// 「旧包也能恢复」这句话就会由一次开新包的运行来背书。
fn choose_package(
    packages: &[PathBuf],
    requested: Option<&str>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let Some(want) = requested else {
        return packages
            .last()
            .cloned()
            .ok_or_else(|| "备份目录里没有任何备份包".into());
    };
    let want = want.strip_suffix(PACKAGE_SUFFIX).unwrap_or(want);
    let name_of = |p: &PathBuf| {
        p.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.strip_suffix(PACKAGE_SUFFIX).unwrap_or(n).to_string())
    };
    packages
        .iter()
        .find(|p| name_of(p).as_deref() == Some(want))
        .cloned()
        .ok_or_else(|| {
            let available: Vec<String> = packages.iter().filter_map(name_of).collect();
            format!(
                "卷上没有叫 {want} 的包；现有 {} 份：{}",
                available.len(),
                available.join(", ")
            )
            .into()
        })
}

/// 第二趟的候选：除最新那份以外的全部。
///
/// 最新那份由第一趟打开，不参加第二趟的排序：它刚写出来、当然从没成功过，若留在
/// 候选里，「从没成功过者优先」会让第二趟每晚都挑中它，旧包一份也轮不到。
fn older_than_newest(packages: &[PathBuf]) -> &[PathBuf] {
    &packages[..packages.len().saturating_sub(1)]
}

/// 窗口里最该被打开的那一份：从没成功打开过的最先，其次上次成功最早的。
///
/// 按卷上的顺序（名字即时间，最老的在前）走一遍：同是「从没成功过」时先到先得，
/// 于是最老的那份先被选中；都有历史时取上次成功最早的那份。纯函数，查询留给调用方。
fn pick_least_recently_proven(
    names: &[String],
    proven_at: &BTreeMap<String, DateTime<Utc>>,
) -> Option<String> {
    let mut best: Option<(&String, Option<DateTime<Utc>>)> = None;
    for name in names {
        let last = proven_at.get(name).copied();
        let better = match (&best, last) {
            (None, _) => true,
            // 已经选中一个从没成功过的：后面同类的不能顶掉它（先到＝更老）。
            (Some((_, None)), _) => false,
            (Some((_, Some(_))), None) => true,
            (Some((_, Some(best_at))), Some(at)) => at < *best_at,
        };
        if better {
            best = Some((name, last));
        }
    }
    best.map(|(name, _)| name.clone())
}

/// 问源库：卷上这些包里，哪一份距上次成功打开最久（从没成功过者优先）。
///
/// 问的是 `backup_drills` 里**成功**的行——失败的行不证明任何东西。卷上的名字由
/// 调用方自己走目录得到，不用写者声称的那组：两处的理由是一样的。
async fn least_recently_proven(
    live: &PgPool,
    packages: &[PathBuf],
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let names: Vec<String> = packages
        .iter()
        .filter_map(|p| p.file_name())
        .filter_map(|n| n.to_str())
        .map(str::to_string)
        .collect();
    if names.is_empty() {
        return Ok(None);
    }
    let rows: Vec<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT package, max(finished_at) FROM backup_drills \
         WHERE error IS NULL AND package = ANY($1) GROUP BY package",
    )
    .bind(&names)
    .fetch_all(live)
    .await?;
    let proven_at: BTreeMap<String, DateTime<Utc>> = rows
        .into_iter()
        .filter_map(|(name, at)| at.map(|at| (name, at)))
        .collect();
    Ok(pick_least_recently_proven(&names, &proven_at))
}

/// 两份文件是不是同一份：同名、同字节数。
///
/// 演练只用它回答「卷外那是不是卷上最新那份」。名字相同而大小不同是复制写到一半，
/// 名字不同是它停在了更早的那一晚——两种都读作「不是同一份」，原因留给读的人。
fn same_file(a: &Path, b: &Path) -> bool {
    a.file_name() == b.file_name()
        && std::fs::metadata(a).ok().map(|m| m.len()) == std::fs::metadata(b).ok().map(|m| m.len())
}

/// 一份包的摘要，算法名跟着值一起存。
///
/// 字节数说的是「写了多少」，摘要说的是「还是不是同一份」。两者在这一格分道：
/// 一份复制被截断又补零、盘上读回来的是别的块、同名文件被覆盖成同样大小的一份，
/// 字节数一格都不动。算法名写进值里，是为了将来换算法时账上两种值不会互相比较。
fn package_digest(path: &Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    // 分块读而不是整个读进内存：包按百 MB 计，而这一路和导出、打包跑在同一个
    // 内存受限的容器里。
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

/// 这个包覆盖了哪些数据，按清单自己的分段写成一串 JSON。
///
/// 为什么要有这一格：其余每一列都是从包**外面**量的——多少字节、什么时候跑完、上一次
/// 演练开不开得开。它们对「包里装着整套权威面」和「包里只装了一个 Secret」给出同样的
/// 读数：两种都是几百字节到几百 MB、都收尾 ok、都装得回去。清单说出了差在哪，而它躺在
/// 包里面，不开一份包就读不到。
///
/// 段名与清单里的段名一致（postgres／qdrant／objects／artifacts／secrets），空数组读作
/// 「这一段一份都没有」，与「这一段没声明」分开：旧包的清单里本来就没有 artifacts 段，
/// 而今天一段都没有是事实。
fn package_coverage(manifest: &BackupManifest) -> String {
    serde_json::json!({
        "secrets": manifest.secrets_keys,
        "postgres": {
            "tables": manifest
                .postgres
                .tables
                .iter()
                .map(|t| serde_json::json!([t.name, t.rows]))
                .collect::<Vec<_>>(),
            "sequences": manifest.postgres.sequences,
        },
        "qdrant": manifest
            .qdrant
            .iter()
            .map(|c| serde_json::json!([c.name, c.points]))
            .collect::<Vec<_>>(),
        "objects": {"files": manifest.objects.files, "bytes": manifest.objects.bytes},
        "artifacts": manifest
            .artifacts
            .iter()
            .map(|r| serde_json::json!([
                r.root,
                r.policy_stores.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ]))
            .collect::<Vec<_>>(),
    })
    .to_string()
}

/// 账上记的摘要和盘上此刻算出来的是否同意。
///
/// `None` 表示这一跑没记摘要（这一次改动之前写的包），**不是**同意：没有任何东西
/// 可比的包，这一格什么也没证明。调用点为此留一条日志，好让「没记」和「记了又对上」
/// 不共享同一个沉默。
fn checksum_violation(name: &str, recorded: Option<&str>, actual: &str) -> Option<String> {
    let recorded = recorded?;
    (recorded != actual).then(|| {
        format!(
            "包 {name} 的字节与写它那一跑记下的摘要对不上：账上 {recorded}，盘上 {actual}。\
             字节数可能一格没变（截断补零、坏块、被同名文件覆盖），变的正是包里的数据"
        )
    })
}

/// 解包用的临时目录，离开作用域就删。
///
/// 包解出来是几 GB 的 CSV，失败路径上也必须清掉：演练每天跑一次，一个不清的
/// 失败路径会按天把容器的可写层填满。
struct TempTree(PathBuf);

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 连接串的 `host:port/db`，去掉 userinfo 与查询串。
///
/// 账本要记的是「读到哪儿／装到哪儿」，不是「用什么身份连的」：口令留在行里等于
/// 把一份凭证抄进备份账本，而账本随包被复制到卷外与异地。userinfo 取**最后一个**
/// `@` 之前的部分——口令里可以有 `@`。
fn endpoint_of(url: &str) -> String {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, p),
        None => (rest, ""),
    };
    let host = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    let database = path.split_once('?').map(|(d, _)| d).unwrap_or(path);
    if database.is_empty() {
        host.to_string()
    } else {
        format!("{host}/{database}")
    }
}

/// 从连接串里换掉数据库名。
///
/// 口令里可能有 `/`、`?` 之类会破坏朴素字符串替换的字符，所以从 `://` 之后
/// **第一个** `/` 切——那之前的整段是 authority（含 userinfo 与端口），不碰它。
fn with_database(url: &str, database: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    match rest.split_once('/') {
        Some((authority, path_and_query)) => {
            let query = path_and_query
                .split_once('?')
                .map(|(_, q)| format!("?{q}"))
                .unwrap_or_default();
            format!("{scheme}://{authority}/{database}{query}")
        }
        // 连接串里没有路径：数据库名取默认值，换名就是把路径补上。
        None => format!("{scheme}://{rest}/{database}"),
    }
}

/// 删掉演练库，连同它上面可能还挂着的连接。
///
/// `DROP DATABASE` 在有别的连接时拒绝执行，而崩溃过一次的演练会在服务端留下
/// 一个自己的连接直到 TCP 超时。先掐掉再删，否则「重新开始」会被上一次的残骸
/// 挡住，且挡住的形状是一个数据库错误而不是「上次没清干净」。
async fn drop_drill_database(live: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(DRILL_DATABASE)
    .execute(live)
    .await?;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {DRILL_DATABASE}"))
        .execute(live)
        .await?;
    Ok(())
}

/// `backup_runs` 逐格比对的摘要：把给定标识的那些行按列文本拼起来取 md5。
///
/// 比的是**行文本**而不是几列：包里的 CSV 灌回来之后，任何一列不同都会让摘要
/// 不同，而逐列比较要在这里再抄一遍表结构，抄漏的那一列就永远比不到。
/// 标识由调用方从演练库读出来，两边都用同一份——用「源库有哪些行」去问会让
/// 备份之后才终局的行混进来，那种行在包里本来就不存在。
const RUNS_DIGEST_SQL: &str = "\
SELECT coalesce(md5(string_agg(t::text, E'\\n' ORDER BY t.run_id)), '') \
FROM backup_runs t WHERE run_id = ANY($1)";

/// 宿主定时器从 restic 仓库里观察到的东西。
///
/// 由 `backup-offsite-restic.sh` 写出：`observed_at` 是这一跑看仓库的时刻，
/// `newest_snapshot_at` 是 `restic snapshots` 报出的最新一个快照的时刻，
/// `key_created_at` 是 `restic key list` 里最新一把钥匙的创建时刻。
///
/// 两个仓库侧时刻都可以是 `null`——仓库里还没有快照，或者一个从没 init 过的
/// 仓库。那是「观察到了，里面没有」；`observed_at` 存在的意义就是把它与「没人
/// 观察」分开：后者没有这一行，前者有这一行、只是两个值是空。没有 `observed_at`
/// 的旧文件按 mtime 补上，文件在现场就说明有人看过。
#[derive(serde::Deserialize)]
struct OffsiteObservation {
    #[serde(default)]
    observed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    newest_snapshot_at: Option<DateTime<Utc>>,
    #[serde(default)]
    key_created_at: Option<DateTime<Utc>>,
}

/// 读观察文件。不在就是没有观察过，不是错误；读不懂要留一条日志，因为那说明
/// 写者与读者对形状的理解已经分叉，而这一面会因此一直读作「没量过」。
fn read_offsite_observation(dir: &Path) -> Option<OffsiteObservation> {
    let path = dir.join("observation.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "异地仓库的观察文件读不出来，这一面按没量过记");
            return None;
        }
    };
    match serde_json::from_str::<OffsiteObservation>(&text) {
        Ok(mut observed) => {
            if observed.observed_at.is_none() {
                observed.observed_at = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .ok()
                    .map(DateTime::<Utc>::from);
            }
            Some(observed)
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "异地仓库的观察文件不是这一版认得的样子，这一面按没量过记");
            None
        }
    }
}

/// 演练：把一个包装进空库，读它装成了什么。
///
/// 失败不返回 `Err` 而是落进 `error`：一次演练的失败是要**被记录下来**的结果，
/// 不是这个函数自己的故障。调用方连不上源库是唯一的例外——那时也没有地方把结果
/// 写下去了。
pub async fn run_drill(cfg: &DrillConfig, live: &PgPool) -> BackupDrillRecord {
    let started_at = Utc::now();
    let mut drill = BackupDrillRecord {
        drill_id: format!("drill-{}", started_at.format(TIMESTAMP_FORMAT)),
        started_at,
        finished_at: started_at,
        package: None,
        package_written_at: None,
        package_bytes: None,
        restore_source: None,
        restore_target: None,
        segment_skew_seconds: None,
        segment_skew_bound_seconds: None,
        packages_on_volume: 0,
        oldest_package_at: None,
        offsite_newest_at: None,
        offsite_matches_newest: None,
        offsite_missing_packages: None,
        offsite_observed_at: None,
        offsite_remote_newest_at: None,
        offsite_key_created_at: None,
        credential_violations_backup_pvc: None,
        credential_violations_offsite: None,
        credential_violations_state_dir: None,
        tables_restored: None,
        tables_expected: None,
        rows_restored: None,
        rows_expected: None,
        source_runs_matched: None,
        generated_columns_recomputed: Vec::new(),
        tables_without_a_home: Vec::new(),
        artifacts_expected: 0,
        artifacts_restored: 0,
        artifact_readings: Vec::new(),
        artifact_errors: Vec::new(),
        error: None,
    };

    // 先自己走一遍目录。这一对数不取自写者的 kept_after：演练存在的理由之一
    // 就是「写者说自己留了几个」需要一个不靠它的读者。
    let packages = packages_in(&cfg.backup_dir);
    drill.packages_on_volume = packages.len() as i32;
    drill.oldest_package_at = packages
        .first()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .and_then(package_instant);

    // 卷外那一份也自己走一遍。和卷上那一对数同一个道理：写者说它复制了，不如
    // 读者自己看见它在。没有配置落点时这一列留空，「从来没有过一份」与「复制
    // 停了」于是由两条不同的读数分开说。
    if let Some(dir) = &cfg.offsite_dir {
        drill.offsite_newest_at = packages_in(dir)
            .last()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .and_then(package_instant);
    }

    // 卷外覆盖了窗口的多少份：卷上每一份去卷外目录找同名的那一份，找不到的就是
    // 「只有这一份」。只比最新那份回答不了这件事——复制是后来才打开的时候，更早
    // 写出来的几份从来没被复制过，而最新那一列照样是绿的、照样是今晚的。
    if let Some(dir) = &cfg.offsite_dir {
        drill.offsite_missing_packages = Some(missing_from_window(&packages, &packages_in(dir)));
    }

    // 仓库侧的两个时刻：宿主那一跑把 `restic snapshots` / `restic key list` 读到的
    // 结果写成一份观察文件，这里只读它。文件不在 = 这一面从来没有被观察过，账本
    // 两列留空；规则从「没有读数」与「读数老了」里各读一条，两种形状分开报。
    if let Some(dir) = &cfg.offsite_state_dir {
        if let Some(observed) = read_offsite_observation(dir) {
            drill.offsite_observed_at = observed.observed_at;
            drill.offsite_remote_newest_at = observed.newest_snapshot_at;
            drill.offsite_key_created_at = observed.key_created_at;
        }
    }

    // 三处凭证材料的现场：包卷、卷外副本、宿主状态目录。演练是唯一以 root
    // 同时挂着这三处的进程，所以这门由它来量；宿主定时器只看得见它自己挨着
    // 的那一个目录。没配落点的那一类留空，读作「这个安装没有这一处」。
    drill.credential_violations_backup_pvc = credential_violations_in(&cfg.backup_dir);
    if let Some(dir) = &cfg.offsite_dir {
        drill.credential_violations_offsite = credential_violations_in(dir);
    }
    if let Some(dir) = &cfg.offsite_state_dir {
        drill.credential_violations_state_dir = credential_violations_in(dir);
    }

    // 一趟有界：到点就把这一趟收尾并落一行，而不是等 Job 的界把进程杀掉。后者
    // 账上什么都不留——「被杀」于是读作「没跑过」，而 Job 面只看见一个 DeadlineExceeded，
    // 说不出是哪一份包、走到哪一步、预算多少。
    // 先落进一个变量再 match：`match timeout(...).await` 的临时量活到整个 match
    // 结束，`drill` 的独占借用于是跟着活到每一个分支里。
    let outcome =
        tokio::time::timeout(cfg.pass_budget, drill_once(cfg, live, packages, &mut drill)).await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => drill.error = Some(scrub_error(&e.to_string(), &cfg.database_url)),
        Err(_) => {
            mark_pass_timed_out(&mut drill, cfg.pass_budget);
            // 临时库这一趟是收不了尾了。下一次开工本来也会先删它，但一份照着包大小
            // 铺开的残库不该留到明晚：删不掉也只是警告，那本来就是下一趟的第一次开工。
            if let Err(e) = drop_drill_database(live).await {
                tracing::warn!(error = %e, "演练超时的临时库没删掉，下一次开工时会再删一次");
            }
        }
    }
    drill.finished_at = Utc::now();
    drill
}

/// 一趟超时之后写到那一行上的结论。
///
/// 单独成函数是为了让它可测：这一行的判词决定读数面能不能把「被杀」与「没跑过」
/// 分开，而它不该靠一次真的挂住的演练来验证。`outcome` 由 `error.is_some()` 推出，
/// 所以这里只写错误文本、并把已经量到的读数留在原处。
fn mark_pass_timed_out(drill: &mut BackupDrillRecord, budget: Duration) {
    drill.error = Some(format!(
        "这一趟 {} 秒（{DRILL_PASS_BUDGET_ENV}）没有跑完就收尾了。本行其余读数是走到那一步\
         之前量到的：表数与行数为空 ＝ 还没走到对数那一段，不是零张表。界由这一跑自己拿着，\
         因为 Job 的 activeDeadlineSeconds 到点是把进程杀掉——那样账上一行都不会有。",
        budget.as_secs(),
    ));
}

/// 同一时刻只许一场演练。临时库名是固定的（崩溃残留因此有确定的答案），代价是两场
/// 同时开工时后一场会把前一场的库 `DROP DATABASE` 掉——2026-10-19 实测：17:27 起的那场
/// 被 17:35 起的另一场杀掉，记成 failed，而它的失败与包无关。这个会话级咨询锁只回答
/// 一件事：「此刻还有人在演练吗」。拿去问库，比在部署里约定「别同时跑」可靠。
///
/// 返回的连接必须活到这一跑结束：锁挂在会话上，会话结束锁才释放。用一条**池外**的
/// 独立连接而不是池里借来的：池里的会话被归还后仍活着，锁会跟着它留给下一个借用者，
/// 而这一跑的每一条提前返回（某一份包失败就停）都不该需要记得手动释放。
pub async fn claim_drill_lock(
    database_url: &str,
) -> Result<Option<sqlx::PgConnection>, sqlx::Error> {
    use sqlx::Connection as _;
    let mut conn = sqlx::PgConnection::connect(database_url).await?;
    let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(DRILL_LOCK_KEY)
        .fetch_one(&mut conn)
        .await?;
    if held {
        Ok(Some(conn))
    } else {
        Ok(None)
    }
}

/// 口令与包的实际状态对不上时，演练不能判绿。
///
/// 为什么这由演练来问：它是唯一同时拿着口令、又看得见包头的进程。写端在没拿到口令
/// 时照写明文、只留一条 warn，于是「加密开关打开了」在部署字段里成立、在盘上不成立
/// 的那一刻，没有任何读数会红——除非演练在这里拦一下。反过来，包已经是密文而演练
/// 这一跑没有口令时，它同样证明不了今晚这份装得回去。
fn encryption_effect_mismatch(keyed: bool, newest_encrypted: bool) -> Option<&'static str> {
    match (keyed, newest_encrypted) {
        (true, false) => Some(
            "演练这一跑带着口令，但卷上最新那份包是明文：写端没拿到 COGNEVA_BACKUP_PACKAGE_KEY，加密没有生效",
        ),
        (false, true) => Some(
            "卷上最新那份包是密文，但演练这一跑没有口令：这一份装不回去，演练证明不了今晚的备份",
        ),
        _ => None,
    }
}

/// 读最新那份包的头，把「口令在不在」和「包是不是密文」对一遍。读文件这一半单独
/// 拿出来，是因为「判断写对了」和「判断接上了」是两件事：只测纯函数的话，调用点
/// 漏掉也照样全绿。
fn newest_package_key_mismatch(newest: &Path, keyed: bool) -> std::io::Result<Option<String>> {
    let encrypted = crate::backup_crypto::package_is_encrypted(newest)?;
    Ok(encryption_effect_mismatch(keyed, encrypted)
        .map(|reason| format!("{reason}（最新一份：{}）", newest.display())))
}

/// 明文包里有没有那两份 Secret 清单。
///
/// 加密包不问：解开它就要口令，能解的人本来就拿得到全部内容。这个问题只在明文包上
/// 有意义，而且它判的是**载体身份**——包里的清单只是 base64，等于把「凭证只住在安全
/// 网关里」这句话按盘上现存的字节推翻。写端没口令就不再导那两片，这条挡的是更早的
/// 代码写下的、还在保留窗口里的那几份。
fn plaintext_package_carrying_credentials(pkg: &Path) -> std::io::Result<Option<String>> {
    if crate::backup_crypto::package_is_encrypted(pkg)? {
        return Ok(None);
    }
    let file = std::fs::File::open(pkg)?;
    let decoder = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?;
        let in_secrets_dir = path
            .components()
            .any(|c| c.as_os_str() == std::ffi::OsStr::new(SECRETS_DIR));
        let is_manifest = path
            .extension()
            .is_some_and(|e| e == std::ffi::OsStr::new("yaml"));
        if in_secrets_dir && is_manifest {
            return Ok(Some(format!(
                "卷上最新那份包是明文，里面带着 Secret 清单（{}）：能读这个文件的人就能拿走全部凭证。\
                 写端现在没口令就不导这两片，盘上这一份是更早的代码写的——要么配 \
                 COGNEVA_BACKUP_PACKAGE_KEY 让新包成为密文并清掉旧明文包，要么把这些凭证按已泄漏处理、\
                 重新签发（最新一份：{}）",
                path.display(),
                pkg.display()
            )));
        }
    }
    Ok(None)
}

async fn drill_once(
    cfg: &DrillConfig,
    live: &PgPool,
    packages: Vec<PathBuf>,
    drill: &mut BackupDrillRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    // 问的是卷上最新那份，不是这一趟打开的：轮换窗口里旧包本来就是明文，点名去开
    // 一份旧包不能替最新那份回答「写端到底用没用口令」。
    if let Some(newest) = packages.last() {
        let keyed = crate::backup_crypto::PackageKey::from_env().is_some();
        if let Some(reason) = newest_package_key_mismatch(newest, keyed)? {
            return Err(reason.into());
        }
        // 读包的人与写包的人得是同一个专用 uid，而且不能是 root。这件事没有
        // 别的读数：谁哪天以 root 往卷里写了一份包，下一场演练不会报「属主不对」，
        // 而是要么读不了（报成「包坏了」），要么在 root 上照样读得动、永远沉默。
        // 所以这里把它判成红，阈值就是「最新那份的属主 = 此刻读它的 uid」。
        if let Some(reason) = package_owner_violation(newest) {
            return Err(reason.into());
        }
        // 明文包里带着凭证清单 ＝ 卷上有一份能读文件就拿走全部 token 的副本。
        // 写端已经不再这么写（没口令就不导那两片），这条判的是**盘上现存的**包：
        // 改之前写出来的那几份还在窗口里，它们的载体身份不会因为写端改了而消失。
        if let Some(reason) = plaintext_package_carrying_credentials(newest)? {
            return Err(reason.into());
        }
    }
    let package = choose_package(&packages, cfg.package.as_deref())
        .map_err(|e| format!("{}：{e}", cfg.backup_dir.display()))?;
    // `outcome=ok` 只说明这一趟跑完了，「读到的是老包」也能 ok。没点名时的默认
    // 那一趟必须是卷上最新那份——否则每晚的绿覆盖的不是今晚写出来的包。
    if let Some(reason) =
        default_pass_must_open_the_newest(&package, &packages, cfg.package.is_some())
    {
        return Err(reason.into());
    }
    let name = package
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("backup package name is not valid UTF-8")?
        .to_string();
    drill.package_written_at = package_instant(&name);
    if drill.package_written_at.is_none() {
        return Err(format!("包名 {name} 里读不出时刻，无法判断它是什么时候写的").into());
    }
    drill.package_bytes = std::fs::metadata(&package).ok().map(|m| m.len() as i64);
    // 留一份名字到清单解析之后：打开的那份字节要拿它跟账本里的那一跑对时。
    drill.package = Some(name.clone());

    // 卷外那一份是不是卷上**最新**那份：同名、同字节数。名字相同而大小不同是复制
    // 写到一半，名字不同是它停在了更早的那一晚——两种都是「第二份不是今晚的备份」，
    // 但它们各自对应不同的修法，所以这里只回答「是不是同一份」，原因留给读的人。
    //
    // 比的是卷上最新那份，不是这一趟打开的那份：读这一列的是「卷外那份是不是最新」
    // 的规则，点名开一份旧包时拿旧包去比，会把一条本来绿的规则判红。
    if let Some(dir) = &cfg.offsite_dir {
        drill.offsite_matches_newest = packages_in(dir)
            .last()
            .zip(packages.last())
            .map(|(copy, volume)| same_file(copy, volume));
    }

    // 解包先做：包本身坏了要在这里停，不能等到把库删了再发现。
    let extract_root = std::env::temp_dir().join(format!("cogneva-drill-{}", uuid::Uuid::new_v4()));
    let tree = TempTree(extract_root.clone());
    let pkg_dir = untar_package(&package, &extract_root)?;

    let manifest: BackupManifest =
        serde_json::from_str(&std::fs::read_to_string(pkg_dir.join(MANIFEST_FILE))?)?;
    tracing::info!(
        package = %package.display(),
        created_at = %manifest.created_at,
        revision = %manifest.revision,
        "drill: package opened"
    );
    // 名字说这是哪一晚的包，内容也得这么说。这一条不读 outcome：它问的是「开的是
    // 不是这一份」，而一套把旧包改名当成新包的流程每一晚都会 ok。
    if let Some(reason) = package_belongs_to_its_run(live, &name, &manifest.created_at).await? {
        return Err(reason.into());
    }
    // 名字对上了还不够：盘上这串字节也得还是写它那一跑交给卷的那一串。一条能解开的
    // 包也可能已经不是原件——坏块、截断补零、被同名文件覆盖，恢复时会以数据不对的形式
    // 暴露，而不是以「包坏了」的形式。
    if let Some(reason) = package_checksum_mismatch(live, &name, &package).await? {
        return Err(reason.into());
    }
    // 清单说这个包里有什么，此刻盘上是事实。这一段覆盖的是 qdrant 与对象层：
    // 它们不进空库，能核的就是「清单声明的字节与文件数是不是真的在包里」。
    verify_package_against_manifest(&pkg_dir, &manifest)?;
    // 恢复端会拒收的包，演练不能判绿：演练要说的是「今晚这份装得回去」，而不
    // 是「包能解开」。跨段偏差是恢复端的第一道拒绝，这里先问同一句话。
    if let Err(e) = check_segment_skew(&manifest) {
        return Err(format!("恢复端会拒绝这个包：{e}").into());
    }
    // 判过界之后再记读数：这个包自己声明的偏差与界一起进账本，面板上才看得见
    // 「离界还有多远」。旧包没有分段表，两列留空——没有声明就没有可比的数。
    if let Ok((_, _, skew)) = segment_span(&manifest.segments) {
        drill.segment_skew_seconds = Some(skew);
        drill.segment_skew_bound_seconds = Some(manifest.max_segment_skew_seconds as i64);
    }
    verify_artifact_stores(&pkg_dir, &manifest, drill).await;
    if manifest.postgres.tables.is_empty() {
        return Err("包的清单没有声明任何表，装回去之后无论对不对都证明不了什么".into());
    }

    // 空库：先删上一次的残骸，再建。删在建之前而不是用完就删，是因为「崩溃
    // 留下了一份旧数据」和「这次装回来的数据」混在一起时，计数会照样对得上。
    drop_drill_database(live).await?;
    sqlx::query(&format!("CREATE DATABASE {DRILL_DATABASE}"))
        .execute(live)
        .await?;
    let scratch_url = with_database(&cfg.database_url, DRILL_DATABASE);
    // 两边各记一条，判「装到哪儿」才判得了。只有目标那一格时，读的人无法说
    // 「同一个服务器上的另一个库」与「另一台机器」有什么区别——而演练要回答的
    // 正是后者：这一套装得回去，还是这一套**换台机器**也建得起来。
    drill.restore_source = Some(endpoint_of(&cfg.database_url));
    drill.restore_target = Some(endpoint_of(&scratch_url));
    tracing::info!(database = DRILL_DATABASE, "drill: scratch database created");

    let result = restore_and_compare(cfg, live, &scratch_url, &pkg_dir, &manifest, drill).await;
    // 无论成败都把库删掉：留着它下一次会先删，但磁盘里那份 1 GB 的残库会一直在。
    if let Err(e) = drop_drill_database(live).await {
        tracing::warn!(error = %e, "drill: 临时库没删掉，下一次演练开工时会再删一次");
    }
    drop(tree);
    result
}

/// 从包里把产物层逐项读回来，与 manifest 的声明比。
///
/// 这不是「解开了」：每个策略库要回答版本数、链头版本与 hash、ACTIVE 指向哪个
/// 版本，并且链校验通过；每个 decision_stats 要回答组数与试验数。读不回来的
/// 单元进 `artifact_errors`，并让这一跑变红——包变大不是证据。
async fn verify_artifact_stores(
    pkg_dir: &Path,
    manifest: &BackupManifest,
    drill: &mut BackupDrillRecord,
) {
    for root in &manifest.artifacts {
        let base = pkg_dir.join(ARTIFACTS_DIR).join(&root.root);
        if root.policies_present {
            let store = cog_reflection::PolicyStore::new(cog_reflection::policies_path(&base));
            for reported in &root.policy_stores {
                drill.artifacts_expected += 1;
                match verify_one_store(&store, &root.root, reported).await {
                    Ok(line) => {
                        drill.artifacts_restored += 1;
                        drill.artifact_readings.push(line);
                    }
                    Err(e) => drill
                        .artifact_errors
                        .push(format!("{}/policies/{}: {e}", root.root, reported.name)),
                }
            }
        }
        if let Some(workspace) = &root.workspace {
            drill.artifacts_expected += 1;
            match measure_tree(&base.join(cog_gateway::ide::WORKSPACE_SUBDIR)) {
                Ok((files, bytes)) if files == workspace.files && bytes == workspace.bytes => {
                    drill.artifacts_restored += 1;
                    drill
                        .artifact_readings
                        .push(format!("{}/workspace: {files} 个文件 / {bytes} 字节", root.root));
                }
                Ok((files, bytes)) => drill.artifact_errors.push(format!(
                    "{}/workspace: 包内实测 {files} 个文件 / {bytes} 字节，清单声明 {} 个 / {} 字节",
                    root.root, workspace.files, workspace.bytes
                )),
                Err(e) => drill
                    .artifact_errors
                    .push(format!("{}/workspace: 读不出来：{e}", root.root)),
            }
        }
        if let Some(stats) = &root.decision_stats {
            drill.artifacts_expected += 1;
            let path = cog_reflection::decision_stats_path(&base);
            match verify_one_stats(&path, &root.root, stats) {
                Ok(line) => {
                    drill.artifacts_restored += 1;
                    drill.artifact_readings.push(line);
                }
                Err(e) => drill.artifact_errors.push(format!(
                    "{}/meta_learning/decision_stats.json: {e}",
                    root.root
                )),
            }
        }
    }
    // 「读得出组数与试验数」证明的是包里的字节完好；它证明不了写者拿到这份字节
    // 还能继续记决策。两件事分开判：一份读得回但写不进去的统计，换机后表现得
    // 和「从来没记过」一样。
    if manifest
        .artifacts
        .iter()
        .any(|r| r.decision_stats.is_some())
    {
        drill.artifacts_expected += 1;
        match decision_roundtrip(pkg_dir, manifest).await {
            Ok(line) => {
                drill.artifacts_restored += 1;
                drill.artifact_readings.push(line);
            }
            Err(e) => drill
                .artifact_errors
                .push(format!("decision_roundtrip: {e}")),
        }
    }
    if !drill.artifact_errors.is_empty() && drill.error.is_none() {
        drill.error = Some(format!(
            "产物层读回失败 {} 项：{}",
            drill.artifact_errors.len(),
            drill.artifact_errors[0]
        ));
    }
}

/// 决策往返：把包里的产物层写回一个空的数据根，用引擎记一条决策，再把文件读回来，
/// 断言那条决策在里面。
///
/// 走的是恢复端真正用的那条写回路径（`restore_artifacts_into`），不是就地读包：
/// 要判的是「写回去的那一份还能被写者接着用」，而不是「包里的字节解析得开」。
/// 记进去的决策带一个固定名字，读回时按它取——其他格子的数值原样不动。
async fn decision_roundtrip(
    pkg_dir: &Path,
    manifest: &BackupManifest,
) -> Result<String, Box<dyn std::error::Error>> {
    use cog_core::{DecisionCategory, DecisionGroupKey, DecisionOutcome};

    let scratch_root =
        std::env::temp_dir().join(format!("cogneva-roundtrip-{}", uuid::Uuid::new_v4()));
    let _tree = TempTree(scratch_root.clone());
    let mut targets = Vec::new();
    for root in &manifest.artifacts {
        let dir = scratch_root.join(&root.root);
        std::fs::create_dir_all(&dir)?;
        targets.push((root.root.clone(), dir));
    }
    let readings = restore_artifacts_into(pkg_dir, manifest, &targets).await?;

    // 取第一个真带决策统计的根：包里的统计可能只在主根上。
    let root = manifest
        .artifacts
        .iter()
        .find(|r| r.decision_stats.is_some())
        .ok_or("包里没有任何决策统计段")?;
    let path = cog_reflection::decision_stats_path(&scratch_root.join(&root.root));
    let snapshot = cog_reflection::DecisionStatsSnapshot::new(path.clone());
    let engine = cog_reflection::MetaLearningEngine::with_durable_state(
        std::sync::Arc::new(cog_reflection::InMemoryRecorder::default()),
        snapshot.clone(),
    )
    .await;
    let group = DecisionGroupKey::by_task_type("drill-roundtrip");
    engine
        .record(
            DecisionCategory::PgeMode,
            &group,
            DECISION_ROUNDTRIP_DECISION,
            DecisionOutcome::Success,
        )
        .await?;

    let restored = snapshot
        .load()
        .await?
        .ok_or_else(|| format!("写完之后 {} 读不出来", path.display()))?;
    let cell = restored
        .get(&(DecisionCategory::PgeMode, group.clone()))
        .and_then(|stats| stats.counts.get(DECISION_ROUNDTRIP_DECISION))
        .copied();
    // 记一条新决策后，原有历史的试验数必须原样还在、且只多这一条。少了说明
    // 引擎读到的是空表、写回的是重写过的空表——这种现象在「新决策读得回来」
    // 的读数里完全看不出来，而它正是换机后证据归零的样子。
    let trials: u64 = restored
        .values()
        .flat_map(|s| s.counts.values())
        .map(|(attempts, _)| u64::from(*attempts))
        .sum();
    let declared = root.decision_stats.as_ref().map_or(0, |s| s.trials);
    if trials != declared + 1 {
        return Err(format!(
            "写回后的试验数是 {trials}，包里声明 {declared} + 探针 1：原有历史没有原样留住"
        )
        .into());
    }
    match cell {
        Some((attempts, successes)) if attempts == 1 && successes == 1 => Ok(format!(
            "{}/meta_learning decision_roundtrip=ok group={} attempts=1 successes=1 历史试验数={declared}（写回 {} 项）",
            root.root,
            group.as_str(),
            readings.len()
        )),
        other => Err(format!(
            "记进去的决策读不回来：{}({:?}) = {other:?}",
            group.as_str(),
            DECISION_ROUNDTRIP_DECISION
        )
        .into()),
    }
}

/// 往返里写进去的那个决策名。它是探针，不是产品决策：读回时按这个名字取，
/// 名字里写清来历，免得日后有人在真实的计数里找它。
const DECISION_ROUNDTRIP_DECISION: &str = "drill_roundtrip_probe";

async fn verify_one_store(
    store: &cog_reflection::PolicyStore,
    root: &str,
    reported: &PolicyStoreReport,
) -> Result<String, String> {
    if let Some(problem) = &reported.problem {
        return Err(format!("打包时已经读出问题：{problem}"));
    }
    let name = &reported.name;
    let versions = store.list_versions(name).await.map_err(|e| e.to_string())?;
    if versions.len() as u64 != reported.versions {
        return Err(format!(
            "包里 {} 个版本，清单声明 {}",
            versions.len(),
            reported.versions
        ));
    }
    let head = *versions.last().ok_or("包里没有版本文件")?;
    let head_artifact = store
        .load_version(name, head)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("链头版本 {head:06} 读不出来"))?;
    if head != reported.head_version || head_artifact.hash != reported.head_hash {
        return Err(format!(
            "链头不一致：包里 {head:06}/{}，清单 {}/{}",
            head_artifact.hash, reported.head_version, reported.head_hash
        ));
    }
    let active = store
        .load_active(name)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!(
                "ACTIVE 指向的版本不在包里（清单说 {}）",
                reported.active_version
            )
        })?;
    if active.version != reported.active_version || active.hash != reported.active_hash {
        return Err(format!(
            "ACTIVE 不一致：包里 {}/{}，清单 {}/{}",
            active.version, active.hash, reported.active_version, reported.active_hash
        ));
    }
    if !store.verify_chain(name).await.map_err(|e| e.to_string())? {
        return Err("版本链校验不通过".into());
    }
    Ok(format!(
        "{root}/policies/{name} versions={} head={:06} active={:06}",
        reported.versions, reported.head_version, reported.active_version
    ))
}

fn verify_one_stats(
    path: &Path,
    root: &str,
    reported: &DecisionStatsReport,
) -> Result<String, String> {
    let bytes = std::fs::metadata(path)
        .map_err(|e| format!("读不到：{e}"))?
        .len();
    if bytes != reported.bytes {
        return Err(format!("{} 字节，清单声明 {}", bytes, reported.bytes));
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("读不到：{e}"))?;
    let (groups, trials) = decision_stats_counts(&text).map_err(|e| format!("解析失败：{e}"))?;
    if groups != reported.groups || trials != reported.trials {
        return Err(format!(
            "组/试验 {groups}/{trials}，清单声明 {}/{}",
            reported.groups, reported.trials
        ));
    }
    Ok(format!(
        "{root}/meta_learning groups={} trials={}",
        reported.groups, reported.trials
    ))
}

async fn restore_and_compare(
    cfg: &DrillConfig,
    live: &PgPool,
    scratch_url: &str,
    pkg_dir: &Path,
    manifest: &BackupManifest,
    drill: &mut BackupDrillRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    // 表结构必须由**这个镜像**的迁移建出来：恢复只做 TRUNCATE + COPY，目标是
    // 一个跑完迁移的空库。改用模板库复制会更快，但那要求源库上没有别的连接——
    // 而源库此刻正被整个平台用着。
    // 四段的耗时各记一行：这个数随时间涨（库在长），而「一趟多少秒」是一个总数，
    // 读的人分不出是迁移、建结构、灌数据还是逐表对数长出来的。界（单趟预算）到点
    // 时日志里最后一行就是走到哪一段，比在账上写一句「超时了」多一个坐标。
    let phase = std::time::Instant::now();
    cog_storage::migrate::Migrator::new(&cfg.migrations_dir)
        .run(scratch_url)
        .await?;
    tracing::info!(
        database = DRILL_DATABASE,
        phase = "migrations",
        elapsed_ms = phase.elapsed().as_millis() as u64,
        "drill: migrations applied"
    );
    let migrations_done = phase.elapsed();

    let scratch = PgPool::connect(scratch_url).await?;
    // 迁移只建得出迁移文件里有的表；各 store 首次启动时自己建的那批表也要先建，
    // 否则下一段的 COPY 会在第一张缺失的表上失败——那是镜像建不出自己的结构，
    // 不是包不可恢复。
    crate::schema_bundle::apply(&scratch)
        .await
        .map_err(|e| format!("drill: schema bundle failed: {e}"))?;
    tracing::info!(
        phase = "store-schema",
        elapsed_ms = (phase.elapsed() - migrations_done).as_millis() as u64,
        "drill: store schema applied"
    );
    let pg_dir = pkg_dir.join(PACKAGE_PG_DIR);
    if !pg_dir.exists() {
        return Err("包里没有 pg 段，这次演练无表可恢复".into());
    }
    let restore_started = std::time::Instant::now();
    let restored = restore_postgres(&scratch, &pg_dir).await?;
    tracing::info!(
        phase = "copy",
        elapsed_ms = restore_started.elapsed().as_millis() as u64,
        tables = restored.tables.len(),
        "drill: postgres segment copied"
    );
    // 装回来的列清单要跟着这一行一起留在账上：一份按旧导出写的包被装回时，
    // 生成列的值不是包里的、是目标此刻算出来的。表数与行数看不见这件事。
    drill.generated_columns_recomputed = restored.recomputed.clone();
    // 包里那些当前 schema 已经没有的表：它们的行没有装回来，因此也不进行数比对。
    // 名字留在这一行上——否则这份包读起来与一份各表齐全的包一模一样。
    drill.tables_without_a_home = restored.tables_without_a_home.clone();
    tracing::info!(
        tables = restored.tables.len(),
        recomputed = restored.recomputed.len(),
        gone = restored.tables_without_a_home.len(),
        "drill: postgres segment restored"
    );
    if !manifest.postgres.tables.is_empty() && restored.tables.is_empty() {
        return Err(format!(
            "包里 {} 张表在当前 schema 里一张都没有了（{}），这一份没有可恢复的数据",
            manifest.postgres.tables.len(),
            restored.tables_without_a_home.join("；")
        )
        .into());
    }

    // 逐表把「装回来几行」与清单声明比。比对的是**这个镜像的结构装得下的那批表**：
    // 被迁移删掉的表由上面的读数点名，它们的行数无从比，也不该被算成"没装上"。
    let compare_started = std::time::Instant::now();
    let without_a_home: std::collections::HashSet<&str> = restored
        .tables_without_a_home
        .iter()
        .map(|t| t.as_str())
        .collect();
    let mut conn = scratch.acquire().await?;
    let expected_tables = manifest
        .postgres
        .tables
        .iter()
        .filter(|t| !without_a_home.contains(t.name.as_str()))
        .count() as i32;
    let mut matched_tables = 0i32;
    let mut rows_restored = 0i64;
    let mut rows_expected = 0i64;
    let mut disagreements = Vec::new();
    for table in &manifest.postgres.tables {
        if without_a_home.contains(table.name.as_str()) {
            continue;
        }
        validate_table_name(&table.name)?;
        let rows: i64 = sqlx::query_scalar(&cog_core::sql::relation_rows_sql(&table.name))
            .fetch_one(&mut *conn)
            .await?;
        rows_restored += rows;
        rows_expected += table.rows;
        if rows == table.rows {
            matched_tables += 1;
        } else {
            disagreements.push(format!(
                "`{}` 装回 {rows} 行，清单 {} 行",
                table.name, table.rows
            ));
        }
    }
    drop(conn);
    drill.tables_restored = Some(matched_tables);
    drill.tables_expected = Some(expected_tables);
    drill.rows_restored = Some(rows_restored);
    drill.rows_expected = Some(rows_expected);

    // 账表的逐格比对：只在**装回来之后就是终局**的行上比。包是在它自己那一行
    // 还开着的时候打出来的，那一行随后由运行自己收尾——拿它去比，比到的是
    // 「备份之后发生的事」，不是恢复对不对。
    // 终局的两个结局由枚举自己定：拼在 SQL 里的副本改了名不会报错，只会让这份
    // 逐格比对读到零行——零行比零行，比完是「一致」。
    let terminal_ids: Vec<String> = sqlx::query_scalar(
        "SELECT run_id FROM backup_runs WHERE outcome IN ($1, $2) ORDER BY run_id",
    )
    .bind(cog_core::BackupOutcome::Ok.as_str())
    .bind(cog_core::BackupOutcome::Failed.as_str())
    .fetch_all(&scratch)
    .await?;
    let restored_digest: String = sqlx::query_scalar(RUNS_DIGEST_SQL)
        .bind(&terminal_ids)
        .fetch_one(&scratch)
        .await?;
    let source_digest: String = sqlx::query_scalar(RUNS_DIGEST_SQL)
        .bind(&terminal_ids)
        .fetch_one(live)
        .await?;
    drill.source_runs_matched = Some(restored_digest == source_digest);
    if drill.source_runs_matched != Some(true) {
        return Err(format!(
            "backup_runs 的 {} 行终局记录与源库不一致：装回来是 {restored_digest}，源库是 \
             {source_digest}",
            terminal_ids.len()
        )
        .into());
    }

    if !disagreements.is_empty() {
        return Err(format!(
            "{}/{} 张表的行数与清单不符：{}",
            expected_tables - matched_tables,
            expected_tables,
            disagreements.join("；")
        )
        .into());
    }
    tracing::info!(
        phase = "compare",
        elapsed_ms = compare_started.elapsed().as_millis() as u64,
        "drill: rows compared"
    );
    Ok(())
}

/// 落库前把错误文本洗干净：口令整段换掉，再交给公共脱敏。
fn scrub_error(text: &str, database_url: &str) -> String {
    let scrubbed = url_password(database_url)
        .map(|secret| cog_core::redact_secrets(text).replace(&secret, "[redacted]"))
        .unwrap_or_else(|| cog_core::redact_secrets(text));
    scrubbed.chars().take(ERROR_TEXT_MAX_CHARS).collect()
}

fn install_subscriber() {
    // 子命令不经 run_app，没有早鸟订阅者；Job Pod 日志全靠这里装。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

/// `cogneva backup`：CronJob 与手动共用入口。
pub async fn run_backup_from_env() -> Result<(), Box<dyn std::error::Error>> {
    install_subscriber();
    let cfg = BackupConfig::from_env()?;
    let package = run_backup(&cfg).await?;
    println!("{}", package.display());
    Ok(())
}

/// `cogneva backup-drill`：演练 CronJob 的入口。
///
/// 演练的结果先落库再决定退出码：一行「证明不了」的记录比一个失败的 Job 更有
/// 用，但两者都要有——只落库不退出非零，集群面看不到这次失败；只退出非零不落库，
/// 读数面看不到为什么。
pub async fn run_drill_from_env() -> Result<(), Box<dyn std::error::Error>> {
    install_subscriber();
    let cfg = DrillConfig::from_env()?;
    let live = PgPool::connect(&cfg.database_url).await?;
    let store = PostgresBackupRunStore::new(live.clone());

    // 这一跑要开一到两份包，中间那块库是同一个固定名字。锁必须盖住**整个循环**，
    // 不是每一趟：两趟之间松手的那几秒足够另一场演练插进来，把下一趟的库 DROP 掉，
    // 然后这一跑记一条与包无关的失败。拿不到锁就整场跳过——另一场做的是同一件事，
    // 重复的那一场不算失败，job 也不该因此变红。
    let _held = match claim_drill_lock(&cfg.database_url).await {
        Ok(Some(conn)) => conn,
        Ok(None) => {
            println!("另一场演练正占着临时库（会话咨询锁没取到），这一场跳过");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    // 先开最新那份，再开窗口里最久没被证明的那份。顺序是判定的一部分：任何一趟
    // 失败都立刻停，于是**最后一行的结论就是这一跑的结论**——若先跑旧包、后跑最新，
    // 旧包的失败会被随后一行的绿盖住，而读数读的正是最新那一行。
    let mut targets: Vec<Option<String>> = vec![cfg.package.clone()];
    if cfg.also_unproven {
        let packages = packages_in(&cfg.backup_dir);
        // 最新那份已经在第一趟里开过了，不参加第二趟的候选：否则「从没成功过」
        // 的排序会永远先把刚写出来的那份挑出来（它当然从没成功过），第二趟每次都
        // 空转，旧包一份也轮不到。
        let candidates = older_than_newest(&packages);
        match least_recently_proven(&live, candidates).await? {
            Some(name) => targets.push(Some(name)),
            // 两句话分开说：卷上一份包都没有，与卷上只有最新那一份、没有第二份可开，
            // 是两件不同的事。合成一句会让读日志的人去查卷，而卷是好的。
            None if packages.is_empty() => {
                tracing::warn!("卷上没有包，这一跑没有可开的")
            }
            None => tracing::warn!("卷上只有最新那一份，第二趟没有别的可开"),
        }
    }

    for target in targets {
        let run_cfg = DrillConfig {
            package: target,
            also_unproven: false,
            ..cfg.clone()
        };
        let drill = run_drill(&run_cfg, &live).await;
        store.record_drill(&drill).await?;
        println!(
            "{} {} {}",
            drill.drill_id,
            if drill.ok() { "ok" } else { "failed" },
            drill.package.as_deref().unwrap_or("-")
        );
        if let Some(error) = &drill.error {
            return Err(format!("演练没有证明这个包可恢复：{error}").into());
        }
        if !drill.ok() {
            return Err("演练的测量没有全部对上，这一行没有留下错误文本".into());
        }
    }
    Ok(())
}

/// `cogneva restore <pkg>`：换机/重装的一次性恢复 Job 入口。
pub async fn run_restore_from_env() -> Result<(), Box<dyn std::error::Error>> {
    install_subscriber();
    let pkg = std::env::args()
        .nth(2)
        .ok_or("usage: cogneva restore <package.tar.zst|dir>")?;
    run_restore(Path::new(&pkg)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 账本里那一格只留地址，不留身份：口令写成 `@` 也不会漏出来，查询串不进格。
    ///
    /// 这一条是给「备份账本随包被复制到卷外与异地」这件事守的门——一个把口令抄进
    /// 账本的实现，在这里就能被抓到，而不是等到某天有人读异地那份账本。
    #[test]
    fn the_restore_endpoint_carries_no_credentials() {
        assert_eq!(
            endpoint_of("postgres://cog_user:p%40ss@postgres:5432/cogneva?sslmode=disable"),
            "postgres:5432/cogneva"
        );
        assert_eq!(
            endpoint_of("postgres://cog_user:s3cr@t@postgres:5432/cogneva"),
            "postgres:5432/cogneva"
        );
        // 没有 scheme、没有库名：只剩地址的那一段原样交出去，不编一个库名。
        assert_eq!(endpoint_of("postgres:5432"), "postgres:5432");
    }

    /// 一趟超时必须留下**一行能读的结论**，而不是让 Job 的界把进程杀掉。
    ///
    /// 这是本文件对 2026-10-27 那次现场的门：`backup-drill` 第一趟跑了 2271 s，
    /// 第二趟在 `activeDeadlineSeconds: 3600` 上被杀，`backup_drills` 里没有那一行，
    /// 于是「窗口里最久没被证明的那份」永远证明不了，而账上读作「这一趟没跑过」。
    #[test]
    fn a_pass_that_runs_past_its_budget_still_leaves_a_row_that_says_so() {
        let mut drill = BackupDrillRecord {
            drill_id: "drill-test".into(),
            started_at: Utc.with_ymd_and_hms(2026, 10, 27, 3, 47, 2).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 10, 27, 3, 47, 2).unwrap(),
            package: Some("cogneva-backup-20261027T031714Z.tar.zst".into()),
            package_written_at: None,
            package_bytes: None,
            restore_source: Some("postgres:5432/cogneva".into()),
            restore_target: None,
            segment_skew_seconds: None,
            segment_skew_bound_seconds: None,
            packages_on_volume: 7,
            oldest_package_at: None,
            offsite_newest_at: None,
            offsite_matches_newest: None,
            offsite_missing_packages: None,
            offsite_observed_at: None,
            offsite_remote_newest_at: None,
            offsite_key_created_at: None,
            credential_violations_backup_pvc: None,
            credential_violations_offsite: None,
            credential_violations_state_dir: None,
            tables_restored: None,
            tables_expected: None,
            rows_restored: None,
            rows_expected: None,
            source_runs_matched: None,
            generated_columns_recomputed: Vec::new(),
            tables_without_a_home: Vec::new(),
            artifacts_expected: 0,
            artifacts_restored: 0,
            artifact_readings: Vec::new(),
            artifact_errors: Vec::new(),
            error: None,
        };
        mark_pass_timed_out(&mut drill, Duration::from_secs(3600));
        let error = drill.error.as_ref().expect("超时的那一趟必须有错误文本");
        // 判词要能回答「界是多少」，否则读的人只能去翻部署清单。
        assert!(error.contains("3600"), "错误文本没写界：{error}");
        // 而且它必须让这一行读作 failed：面读数按 outcome 取行，不按过程猜。
        assert!(!drill.ok(), "超时的行被判成了成功");
    }

    /// 界读不成数时按默认值跑，而不是让整场演练起不来——起不来的那一晚，
    /// 账上连一行说「为什么」的都没有。
    #[test]
    fn a_budget_that_cannot_be_read_falls_back_instead_of_stopping_the_drill() {
        let default = Duration::from_secs(DRILL_PASS_BUDGET_DEFAULT_SECS);
        for raw in [None, Some(""), Some("  "), Some("0"), Some("ten")] {
            assert_eq!(pass_budget_from(raw), default, "raw={raw:?}");
        }
        assert_eq!(pass_budget_from(Some(" 900 ")), Duration::from_secs(900));
    }

    #[test]
    fn package_names_sort_by_time() {
        let a = package_name(Utc.with_ymd_and_hms(2026, 9, 14, 1, 0, 0).unwrap());
        let b = package_name(Utc.with_ymd_and_hms(2026, 9, 14, 2, 0, 0).unwrap());
        assert!(a < b, "时间戳命名必须字典序可排：{a} vs {b}");
        assert!(a.starts_with(PACKAGE_PREFIX) && a.ends_with(PACKAGE_SUFFIX));
    }

    #[test]
    fn the_offsite_observation_distinguishes_no_file_from_no_value() {
        let dir = tempfile::tempdir().unwrap();

        // 没有文件 = 没人观察过。账本留空列，规则读作「没量过」。
        assert!(read_offsite_observation(dir.path()).is_none());

        // 文件在、里面是两个 null = 观察过了，仓库里还没有快照/钥匙。
        // 这与「没量过」是两种事实，读法也不同，所以解析要成功而不是退回 None。
        std::fs::write(
            dir.path().join("observation.json"),
            r#"{"observed_at":"2026-10-19T02:30:00Z","newest_snapshot_at":null,"key_created_at":null}"#,
        )
        .unwrap();
        let observed = read_offsite_observation(dir.path()).expect("文件在就要解析出来");
        assert!(observed.newest_snapshot_at.is_none());
        assert!(observed.key_created_at.is_none());
        // 「观察过了，里面没有」与「没人观察」靠这一列分开：后者是 None。
        assert_eq!(
            observed.observed_at.unwrap().timestamp(),
            Utc.with_ymd_and_hms(2026, 10, 19, 2, 30, 0)
                .unwrap()
                .timestamp()
        );

        // 有值：带纳秒与偏移的 RFC3339 也要认，脚本归一失败时会原样写出来。
        std::fs::write(
            dir.path().join("observation.json"),
            r#"{"newest_snapshot_at":"2026-10-19T10:30:00.123456789+08:00","key_created_at":"2026-01-02T03:04:05Z"}"#,
        )
        .unwrap();
        let observed = read_offsite_observation(dir.path()).expect("文件在就要解析出来");
        // 旧文件没有 observed_at：文件在现场就说明有人看过，按 mtime 补上，
        // 否则「观察过」会被读成「没人观察」。
        assert!(observed.observed_at.is_some());
        // 比到秒：解析保留纳秒（restic 写的就是纳秒），而这里要钉的是时区
        // 归一（+08:00 → UTC）与字段落位，不是小数位的精度。
        assert_eq!(
            observed.newest_snapshot_at.unwrap().timestamp(),
            Utc.with_ymd_and_hms(2026, 10, 19, 2, 30, 0)
                .unwrap()
                .timestamp()
        );
        assert_eq!(
            observed.key_created_at.unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap()
        );

        // 读不懂也算没量过，但现场要留日志——不在这里断言日志，只钉住返回值形状。
        std::fs::write(dir.path().join("observation.json"), "{not json").unwrap();
        assert!(read_offsite_observation(dir.path()).is_none());
    }

    #[test]
    fn a_drill_without_a_name_opens_the_newest_package() {
        let packages: Vec<PathBuf> = ["2026-10-18", "2026-10-19"]
            .iter()
            .map(|day| {
                PathBuf::from(format!(
                    "/backups/{PACKAGE_PREFIX}{day}-010101{PACKAGE_SUFFIX}"
                ))
            })
            .collect();
        let chosen = choose_package(&packages, None).unwrap();
        assert_eq!(
            chosen, packages[1],
            "不点名时开最新那份，行为与加这个参数之前一致"
        );
    }

    #[test]
    fn a_package_written_by_another_uid_is_a_failure_and_so_is_reading_it_as_root() {
        // 同一个专用 uid：放行。
        assert!(owner_violation(1001, 1001, "/backups/p.tar.zst").is_none());
        // 包是 root 写的、读它的是 1001：下一场演练能读就沉默，读不了就报成「包坏了」，
        // 两种都指不到真正该修的地方。
        let wrong_owner = owner_violation(0, 1001, "/backups/p.tar.zst").unwrap();
        assert!(wrong_owner.contains("uid 0"), "{wrong_owner}");
        assert!(wrong_owner.contains("uid 1001"), "{wrong_owner}");
        // 两边都是 root：属主对得上，但「只有属主可读」这时等于谁都能读。
        let all_root = owner_violation(0, 0, "/backups/p.tar.zst").unwrap();
        assert!(all_root.contains("不是隔离"), "{all_root}");
        // 读的人是 root、包属于 1001：同样不是隔离，也不能借「属主不一致」蒙过去。
        assert!(owner_violation(1001, 0, "/backups/p.tar.zst").is_some());
    }

    #[test]
    fn the_package_in_hand_has_to_be_the_one_its_name_names() {
        let started = Utc.with_ymd_and_hms(2026, 10, 20, 3, 17, 0).unwrap();
        let finished = Utc.with_ymd_and_hms(2026, 10, 20, 3, 25, 0).unwrap();
        let name = package_name(started);
        // 正常：包内时刻落在那一跑里。
        let inside = Utc.with_ymd_and_hms(2026, 10, 20, 3, 24, 0).unwrap();
        assert!(run_window_violation(&name, inside, started, finished).is_none());
        // 旧包的字节被复制成新名字：名字、大小、mtime 都对，内容里的钟停在几个月前。
        let from_an_old_run = Utc.with_ymd_and_hms(2026, 7, 1, 3, 20, 0).unwrap();
        let renamed = run_window_violation(&name, from_an_old_run, started, finished).unwrap();
        assert!(renamed.contains("名字与内容是两跑的东西"), "{renamed}");
        // 时钟粒度不算跑偏：起止边缘各一分钟以内仍然放行。
        let edge = finished + chrono::Duration::seconds(30);
        assert!(run_window_violation(&name, edge, started, finished).is_none());
    }

    #[test]
    fn the_default_pass_has_to_open_the_newest_package() {
        let packages: Vec<PathBuf> = ["2026-10-18", "2026-10-19"]
            .iter()
            .map(|day| {
                PathBuf::from(format!(
                    "/backups/{PACKAGE_PREFIX}{day}-010101{PACKAGE_SUFFIX}"
                ))
            })
            .collect();
        assert!(default_pass_must_open_the_newest(&packages[1], &packages, false).is_none());
        // 默认那一趟打开了老包：这一跑仍然可能 ok，但它证明的不是今晚那份。
        let drifted = default_pass_must_open_the_newest(&packages[0], &packages, false).unwrap();
        assert!(drifted.contains("选择规则"), "{drifted}");
        // 点名开老包是设计好的用法（第二趟），不判。
        assert!(default_pass_must_open_the_newest(&packages[0], &packages, true).is_none());
        // 卷上一份都没有时由 choose_package 报，这里不重复判。
        assert!(default_pass_must_open_the_newest(&packages[0], &[], false).is_none());
    }

    #[test]
    fn a_drill_that_cannot_match_the_key_to_the_package_refuses_to_judge() {
        // 「加密开了」最容易被误报成已生效的形状：部署里配了口令，而盘上最新那份
        // 仍是明文。写端只留一条 warn，这一跑如果照旧判绿，就没人会知道。
        assert!(encryption_effect_mismatch(true, false).is_some());
        // 反方向同样是「装不回去」：包是密文，而这台机器没有口令。
        assert!(encryption_effect_mismatch(false, true).is_some());
        // 两种对得上：都配了，或都没有——后一种是打开加密之前的老安装。
        assert!(encryption_effect_mismatch(true, true).is_none());
        assert!(encryption_effect_mismatch(false, false).is_none());

        // 只有纯函数被调用不算数：调用点读的是**盘上那份文件的头**。这里用真的
        // 文件走一遍，两个方向都问一次。
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("plain.tar.zst");
        std::fs::write(&plain, b"not an encrypted package").unwrap();
        let reason = newest_package_key_mismatch(&plain, true)
            .unwrap()
            .expect("口令在、包是明文，演练必须拒绝判绿");
        assert!(reason.contains("明文"), "reason={reason}");
        assert!(newest_package_key_mismatch(&plain, false)
            .unwrap()
            .is_none());

        let sealed = dir.path().join("sealed.tar.zst");
        let mut bytes = crate::backup_crypto::MAGIC.to_vec();
        bytes.extend_from_slice(&[0u8; 16]);
        std::fs::write(&sealed, &bytes).unwrap();
        assert!(newest_package_key_mismatch(&sealed, true)
            .unwrap()
            .is_none());
        let reason = newest_package_key_mismatch(&sealed, false)
            .unwrap()
            .expect("包是密文、这一跑没口令，演练必须拒绝判绿");
        assert!(reason.contains("密文"), "reason={reason}");
    }

    /// 一份包在写下之后被改过字节，就不是那一晚的备份了。
    ///
    /// 这一格专门挑**等长**的替换：字节数看不见它，账上那一列也不动，只有摘要能把它
    /// 说出来。真正的现场是坏块、复制被截断后补零、同名文件覆盖——三者都以「包能解开」
    /// 的样子出现，直到恢复出来才发现数据不对。
    /// 账上那一格要说得出包里装着什么，而不只是「有这么一个包」。
    ///
    /// 这一格存在的理由是它替换掉的那种读数：字节数对「整套权威面」和「一个 Secret」
    /// 给出同样的量级判断，而两者收尾都是 ok。所以断言不看有没有字段，而是逐段读回来，
    /// 并且要求「一段都没有」写成空数组而不是缺席——缺席按约定是「旧包没声明」。
    #[test]
    fn the_run_records_what_its_package_covered() {
        let mut manifest: BackupManifest = serde_json::from_str(
            r#"{
                "version": "1",
                "revision": "abc123",
                "instance_fingerprint": "fp",
                "namespace": "cogneva",
                "created_at": "2026-10-24T03:17:00Z",
                "secrets_keys": ["pg-password", "jwt-secret"]
            }"#,
        )
        .expect("最小清单能读出来");
        manifest.postgres.tables = vec![
            TableReport {
                name: "public.backup_runs".into(),
                rows: 42,
            },
            TableReport {
                name: "public.empty_table".into(),
                rows: 0,
            },
        ];
        manifest.postgres.sequences = 3;
        manifest.qdrant = vec![CollectionReport {
            name: "code".into(),
            snapshot: "code.snapshot".into(),
            points: 1024,
        }];
        manifest.objects = ObjectsReport {
            files: 7,
            bytes: 2048,
        };
        manifest.artifacts = vec![ArtifactRootReport {
            root: "app".into(),
            data_dir: "/data".into(),
            policies_present: true,
            policy_stores: vec![PolicyStoreReport {
                name: "meta_learning".into(),
                ..Default::default()
            }],
            decision_stats: None,
            workspace: None,
        }];

        let value: serde_json::Value =
            serde_json::from_str(&package_coverage(&manifest)).expect("覆盖清单是 JSON");
        assert_eq!(
            value["secrets"],
            serde_json::json!(["pg-password", "jwt-secret"])
        );
        // 反例之一：行数为 0 的表必须还在单子上。「有这张表、它是空的」和「没这张表」
        // 是两件不同的事，把零当成没有就会让一次漏备读成一次正常。
        assert_eq!(
            value["postgres"]["tables"],
            serde_json::json!([["public.backup_runs", 42], ["public.empty_table", 0]])
        );
        assert_eq!(value["postgres"]["sequences"], 3);
        assert_eq!(value["qdrant"], serde_json::json!([["code", 1024]]));
        assert_eq!(
            value["objects"],
            serde_json::json!({"files": 7, "bytes": 2048})
        );
        assert_eq!(
            value["artifacts"],
            serde_json::json!([["app", ["meta_learning"]]])
        );

        // 另一头：一段都没有时写成空数组。读的人据此能说「这一段今天空着」，
        // 而不是「这一段没写」。
        let bare: BackupManifest = serde_json::from_str(
            r#"{
                "version": "1",
                "revision": "abc123",
                "instance_fingerprint": "fp",
                "namespace": "cogneva",
                "created_at": "2026-10-24T03:17:00Z"
            }"#,
        )
        .expect("最小清单能读出来");
        let value: serde_json::Value = serde_json::from_str(&package_coverage(&bare)).unwrap();
        assert_eq!(value["qdrant"], serde_json::json!([]));
        assert_eq!(value["artifacts"], serde_json::json!([]));
        assert_eq!(value["postgres"]["tables"], serde_json::json!([]));
    }

    #[test]
    fn a_package_whose_bytes_changed_since_it_was_written_is_not_a_backup() {
        let dir = tempfile::tempdir().unwrap();
        let pkg = dir
            .path()
            .join(format!("{PACKAGE_PREFIX}20261024T031700Z{PACKAGE_SUFFIX}"));
        std::fs::write(&pkg, b"first night's bytes").unwrap();
        let recorded = package_digest(&pkg).unwrap();
        assert!(
            recorded.starts_with("sha256:"),
            "算法名要跟着值走，否则换算法那天账上两种值会互相比较：{recorded}"
        );
        let again = package_digest(&pkg).unwrap();
        assert_eq!(recorded, again, "同一份字节两次算出来的摘要必须一样");
        assert!(checksum_violation("pkg", Some(&recorded), &again).is_none());

        assert_eq!(
            b"first night's bytes".len(),
            b"FIRST night's bytes".len(),
            "夹具得等长，否则这一格验的还是字节数，不是摘要"
        );
        std::fs::write(&pkg, b"FIRST night's bytes").unwrap();
        let changed = package_digest(&pkg).unwrap();
        let reason =
            checksum_violation("pkg", Some(&recorded), &changed).expect("等长的不同字节必须判红");
        assert!(
            reason.contains(&recorded) && reason.contains(&changed),
            "判词要把账上和盘上两个值都写出来，读的人才知道差在哪：{reason}"
        );

        // 这一跑压根没记摘要：没有可比的，也就什么也没证明。它不许被读成「对上了」。
        assert!(checksum_violation("pkg", None, &changed).is_none());
    }

    #[test]
    fn naming_a_package_opens_that_package() {
        // 这一条就是这次改动本身：保留窗口里的旧包必须能被人点名打开，否则
        // 「旧包也恢复得回去」永远只是一句没被验证过的话。
        let packages: Vec<PathBuf> = ["2026-10-18", "2026-10-19"]
            .iter()
            .map(|day| {
                PathBuf::from(format!(
                    "/backups/{PACKAGE_PREFIX}{day}-010101{PACKAGE_SUFFIX}"
                ))
            })
            .collect();
        let oldest = packages[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let chosen = choose_package(&packages, Some(&oldest)).unwrap();
        assert_eq!(chosen, packages[0], "点名旧包就得开旧包，不能悄悄退回最新");

        // 带不带后缀都认：卷上的名字是给人抄的，抄到哪一段都该能用。
        let bare = oldest.strip_suffix(PACKAGE_SUFFIX).unwrap();
        let chosen = choose_package(&packages, Some(bare)).unwrap();
        assert_eq!(chosen, packages[0]);
    }

    #[test]
    fn naming_a_package_that_is_not_there_names_the_ones_that_are() {
        // 静默退回最新是最坏的一种：那一趟会绿，而它证明的是另一份包。
        let packages: Vec<PathBuf> = ["2026-10-18", "2026-10-19"]
            .iter()
            .map(|day| {
                PathBuf::from(format!(
                    "/backups/{PACKAGE_PREFIX}{day}-010101{PACKAGE_SUFFIX}"
                ))
            })
            .collect();
        let err = choose_package(&packages, Some("2026-01-01-010101"))
            .unwrap_err()
            .to_string();
        assert!(!err.is_empty());
        assert!(
            err.contains("2026-10-18-010101") && err.contains("2026-10-19-010101"),
            "报错要把可选的名字列出来，否则读的人只知道点什么不对：{err}"
        );

        // 卷上一份都没有时，不点名也不能凭空虚开一份。
        assert!(choose_package(&[], None).is_err());
    }

    #[test]
    fn the_second_pick_is_the_package_least_recently_proven() {
        let names: Vec<String> = ["p1", "p2", "p3"].iter().map(|s| s.to_string()).collect();
        let at = |day: u32| Utc.with_ymd_and_hms(2026, 10, day, 0, 0, 0).unwrap();

        // 一份都没成功过：选最老的（名字即时间，p1 在前）。
        assert_eq!(
            pick_least_recently_proven(&names, &BTreeMap::new()),
            Some("p1".to_string())
        );

        // p3 从没成功过，另外两份都有历史：从没成功过的优先。
        let mut proven = BTreeMap::new();
        proven.insert("p1".to_string(), at(19));
        proven.insert("p2".to_string(), at(12));
        assert_eq!(
            pick_least_recently_proven(&names, &proven),
            Some("p3".to_string()),
            "从没被证明过的一份要排在「很久以前证明过」的前面"
        );

        // 三份都有历史：取上次成功最早的那份，于是窗口里的每一份轮流被打开。
        proven.insert("p3".to_string(), at(18));
        assert_eq!(
            pick_least_recently_proven(&names, &proven),
            Some("p2".to_string())
        );

        assert_eq!(pick_least_recently_proven(&[], &proven), None);

        // 第二趟的候选永远不含最新那份：它就是第一趟要开的那一份，留在候选里
        // 会让「从没成功过」的排序每晚都先挑中它，旧包永远轮不到。
        let paths: Vec<PathBuf> = names.iter().map(PathBuf::from).collect();
        assert_eq!(older_than_newest(&paths), &paths[..2]);
        assert!(older_than_newest(&paths[..1]).is_empty());
        assert!(older_than_newest(&[]).is_empty());
    }

    #[test]
    fn the_offsite_column_compares_name_and_size() {
        // 这一列问的是「卷外那份是不是卷上最新那份」。点名开旧包时若拿被打开的那份
        // 去比，一条本来绿的规则会被判红——比较的对象是卷上最新，不是这一趟开的那份。
        let root = std::env::temp_dir().join(format!("cogneva-offsite-{}", uuid::Uuid::new_v4()));
        let vol = root.join("vol");
        let off = root.join("off");
        std::fs::create_dir_all(&vol).unwrap();
        std::fs::create_dir_all(&off).unwrap();
        let name = "cogneva-backup-20261019T031700Z.tar.zst";
        std::fs::write(vol.join(name), vec![0u8; 10]).unwrap();
        std::fs::write(off.join(name), vec![0u8; 10]).unwrap();
        assert!(same_file(&vol.join(name), &off.join(name)));

        // 同名而大小不同 = 复制写到一半。
        std::fs::write(off.join(name), vec![0u8; 4]).unwrap();
        assert!(!same_file(&vol.join(name), &off.join(name)));

        // 名字不同 = 它停在了更早的那一晚。
        let older = "cogneva-backup-20261018T031700Z.tar.zst";
        std::fs::write(vol.join(older), vec![0u8; 10]).unwrap();
        std::fs::write(off.join(older), vec![0u8; 10]).unwrap();
        assert!(!same_file(&vol.join(older), &off.join(name)));

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_window_counts_the_packages_the_second_copy_never_had() {
        // 卷外那份是今晚的，不等于窗口里每一份都有第二份：复制是后来才打开的，
        // 更早的几份从没被复制过。「是不是最新」对这一列是无害的绿。
        let root = std::env::temp_dir().join(format!("cogneva-window-{}", uuid::Uuid::new_v4()));
        let vol = root.join("vol");
        let off = root.join("off");
        std::fs::create_dir_all(&vol).unwrap();
        std::fs::create_dir_all(&off).unwrap();
        let newest = "cogneva-backup-20261019T031700Z.tar.zst";
        let middle = "cogneva-backup-20261018T031700Z.tar.zst";
        let oldest = "cogneva-backup-20261017T031700Z.tar.zst";
        for name in [newest, middle, oldest] {
            std::fs::write(vol.join(name), vec![0u8; 10]).unwrap();
        }
        std::fs::write(off.join(newest), vec![0u8; 10]).unwrap();

        let count = |vol: &std::path::Path, off: &std::path::Path| {
            missing_from_window(&packages_in(vol), &packages_in(off))
        };
        assert_eq!(count(&vol, &off), 2);

        // 补上中间那份，只差最老那份。
        std::fs::write(off.join(middle), vec![0u8; 10]).unwrap();
        assert_eq!(count(&vol, &off), 1);

        // 窗口全覆盖是零，不是缺席。
        std::fs::write(off.join(oldest), vec![0u8; 10]).unwrap();
        assert_eq!(count(&vol, &off), 0);

        // 卷外比卷上多不是欠账：多出来的那份不在这窗口里。
        std::fs::write(off.join("cogneva-backup-20261016T031700Z.tar.zst"), b"x").unwrap();
        assert_eq!(count(&vol, &off), 0);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_drill_reads_its_own_package_operand() {
        let argv = |words: &[&str]| words.iter().map(|w| w.to_string()).collect::<Vec<_>>();
        // 调用方已经跳掉程序名，这里收到的第一个词是子命令名。
        assert_eq!(
            requested_package(argv(&["backup-drill"]).into_iter()),
            Ok((None, false))
        );
        assert_eq!(
            requested_package(argv(&["backup-drill", "--package", "p-1"]).into_iter()),
            Ok((Some("p-1".to_string()), false))
        );
        assert_eq!(
            requested_package(argv(&["backup-drill", "--also-unproven"]).into_iter()),
            Ok((None, true))
        );
        // 两件事不能在同一跑里同时要。
        assert!(requested_package(
            argv(&["backup-drill", "--package", "p-1", "--also-unproven"]).into_iter()
        )
        .is_err());
        // 拼错或多余的操作数必须报错：落回「开最新那份」会让这一趟的结论不可信。
        assert!(requested_package(argv(&["backup-drill", "--packag", "p-1"]).into_iter()).is_err());
        assert!(requested_package(argv(&["backup-drill", "--package"]).into_iter()).is_err());
    }

    #[test]
    fn the_restore_names_the_columns_the_package_carries() {
        // A package written before a column was added must still load: the
        // statement has to name the columns the CSV holds instead of letting
        // the driver map them by position against today's wider table.
        let columns = csv_columns("id,name,created_at\r\n").unwrap();
        assert_eq!(columns, vec!["id", "name", "created_at"]);

        let statement = copy_from_statement("cog_events", &columns);
        assert!(
            statement.contains("(\"id\", \"name\", \"created_at\")"),
            "the column list is what keeps an older package loadable: {statement}"
        );

        // The header comes out of the package, so it is untrusted input to a
        // statement: anything that is not a plain identifier is refused rather
        // than quoted and handed to the server.
        for header in [
            "id; DROP TABLE users",
            "id,na\"me",
            "id,,name",
            "",
            "0id",
            "Id",
        ] {
            assert!(
                csv_columns(header).is_err(),
                "header {header:?} should not become a column list"
            );
        }

        // The refusal must not carry the header back out: the error lands in
        // backup_drills.error and in the package's own logs.
        let err = csv_columns("id; DROP TABLE users").unwrap_err().to_string();
        assert!(
            !err.contains("DROP"),
            "the rejected header must not be echoed: {err}"
        );
    }

    #[test]
    fn a_generated_column_is_left_out_of_the_load_rather_than_condemning_the_package() {
        // A package the old export wrote carries the generated column. Its values
        // are recomputed by the target from the same row, so the load names only
        // the writable columns -- the reason a package of this shape still
        // restores instead of being refused.
        let package = vec!["id".to_string(), "body".to_string(), "is_hot".to_string()];
        let computed = vec!["is_hot".to_string()];
        assert_eq!(
            writable_columns(&package, &computed),
            vec![&"id".to_string(), &"body".to_string()]
        );

        // Nothing to leave out is what selects the direct load instead of the
        // staging table; the two branches must be told apart by the same list.
        assert_eq!(writable_columns(&package, &[]).len(), package.len());
    }

    #[test]
    fn a_column_the_schema_dropped_stops_the_restore_instead_of_losing_its_values() {
        // Naming the package's columns fixes added columns. A column the schema
        // since *dropped* is the other direction: it has no home in the target,
        // so naming it fails loudly and skipping it silently discards data that
        // a row-count comparison cannot see. Restore must refuse and say which.
        let package = vec!["id".to_string(), "retired_at".to_string()];
        let current = vec!["id".to_string(), "created_at".to_string()];
        assert_eq!(
            columns_without_a_home(&package, &current),
            vec![&"retired_at".to_string()]
        );

        // Columns added after the package was written are supplied by defaults,
        // which is the ordinary case and must not be treated as an error.
        let older = vec!["id".to_string()];
        assert!(columns_without_a_home(&older, &current).is_empty());
    }

    #[test]
    fn a_table_the_schema_dropped_is_skipped_and_named_rather_than_condemning_the_package() {
        // 一次删表的迁移之后，保留窗口里的旧包会点出一张今天不存在的表。整包拒绝
        // 等于让那次迁移作废整个窗口；静默跳过则让这份包读起来与完整恢复一模一样。
        // 两条都不是答案：跳过它，并把名字带回去。
        let package = vec![
            "agent_performance".to_string(),
            "messages".to_string(),
            "users".to_string(),
        ];
        let current = vec!["messages".to_string(), "users".to_string()];
        assert_eq!(
            tables_without_a_home(&package, &current),
            vec![&"agent_performance".to_string()]
        );

        // 包里没被删过表时这一格必须是空的，否则每一份包都会背着一份"跳过"的记录。
        let intact = vec!["messages".to_string(), "users".to_string()];
        assert!(tables_without_a_home(&intact, &current).is_empty());
    }

    #[test]
    fn retention_keeps_newest_n_and_ignores_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut names = Vec::new();
        for day in 1..=5 {
            let name = package_name(Utc.with_ymd_and_hms(2026, 9, day, 0, 0, 0).unwrap());
            std::fs::write(dir.path().join(&name), b"x").unwrap();
            names.push(name);
        }
        std::fs::write(dir.path().join("notes.txt"), b"keep me").unwrap();

        let deleted = enforce_retention(dir.path(), 2);

        assert_eq!(deleted.len(), 3, "5 份留 2 份必须删最旧 3 份");
        for name in &names[..3] {
            assert!(!dir.path().join(name).exists(), "{name} 应被删");
        }
        for name in &names[3..] {
            assert!(dir.path().join(name).exists(), "{name} 应保留");
        }
        assert!(dir.path().join("notes.txt").exists(), "非包文件不许动");
    }

    #[test]
    fn secret_manifest_is_applyable_sorted_and_base64() {
        // 复刻 K8s Secret 卷的真实形态：每个 key 是指向 ..data/<key> 的符号
        // 链接，另有 ..2024_01_01_00_00_00.000000000 时间戳目录。
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let data_dir = root.join("..2024_01_01_00_00_00.000000000");
        std::fs::create_dir(&data_dir).unwrap();
        std::fs::write(data_dir.join("pg-password"), b"s3cret\n").unwrap();
        std::fs::write(data_dir.join("jwt-secret"), b"abc").unwrap();
        std::os::unix::fs::symlink(&data_dir, root.join("..data")).unwrap();
        std::os::unix::fs::symlink("..data/pg-password", root.join("pg-password")).unwrap();
        std::os::unix::fs::symlink("..data/jwt-secret", root.join("jwt-secret")).unwrap();

        let (yaml, keys) = build_secret_manifest(root, "cogneva", SECRET_NAME_INTERNAL).unwrap();

        assert_eq!(keys, vec!["jwt-secret", "pg-password"], "key 必须排序");
        assert!(yaml.starts_with("apiVersion: v1\nkind: Secret\n"));
        assert!(yaml.contains("name: cogneva-secrets\n  namespace: cogneva\n"));
        assert!(yaml.contains("  jwt-secret: YWJj\n"));
        assert!(yaml.contains("  pg-password: czNjcmV0Cg==\n"));
        assert!(!yaml.contains("..data"), "内部链接文件不许进 manifest");
    }

    #[test]
    fn secret_manifest_fails_on_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(build_secret_manifest(dir.path(), "cogneva", SECRET_NAME_INTERNAL).is_err());
    }

    #[test]
    fn copy_tree_counts_files_and_bytes() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(src.path().join("memory/raw")).unwrap();
        std::fs::write(src.path().join("memory/raw/a.json"), b"12345").unwrap();
        std::fs::write(src.path().join("top.bin"), b"12").unwrap();

        let (files, bytes) = copy_tree(src.path(), &dst.path().join("storage")).unwrap();

        assert_eq!((files, bytes), (2, 7));
        assert!(dst.path().join("storage/memory/raw/a.json").exists());
        // 源不存在不是错误：新装实例没有 raw 层。
        let (files, bytes) =
            copy_tree(&src.path().join("nonexistent"), &dst.path().join("empty")).unwrap();
        assert_eq!((files, bytes), (0, 0));
    }

    /// 「这一棵不在」与「这一棵是空的」必须分成两种事：`copy_tree` 对两者都回
    /// `(0, 0)`，所以出包前的这个判断就是唯一能把它们分开的地方——它答 `None`
    /// 时，清单里才会多出一条点名对象层的有损面。
    #[test]
    fn a_data_root_without_an_objects_subtree_says_it_has_none() {
        let data = tempfile::tempdir().unwrap();
        assert_eq!(
            local_objects_dir(data.path()),
            None,
            "没有 storage/ 子树时不能假装那是一棵空树"
        );
        std::fs::create_dir_all(data.path().join("storage/bucket")).unwrap();
        assert_eq!(
            local_objects_dir(data.path()),
            Some(data.path().join("storage")),
            "子树在盘上时就该原样交出去"
        );
    }

    #[test]
    fn an_open_credential_surface_stops_the_next_backup_and_a_clean_one_does_not() {
        let at = Utc::now();
        let mut drill = cog_core::BackupDrillRecord {
            drill_id: "drill-20261019T145214Z".into(),
            started_at: at,
            finished_at: at,
            package: Some("cogneva-backup-20261019T103951Z.tar.zst".into()),
            package_written_at: Some(at),
            package_bytes: Some(1),
            restore_source: Some("postgres:5432/cogneva".into()),
            restore_target: Some("postgres:5432/cogneva_drill".into()),
            segment_skew_seconds: Some(1),
            segment_skew_bound_seconds: Some(300),
            packages_on_volume: 1,
            oldest_package_at: Some(at),
            offsite_newest_at: None,
            offsite_matches_newest: None,
            offsite_missing_packages: None,
            offsite_observed_at: None,
            offsite_remote_newest_at: None,
            offsite_key_created_at: None,
            credential_violations_backup_pvc: Some(0),
            credential_violations_offsite: None,
            credential_violations_state_dir: None,
            tables_restored: Some(1),
            tables_expected: Some(1),
            rows_restored: Some(1),
            rows_expected: Some(1),
            source_runs_matched: Some(true),
            generated_columns_recomputed: Vec::new(),
            tables_without_a_home: Vec::new(),
            artifacts_expected: 0,
            artifacts_restored: 0,
            artifact_readings: Vec::new(),
            artifact_errors: Vec::new(),
            error: None,
        };

        // 量到 0 与没量过都不拦：拦的依据必须是量到的数。
        assert_eq!(credential_surface_refusal(Some(&drill)), None);
        assert_eq!(credential_surface_refusal(None), None);

        drill.credential_violations_backup_pvc = Some(2);
        drill.credential_violations_state_dir = Some(1);
        let reason = credential_surface_refusal(Some(&drill)).expect("开放权限要拦住下一份包");
        assert!(
            reason.contains("备份卷 2 条"),
            "读数要说出是哪一处：{reason}"
        );
        assert!(
            reason.contains("状态目录 1 条"),
            "读数要说出是哪一处：{reason}"
        );
        assert!(
            reason.contains("backup-drill"),
            "拦住之后要给出解除的办法：{reason}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_credential_surface_counts_each_open_entry() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("backups");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let pkg = dir.join("cogneva-backup-20261018T031700Z.tar.zst");
        std::fs::write(&pkg, b"x").unwrap();
        std::fs::set_permissions(&pkg, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(credential_violations_in(&dir), Some(0), "0700/0600 是干净");

        // 各多一位组权限：目录一条、包文件一条。
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o750)).unwrap();
        std::fs::set_permissions(&pkg, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(credential_violations_in(&dir), Some(2));

        // 判据只看目录本身与顶层 *.tar.zst：松散文件不进计数。
        let loose = dir.join("notes.txt");
        std::fs::write(&loose, b"x").unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(credential_violations_in(&dir), Some(2));

        // 没有这个落点不是 0：那一格要说的是「有没有量」，不是「干净」。
        assert_eq!(credential_violations_in(&root.path().join("absent")), None);
    }

    #[test]
    fn tar_round_trip_preserves_layout() {
        let staging = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(staging.path().join("pg")).unwrap();
        std::fs::write(staging.path().join("manifest.json"), b"{}").unwrap();
        std::fs::write(staging.path().join("pg/public.t.csv"), b"a,b\n1,2\n").unwrap();

        let out_dir = tempfile::tempdir().unwrap();
        let pkg = out_dir
            .path()
            .join("cogneva-backup-20260914T000000Z.tar.zst");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20260914T000000Z",
            &pkg,
            None,
        )
        .unwrap();

        let extract = tempfile::tempdir().unwrap();
        let root = untar_package(&pkg, extract.path()).unwrap();
        assert_eq!(
            std::fs::read(root.join("pg/public.t.csv")).unwrap(),
            b"a,b\n1,2\n"
        );
        assert!(root.join("manifest.json").exists());
    }

    /// 凭证只住在安全网关里，这条是从「包这个载体」这一侧守的：包不是密文，两片
    /// Secret 清单就根本不进包。
    ///
    /// 没有这条时发生的事不是「少导出一份」，而是**多出两处凭证载体**：卷上一份、
    /// 卷外副本一份，两份都只是 base64，能读文件的人拿走全部 token。0600 与 0700
    /// 挡同机别的账号，挡不住拿到盘的人和 PVC 快照。清单里必须有对应的有损面，
    /// 否则恢复端只会看到 `secrets_keys` 空着，把它读成「老包当年没声明」。
    #[test]
    fn credentials_stay_out_of_a_package_that_is_not_sealed() {
        let internal = tempfile::tempdir().unwrap();
        std::fs::write(internal.path().join("pg-password"), b"hunter2").unwrap();
        std::fs::write(internal.path().join("jwt-secret"), b"jwt").unwrap();
        let oob = tempfile::tempdir().unwrap();
        std::fs::write(oob.path().join("github-token"), b"ghp_x").unwrap();

        let plain = tempfile::tempdir().unwrap();
        let out = plain.path().join("secrets");
        let keys =
            export_secret_manifests(internal.path(), oob.path(), "cogneva", &out, false).unwrap();
        assert!(keys.is_empty(), "没加密就不该声明带了任何键：{keys:?}");
        assert!(
            !out.exists(),
            "连那两行都不该落：一个空的 secrets/ 目录会读成「这一趟确实导过，只是没键」"
        );
        let surfaces = lossy_surfaces(true, false, true);
        assert!(
            surfaces.iter().any(|s| s.name == "credentials"),
            "不导凭证必须被点名成有损面"
        );

        // 有口令时照常导出，两条路不能只有拦的那一半。
        let sealed = tempfile::tempdir().unwrap();
        let out = sealed.path().join("secrets");
        let keys =
            export_secret_manifests(internal.path(), oob.path(), "cogneva", &out, true).unwrap();
        assert!(
            keys.contains(&"cogneva-secrets/pg-password".to_string()),
            "{keys:?}"
        );
        assert!(
            keys.contains(&"cogneva-secrets-oob/github-token".to_string()),
            "{keys:?}"
        );
        let yaml = std::fs::read_to_string(out.join("cogneva-secrets-oob.yaml")).unwrap();
        assert!(yaml.contains("github-token: "), "{yaml}");
    }

    /// 明文包里带着凭证清单就是一份可读的凭证副本——判它的人得能从字节里看出来。
    ///
    /// 这条挡的是写端改动**之前**已经在盘上的那几份：它们的载体身份不会因为写端改了
    /// 而消失，而保留窗口让它们再躺七晚。
    #[test]
    fn a_plaintext_package_carrying_credential_manifests_is_reported() {
        use crate::backup_crypto::PackageKey;
        let staging = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(staging.path().join("secrets")).unwrap();
        std::fs::write(
            staging.path().join("secrets/cogneva-secrets-oob.yaml"),
            b"apiVersion: v1\n",
        )
        .unwrap();
        std::fs::create_dir_all(staging.path().join("pg")).unwrap();
        std::fs::write(staging.path().join("pg/public.t.csv"), b"a,b\n").unwrap();

        let out = tempfile::tempdir().unwrap();
        let plain = out.path().join("cogneva-backup-20261019T000000Z.tar.zst");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20261019T000000Z",
            &plain,
            None,
        )
        .unwrap();
        let reason = plaintext_package_carrying_credentials(&plain)
            .unwrap()
            .expect("明文包里有 Secret 清单，必须报出来");
        assert!(reason.contains("cogneva-secrets-oob.yaml"), "{reason}");

        // 同一个 staging 打成密文包就不再是「可读的副本」：判据是包的形式，不是内容。
        let sealed = out.path().join("cogneva-backup-20261020T000000Z.tar.zst");
        let key = PackageKey::from_passphrase(b"a passphrase kept off this machine");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20261020T000000Z",
            &sealed,
            Some(&key),
        )
        .unwrap();
        assert!(plaintext_package_carrying_credentials(&sealed)
            .unwrap()
            .is_none());

        // 明文的业务包（没有凭证可抄）不该被这条拦：判据是「带着凭证」，不是「明文」。
        std::fs::remove_dir_all(staging.path().join("secrets")).unwrap();
        let clean = out.path().join("cogneva-backup-20261021T000000Z.tar.zst");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20261021T000000Z",
            &clean,
            None,
        )
        .unwrap();
        assert!(plaintext_package_carrying_credentials(&clean)
            .unwrap()
            .is_none());
    }

    #[cfg(unix)]
    #[test]
    fn the_package_and_its_secret_entry_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let mode_of =
            |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

        let staging = tempfile::tempdir().unwrap();
        let secrets = staging.path().join("secrets");
        std::fs::create_dir_all(&secrets).unwrap();
        let yaml = secrets.join("cogneva-secrets-oob.yaml");
        std::fs::write(&yaml, b"apiVersion: v1\n").unwrap();
        restrict_to_owner(&yaml, 0o600).unwrap();

        let out_dir = tempfile::tempdir().unwrap();
        let pkg = out_dir
            .path()
            .join("cogneva-backup-20260914T000000Z.tar.zst");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20260914T000000Z",
            &pkg,
            None,
        )
        .unwrap();
        assert_eq!(
            mode_of(&pkg),
            0o600,
            "包里有 Secret 清单，产物不能对属主以外可读"
        );

        // 解包根目录是调用方给的，自己看着收不回：untar_package 负责把它收成 0700。
        let dest = out_dir.path().join("extract");
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755)).unwrap();
        let root = untar_package(&pkg, &dest).unwrap();
        assert_eq!(mode_of(&dest), 0o700);
        assert_eq!(
            mode_of(&root.join("secrets/cogneva-secrets-oob.yaml")),
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_offsite_copy_is_owner_only_too() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let vol = root.path().join("vol");
        let off = root.path().join("off");
        std::fs::create_dir_all(&vol).unwrap();
        let pkg = vol.join("cogneva-backup-20261019T031700Z.tar.zst");
        std::fs::write(&pkg, vec![7u8; 32]).unwrap();

        copy_offsite(&off, &pkg, 7);

        let copy = off.join("cogneva-backup-20261019T031700Z.tar.zst");
        let mode = std::fs::metadata(&copy).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "卷外那份离开 PVC 之后同样装着 Secret 清单");
        assert!(!off
            .join("cogneva-backup-20261019T031700Z.tar.zst.partial")
            .exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_published_package_is_a_complete_owner_only_copy() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let vol = root.path().join("vol");
        let out = root.path().join("exports");
        std::fs::create_dir_all(&vol).unwrap();
        let pkg = vol.join("cogneva-backup-20261027T031700Z.tar.zst");
        std::fs::write(&pkg, vec![9u8; 64]).unwrap();

        let target = publish_package(&out, &pkg).unwrap();

        assert_eq!(target.file_name().unwrap(), pkg.file_name().unwrap());
        assert_eq!(std::fs::read(&target).unwrap(), vec![9u8; 64]);
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "包离开备份卷之后仍然是整库内容");
        // 下载端只看得到整份包：半个文件的临时名不能留在目录里。
        let leftovers: Vec<String> = std::fs::read_dir(&out)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with('.'))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[cfg(unix)]
    #[test]
    fn exporting_the_same_package_twice_is_one_file_not_two() {
        let root = tempfile::tempdir().unwrap();
        let vol = root.path().join("vol");
        let out = root.path().join("exports");
        std::fs::create_dir_all(&vol).unwrap();
        let pkg = vol.join("cogneva-backup-20261027T031700Z.tar.zst");
        std::fs::write(&pkg, vec![3u8; 16]).unwrap();

        publish_package(&out, &pkg).unwrap();
        publish_package(&out, &pkg).unwrap();

        assert_eq!(std::fs::read_dir(&out).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn a_package_written_before_the_copy_existed_is_copied_by_the_next_run() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let vol = root.path().join("vol");
        let off = root.path().join("off");
        std::fs::create_dir_all(&vol).unwrap();
        std::fs::create_dir_all(&off).unwrap();
        let names = [
            "cogneva-backup-20261017T031701Z.tar.zst",
            "cogneva-backup-20261018T031700Z.tar.zst",
            "cogneva-backup-20261019T031700Z.tar.zst",
        ];
        for (i, name) in names.iter().enumerate() {
            std::fs::write(vol.join(name), vec![i as u8; 16]).unwrap();
        }
        // 卷外复制是第三天才有的事：前两份写下时它还不存在，所以一直缺着。
        std::fs::copy(vol.join(names[2]), off.join(names[2])).unwrap();
        assert_eq!(
            missing_from_window(&packages_in(&vol), &packages_in(&off)),
            2
        );

        assert_eq!(backfill_offsite(&vol, &off, 7), 2);
        // 判据就是演练那一列：补完必须回到 0，否则这条修法只是把同样的缺口留给下一晚。
        assert_eq!(
            missing_from_window(&packages_in(&vol), &packages_in(&off)),
            0,
            "补完之后 offsite_missing_packages 必须读作 0"
        );
        assert_eq!(std::fs::read(off.join(names[0])).unwrap(), vec![0u8; 16]);
        let mode = std::fs::metadata(off.join(names[0]))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "补出来的这一份同样背着 Secret 清单");

        // 第二晚没有缺口，不该再复制一遍。
        assert_eq!(backfill_offsite(&vol, &off, 7), 0);
    }

    #[test]
    fn the_package_frame_carries_a_content_checksum() {
        let staging = tempfile::tempdir().unwrap();
        std::fs::write(staging.path().join("f"), b"x").unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let pkg = out_dir
            .path()
            .join("cogneva-backup-20260914T000000Z.tar.zst");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20260914T000000Z",
            &pkg,
            None,
        )
        .unwrap();

        // zstd frame: 4 字节 magic，第 5 字节是 Frame_Header_Descriptor，其
        // bit2(0x04) 即 Content_Checksum_flag。默认不写，这里必须为 1——否则
        // `zstd -t` 只解不校，翻转一个位仍然判通过。
        let bytes = std::fs::read(&pkg).unwrap();
        assert_eq!(&bytes[..4], &[0x28, 0xb5, 0x2f, 0xfd], "not a zstd frame");
        assert_ne!(
            bytes[4] & 0x04,
            0,
            "the zstd frame must set Content_Checksum_flag"
        );
    }

    /// 包级加密的验收：盘上的字节离开这台机器后，没有口令就只是一个不透明的文件。
    ///
    /// 四条一起量，缺一条都能让「加密了」徒有其名：字节里没有明文、按 zstd 解不开、
    /// 没钥匙解包明确被拒（而不是解出垃圾）、有钥匙能逐字节还原。
    #[test]
    fn a_package_is_not_readable_without_the_key() {
        use crate::backup_crypto::PackageKey;
        let staging = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(staging.path().join("secrets")).unwrap();
        let secret_yaml = b"apiVersion: v1\nkind: Secret\nstringData:\n  pg-password: hunter2\n";
        std::fs::write(
            staging.path().join("secrets/cogneva-secrets.yaml"),
            secret_yaml,
        )
        .unwrap();

        let out_dir = tempfile::tempdir().unwrap();
        let pkg = out_dir
            .path()
            .join("cogneva-backup-20260914T000000Z.tar.zst");
        let key = PackageKey::from_passphrase(b"a passphrase kept off this machine");
        tar_package_with_key(
            staging.path(),
            "cogneva-backup-20260914T000000Z",
            &pkg,
            Some(&key),
        )
        .unwrap();

        let bytes = std::fs::read(&pkg).unwrap();
        assert!(
            bytes.starts_with(crate::backup_crypto::MAGIC),
            "加密包有自己的文件头"
        );
        assert!(
            !bytes
                .windows(b"pg-password".len())
                .any(|w| w == b"pg-password"),
            "密文里不该出现包内的明文"
        );
        // 构造解码器不读帧头，拿它当判据会永远通过；真正要问的是「解出来了吗」。
        use std::io::Read;
        let mut decoder = zstd::Decoder::new(std::io::Cursor::new(bytes.as_slice()))
            .expect("zstd 解码器的构造本身不校验帧头");
        let mut plain = Vec::new();
        assert!(
            decoder.read_to_end(&mut plain).is_err(),
            "拿 zstd 直接解必须失败，否则「加密」只是换了个文件头"
        );

        let dest = out_dir.path().join("extract");
        let err = untar_package_with_key(&pkg, &dest, None).unwrap_err();
        assert!(
            err.to_string().contains(crate::backup_crypto::KEY_ENV),
            "没钥匙要明说是缺哪一项，不能报成包损坏：{err}"
        );

        let root = untar_package_with_key(&pkg, &dest, Some(&key)).unwrap();
        assert_eq!(
            std::fs::read(root.join("secrets/cogneva-secrets.yaml")).unwrap(),
            secret_yaml
        );
    }

    #[test]
    fn csv_rows_are_counted_by_records_not_by_newlines() {
        let dir = tempfile::tempdir().unwrap();
        // 普通两行；0 行只有表头；字段里嵌换行；字段里是成对的转义引号。
        let cases: [(&str, &[u8], i64); 4] = [
            ("plain.csv", b"a,b\n1,2\n3,4\n", 2),
            ("header-only.csv", b"a,b\n", 0),
            ("embedded-newline.csv", b"a,b\n\"x\ny\",2\n", 1),
            ("escaped-quotes.csv", b"a,b\n\"he said \"\"hi\"\"\",2\n", 1),
        ];
        for (name, body, want) in cases {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            assert_eq!(count_csv_rows(&path).unwrap(), want, "{name}");
        }
    }

    fn manifest_for(pkg: &BackupManifest) -> String {
        serde_json::to_string(pkg).unwrap()
    }

    fn sample_manifest() -> BackupManifest {
        BackupManifest {
            version: "1.0.0".into(),
            revision: "abc".into(),
            instance_fingerprint: "fp".into(),
            namespace: "cogneva".into(),
            created_at: now_rfc3339(),
            secrets_keys: vec!["cogneva-secrets/jwt-secret".into()],
            postgres: PostgresReport {
                tables: vec![TableReport {
                    name: "t".into(),
                    rows: 2,
                }],
                sequences: 1,
            },
            qdrant: vec![CollectionReport {
                name: "c1".into(),
                snapshot: "c1-123.snapshot".into(),
                points: 7,
            }],
            objects: ObjectsReport { files: 1, bytes: 5 },
            artifacts: Vec::new(),
            image: "registry.example/cogneva:test".into(),
            schema_version: 1,
            lossy_surfaces: lossy_surfaces(false, true, true),
            segments: stamps(&[
                ("postgres", "2026-10-19T00:00:00Z"),
                ("artifacts", "2026-10-19T00:00:10Z"),
            ]),
            max_segment_skew_seconds: 300,
            evolution_bundle: None,
        }
    }

    fn write_package_like(manifest: &BackupManifest, dir: &Path) {
        std::fs::create_dir_all(dir.join("pg")).unwrap();
        std::fs::create_dir_all(dir.join("qdrant")).unwrap();
        std::fs::create_dir_all(dir.join("objects/storage")).unwrap();
        std::fs::create_dir_all(dir.join("secrets")).unwrap();
        std::fs::write(dir.join("manifest.json"), manifest_for(manifest)).unwrap();
        std::fs::write(dir.join("pg/public.t.csv"), b"a,b\n1,2\n3,4\n").unwrap();
        std::fs::write(
            dir.join("pg/sequences.sql"),
            b"SELECT setval('t_id_seq', 2, true);\n",
        )
        .unwrap();
        std::fs::write(dir.join("qdrant/c1.snapshot"), b"snap").unwrap();
        std::fs::write(dir.join("objects/storage/x.bin"), b"12345").unwrap();
        std::fs::write(
            dir.join("secrets/cogneva-secrets.yaml"),
            b"apiVersion: v1\n",
        )
        .unwrap();
    }

    #[test]
    fn a_package_that_matches_its_manifest_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = sample_manifest();
        write_package_like(&manifest, dir.path());
        verify_package_against_manifest(dir.path(), &manifest).unwrap();
    }

    #[test]
    fn a_package_that_disagrees_with_its_manifest_is_rejected() {
        // 行数对不上：包被截断过的典型形状。
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = sample_manifest();
        write_package_like(&manifest, dir.path());
        manifest.postgres.tables[0].rows = 3;
        let err = verify_package_against_manifest(dir.path(), &manifest).unwrap_err();
        assert!(err.to_string().contains("declared 3 row"), "{err}");

        // 声明的表不在包里。
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = sample_manifest();
        write_package_like(&manifest, dir.path());
        std::fs::remove_file(dir.path().join("pg/public.t.csv")).unwrap();
        manifest.postgres.tables[0].rows = 0;
        let err = verify_package_against_manifest(dir.path(), &manifest).unwrap_err();
        assert!(err.to_string().contains("is not in the package"), "{err}");

        // objects 计数对不上。
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = sample_manifest();
        write_package_like(&manifest, dir.path());
        manifest.objects.bytes = 6;
        assert!(verify_package_against_manifest(dir.path(), &manifest).is_err());

        // 声明的集合缺快照文件。
        let dir = tempfile::tempdir().unwrap();
        let manifest = sample_manifest();
        write_package_like(&manifest, dir.path());
        std::fs::remove_file(dir.path().join("qdrant/c1.snapshot")).unwrap();
        assert!(verify_package_against_manifest(dir.path(), &manifest).is_err());
    }

    #[test]
    fn an_older_manifest_missing_a_section_is_not_an_error() {
        // 旧格式的包：manifest 里没有 qdrant / objects / postgres 段。按原设计
        // 缺段不是错——解析要能过（serde default），核包要跳过该段而不是当成
        // 「声明为零」去拦。
        let json = r#"{"version":"0.1.0","revision":"old","instance_fingerprint":"fp",
                       "namespace":"cogneva","created_at":"2026-01-01T00:00:00Z"}"#;
        let manifest: BackupManifest = serde_json::from_str(json).unwrap();
        assert!(manifest.qdrant.is_empty());
        assert!(manifest.postgres.tables.is_empty());
        assert_eq!(manifest.objects.files, 0);

        // 一个空目录（没有 pg/qdrant/objects/secrets）不该被判非零。
        let dir = tempfile::tempdir().unwrap();
        verify_package_against_manifest(dir.path(), &manifest).unwrap();
    }

    #[test]
    fn package_instant_reads_the_clock_out_of_the_name() {
        let at = Utc.with_ymd_and_hms(2026, 10, 15, 3, 17, 0).unwrap();
        assert_eq!(package_instant(&package_name(at)), Some(at));
        // 不是包名，或者名字里没有可解的时刻：宁可没有这个数，也不要一个猜的。
        assert_eq!(package_instant("notes.txt"), None);
        assert_eq!(
            package_instant("cogneva-backup-notatimestamp.tar.zst"),
            None
        );
        assert_eq!(package_instant("other-20261015T031700Z.tar.zst"), None);
    }

    #[test]
    fn retained_state_reports_the_count_and_the_oldest_name_instant() {
        let dir = tempfile::tempdir().unwrap();
        for day in [9, 11, 10] {
            std::fs::write(
                dir.path().join(package_name(
                    Utc.with_ymd_and_hms(2026, 10, day, 3, 17, 0).unwrap(),
                )),
                b"x",
            )
            .unwrap();
        }
        std::fs::write(dir.path().join("notes.txt"), b"keep me").unwrap();

        let (kept, oldest) = retained_state(dir.path());

        assert_eq!(kept, Some(3), "非包文件不算留下的份数");
        assert_eq!(
            oldest,
            Some(Utc.with_ymd_and_hms(2026, 10, 9, 3, 17, 0).unwrap()),
            "最老的那个要按名字里的时刻算，不是按目录里读到的顺序"
        );
    }

    #[test]
    fn url_password_finds_only_what_is_there() {
        assert_eq!(
            url_password("postgres://cog_user:s3cret@postgres:5432/cogneva").as_deref(),
            Some("s3cret")
        );
        // 没有口令的 URL 没有要抹的东西，也不能凭空抹出个空串再到处替换。
        assert_eq!(
            url_password("postgres://cog_user@postgres:5432/cogneva"),
            None
        );
        assert_eq!(
            url_password("postgres://cog_user:@postgres:5432/cogneva"),
            None
        );
        assert_eq!(url_password("http://qdrant:6333"), None);
        assert_eq!(url_password("not a url"), None);
    }

    #[test]
    fn the_recorded_error_does_not_carry_the_database_password() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BackupConfig {
            database_url: "postgres://cog_user:s3cret@postgres:5432/cogneva".into(),
            qdrant_http_url: "http://qdrant:6333".into(),
            backup_dir: dir.path().to_path_buf(),
            secrets_dir: dir.path().to_path_buf(),
            oob_secrets_dir: dir.path().to_path_buf(),
            data_dir: dir.path().to_path_buf(),
            extra_data_roots: Vec::new(),
            git_remote_dir: None,
            publish_dir: None,
            keep_count: 7,
            run_deadline_seconds: DEFAULT_RUN_DEADLINE_SECONDS,
            offsite_dir: None,
            namespace: "cogneva".into(),
        };
        let err: Box<dyn std::error::Error> = Box::new(std::io::Error::other(
            "connect failed for postgres://cog_user:s3cret@postgres:5432/cogneva",
        ));

        let recorded = recordable_error(err.as_ref(), &cfg);

        assert!(
            !recorded.contains("s3cret"),
            "口令跟着行一起进备份包了：{recorded}"
        );
        assert!(
            recorded.contains("[redacted]"),
            "抹掉之后要留下痕迹，不然读的人以为错误本来就没写 URL：{recorded}"
        );
    }

    #[test]
    fn the_second_copy_is_written_and_pruned_by_the_same_count() {
        let volume = tempfile::tempdir().unwrap();
        let offsite = tempfile::tempdir().unwrap();
        for day in [16, 17] {
            let name = package_name(Utc.with_ymd_and_hms(2026, 10, day, 3, 17, 0).unwrap());
            std::fs::write(offsite.path().join(name), b"old").unwrap();
        }
        let newest = package_name(Utc.with_ymd_and_hms(2026, 10, 18, 3, 17, 0).unwrap());
        let package = volume.path().join(&newest);
        std::fs::write(&package, b"new bytes").unwrap();

        copy_offsite(offsite.path(), &package, 2);

        assert!(
            offsite.path().join(&newest).exists(),
            "第二份必须是这一晚的包"
        );
        assert_eq!(
            std::fs::read(offsite.path().join(&newest)).unwrap(),
            b"new bytes"
        );
        assert_eq!(
            packages_in(offsite.path()).len(),
            2,
            "卷外也要按同一个份数裁，不然它只涨不落"
        );
    }

    #[test]
    fn a_half_written_copy_never_looks_like_a_package() {
        // 复制先写 `<包名>.partial` 再改名。演练是遍历目录找最新包的，一个半份
        // 文件如果匹配包名形状，就会被它当成「最新那份」去装。
        let dir = tempfile::tempdir().unwrap();
        let name = package_name(Utc.with_ymd_and_hms(2026, 10, 18, 3, 17, 0).unwrap());
        std::fs::write(dir.path().join(format!("{name}.partial")), b"half").unwrap();

        assert!(packages_in(dir.path()).is_empty());
    }

    #[test]
    fn with_database_swaps_only_the_path_and_keeps_the_query() {
        // 演练库和源库只差一个库名：把整条 URL 除了路径原样搬过去，连接参数
        // （sslmode、连接超时）才不会在演练那一侧悄悄变掉。
        assert_eq!(
            with_database(
                "postgres://cog_user:s3cret@postgres:5432/cogneva",
                DRILL_DATABASE
            ),
            "postgres://cog_user:s3cret@postgres:5432/cog_drill"
        );
        assert_eq!(
            with_database(
                "postgres://cog_user:s3cret@postgres:5432/cogneva?sslmode=disable",
                DRILL_DATABASE
            ),
            "postgres://cog_user:s3cret@postgres:5432/cog_drill?sslmode=disable"
        );
        // 路径为空（`/` 结尾）：库名照样要补上，不能拼出一个空库名的 URL。
        assert_eq!(
            with_database("postgres://cog_user@postgres:5432/", DRILL_DATABASE),
            "postgres://cog_user@postgres:5432/cog_drill"
        );
        // 连路径都没有：补路径。没有 `://` 的串不是 URL，原样返回——猜一个
        // 出来只会在建库的时候炸在一个更难读的地方。
        assert_eq!(
            with_database("postgres://cog_user@postgres:5432", DRILL_DATABASE),
            "postgres://cog_user@postgres:5432/cog_drill"
        );
        assert_eq!(with_database("not a url", DRILL_DATABASE), "not a url");
    }

    #[test]
    fn table_name_whitelist_blocks_injection() {
        assert!(validate_table_name("audit_logs").is_ok());
        assert!(validate_table_name("t1").is_ok());
        assert!(validate_table_name("evil\"; DROP TABLE x; --").is_err());
        assert!(validate_table_name("Upper").is_err());
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn extra_data_roots_must_name_their_path_and_not_collide() {
        let var = "COGNEVA_BACKUP_EXTRA_DATA_ROOTS";
        assert!(parse_extra_data_roots(var, None).unwrap().is_empty());
        assert!(parse_extra_data_roots(var, Some("  ".into()))
            .unwrap()
            .is_empty());
        let roots = parse_extra_data_roots(
            var,
            Some("evolution=/var/lib/cogneva-evolution-data,memory=/var/lib/memory".into()),
        )
        .unwrap();
        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0].0, "evolution");
        assert_eq!(roots[0].1, PathBuf::from("/var/lib/cogneva-evolution-data"));

        // 坏条目让备份失败，不静默跳过：少一个根而包照样「成功」正是换机后
        // 才发现某份状态从没进过包的形状。
        assert!(parse_extra_data_roots(var, Some("evolution".into())).is_err());
        assert!(parse_extra_data_roots(var, Some("=/var/lib/x".into())).is_err());
        assert!(parse_extra_data_roots(var, Some("app=/var/lib/x".into())).is_err());
        assert!(parse_extra_data_roots(var, Some("a=/x,a=/y".into())).is_err());
    }

    /// 只填本节用得到的段：其余段空着，恢复产物层不看它们。
    fn manifest_with(artifacts: Vec<ArtifactRootReport>) -> BackupManifest {
        BackupManifest {
            version: "0.1.0".into(),
            revision: "r".into(),
            instance_fingerprint: "fp".into(),
            namespace: "cogneva".into(),
            created_at: "2026-10-19T00:00:00Z".into(),
            secrets_keys: Vec::new(),
            postgres: PostgresReport::default(),
            qdrant: Vec::new(),
            objects: ObjectsReport::default(),
            artifacts,
            image: String::new(),
            schema_version: 0,
            lossy_surfaces: Vec::new(),
            segments: Vec::new(),
            max_segment_skew_seconds: 0,
            evolution_bundle: None,
        }
    }

    /// 代次判据：包比本二进制新，装回去会缺列——必须拒绝，不是警告。
    #[test]
    fn a_package_from_a_newer_schema_is_refused() {
        let mut manifest = manifest_with(Vec::new());
        manifest.revision = "abc".into();
        manifest.schema_version = 43;
        let err = check_provenance_against(&manifest, "abc", 42, &RevAcceptance::default())
            .expect_err("更新的 schema 不能装");
        assert!(err.to_string().contains("schema"), "{err}");
    }

    /// 包更旧是正常升级路径：迁移往前跑，不该拦。
    #[test]
    fn an_older_schema_package_is_a_normal_upgrade() {
        let mut manifest = manifest_with(Vec::new());
        manifest.revision = "abc".into();
        manifest.schema_version = 41;
        check_provenance_against(&manifest, "abc", 42, &RevAcceptance::default()).unwrap();
    }

    /// 同一个 rev 的包直接放行：这是日常恢复，不该要任何额外声明。
    #[test]
    fn a_package_from_the_same_revision_is_accepted() {
        let mut manifest = manifest_with(Vec::new());
        manifest.revision = "abc".into();
        manifest.schema_version = 42;
        check_provenance_against(&manifest, "abc", 42, &RevAcceptance::default()).unwrap();
    }

    /// 跨代恢复默认拒绝，且错误里要说清怎么显式接受。
    #[test]
    fn a_package_from_another_revision_needs_an_explicit_accept() {
        let mut manifest = manifest_with(Vec::new());
        manifest.revision = "0000000-not-this-binary".into();
        manifest.schema_version = 42;
        let err = check_provenance_against(&manifest, "abc", 42, &RevAcceptance::default())
            .expect_err("跨代默认要拦");
        assert!(
            err.to_string()
                .contains("COGNEVA_RESTORE_ACCEPT_REV_MISMATCH"),
            "错误里要点名那个开关：{err}"
        );
    }

    /// 包比源码旧是正常的升级路径：脚本在宿主上证明了「包 rev 是本源的祖先」，
    /// 二进制核对证明点名的两个 rev 就是眼前的这一对，放行，不要人再按一次开关。
    #[test]
    fn a_package_from_an_ancestor_revision_is_accepted_without_a_manual_accept() {
        let mut manifest = manifest_with(Vec::new());
        manifest.revision = "abc".into();
        manifest.schema_version = 42;
        let acceptance = RevAcceptance {
            explicit: false,
            ancestry_package: Some("abc".into()),
            ancestry_source: Some("def".into()),
        };
        check_provenance_against(&manifest, "def", 42, &acceptance).unwrap();
    }

    /// 证明是从另一次恢复上抄来的（rev 与眼前的包或二进制对不上）时不算数。
    #[test]
    fn an_ancestry_proof_for_other_revisions_is_not_a_proof() {
        let mut manifest = manifest_with(Vec::new());
        manifest.revision = "abc".into();
        manifest.schema_version = 42;
        let stale_source = RevAcceptance {
            explicit: false,
            ancestry_package: Some("abc".into()),
            ancestry_source: Some("011".into()),
        };
        assert!(check_provenance_against(&manifest, "def", 42, &stale_source).is_err());
        let wrong_package = RevAcceptance {
            explicit: false,
            ancestry_package: Some("000".into()),
            ancestry_source: Some("def".into()),
        };
        assert!(check_provenance_against(&manifest, "def", 42, &wrong_package).is_err());
    }

    /// 有损面每一条都要能回答「丢了什么、为什么可丢」；只有名字的条目等于把
    /// 问题留给下一个读包的人。
    #[test]
    fn every_lossy_surface_says_what_is_lost_and_why_it_is_ok() {
        // 两种取值都要说得清：带了就不该再把那一格列成损失，没带才是。
        for bundle_present in [false, true] {
            for credentials_present in [false, true] {
                for objects_absent in [false, true] {
                    let surfaces =
                        lossy_surfaces(bundle_present, credentials_present, objects_absent);
                    assert!(!surfaces.is_empty());
                    for s in &surfaces {
                        assert!(!s.name.is_empty());
                        assert!(!s.lost.is_empty(), "{} 没写丢了什么", s.name);
                        assert!(
                            !s.acceptable_because.is_empty(),
                            "{} 没写为什么可丢",
                            s.name
                        );
                    }
                    let named = surfaces.iter().any(|s| s.name == "evolution-bare-repo");
                    assert_eq!(named, !bundle_present, "bundle_present={bundle_present}");
                    let named = surfaces.iter().any(|s| s.name == "credentials");
                    assert_eq!(
                        named, !credentials_present,
                        "credentials_present={credentials_present}"
                    );
                    let named = surfaces.iter().any(|s| s.name == "objects");
                    assert_eq!(named, objects_absent, "objects_absent={objects_absent}");
                    // 只有「换机后得有人去做」的两面是这样；其余都是平台自己长回来，
                    // 收尾那句话只点这一份名单。
                    let mut expected: Vec<&str> = Vec::new();
                    if objects_absent {
                        expected.push("objects");
                    }
                    if !credentials_present {
                        expected.push("credentials");
                    }
                    let follow_up: Vec<&str> = surfaces
                        .iter()
                        .filter(|s| s.recovery == SurfaceRecovery::NeedsFollowUp)
                        .map(|s| s.name.as_str())
                        .collect();
                    assert_eq!(
                        follow_up, expected,
                        "bundle_present={bundle_present} \
                         credentials_present={credentials_present} objects_absent={objects_absent}"
                    );
                }
            }
        }
    }

    /// 老包里的有损面没有 `recovery` 这一格。缺省必须读成「要人去看」：没声明过的
    /// 面被判成自己会长回来，就是静默放过——而那正是这张表存在的原因。
    #[test]
    fn a_surface_from_an_older_package_is_not_treated_as_self_rebuilding() {
        let surface: LossySurface = serde_json::from_str(
            r#"{"name":"objects","lost":"对象层","acceptable_because":"按约定找 storage/"}"#,
        )
        .unwrap();
        assert_eq!(surface.recovery, SurfaceRecovery::NeedsFollowUp);
    }

    /// 收尾那句话要按「换机后谁得动手」分开读。全部照列会让它每次都亮（nats / redis /
    /// meilisearch 每次恢复都缺席），一个都不列则「对象层没带」与「什么都带回来了」同形。
    #[test]
    fn the_restore_summary_names_only_the_surfaces_that_need_follow_up() {
        // 老包没有这一格：读出来就是一句完成。
        let mut manifest = sample_manifest();
        manifest.lossy_surfaces.clear();
        assert_eq!(restore_summary(&manifest), "restore complete");

        // 基础那几项（含数据根下不带走的子目录）都自己长回来，外加包带了对象与凭证：
        // 收尾不该点名任何东西。
        manifest.lossy_surfaces = lossy_surfaces(true, true, false);
        assert!(!manifest.lossy_surfaces.is_empty());
        assert_eq!(restore_summary(&manifest), "restore complete");

        // 没带对象层、也没带凭证：这两样必须被点名，顺序照清单。
        manifest.lossy_surfaces = lossy_surfaces(true, false, true);
        let summary = restore_summary(&manifest);
        let objects = summary
            .find("objects")
            .unwrap_or_else(|| panic!("{summary}"));
        let credentials = summary
            .find("credentials")
            .unwrap_or_else(|| panic!("{summary}"));
        assert!(objects < credentials, "{summary}");
        for rebuilt in ["nats", "redis", "meilisearch", "cluster-registry"] {
            assert!(
                !summary.contains(rebuilt),
                "{rebuilt} 自己长回来，不该进收尾：{summary}"
            );
        }
    }

    /// 清单说带了 bundle，包里却没有那一份：核包阶段就要停，别等种的时候才发现。
    #[test]
    fn a_package_that_declares_a_bundle_without_the_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = sample_manifest();
        manifest.evolution_bundle = Some(EvolutionBundleReport {
            bytes: 12,
            refs: 1,
            head: "refs/heads/main".into(),
            commit: "abc".into(),
        });
        write_package_like(&manifest, dir.path());
        let err = verify_package_against_manifest(dir.path(), &manifest).unwrap_err();
        assert!(err.to_string().contains("evolution.bundle"), "{err}");

        std::fs::create_dir_all(dir.path().join("git")).unwrap();
        std::fs::write(dir.path().join("git/evolution.bundle"), b"0123456789ab").unwrap();
        verify_package_against_manifest(dir.path(), &manifest).unwrap();

        // 字节数对不上同样是坏：恢复端要靠它确认「种回去的是出包那一份」。
        std::fs::write(dir.path().join("git/evolution.bundle"), b"short").unwrap();
        let err = verify_package_against_manifest(dir.path(), &manifest).unwrap_err();
        assert!(err.to_string().contains("package holds 5"), "{err}");
    }

    /// 打包与种子走一遍真 git：打出来的是完整历史，种进空仓后 ref 与 HEAD 都在；
    /// 再种一次不改变任何东西（可重入的恢复会再跑这一段）。
    #[test]
    fn an_evolution_bundle_round_trips_into_a_fresh_bare_repo() {
        let src = tempfile::tempdir().unwrap();
        let src_path = src.path();
        let run = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(src_path)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        run(&["init", "-q"]);
        run(&["config", "user.email", "t@example.com"]);
        run(&["config", "user.name", "t"]);
        run(&["symbolic-ref", "HEAD", "refs/heads/main"]);
        std::fs::write(src_path.join("a"), b"a").unwrap();
        run(&["add", "a"]);
        run(&["commit", "-qm", "a", "--", "a"]);
        run(&["branch", "evolution/one"]);
        let head_commit = run(&["rev-parse", "HEAD"]);

        // 备份读到的是进化用的 bare 仓（`/git-remote` 那一卷），不是检出的工作区。
        let bare = tempfile::tempdir().unwrap();
        let clone = std::process::Command::new("git")
            .args(["clone", "-q", "--bare"])
            .arg(src_path)
            .arg(bare.path())
            .output()
            .unwrap();
        assert!(
            clone.status.success(),
            "git clone --bare: {}",
            String::from_utf8_lossy(&clone.stderr)
        );

        let out = tempfile::tempdir().unwrap();
        let report = bundle_evolution(Some(bare.path()), out.path())
            .unwrap()
            .expect("有提交的仓必须打得出 bundle");
        assert!(report.bytes > 0);
        assert_eq!(report.refs, 2, "main 与 evolution/one 两个 ref");
        assert_eq!(report.head, "refs/heads/main");
        assert_eq!(report.commit, head_commit);

        let bundle = out.path().join("evolution.bundle");
        let target = tempfile::tempdir().unwrap();
        seed_evolution_bundle_into(&bundle, &report, target.path()).unwrap();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(target.path())
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        assert_eq!(git(&["rev-parse", "refs/heads/main"]), head_commit);
        assert_eq!(git(&["rev-parse", "refs/heads/evolution/one"]), head_commit);
        assert_eq!(git(&["symbolic-ref", "HEAD"]), "refs/heads/main");

        // 第二遍：同一份包再种一次，ref 与 HEAD 原样。
        seed_evolution_bundle_into(&bundle, &report, target.path()).unwrap();
        assert_eq!(git(&["rev-parse", "refs/heads/main"]), head_commit);
        assert_eq!(git(&["symbolic-ref", "HEAD"]), "refs/heads/main");
    }

    fn manifest_with_segments(segments: Vec<SegmentStamp>, bound: u64) -> BackupManifest {
        let mut manifest = manifest_with(Vec::new());
        manifest.segments = segments;
        manifest.max_segment_skew_seconds = bound;
        manifest
    }

    fn stamps(times: &[(&str, &str)]) -> Vec<SegmentStamp> {
        times
            .iter()
            .map(|(name, at)| SegmentStamp {
                name: name.to_string(),
                at: at.to_string(),
            })
            .collect()
    }

    /// 相隔太久的几段不是「同一份数据」：恢复端要按包自己声明的界拒收。
    #[test]
    fn a_package_whose_segments_are_far_apart_is_refused() {
        let manifest = manifest_with_segments(
            stamps(&[
                ("postgres", "2026-10-19T00:00:00Z"),
                ("objects", "2026-10-19T00:20:00Z"),
            ]),
            300,
        );
        let err = check_segment_skew(&manifest).unwrap_err();
        assert!(err.contains("1200"), "错误里要点出差了多少秒：{err}");
        assert!(err.contains("postgres"), "错误里要点出是哪两段：{err}");
        assert!(err.contains("objects"), "错误里要点出是哪两段：{err}");
    }

    #[test]
    fn a_package_whose_segments_are_close_is_accepted() {
        let manifest = manifest_with_segments(
            stamps(&[
                ("postgres", "2026-10-19T00:00:00Z"),
                ("artifacts", "2026-10-19T00:01:30Z"),
            ]),
            300,
        );
        assert!(check_segment_skew(&manifest).is_ok());
    }

    /// 旧格式没有分段表：当年没声明，不是「相隔 0 秒」——不判，也不拦。
    #[test]
    fn a_package_without_segment_stamps_is_not_judged() {
        let manifest = manifest_with_segments(Vec::new(), 0);
        assert!(check_segment_skew(&manifest).is_ok());
    }

    /// 成对的声明只出现一半，读不出该按什么判：拒绝，不默认给一个界。
    #[test]
    fn a_package_that_declares_segments_without_a_bound_is_refused() {
        let manifest = manifest_with_segments(stamps(&[("postgres", "2026-10-19T00:00:00Z")]), 0);
        assert!(check_segment_skew(&manifest).is_err());
    }

    /// 时刻读不出来＝声明不可读，不能当成「没有偏差」放过去。
    #[test]
    fn a_segment_stamp_that_does_not_parse_is_refused() {
        let manifest = manifest_with_segments(stamps(&[("postgres", "不是时刻")]), 300);
        assert!(check_segment_skew(&manifest).is_err());
    }

    /// 引擎把决策统计写在哪，备份就去哪取——这条接线没有任何东西核过。
    ///
    /// 写方按数据根基拼一条路径落盘，备份方按数据根再拼一条路径取。备份侧取
    /// 文件用的是 `is_file()`：两个名字一旦不一样，它不报错、不告警，只是那一段
    /// 证据整段不进包，而「包里没带」与「本来就没有」在清单上同形——换机之后
    /// 调参证据全丢，谁也不会在备份那一刻知道。所以这里不比对字符串，而是让
    /// 引擎真的写下一次决策，再问备份有没有取到。
    #[tokio::test]
    async fn the_engine_writes_decision_stats_where_the_backup_looks() {
        let data = tempfile::tempdir().unwrap();
        let engine = cog_reflection::MetaLearningEngine::with_durable_state(
            std::sync::Arc::new(cog_reflection::InMemoryRecorder::new()),
            cog_reflection::DecisionStatsSnapshot::new(cog_reflection::decision_stats_path(
                data.path(),
            )),
        )
        .await;
        let group = cog_core::DecisionGroupKey::by_task_type("backup-wiring");
        engine
            .record(
                cog_core::DecisionCategory::PgeMode,
                &group,
                "adopt",
                cog_core::DecisionOutcome::Success,
            )
            .await
            .unwrap();
        let written = cog_reflection::decision_stats_path(data.path());
        assert!(
            written.is_file(),
            "引擎没有落在它自己声明的位置上：{}",
            written.display()
        );

        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("app", data.path(), staging.path())
            .await
            .unwrap();
        assert!(
            report.decision_stats.is_some(),
            "备份没取到引擎刚写下的那份决策统计——它按另一条路径找，而那边 is_file() \
             为假时不报错"
        );
        assert!(
            staging
                .path()
                .join("artifacts/app")
                .join(cog_reflection::DATA_SUBDIR)
                .join(cog_reflection::DECISION_STATS_FILE)
                .is_file(),
            "包的产物层里没有那份统计表"
        );
    }

    /// 进程把策略库建在哪，备份就去哪抄——这条接线同样没有任何东西核过。
    ///
    /// 备份侧判的是 `is_dir()`：两侧的目录名一旦分家，它不报错，只是整棵策略树
    /// 不进包，清单上 `policies_present` 与「这个根本来没有策略库」同形。恢复端
    /// 不会核对一个没被声明的东西，于是换机之后策略链从零开始，而备份那一刻的
    /// 读数全绿。
    #[tokio::test]
    async fn the_store_the_process_builds_is_the_tree_the_backup_copies() {
        let data = tempfile::tempdir().unwrap();
        let store = cog_reflection::PolicyStore::new(cog_reflection::policies_path(data.path()));
        store
            .save_new_version(
                cog_reflection::META_LEARNING_POLICY,
                serde_json::json!({"min_samples": 40}),
                "t",
            )
            .await
            .unwrap();
        let written =
            cog_reflection::policies_path(data.path()).join(cog_reflection::META_LEARNING_POLICY);
        assert!(
            written.is_dir(),
            "策略库没有落在它自己声明的位置上：{}",
            written.display()
        );

        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("app", data.path(), staging.path())
            .await
            .unwrap();
        assert!(
            report.policies_present,
            "备份没找到进程刚建的策略库——它按另一个目录名找，而那边 is_dir() 为假时不报错"
        );
        assert_eq!(
            report.policy_stores.len(),
            1,
            "包里带的策略库条数与盘上不符：{:?}",
            report.policy_stores
        );
        assert!(staging
            .path()
            .join("artifacts/app")
            .join(cog_reflection::POLICIES_SUBDIR)
            .is_dir());
    }

    #[tokio::test]
    async fn the_workspace_the_editor_serves_is_the_workspace_the_backup_carries() {
        let data = tempfile::tempdir().unwrap();
        // 源树按编辑器那一侧的取法建：工作区根在部署里可能被
        // `COGNEVA_WORKSPACE_ROOT` 挪走，备份要跟着挪，不能自己拼一个目录名。
        let user = cog_gateway::ide::workspace_root(data.path())
            .join("6f1c2a44-0000-0000-0000-000000000001");
        std::fs::create_dir_all(&user).unwrap();
        std::fs::write(user.join("draft.md"), b"hi").unwrap();

        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("app", data.path(), staging.path())
            .await
            .unwrap();
        let workspace = report.workspace.expect(
            "备份没取到编辑器正在用的那棵树：它按另一个目录找，而那边 is_dir() 为假时不报错，\
             包照样生成、清单里也没有这一项",
        );
        assert_eq!(workspace.files, 1, "包里的工作区文件数与源树不符");
        assert!(
            staging
                .path()
                .join("artifacts/app")
                .join(cog_gateway::ide::WORKSPACE_SUBDIR)
                .join("6f1c2a44-0000-0000-0000-000000000001/draft.md")
                .is_file(),
            "包的产物层里没有那份草稿"
        );
    }

    /// 恢复侧要真的把产物层写回去：只把包放进目录不算恢复。
    #[tokio::test]
    async fn restore_writes_back_policies_and_decision_stats() {
        let pkg = tempfile::tempdir().unwrap();
        let source = pkg.path().join("artifacts/evolution");
        let live = tempfile::tempdir().unwrap();
        let data = live.path().join("data");
        let store = cog_reflection::PolicyStore::new(data.join("policies"));
        store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"min_samples": 40}),
                "t",
            )
            .await
            .unwrap();
        std::fs::create_dir_all(data.join("meta_learning")).unwrap();
        std::fs::write(
            data.join("meta_learning/decision_stats.json"),
            serde_json::json!({"version": 1, "groups": [{"counts": {"a": [3, 2]}}]}).to_string(),
        )
        .unwrap();
        std::fs::write(
            data.join("policies/meta_learning.mode/000002.json.tmp"),
            "{}",
        )
        .unwrap();
        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", &data, staging.path())
            .await
            .unwrap();
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/evolution"), &source).unwrap();
        let manifest = manifest_with(vec![report]);

        // 目标根一开始是空的：写回这件事本身就是被测对象。
        let target = tempfile::tempdir().unwrap();
        let readings = restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("evolution".to_string(), target.path().to_path_buf())],
        )
        .await
        .unwrap();

        assert_eq!(readings.len(), 2, "每个库一行、每份统计一行：{readings:?}");
        let restored = cog_reflection::PolicyStore::new(target.path().join("policies"));
        assert!(restored.verify_chain("meta_learning.mode").await.unwrap());
        assert!(target
            .path()
            .join("policies/meta_learning.mode/ACTIVE")
            .exists());
        assert!(target
            .path()
            .join("meta_learning/decision_stats.json")
            .exists());
        assert!(
            !target
                .path()
                .join("policies/meta_learning.mode/000002.json.tmp")
                .exists(),
            "暂存目录不能变成正式目录里的一部分"
        );
        assert!(
            std::fs::read_dir(target.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".restore-")),
            "暂存目录用完要删掉"
        );
    }

    /// 编辑器工作区是数据根下唯一没有第二条重建路径的子树：用户在编辑器里新建的
    /// 文件只有这一份。它没有进包时，备份照样绿、恢复演练照样绿，换机后那棵树
    /// 就静默没了——所以这里从「数据根里的文件」一路测到「恢复端写回去的那一份」。
    #[tokio::test]
    async fn the_editor_workspace_travels_in_the_package_and_comes_back_whole() {
        let pkg = tempfile::tempdir().unwrap();
        let live = tempfile::tempdir().unwrap();
        let data = live.path().join("data");
        let user = data.join("workspace/6f1c2a44-0000-0000-0000-000000000001");
        std::fs::create_dir_all(user.join("notes")).unwrap();
        std::fs::write(user.join("notes/todo.md"), "# 待办\n\n- 一件\n").unwrap();
        std::fs::write(user.join("scratch.txt"), "草稿").unwrap();

        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("app", &data, staging.path())
            .await
            .unwrap();
        let workspace = report
            .workspace
            .as_ref()
            .expect("数据根里有 workspace/ 时清单必须声明它");
        assert_eq!(workspace.files, 2);
        assert_eq!(
            workspace.bytes,
            "# 待办\n\n- 一件\n".len() as u64 + "草稿".len() as u64
        );

        let source = pkg.path().join("artifacts/app");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/app"), &source).unwrap();
        let manifest = manifest_with(vec![report]);

        let target = tempfile::tempdir().unwrap();
        let readings = restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("app".to_string(), target.path().to_path_buf())],
        )
        .await
        .unwrap();

        assert!(
            readings.iter().any(|line| line.contains("工作区")),
            "恢复读数里要点出工作区这一份：{readings:?}"
        );
        assert_eq!(
            std::fs::read_to_string(
                target
                    .path()
                    .join("workspace/6f1c2a44-0000-0000-0000-000000000001/notes/todo.md")
            )
            .unwrap(),
            "# 待办\n\n- 一件\n"
        );
        assert_eq!(
            std::fs::read_to_string(
                target
                    .path()
                    .join("workspace/6f1c2a44-0000-0000-0000-000000000001/scratch.txt")
            )
            .unwrap(),
            "草稿"
        );
    }

    /// 清单说这个根有工作区、包里却没有那一棵树：恢复必须停，而不是写回一个空
    /// 目录——空目录与「用户什么都没写」在盘上长得一模一样。
    #[tokio::test]
    async fn a_package_that_declares_a_workspace_without_the_tree_is_refused() {
        let pkg = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pkg.path().join("artifacts/app")).unwrap();
        let manifest = manifest_with(vec![ArtifactRootReport {
            root: "app".to_string(),
            data_dir: "/var/lib/cogneva-data".to_string(),
            workspace: Some(WorkspaceReport { files: 1, bytes: 4 }),
            ..Default::default()
        }]);
        let target = tempfile::tempdir().unwrap();
        let err = restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("app".to_string(), target.path().to_path_buf())],
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("workspace"),
            "错误里要点出缺的是哪一段：{err}"
        );
    }

    /// 写回成功与「新进程加载了这一份」不是同一件事：恢复端要把包里的版本与证据
    /// 规模留在数据根里，新进程开机时才有可对的标准。
    #[tokio::test]
    async fn restore_leaves_the_new_process_something_to_check_against() {
        let pkg = tempfile::tempdir().unwrap();
        let source = pkg.path().join("artifacts/evolution");
        let live = tempfile::tempdir().unwrap();
        let data = live.path().join("data");
        let store = cog_reflection::PolicyStore::new(data.join("policies"));
        store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"min_samples": 40}),
                "t",
            )
            .await
            .unwrap();
        std::fs::create_dir_all(data.join("meta_learning")).unwrap();
        std::fs::write(
            data.join("meta_learning/decision_stats.json"),
            serde_json::json!({"version": 1, "groups": [{"counts": {"a": [3, 2]}}]}).to_string(),
        )
        .unwrap();
        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", &data, staging.path())
            .await
            .unwrap();
        let declared_store = report.policy_stores[0].clone();
        let declared_trials = report.decision_stats.as_ref().unwrap().trials;
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/evolution"), &source).unwrap();
        let manifest = manifest_with(vec![report]);

        let target = tempfile::tempdir().unwrap();
        restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("evolution".to_string(), target.path().to_path_buf())],
        )
        .await
        .unwrap();

        let path = cog_reflection::RestoreExpectation::path_in(target.path());
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("恢复端没有留下交接单 {}：{e}", path.display()));
        let expectation: cog_reflection::RestoreExpectation = serde_json::from_slice(&raw).unwrap();
        assert_eq!(expectation.package_created_at, manifest.created_at);
        assert_eq!(expectation.package_revision, manifest.revision);
        assert_eq!(expectation.policy_stores.len(), 1);
        assert_eq!(expectation.policy_stores[0].name, declared_store.name);
        assert_eq!(
            expectation.policy_stores[0].active_version, declared_store.active_version,
            "单子上的版本必须就是包里声明的那个，不能另算一个"
        );
        assert_eq!(
            expectation.decision_trials,
            Some(declared_trials),
            "证据规模也要留下，否则「少了」这件事在进程侧没有标准"
        );
    }

    /// 包里这一根什么都没带：没有可对的对象，不该留一张空单子让新进程去核对。
    #[tokio::test]
    async fn a_root_that_carries_nothing_leaves_no_hand_over() {
        let pkg = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pkg.path().join("artifacts/app")).unwrap();
        let manifest = manifest_with(vec![ArtifactRootReport {
            root: "app".into(),
            data_dir: "/var/lib/cogneva".into(),
            policies_present: false,
            policy_stores: Vec::new(),
            decision_stats: None,
            workspace: None,
        }]);
        let target = tempfile::tempdir().unwrap();
        restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("app".to_string(), target.path().to_path_buf())],
        )
        .await
        .unwrap();
        assert!(!cog_reflection::RestoreExpectation::path_in(target.path()).exists());
    }

    /// 演练的往返面：包里的统计写回一个空根之后，写者还得能接着往里记，且原有
    /// 历史原样留着。只验「包里那份读得回」的演练，对这件事是瞎的。
    #[tokio::test]
    async fn decision_roundtrip_writes_back_a_tree_the_engine_keeps_using() {
        let pkg = tempfile::tempdir().unwrap();
        let source = pkg.path().join("artifacts/evolution");
        let data = tempfile::tempdir().unwrap();
        let store = cog_reflection::PolicyStore::new(data.path().join("policies"));
        store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"min_samples": 40}),
                "t",
            )
            .await
            .unwrap();
        std::fs::create_dir_all(data.path().join("meta_learning")).unwrap();
        std::fs::write(
            data.path().join("meta_learning/decision_stats.json"),
            serde_json::json!({
                "version": 1,
                "groups": [{"category": "pge_mode", "key": "bugfix", "counts": {"pipeline": [7, 5]}}]
            })
            .to_string(),
        )
        .unwrap();
        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", data.path(), staging.path())
            .await
            .unwrap();
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/evolution"), &source).unwrap();
        let manifest = manifest_with(vec![report]);

        let line = decision_roundtrip(pkg.path(), &manifest).await.unwrap();
        assert!(line.contains("decision_roundtrip=ok"), "{line}");
        assert!(
            line.contains("历史试验数=7"),
            "读数要带上原有的试验数，否则「原样留住」无从核对：{line}"
        );
    }

    /// 反例：读得回但引擎不认识的统计（少了 category/key 的老形状）。按「组数与
    /// 试验数」数得过去，可写者拿到它只能从空开始——往返必须判红。
    #[tokio::test]
    async fn decision_roundtrip_refuses_stats_the_engine_cannot_load() {
        let pkg = tempfile::tempdir().unwrap();
        let source = pkg.path().join("artifacts/evolution");
        let data = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(data.path().join("meta_learning")).unwrap();
        std::fs::write(
            data.path().join("meta_learning/decision_stats.json"),
            serde_json::json!({"version": 1, "groups": [{"counts": {"a": [3, 2]}}]}).to_string(),
        )
        .unwrap();
        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", data.path(), staging.path())
            .await
            .unwrap();
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/evolution"), &source).unwrap();
        let manifest = manifest_with(vec![report]);

        let err = decision_roundtrip(pkg.path(), &manifest).await.unwrap_err();
        assert!(
            err.to_string().contains("原有历史没有原样留住"),
            "要把「写者读不回来」点出来：{err}"
        );
    }

    /// 包里有清单没声明的库：不许静默写进去。没有声明就没有可比的对象，
    /// 写进去等于让恢复端替备份端补一条它从没做过的判断。
    #[tokio::test]
    async fn a_store_the_manifest_does_not_declare_blocks_the_restore() {
        let pkg = tempfile::tempdir().unwrap();
        let source = pkg.path().join("artifacts/evolution");
        let source_data = tempfile::tempdir().unwrap();
        let store = cog_reflection::PolicyStore::new(source_data.path().join("policies"));
        store
            .save_new_version("meta_learning.mode", serde_json::json!({"n": 1}), "t")
            .await
            .unwrap();
        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", source_data.path(), staging.path())
            .await
            .unwrap();
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/evolution"), &source).unwrap();
        // 清单之后又冒出来一个库：备份端没读过它，恢复端不能替它签字。
        std::fs::create_dir_all(source.join("policies/surprise.mode")).unwrap();

        let target = tempfile::tempdir().unwrap();
        let err = restore_artifacts_into(
            pkg.path(),
            &manifest_with(vec![report]),
            &[("evolution".to_string(), target.path().to_path_buf())],
        )
        .await
        .expect_err("多出来的库没有声明");
        assert!(err.to_string().contains("策略库"), "{err}");
    }

    /// 包里的 ACTIVE 指不到版本时，恢复必须失败，且**不能**把现网那份覆盖掉。
    #[tokio::test]
    async fn a_broken_package_leaves_the_live_store_untouched() {
        let pkg = tempfile::tempdir().unwrap();
        let source = pkg.path().join("artifacts/evolution");
        let source_data = tempfile::tempdir().unwrap();
        let store = cog_reflection::PolicyStore::new(source_data.path().join("policies"));
        store
            .save_new_version("meta_learning.mode", serde_json::json!({"n": 1}), "t")
            .await
            .unwrap();
        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", source_data.path(), staging.path())
            .await
            .unwrap();
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::rename(staging.path().join("artifacts/evolution"), &source).unwrap();

        // 现场：包已经解开了，但 ACTIVE 指向一个没有的版本。
        std::fs::write(source.join("policies/meta_learning.mode/ACTIVE"), "9").unwrap();

        let manifest = manifest_with(vec![report]);

        let target = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(target.path().join("policies/meta_learning.mode")).unwrap();
        std::fs::write(
            target.path().join("policies/meta_learning.mode/ACTIVE"),
            "keep me",
        )
        .unwrap();

        let err = restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("evolution".to_string(), target.path().to_path_buf())],
        )
        .await
        .expect_err("ACTIVE 指向不存在的版本不能恢复成功");
        assert!(
            err.to_string().contains("ACTIVE"),
            "要点名缺的是 ACTIVE：{err}"
        );
        assert_eq!(
            std::fs::read_to_string(target.path().join("policies/meta_learning.mode/ACTIVE"))
                .unwrap(),
            "keep me",
            "验不过的包不许动现网那一份"
        );
    }

    /// 包里有一个根、恢复端给不出目标路径：整次失败，不静默少写一个根。
    #[tokio::test]
    async fn a_packaged_root_without_a_target_path_fails_the_restore() {
        let pkg = tempfile::tempdir().unwrap();
        let manifest = manifest_with(vec![ArtifactRootReport {
            root: "evolution".into(),
            data_dir: "/var/lib/cogneva-evolution-data".into(),
            policies_present: false,
            policy_stores: Vec::new(),
            decision_stats: None,
            workspace: None,
        }]);
        let err = restore_artifacts_into(
            pkg.path(),
            &manifest,
            &[("app".to_string(), pkg.path().join("data"))],
        )
        .await
        .expect_err("包里的根必须有目标路径");
        assert!(err.to_string().contains("COGNEVA_RESTORE_EXTRA_DATA_ROOTS"));
    }

    /// 包只收持久件：版本 JSON 与 ACTIVE，写到一半的 `.tmp` 不进包；读回的数
    /// 要与文件一致，否则演练没有可比的对象。
    #[tokio::test]
    async fn artifacts_carry_versions_and_decision_stats_but_not_tmp_files() {
        let root = tempfile::tempdir().unwrap();
        let data = root.path().join("data");
        let store = data.join("policies").join("meta_learning.mode");
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(
            store.join("000001.json"),
            serde_json::json!({
                "name": "meta_learning.mode",
                "version": 1,
                "payload": {"min_samples": 40},
                "parent_hash": "genesis",
                "hash": "abc",
                "created_at": "2026-10-19T00:00:00Z",
                "reason": "test"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(store.join("ACTIVE"), "1").unwrap();
        std::fs::write(store.join("000002.json.tmp"), "{}").unwrap();
        std::fs::create_dir_all(data.join("meta_learning")).unwrap();
        std::fs::write(
            data.join("meta_learning/decision_stats.json"),
            serde_json::json!({"version": 1, "groups": [{"counts": {"a": [3, 2]}}]}).to_string(),
        )
        .unwrap();

        let staging = tempfile::tempdir().unwrap();
        let report = collect_artifact_root("evolution", &data, staging.path())
            .await
            .unwrap();
        assert_eq!(report.root, "evolution");
        assert!(report.policies_present);
        assert_eq!(report.policy_stores.len(), 1);
        let policies = &report.policy_stores[0];
        assert_eq!(policies.versions, 1);
        assert_eq!(policies.head_version, 1);
        assert_eq!(policies.head_hash, "abc");
        assert_eq!(policies.active_version, 1);
        assert!(policies.problem.is_none());
        let stats = report.decision_stats.as_ref().unwrap();
        assert_eq!(stats.groups, 1);
        assert_eq!(stats.trials, 3);

        let copied = staging
            .path()
            .join("artifacts/evolution/policies/meta_learning.mode");
        assert!(copied.join("000001.json").exists());
        assert!(copied.join("ACTIVE").exists());
        assert!(
            !copied.join("000002.json.tmp").exists(),
            "写到一半的临时文件不是版本"
        );
    }

    /// 演练读回：链、head、active 都要与清单对上；删掉 ACTIVE 必须翻红，
    /// 而不是「文件都解开了所以算恢复」。
    #[tokio::test]
    async fn artifact_readback_fails_when_active_disappears() {
        let root = tempfile::tempdir().unwrap();
        let policies = root.path().join("policies");
        let store = cog_reflection::PolicyStore::new(&policies);
        store
            .save_new_version(
                "meta_learning.mode",
                serde_json::json!({"min_samples": 40}),
                "t",
            )
            .await
            .unwrap();
        let report = describe_policy_stores(&policies).await.unwrap();
        let reported = &report[0];
        assert!(verify_one_store(&store, "evolution", reported)
            .await
            .is_ok());

        std::fs::remove_file(policies.join("meta_learning.mode").join("ACTIVE")).unwrap();
        let err = verify_one_store(&store, "evolution", reported)
            .await
            .expect_err("ACTIVE 没了不能读成恢复成功");
        assert!(err.contains("ACTIVE"), "错误要点名缺的是 ACTIVE：{err}");
    }

    // ------------------------------------------------------------------
    // Decision stats exported from the table
    // ------------------------------------------------------------------

    fn table_cell(
        category: &str,
        key: &str,
        decision: &str,
        a: i64,
        s: i64,
    ) -> cog_core::DecisionStatCell {
        cog_core::DecisionStatCell {
            category: category.to_string(),
            group_key: key.to_string(),
            decision: decision.to_string(),
            attempts: a,
            successes: s,
        }
    }

    fn exported(body: &str) -> serde_json::Value {
        serde_json::from_str(body).expect("the export is JSON")
    }

    /// The file on the volume can be older than the table: the table is the
    /// author, so where both describe a cell the table's count wins.
    #[test]
    fn the_export_takes_the_tables_count_where_the_file_is_behind() {
        let file = r#"{"version":1,"groups":[{"category":"pge_mode","key":"bugfix","counts":{"pipeline":[3,2]}}]}"#;
        let body = merge_decision_stats(
            Some(file),
            &[table_cell("pge_mode", "bugfix", "pipeline", 9, 7)],
        )
        .expect("a row on either side is enough to export");
        let doc = exported(&body);
        assert_eq!(
            doc["groups"][0]["counts"]["pipeline"],
            serde_json::json!([9, 7])
        );
        assert_eq!(decision_stats_counts(&body).unwrap(), (1, 9));
    }

    /// The other direction matters just as much: evidence the table has not
    /// been raised to yet (recorded while the database was down) must not be
    /// dropped by an export that only looked at the table.
    #[test]
    fn the_export_keeps_a_file_count_the_table_has_not_caught_up_to() {
        let file = r#"{"version":1,"groups":[{"category":"pge_mode","key":"bugfix","counts":{"pipeline":[12,9],"roundtable":[4,1]}}]}"#;
        let body = merge_decision_stats(
            Some(file),
            &[table_cell("pge_mode", "bugfix", "pipeline", 5, 4)],
        )
        .expect("the file alone is enough");
        let doc = exported(&body);
        assert_eq!(
            doc["groups"][0]["counts"]["pipeline"],
            serde_json::json!([12, 9])
        );
        assert_eq!(
            doc["groups"][0]["counts"]["roundtable"],
            serde_json::json!([4, 1])
        );
        assert_eq!(decision_stats_counts(&body).unwrap(), (1, 16));
    }

    /// A group only the table knows has to appear, and the export stays sorted
    /// so that the same evidence always produces the same bytes.
    #[test]
    fn the_export_adds_groups_from_both_sides_deterministically() {
        let file = r#"{"version":1,"groups":[{"category":"pge_mode","key":"feature","counts":{"pipeline":[2,1]}}]}"#;
        let cells = [
            table_cell("pge_mode", "bugfix", "pipeline", 6, 5),
            table_cell("pge_mode", "feature", "pipeline", 1, 0),
        ];
        let first = merge_decision_stats(Some(file), &cells).expect("both sides have evidence");
        let second = merge_decision_stats(Some(file), &cells).expect("same input");
        assert_eq!(first, second, "the export must not depend on hash order");
        let doc = exported(&first);
        assert_eq!(doc["groups"].as_array().unwrap().len(), 2);
        assert_eq!(decision_stats_counts(&first).unwrap(), (2, 8));
    }

    /// Nothing on either side is not an empty export: the package leaves the
    /// file out and the manifest records no decision-stats reading, rather than
    /// claiming a section that has nothing in it.
    #[test]
    fn nothing_anywhere_exports_nothing() {
        assert!(merge_decision_stats(None, &[]).is_none());
        assert!(merge_decision_stats(Some("not json"), &[]).is_none());
        assert!(merge_decision_stats(Some(r#"{"version":1,"groups":[]}"#), &[]).is_none());
    }

    /// A single unreadable cell is skipped, not fatal: one bad pair must not
    /// cost the whole export the evidence that is still readable.
    #[test]
    fn a_broken_cell_does_not_take_the_rest_of_the_export_with_it() {
        let file = r#"{"version":1,"groups":[{"category":"pge_mode","key":"bugfix","counts":{"pipeline":[4,3],"broken":"nope"}}]}"#;
        let body = merge_decision_stats(Some(file), &[]).expect("the readable cell still exports");
        let doc = exported(&body);
        assert_eq!(
            doc["groups"][0]["counts"]["pipeline"],
            serde_json::json!([4, 3])
        );
        assert!(doc["groups"][0]["counts"].get("broken").is_none());
    }
}

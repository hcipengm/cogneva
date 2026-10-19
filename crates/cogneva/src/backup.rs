//! `cogneva backup` / `cogneva restore`：把权威数据面打成可搬运备份包。
//!
//! 权威面 = 关系数据（逻辑导出）+ 向量索引（服务端一致性快照）+ 对象 raw 层
//! （文件级复制）+ 实例身份（Secret 打成 apply-able manifest）。其余状态
//! （NATS/Redis/Meilisearch/registry/进化克隆）全部可重建，不进包。
//!
//! 包布局：
//! ```text
//! <pkg>/manifest.json                 版本/rev/实例指纹/时间/各段内容清单
//! <pkg>/secrets/cogneva-secrets.yaml      apply-able Secret（data 为 base64）
//! <pkg>/secrets/cogneva-secrets-oob.yaml  同上，带外凭证那一份（只被网关读）
//! <pkg>/pg/public.<table>.csv         COPY ... TO STDOUT CSV（分区父表含全分区）
//! <pkg>/pg/sequences.sql              全部序列的 setval 语句
//! <pkg>/qdrant/<collection>.snapshot  服务端一致性快照
//! <pkg>/objects/storage/...           cogneva-data-pvc 的 storage 子树原样
//! <pkg>/artifacts/<root>/policies/... 每个数据根的产物级进化策略库
//! <pkg>/artifacts/<root>/meta_learning/decision_stats.json  调参证据
//! ```
//!
//! `artifacts/` 只收 `policies/` 与 `meta_learning/decision_stats.json`，不收数据根
//! 的其余子树（工作树、数仓、原始日志）：那些要么可重建，要么体积以百 GiB 计，
//! 且不属于「换机后必须逐字节相同」的权威面。
//! 整包打成 `cogneva-backup-<UTC 时间戳>.tar.zst`，时间戳字典序即新旧序。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use cog_core::{
    BackupDrillRecord, BackupOutcome, BackupRunClosing, BackupRunOpening, BackupRunStore,
};
use cog_storage::{PostgresBackupRunStore, PostgresDecisionStatsStore};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// 包名前缀与时间戳格式：字典序排序即时间序，保留策略依赖这一点。
pub const PACKAGE_PREFIX: &str = "cogneva-backup-";
pub const PACKAGE_SUFFIX: &str = ".tar.zst";
const TIMESTAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

/// 落库的错误文本上限。错误本身通常只有一行，这个界是为了让一条失控的长错误
/// （整段响应体、整张表名清单）不至于把行撑成不可读的东西。
const ERROR_TEXT_MAX_CHARS: usize = 2000;

/// 一次运行默认的期限，也是集群清单里那个 `activeDeadlineSeconds` 的默认值。
///
/// 实测一次运行 41 秒（含导出、快照、打包、裁剪），一小时的界是它的一百多倍：
/// 落在里面的运行照跑，落在外面的那个不是慢，是卡死了——而它卡着的时候，
/// 调度器的 `Forbid` 会让后面每一次都不再开始。没有这个界，一次挂住的运行
/// 就能把备份永久停掉，顺带把「谁来给它收尾」也一并停掉。
const DEFAULT_RUN_DEADLINE_SECONDS: i32 = 3600;

/// 内部实例密钥对象名：业务 Pod 读的那一份（数据库/缓存/内部签名/S3/邮件）。
pub const SECRET_NAME_INTERNAL: &str = "cogneva-secrets";
/// 带外凭证对象名：只被安全网关读的那一份（LLM 上游、平台 token、git 身份私钥…）。
/// 两片是权限边界，不是分类：内部那份是整对象 get，谁持它就读得到里面每一个键。
pub const SECRET_NAME_OOB: &str = "cogneva-secrets-oob";

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
    /// cogneva-data-pvc 挂载根；对象子树取其下 `storage/`。
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
    /// 它不在数据根下，是一个独立的卷：`data-root-subdirs.tsv` 里 `git-remote` 这一
    /// 行的权威面就是它。已推的 rev 能从公开远端重取，未推的不能——那些才是进化
    /// 自己产出的状态，必须逐字进入包。
    pub git_remote_dir: Option<PathBuf>,
    pub namespace: String,
}

impl BackupConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let database_url = std::env::var("COGNEVA_DATABASE_URL")
            .map_err(|_| "COGNEVA_DATABASE_URL is required for backup")?;
        let qdrant_http_url = std::env::var("COGNEVA_QDRANT_HTTP_URL")
            .map_err(|_| "COGNEVA_QDRANT_HTTP_URL is required for backup")?;
        Ok(Self {
            database_url,
            qdrant_http_url,
            backup_dir: PathBuf::from(
                std::env::var("COGNEVA_BACKUP_DIR").unwrap_or_else(|_| "/backups".into()),
            ),
            secrets_dir: PathBuf::from(
                std::env::var("COGNEVA_SECRETS_DIR").unwrap_or_else(|_| "/secrets".into()),
            ),
            oob_secrets_dir: PathBuf::from(
                std::env::var("COGNEVA_OOB_SECRETS_DIR").unwrap_or_else(|_| "/secrets-oob".into()),
            ),
            data_dir: PathBuf::from(
                std::env::var("COGNEVA_DATA_DIR")
                    .unwrap_or_else(|_| "/var/lib/cogneva-data".into()),
            ),
            extra_data_roots: parse_extra_data_roots(
                "COGNEVA_BACKUP_EXTRA_DATA_ROOTS",
                std::env::var("COGNEVA_BACKUP_EXTRA_DATA_ROOTS").ok(),
            )?,
            keep_count: std::env::var("COGNEVA_BACKUP_KEEP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(7),
            run_deadline_seconds: std::env::var("COGNEVA_BACKUP_RUN_DEADLINE_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_RUN_DEADLINE_SECONDS),
            offsite_dir: std::env::var("COGNEVA_BACKUP_OFFSITE_DIR")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            git_remote_dir: std::env::var("COGNEVA_BACKUP_GIT_REMOTE_DIR")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            namespace: std::env::var("COGNEVA_NAMESPACE").unwrap_or_else(|_| "cogneva".into()),
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
/// 备份与恢复各读一个 env（`COGNEVA_BACKUP_EXTRA_DATA_ROOTS` /
/// `COGNEVA_RESTORE_EXTRA_DATA_ROOTS`），形状同一条：恢复端面对的是一个包里已经
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

/// 包里的列中，目标表接受写入的那些：生成列由目标自己算，不在这份名单里。
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
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{table}\""))
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
    std::fs::write(out_dir.join("sequences.sql"), &sql)?;
    report.sequences = sequences.len();
    // 出错时 tx 在 drop 时回滚；成功路径显式提交，把快照释放掉。
    tx.commit().await?;
    Ok(report)
}

/// 恢复做了什么。
///
/// `tables` 是装回去的表；`recomputed` 是包里有值、装载时却由目标自己算回来的
/// 列，记作 `表.列`。后者不进成败判决——值由同一行的其它列算出，不搬不丢数据
/// ——但必须被写下来：生成表达式一旦改动，旧包会被装成「当时不曾有过」的值，
/// 而表数、行数、源库比对三项全都照旧是全绿，这一格是唯一会露出来的地方。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreReport {
    pub tables: Vec<String>,
    pub recomputed: Vec<String>,
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

    let seq_path = pkg_pg_dir.join("sequences.sql");
    if seq_path.exists() {
        let sql = std::fs::read_to_string(seq_path)?;
        for line in sql.lines().filter(|l| !l.trim().is_empty()) {
            sqlx::query(line).execute(pool).await?;
        }
    }
    Ok(RestoreReport { tables, recomputed })
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

fn tar_package(staging: &Path, staging_name: &str, out: &Path) -> std::io::Result<()> {
    let tmp = out.with_extension("tmp");
    {
        let file = std::fs::File::create(&tmp)?;
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
    std::fs::rename(&tmp, out)
}

fn untar_package(pkg: &Path, dest: &Path) -> std::io::Result<PathBuf> {
    let file = std::fs::File::open(pkg)?;
    let decoder = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(dest)?;
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

pub async fn run_backup(cfg: &BackupConfig) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&cfg.backup_dir)?;
    let now = Utc::now();
    let staging_name = package_name(now)
        .trim_end_matches(PACKAGE_SUFFIX)
        .to_string();
    let staging = cfg.backup_dir.join(&staging_name);
    std::fs::create_dir_all(&staging)?;

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

/// 真正跑一次备份并收尾：导出、写清单、打包、清 staging、裁保留。
async fn finish_backup(
    cfg: &BackupConfig,
    now: chrono::DateTime<Utc>,
    staging: &Path,
    staging_name: &str,
) -> Result<LeftBehind, Box<dyn std::error::Error>> {
    match run_backup_into(cfg, staging).await {
        Ok(manifest) => {
            let manifest_json = serde_json::to_string_pretty(&manifest)?;
            std::fs::write(staging.join("manifest.json"), format!("{manifest_json}\n"))?;
            let package = cfg.backup_dir.join(package_name(now));
            tar_package(staging, staging_name, &package)?;
            std::fs::remove_dir_all(staging)?;
            let deleted = enforce_retention(&cfg.backup_dir, cfg.keep_count);
            if let Some(dir) = &cfg.offsite_dir {
                copy_offsite(dir, &package, cfg.keep_count);
            }
            let (kept_after, oldest_kept_at) = retained_state(&cfg.backup_dir);
            tracing::info!(
                package = %package.display(),
                kept = cfg.keep_count,
                pruned = deleted.len(),
                "backup package complete"
            );
            let bytes = std::fs::metadata(&package).ok().map(|m| m.len() as i64);
            Ok(LeftBehind {
                package,
                bytes,
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
) -> Result<BackupManifest, Box<dyn std::error::Error>> {
    let secrets_dir = staging.join("secrets");
    std::fs::create_dir_all(&secrets_dir)?;
    // 两片各出一份 apply-able manifest。键名带上对象前缀：只列裸键名的话，
    // 包清单读起来像"这些键都在一个对象里"，而恢复端是按对象 apply 的——
    // 那句误读会在换机时才暴露。
    let mut keys = Vec::new();
    for (dir, object_name) in [
        (&cfg.secrets_dir, SECRET_NAME_INTERNAL),
        (&cfg.oob_secrets_dir, SECRET_NAME_OOB),
    ] {
        let (yaml, object_keys) = match build_secret_manifest(dir, &cfg.namespace, object_name) {
            Ok(v) => v,
            // 带外那份可能还没被创建过（全新安装、向导还没跑）：那不是备份失败，
            // 包里少一份空对象而已。内部那份缺失是真问题（含实例指纹）。
            Err(e) if object_name == SECRET_NAME_OOB => {
                tracing::warn!(dir = %dir.display(), error = %e, "备份：带外凭证对象读不到，跳过该片");
                continue;
            }
            Err(e) => return Err(e),
        };
        std::fs::write(secrets_dir.join(format!("{object_name}.yaml")), yaml)?;
        keys.extend(
            object_keys
                .into_iter()
                .map(|k| format!("{object_name}/{k}")),
        );
    }

    // 每一段取完就盖一个时刻。写在每段之后而不是统一在开头取：要量的是「这段
    // 数据是什么时候被取走的」，不是「这一跑什么时候开始」。
    let mut segments = Vec::new();
    let pool = PgPool::connect(&cfg.database_url).await?;
    let postgres = backup_postgres(&pool, &staging.join("pg")).await?;
    segments.push(segment_stamp("postgres"));

    let qdrant = QdrantHttp::new(&cfg.qdrant_http_url);
    let qdrant_dir = staging.join("qdrant");
    std::fs::create_dir_all(&qdrant_dir)?;
    let mut collections = Vec::new();
    for name in qdrant.collection_names().await? {
        collections.push(qdrant.snapshot_collection(&name, &qdrant_dir).await?);
    }
    segments.push(segment_stamp("qdrant"));

    let (files, bytes) = copy_tree(
        &cfg.data_dir.join("storage"),
        &staging.join("objects").join("storage"),
    )?;
    segments.push(segment_stamp("objects"));
    let artifacts = collect_artifacts(cfg, staging, Some(&pool)).await?;
    segments.push(segment_stamp("artifacts"));

    // 进化 bare 仓：它不在数据根里，是独立的一卷，所以单独打成 git bundle。
    // 打包失败不让整次备份失败——没有 bundle 的包仍然救得回其余全部状态，而
    // 这一格的损失会在 manifest 的 `lossy_surfaces` 里被点名。相反，成功就必须
    // 在 manifest 里留下读数，恢复端才知道该把哪一份种回去。
    let evolution_bundle = match bundle_evolution(
        cfg.git_remote_dir.as_deref(),
        &staging.join("git"),
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
        .or_else(|| std::env::var("COGNEVA_INSTANCE_FINGERPRINT").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());

    Ok(BackupManifest {
        version: env!("CARGO_PKG_VERSION").to_string(),
        revision: env!("COGNEVA_GIT_REVISION").to_string(),
        instance_fingerprint: fingerprint,
        namespace: cfg.namespace.clone(),
        created_at: now_rfc3339(),
        secrets_keys: keys,
        postgres,
        qdrant: collections,
        objects: ObjectsReport { files, bytes },
        artifacts,
        image: std::env::var("COGNEVA_IMAGE_REF").unwrap_or_default(),
        schema_version: current_schema_version(),
        lossy_surfaces: lossy_surfaces(evolution_bundle.is_some()),
        segments,
        max_segment_skew_seconds: bound,
        evolution_bundle,
    })
}

/// 本二进制带的最新 PG 迁移号。读不出来记 0（没声明），不让备份失败：
/// 迁移目录缺席是打包问题，应当由别处的门去红，不是让这一晚没有备份。
fn current_schema_version() -> i64 {
    let app_dir = std::env::var("COGNEVA_APP_DIR").unwrap_or_else(|_| "/opt/cogneva".into());
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
fn lossy_surfaces(bundle_present: bool) -> Vec<LossySurface> {
    let mut surfaces: Vec<LossySurface> = [
        (
            "nats",
            "在途事件与 JetStream 里尚未被消费的消息",
            "事件是通知面，权威状态在 PG 与对象层；消费者重连后从权威状态重建",
        ),
        (
            "redis",
            "缓存、租约与去重计数",
            "全部是可重建的派生状态；租约在重启后按既有规则重新竞争",
        ),
        (
            "meilisearch",
            "搜索索引",
            "索引由权威数据重建，恢复后按索引任务重跑",
        ),
        (
            "cluster-registry",
            "containerd 里已拉取的镜像层",
            "镜像是构建产物，不是权威面；由源码 rev 重建或从 release 重新拉取",
        ),
        (
            "pending-changes",
            "还没投递出去的已生成变更（数据根下 pending-changes/）",
            "已投递的变更以 change_ledger 与 git 上的提交为准；没投递的那次生成换机后重跑，代价是一次生成",
        ),
    ]
    .into_iter()
    .map(|(name, lost, why)| LossySurface {
        name: name.to_string(),
        lost: lost.to_string(),
        acceptable_because: why.to_string(),
    })
    .collect();
    if !bundle_present {
        surfaces.insert(
            4,
            LossySurface {
                name: "evolution-bare-repo".to_string(),
                lost: "未推送到远端的进化 rev（这个包没有带 git bundle）".to_string(),
                acceptable_because:
                    "已推的 rev 可从公开远端重取；未推的那部分确实是损失——出包时仓库还没 seed \
                     或 bundle 没打成，出包日志里有那一行告警"
                        .to_string(),
            },
        );
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
    let bundle = out_dir.join("evolution.bundle");
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
/// 只抄 `policies/` 与 `meta_learning/decision_stats.json`。写点已经是
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
        .join("artifacts")
        .join("app")
        .join("meta_learning")
        .join("decision_stats.json");
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

    let policies_src = dir.join("policies");
    if policies_src.is_dir() {
        report.policies_present = true;
        let dst = staging.join("artifacts").join(root).join("policies");
        copy_policy_tree(&policies_src, &dst)?;
        report.policy_stores = describe_policy_stores(&policies_src).await?;
    }

    let stats_src = dir.join("meta_learning").join("decision_stats.json");
    if stats_src.is_file() {
        let dst = staging.join("artifacts").join(root).join("meta_learning");
        std::fs::create_dir_all(&dst)?;
        std::fs::copy(&stats_src, dst.join("decision_stats.json"))?;
        let bytes = std::fs::metadata(&stats_src)?.len();
        let text = std::fs::read_to_string(&stats_src)?;
        let (groups, trials) = decision_stats_counts(&text)?;
        report.decision_stats = Some(DecisionStatsReport {
            groups,
            trials,
            bytes,
        });
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
        let pg_dir = pkg_dir.join("pg");
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
        let seq_path = pg_dir.join("sequences.sql");
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
        let qdrant_dir = pkg_dir.join("qdrant");
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
        let (files, bytes) = measure_tree(&pkg_dir.join("objects").join("storage"))?;
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
        let secrets_dir = pkg_dir.join("secrets");
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
        let path = pkg_dir.join("git").join("evolution.bundle");
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

/// 恢复入口：`pkg` 可为 .tar.zst 或已解包目录。逐段恢复存在的部分，
/// 缺段记日志不判负——包可能来自只备份了部分段的旧版本。
///
/// 动手恢复之前先拿 manifest 逐段核包（`verify_package_against_manifest`）：
/// 内容与清单对不上就退非零，绝不带着分歧走到 `TRUNCATE` 那一步。
pub async fn run_restore(pkg: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("COGNEVA_DATABASE_URL")
        .map_err(|_| "COGNEVA_DATABASE_URL is required for restore")?;
    let data_dir = PathBuf::from(
        std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into()),
    );

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

    let manifest_path = pkg_dir.join("manifest.json");
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

    let pg_dir = pkg_dir.join("pg");
    if pg_dir.exists() {
        let pool = PgPool::connect(&database_url).await?;
        let report = restore_postgres(&pool, &pg_dir).await?;
        tracing::info!(
            tables = report.tables.len(),
            recomputed = report.recomputed.len(),
            "postgres restored"
        );
    }

    let qdrant_dir = pkg_dir.join("qdrant");
    if qdrant_dir.exists() {
        let qdrant_http_url = std::env::var("COGNEVA_QDRANT_HTTP_URL")
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

    let objects_src = pkg_dir.join("objects").join("storage");
    if objects_src.exists() {
        let (files, bytes) = copy_tree(&objects_src, &data_dir.join("storage"))?;
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
    tracing::info!("restore complete");
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
    let bundle = pkg_dir.join("git").join("evolution.bundle");
    let target = match std::env::var("COGNEVA_RESTORE_GIT_REMOTE_DIR") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => {
            if std::env::var("COGNEVA_RESTORE_SKIP_GIT_BUNDLE").as_deref() == Ok("1") {
                tracing::warn!(
                    bundle_bytes = report.bytes,
                    refs = report.refs,
                    "包里有进化 bare 仓的 bundle，但恢复端没有落点且被显式跳过：未推的 rev 留在包里，没有种回 /host-git"
                );
                return Ok(());
            }
            return Err("包里有进化 bare 仓的 git bundle，但恢复端没有落点：\
                 设 COGNEVA_RESTORE_GIT_REMOTE_DIR 指向 /host-git 那一卷；\
                 确认目标不需要进化时显式设 COGNEVA_RESTORE_SKIP_GIT_BUNDLE=1"
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
        env!("COGNEVA_GIT_REVISION"),
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
            current_image = %std::env::var("COGNEVA_IMAGE_REF").unwrap_or_default(),
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
        "COGNEVA_RESTORE_EXTRA_DATA_ROOTS",
        std::env::var("COGNEVA_RESTORE_EXTRA_DATA_ROOTS").ok(),
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
    let dir = target.join("meta_learning");
    std::fs::create_dir_all(&dir)?;
    let path = cog_reflection::RestoreExpectation::path_in(target);
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
    let base = pkg_dir.join("artifacts").join(&root.root);
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
        let src = base.join("policies");
        if !src.is_dir() {
            return Err(format!("{} 不在包里，但清单说这个根有策略库", src.display()).into());
        }
        let staged = scratch.join("policies");
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
        replace_dir(&staged, &target.join("policies"))?;
    }

    if let Some(stats) = &root.decision_stats {
        let src = base.join("meta_learning").join("decision_stats.json");
        let staged = scratch.join("decision_stats.json");
        std::fs::copy(&src, &staged)?;
        std::fs::File::open(&staged)?.sync_all()?;
        let line = verify_one_stats(&staged, &root.root, stats)
            .map_err(|e| format!("{}/meta_learning/decision_stats.json: {e}", root.root))?;
        readings.push(line);
        let dir = target.join("meta_learning");
        std::fs::create_dir_all(&dir)?;
        std::fs::rename(&staged, dir.join("decision_stats.json"))?;
        std::fs::File::open(&dir)?.sync_all()?;
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

/// 演练配置。只要库、卷与迁移目录。
///
/// 不复用 [`BackupConfig`]：它要求 qdrant 端点，而演练不碰向量与对象层——那两段
/// 要真恢复就得有一个空白目标环境，这一趟要证明的是「包里的关系数据与它自己的账
/// 能装回一个跑完迁移的空库」。让演练 Job 为此背上一个 qdrant 地址，是把一个能
/// 跑的进程挡在一个它用不上的依赖后面。
pub struct DrillConfig {
    pub database_url: String,
    pub backup_dir: PathBuf,
    /// 备份卷之外的第二个落点，与写者读同一个 env。`None` = 这个安装没有第二
    /// 个落点，演练会把这件事实记成空列；读那一面的规则由「从来没有过一份」来判。
    pub offsite_dir: Option<PathBuf>,
    /// 迁移目录：演练要在一个空库上把表结构建出来，用的必须是**这个镜像**里的
    /// 迁移，而不是别处的。镜像把迁移装在 `/opt/cogneva/crates/cog-storage/migrations`。
    pub migrations_dir: PathBuf,
}

impl DrillConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let database_url = std::env::var("COGNEVA_DATABASE_URL")
            .map_err(|_| "COGNEVA_DATABASE_URL is required for backup-drill")?;
        let app_dir = std::env::var("COGNEVA_APP_DIR").unwrap_or_else(|_| "/opt/cogneva".into());
        Ok(Self {
            database_url,
            backup_dir: PathBuf::from(
                std::env::var("COGNEVA_BACKUP_DIR").unwrap_or_else(|_| "/backups".into()),
            ),
            offsite_dir: std::env::var("COGNEVA_BACKUP_OFFSITE_DIR")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            migrations_dir: cog_storage::migrate::migrations_dir(&app_dir),
        })
    }
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
        packages_on_volume: 0,
        oldest_package_at: None,
        offsite_newest_at: None,
        offsite_matches_newest: None,
        tables_restored: None,
        tables_expected: None,
        rows_restored: None,
        rows_expected: None,
        source_runs_matched: None,
        generated_columns_recomputed: Vec::new(),
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

    if let Err(e) = drill_once(cfg, live, packages, &mut drill).await {
        drill.error = Some(scrub_error(&e.to_string(), &cfg.database_url));
    }
    drill.finished_at = Utc::now();
    drill
}

async fn drill_once(
    cfg: &DrillConfig,
    live: &PgPool,
    packages: Vec<PathBuf>,
    drill: &mut BackupDrillRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let package = packages
        .last()
        .cloned()
        .ok_or_else(|| format!("{} 里没有任何备份包", cfg.backup_dir.display()))?;
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
    drill.package = Some(name);

    // 卷外那一份是不是**这个**包：同名、同字节数。名字相同而大小不同是复制写到
    // 一半，名字不同是它停在了更早的那一晚——两种都是「第二份不是今晚的备份」，
    // 但它们各自对应不同的修法，所以这里只回答「是不是同一份」，原因留给读的人。
    if let Some(dir) = &cfg.offsite_dir {
        drill.offsite_matches_newest = packages_in(dir).last().map(|copy| {
            copy.file_name() == package.file_name()
                && std::fs::metadata(copy).ok().map(|m| m.len() as i64) == drill.package_bytes
        });
    }

    // 解包先做：包本身坏了要在这里停，不能等到把库删了再发现。
    let extract_root = std::env::temp_dir().join(format!("cogneva-drill-{}", uuid::Uuid::new_v4()));
    let tree = TempTree(extract_root.clone());
    let pkg_dir = untar_package(&package, &extract_root)?;

    let manifest: BackupManifest =
        serde_json::from_str(&std::fs::read_to_string(pkg_dir.join("manifest.json"))?)?;
    tracing::info!(
        package = %package.display(),
        created_at = %manifest.created_at,
        revision = %manifest.revision,
        "drill: package opened"
    );
    // 清单说这个包里有什么，此刻盘上是事实。这一段覆盖的是 qdrant 与对象层：
    // 它们不进空库，能核的就是「清单声明的字节与文件数是不是真的在包里」。
    verify_package_against_manifest(&pkg_dir, &manifest)?;
    // 恢复端会拒收的包，演练不能判绿：演练要说的是「今晚这份装得回去」，而不
    // 是「包能解开」。跨段偏差是恢复端的第一道拒绝，这里先问同一句话。
    if let Err(e) = check_segment_skew(&manifest) {
        return Err(format!("恢复端会拒绝这个包：{e}").into());
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
        let base = pkg_dir.join("artifacts").join(&root.root);
        if root.policies_present {
            let store = cog_reflection::PolicyStore::new(base.join("policies"));
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
        if let Some(stats) = &root.decision_stats {
            drill.artifacts_expected += 1;
            let path = base.join("meta_learning").join("decision_stats.json");
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
    let path = scratch_root
        .join(&root.root)
        .join("meta_learning")
        .join("decision_stats.json");
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
    cog_storage::migrate::Migrator::new(&cfg.migrations_dir)
        .run(scratch_url)
        .await?;
    tracing::info!(database = DRILL_DATABASE, "drill: migrations applied");

    let scratch = PgPool::connect(scratch_url).await?;
    // 迁移只建得出迁移文件里有的表；各 store 首次启动时自己建的那批表也要先建，
    // 否则下一段的 COPY 会在第一张缺失的表上失败——那是镜像建不出自己的结构，
    // 不是包不可恢复。
    crate::schema_bundle::apply(&scratch)
        .await
        .map_err(|e| format!("drill: schema bundle failed: {e}"))?;
    tracing::info!("drill: store schema applied");
    let pg_dir = pkg_dir.join("pg");
    if !pg_dir.exists() {
        return Err("包里没有 pg 段，这次演练无表可恢复".into());
    }
    let restored = restore_postgres(&scratch, &pg_dir).await?;
    // 装回来的列清单要跟着这一行一起留在账上：一份按旧导出写的包被装回时，
    // 生成列的值不是包里的、是目标此刻算出来的。表数与行数看不见这件事。
    drill.generated_columns_recomputed = restored.recomputed.clone();
    tracing::info!(
        tables = restored.tables.len(),
        recomputed = restored.recomputed.len(),
        "drill: postgres segment restored"
    );

    // 逐表把「装回来几行」与清单声明比。清单里的表在当前 schema 上缺一张，
    // 下面的 count 会直接报错——那也是一次真实的失败：这个包装不回今天的结构。
    let mut conn = scratch.acquire().await?;
    let expected_tables = manifest.postgres.tables.len() as i32;
    let mut matched_tables = 0i32;
    let mut rows_restored = 0i64;
    let mut rows_expected = 0i64;
    let mut disagreements = Vec::new();
    for table in &manifest.postgres.tables {
        validate_table_name(&table.name)?;
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{}\"", table.name))
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
    let terminal_ids: Vec<String> = sqlx::query_scalar(
        "SELECT run_id FROM backup_runs WHERE outcome IN ('ok', 'failed') ORDER BY run_id",
    )
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
    let drill = run_drill(&cfg, &live).await;
    let store = PostgresBackupRunStore::new(live.clone());
    store.record_drill(&drill).await?;
    println!(
        "{} {}",
        drill.drill_id,
        if drill.ok() { "ok" } else { "failed" }
    );
    if let Some(error) = &drill.error {
        return Err(format!("演练没有证明这个包可恢复：{error}").into());
    }
    if !drill.ok() {
        return Err("演练的测量没有全部对上，这一行没有留下错误文本".into());
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

    #[test]
    fn package_names_sort_by_time() {
        let a = package_name(Utc.with_ymd_and_hms(2026, 9, 14, 1, 0, 0).unwrap());
        let b = package_name(Utc.with_ymd_and_hms(2026, 9, 14, 2, 0, 0).unwrap());
        assert!(a < b, "时间戳命名必须字典序可排：{a} vs {b}");
        assert!(a.starts_with(PACKAGE_PREFIX) && a.ends_with(PACKAGE_SUFFIX));
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
        tar_package(staging.path(), "cogneva-backup-20260914T000000Z", &pkg).unwrap();

        let extract = tempfile::tempdir().unwrap();
        let root = untar_package(&pkg, extract.path()).unwrap();
        assert_eq!(
            std::fs::read(root.join("pg/public.t.csv")).unwrap(),
            b"a,b\n1,2\n"
        );
        assert!(root.join("manifest.json").exists());
    }

    #[test]
    fn the_package_frame_carries_a_content_checksum() {
        let staging = tempfile::tempdir().unwrap();
        std::fs::write(staging.path().join("f"), b"x").unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let pkg = out_dir
            .path()
            .join("cogneva-backup-20260914T000000Z.tar.zst");
        tar_package(staging.path(), "cogneva-backup-20260914T000000Z", &pkg).unwrap();

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
            lossy_surfaces: lossy_surfaces(false),
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
        // 两种包都要说得清：带了 bundle 就不该再把进化仓列成损失，没带才是。
        for bundle_present in [false, true] {
            let surfaces = lossy_surfaces(bundle_present);
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

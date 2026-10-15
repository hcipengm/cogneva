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
//! ```
//! 整包打成 `cogneva-backup-<UTC 时间戳>.tar.zst`，时间戳字典序即新旧序。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::Engine;
use chrono::{SecondsFormat, Utc};
use cog_core::{BackupOutcome, BackupRunClosing, BackupRunOpening, BackupRunStore};
use cog_storage::PostgresBackupRunStore;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReport {
    pub name: String,
    pub rows: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionReport {
    pub name: String,
    pub snapshot: String,
    pub points: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresReport {
    pub tables: Vec<TableReport>,
    pub sequences: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsReport {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: String,
    pub revision: String,
    pub instance_fingerprint: String,
    pub namespace: String,
    pub created_at: String,
    pub secrets_keys: Vec<String>,
    pub postgres: PostgresReport,
    pub qdrant: Vec<CollectionReport>,
    pub objects: ObjectsReport,
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
    pub keep_count: usize,
    /// 这一次运行最长能跑多久。集群侧同一个值同时是 Job 的
    /// `activeDeadlineSeconds`（到点由 kubelet 杀容器）和这里的 env——一个值
    /// 两处承载，不许各自写一份。
    ///
    /// 它落进账本那一行的 `run_deadline_seconds`，因为那个数要跟着这一行走：
    /// 一行运行中的记录没人在跑，只有拿**它自己被开出来时**的期限才能判，用
    /// 今天的旋钮去判昨天那一次是两回事。
    pub run_deadline_seconds: i32,
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
            keep_count: std::env::var("COGNEVA_BACKUP_KEEP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(7),
            run_deadline_seconds: std::env::var("COGNEVA_BACKUP_RUN_DEADLINE_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_RUN_DEADLINE_SECONDS),
            namespace: std::env::var("COGNEVA_NAMESPACE").unwrap_or_else(|_| "cogneva".into()),
        })
    }
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

/// 运行时发现表清单：新表随迁移自动进备份，不维护手工列表。
pub async fn discover_tables(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let tables: Vec<String> = sqlx::query_scalar(DISCOVER_TABLES_SQL)
        .fetch_all(pool)
        .await?;
    Ok(tables)
}

/// PG 逻辑导出：逐表 COPY TO CSV + 全部序列的 setval 语句。
/// 运行中实例的数据目录文件级拷贝不保证一致，逻辑导出是唯一安全形态。
/// 导出一律用 COPY (SELECT *) 形式：PG 不允许 COPY <分区父表> TO，
/// 而 SELECT 形式对普通表与分区父表都成立，列序同为表定义序。
pub async fn backup_postgres(
    pool: &PgPool,
    out_dir: &Path,
) -> Result<PostgresReport, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(out_dir)?;
    let tables = discover_tables(pool).await?;
    let mut report = PostgresReport {
        tables: Vec::with_capacity(tables.len()),
        sequences: 0,
    };

    let mut conn = pool.acquire().await?;
    for table in &tables {
        validate_table_name(table)?;
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{table}\""))
            .fetch_one(&mut *conn)
            .await?;
        let path = out_dir.join(format!("public.{table}.csv"));
        let mut file = std::fs::File::create(&path)?;
        let mut copy_out = conn
            .copy_out_raw(&format!(
                "COPY (SELECT * FROM \"{table}\") TO STDOUT WITH (FORMAT csv, HEADER true)"
            ))
            .await?;
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
    .fetch_all(&mut *conn)
    .await?;
    let mut sql = String::new();
    for seq in &sequences {
        validate_table_name(seq)?;
        let (last_value, is_called): (i64, bool) =
            sqlx::query_as(&format!("SELECT last_value, is_called FROM \"{seq}\""))
                .fetch_one(&mut *conn)
                .await?;
        sql.push_str(&format!(
            "SELECT setval('{seq}', {last_value}, {is_called});\n"
        ));
    }
    std::fs::write(out_dir.join("sequences.sql"), &sql)?;
    report.sequences = sequences.len();
    Ok(report)
}

/// PG 恢复：整包表一次性 TRUNCATE ... RESTART IDENTITY CASCADE，
/// 同一事务内关掉触发器（含外键检查）逐表 COPY FROM，最后序列归位。
/// 目标必须是跑完迁移的库（fresh install 天然是）；session_replication_role
/// 需要超级用户，集群的 POSTGRES_USER 与 CI 的 postgres 都满足。
pub async fn restore_postgres(
    pool: &PgPool,
    pkg_pg_dir: &Path,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut tables = Vec::new();
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
            let mut file = std::fs::File::open(pkg_pg_dir.join(format!("public.{table}.csv")))?;
            let mut copy_in = tx
                .copy_in_raw(&format!(
                    "COPY \"{table}\" FROM STDIN WITH (FORMAT csv, HEADER true)"
                ))
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
    Ok(tables)
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
        let encoder = zstd::Encoder::new(file, 3)?;
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
    let scrubbed = url_password(&cfg.database_url)
        .map(|secret| cog_core::redact_secrets(&err.to_string()).replace(&secret, "[redacted]"))
        .unwrap_or_else(|| cog_core::redact_secrets(&err.to_string()));
    scrubbed.chars().take(ERROR_TEXT_MAX_CHARS).collect()
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

    let pool = PgPool::connect(&cfg.database_url).await?;
    let postgres = backup_postgres(&pool, &staging.join("pg")).await?;

    let qdrant = QdrantHttp::new(&cfg.qdrant_http_url);
    let qdrant_dir = staging.join("qdrant");
    std::fs::create_dir_all(&qdrant_dir)?;
    let mut collections = Vec::new();
    for name in qdrant.collection_names().await? {
        collections.push(qdrant.snapshot_collection(&name, &qdrant_dir).await?);
    }

    let (files, bytes) = copy_tree(
        &cfg.data_dir.join("storage"),
        &staging.join("objects").join("storage"),
    )?;

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
    })
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// 恢复入口：`pkg` 可为 .tar.zst 或已解包目录。逐段恢复存在的部分，
/// 缺段记日志不判负——包可能来自只备份了部分段的旧版本。
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
    if manifest_path.exists() {
        let manifest: BackupManifest =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
        tracing::info!(
            version = %manifest.version,
            revision = %manifest.revision,
            instance = %manifest.instance_fingerprint,
            created_at = %manifest.created_at,
            "restoring package"
        );
    } else {
        return Err(format!("{} missing manifest.json", pkg_dir.display()).into());
    }

    let pg_dir = pkg_dir.join("pg");
    if pg_dir.exists() {
        let pool = PgPool::connect(&database_url).await?;
        let tables = restore_postgres(&pool, &pg_dir).await?;
        tracing::info!(tables = tables.len(), "postgres restored");
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

    if let Some((root, _)) = &extract_root {
        let _ = std::fs::remove_dir_all(root);
    }
    tracing::info!("restore complete");
    Ok(())
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
            keep_count: 7,
            run_deadline_seconds: DEFAULT_RUN_DEADLINE_SECONDS,
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
    fn table_name_whitelist_blocks_injection() {
        assert!(validate_table_name("audit_logs").is_ok());
        assert!(validate_table_name("t1").is_ok());
        assert!(validate_table_name("evil\"; DROP TABLE x; --").is_err());
        assert!(validate_table_name("Upper").is_err());
        assert!(validate_table_name("").is_err());
    }
}

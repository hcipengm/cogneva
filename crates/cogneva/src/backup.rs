//! `cogneva backup` / `cogneva restore`：把权威数据面打成可搬运备份包。
//!
//! 权威面 = 关系数据（逻辑导出）+ 向量索引（服务端一致性快照）+ 对象 raw 层
//! （文件级复制）+ 实例身份（Secret 打成 apply-able manifest）。其余状态
//! （NATS/Redis/Meilisearch/registry/进化克隆）全部可重建，不进包。
//!
//! 包布局：
//! ```text
//! <pkg>/manifest.json                 版本/rev/实例指纹/时间/各段内容清单
//! <pkg>/secrets/cogneva-secrets.yaml  apply-able Secret（data 为 base64）
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
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// 包名前缀与时间戳格式：字典序排序即时间序，保留策略依赖这一点。
pub const PACKAGE_PREFIX: &str = "cogneva-backup-";
pub const PACKAGE_SUFFIX: &str = ".tar.zst";
const TIMESTAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

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
    /// cogneva-data-pvc 挂载根；对象子树取其下 `storage/`。
    pub data_dir: PathBuf,
    pub keep_count: usize,
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
            data_dir: PathBuf::from(
                std::env::var("COGNEVA_DATA_DIR")
                    .unwrap_or_else(|_| "/var/lib/cogneva-data".into()),
            ),
            keep_count: std::env::var("COGNEVA_BACKUP_KEEP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(7),
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
            let data = std::fs::read(pkg_pg_dir.join(format!("public.{table}.csv")))?;
            let mut copy_in = tx
                .copy_in_raw(&format!(
                    "COPY \"{table}\" FROM STDIN WITH (FORMAT csv, HEADER true)"
                ))
                .await?;
            copy_in.send(data).await?;
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
pub fn build_secret_manifest(
    secrets_dir: &Path,
    namespace: &str,
) -> Result<(String, Vec<String>), Box<dyn std::error::Error>> {
    let mut data = BTreeMap::new();
    for entry in std::fs::read_dir(secrets_dir)? {
        let entry = entry?;
        let path = entry.path();
        // 挂载点下还有 ..data / ..2024_xxx 这类符号链接，只收普通文件。
        if !path.is_file() || path.is_symlink() {
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
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: cogneva-secrets\n  \
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

/// 保留策略：包名时间戳字典序即新旧序，留最新 N 份。删除旧包失败只告警——
/// 保留失败绝不能反过来弄丢新包。
pub fn enforce_retention(dir: &Path, keep: usize) -> Vec<PathBuf> {
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

pub async fn run_backup(cfg: &BackupConfig) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&cfg.backup_dir)?;
    let now = Utc::now();
    let staging_name = package_name(now)
        .trim_end_matches(PACKAGE_SUFFIX)
        .to_string();
    let staging = cfg.backup_dir.join(&staging_name);
    std::fs::create_dir_all(&staging)?;

    let result = run_backup_into(cfg, &staging).await;
    match result {
        Ok(manifest) => {
            let manifest_json = serde_json::to_string_pretty(&manifest)?;
            std::fs::write(staging.join("manifest.json"), format!("{manifest_json}\n"))?;
            let final_path = cfg.backup_dir.join(package_name(now));
            tar_package(&staging, &staging_name, &final_path)?;
            std::fs::remove_dir_all(&staging)?;
            let deleted = enforce_retention(&cfg.backup_dir, cfg.keep_count);
            tracing::info!(
                package = %final_path.display(),
                kept = cfg.keep_count,
                pruned = deleted.len(),
                "backup package complete"
            );
            Ok(final_path)
        }
        Err(e) => {
            // 半成品 staging 必须清掉：留在原地会被当成可恢复包误用。
            let _ = std::fs::remove_dir_all(&staging);
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
    let (yaml, keys) = build_secret_manifest(&cfg.secrets_dir, &cfg.namespace)?;
    std::fs::write(secrets_dir.join("cogneva-secrets.yaml"), yaml)?;

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
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("pg-password"), b"s3cret\n").unwrap();
        std::fs::write(dir.path().join("jwt-secret"), b"abc").unwrap();
        // 挂载 Secret 卷里的内部符号链接命名，不许进包。
        std::fs::write(dir.path().join("..data"), b"garbage").unwrap();

        let (yaml, keys) = build_secret_manifest(dir.path(), "cogneva").unwrap();

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
        assert!(build_secret_manifest(dir.path(), "cogneva").is_err());
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
    fn table_name_whitelist_blocks_injection() {
        assert!(validate_table_name("audit_logs").is_ok());
        assert!(validate_table_name("t1").is_ok());
        assert!(validate_table_name("evil\"; DROP TABLE x; --").is_err());
        assert!(validate_table_name("Upper").is_err());
        assert!(validate_table_name("").is_err());
    }
}

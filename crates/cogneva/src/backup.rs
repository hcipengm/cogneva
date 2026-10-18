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
use cog_core::{
    BackupDrillRecord, BackupOutcome, BackupRunClosing, BackupRunOpening, BackupRunStore,
};
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
            offsite_dir: std::env::var("COGNEVA_BACKUP_OFFSITE_DIR")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
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
    for column in &columns {
        if column.is_empty()
            || !column
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(format!("unsafe column name in package: {column:?}").into());
        }
    }
    Ok(columns)
}

fn copy_from_statement(table: &str, columns: &[String]) -> String {
    let list: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    format!(
        "COPY \"{table}\" ({}) FROM STDIN WITH (FORMAT csv, HEADER true)",
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
            let path = pkg_pg_dir.join(format!("public.{table}.csv"));
            let columns = csv_columns(&{
                use std::io::BufRead;
                let mut header = String::new();
                std::io::BufReader::new(std::fs::File::open(&path)?).read_line(&mut header)?;
                header
            })?;
            let mut file = std::fs::File::open(&path)?;
            let mut copy_in = tx
                .copy_in_raw(&copy_from_statement(table, &columns))
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
        // 清单说这个包里有什么，此刻盘上是事实——两者对不上就停在这里。
        verify_package_against_manifest(&pkg_dir, &manifest)?;
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
    tracing::info!(tables = restored.len(), "drill: postgres segment restored");

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
        for header in ["id; DROP TABLE users", "id,na\"me", "id,,name", ""] {
            assert!(
                csv_columns(header).is_err(),
                "header {header:?} should not become a column list"
            );
        }
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
}

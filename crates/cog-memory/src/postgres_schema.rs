//! PostgreSQL-backed implementation of [`SchemaBackend`].
//! This is the production-grade Layer 1 (Schema) backend.  It stores
//! [`SchemaEntry`] rows in a single `schema_entries` table. `properties` is an
//! open-set payload stored as text; the three keys any query reads by name are
//! columns beside it (see [`RELATION_FROM_KEY`]).
//! The table shape lives in [`SCHEMA_ENTRIES_DDL`] alone, so that the columns
//! a statement may bind and the columns the table actually has cannot drift
//! apart in two places.  Call [`PostgresSchemaBackend::init_table`] at startup
//! to apply it idempotently.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use std::time::Duration;

use cog_core::{fill_slots, SchemaEntry, SchemaKind, SchemaSearchResult, SourceRef};
use cog_core::{merge_schema_observation, SchemaBackend};
use cog_core::{SFError, SFResult};

/// The three `properties` keys a relation entry is classified by.
///
/// They were values inside the document and were filtered on with `->>`
/// (`from`, `to`, `relation_type`). A key read by value wants a column, so each
/// has one — and the document no longer carries it: one fact, one place. The
/// store splits them off on write and puts them back on read, so a caller still
/// sees the map it wrote.
pub const RELATION_FROM_KEY: &str = "from";
// not-a-bound: 单位换算（定义性常量：改它只改单位，不改任何阈值行为）
pub const RELATION_TO_KEY: &str = "to";
pub const RELATION_TYPE_KEY: &str = "relation_type";

/// SQL DDL applied by [`PostgresSchemaBackend::init_table`].
///
/// The shape only, and not the whole of it: three statements that fill rows
/// sit between this text and [`SCHEMA_ENTRIES_AFTER_REPAIR`], because
/// `SET NOT NULL` can only run once the rows are filled and the filling is the
/// expensive part. Anything that walks the rows belongs in the repair, not
/// here -- this list holds `ACCESS EXCLUSIVE` from its first `ALTER` to its
/// last statement, so a table walk appended to it keeps readers out for the
/// whole walk. Measured on a rolling restart of the entry store, which merges
/// what this list keeps apart: twelve seconds for the `ALTER`s, then
/// twenty-six more for the correction that shared their transaction.
pub const SCHEMA_ENTRIES_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_entries (
    id                TEXT        PRIMARY KEY,
    namespace         TEXT        NOT NULL DEFAULT 'default',
    kind              TEXT        NOT NULL,
    name              TEXT        NOT NULL,
    key               TEXT        NOT NULL,
    -- 开集载荷存 text：它整块写、整块读，没有被按名索引进去的形状。
    -- 没有默认值：两台机器（新装的与升上来的）形状要一样，而 `SET DEFAULT` 是
    -- 一条从目录里答不出来的语句——它会被每一次启动重发，每发一次取一次
    -- ACCESS EXCLUSIVE。写这一列的两条语句都带值。
    properties        TEXT        NOT NULL,
    -- 三个按键读的键升成列。关系查询按值筛这三格，索引也在这三格上。
    rel_from          TEXT,
    rel_to            TEXT,
    rel_relation_type TEXT,
    raw_uri           TEXT        NOT NULL,
    range_spec        TEXT,
    extractor_version TEXT        NOT NULL,
    extracted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confidence        REAL        NOT NULL DEFAULT 1.0,
    importance        REAL        NOT NULL DEFAULT 0.5,
    observed_by       TEXT[]      NOT NULL DEFAULT '{}',
    occurrences       BIGINT      NOT NULL DEFAULT 1,
    first_seen        TIMESTAMPTZ,
    last_seen         TIMESTAMPTZ,
    retrieval_count   BIGINT      NOT NULL DEFAULT 0,
    last_retrieved_at TIMESTAMPTZ
);
-- 删之前的那一份副本：整行 row_to_json 的文本，按 (id, namespace) 一条。
-- 事实面只有一种删法——写副本、读回来逐字节比、然后才删；这张表自己不删。
CREATE TABLE IF NOT EXISTS schema_entries_archive (
    id          TEXT        NOT NULL,
    namespace   TEXT        NOT NULL,
    body        TEXT        NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS observed_by TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS occurrences BIGINT NOT NULL DEFAULT 1;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS first_seen  TIMESTAMPTZ;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS last_seen   TIMESTAMPTZ;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS retrieval_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS last_retrieved_at TIMESTAMPTZ;
-- 老库：三格先加列（回填在 [`SCHEMA_ENTRIES_REPAIR`] 里，那一步要扫表），
-- 载荷列从 jsonb 搬成 text。默认值要跟着换：jsonb 的 `'{}'::jsonb` 不能自动
-- 转成 text，直接 `TYPE` 会以「default cannot be cast automatically」失败。
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS rel_from TEXT;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS rel_to TEXT;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS rel_relation_type TEXT;
ALTER TABLE schema_entries ALTER COLUMN properties DROP DEFAULT;
ALTER TABLE schema_entries ALTER COLUMN properties TYPE TEXT USING properties::text;
CREATE INDEX IF NOT EXISTS idx_schema_entries_rel_from ON schema_entries(namespace, rel_from);
CREATE INDEX IF NOT EXISTS idx_schema_entries_rel_to   ON schema_entries(namespace, rel_to);
"#;

/// The rows [`SCHEMA_ENTRIES_DDL`] leaves unset, filled in its own transaction.
///
/// Rows written before the observer list existed name their one origin in
/// `raw_uri`. Seeding the list from that column is what keeps them answerable
/// by `schema_for_raw`: an entry whose list stays empty looks unextracted, and
/// the reconciliation pass would extract every one of them again on each sweep.
/// Idempotent, and repeated at every startup so a stale writer's rows are
/// picked up too.
///
/// Separate from the DDL because it walks the table, and the DDL's list holds
/// the table `ACCESS EXCLUSIVE` until its last statement ends. A writer asks
/// only for `ROW EXCLUSIVE`, which is the strongest lock a reader never waits
/// behind.
pub const SCHEMA_ENTRIES_REPAIR: &str = r#"
UPDATE schema_entries SET observed_by = ARRAY[raw_uri] WHERE cardinality(observed_by) = 0;
UPDATE schema_entries SET first_seen = extracted_at WHERE first_seen IS NULL;
UPDATE schema_entries SET last_seen  = extracted_at WHERE last_seen  IS NULL;
-- 三个按键读的键从文档搬进列，搬完从文档里删掉：同一个事实留在两处就是两个
-- 写者，改一处不改另一处。谓词先按文本找（不解析 JSON），所以每一次启动的代价
-- 是一次列扫描而不是一次逐行解析；搬完之后它读到零行，`jsonb` 那一侧一次也不跑。
UPDATE schema_entries
   SET rel_from = properties::jsonb ->> 'from',
       rel_to = properties::jsonb ->> 'to',
       rel_relation_type = properties::jsonb ->> 'relation_type',
       properties = (properties::jsonb - 'from' - 'to' - 'relation_type')::text
 WHERE properties LIKE '%"from"%'
    OR properties LIKE '%"to"%'
    OR properties LIKE '%"relation_type"%';
"#;

/// What can only be said once the rows are filled: the two columns the repair
/// guarantees are non-null, and the indexes over the table.
///
/// This has to come after [`SCHEMA_ENTRIES_REPAIR`] -- `SET NOT NULL` on a
/// column the repair has not filled fails outright -- and it is a third list
/// rather than a tail of the first for the same reason the repair is a second:
/// it takes `ACCESS EXCLUSIVE` again for the two `SET NOT NULL`s. That costs
/// what it costs once, on the first start after the columns appeared; on every
/// start after it is a catalog read, measured at 0.6 ms on a column that is
/// already non-null (and 400k rows beside it), because a satisfied constraint
/// needs no scan.
pub const SCHEMA_ENTRIES_AFTER_REPAIR: &str = r#"
ALTER TABLE schema_entries ALTER COLUMN first_seen SET NOT NULL;
ALTER TABLE schema_entries ALTER COLUMN last_seen  SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_schema_entries_namespace ON schema_entries(namespace);
CREATE INDEX IF NOT EXISTS idx_schema_entries_key       ON schema_entries(namespace, key);
CREATE INDEX IF NOT EXISTS idx_schema_entries_name_lower ON schema_entries(namespace, LOWER(name));
CREATE INDEX IF NOT EXISTS idx_schema_entries_raw_uri   ON schema_entries(namespace, raw_uri);
CREATE INDEX IF NOT EXISTS idx_schema_entries_kind      ON schema_entries(namespace, kind);
CREATE INDEX IF NOT EXISTS idx_schema_entries_observed_by ON schema_entries USING GIN (observed_by);
"#;

/// Columns every `SELECT` in this module returns, in the order
/// [`PostgresSchemaBackend::row_to_entry`] reads them.
const ENTRY_COLUMNS: &str = "namespace, id, kind, name, key, properties, rel_from, rel_to, \
                             rel_relation_type, raw_uri, range_spec, \
                             extractor_version, extracted_at, confidence, importance, \
                             observed_by, occurrences, first_seen, last_seen, \
                             retrieval_count, last_retrieved_at";

/// The relation query, with a slot per word it says out loud.
///
/// The row's kind and the three direction words are declared in cog-core; this
/// statement only writes them down. Spelled here, a renamed variant would leave
/// the query matching nothing — which is exactly what an entity with no
/// relations returns.
const RELATIONS_SQL_TEMPLATE: &str = r#"
            SELECT namespace, id, kind, name, key, properties, rel_from, rel_to,
                   rel_relation_type, raw_uri, range_spec,
                   extractor_version, extracted_at, confidence, importance
            FROM schema_entries
            WHERE namespace = $1 AND kind = '{kind:relation}'
              AND (
                  ($2 = '{dir:from}' AND rel_from = $3)
                  OR ($2 = '{dir:to}' AND rel_to = $3)
                  OR ($2 = '{dir:both}' AND (rel_from = $3 OR rel_to = $3))
              )
              AND ($4::TEXT IS NULL OR rel_relation_type = $4)
            "#;

/// 关系查询要填的槽：行的 `kind` 与三个方向词。
///
/// 两族词都在 cog-core 里声明；这条查询只是把它们写进语句，不再自己拼一遍。
fn direction_slots() -> Vec<(&'static str, &'static str)> {
    let mut slots = vec![("kind", SchemaKind::Relation.as_str())];
    slots.extend(
        cog_core::RelationDirection::ALL
            .iter()
            .map(|direction| ("dir", direction.as_str())),
    );
    slots
}

/// PostgreSQL-backed Schema layer.
/// Wraps a shared [`PgPool`] so callers can reuse an existing connection pool
/// (e.g. the one inside `cog_adapters::PostgresAdapter`).
pub struct PostgresSchemaBackend {
    pool: PgPool,
}

impl PostgresSchemaBackend {
    /// Build a backend on top of an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connect to a PostgreSQL DSN with sensible default pool sizing.
    pub async fn connect(dsn: impl AsRef<str>) -> SFResult<Self> {
        Self::connect_with_options(dsn, 16, 1, 10, 600).await
    }

    /// Connect with explicit pool options.
    pub async fn connect_with_options(
        dsn: impl AsRef<str>,
        max_connections: u32,
        min_connections: u32,
        acquire_timeout_secs: u64,
        idle_timeout_secs: u64,
    ) -> SFResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .idle_timeout(Some(Duration::from_secs(idle_timeout_secs)))
            .connect(dsn.as_ref())
            .await
            .map_err(|e| SFError::Agent(format!("PostgresSchemaBackend connect failed: {}", e)))?;
        Ok(Self { pool })
    }

    /// Borrow the underlying pool (useful for sharing it with other components).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the table + index DDL.  Safe to call repeatedly (uses
    /// `CREATE ... IF NOT EXISTS`).
    ///
    /// Three lists, in this order: the shape, the rows, and what depends on
    /// the rows. Each is its own transaction, so the table is closed to readers
    /// for the length of the shape and no longer.
    pub async fn init_table(&self, runner: &dyn cog_core::SchemaRunner) -> SFResult<()> {
        // Through the shared runner, not `raw_sql` directly: this list holds
        // ALTERs, and an ALTER takes its lock before it checks whether it has
        // anything to do, so on a restart it queues behind the replica that is
        // still reading `schema_entries`. The runner bounds that wait instead
        // of letting the newcomer freeze the replica it replaces.
        for schema in [
            SCHEMA_ENTRIES_DDL,
            SCHEMA_ENTRIES_REPAIR,
            SCHEMA_ENTRIES_AFTER_REPAIR,
        ] {
            runner.apply_schema(schema).await.map_err(|e| {
                SFError::Agent(format!("PostgresSchemaBackend init_table failed: {}", e))
            })?;
        }
        Ok(())
    }

    /// 把三个升成列的键从文档里取出来，剩下的存成文本。
    ///
    /// 与 [`Self::row_to_entry`] 对称：那边把列拼回文档，这边把键拆出去。
    /// 两处都只有这一份拼写，所以「文档里有这三个键」不会在库里出现第二次。
    fn split_relation_keys(
        properties: &serde_json::Value,
    ) -> (String, Option<String>, Option<String>, Option<String>) {
        let mut document = properties
            .as_object()
            .cloned()
            .unwrap_or(Default::default());
        let mut take = |key: &str| {
            document
                .remove(key)
                .and_then(|value| value.as_str().map(str::to_string))
        };
        let from = take(RELATION_FROM_KEY);
        let to = take(RELATION_TO_KEY);
        let relation_type = take(RELATION_TYPE_KEY);
        let rest = serde_json::Value::Object(document).to_string();
        (rest, from, to, relation_type)
    }

    fn row_to_entry(row: &PgRow) -> SFResult<SchemaEntry> {
        let namespace: String = row
            .try_get("namespace")
            .map_err(|e| SFError::Agent(format!("decode namespace: {}", e)))?;
        let kind_str: String = row
            .try_get("kind")
            .map_err(|e| SFError::Agent(format!("decode kind: {}", e)))?;
        let kind = parse_schema_kind(&kind_str)?;

        let raw_uri: String = row
            .try_get("raw_uri")
            .map_err(|e| SFError::Agent(format!("decode raw_uri: {}", e)))?;
        let range_spec: Option<String> = row
            .try_get("range_spec")
            .map_err(|e| SFError::Agent(format!("decode range_spec: {}", e)))?;
        let extractor_version: String = row
            .try_get("extractor_version")
            .map_err(|e| SFError::Agent(format!("decode extractor_version: {}", e)))?;
        let extracted_at: DateTime<Utc> = row
            .try_get("extracted_at")
            .map_err(|e| SFError::Agent(format!("decode extracted_at: {}", e)))?;
        let confidence: f32 = row
            .try_get("confidence")
            .map_err(|e| SFError::Agent(format!("decode confidence: {}", e)))?;
        let importance: f32 = row
            .try_get("importance")
            .map_err(|e| SFError::Agent(format!("decode importance: {}", e)))?;
        // 载荷是 JSON 文本，三个按键读的键在列里；读的一端把列拼回文档，
        // 所以调用方看到的仍是它写进去的那张 map。坏行读成空文档而不是让整批
        // 读失败：一条条目的其余列仍然是真的。
        let properties_text: String = row
            .try_get("properties")
            .map_err(|e| SFError::Agent(format!("decode properties: {}", e)))?;
        let mut properties: serde_json::Value =
            serde_json::from_str(&properties_text).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(document) = properties.as_object_mut() {
            for (column, key) in [
                ("rel_from", RELATION_FROM_KEY),
                ("rel_to", RELATION_TO_KEY),
                ("rel_relation_type", RELATION_TYPE_KEY),
            ] {
                if let Ok(Some(value)) = row.try_get::<Option<String>, _>(column) {
                    document.insert(key.to_string(), serde_json::Value::String(value));
                }
            }
        }
        let id: String = row
            .try_get("id")
            .map_err(|e| SFError::Agent(format!("decode id: {}", e)))?;
        let name: String = row
            .try_get("name")
            .map_err(|e| SFError::Agent(format!("decode name: {}", e)))?;
        let key: String = row
            .try_get("key")
            .map_err(|e| SFError::Agent(format!("decode key: {}", e)))?;
        let observed_by: Vec<String> = row
            .try_get("observed_by")
            .map_err(|e| SFError::Agent(format!("decode observed_by: {}", e)))?;
        let occurrences: i64 = row
            .try_get("occurrences")
            .map_err(|e| SFError::Agent(format!("decode occurrences: {}", e)))?;
        let first_seen: DateTime<Utc> = row
            .try_get("first_seen")
            .map_err(|e| SFError::Agent(format!("decode first_seen: {}", e)))?;
        let last_seen: DateTime<Utc> = row
            .try_get("last_seen")
            .map_err(|e| SFError::Agent(format!("decode last_seen: {}", e)))?;
        let retrieval_count: i64 = row
            .try_get("retrieval_count")
            .map_err(|e| SFError::Agent(format!("decode retrieval_count: {}", e)))?;
        let last_retrieved_at: Option<DateTime<Utc>> = row
            .try_get("last_retrieved_at")
            .map_err(|e| SFError::Agent(format!("decode last_retrieved_at: {}", e)))?;

        Ok(SchemaEntry {
            id,
            namespace,
            kind,
            name,
            key,
            properties,
            source_ref: SourceRef {
                raw_uri,
                range: range_spec,
                extractor_version,
                extracted_at,
            },
            observed_by,
            occurrences: occurrences.max(0) as u64,
            first_seen,
            last_seen,
            confidence,
            importance,
            extracted_at,
            retrieval_count: retrieval_count.max(0) as u64,
            last_retrieved_at,
        })
    }
}

/// Convert a [`SchemaKind`] to its canonical lowercase string form.
fn schema_kind_str(kind: SchemaKind) -> &'static str {
    kind.as_str()
}

/// Parse a [`SchemaKind`] from its canonical lowercase string form.
fn parse_schema_kind(s: &str) -> SFResult<SchemaKind> {
    // One declaration, in the enum: this reader used to carry the ten words a
    // second time, and a word added to the enum but not here would have read as
    // "unknown kind" on rows the writer had just stored.
    SchemaKind::parse(s).ok_or_else(|| SFError::Agent(format!("unknown schema kind: {s}")))
}

/// 删之前先写一份读得回来的副本，读回来逐字节比，然后才删。两条删路径（最后一位
/// 观察者被摘掉、显式删一条）共用这一处：两份写法分家时，其中一条会漏掉副本，
/// 而漏掉的那条看起来与「这张表本来就没有这一行」一模一样。
async fn archive_and_delete(
    conn: &mut sqlx::PgConnection,
    namespace: &str,
    id: &str,
) -> SFResult<()> {
    let body: Option<String> = sqlx::query_scalar(
        "SELECT row_to_json(t)::text FROM schema_entries t \
          WHERE namespace = $1 AND id = $2",
    )
    .bind(namespace)
    .bind(id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| SFError::Agent(format!("archive_and_delete read: {e}")))?;
    let Some(body) = body else {
        return Ok(());
    };
    sqlx::query("INSERT INTO schema_entries_archive (id, namespace, body) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(namespace)
        .bind(&body)
        .execute(&mut *conn)
        .await
        .map_err(|e| SFError::Agent(format!("archive_and_delete archive: {e}")))?;
    let back: String = sqlx::query_scalar(
        "SELECT body FROM schema_entries_archive WHERE id = $1 AND namespace = $2",
    )
    .bind(id)
    .bind(namespace)
    .fetch_one(&mut *conn)
    .await
    .map_err(|e| SFError::Agent(format!("archive_and_delete read-back: {e}")))?;
    crate::deletion::deletion_is_allowed(back == body)?;
    sqlx::query("DELETE FROM schema_entries WHERE namespace = $1 AND id = $2")
        .bind(namespace)
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(|e| SFError::Agent(format!("archive_and_delete delete: {e}")))?;
    Ok(())
}

#[async_trait]
impl SchemaBackend for PostgresSchemaBackend {
    /// Write one observation of a fact, merging it into the row that identity
    /// already names.
    ///
    /// The read and the write have to see the same row or a concurrent store
    /// of the same identity loses one of the two observations, so the merge
    /// runs inside a transaction on a locked row. The `DO NOTHING` insert is
    /// what puts a row there to lock when none existed: two inserters racing
    /// on a fresh identity both come out of it, one having inserted and one
    /// not, and the loser then merges into the winner's row instead of failing
    /// on the primary key. Merging the just-inserted row with itself is a
    /// no-op, which is what makes one code path serve both cases.
    async fn store_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Agent(format!("store_schema begin: {}", e)))?;

        let (properties, rel_from, rel_to, rel_relation_type) =
            Self::split_relation_keys(&entry.properties);
        sqlx::query(
            r#"
            INSERT INTO schema_entries
                (id, namespace, kind, name, key, properties, raw_uri, range_spec,
                 extractor_version, extracted_at, confidence, importance,
                 observed_by, occurrences, first_seen, last_seen,
                 rel_from, rel_to, rel_relation_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17, $18, $19)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&entry.id)
        .bind(&entry.namespace)
        .bind(schema_kind_str(entry.kind))
        .bind(&entry.name)
        .bind(&entry.key)
        .bind(&properties)
        .bind(&entry.source_ref.raw_uri)
        .bind(&entry.source_ref.range)
        .bind(&entry.source_ref.extractor_version)
        .bind(entry.extracted_at)
        .bind(entry.confidence)
        .bind(entry.importance)
        .bind(&entry.observed_by)
        .bind(entry.occurrences as i64)
        .bind(entry.first_seen)
        .bind(entry.last_seen)
        .bind(rel_from)
        .bind(rel_to)
        .bind(rel_relation_type)
        .execute(&mut *tx)
        .await
        .map_err(|e| SFError::Agent(format!("store_schema insert: {}", e)))?;

        let select = format!("SELECT {ENTRY_COLUMNS} FROM schema_entries WHERE id = $1 FOR UPDATE");
        let stored = sqlx::query(&select)
            .bind(&entry.id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| SFError::Agent(format!("store_schema lock: {}", e)))?;
        let merged = merge_schema_observation(&Self::row_to_entry(&stored)?, entry);

        let (properties, rel_from, rel_to, rel_relation_type) =
            Self::split_relation_keys(&merged.properties);
        sqlx::query(
            r#"
            UPDATE schema_entries SET
                namespace = $2, kind = $3, name = $4, key = $5, properties = $6,
                raw_uri = $7, range_spec = $8, extractor_version = $9,
                extracted_at = $10, confidence = $11, importance = $12,
                observed_by = $13, occurrences = $14, first_seen = $15, last_seen = $16,
                rel_from = $17, rel_to = $18, rel_relation_type = $19
            WHERE id = $1
            "#,
        )
        .bind(&merged.id)
        .bind(&merged.namespace)
        .bind(schema_kind_str(merged.kind))
        .bind(&merged.name)
        .bind(&merged.key)
        .bind(&properties)
        .bind(&merged.source_ref.raw_uri)
        .bind(&merged.source_ref.range)
        .bind(&merged.source_ref.extractor_version)
        .bind(merged.extracted_at)
        .bind(merged.confidence)
        .bind(merged.importance)
        .bind(&merged.observed_by)
        .bind(merged.occurrences as i64)
        .bind(merged.first_seen)
        .bind(merged.last_seen)
        .bind(rel_from)
        .bind(rel_to)
        .bind(rel_relation_type)
        .execute(&mut *tx)
        .await
        .map_err(|e| SFError::Agent(format!("store_schema update: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| SFError::Agent(format!("store_schema commit: {}", e)))?;
        Ok(())
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        let sql =
            format!("SELECT {ENTRY_COLUMNS} FROM schema_entries WHERE id = $1 AND namespace = $2");
        let row = sqlx::query(&sql)
            .bind(id)
            .bind(namespace)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("get_schema failed: {}", e)))?;

        match row {
            Some(r) => Ok(Some(Self::row_to_entry(&r)?)),
            None => Ok(None),
        }
    }

    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>> {
        let pattern = format!("%{}%", query);
        // Substring hits all score 1.0 on similarity, so importance is the only
        // ranking signal; ordering in SQL (not after the LIMIT) is what makes the
        // truncation keep the higher-rated facts rather than an arbitrary subset.
        // The id is the tie-break for the same reason the in-memory rankings use
        // it: equal importances would otherwise let the database return any of
        // them, and which ones survive the LIMIT would follow that choice.
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM schema_entries \
             WHERE namespace = $1 AND (name ILIKE $2 OR key ILIKE $2) \
             ORDER BY importance DESC, id ASC \
             LIMIT $3"
        );
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .bind(&pattern)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("search_schema failed: {}", e)))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let entry = Self::row_to_entry(row)?;
            let score = cog_core::importance_weighted_score(1.0, entry.importance);
            results.push(SchemaSearchResult { entry, score });
        }
        Ok(results)
    }

    /// Membership over the observer list, not equality against the single
    /// `raw_uri`: an entry merged from several sources belongs to each of
    /// them, and matching on the column would answer "nothing extracted" for
    /// every source but the one that wrote last — which is what drives the
    /// reconciliation pass to re-extract them over and over.
    ///
    /// The `raw_uri` arm is the same union [`SchemaEntry::observed_from`]
    /// applies, restated in SQL so a row and a decoded entry answer the same
    /// question the same way: `source_ref` names an origin whether or not the
    /// list still carries it.
    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        let raw_uri = format!("memory://{}", raw_id);
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM schema_entries \
             WHERE namespace = $1 AND ($2 = ANY(observed_by) OR raw_uri = $2)"
        );
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .bind(&raw_uri)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("schema_for_raw failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }
        Ok(entries)
    }

    /// Withdraw one origin from an entry, atomically, deleting the row only
    /// when that was the last origin.
    ///
    /// A read-modify-write in the caller would race another writer's merge on
    /// the same row: the merge would put the origin back and the deletion
    /// would be undone silently. Both outcomes are decided here from one
    /// snapshot of the row.
    async fn forget_schema_source(&self, namespace: &str, id: &str, raw_uri: &str) -> SFResult<()> {
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM schema_entries \
             WHERE namespace = $1 AND id = $2 FOR UPDATE"
        );
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Agent(format!("forget_schema_source begin: {}", e)))?;
        let row = sqlx::query(&sql)
            .bind(namespace)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SFError::Agent(format!("forget_schema_source lock: {}", e)))?;

        let Some(row) = row else {
            return Ok(());
        };
        match Self::row_to_entry(&row)?.without_observer(raw_uri) {
            Some(entry) => {
                sqlx::query(
                    "UPDATE schema_entries SET raw_uri = $2, observed_by = $3 WHERE id = $1",
                )
                .bind(id)
                .bind(&entry.source_ref.raw_uri)
                .bind(&entry.observed_by)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    SFError::Agent(format!("forget_schema_source update failed: {}", e))
                })?;
            }
            None => {
                archive_and_delete(&mut tx, namespace, id).await?;
            }
        }
        tx.commit()
            .await
            .map_err(|e| SFError::Agent(format!("forget_schema_source commit: {}", e)))?;
        Ok(())
    }

    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>> {
        let sql = format!("SELECT {ENTRY_COLUMNS} FROM schema_entries WHERE namespace = $1");
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("list_schema failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }
        Ok(entries)
    }

    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Agent(format!("delete_schema begin: {e}")))?;
        archive_and_delete(&mut tx, namespace, id).await?;
        tx.commit()
            .await
            .map_err(|e| SFError::Agent(format!("delete_schema commit: {e}")))?;
        Ok(())
    }

    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: cog_core::RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>> {
        let direction_str = direction.as_str();
        let sql = fill_slots(RELATIONS_SQL_TEMPLATE, &direction_slots());
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .bind(direction_str)
            .bind(entity)
            .bind(relation_type)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("query_relations failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }
        Ok(entries)
    }

    /// Write the entry as given, merging only its properties.
    ///
    /// The conflict target is the primary key, which is the identity the
    /// caller selected by. A conflict target of `(namespace, key)` would need
    /// a unique index on those columns that does not exist — the statement
    /// would fail outright rather than update anything.
    async fn update_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        let (properties, rel_from, rel_to, rel_relation_type) =
            Self::split_relation_keys(&entry.properties);
        sqlx::query(
            r#"
            INSERT INTO schema_entries
                (id, namespace, kind, name, key, properties, raw_uri, range_spec,
                 extractor_version, extracted_at, confidence, importance,
                 observed_by, occurrences, first_seen, last_seen,
                 rel_from, rel_to, rel_relation_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17, $18, $19)
            ON CONFLICT (id) DO UPDATE SET
                namespace         = EXCLUDED.namespace,
                kind              = EXCLUDED.kind,
                name              = EXCLUDED.name,
                key               = EXCLUDED.key,
                -- 载荷是 text：合并仍在库侧完成，因为两个写者同时改一行时，
                -- 读出来在应用层合并会丢一次更新。`::jsonb` 只在这一处出现，
                -- 而它不是查询——没有任何一条语句按键筛过这一列。
                properties        = (schema_entries.properties::jsonb || EXCLUDED.properties::jsonb)::text,
                rel_from          = COALESCE(EXCLUDED.rel_from, schema_entries.rel_from),
                rel_to            = COALESCE(EXCLUDED.rel_to, schema_entries.rel_to),
                rel_relation_type = COALESCE(EXCLUDED.rel_relation_type, schema_entries.rel_relation_type),
                raw_uri           = EXCLUDED.raw_uri,
                range_spec        = EXCLUDED.range_spec,
                extractor_version = EXCLUDED.extractor_version,
                extracted_at      = EXCLUDED.extracted_at,
                confidence        = EXCLUDED.confidence,
                importance        = EXCLUDED.importance,
                observed_by       = EXCLUDED.observed_by,
                occurrences       = EXCLUDED.occurrences,
                first_seen        = EXCLUDED.first_seen,
                last_seen         = EXCLUDED.last_seen
            "#,
        )
        .bind(&entry.id)
        .bind(&entry.namespace)
        .bind(schema_kind_str(entry.kind))
        .bind(&entry.name)
        .bind(&entry.key)
        .bind(&properties)
        .bind(&entry.source_ref.raw_uri)
        .bind(&entry.source_ref.range)
        .bind(&entry.source_ref.extractor_version)
        .bind(entry.extracted_at)
        .bind(entry.confidence)
        .bind(entry.importance)
        .bind(&entry.observed_by)
        .bind(entry.occurrences as i64)
        .bind(entry.first_seen)
        .bind(entry.last_seen)
        .bind(rel_from)
        .bind(rel_to)
        .bind(rel_relation_type)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("update_schema failed: {}", e)))?;
        Ok(())
    }

    /// Bump the retrieval counters for every id in one statement.
    ///
    /// One statement for the batch, not one per id: the entries reach the model
    /// as a list from a single assembly, so they are recorded the same way. Ids
    /// with no row are silently skipped — a search can name an entry the store
    /// has since dropped, and failing the whole batch over a stale id would lose
    /// the readings for every live entry in it.
    async fn record_schema_retrieval(&self, ids: &[String]) -> SFResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE schema_entries \
             SET retrieval_count = retrieval_count + 1, last_retrieved_at = NOW() \
             WHERE id = ANY($1)",
        )
        .bind(ids)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("record_schema_retrieval failed: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The DDL is the one place the table's shape lives, and `ENTRY_COLUMNS` is
    /// what every read selects. A column named in one and missing from the other
    /// fails only against a live database — the kind of drift that reaches
    /// production because nothing here reads it. Checked without a database so
    /// the two lists cannot disagree silently.
    #[test]
    fn every_selected_column_exists_in_the_ddl() {
        for column in ENTRY_COLUMNS.split(',').map(|c| c.trim()) {
            assert!(
                column.is_empty() || SCHEMA_ENTRIES_DDL.contains(column),
                "ENTRY_COLUMNS names {column}, which the DDL never creates"
            );
        }
    }

    /// 关系查询里的 kind 与三个方向词必须逐字带着枚举的词。
    ///
    /// 一个没填的槽会让这条语句在第一次查询时被数据库拒解析；一个填错的词则更安静
    /// ——选不中任何行，读起来就是"这个实体没有关系"。
    #[test]
    fn the_relation_query_names_its_kind_and_directions() {
        let sql = fill_slots(RELATIONS_SQL_TEMPLATE, &direction_slots());
        assert!(!sql.contains('{'), "还有槽没有填：{sql}");
        assert!(sql.contains(&format!("kind = '{}'", SchemaKind::Relation.as_str())));
        for direction in cog_core::RelationDirection::ALL {
            assert!(
                sql.contains(&format!("= '{}' AND", direction.as_str())),
                "查询里没有 {} 这个方向",
                direction.as_str()
            );
        }
    }
}

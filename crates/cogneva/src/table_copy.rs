//! `cogneva table-copy <source> <target> [--batch N] [--verify]`：把一张表搬到另一张，
//! **可以中断、可以从任意一点接着做**。
//!
//! 水位不另存：目标表自己的主键就是水位——「搬到哪儿了」这件事已经写在目标里，再存
//! 一个计数就多了一份会漂的副本（进程崩在「写入目标」与「记账」之间，两边说的就不再
//! 是同一件事）。所以续跑从目标的最大主键之后开始；重放同一批靠主键冲突吞掉。
//!
//! 两侧在中断期间都答得对：源表一个字都没动；目标表只回答它已经有的事实。**切换读的
//! 那一半不在这里**（台账 S32）：搬完之前读源表，搬完并核对摘要之后再切。
//!
//! `--batch` 不写死的理由：一批是多大由这条命令的调用者按当时的库压决定；它不是一个
//! 藏在代码里的默认值。不给时每次搬完剩下的全部。

use sqlx::{PgPool, Row};

use crate::table_digest::{checked_table, digest_of, full_ident};

/// 一次搬表：源、目标、用来续跑与去重的主键列。
pub struct CopyPlan {
    pub source: String,
    pub target: String,
    pub keys: Vec<String>,
    /// 可以写进去的列（生成列不在里面：它们的值由库算，插进去会直接报错）。
    pub columns: Vec<String>,
}

impl CopyPlan {
    /// 目标表：与源同列同类型，主键与源一致——主键既是去重的依据，也是续跑的水位。
    pub fn create_target_sql(&self) -> String {
        let keys = self.quoted_keys().join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS INCLUDING GENERATED, \
             PRIMARY KEY ({keys}))",
            full_ident(&self.target),
            full_ident(&self.source)
        )
    }

    /// 搬到哪儿了：目标里最大的那个主键。空目标返回 `None`。
    pub fn resume_after_sql(&self) -> String {
        format!(
            "SELECT {} FROM {} ORDER BY {} DESC LIMIT 1",
            self.quoted_keys().join(", "),
            full_ident(&self.target),
            self.quoted_keys().join(", ")
        )
    }

    /// 还剩多少条没搬：主键大于水位的那些。
    pub fn remaining_sql(&self, bound: usize) -> String {
        let keys = self.quoted_keys().join(", ");
        let placeholders = (1..=bound)
            .map(|at| format!("${at}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT COUNT(*)::BIGINT FROM {} WHERE ({keys}) > ({placeholders})",
            full_ident(&self.source)
        )
    }

    /// 下一批：从水位之后按主键顺序取；冲突吞掉，重放写的是同一条。
    pub fn copy_batch_sql(&self, bound: usize, limit: Option<i64>) -> String {
        let keys = self.quoted_keys().join(", ");
        let placeholders = (1..=bound)
            .map(|at| format!("${at}"))
            .collect::<Vec<_>>()
            .join(", ");
        let limit = limit
            .map(|n| format!(" LIMIT {n}"))
            .unwrap_or(Default::default());
        let columns = self.quoted_columns().join(", ");
        format!(
            "INSERT INTO {} ({columns}) SELECT {columns} FROM {} WHERE ({keys}) > ({placeholders}) \
             ORDER BY {keys}{limit} ON CONFLICT DO NOTHING",
            full_ident(&self.target),
            full_ident(&self.source)
        )
    }

    fn quoted_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect()
    }

    fn quoted_keys(&self) -> Vec<String> {
        self.keys.iter().map(|key| format!("\"{key}\"")).collect()
    }
}

/// 源表的主键列，按主键里的顺序。没有主键的表不能这样搬：没有键就没有水位，中断之后
/// 谁也说不清搬到哪儿了，重放也没有东西可以吞。
pub async fn primary_key(pool: &PgPool, table: &str) -> Result<Vec<String>, String> {
    let rows = sqlx::query(
        "SELECT a.attname \
           FROM pg_index i \
           JOIN pg_attribute a \
             ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
          WHERE i.indrelid = $1::regclass AND i.indisprimary \
          ORDER BY array_position(i.indkey, a.attnum)",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("read the primary key of {table}: {e}"))?;
    let keys: Vec<String> = rows.iter().map(|row| row.get("attname")).collect();
    if keys.is_empty() {
        return Err(format!(
            "{table} 没有主键：没有键就没有水位，搬运中断之后说不清搬到哪儿了"
        ));
    }
    Ok(keys)
}

/// 能写进去的列：生成列不在里面。源表里带生成列（事件流带了一整排）时，`SELECT *`
/// 与 `INSERT ... SELECT *` 会在「往生成列里写值」上直接失败——而那条错发生在第一批，
/// 看起来像「这张表搬不了」，实际只是列清单要按目标算。
pub async fn insertable_columns(pool: &PgPool, table: &str) -> Result<Vec<String>, String> {
    let rows = sqlx::query(
        "SELECT attname FROM pg_attribute \
          WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated = '' \
          ORDER BY attnum",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("read the columns of {table}: {e}"))?;
    let columns: Vec<String> = rows.iter().map(|row| row.get("attname")).collect();
    if columns.is_empty() {
        return Err(format!("{table} 一列都没有：这不是一张能搬的表"));
    }
    Ok(columns)
}

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "table-copy")
        .skip(1)
        .collect();
    let positional: Vec<&String> = args.iter().filter(|arg| !arg.starts_with("--")).collect();
    let source = checked_table(
        positional
            .first()
            .ok_or("usage: cogneva table-copy <source> <target>")?,
    )?;
    let target = checked_table(
        positional
            .get(1)
            .ok_or("usage: cogneva table-copy <source> <target>")?,
    )?;
    if source == target {
        return Err("源与目标是同一张表：搬它到自己身上没有意义，也没有水位可断".into());
    }
    let batch: Option<i64> = args
        .iter()
        .position(|arg| arg == "--batch")
        .map(|at| {
            args.get(at + 1)
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|n| *n > 0)
                .ok_or("--batch 后面要跟一个正整数")
        })
        .transpose()?;
    let verify = args.iter().any(|arg| arg == "--verify");

    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the table")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let keys = primary_key(&pool, &source).await?;
    let columns = insertable_columns(&pool, &source).await?;
    let plan = CopyPlan {
        source: source.clone(),
        target: target.clone(),
        keys,
        columns,
    };
    sqlx::query(&plan.create_target_sql())
        .execute(&pool)
        .await?;

    let mut moved = 0i64;
    loop {
        let last = sqlx::query(&plan.resume_after_sql())
            .fetch_optional(&pool)
            .await?;
        let bound = plan.keys.len();
        let outcome = match last {
            Some(row) => {
                let sql = plan.copy_batch_sql(bound, batch);
                let mut query = sqlx::query(&sql);
                for at in 0..bound {
                    query = bind_key(query, &row, at)?;
                }
                query.execute(&pool).await?.rows_affected()
            }
            None => {
                // 空目标：没有水位，整批从头上搬。`WHERE (keys) > (NULL...)` 不是这个
                // 意思，所以这一支单独写。
                let limit = batch
                    .map(|n| format!(" LIMIT {n}"))
                    .unwrap_or(Default::default());
                let columns = plan
                    .columns
                    .iter()
                    .map(|column| format!("\"{column}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                sqlx::query(&format!(
                    "INSERT INTO {} ({columns}) SELECT {columns} FROM {} ORDER BY {}{limit} \
                     ON CONFLICT DO NOTHING",
                    full_ident(&plan.target),
                    full_ident(&plan.source),
                    plan.keys
                        .iter()
                        .map(|key| format!("\"{key}\""))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .execute(&pool)
                .await?
                .rows_affected()
            }
        };
        moved += outcome as i64;
        println!("copied\t{outcome}");
        if outcome == 0 {
            break;
        }
        println!("moved_total\t{moved}");
    }

    if verify {
        let (source_rows, source_digest) = digest_of(&pool, &source).await?;
        let (target_rows, target_digest) = digest_of(&pool, &target).await?;
        println!("source_rows\t{source_rows}");
        println!("target_rows\t{target_rows}");
        println!("source_digest\t{source_digest}");
        println!("target_digest\t{target_digest}");
        if source_digest != target_digest || source_rows != target_rows {
            return Err("两侧不是逐条相同：搬完不等于搬对，切换读之前先查这几行".into());
        }
    }
    Ok(())
}

/// 把水位那一行的第 `at` 列绑到查询上。列类型由数据库回答，所以先取类型再绑：
/// 一律按字符串绑会把时间与数字读成另一种类型，而错的那一种在 `>` 比较上不报错。
fn bind_key<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    row: &sqlx::postgres::PgRow,
    at: usize,
) -> Result<
    sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    Box<dyn std::error::Error>,
> {
    use sqlx::{TypeInfo, ValueRef};
    let raw = row.try_get_raw(at)?;
    if raw.is_null() {
        return Err("水位那一列是 NULL：主键不该是 NULL，读到的这一行不能当水位".into());
    }
    let type_name = raw.type_info().name().to_string();
    let query = match type_name.as_str() {
        "INT2" => query.bind(row.try_get::<i16, _>(at)?),
        "INT4" => query.bind(row.try_get::<i32, _>(at)?),
        "INT8" => query.bind(row.try_get::<i64, _>(at)?),
        "UUID" => query.bind(row.try_get::<uuid::Uuid, _>(at)?),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" => query.bind(row.try_get::<String, _>(at)?),
        "TIMESTAMPTZ" => query.bind(row.try_get::<chrono::DateTime<chrono::Utc>, _>(at)?),
        other => {
            return Err(format!(
                "主键列的类型是 {other}，这条命令还不认识它——续跑的水位不能按猜的类型比较"
            )
            .into())
        }
    };
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(keys: &[&str]) -> CopyPlan {
        CopyPlan {
            source: "public.src".into(),
            target: "public.dst".into(),
            keys: keys.iter().map(|key| key.to_string()).collect(),
            columns: ["a", "b", "c"]
                .iter()
                .map(|column| column.to_string())
                .collect(),
        }
    }

    #[test]
    fn the_target_carries_the_sources_key_because_that_is_the_watermark() {
        let sql = plan(&["id"]).create_target_sql();
        assert!(sql.contains("LIKE \"public\".\"src\""), "{sql}");
        assert!(sql.contains("PRIMARY KEY (\"id\")"), "{sql}");
    }

    #[test]
    fn the_watermark_is_the_targets_own_largest_key() {
        let sql = plan(&["id"]).resume_after_sql();
        assert_eq!(
            sql,
            "SELECT \"id\" FROM \"public\".\"dst\" ORDER BY \"id\" DESC LIMIT 1"
        );
        let sql = plan(&["a", "b"]).resume_after_sql();
        assert_eq!(
            sql,
            "SELECT \"a\", \"b\" FROM \"public\".\"dst\" ORDER BY \"a\", \"b\" DESC LIMIT 1"
        );
    }

    #[test]
    fn a_batch_orders_by_the_key_and_swallows_replays() {
        let sql = plan(&["id"]).copy_batch_sql(1, Some(500));
        assert!(sql.contains("WHERE (\"id\") > ($1)"), "{sql}");
        assert!(sql.contains("ORDER BY \"id\" LIMIT 500"), "{sql}");
        assert!(sql.contains("ON CONFLICT DO NOTHING"), "{sql}");
        // 复合键是行比较，不是两列各自比较：后者在第二列上会越过第一列的顺序。
        let sql = plan(&["a", "b"]).copy_batch_sql(2, None);
        assert!(sql.contains("WHERE (\"a\", \"b\") > ($1, $2)"), "{sql}");
        assert!(!sql.contains("LIMIT"), "不给批大小就是整批：{sql}");
    }

    #[test]
    fn the_remaining_count_uses_the_same_bound() {
        let sql = plan(&["a", "b"]).remaining_sql(2);
        assert!(sql.contains("WHERE (\"a\", \"b\") > ($1, $2)"), "{sql}");
        assert!(sql.contains("COUNT(*)"), "{sql}");
    }
}

//! `cogneva table-digest <table>`：一张表的逐行内容摘要。
//!
//! 迁移前后各跑一次，两个摘要相同就是「逐行相同」的一个可核对读数；不同就说明有行
//! 变了、少了或多了。排序按行的 JSON 文本，不依赖主键形状——迁移前后列顺序可以不同，
//! 行内容才是要比的东西。

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let table = std::env::args()
        .skip_while(|arg| arg != "table-digest")
        .nth(1)
        .ok_or("usage: cogneva table-digest <table>")?;
    let table = checked_table(&table)?;
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding the table")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let (rows, digest) = digest_of(&pool, &table).await?;
    println!("table\t{table}");
    println!("rows\t{rows}");
    println!("digest\t{digest}");
    Ok(())
}

/// 一张表的行数与逐行摘要。迁移前后各算一次，两对数都相同才是「逐条相同」。
pub(crate) async fn digest_of(
    pool: &sqlx::PgPool,
    table: &str,
) -> Result<(i64, String), Box<dyn std::error::Error>> {
    let quoted = full_ident(table);
    let (rows,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*)::BIGINT FROM {quoted}"))
        .fetch_one(pool)
        .await?;
    let (digest,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT md5(COALESCE(string_agg(row_to_json(t)::text, E'\\n' \
          ORDER BY row_to_json(t)::text), '')) FROM {quoted} t"
    ))
    .fetch_one(pool)
    .await?;
    Ok((rows, digest.unwrap_or_default()))
}

/// `cogneva table-diff <left> <right>`：两张同名/同形的表逐行对照，先给条数、再给
/// 头几处不同（两边各最多二十行）。迁移前后、双写两侧对账用同一句。
pub async fn diff_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "table-diff")
        .skip(1)
        .collect();
    let left = checked_table(
        args.first()
            .ok_or("usage: cogneva table-diff <left> <right>")?,
    )?;
    let right = checked_table(
        args.get(1)
            .ok_or("usage: cogneva table-diff <left> <right>")?,
    )?;
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding both tables")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let (only_left, only_right) = diff_counts(&pool, &left, &right).await?;
    let (left, right) = (full_ident(&left), full_ident(&right));
    println!("only_left\t{only_left}");
    println!("only_right\t{only_right}");
    for (direction, query) in [
        (
            "left",
            format!(
                "SELECT row_to_json(t)::text FROM {left} t \
                 EXCEPT SELECT row_to_json(t)::text FROM {right} t LIMIT 20"
            ),
        ),
        (
            "right",
            format!(
                "SELECT row_to_json(t)::text FROM {right} t \
                 EXCEPT SELECT row_to_json(t)::text FROM {left} t LIMIT 20"
            ),
        ),
    ] {
        let rows: Vec<(String,)> = sqlx::query_as(&query).fetch_all(&pool).await?;
        for (row,) in rows {
            println!("{direction}\t{row}");
        }
    }
    Ok(())
}

/// 两张表逐行对照的条数：只在左边、只在右边的行数。摘要是「一样不一样」，这两个数
/// 是「差多少」；换表分三段的那道门读的是后者。
pub(crate) async fn diff_counts(
    pool: &sqlx::PgPool,
    left: &str,
    right: &str,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let (left, right) = (full_ident(left), full_ident(right));
    let (only_left,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*)::BIGINT FROM (\
            SELECT row_to_json(t)::text AS row FROM {left} t \
            EXCEPT SELECT row_to_json(t)::text FROM {right} t) x"
    ))
    .fetch_one(pool)
    .await?;
    let (only_right,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*)::BIGINT FROM (\
            SELECT row_to_json(t)::text AS row FROM {right} t \
            EXCEPT SELECT row_to_json(t)::text FROM {left} t) x"
    ))
    .fetch_one(pool)
    .await?;
    Ok((only_left, only_right))
}

/// 表名只允许小写字母、数字与下划线，可带一个 schema 前缀：拼进 SQL 的必须是这两个
/// 形状之一，别的直接拒——这一层不接受任意标识符。
pub(crate) fn checked_table(raw: &str) -> Result<String, String> {
    let parts: Vec<&str> = raw.split('.').collect();
    let valid = parts.iter().all(|part| {
        !part.is_empty()
            && part
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    });
    if !valid || parts.len() > 2 {
        return Err(format!("`{raw}` is not a table name"));
    }
    Ok(raw.to_string())
}

pub(crate) fn full_ident(table: &str) -> String {
    table
        .split('.')
        .map(|part| format!("\"{part}\""))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_table_names_are_accepted() {
        assert!(checked_table("cogneva_event").is_ok());
        assert!(checked_table("public.cogneva_event").is_ok());
        assert!(checked_table("cogneva_event; DROP TABLE x").is_err());
        assert!(checked_table("Cogneva").is_err());
        assert!(checked_table("a.b.c").is_err());
        assert_eq!(full_ident("public.x"), "\"public\".\"x\"");
    }
}

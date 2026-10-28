//! 把一个名字写进 SQL 时的那一层引号，以及量一条关系的那两把尺子（行数与字节）。
//!
//! 这个平台上不止一个 crate 会把自己的表名拼进语句：容量清扫器量行数、量字节，
//! 分区维护器按表名建分区，可观测面的交换归档清扫自己那张表。每处都自己写一遍
//! 「加引号」，就是每处一份拼法——而这份拼法有两个真正会分叉的地方，且分叉了
//! 不会让任何一处失败：
//!
//! - **带点号的名字**。`schema.table` 是两个标识符，不是一个。整串加引号得到的是
//!   一张名叫 `schema.table` 的表，它在库里不存在，于是 `SELECT count(*)` 报错或
//!   返回空——「这张表没有行」与「这个名字根本不是那张表」在读数上同形。
//! - **名字里本来就有的引号字符**。转义（`"` → `""`）与过滤（把非字母数字字符
//!   丢掉）都能得到「能跑的 SQL」，但过滤是把名字改成了另一个名字：`a"b` 变成
//!   `ab`，语句照样执行，读的是一个不同的关系。
//!
//! 所以拼法只有一份，放在契约层：调用方在 cog-storage 与 cog-observability 两边，
//! 两边读的是同一个库、同一个面板上的读数，名字的拼法没有理由有两份。转义而不是
//! 过滤——这条选择是为了让「名字里有奇怪字符」这件事以一条 SQL 报错出现，而不是
//! 以「读到另一个关系的零行」出现。

/// 把一个关系名引成 SQL 标识符，允许 `schema.table` 这种限定写法。
///
/// 点号分段，每一段各自加引号并把段内已有的引号翻倍。段内其余字符原样保留：
/// 它们要么本来就合法，要么会被数据库自己拒绝——两种结局都比悄悄换一个名字好。
pub fn quote_relation(name: &str) -> String {
    name.split('.')
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

/// 一条关系里有多少行。
///
/// 这是容量面与归档面共用的那把尺子：三个容量清扫器、交换归档清扫与用量账清扫
/// 都按它读「这张表还剩多少」，读到的数并排出现在同一个面板上。各写一份的时候，
/// 分叉不会让任何一处失败——有人加一个 `WHERE`（只数某一段时间）、把 `count(*)`
/// 换成估算、或忘了引号，面板上只是那一条线的量纲悄悄变了，「这张表比那张表长得
/// 快」这句话从此不成立，且没有任何东西会说不成立。
///
/// 拼法本身没有参数以外的自由度：表名经 [`quote_relation`] 加引号，其余一个字
/// 都不许改。要数别的（默认分区、带条件的子集），那是另一个问题，写在自己那里
/// 并说明为什么不是这一把尺子。
pub fn relation_rows_sql(relation: &str) -> String {
    format!("SELECT count(*) FROM {}", quote_relation(relation))
}

/// 一条关系在盘上占多少字节：索引与 TOAST 都算在内。
///
/// 与 [`relation_rows_sql`] 同一个理由，另加一处更容易分叉的地方：**表不存在时
/// 会发生什么**。`to_regclass($1)` 把不认识的名字读成 NULL，于是
/// `pg_total_relation_size(NULL)` 也是 NULL——调用方拿到的是「没有这个数」，可以
/// 把名字写进错误里说清楚少的是哪张表。`$1::regclass` 在同一种情形下抛的是数据库
/// 自己的错，读数是同一件事，说出来的话却不是。两边量的既然是同一张表，缺表这件
/// 事的形状也只能有一种：所以这里用 `to_regclass`，调用方按 `Option<i64>` 解，
/// 拿到 NULL 时自己给出带名字的错误。
pub const RELATION_BYTES_SQL: &str = "SELECT pg_total_relation_size(to_regclass($1))";

/// 一条关系里最老的一行有多旧，单位秒；空表读成 NULL。
///
/// 与上面两把尺子同一族：任务事件归档、交换归档与用量账各发布一条「最新的东西
/// 排到多久以前」，三条线并排比着看，比的是一场保留策略在这张表上还剩多少真。
/// 时间列各表不同（写入口起的名），所以它是参数；除此之外一个字都不许改——
/// 换成 `avg`、加一个 `WHERE`、把 NULL 换成 0（0 秒是「刚刚」），三条线就不再
/// 量同一件事，而它们各自照旧读得通。
///
/// NULL 留给调用方：空表没有最老一行，那不是一个年龄，也不该被算成一个。
pub fn relation_oldest_age_sql(relation: &str, time_column: &str) -> String {
    format!(
        "SELECT EXTRACT(EPOCH FROM (NOW() - min({})))::bigint AS age FROM {}",
        quote_relation(time_column),
        quote_relation(relation)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_name_is_one_quoted_identifier() {
        assert_eq!(quote_relation("cog_checkpoints"), "\"cog_checkpoints\"");
    }

    #[test]
    fn a_schema_qualified_name_is_quoted_in_parts() {
        assert_eq!(
            quote_relation("public.cog_events"),
            "\"public\".\"cog_events\""
        );
    }

    /// 名字里的引号是转义出来的，不是被丢掉的：丢掉的后果是语句照样跑，读的是
    /// 另一个关系，而两边的读数都正常。
    #[test]
    fn a_name_carrying_a_quote_is_escaped_not_rewritten() {
        assert_eq!(quote_relation("a\"b"), "\"a\"\"b\"");
        assert_ne!(quote_relation("a\"b"), "\"ab\"");
    }

    /// 行数那一把尺子除了表名之外没有自由度：同一个名字每次都读出同一句，限定名
    /// 按段加引号。少一个引号或多一个 `WHERE`，都让这条线量的是另一个数。
    #[test]
    fn the_row_count_reads_one_relation_and_nothing_else() {
        assert_eq!(
            relation_rows_sql("cog_checkpoints"),
            "SELECT count(*) FROM \"cog_checkpoints\""
        );
        assert_eq!(
            relation_rows_sql("archive.cog_events"),
            "SELECT count(*) FROM \"archive\".\"cog_events\""
        );
    }

    /// 年龄那把尺子只换时间列，其余照旧：列名也走同一层引号。
    #[test]
    fn the_age_reading_varies_only_in_the_time_column() {
        assert_eq!(
            relation_oldest_age_sql("cog_task_events", "created_at"),
            "SELECT EXTRACT(EPOCH FROM (NOW() - min(\"created_at\")))::bigint AS age \
             FROM \"cog_task_events\""
        );
        // 同一个问题在另一张表上只差名字：语句的其余部分是同一串字符。
        let a = relation_oldest_age_sql("cog_task_events", "created_at");
        let b = relation_oldest_age_sql("gateway_llm_exchange", "ts");
        assert_eq!(
            a.replace("\"cog_task_events\"", "")
                .replace("\"created_at\"", ""),
            b.replace("\"gateway_llm_exchange\"", "")
                .replace("\"ts\"", "")
        );
    }

    /// 缺表是一个值（NULL），不是数据库自己的异常：调用方要能在错误里点出名字。
    #[test]
    fn the_byte_reading_treats_a_missing_relation_as_a_value() {
        assert!(RELATION_BYTES_SQL.contains("to_regclass($1)"));
        assert!(!RELATION_BYTES_SQL.contains("$1::regclass"));
    }
}

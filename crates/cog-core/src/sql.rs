//! 把一个名字写进 SQL 时的那一层引号。
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
}

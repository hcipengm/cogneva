//! principle: feedback_no_hardcoding_config_surface
//!
//! 「这条关系还剩多少行、占多少字节」是同一把尺子，被两个 crate 各写了一遍：
//! cog-storage 的三个容量清扫器与任务事件归档、cog-observability 的交换归档与
//! 用量账清扫、cogneva 的备份与恢复核对。它们读到的数并排出现在同一张面板上，
//! 而五份拼法之间没有一处会比较它们。
//!
//! 分叉不会让任何一处失败：有人给其中一份加一个 `WHERE`（只数某一分区）、把
//! `count(*)` 换成估算、或者换掉引号写法，面板上只是那一条线的量纲悄悄变了。
//! 于是「这张表比那张表长得快」这句话从此不成立，而没有任何东西会说不成立——
//! 每一条线自己都读得通，只是它们不再量同一件事。
//!
//! 字节这一把还多一处更容易分叉的地方：**表不存在时会发生什么**。`to_regclass($1)`
//! 把不认识的名字读成 NULL，调用方拿到的是「没有这个数」，可以把名字写进错误；
//! `$1::regclass` 在同一种情形下抛数据库自己的错。两边量的是同一张表，缺表这件事
//! 说出来的话也只能有一种，所以拼法只有一份：`cog_core::sql` 里的
//! `relation_rows_sql` 与 `RELATION_BYTES_SQL`。
//!
//! 本门盯的是它不再被抄回去：生产行里，`pg_total_relation_size(` 只许出现在声明处；
//! 把表名插进 `count(*)` 的那种语句（`FROM {…}`）也只许出现在声明处，除了一处
//! 登记在案的「同形不同物」——默认分区的积压。
//!
//! 判不了什么：
//!
//! - `SELECT count(*) … WHERE …` 这类带条件的计数。它们答的是另一个问题（某段时间
//!   里的行数），本门没有题目。
//! - 语句跑起来之后怎么解码（`query_scalar` 还是取别名字段）。那是每个调用点自己
//!   的事，与量哪件事无关。
//! - 别的库函数（`pg_relation_size`、`pg_table_size`）。它们今天不在树上；等有人
//!   第一次写进来，那才是需要一次判断的地方。

mod common;

use std::path::Path;

use common::routes::{repo_root, walk};
use common::rust_production::production_lines;

/// 这两把尺子唯一允许被写成 SQL 的地方。
const DECLARED_IN: &str = "crates/cog-core/src/sql.rs";

/// 普查下限。测到的那一天 `crates/*/src` 有 700 余个源文件。
const MIN_FILES: usize = 300;

/// 登记在案的「同形不同物」：同一个 `count(*)` 形状，答的是另一件事。
const REGISTERED_OTHER_QUESTIONS: &[(&str, &str)] = &[(
    "crates/cog-storage/src/partition_maintainer.rs",
    "`FROM ONLY {default}` 数的是默认分区本身——不在任何月度区间里的那些行——\
     要的答案是「窗口是不是落后了」，不是这一族里的普通表有多少行",
)];

/// 把表名插进 `count(*)` 的语句行数。
///
/// 认三种插法：`FROM {}`、`FROM \"{}\"`（自己套引号，另一份拼法——注意这句源码里
/// 引号前面真的有一个反斜杠，照着写才认得出来）、`FROM ONLY {}`。
/// 带 `WHERE` 的计数不在此列：它答的不是「这张表有多少行」。
fn row_counts(text: &str) -> usize {
    production_lines(text)
        .into_iter()
        .filter(|line| line.contains("count(*)"))
        .filter(|line| {
            line.contains("FROM {") || line.contains("FROM ONLY {") || line.contains(r#"FROM \"{"#)
        })
        .count()
}

/// 量字节的语句行数。
fn byte_reads(text: &str) -> usize {
    production_lines(text)
        .into_iter()
        .filter(|line| line.contains("pg_total_relation_size("))
        .count()
}

/// 每个文件里这两把尺子的落点。
fn census(root: &Path) -> (usize, Vec<(String, usize, usize)>) {
    let mut files = Vec::new();
    for crate_dir in std::fs::read_dir(root.join("crates")).expect("crates/") {
        let path = crate_dir.expect("a crate directory").path();
        walk(&path.join("src"), &["rs"], &mut files);
    }
    files.sort();
    let mut sites = Vec::new();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let rows = row_counts(&text);
        let bytes = byte_reads(&text);
        if rows > 0 || bytes > 0 {
            let file = path
                .strip_prefix(root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            sites.push((file, rows, bytes));
        }
    }
    (files.len(), sites)
}

#[test]
fn a_relation_is_measured_with_one_statement_wherever_it_is_measured() {
    let (files, sites) = census(&repo_root());
    assert!(
        files >= MIN_FILES,
        "只读到 {files} 个源文件（下限 {MIN_FILES}），这条判据没有题目"
    );

    let declared = sites.iter().find(|(file, _, _)| file == DECLARED_IN);
    let declared = declared.unwrap_or_else(|| {
        panic!("{DECLARED_IN} 里这两把尺子一条都没有了：它们被搬走或改名，本门没有题目")
    });
    assert!(
        declared.1 > 0 && declared.2 > 0,
        "{DECLARED_IN} 少了一把尺子（行数 {} 处、字节 {} 处）：两把都是契约的一部分",
        declared.1,
        declared.2
    );

    let allowed: Vec<&str> = REGISTERED_OTHER_QUESTIONS
        .iter()
        .map(|(file, _)| *file)
        .collect();
    let mut findings = Vec::new();
    for (file, rows, bytes) in &sites {
        if file == DECLARED_IN {
            continue;
        }
        if *bytes > 0 {
            findings.push(format!(
                "  {file}: {bytes} 处 pg_total_relation_size，量的是字节"
            ));
        }
        if *rows > 0 && !allowed.contains(&file.as_str()) {
            findings.push(format!(
                "  {file}: {rows} 处把表名插进 count(*)，量的是行数"
            ));
        }
    }

    assert!(
        findings.is_empty(),
        "关系大小被按另一套拼法量了一遍。每一份都能跑，所以没有任何东西会失败：\
         它们只是让面板上的某一条线换了量纲，于是「这张表比那张表长得快」不再成立，\
         而「它们不再量同一件事」本身没有读数。收拢的去处是 \
         `cog_core::sql::relation_rows_sql` 与 `cog_core::sql::RELATION_BYTES_SQL`。\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_census_reads_every_spelling_of_an_injected_name_and_nothing_from_prose() {
    // 三种插法都是第二处：自己套引号的那一种最容易漏，因为它根本不用那层引号函数。
    assert_eq!(
        row_counts(r#"let s = format!("SELECT count(*) FROM {}", n);"#),
        1
    );
    assert_eq!(
        row_counts(r#"let s = format!("SELECT count(*) FROM \"{}\"", n);"#),
        1
    );
    assert_eq!(
        row_counts(r#"let s = format!("SELECT count(*) AS n FROM {LEDGER_TABLE}");"#),
        1
    );
    assert_eq!(
        row_counts(r#"let s = format!("SELECT count(*) FROM ONLY {default}");"#),
        1
    );

    // 带条件的计数答的是另一个问题，不是这一把尺子。
    assert_eq!(
        row_counts(r#"let s = "SELECT count(*) FROM alerts WHERE fired = true";"#),
        0
    );
    // 注释里提到它不算第二处。
    assert_eq!(row_counts("// SELECT count(*) FROM {}\nlet a = 1;\n"), 0);

    assert_eq!(
        byte_reads(r#"let s = "SELECT pg_total_relation_size(to_regclass($1))";"#),
        1
    );
    assert_eq!(
        byte_reads(r#"let s = "SELECT pg_total_relation_size($1::regclass)";"#),
        1
    );
    assert_eq!(
        byte_reads("// pg_total_relation_size 以前在这里\nlet a = 1;\n"),
        0
    );
}

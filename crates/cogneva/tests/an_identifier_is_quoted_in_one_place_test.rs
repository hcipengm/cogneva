//! principle: feedback_no_hardcoding_config_surface
//!
//! 「把一个名字包进 SQL 双引号」这件事，树上曾经有三份实现，其中两份同名
//! （`quote_ident`）而不同义：一份把整串引起来并把里面的引号翻倍，另一份按点号
//! 分段、把字母表外的字符丢掉。两份都「能跑」，于是没有任何东西会失败。
//!
//! 分叉点有两个，都不响：
//!
//! - **点号**。`schema.table` 是两个标识符。整串加引号得到一张名叫
//!   `schema.table` 的表，它不存在——「这张表没有行」与「这个名字不是那张表」
//!   在读数上同形。
//! - **名字里本来就有的引号**。转义是把它写对，过滤是把它改掉：`a"b` 变成 `ab`，
//!   语句照样执行，读的是一个不同的关系。
//!
//! 这两个函数的名字还一样，而调用点分在两个 crate（cog-storage 的容量清扫与分区
//! 维护、cog-observability 的交换归档）。从一个文件抄一行调用到另一个文件，得到的
//! 是另一种语义，编译不报错、测试不报错。
//!
//! 所以拼法只有一份：`cog_core::sql::quote_relation`。本门管的是「树上不再长出
//! 第二份」——普查 `crates/*/src` 的生产行，任何 `fn quote…` 定义都必须在这张
//! 登记表上，且登记表里的每一行都必须真在树上（没有过期的登记）。
//!
//! 登记表而不是白名单：一个与 SQL 无关的 `quote_*` 是正当的，它需要的是一条写明
//! 「为什么它不算同一个拼法」的登记行，而不是被本门当成违规。今天生产代码里只有
//! 一份，所以登记表只有一行——但正因为它是一张表，下一个正当的 `quote_*` 出现时
//! 是被登记，而不是被这条判据判成错误。
//!
//! 判不了什么：
//!
//! - 不走函数、直接把 `format!("\"{}\"", name)` 写在语句旁边的拼法。本门按定义判，
//!   不按语义判。
//! - 拼法本身对不对（点号分段、引号翻倍）。那由 `cog_core::sql` 自己的单测覆盖。

mod common;

use std::path::Path;

use common::routes::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

/// 允许存在的 `quote…` 函数：文件、函数名、以及它为什么不是同一份拼法。
const REGISTERED: &[(&str, &str, &str)] = &[(
    "crates/cog-core/src/sql.rs",
    "quote_relation",
    "把关系名写进 SQL 的那一份——树上唯一的拼法，别的 crate 引用它",
)];

/// 一处定义：文件与该文件的函数名。
type Site = (String, String);

/// 生产行里每个 `fn quote…` 定义。
fn quoters_in(file: &str, text: &str) -> Vec<Site> {
    let mut found = Vec::new();
    for line in production_lines(text) {
        let Some(pos) = line.find("fn quote") else {
            continue;
        };
        let name: String = line[pos + "fn ".len()..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.len() > "quote".len() {
            found.push((file.to_string(), name));
        }
    }
    found
}

fn census(root: &Path) -> Vec<Site> {
    let files = production_sources(root);
    let mut found = Vec::new();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, path);
        found.extend(quoters_in(&file, &text));
    }
    found
}

#[test]
fn a_relation_name_is_quoted_in_one_place() {
    let found = census(&repo_root());
    let unregistered: Vec<&Site> = found
        .iter()
        .filter(|(file, name)| !REGISTERED.iter().any(|(f, n, _)| f == file && n == name))
        .collect();
    let stale: Vec<&(&str, &str, &str)> = REGISTERED
        .iter()
        .filter(|(f, n, _)| !found.iter().any(|(file, name)| file == f && name == n))
        .collect();

    assert!(
        unregistered.is_empty() && stale.is_empty(),
        "树上的 `quote…` 函数必须与登记表一一对应。\n\
         没登记的：{unregistered:?}\n\
         登记了但树上没有的：{stale:?}\n\
         把名字拼进 SQL 有唯一一份实现 `cog_core::sql::quote_relation`（点号分段、\
         段内引号翻倍）；新的一处要么改成引用它，要么在 `REGISTERED` 里加一行\
         说明为什么它不是同一份拼法。"
    );

    assert_eq!(
        found
            .iter()
            .filter(|(file, name)| file == "crates/cog-core/src/sql.rs" && name == "quote_relation")
            .count(),
        1,
        "唯一那份拼法不见了：{found:?}"
    );
}

#[test]
fn the_census_counts_a_second_definition_and_reads_nothing_out_of_prose() {
    let one = "pub(crate) fn quote_relation(name: &str) -> String {\n    name.to_string()\n}\n";
    assert_eq!(quoters_in("a.rs", one).len(), 1);

    // 第二处就是第二处，哪怕签名一模一样、哪怕在不同的文件里。
    let two = format!("{one}{}", one.replace("quote_relation", "quote_ident"));
    let names: Vec<String> = quoters_in("b.rs", &two)
        .into_iter()
        .map(|(_, name)| name)
        .collect();
    assert_eq!(names, vec!["quote_relation", "quote_ident"]);

    // 注释里提到这个函数不是一处定义。
    let prose = "// 这里以前有一份 fn quote_ident，已经收进 cog_core::sql\nlet a = 1;\n";
    assert_eq!(quoters_in("c.rs", prose).len(), 0);
}

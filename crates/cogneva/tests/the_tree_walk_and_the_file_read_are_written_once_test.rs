//! 「怎么读一个文件」「怎么走一棵树」各只许有一份实现。
//!
//! 这两条规则短到看起来不值得共享，所以它们在夹具里被抄了十二遍和两遍——连同那句
//! 解释「读不出来就 panic」的注释一起抄。抄的人不是懒：每个测试二进制都是一棵独立的
//! crate，`common` 不是自动可见的，手边最快的一条路就是复制。
//!
//! 代价落在**改的时候**：`routes::walk` 里那句「读不出来就 panic」是这条规则的全部
//! 价值（跳过一个读不出来的子目录不会让任何断言变红，剩下的那份普查往往仍高于下限，
//! 「少读了一块」与「那一块都合规」在判词上分不开），而抄件里那句话是注释，不是机制。
//! 谁要是在其中一份里把 `unwrap_or_else(panic!)` 换成 `flatten()` 或 `else { continue }`，
//! 另外十三份不会跟着变，也不会有任何东西发现它们已经不同——这正是这一族缺陷的样子：
//! **同一份规则被写多遍，而没有任何东西比较它们**。
//!
//! 判据按**签名**判，不按出现次数：被抄的那两份的形状是固定的——
//! `fn read(path: &Path) -> String` 与
//! `fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>)`。
//! 这比「凡 `read_dir` 即红」窄得多，是量过的：`crates/*/tests` 里有三十来份夹具自己
//! 走目录，其中绝大多数是不同形状（只下一层、带着自己的过滤与解析、返回相对名字），
//! 那些不是这条规则的副本。这里只抓逐字抄过去的那一种。
//!
//! 三条断言，各自单独会长回来：副本为零、共享模块还在真干活、还有人在用它。

mod common;

use std::path::PathBuf;

use common::routes::repo_root;
use common::rust_production::relative_name;

/// 两条规则的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/routes.rs";

/// 被抄走的那两个签名，逐字。
const COPIED_SIGNATURES: [&str; 2] = [
    "fn read(path: &Path) -> String",
    "fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>)",
];

/// 这条门自己。它把两个签名当字符串写着（判据要看的就是那两行），所以在真树上会被
/// 自己点名——**只摘这一面**：下面要求它至少还认得出自己的两份标本，判据哪天退化成
/// 什么都认不出，这一条先红，而不是让摘除悄悄掩盖掉一个瞎了的判据。
const THE_PREDICATE: &str =
    "crates/cogneva/tests/the_tree_walk_and_the_file_read_are_written_once_test.rs";

/// 普查面：`crates/**/tests/**/*.rs`，今天 316 份。
///
/// 不做 `src/` 是量过的、不是漏的：`common/routes.rs` 里有一句
/// `pub use super::root::repo_root;`，它要求这两份编在同一个父模块下。`tests/` 的
/// 父模块是 `common/mod.rs`，天然满足；`src/` 里的那个父模块今天只有一个名字
/// （`checkout`，指的是 `root.rs`），把 `routes` 也挂进去要连带改根的挂法，而它服务
/// 的是全仓七十多处 `crate::checkout::repo_root()`。`src/` 里唯一的同形物是
/// `startup_guard.rs` 单测模块里那三行 shim，留作下一步；判据只覆盖夹具，
/// 覆盖不到的地方在这行写清楚，而不是让「全仓只有一处」瞒着它。

/// 引用共享读取的文件下限。今天 17 份，取一半。
const MIN_READERS: usize = 8;

/// 引用共享遍历的文件下限。今天 2 份——真数，不是估的；`walk` 只服务那两处「按扩展名
/// 收一棵子树」的普查，别的夹具走的是别的形状。这个数再往下掉就说明有人在搬走它。
const MIN_WALKERS: usize = 2;

/// 一次普查：副本、读文件数、引用 `read` 的文件数、引用 `walk` 的文件数。
struct Census {
    copies: Vec<(String, usize, String)>,
    files: usize,
    readers: usize,
    walkers: usize,
}

fn census(root: &std::path::Path, files_in: &[PathBuf]) -> Census {
    let mut copies = Vec::new();
    let mut files = 0usize;
    let (mut readers, mut walkers) = (0usize, 0usize);
    for path in files_in {
        let name = relative_name(root, path);
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        files += 1;
        if name != THE_ONE {
            if text.contains("routes::read") || text.contains("routes::{read") {
                readers += 1;
            }
            if text.contains("routes::walk") || text.contains("routes::{read, walk") {
                walkers += 1;
            }
        }
        for (index, line) in text.lines().enumerate() {
            // 注释行跳过：说明里写着这个签名，不等于又抄了一份。
            if line.trim_start().starts_with("//") {
                continue;
            }
            if name != THE_ONE
                && COPIED_SIGNATURES
                    .iter()
                    .any(|signature| line.contains(signature))
            {
                copies.push((name.clone(), index + 1, line.trim().to_string()));
            }
        }
    }
    Census {
        copies,
        files,
        readers,
        walkers,
    }
}

/// `crates/` 下每一个 `.rs`，不设下限——调用点自己带下限，也给假树留一条路。
fn all_files(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    common::routes::walk(root.join("crates").as_path(), &["rs"], &mut out);
    out.retain(|path| relative_name(root, path).contains("/tests/"));
    out.sort();
    out
}

#[test]
fn the_tree_walk_and_the_file_read_are_written_once() {
    let root = repo_root();
    let found = census(&root, &all_files(&root));

    // 下限落在题目上：读不到文件时，「副本为零」是一句关于空集的话。
    assert!(
        found.files >= 250,
        "只读到 {} 个夹具源文件（下限 250）：遍历没走到，下面的「没有副本」不算数",
        found.files
    );
    // 摘掉这份门自己之前先问一句判据还认不认得出它。判据若是瞎的，下面的清单为空
    // 与「全仓只剩一处」同形——那正是这条门存在的理由。
    let own: Vec<&(String, usize, String)> = found
        .copies
        .iter()
        .filter(|(file, _, _)| file == THE_PREDICATE)
        .collect();
    assert!(
        own.len() >= 2,
        "这条门自己至少写着两份标本（两个签名各一行），读到 {} 处：{:?}。\
         一处都读不到说明判据已经瞎了，那么把它摘掉就只是在掩盖它",
        own.len(),
        own
    );

    let elsewhere: Vec<&(String, usize, String)> = found
        .copies
        .iter()
        .filter(|(file, _, _)| file != THE_PREDICATE)
        .collect();
    assert!(
        elsewhere.is_empty(),
        "这些地方又抄了一份共享的读取 / 遍历：{elsewhere:?}\n\
         改用 `common::routes::read` 与 `common::routes::walk`。抄件的注释里也写着\
         「读不出来就 panic」，但那是注释不是机制——把它换成 `else {{ continue }}` \
         的那一份不会红，也不会有人发现另外几份已经和它不一样了。"
    );

    // 反过来问：被指认的那一处真的还在干这件事吗。少了这一问，把它换成空壳之后
    // 本判据照绿——它只证明了「别处没有」。
    let the_one = std::fs::read_to_string(root.join(THE_ONE)).expect("共享遍历读得到");
    assert!(
        the_one.contains("pub fn read(path: &Path) -> String")
            && the_one.contains("extensions: &[&str]"),
        "{THE_ONE} 里不再定义这两个函数：那么「只有一处」就是指着这份不干这件事的文件"
    );
    assert!(
        the_one.contains("panic!") && the_one.contains("read_dir"),
        "{THE_ONE} 里的遍历不再 panic：读不出来时安静地少读一块，判出来的结论与\
         「那一块都合规」同形"
    );

    assert!(
        found.readers >= MIN_READERS,
        "只有 {} 份夹具在用共享的 `read`（下限 {MIN_READERS}）：规则定义了一处，却几乎\
         没人读它——那与没定义同形",
        found.readers
    );
    assert!(
        found.walkers >= MIN_WALKERS,
        "只有 {} 份夹具在用共享的 `walk`（下限 {MIN_WALKERS}）：遍历被搬走了，\
         或者被绕开了",
        found.walkers
    );
}

/// 判据自检：真搭一棵树，把各种写法放进去，看它认不认得出。
///
/// 只断真树的那一条证明不了它还会响——真树今天恰好干净，而干净与瞎同形。
#[test]
fn a_copy_on_disk_is_reported() {
    let root = std::env::temp_dir().join(format!("census-read-walk-{}", std::process::id()));
    let dir = root.join("crates/probe/tests");
    std::fs::create_dir_all(&dir).expect("a scratch tests directory");

    std::fs::write(
        dir.join("consumer.rs"),
        "mod common;\nuse common::routes::{read, walk};\nfn f(p: &std::path::Path) { let _ = read(p); }\n",
    )
    .expect("a consumer");
    let found = census(&root, &all_files(&root));
    assert!(found.copies.is_empty(), "借共享的那一份不该被点名");
    assert_eq!(found.readers, 1, "读文件的人要数得出来");
    assert_eq!(found.walkers, 1, "走目录的人要数得出来");

    // 只在注释里写着签名：不是副本。
    std::fs::write(
        dir.join("mention.rs"),
        "// fn read(path: &Path) -> String { std::fs::read_to_string(path).unwrap() }\npub const X: u32 = 1;\n",
    )
    .expect("a mention");
    let found = census(&root, &all_files(&root));
    assert!(
        found.copies.is_empty(),
        "注释里的签名不该算成副本：{:?}",
        found.copies
    );

    // 逐字抄过去的那两份，都要点名，且要带能翻到的行号。
    std::fs::write(
        dir.join("copy.rs"),
        "fn read(path: &Path) -> String {\n    std::fs::read_to_string(path).unwrap()\n}\n\nfn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {\n    let _ = (dir, extensions, out);\n}\n",
    )
    .expect("two copies");
    let found = census(&root, &all_files(&root));
    assert_eq!(
        found.copies.len(),
        2,
        "两份抄件都要数得出来：{:?}",
        found.copies
    );
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 5),
        "报出来的位置要能直接翻到（`walk` 那一处在第 5 行）：{:?}",
        found.copies
    );

    std::fs::remove_dir_all(&root).ok();
}

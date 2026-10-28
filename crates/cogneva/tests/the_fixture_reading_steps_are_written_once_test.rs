//! 「怎么读一个文件」「怎么走一棵树」「怎么去掉注释」各只许有一份实现。
//!
//! 这三条规则短到看起来不值得共享，所以它们在夹具里被抄了十二遍、两遍和六遍——连同
//! 那句解释「读不出来就 panic」的注释一起抄。抄的人不是懒：每个测试二进制都是一棵
//! 独立的 crate，`common` 不是自动可见的，手边最快的一条路就是复制。去注释那一条抄得
//! 更没声：六份里三种写法（按字节 / 按字符 / 数嵌套层数），谁抄的都不算错，错的是
//! 它们从此各走各的——Rust 的块注释**可以嵌套**，数层数的那一份与只看第一个 `*/`
//! 的那一份，对同一段源码给出的「代码」不是同一段。
//!
//! 代价落在**改的时候**：`fsio::walk` 里那句「读不出来就 panic」是这条规则的全部
//! 价值（跳过一个读不出来的子目录不会让任何断言变红，剩下的那份普查往往仍高于下限，
//! 「少读了一块」与「那一块都合规」在判词上分不开），而抄件里那句话是注释，不是机制。
//! 谁要是在其中一份里把 `unwrap_or_else(panic!)` 换成 `flatten()` 或 `else { continue }`，
//! 另外十三份不会跟着变，也不会有任何东西发现它们已经不同——这正是这一族缺陷的样子：
//! **同一份规则被写多遍，而没有任何东西比较它们**。
//!
//! 判据按**签名**判，不按出现次数：被抄的那几份的形状是固定的——
//! `fn read(path: &Path) -> String`、`fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>)`
//! 与 `fn strip_comments(text: &str) -> String`。
//! 这比「凡 `read_dir` 即红」窄得多，是量过的：夹具与产出面里有几十份文件自己走目录，
//! 其中绝大多数是不同形状（只下一层、带着自己的过滤与解析、返回相对名字），那些不是
//! 这条规则的副本。这里只抓逐字抄过去的那一种。
//!
//! 断言各自单独会长回来：副本为零、共享模块还在真干活、还有人在用它。

mod common;

use std::path::PathBuf;

use common::routes::repo_root;
use common::rust_production::relative_name;

/// 读文件、走目录这两条的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/fsio.rs";

/// 去注释那一条的定义在 `routes` 里——它要读同一批源码，和注册路线的读法住在一起。
const STRIP_COMMENTS_HOME: &str = "crates/cogneva/tests/common/routes.rs";

/// 被抄走的签名，逐字。前两个在 `fsio.rs`，第三个在 `routes.rs`。
const COPIED_SIGNATURES: [&str; 3] = [
    "fn read(path: &Path) -> String",
    "fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>)",
    "fn strip_comments(text: &str) -> String",
];

/// 这条门自己。它把签名当字符串写着（判据要看的就是那几行），所以在真树上会被
/// 自己点名——**只摘这一面**：下面要求它至少还认得出自己的几份标本，判据哪天退化成
/// 什么都认不出，这一条先红，而不是让摘除悄悄掩盖掉一个瞎了的判据。
const THE_PREDICATE: &str =
    "crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs";

// 普查面：`crates/**/*.rs`，夹具与产出面一起算。
//
// `src/` 必须在里面：这一族抄件上一轮就长在 `src/` 的单测模块里（`startup_guard.rs`
// 自己那三行 `read`），而只扫夹具的判据对它是瞎的。`fsio.rs` 不 import 任何兄弟
// 模块，正是为了让 `src/` 也能按 `#[path]` 编它——判据能覆盖 `src/`，前提是那份
// 规则本身挂得过去。

/// 引用共享读取的文件下限。今天 18 份，取一半。
const MIN_READERS: usize = 8;

/// 引用共享遍历的文件下限。今天 2 份——真数，不是估的；`walk` 只服务那两处「按扩展名
/// 收一棵子树」的普查，别的夹具走的是别的形状。这个数再往下掉就说明有人在搬走它。
const MIN_WALKERS: usize = 2;

/// 引用共享去注释的文件下限。今天 15 份。
const MIN_STRIPPERS: usize = 8;

/// 一次普查：副本、读文件数、三种消费者各几份。
struct Census {
    copies: Vec<(String, usize, String)>,
    files: usize,
    readers: usize,
    walkers: usize,
    strippers: usize,
}

/// 这份文件从哪个 `::{…}` 里导入了哪些名字。
///
/// 跨行的导入也要读得到：夹具里常见的是
/// `use common::routes::{read, repo_root, strip_comments, walk};` 折成两三行。
fn imported_names(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("::{") {
        let start = cursor + at + 3;
        let Some(len) = text[start..].find('}') else {
            break;
        };
        for part in text[start..start + len].split(',') {
            let word = part.trim();
            if !word.is_empty() {
                out.push(word.to_string());
            }
        }
        cursor = start + len;
    }
    out
}

fn census(root: &std::path::Path, files_in: &[PathBuf]) -> Census {
    let mut copies = Vec::new();
    let mut files = 0usize;
    let (mut readers, mut walkers, mut strippers) = (0usize, 0usize, 0usize);
    let shared = [THE_ONE, STRIP_COMMENTS_HOME];
    for path in files_in {
        let name = relative_name(root, path);
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        files += 1;
        let imported = imported_names(&text);
        if name != THE_ONE {
            if imported.contains(&"read".to_string()) || text.contains("routes::read") {
                readers += 1;
            }
            if imported.contains(&"walk".to_string()) || text.contains("routes::walk") {
                walkers += 1;
            }
        }
        if !shared.contains(&name.as_str()) && text.contains("strip_comments") {
            strippers += 1;
        }
        for (index, line) in text.lines().enumerate() {
            // 注释行跳过：说明里写着这个签名，不等于又抄了一份。
            if line.trim_start().starts_with("//") {
                continue;
            }
            if !shared.contains(&name.as_str())
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
        strippers,
    }
}

/// `crates/` 下每一个 `.rs`，不设下限——调用点自己带下限，也给假树留一条路。
fn all_files(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    common::routes::walk(root.join("crates").as_path(), &["rs"], &mut out);
    out.sort();
    out
}

#[test]
fn the_fixture_reading_steps_are_written_once() {
    let root = repo_root();
    let found = census(&root, &all_files(&root));

    // 下限落在题目上：读不到文件时，「副本为零」是一句关于空集的话。今天 1075 份。
    assert!(
        found.files >= 800,
        "只读到 {} 个 crate 源文件（下限 800）：遍历没走到，下面的「没有副本」不算数",
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
        own.len() >= 3,
        "这条门自己至少写着三份标本（三个签名各一行），读到 {} 处：{:?}。\
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
        "这些地方又抄了一份共享的读取 / 遍历 / 去注释：{elsewhere:?}\n\
         改用 `common::routes::read`、`common::routes::walk` 与 \
         `common::routes::strip_comments`。抄件的注释里也写着「读不出来就 panic」，\
         但那是注释不是机制——把它换成 `else {{ continue }}` 的那一份不会红，\
         也不会有人发现另外几份已经和它不一样了。"
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
    let strip_home =
        std::fs::read_to_string(root.join(STRIP_COMMENTS_HOME)).expect("共享去注释读得到");
    assert!(
        strip_home.contains("pub fn strip_comments(text: &str) -> String"),
        "{STRIP_COMMENTS_HOME} 里不再定义 `strip_comments`：「只有一处」就是指着这份\
         不干这件事的文件"
    );
    assert!(
        strip_home.contains("depth") && strip_home.contains("chars[i] == '\\n'"),
        "{STRIP_COMMENTS_HOME} 里的去注释不再按嵌套层数走：Rust 的块注释可以嵌套，\
         只看第一个 `*/` 会把后半截注释当成代码读"
    );

    assert!(
        found.readers >= MIN_READERS,
        "只有 {} 份文件在用共享的 `read`（下限 {MIN_READERS}）：规则定义了一处，却几乎\
         没人读它——那与没定义同形",
        found.readers
    );
    assert!(
        found.walkers >= MIN_WALKERS,
        "只有 {} 份文件在用共享的 `walk`（下限 {MIN_WALKERS}）：遍历被搬走了，\
         或者被绕开了",
        found.walkers
    );
    assert!(
        found.strippers >= MIN_STRIPPERS,
        "只有 {} 份文件在用共享的 `strip_comments`（下限 {MIN_STRIPPERS}）：\
         去注释的写法被搬走了，或者被绕开了",
        found.strippers
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
        "mod common;\nuse common::routes::{read, strip_comments, walk};\nfn f(p: &std::path::Path) { let _ = read(p); }\n",
    )
    .expect("a consumer");
    let found = census(&root, &all_files(&root));
    assert!(found.copies.is_empty(), "借共享的那一份不该被点名");
    assert_eq!(found.readers, 1, "读文件的人要数得出来");
    assert_eq!(found.walkers, 1, "走目录的人要数得出来");
    assert_eq!(found.strippers, 1, "去注释的人要数得出来");

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

    // 逐字抄过去的那三份，都要点名，且要带能翻到的行号。
    std::fs::write(
        dir.join("copy.rs"),
        "fn read(path: &Path) -> String {\n    std::fs::read_to_string(path).unwrap()\n}\n\nfn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {\n    let _ = (dir, extensions, out);\n}\n\nfn strip_comments(text: &str) -> String {\n    text.to_string()\n}\n",
    )
    .expect("three copies");
    let found = census(&root, &all_files(&root));
    assert_eq!(
        found.copies.len(),
        3,
        "三份抄件都要数得出来：{:?}",
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
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/tests/copy.rs" && *line == 9),
        "去注释那一份也要点名（第 9 行）：{:?}",
        found.copies
    );

    // 抄件长在 `src/` 的单测模块里也要点名：这一族抄件的上一轮就长在那儿
    // （`startup_guard.rs` 的三行 shim），只扫夹具的判据对它一声不响。
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).expect("a scratch src directory");
    std::fs::write(
        src.join("unit.rs"),
        "fn read(path: &Path) -> String {\n    std::fs::read_to_string(path).unwrap()\n}\n",
    )
    .expect("a copy in a unit test module");
    let found = census(&root, &all_files(&root));
    assert!(
        found
            .copies
            .iter()
            .any(|(file, line, _)| file == "crates/probe/src/unit.rs" && *line == 1),
        "`src/` 里的抄件也要数得出来，否则这份门只看得见夹具：{:?}",
        found.copies
    );

    std::fs::remove_dir_all(&root).ok();
}

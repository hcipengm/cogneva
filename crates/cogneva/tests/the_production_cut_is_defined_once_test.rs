//! 「哪一半是产出代码」只许在一处算出来。
//!
//! 这条切口原先在仓里长着几十份，形状都一样：找到源码里第一处 `#[cfg(test)]`，
//! 切在它前面。测试模块都在文件末尾时这是对的，而只要文件中间有一个被
//! `#[cfg(test)]` 装点的项（一个只给用例换读数来源的 setter、一把只给用例用的锁），
//! 切在那里的那一份就把后面全部产出丢掉。丢掉之后判据不会红：它读到的是「那里
//! 没有这样的调用点」，与「那里本来就没有」是同一句话——而这几百条门存在的理由
//! 恰恰是「没读」与「没问题」不能同形。
//!
//! 实测（2026-10-28）：`mainline_rollout.rs` 中间多出一个 test-only setter 之后，
//! `rollout_resource_reading_contract_test` 读到 0 处 `report_termination_message(`
//! 而不是 3 处，判词却写着「结束的两种结局都留读数」。`mainline_deployer.rs` 里
//! 同样的切法只读前三分之一：它第一处 `#[cfg(test)]` 装点的是单个 helper
//! （`heartbeat_message`），其后还有近两千行产出（`rbac_target`、
//! `build_rollout_bundle`、`ReleaseSet`……）。读它的那几条门今天恰好都还数得对，
//! 只因为它们数的针落在前三分之一里——换一根针就静默归零，而判词不变。
//!
//! 所以这里判三件事，每一件都是单独会长回来的：
//!
//!   - **标记**：把测试边界写成字符串字面量（`"#[cfg(test)]"` / `"mod tests"`）的，
//!     除 `common/rust_production.rs` 外一处都不许有。要用就问那一份，缺投影就给它
//!     加一个——加在那一份里，这件事本身就是在说「这条规则只有一份」。
//!   - **算法**：被指认的那一处真的还在算边界（标记还在、`production_text` 还在）。
//!     少了这一问，把算法换成空函数之后本判据照绿，它只证明了「别处没有」。
//!   - **使用者**：真的有几十份门在读它。只定义一次而没人用，与「规则装在一个没人
//!     打开的文件里」同形。
//!
//! 判据按出现形式判，不按裸子串：注释里提到标记不算（`///` 起行的那一行跳过）——
//! 这份文件自己就在大段解释它，按裸子串判会先把它自己判成第二处。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;
use common::rust_production::{all_crate_files, relative_name};

/// 算边界的那一处。
const THE_ONE: &str = "crates/cogneva/tests/common/rust_production.rs";

/// 人口下限：`crates/` 下的 `.rs` 今天有一千余份，读到的比这少说明遍历没走到。
const MIN_FILES: usize = 800;

/// 引用切口的文件下限。今天 71 份，取一半——这个数要低到「正常的增删不会碰它」，
/// 又要高到「共享模块被谁绕开了」当场能看见。
const MIN_CONSUMERS: usize = 40;

/// 两种把边界写成字符串字面量的样子：找标记，或按标记切。
///
/// 写成「带引号」而不是裸的 `#[cfg(test)]`：裸子串在注释、文档与说明里到处都是，
/// 按它判会把每一个解释这件事的文件都判成第二处；这里要抓的是**能跑的那一份**。
const MARKERS: [&str; 2] = ["\"#[cfg(test)]\"", "\"mod tests\""];

/// 一处命中：文件、行号、那行的样子。
type Site = (String, usize, String);

/// 一次普查：命中的标记、读到的文件数、引用切口的文件数。
struct Census {
    sites: Vec<Site>,
    files: usize,
    consumers: usize,
}

fn census(root: &Path, files_in: &[PathBuf]) -> Census {
    let mut sites = Vec::new();
    let mut files = 0usize;
    let mut consumers = 0usize;
    for path in files_in {
        let name = relative_name(root, path);
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        files += 1;
        if name != THE_ONE && uses_the_cut(&text) {
            consumers += 1;
        }
        for (index, line) in text.lines().enumerate() {
            // 注释行跳过：`// 切在 "#[cfg(test)]" 之前` 是在说这件事，不是在算它。
            if line.trim_start().starts_with("//") {
                continue;
            }
            if MARKERS.iter().any(|marker| line.contains(marker)) {
                sites.push((name.clone(), index + 1, line.trim().to_string()));
            }
        }
    }
    Census {
        sites,
        files,
        consumers,
    }
}

/// 这份文件在用共享切口里的哪一个投影。
fn uses_the_cut(text: &str) -> bool {
    [
        "production_text",
        "production_lines",
        "production_numbered_lines",
        "production_line_mask",
    ]
    .iter()
    .any(|name| text.contains(name))
}

#[test]
fn the_production_cut_is_defined_once_and_used_everywhere() {
    let root = repo_root();
    let found = census(&root, &all_crate_files(&root));

    assert!(
        found.files >= MIN_FILES,
        "只读到 {} 个 crate 源文件（下限 {MIN_FILES}）：遍历没走到，下面的「只有一处」不算数",
        found.files
    );

    let elsewhere: Vec<&Site> = found
        .sites
        .iter()
        .filter(|(file, _, _)| file != THE_ONE)
        .collect();
    assert!(
        elsewhere.is_empty(),
        "这些文件自己算了一遍「产出面在哪里」：{elsewhere:?}\n\
         切在第一个 `#[cfg(test)]` 上的那一份，会在文件中间有 test-only 项时把后面的产出\
         整段丢掉，而「那里没有」与「那里没读」在判词上是同一句话。\
         改用 `common::rust_production` 里的 `production_text` / `production_numbered_lines` / \
         `production_line_mask`；缺投影就补在那份共享模块里，别在调用点再切一次。"
    );

    // 反过来问一句：被指认的那一处真的还在算边界吗。少了这一问，把它换成空函数、
    // 或者换个标记，本判据照绿——它只证明了「别处没有」。
    let the_one = std::fs::read_to_string(root.join(THE_ONE)).expect("共享切口读得到");
    assert!(
        MARKERS.iter().any(|marker| the_one.contains(marker)),
        "{THE_ONE} 里不再把测试标记写成字符串：边界已经不是在这里算的了，\
         那么「只有一处」就是指着这份不干这件事的文件"
    );
    assert!(
        the_one.contains("pub fn production_text("),
        "{THE_ONE} 里没有 `production_text`：切口的产出面不见了，读它的几十份门会一起变空"
    );

    assert!(
        found.consumers >= MIN_CONSUMERS,
        "只有 {} 份文件在用共享切口（下限 {MIN_CONSUMERS}）：规则定义了一处，却几乎没有门\
         在读它——那与没定义同形",
        found.consumers
    );
}

/// 判据自检：真搭一棵树，把各种写法放进去，看它认不认得出。
///
/// 只断真树的那一条证明不了它还会响——真树今天恰好干净，而干净与瞎同形。
#[test]
fn a_hand_rolled_cut_on_disk_is_reported() {
    let root = std::env::temp_dir().join(format!("census-production-cut-{}", std::process::id()));
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).expect("a scratch crate");

    // 1) 正当的消费者：借共享切口，不算第二处，计一个使用者。
    std::fs::write(
        src.join("consumer.rs"),
        "fn f(text: &str) -> String { crate::rust_production::production_text(text) }\n",
    )
    .expect("a consumer");
    let found = census(&root, &common::rust_production::crate_files_of(&root));
    assert_eq!(found.files, 1, "假树里的文件要读得到");
    assert!(
        found.sites.is_empty(),
        "借共享切口不该被当成第二处：{:?}",
        found.sites
    );
    assert_eq!(found.consumers, 1, "使用者要数得出来");

    // 2) 只在注释里提到标记：不是能跑的那一份。
    std::fs::write(
        src.join("mention.rs"),
        "/// 切在 \"#[cfg(test)]\" 之前是不对的\n// let cut = text.split(\"#[cfg(test)]\");\npub const X: u32 = 1;\n",
    )
    .expect("a mention");
    let found = census(&root, &common::rust_production::crate_files_of(&root));
    assert!(
        found.sites.is_empty(),
        "注释里的标记不该算成第二处：{:?}",
        found.sites
    );

    // 3) 手写的切口，两种写法都要数得出来，且要带原始行号。
    std::fs::write(
        src.join("cut.rs"),
        "fn f(text: &str) -> usize {\n    let cut = text.find(\"#[cfg(test)]\").unwrap_or(text.len());\n    cut\n}\nfn g() -> usize {\n    \"mod tests\".len()\n}\n",
    )
    .expect("a hand-rolled cut");
    let found = census(&root, &common::rust_production::crate_files_of(&root));
    assert_eq!(
        found.sites.len(),
        2,
        "两份手写的切口都要数得出来：{:?}",
        found.sites
    );
    assert!(
        found
            .sites
            .iter()
            .any(|(file, line, _)| file == "crates/probe/src/cut.rs" && *line == 2),
        "报出来的位置要能直接翻到（`find` 那一处是第 2 行）：{:?}",
        found.sites
    );
    assert!(
        found.sites.iter().all(|(file, _, _)| file != THE_ONE),
        "它们都不在算边界的那一处，所以真树上的门会红：{:?}",
        found.sites
    );

    // 4) 另一棵树：抄件长在别的 crate 的 `tests/` 里，一个 `src/` 都没碰。
    let tests = root.join("crates/probe/tests");
    std::fs::create_dir_all(&tests).expect("a second crate's tests directory");
    std::fs::write(
        tests.join("copy.rs"),
        "fn production_source(text: &str) -> &str {\n    text.split(\"#[cfg(test)]\").next().unwrap()\n}\n",
    )
    .expect("a copy among the gates");
    let found = census(&root, &common::rust_production::crate_files_of(&root));
    assert!(
        found
            .sites
            .iter()
            .any(|(file, _, _)| file == "crates/probe/tests/copy.rs"),
        "别的 crate 的夹具里的抄件也要数得出来，否则这份门只看得见产出面：{:?}",
        found.sites
    );

    std::fs::remove_dir_all(&root).ok();
}

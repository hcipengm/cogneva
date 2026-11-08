//! principle: feedback_an_optimization_must_name_the_share_it_cuts_and_prove_the_output_identical
//!
//! 「快」不是重构的验收标准，**同样的输出**才是。记忆里那个例子：97 条 `git show`
//! 换成一次 `cat-file --batch`，快是副作用，能不能交付全看那 97 条输出里有没有一条
//! 变了样。于是这条原则的验收标准有两半，缺一半就不算验收：点名砍掉的是哪一份份额
//! （那 97 次起进程），并证明剩下的那份与原来逐字节相同。少了后半句，快只是把工作量
//! 挪到了读不出来的一侧。
//!
//! 这棵树自己踩过同一个捷径，就在这条原则的账本里。`cog_core::principle_ledger`
//! 回答「这条原则有没有门」时，不再「每个文件 × 每种注释拼法」各读一遍，而是读一张
//! 声明过的名册 `PLATFORM_REGISTRY_SOURCES`：`(pathspec, 前缀)` 成对，`.rs` 两种前缀、
//! 两个 `*.test.sh` 目录用 shell 注释。这份名册就是被砍掉的份额——从全树全拼法砍到
//! 四对——而它唯一能站住的方式，是它找到的与穷举读到的**完全一样**：同样的路径、
//! 同样的 slug，一条不多、一条不少。
//!
//! 本门读的就是这两边，都是这棵树、这个 revision 上的读：
//!
//! * 穷举那边：`crates/` 下每个 `.rs`（按路径排序），加上名册里每个 shell 面的目录
//!   中的每个 `*.test.sh`（目录不在就跳过，不因此判红）。凡注释正文以 `principle:`
//!   起头的行都算一条声明，标记取最长的那个（`//!`、`///`、`//`，shell 里是 `#`），
//!   记下 (路径, 标记, slug)。`principle:` 后面空着、或者 slug 里带空白的，是**拼坏了
//!   的声明**：按「文件:行」报出来，而不是从两边一起过滤掉——滤掉的那个拼法在名册
//!   那边也正好不存在，红不了。
//! * 捷径那边：同一批命中拿名册判——路径被 `is_a_registry_source` 接受，且这一行是
//!   用闭集里的某个前缀写下的。两边 (路径, slug) 集合必须相等，差异两边都打出来：
//!   写了第四种拼法、或者在名册的 pathspec 之外写了声明，都会红，并且点出是哪个文件。
//!
//! 名册自己也判：`PLATFORM_DECLARATION_PREFIXES` 里每个前缀至少配一条 pathspec，每条
//! pathspec 用的前缀必须是闭集里的一个——没配上对的前缀，正是捷径已经不再搜的那个
//! 拼法；每个前缀还得真写在这棵树的合同文件里，于是本门比的是这个 revision 声明的
//! 那张名册，而不是调用方手里内联的一份。
//!
//! 地板，让停下来的读者不能照样过：`crates/` 下至少真的读了 500 个 `.rs`（树里一千
//! 二百多份），至少 3 条声明，且至少一条写在 `//!` 这个模块头拼法下面。
//!
//! 判不了什么：
//!
//! * 一条声明所指的门是否真的执行这条规则。本门读的是「声明写了什么」，不是「那个
//!   测试会不会红」。
//! * slug 在宿主名册上是否存在——那张名册在另一侧，不在这棵树里。
//! * 两种读法谁快。速度不是这里任何一条断言读的东西，那正是这条原则要压住的那一半。

use std::collections::BTreeSet;

use cog_core::{is_a_registry_source, PLATFORM_DECLARATION_PREFIXES, PLATFORM_REGISTRY_SOURCES};

use crate::common;

use common::routes::{read, repo_root, walk};
use common::rust_production::relative_name;

/// 名册的前缀字面量写在这份合同文件里：本门从树里读它，而不是从调用方手里接。
const CONTRACT_FILE: &str = "crates/cog-core/src/contract/principle_ledger.rs";

/// 一份 `.rs` 文件里一句声明可能用的注释标记，最长的先认。
///
/// 这是**穷举**那一边的集合，比名册宽一格：`//` 这份树今天一条都没写，但真写了的
/// 必须被读到，而不是被名册漏掉之后两边都看不见它。
const RUST_MARKERS: &[&str] = &["//!", "///", "//"];

/// shell 门里的那一种。
const SHELL_MARKERS: &[&str] = &["#"];

/// `crates/` 下至少读到多少个 `.rs`：树里一千二百多份，读到五百以下说明走目录那段
/// 已经停了，而在它漏掉的文件上做出的比较是因为漏掉才绿的。
const RUST_FILE_FLOOR: usize = 500;

/// 至少读到几条声明：树里三百多条，读到三条以下说明读法本身坏了。
const DECLARATION_FLOOR: usize = 3;

/// 穷举读到的一条声明：哪份文件、哪种注释标记、slug 是什么、那一行怎么写的。
///
/// `written` 是那一行去掉行首缩进后的原文。捷径那一边必须按原文判：把标记和 slug
/// 拼回去，`//!  principle:` 这种多一个空格的写法就会自己长回合规的样子，而名册的
/// 锚点是字面的，它读不到这一行。
struct Declaration {
    path: String,
    marker: &'static str,
    slug: String,
    written: String,
}

/// 一份文件里的每条声明；拼坏的按「文件:行」记进 `malformed`。
fn declarations_in(
    path: &str,
    text: &str,
    markers: &[&'static str],
    malformed: &mut Vec<String>,
) -> Vec<Declaration> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let written = line.trim_start();
        let Some(&marker) = markers.iter().find(|marker| written.starts_with(**marker)) else {
            continue;
        };
        let Some(rest) = written[marker.len()..]
            .trim_start()
            .strip_prefix("principle:")
        else {
            continue;
        };
        let slug = rest.trim();
        if slug.is_empty() || slug.chars().any(char::is_whitespace) {
            malformed.push(format!("{path}:{}: {written}", index + 1));
            continue;
        }
        found.push(Declaration {
            path: path.to_string(),
            marker,
            slug: slug.to_string(),
            written: written.to_string(),
        });
    }
    found
}

/// 名册里每个 shell 面拆开后的 (目录, 文件名后缀)。
///
/// 从名册推出来而不是在这里再写一遍目录：名册多一处 shell 面，穷举跟着多读一处。
/// 写成两份的话，新加的那一处只会出现在捷径那边，而两边都读到的那批文件照样相等。
fn shell_gate_sources() -> Vec<(&'static str, &'static str)> {
    PLATFORM_REGISTRY_SOURCES
        .iter()
        .filter_map(|(pathspec, _)| pathspec.split_once("/*"))
        .collect()
}

/// 树上的每一条声明，穷举读法：`crates/` 下每个 `.rs`，加上每个 shell 面的目录。
///
/// 第三个返回值是读了多少个 `.rs`，好让地板判读者有没有真的读。
fn exhaustive_declarations() -> (Vec<Declaration>, Vec<String>, usize) {
    let root = repo_root();
    let mut hits = Vec::new();
    let mut malformed = Vec::new();

    let mut sources = Vec::new();
    walk(&root.join("crates"), &["rs"], &mut sources);
    sources.sort();
    for path in &sources {
        let name = relative_name(&root, path);
        hits.extend(declarations_in(
            &name,
            &read(path),
            RUST_MARKERS,
            &mut malformed,
        ));
    }
    let rust_files = sources.len();

    for (dir, suffix) in shell_gate_sources() {
        let dir = root.join(dir);
        if !dir.is_dir() {
            continue;
        }
        let mut gates = Vec::new();
        walk(&dir, &["sh"], &mut gates);
        gates.sort();
        for path in &gates {
            let is_gate = path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(suffix));
            if !is_gate {
                continue;
            }
            let name = relative_name(&root, path);
            hits.extend(declarations_in(
                &name,
                &read(path),
                SHELL_MARKERS,
                &mut malformed,
            ));
        }
    }

    (hits, malformed, rust_files)
}

/// 名册读不读得到这一行：路径在名册的 pathspec 里，且这一行是用闭集里的某个前缀
/// 写下的。两个条件都不是这门写出来的，门只是把它们合起来问一次。
fn read_by_the_roster(declaration: &Declaration) -> bool {
    is_a_registry_source(&declaration.path)
        && PLATFORM_DECLARATION_PREFIXES
            .iter()
            .any(|prefix| declaration.written.starts_with(*prefix))
}

/// 名册两个方向都全，且每个前缀都写在这棵树的合同文件里。
///
/// 一个没配上对的前缀，是捷径已经不再搜的那个拼法：闭集说了三个，名册只搜两个，于是
/// 第三种拼法写下的声明在捷径那边等于不存在——而「不存在」与「没有门」在这条原则的
/// 账上同形。
fn the_roster_is_total() {
    for prefix in PLATFORM_DECLARATION_PREFIXES {
        assert!(
            PLATFORM_REGISTRY_SOURCES
                .iter()
                .any(|(_, paired)| paired == prefix),
            "{prefix:?} is readable but no pathspec in PLATFORM_REGISTRY_SOURCES is searched with \
             it: a spelling the shortcut no longer looks for"
        );
    }
    for (pathspec, prefix) in PLATFORM_REGISTRY_SOURCES {
        assert!(
            PLATFORM_DECLARATION_PREFIXES.contains(prefix),
            "{pathspec:?} is searched with {prefix:?}, which is not one of \
             PLATFORM_DECLARATION_PREFIXES"
        );
    }
    let contract = read(&repo_root().join(CONTRACT_FILE));
    for prefix in PLATFORM_DECLARATION_PREFIXES {
        let literal = format!("\"{prefix}\"");
        assert!(
            contract.contains(&literal),
            "{CONTRACT_FILE} does not hold {literal}: the roster this gate compares against is \
             then one a caller could have inlined, not the one this revision declares"
        );
    }
}

/// 声明集合的比较形状：(路径, slug)。同一份文件里写两遍同一个 slug 折成一条——
/// 两边折法一致，于是「两边相等」说的是同一件事。
fn pairs<'a>(declarations: impl Iterator<Item = &'a Declaration>) -> BTreeSet<(String, String)> {
    declarations
        .map(|declaration| (declaration.path.clone(), declaration.slug.clone()))
        .collect()
}

/// 捷径与穷举读到的是同一批声明：一条不多、一条不少。
#[test]
fn the_shortcut_finds_what_the_exhaustive_read_finds() {
    the_roster_is_total();

    let (hits, malformed, rust_files) = exhaustive_declarations();
    assert!(
        malformed.is_empty(),
        "{} declaration(s) are written with `principle:` followed by nothing but an empty or \
         whitespace-carrying slug, so neither reading can key on them:\n{}",
        malformed.len(),
        malformed.join("\n")
    );
    assert!(
        rust_files >= RUST_FILE_FLOOR,
        "the exhaustive read opened {rust_files} `.rs` files under crates/, fewer than the \
         {RUST_FILE_FLOOR} this tree is known to hold; the walk has stopped reading, and a \
         comparison is green for exactly that reason"
    );
    assert!(
        hits.len() >= DECLARATION_FLOOR,
        "the exhaustive read found {} declaration(s), fewer than the {DECLARATION_FLOOR} this \
         tree is known to hold",
        hits.len()
    );
    assert!(
        hits.iter().any(|declaration| declaration.marker == "//!"),
        "no declaration was found under the module-header spelling: the reader that finds the \
         other spellings has stopped reading this one"
    );

    let exhaustive = pairs(hits.iter());
    let shortcut = pairs(
        hits.iter()
            .filter(|declaration| read_by_the_roster(declaration)),
    );

    // 捷径是穷举的一个过滤，所以第二个差集按构造为空；两个都打出来是因为红的那个
    // 方向几乎总是第一个，而读判决的人要在同一句里看到是哪份文件。
    let only_exhaustive: Vec<_> = exhaustive.difference(&shortcut).collect();
    let only_shortcut: Vec<_> = shortcut.difference(&exhaustive).collect();
    assert!(
        only_exhaustive.is_empty() && only_shortcut.is_empty(),
        "the roster reads a different set of declarations than the whole tree does.\n\
         written where the roster does not look, {} (path, slug):\n{:#?}\n\
         read by the roster and not found by the tree, {} (path, slug):\n{:#?}",
        only_exhaustive.len(),
        only_exhaustive,
        only_shortcut.len(),
        only_shortcut
    );
}

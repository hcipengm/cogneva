//! principle: feedback_no_hardcoding_config_surface
//!
//! 「把一份文件的整份内容换掉」这条规则，树里手写过五遍以上。
//!
//! 每一遍都是同一个动作：在目标**旁边**写一个临时文件，再把临时文件 rename 到
//! 目标名上——于是读者要么看见整份旧的，要么看见整份新的，不会看见半份。每一遍
//! 又各自决定临时文件叫什么（`json.tmp`、`tmp-<pid>`、`.<name>.json.tmp`、
//! `.<name>.<pid>.<seq>.tmp`）、失败时留不留残骸、要不要 fsync。
//!
//! 五份里改错任何一份，另外四份不会红。而**改错的表现**恰恰是安静的：临时文件
//! 放到别的目录，rename 就跨了文件系统、退回成「先删后写」，中间那一段读者看到
//! 的是「文件不存在」；不设权限位，rename 过去的那一份就是只有本进程能写的；
//! 不 fsync 目录，掉电后目录项回到旧值，读端拿到的是「这份读数从来没有过」。
//!
//! 判据：
//!
//! 1. 生产代码里不许再出现「自己写临时文件、自己 rename 到目标」这一对手写步骤；
//!    这类写入一律走 [`cog_core::atomic_write`]。判法是行为判据而不是关键词判据：
//!    一个 `rename` 调用的源如果不是「同一个函数里刚被 `write` / `create` / `open`
//!    创建出来的那个名字」，它就不是一次整份替换（是一条真正的 move——轮转、
//!    journal 归档、目录换名），不归本门管。这样绕开判据要说谎（把临时文件叫成
//!    别的名字）也必须同时真的不写那个文件，那条路不通。
//! 2. 例外要登记，且不许过期：登记的那一处必须还在，否则删掉。「包的字节要流式
//!    写」这种理由是真的，但只能有一条，不能变成惯例。
//! 3. 普查有下限：读到的 rename 调用点太少时本门自己红，「一个都没读到」与
//!    「一处都没有」必须是两个读数。
//!
//! 判不了什么：
//!
//! - 跨文件的替换（先清空 A 再写 B），以及「先删后建」——那些不是 rename。
//! - 内容是谁写的：`std::fs::copy` 到临时名再 rename 的形状本门也抓，因为它同样
//!   是「自己拼临时名」；但那样一来判据会变成两条，眼下树上那几处都在同一 crate
//!   的下载路径里，等它们需要统一时再加。

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use common::routes::{repo_root, walk};
use common::rust_production::production_text;

/// 唯一的那一处实现。
const IMPLEMENTATION: &str = "cog-core/src/atomic_write.rs";

/// 自己拼临时名、自己 rename、但因为技术原因不能走 [`cog_core::atomic_write`] 的
/// 那些地方。每一条写清「为什么那一处不行」，而不是「暂时没改」。
const REGISTERED: &[(&str, &str, &str)] = &[
    (
        "cogneva/src/backup.rs",
        "tmp",
        "包的字节由 tar→zstd→加密三层流式写出，为了 rename 而把整份读回内存会把\
         一个多 GiB 的包变成内存峰值；这里唯一要满足的原子性就是最后那一步 rename。",
    ),
    (
        "cog-reflection/src/build_gate/mod.rs",
        "tmp",
        "gate 文件必须以 O_NOFOLLOW|O_CLOEXEC 打开，并且在**写入之前**就设好共享\
         权限位——通用写函数既不接受自定义旗标，也不给「内容出现之前设权限」这一步。",
    ),
];

/// 普查下限：测到的那一天是 26 处 rename 调用点。低于下限说明本门读得太少，
/// 这时的「全绿」什么也不证明。
const MIN_SITES: usize = 20;

/// 会创建一份文件的调用名。判据里只要源名出现在这类调用的实参位置，就说明
/// 「这个临时文件是本函数写出来的」。
///
/// `open` 单看不说明什么——`File::open(&x)?.sync_all()` 是打开来 fsync 的，读的是
/// 一份别人写的文件。所以它只在**同一个表达式里带着 `OpenOptions`** 时才算创建。
fn creates_a_file(callee: &str) -> bool {
    callee.ends_with("write") || callee.ends_with("create")
}

/// 从 `at`（`(` 的位置）往回看，这个调用是不是一次「打开来写」。
fn opens_for_writing(window: &str, at: usize) -> bool {
    let before = &window[..at];
    let tail = &before[before.len().saturating_sub(400)..];
    tail.rfind("OpenOptions").is_some()
}

/// 从 `end` 往回跳空白，返回紧邻的那个标识符。
fn ident_before(bytes: &[u8], mut end: usize) -> String {
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let stop = end;
    while end > 0 && (bytes[end - 1].is_ascii_alphanumeric() || bytes[end - 1] == b'_') {
        end -= 1;
    }
    String::from_utf8_lossy(&bytes[end..stop]).into_owned()
}

/// rename 调用的源表达式，如果它是一个裸标识符（`&tmp` / `tmp`），返回那个名字。
fn simple_source(arg: &str) -> Option<String> {
    let arg = arg.trim().trim_start_matches('&').trim();
    let ok = !arg.is_empty()
        && arg.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !arg.as_bytes()[0].is_ascii_digit();
    ok.then(|| arg.to_string())
}

/// 这个文件的这一处 rename：源名，以及「同一个函数里有没有创建过这个名字」。
struct Site {
    file: String,
    source: Option<String>,
    hand_rolled: bool,
}

/// 一个文本里所有 rename 调用点，按出现顺序。
fn sites_in(file: &str, text: &str) -> Vec<Site> {
    let bytes = text.as_bytes();
    let needle = b"::rename(";
    let mut out = Vec::new();
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] != needle {
            i += 1;
            continue;
        }
        let open = i + needle.len();
        // 第一实参到第一个逗号为止（树上每处 rename 都有两个实参）。
        let mut j = open;
        while j < bytes.len() && bytes[j] != b',' && bytes[j] != b')' {
            j += 1;
        }
        let arg = std::str::from_utf8(&bytes[open..j]).unwrap_or("");
        let source = simple_source(arg);
        let hand_rolled = match &source {
            Some(name) => {
                // 这个 rename 所在的函数：从调用点往回找最近的一行 `fn `。
                let head = &text[..i];
                let fn_start = fn_start_before(head);
                // 同一行链式写法也在窗口里。
                creation_before(&text[fn_start..i], name)
            }
            None => false,
        };
        out.push(Site {
            file: file.to_string(),
            source,
            hand_rolled,
        });
        i = j;
    }
    out
}

/// 这个 rename 所在的函数的起点。
///
/// `fn` 前面往往还有 `pub` / `async` / 缩进，所以不能只找行首的 `fn `——找错了
/// 就把窗口放大到整个文件，读端于是拿别的函数里写过的同名变量当证据。
fn fn_start_before(head: &str) -> usize {
    let bytes = head.as_bytes();
    for (at, _) in head.rmatch_indices("fn ") {
        let before_ok = at == 0 || bytes[at - 1].is_ascii_whitespace();
        let mut k = at + 3;
        while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
            k += 1;
        }
        let after_ok = k > at + 3 && matches!(bytes.get(k), Some(b'(') | Some(b'<'));
        if before_ok && after_ok {
            return at;
        }
    }
    0
}

/// `window` 里有没有一个创建文件的调用，实参是这个源名。
fn creation_before(window: &str, name: &str) -> bool {
    let bytes = window.as_bytes();
    for (at, _) in window.match_indices('(') {
        let callee = ident_before(bytes, at);
        if !(creates_a_file(&callee) || (callee == "open" && opens_for_writing(window, at))) {
            continue;
        }
        let rest = &window[at + 1..];
        let rest = rest.trim_start();
        let rest = rest.strip_prefix('&').unwrap_or(rest);
        let hit = rest.starts_with(name)
            && !rest[name.len()..]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
        if hit {
            return true;
        }
    }
    false
}

/// 每个 crate 的源码里所有 rename 调用点。
fn all_sites(crates_dir: &Path) -> Vec<Site> {
    let Ok(entries) = std::fs::read_dir(crates_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut dirs: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    dirs.sort();
    for dir in dirs {
        let src = dir.join("src");
        if !src.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        walk(&src, &["rs"], &mut files);
        files.sort();
        for path in files {
            let rel = path
                .strip_prefix(crates_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let production = production_text(&common::routes::read(&path));
            out.extend(sites_in(&rel, &production));
        }
    }
    out
}

fn registered(file: &str, source: &str) -> bool {
    REGISTERED
        .iter()
        .any(|(f, s, _)| *f == file && *s == source)
}

#[test]
fn a_file_is_replaced_atomically_in_one_place() {
    let root = repo_root();
    let sites = all_sites(&root.join("crates"));
    assert!(
        sites.len() >= MIN_SITES,
        "只读到 {} 处 rename 调用点（下限 {MIN_SITES}）：读得太少，「一处都没有手写」就是个空承诺",
        sites.len()
    );
    let hand_rolled: Vec<String> = sites
        .iter()
        .filter(|s| s.file != IMPLEMENTATION && s.hand_rolled)
        .filter(|s| !registered(&s.file, s.source.as_deref().unwrap_or_default()))
        .map(|s| {
            format!(
                "  {} 里自己写临时文件再 rename（源是 {:?}）",
                s.file,
                s.source.as_deref().unwrap_or_default()
            )
        })
        .collect();
    assert!(
        hand_rolled.is_empty(),
        "这些地方把自己写的临时文件 rename 成了目标文件——也就是又抄了一遍\
         「整份替换」这条规则。抄第二遍的那一刻，第一遍改对（换临时名、设权限、\
         fsync 目录）不会让这里跟着对：\n{}\n\
         改用 cog_core::atomic_write（Durability 选一档），或在 REGISTERED 里写清\
         为什么这一处走不通：",
        hand_rolled.join("\n")
    );
}

#[test]
fn a_registration_that_no_longer_matches_a_site_is_stale() {
    let root = repo_root();
    let sites = all_sites(&root.join("crates"));
    let stale: Vec<&str> = REGISTERED
        .iter()
        .filter(|(file, source, _)| {
            !sites
                .iter()
                .any(|s| s.file == *file && s.hand_rolled && s.source.as_deref() == Some(*source))
        })
        .map(|(_, source, _)| *source)
        .collect();
    assert!(
        stale.is_empty(),
        "这些登记已经不再对应任何手写替换：{stale:?}。登记是给一处现场发的许可，\
         现场没了就该收回——留着它，下一次真的有人手写就没有判据拦了。"
    );
}

/// 对照：手写一遍必须被报出来。少了这条，`all_sites` 哪天退化成「什么都读不
/// 出来」，上面两条判据仍然全绿。
#[test]
fn a_hand_rolled_replace_is_reported() {
    let text = "fn save(path: &Path, body: &[u8]) -> std::io::Result<()> {\n\
                    let tmp = path.with_extension(\"tmp\");\n\
                    std::fs::write(&tmp, body)?;\n\
                    std::fs::rename(&tmp, path)\n\
                }\n";
    let sites = sites_in("cog-x/src/lib.rs", text);
    assert_eq!(sites.len(), 1, "{}", sites.len());
    assert_eq!(sites[0].source.as_deref(), Some("tmp"));
    assert!(sites[0].hand_rolled);
}

/// 对照：一个真正的 move 不是整份替换。少了这条，树上每一处日志轮转、journal
/// 归档都会被判成手写替换。
#[test]
fn a_move_of_a_file_that_was_not_written_here_is_not_a_replace() {
    let text =
        "async fn rotate(dir: &Path, current: &Path, target: &Path) -> std::io::Result<()> {\n\
                    tokio::fs::rename(current, target).await\n\
                }\n";
    let sites = sites_in("cog-x/src/lib.rs", text);
    assert_eq!(sites.len(), 1);
    assert!(!sites[0].hand_rolled);
}

/// 对照：临时名在别的函数里写、这里只负责 rename，也还是同一次手写替换——判据
/// 按「创建」与「改名」之间没有隔着一条 crate 边界来算，而不是按缩进或变量名。
#[test]
fn a_replace_split_across_lines_is_still_reported() {
    let text = "fn save(path: &Path) -> std::io::Result<()> {\n\
                    let tmp = path.with_extension(\"json.tmp\");\n\
                    let file = std::fs::File::create(&tmp)?;\n\
                    drop(file);\n\
                    std::fs::rename(&tmp, path)\n\
                }\n";
    let sites = sites_in("cog-x/src/lib.rs", text);
    assert!(sites[0].hand_rolled);
}

/// 对照：链式写法（`fs::write(..).and_then(|_| rename(..))`）也要被抓到——那是
/// 树上真实存在的一种写法，读漏它就等于给这种写法开后门。
#[test]
fn a_chained_write_then_rename_is_reported() {
    let text = "fn save(path: &Path, body: &str) -> bool {\n\
                    let tmp = path.with_extension(\"json.tmp\");\n\
                    std::fs::write(&tmp, body)\n\
                        .and_then(|_| std::fs::rename(&tmp, &path))\n\
                        .is_ok()\n\
                }\n";
    let sites = sites_in("cog-x/src/lib.rs", text);
    assert!(sites[0].hand_rolled);
}

/// 对照：测试夹具里的手写替换不读。少了这条，本门会要求测试自己也不许这么写。
#[test]
fn a_hand_rolled_replace_in_a_test_module_is_not_production() {
    let text = "pub fn a() {}\n\
                #[cfg(test)]\n\
                mod tests {\n\
                    fn save(path: &Path) -> std::io::Result<()> {\n\
                        let tmp = path.with_extension(\"tmp\");\n\
                        std::fs::write(&tmp, b\"x\")?;\n\
                        std::fs::rename(&tmp, path)\n\
                    }\n\
                }\n";
    let trimmed = production_text(text);
    assert!(sites_in("cog-x/src/lib.rs", &trimmed).is_empty());
}

/// 对照：跨 crate 的两处同名临时文件不会被算成同一处——本门按「谁写的」判，不按
/// 名字判。
#[test]
fn two_crates_writing_the_same_temp_name_are_two_sites() {
    let a = "fn save(path: &Path) -> std::io::Result<()> {\n\
                 let tmp = path.with_extension(\"tmp\");\n\
                 std::fs::write(&tmp, b\"x\")?;\n\
                 std::fs::rename(&tmp, path)\n\
             }\n";
    let sites: Vec<Site> = vec![
        sites_in("cog-a/src/lib.rs", a).remove(0),
        sites_in("cog-b/src/lib.rs", a).remove(0),
    ];
    let files: BTreeSet<&str> = sites.iter().map(|s| s.file.as_str()).collect();
    assert_eq!(files.len(), 2);
}

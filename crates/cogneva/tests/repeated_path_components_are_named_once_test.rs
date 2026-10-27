//! principle: feedback_no_hardcoding_config_surface
//!
//! 同一段路径在一个 crate 里写了两遍，而中间没有任何东西比较这两个写法的值。
//!
//! 这是 `one_value_has_one_name_inside_its_crate` 的反面：那一条读的是
//! `const NAME: &str = "…";` 这样的**声明**，因此它只能看见已经被起了名字的
//! 值。一个组件从头到尾都没被写成常量时——`dir.join("journal.json")` 抄三遍、
//! `pkg_dir.join("pg")` 抄四遍——那里根本没有两个名字可以比，那条门看不见它。
//! 而漂移恰恰是从这种地方渗出来的：改一处、另一处照旧，
//! 读的那一半从此对着一棵空的子树，返回 `Ok((0, 0))`。
//!
//! 判据：
//!
//! 1. 生产代码里，同一个 crate 里同一段路径最多出现一次；第二次出现要么改为引用
//!    那个常量，要么在 [`SAME_PATH_DIFFERENT_THING`] 里写清它们为什么同形却不是
//!    同一段路径。「一段路径」读的是三种拼法：`.join("…")`、`Path::new("…")` 与
//!    `PathBuf::from("…")`。
//! 2. 登记不许过期：登记的那一组必须还在，否则删掉——留着一条不对应任何重复的
//!    许可，下一次真的抄了同一个组件就没有判据拦了。
//! 3. 普查有下限：读到的组件数或 crate 数太少时本门自己红，免得「一个都没读到」
//!    与「一次都没有重复」是同一个读数。
//!
//! 判不了什么：
//!
//! - 跨 crate 的同一段路径（`storage` 在 cog-reflection 与 cogneva 各有一份）。
//!   两个 crate 各自的布局可以各自演化，强行合并会把两份布局焊在一起。
//! - 路径的**含义**：`join("run")` 与 `join("run")` 同形，本门只知道它们同形。
//! - 拼接起来才成形的那一段（`join("deploy/rendered")`），按它写成的那个字符串
//!   判，不拆开。
//! - 字符串的 `Vec::join`（`.join(", ")`）：只认「一个词」的形状，见
//!   [`is_component`]。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::routes::{repo_root, walk};
use common::rust_production::production_text;

/// 同一个 crate 里同形的路径组件、却不是同一段路径的那些组。
///
/// 每一条写的是「为什么它们同形却不是一段路径」。没有写进这里的一律当缺陷：
/// 要么让第二处引用第一处的常量，要么在这里说明理由。
const SAME_PATH_DIFFERENT_THING: &[(&str, &str, &str)] = &[];

/// 普查下限：测到的那一天是 123 个组件、16 个 crate。低于下限说明本门读得
/// 太少，而这时的「全绿」什么也不证明。
const MIN_COMPONENTS: usize = 80;
const MIN_CRATES: usize = 12;

/// 一个路径组件该有的形状：小写、数字与 `_ . - /`，不超过 40 字节。
///
/// 这道判据把 `.join(", ")`、`.join(" ")`、`.join(" | ")` 这些**字符串**的
/// 连接挡在外面——它们同样是 `join(`，但拼出来的不是路径。带空格的路径组件
/// 不进这道门：那种字面量在树上没有，一旦出现会体现为普查数下降。
fn is_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 40
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-' | b'/')
        })
}

/// 三种把一段路径写进代码的拼法。`join` 是接在一段后面的组件，`Path::new` /
/// `PathBuf::from` 是整段：同一个词无论从哪一种写出来，写两遍就是两处。
const PATH_FORMS: [&str; 3] = [".join(", "Path::new(", "PathBuf::from("];

/// 文本里所有按 [`PATH_FORMS`] 写出来的路径字面量，按出现顺序。
///
/// 手写扫描而不是正则：形式与它的字面量之间允许换行（树上现在没有这种写法，
/// 但读漏一处就是一个不出声的缺口）。转义过的字面量不读——它不是一个词。
fn path_components(text: &str) -> Vec<String> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    for needle in PATH_FORMS {
        let needle = needle.as_bytes();
        let mut i = 0;
        while i + needle.len() <= b.len() {
            if &b[i..i + needle.len()] != needle {
                i += 1;
                continue;
            }
            let mut j = i + needle.len();
            while j < b.len() && b[j].is_ascii_whitespace() {
                j += 1;
            }
            if j >= b.len() || b[j] != b'"' {
                i += needle.len();
                continue;
            }
            let start = j + 1;
            let mut k = start;
            let mut escaped = false;
            while k < b.len() && b[k] != b'"' {
                if b[k] == b'\\' {
                    escaped = true;
                }
                k += 1;
            }
            if k >= b.len() {
                break;
            }
            let value = std::str::from_utf8(&b[start..k]).unwrap_or("");
            if !escaped && is_component(value) {
                out.push(value.to_string());
            }
            i = k + 1;
        }
    }
    out
}

/// 一个 crate 里被写了两遍以上的组件：值，以及它出现的次数。
type Repeat = (String, usize, BTreeSet<String>);

/// 每个 crate 里重复的路径组件，以及读过几个组件、几个 crate。
fn repeats(root: &Path) -> (Vec<(String, Repeat)>, usize, usize) {
    let crates_dir = root.join("crates");
    let Ok(entries) = std::fs::read_dir(&crates_dir) else {
        return (Vec::new(), 0, 0);
    };
    let mut out = Vec::new();
    let mut total = 0usize;
    let mut crates = 0usize;
    for entry in entries.flatten() {
        let krate = entry.file_name().to_string_lossy().into_owned();
        let src = entry.path().join("src");
        if !src.is_dir() {
            continue;
        }
        crates += 1;
        let mut files = Vec::new();
        walk(&src, &["rs"], &mut files);
        files.sort();
        let mut seen: BTreeMap<String, (usize, BTreeSet<String>)> = BTreeMap::new();
        for path in files {
            let text = common::routes::read(&path);
            let production = production_text(&text);
            for component in path_components(&production) {
                total += 1;
                let slot = seen.entry(component).or_default();
                slot.0 += 1;
                let name = path
                    .strip_prefix(&src)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .into_owned();
                slot.1.insert(name);
            }
        }
        for (value, (count, files)) in seen {
            if count > 1 {
                out.push((krate.clone(), (value, count, files)));
            }
        }
    }
    (out, total, crates)
}

fn registered(krate: &str, value: &str) -> bool {
    SAME_PATH_DIFFERENT_THING
        .iter()
        .any(|(k, v, _)| *k == krate && *v == value)
}

#[test]
fn repeated_path_components_are_named_once() {
    let root = repo_root();
    let (repeats_found, total, crates) = repeats(&root);
    assert!(
        total >= MIN_COMPONENTS,
        "只读到 {total} 个路径组件（下限 {MIN_COMPONENTS}）：读得太少，「没有重复」就是个空承诺"
    );
    assert!(
        crates >= MIN_CRATES,
        "只读到 {crates} 个 crate 的源码（下限 {MIN_CRATES}）"
    );
    let unregistered: Vec<String> = repeats_found
        .iter()
        .filter(|(krate, (value, _, _))| !registered(krate, value))
        .map(|(krate, (value, count, files))| {
            format!(
                "  {krate} 里 {value:?} 写了 {count} 遍（{}）",
                files.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    assert!(
        unregistered.is_empty(),
        "这些路径组件在同一个 crate 里各写了不止一遍。抄第二遍的那一刻，改第一遍\
         不会让第二遍红——读不出来的那一侧只会安静地换成一棵空目录：\n{}\n\
         要么把它写成常量、第二处引用它，要么在 SAME_PATH_DIFFERENT_THING 里写清\
         为什么它们同形却不是同一段路径：",
        unregistered.join("\n")
    );
}

#[test]
fn a_registration_that_no_longer_matches_a_repeat_is_stale() {
    let root = repo_root();
    let (repeats_found, _, _) = repeats(&root);
    let stale: Vec<&str> = SAME_PATH_DIFFERENT_THING
        .iter()
        .filter(|(krate, value, _)| {
            !repeats_found
                .iter()
                .any(|(k, (v, count, _))| k == krate && v == value && *count > 1)
        })
        .map(|(_, value, _)| *value)
        .collect();
    assert!(
        stale.is_empty(),
        "这些登记已经不再对应任何重复：{stale:?}。登记是给一处现场发的许可，\
         现场没了许可就该收回——留着它，下一次有人真的抄了同一个组件就没有判据拦了。"
    );
}

/// 对照：同一个 crate 里同一个组件写两遍必须被报出来。少了这条，`repeats`
/// 哪天退化成「什么都读不出来」，上面两条判据仍然全绿。
#[test]
fn a_component_written_twice_is_reported() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "fn a(d: &Path) { d.join(\"journal.json\"); }\n\
         fn b(d: &Path) { d.join(\"journal.json\"); }\n",
    )
    .expect("write");
    let (found, total, _) = repeats(tmp.path());
    assert_eq!(total, 2);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].1 .0, "journal.json");
    assert_eq!(found[0].1 .1, 2);
}

/// 对照：整段路径写成 `Path::new` / `PathBuf::from` 的字面量也在这条判据里。
/// 少了这条，`Path::new("/dev/kvm")` 这种写法抄两遍就没人看。
#[test]
fn a_whole_path_written_twice_is_reported() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "fn a() -> bool { Path::new(\"/dev/kvm\").exists() }\n\
         fn b() -> bool { PathBuf::from(\"/dev/kvm\").exists() }\n",
    )
    .expect("write");
    let (found, total, _) = repeats(tmp.path());
    assert_eq!(total, 2);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].1 .0, "/dev/kvm");
}

/// 对照：一个字符串的 `Vec::join` 不是路径组件。少了这条，树上每一个
/// `.join(", ")` 都会被判成重复。
#[test]
fn a_string_join_is_not_a_path_component() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "fn a(v: &[String]) -> String { v.join(\", \") }\n\
         fn b(v: &[String]) -> String { v.join(\", \") }\n",
    )
    .expect("write");
    let (found, total, _) = repeats(tmp.path());
    assert_eq!(total, 0);
    assert!(found.is_empty(), "{found:?}");
}

/// 对照：测试夹具里的重复不读。少了这条，一处夹具会让本门报一个部署上并不
/// 存在的重复。
#[test]
fn a_repeat_inside_a_test_module_is_not_production() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("crates/cog-x/src");
    std::fs::create_dir_all(&src).expect("mkdir");
    std::fs::write(
        src.join("lib.rs"),
        "pub fn a(d: &Path) { d.join(\"pg\"); }\n\
         #[cfg(test)]\n\
         mod tests {\n\
             fn b(d: &Path) { d.join(\"pg\"); }\n\
         }\n",
    )
    .expect("write");
    let (found, total, _) = repeats(tmp.path());
    assert_eq!(total, 1);
    assert!(found.is_empty(), "{found:?}");
}

/// 对照：不同 crate 里同名的组件不算重复——两份布局可以各自演化。
#[test]
fn the_same_component_in_two_crates_is_not_reported_here() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for krate in ["cog-a", "cog-b"] {
        let src = tmp.path().join(format!("crates/{krate}/src"));
        std::fs::create_dir_all(&src).expect("mkdir");
        std::fs::write(
            src.join("lib.rs"),
            "pub fn a(d: &Path) { d.join(\"storage\"); }\n",
        )
        .expect("write");
    }
    let (found, total, _) = repeats(tmp.path());
    assert_eq!(total, 2);
    assert!(found.is_empty(), "{found:?}");
}

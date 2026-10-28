//! 「仓库根在哪」只在一处算出来。
//!
//! 全仓的集成测试里曾有 80 余份 `fn repo_root()` / `fn workspace_root()`，十种写法：
//! `join("../..")`、`.parent().parent()`、`.ancestors().nth(2)`，有的 `canonicalize()`
//! 有的不。它们指同一个目录，所以平时看不出差别——差别只在某一处被改动的那天出现：
//! 根错一级的门会在原地读空，而读空与「这条判据没有题目」在输出上同形，绿得一样。
//!
//! 人口是 `crates/` 下的**全部** `.rs`，不是只到 `tests/`：同样的写法也长在 `src/`
//! 的单测模块里（`#[cfg(test)]` 以下那段）和构建脚本里，把人口切成「测试目录」等于
//! 让大半个仓留在盲区——第一次扩到 `src/` 就数出 40 余处。
//!
//! 判据有两个面，因为它们各自都会长回来：
//!
//!   - 定义：不收参数的 `fn repo_root(` / `fn workspace_root(`，且函数体里提到
//!     `CARGO_MANIFEST_DIR`，在 `crates/` 下只许有一处，且在
//!     `crates/cogneva/tests/common/root.rs`；别的 crate 的测试按路径把那一份
//!     `#[path]` 编进来，不另写一份；
//!   - 算法：靠 `CARGO_MANIFEST_DIR` 往上走的那一行也只许在这一处出现；反过来还要求
//!     这一处真的还在算根（起点仍是那个变量、认根的判据仍是 `[workspace]`、结果不含
//!     `..`），否则「别处没有」也可能只是「谁都不算了」。加了新名字、换个函数名，
//!     定义面会绿，算法面会红。
//!
//! 两面的对象都按出现形式判：注释里的 `fn repo_root()` 不是定义，登记表里的
//! `"CARGO_MANIFEST_DIR"` 字符串不是算法，`include_str!("../../x")` 也不是算法
//! ——那条路径由编译器在编译期核，错了当场构建失败，不会静默读到另一棵树。
//!
//! 这份文件自己也在人口里。它举的标本因此写成「不起行、引号转义」的样子——标本若
//! 起行写成一个真的定义、或者真写出 `env!("CARGO_MANIFEST_DIR")`，第一条命中的就是
//! 它自己。这不是巧合而是要求：改标本写法时先想清楚这条门会怎么读它。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;
use common::rust_production::{all_crate_files, crate_files_of, relative_name};

/// 算根的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/root.rs";

/// 人口下限：全仓 `crates/` 下的 `.rs` 今天有一千余份，读到的比这少说明遍历没走到。
const MIN_FILES: usize = 800;

/// 这条门自己。它不读盘、不拼路径，但它必须写出「往上走」的样子：判据里那两根针、
/// 还有喂给判据的标本。算法面因此把它摘掉——**只摘这一面**，定义面照旧看着，一份真的
/// `fn repo_root` 写在这里同样是第二份答案。
///
/// 摘除是有条件的：下面要求它仍然被数出若干处。判据哪天退化成一概不认，这一条会先红，
/// 而不是让摘除悄悄掩盖掉一个瞎了的判据。
const THE_PREDICATE: &str = "crates/cogneva/tests/the_repo_root_is_computed_in_one_place_test.rs";

/// 一处命中：文件、行号、那行的样子。
type Site = (String, usize, String);

/// 一份「这棵树在哪」的定义：不收参数的那几种名字。
///
/// 收参数的 `workspace_root(data_dir)` 不是同一个问题——它算的是用户工作区，
/// 由外部传入的数据根派生，与 checkout 无关。按名字判会把它当成第二份答案。
fn definitions(line: &str) -> bool {
    let trimmed = line.trim_start();
    let trimmed = trimmed.strip_prefix("pub ").unwrap_or(trimmed);
    for name in ["repo_root", "workspace_root"] {
        let head = format!("fn {name}(");
        if let Some(rest) = trimmed.strip_prefix(&head) {
            if rest.trim_start().starts_with(')') {
                return true;
            }
        }
    }
    false
}

/// 这一行是不是把某个路径往上走了两级。
///
/// 引号必须是**未转义**的：这份文件自己的判据自检把标本写成 `\"../..\"` 的样子，
/// 转义之后它是一段字符串里的字样，不是一次路径拼接。少了这一问，这条门会把写着
/// 「判据自检」的那份文件本身判成第二处算根。
fn climbs_out(line: &str) -> bool {
    let bytes = line.as_bytes();
    let mut from = 0usize;
    while let Some(found) = line[from..].find("\"../..") {
        let at = from + found;
        if at == 0 || bytes[at - 1] != b'\\' {
            return true;
        }
        from = at + 1;
    }
    line.contains(".parent().parent()") || line.contains("ancestors(")
}

/// 这条路径由编译器核：写错了构建就失败，不会静默指向另一棵树。
///
/// 它们和 `env!()` 拼出来的路径不是一回事——后者在编译期是一段普通字符串，指的
/// 是哪棵树要到运行那天才知道，也只有在读写空集时才看得出来。
fn the_compiler_checks_this_path(line: &str) -> bool {
    [
        "include_str!",
        "include_bytes!",
        "include!(",
        "#[path",
        "mod ",
    ]
    .iter()
    .any(|marker| line.contains(marker))
}

fn names_the_manifest_dir(text: &str) -> bool {
    text.contains("CARGO_MANIFEST_DIR")
}

fn sites(root: &Path, files_in: &[PathBuf]) -> (Vec<Site>, Vec<Site>, usize) {
    let mut defs = Vec::new();
    let mut climbs = Vec::new();
    let mut files = 0usize;
    for path in files_in {
        let name = relative_name(root, path);
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        files += 1;
        let lines: Vec<&str> = text.lines().collect();
        let mentions = names_the_manifest_dir(&text);
        for (index, line) in lines.iter().enumerate() {
            // 原始行号：报出来的位置要能直接翻到。注释行跳过——`fn repo_root()` 写在
            // 说明里不是定义，而这份文件本身就在说明它。
            if line.trim_start().starts_with("//") {
                continue;
            }
            // 定义面要看函数体：两个同名的 `repo_root` 里，跑起来的二进制读的是
            // `COGNEVA_REPO_ROOT`（解包出来的资产目录），测试读的是编译期这份
            // checkout。同名不同物，函数体里提不提 `CARGO_MANIFEST_DIR` 是它们的差别。
            let body = &lines[index..lines.len().min(index + 7)];
            if definitions(line) && body.iter().any(|l| names_the_manifest_dir(l)) {
                defs.push((name.clone(), index + 1, line.trim().to_string()));
            }
            if mentions && climbs_out(line) && !the_compiler_checks_this_path(line) {
                climbs.push((name.clone(), index + 1, line.trim().to_string()));
            }
        }
    }
    (defs, climbs, files)
}

#[test]
fn the_repo_root_is_computed_in_one_place() {
    let root = repo_root();
    let (defs, climbs, files) = sites(&root, &all_crate_files(&root));

    assert!(
        files >= MIN_FILES,
        "只读到 {files} 个 crate 源文件（下限 {MIN_FILES}）：遍历没走到，下面的「只有一处」不算数"
    );

    let elsewhere: Vec<&Site> = defs.iter().filter(|(file, _, _)| file != THE_ONE).collect();
    assert!(
        elsewhere.is_empty(),
        "这些文件自己定义了一份仓库根：{elsewhere:?}\n\
         用 `common::routes::repo_root()`。多一份定义就是多一份会漂移的答案，\
         而根漂移时读数只会变成零，与「没有题目」同形。"
    );
    assert_eq!(
        defs.len(),
        1,
        "算根的定义应当恰好一处（{THE_ONE}），读到 {} 处：{defs:?}",
        defs.len()
    );

    // 摘掉这份门自己之前先问一句判据还认不认得出它。判据若是瞎的，下面的清单为空
    // 与「全仓只剩一处」同形——那正是这条门存在的理由。
    let predicate_side: Vec<&Site> = climbs
        .iter()
        .filter(|(file, _, _)| file == THE_PREDICATE)
        .collect();
    assert!(
        predicate_side.len() >= 3,
        "这条门自己至少有三处「看着像算根」（两根针 ＋ 标本），读到 {} 处：{predicate_side:?}。\
         一处都读不到说明判据已经瞎了，那么把这一份摘掉就只是在掩盖它",
        predicate_side.len()
    );

    let out: Vec<&Site> = climbs
        .iter()
        .filter(|(file, _, _)| file != THE_ONE && file != THE_PREDICATE)
        .collect();
    assert!(
        out.is_empty(),
        "这些文件自己在算仓库根（`CARGO_MANIFEST_DIR` 加往上一级）：{out:?}\n\
         换了个函数名不等于换了个答案——根仍然是从第二个地方算出来的。\
         crate 相对路径（`join(\"src/x.rs\")`）不算，编译器按路径核的 include 也不算。"
    );

    // 反过来也要问一句：被指认的那一处真的还在算根吗。少了这一问，把算法
    // 换成 `PathBuf::from(\"/\")` 之后本测试照绿——它只证明了「别处没有」。
    let the_one = std::fs::read_to_string(root.join(THE_ONE)).expect("算根的那一份读得到");
    assert!(
        the_one.contains("env!(\"CARGO_MANIFEST_DIR\")"),
        "{THE_ONE} 里没有 `CARGO_MANIFEST_DIR`：起点不是这个 crate 的目录，算法已经换了"
    );
    assert!(
        the_one.contains("[workspace]"),
        "{THE_ONE} 里不再按 `[workspace]` 认根：判定依据变了，这条门的「唯一一处」就成了指着 \
         一份不干这件事的文件"
    );
}

/// 根这一条路径里有没有往上走的那一段。
fn carries_a_dot_dot(path: &Path) -> bool {
    path.components()
        .any(|component| component == std::path::Component::ParentDir)
}

/// 根必须是「干净」的一条路径：不含 `..`，且已经是自己的 `canonicalize()`。
///
/// 「算根只有一处」挡不住这一类的病：**一处**算出来的根照样可以带 `..`——它指的是
/// 同一个目录，只是拼法不同。全仓的判据大多拿根拼路径，再和 `cargo metadata` 给出的
/// 规范绝对路径比；两边拼法不同就逐字不相等，报告出来的形状是「整棵树都没有 workspace
/// 成员编译」，与真正的原因无关（实测：crates/cogneva/../.. 让 crate 成员一个都认不出来，
/// 反空集护栏也一起归零）。
///
/// 所以这一条把「粘出来的根」和「cargo 认的根」的差别显式钉住：要求它就是自己的规范形式。
#[test]
fn the_root_is_a_clean_path_comparable_to_cargos() {
    let root = repo_root();
    assert!(
        !carries_a_dot_dot(&root),
        "算出来的根里留着 `..`：{}。同一个目录的两种拼法，在按字面比路径的地方不相等——\
         别人 canonicalize 过的成员目录会和它对不上，而报告出来的形状会是「这棵树没有被\
         编译」，与真正的原因无关。",
        root.display()
    );
    let canonical = root
        .canonicalize()
        .unwrap_or_else(|e| panic!("{} 解不出规范形式：{e}", root.display()));
    assert_eq!(
        canonical,
        root,
        "根不是自己的规范形式（{} ≠ {}）：软链或未归一的写法会让同一目录的两次读数不相等",
        canonical.display(),
        root.display()
    );
}

/// 判据自检：认得出真定义，也认得出看着像定义的东西。
///
/// 少了这一段，`definitions` 哪天退化成一个裸子串匹配，上面那条照样绿——
/// 而它会开始把注释、文档和字符串里的字样都算成第二份定义。
#[test]
fn the_predicate_reads_definitions_and_not_mentions() {
    assert!(definitions("fn repo_root() -> PathBuf {"));
    assert!(definitions("    pub fn workspace_root() -> PathBuf {"));

    assert!(!definitions("/// fn repo_root() -> PathBuf {"));
    assert!(!definitions("// fn workspace_root() -> PathBuf {"));
    assert!(!definitions("let name = \"fn repo_root()\";"));
    assert!(!definitions("fn repo_rooted() -> PathBuf {"));
    // 收参数的 `workspace_root(data_dir)` 算的是用户工作区，不是这棵树。
    assert!(!definitions(
        "pub fn workspace_root(data_dir: &Path) -> PathBuf {"
    ));

    // 往上走的四种写法。
    assert!(climbs_out(
        "    PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\")"
    ));
    assert!(climbs_out("            .join(\"../../deploy/helm\")"));
    assert!(climbs_out(
        "    Path::new(env!(\"CARGO_MANIFEST_DIR\")).parent().parent()"
    ));
    assert!(climbs_out("    root.ancestors().nth(2)"));

    // 不往上走的四种。
    assert!(!climbs_out("    let path = base.join(\"src/metric.rs\");"));
    assert!(!climbs_out(
        "    let x = concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/migrations\");"
    ));
    // 转义的引号是标本，不是拼接：这条门自己就靠它不把自己数进去。
    assert!(!climbs_out(
        "    assert!(climbs_out(\"    join(\\\"../..\\\")\"));"
    ));
    // 编译器核的路径写错了构建就红，不在这里。
    assert!(the_compiler_checks_this_path(
        "    let t = include_str!(\"../../../deploy/k8s/x.yaml\");"
    ));
    assert!(the_compiler_checks_this_path(
        "    #[path = \"../../cogneva/tests/common/root.rs\"]"
    ));

    // 「根里有没有 `..`」这一问也要能红：真树今天是干净的，而干净与恒假的判据同形。
    assert!(carries_a_dot_dot(Path::new("/a/b/crates/cogneva/../..")));
    assert!(carries_a_dot_dot(Path::new("../..")));
    assert!(!carries_a_dot_dot(Path::new("/a/b/crates/cogneva")));
    // 名字里带两个点的目录不是往上走的一段。
    assert!(!carries_a_dot_dot(Path::new("/a/dir..with..dots/b")));
}

/// 消融对照：把一份本地定义放回一棵假树，两个面都必须红；放回**另一个 crate**
/// 的 `tests/` 里同样要红；放进 `src/` 里也要红——`src/` 的单测正是这次数出来的
/// 大半人口；只有编译器核的写法不算。
///
/// 只断真树的那条判据证明不了它还会响——真树今天恰好干净，而干净与瞎同形。
#[test]
fn a_local_copy_put_back_reddens_both_faces() {
    let root = std::env::temp_dir().join(format!("census-repo-root-{}", std::process::id()));
    let dir = root.join("crates/cogneva/tests");
    std::fs::create_dir_all(&dir).expect("a scratch tests directory");
    std::fs::write(
        dir.join("clean.rs"),
        "// fn repo_root() -> PathBuf { 注释里的不算\nconst X: u32 = 1;\n",
    )
    .expect("a file that only mentions");
    let (defs, climbs, files) = sites(&root, &crate_files_of(&root));
    assert_eq!(files, 1, "假树里的文件要读得到：{files}");
    assert!(defs.is_empty(), "注释里的字样不该算成定义：{defs:?}");
    assert!(climbs.is_empty());

    // 只提到变量名、不往上走的文件不算：crate 相对路径是正当用法。
    std::fs::write(
        dir.join("relative.rs"),
        "fn f() -> PathBuf { PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"src/x.rs\") }\n",
    )
    .expect("a crate-relative use");
    let (defs, climbs, _) = sites(&root, &crate_files_of(&root));
    assert!(defs.is_empty());
    assert!(
        climbs.is_empty(),
        "crate 相对路径不该被当成算根：{climbs:?}"
    );

    // 编译器核的路径也不算。
    std::fs::write(
        dir.join("included.rs"),
        "const T: &str = include_str!(\"../../x.yaml\");\nconst E: &str = \"CARGO_MANIFEST_DIR\";\n",
    )
    .expect("a compile-checked path");
    let (_, climbs, _) = sites(&root, &crate_files_of(&root));
    assert!(
        climbs.is_empty(),
        "编译器核的路径不该被当成算根：{climbs:?}"
    );

    std::fs::write(
        dir.join("copy.rs"),
        "fn workspace_root() -> PathBuf {\n    PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\")\n}\n",
    )
    .expect("the second copy");
    let (defs, climbs, _) = sites(&root, &crate_files_of(&root));
    assert_eq!(defs.len(), 1, "第二份定义要数得出来：{defs:?}");
    assert_eq!(climbs.len(), 1, "第二份算法也要数得出来：{climbs:?}");
    assert!(
        defs[0].0 != THE_ONE,
        "它不在算根的那一处，所以真树上的门会红：{defs:?}"
    );

    // 另一棵树：抄件长在别的 crate 的测试里，一个 crate 的目录都没碰。
    let other = root.join("crates/probe/tests");
    std::fs::create_dir_all(&other).expect("a second crate's tests directory");
    std::fs::write(
        other.join("copy.rs"),
        "fn repo_root() -> PathBuf { PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\") }\n",
    )
    .expect("a copy in another crate");
    let (defs, climbs, _) = sites(&root, &crate_files_of(&root));
    assert_eq!(
        defs.iter()
            .filter(|(file, _, _)| file == "crates/probe/tests/copy.rs")
            .count(),
        1,
        "别的 crate 的测试里的抄件也要数得出来，否则这份门只看得见 cogneva 自己：{defs:?}"
    );
    assert_eq!(climbs.len(), 2, "两份算法都要数得出来：{climbs:?}");

    // 同一个 crate 的 `src/`：这次数出来的大半人口就在这里，两个面都得算。
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).expect("a scratch source directory");
    std::fs::write(
        src.join("lib.rs"),
        "fn repo_root() -> PathBuf { PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\") }\n",
    )
    .expect("a source file");
    let (defs, climbs, _) = sites(&root, &crate_files_of(&root));
    assert_eq!(defs.len(), 3, "src/ 里的定义要数得出来：{defs:?}");
    assert_eq!(climbs.len(), 3, "src/ 里的算法要数得出来：{climbs:?}");

    std::fs::remove_dir_all(&root).ok();
}

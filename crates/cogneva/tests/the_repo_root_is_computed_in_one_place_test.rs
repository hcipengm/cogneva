//! 「仓库根在哪」只在一处算出来。
//!
//! 全仓的集成测试里曾有 80 余份 `fn repo_root()` / `fn workspace_root()`，十种写法：
//! `join("../..")`、`.parent().parent()`、`.ancestors().nth(2)`，有的 `canonicalize()`
//! 有的不。它们指同一个目录，所以平时看不出差别——差别只在某一处被改动的那天出现：
//! 根错一级的门会在原地读空，而读空与「这条判据没有题目」在输出上同形，绿得一样。
//!
//! 判据有两个面，因为它们各自都会长回来：
//!
//!   - 定义：`fn repo_root(` / `fn workspace_root(` 在**所有** crate 的 `tests/` 下只许
//!     有一处，且在 `crates/cogneva/tests/common/root.rs`；别的 crate 的测试按路径把
//!     那一份 `#[path]` 编进来，不另写一份；
//!   - 算法：靠 `env!("CARGO_MANIFEST_DIR")` 往上走的那一行也只许在这一处出现；
//!     反过来还要求这一处真的还在算根（起点仍是那个变量、认根的判据仍是 `[workspace]`），
//!     否则「别处没有」也可能只是「谁都不算了」。加了新名字、换个函数名，定义面会绿，
//!     算法面会红。
//!
//! 两面的对象都按出现形式判：注释里的 `fn repo_root()` 不是定义，登记表里的
//! `"CARGO_MANIFEST_DIR"` 字符串不是算法。
//!
//! 这份文件自己也在人口里。它举的标本因此写成「不起行、引号转义」的样子——标本若
//! 起行写成一个真的定义、或者真写出 `env!("CARGO_MANIFEST_DIR")`，第一条命中的就是
//! 它自己。这不是巧合而是要求：改标本写法时先想清楚这条门会怎么读它。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;
use common::rust_production::{all_crate_files, crate_files_of, relative_name};

/// 这一份普查的人口：每个 crate 的 `tests/` 下的全部 `.rs`。
///
/// 比「某个目录」宽，因为算根的写法长在每个 crate 的测试里，只盯着 cogneva 那一份
/// 就是把另外十几个 crate 的抄件留在盲区。
fn in_a_tests_directory(name: &str) -> bool {
    name.starts_with("crates/") && name.split('/').nth(2) == Some("tests")
}

/// 算根的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/root.rs";

/// 人口下限：全仓 `crates/*/tests` 今天有 300 余份文件，读到的比这少说明遍历没走到。
const MIN_FILES: usize = 150;

/// 一处命中：文件、行号、那行的样子。
type Site = (String, usize, String);

fn definitions(line: &str) -> bool {
    let trimmed = line.trim_start();
    let trimmed = trimmed.strip_prefix("pub ").unwrap_or(trimmed);
    for name in ["repo_root", "workspace_root"] {
        let head = format!("fn {name}");
        if let Some(rest) = trimmed.strip_prefix(&head) {
            let rest = rest.trim_start();
            if rest.starts_with("()") || rest.starts_with("(") {
                return true;
            }
        }
    }
    false
}

/// 这一行是在「算根」，还是只是在「指自己这个 crate」。
///
/// `CARGO_MANIFEST_DIR` 本身是 crate 目录，`join("src/metric.rs")`、`join("/migrations")`
/// 都是正当的 crate 相对路径，与根无关。算根的形状是**从 crate 目录往上走**：同一行里
/// 出现 `..`、`.parent()` 或 `.ancestors(`。只按裸子串判会把十几个 crate 的正当用法
/// 一起点名，那种门最后只有两种结局——被一条一条加进豁免，或被关掉。
fn computes_the_root(line: &str) -> bool {
    line.contains("env!(\"CARGO_MANIFEST_DIR\")")
        && (line.contains("..") || line.contains(".parent()") || line.contains("ancestors("))
}

fn sites(root: &Path, files_in: &[PathBuf]) -> (Vec<Site>, Vec<Site>, usize) {
    let mut defs = Vec::new();
    let mut envs = Vec::new();
    let mut files = 0usize;
    for path in files_in {
        let name = relative_name(root, path);
        if !in_a_tests_directory(&name) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        files += 1;
        // 原始行号：报出来的位置要能直接翻到。注释行跳过——`fn repo_root()` 写在
        // 说明里不是定义，而这份文件本身就在说明它。
        for (index, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if definitions(line) {
                defs.push((name.clone(), index + 1, line.trim().to_string()));
            }
            if computes_the_root(line) {
                envs.push((name.clone(), index + 1, line.trim().to_string()));
            }
        }
    }
    (defs, envs, files)
}

#[test]
fn the_repo_root_is_computed_in_one_place() {
    let root = repo_root();
    let (defs, envs, files) = sites(&root, &all_crate_files(&root));

    assert!(
        files >= MIN_FILES,
        "只读到 {files} 个测试源文件（下限 {MIN_FILES}）：遍历没走到，下面的「只有一处」不算数"
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

    let env_outside: Vec<&Site> = envs.iter().filter(|(file, _, _)| file != THE_ONE).collect();
    assert!(
        env_outside.is_empty(),
        "这些文件自己在算仓库根（`CARGO_MANIFEST_DIR` 加往上一级）：{env_outside:?}\n\
         换了个函数名不等于换了个答案——根仍然是从第二个地方算出来的。\
         crate 相对路径（`join(\"src/x.rs\")`）不算，那种用法不在此列。"
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

/// 判据自检：认得出真定义，也认得出三个看着像定义的东西。
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

    assert!(computes_the_root(
        "    PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\")"
    ));
    assert!(computes_the_root(
        "    let root = Path::new(env!(\"CARGO_MANIFEST_DIR\")).join(\"../../skills\");"
    ));
    assert!(computes_the_root(
        "    Path::new(env!(\"CARGO_MANIFEST_DIR\")).parent().and_then(Path::parent)"
    ));
    assert!(!computes_the_root(
        "    (\"CARGO_MANIFEST_DIR\", \"构建期由工具链注入\"),"
    ));
    // crate 相对路径不是根：指自己那个 crate 是正当用法，混进来就是一串假阳。
    assert!(!computes_the_root(
        "    let path = std::path::PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"src/metric.rs\");"
    ));
    assert!(!computes_the_root(
        "    cog_storage::migrate::Migrator::new(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/migrations\"))"
    ));

    // 「根里有没有 `..`」这一问也要能红：真树今天是干净的，而干净与恒假的判据同形。
    assert!(carries_a_dot_dot(Path::new("/a/b/crates/cogneva/../..")));
    assert!(carries_a_dot_dot(Path::new("../..")));
    assert!(!carries_a_dot_dot(Path::new("/a/b/crates/cogneva")));
    // 名字里带两个点的目录不是往上走的一段。
    assert!(!carries_a_dot_dot(Path::new("/a/dir..with..dots/b")));
}

/// 消融对照：把一份本地定义放回一棵假树，两个面都必须红；放回**另一个 crate**
/// 的 `tests/` 里同样要红；放进 `src/` 则不算——那是生产代码，不是门。
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
    let (defs, envs, files) = sites(&root, &crate_files_of(&root));
    assert_eq!(files, 1, "假树里的文件要读得到：{files}");
    assert!(defs.is_empty(), "注释里的字样不该算成定义：{defs:?}");
    assert!(envs.is_empty());

    std::fs::write(
        dir.join("copy.rs"),
        "fn workspace_root() -> PathBuf {\n    PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\")\n}\n",
    )
    .expect("the second copy");
    let (defs, envs, _) = sites(&root, &crate_files_of(&root));
    assert_eq!(defs.len(), 1, "第二份定义要数得出来：{defs:?}");
    assert_eq!(envs.len(), 1, "第二份算法也要数得出来：{envs:?}");
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
    let (defs, envs, _) = sites(&root, &crate_files_of(&root));
    assert_eq!(
        defs.iter()
            .filter(|(file, _, _)| file == "crates/probe/tests/copy.rs")
            .count(),
        1,
        "别的 crate 的测试里的抄件也要数得出来，否则这份门只看得见 cogneva 自己：{defs:?}"
    );
    assert_eq!(envs.len(), 2, "两份算法都要数得出来：{envs:?}");

    // 生产代码不算：判据问的是「测试里有几处在算根」，不是「谁提到了这个变量」。
    let src = root.join("crates/probe/src");
    std::fs::create_dir_all(&src).expect("a scratch source directory");
    std::fs::write(
        src.join("lib.rs"),
        "pub fn f() -> PathBuf { PathBuf::from(env!(\"CARGO_MANIFEST_DIR\")).join(\"../..\") }\n",
    )
    .expect("a source file");
    let (defs, envs, _) = sites(&root, &crate_files_of(&root));
    assert_eq!(defs.len(), 2, "src/ 里的定义不该被读进来：{defs:?}");
    assert_eq!(envs.len(), 2, "src/ 里的算法不该被读进来：{envs:?}");

    std::fs::remove_dir_all(&root).ok();
}

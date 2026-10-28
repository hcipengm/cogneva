//! 「仓库根在哪」只在一处算出来。
//!
//! 这个目录里曾有 75 份 `fn repo_root()` / `fn workspace_root()`，十种写法：`join("../..")`、
//! `.parent().parent()`、`.ancestors().nth(2)`，有的 `canonicalize()` 有的不。它们指同一个
//! 目录，所以平时看不出差别——差别只在某一处被改动的那天出现：根往上一级的门会在原地读
//! 空，而读空与「这条判据没有题目」在输出上同形，绿得一样。
//!
//! 判据有两个面，因为它们各自都会长回来：
//!
//!   - 定义：`fn repo_root(` / `fn workspace_root(` 在 `tests/` 下只许有一处，且在
//!     `common/routes.rs`；
//!   - 算法：`env!("CARGO_MANIFEST_DIR")` 也只许在这一处出现。加了新名字、换个函数名，
//!     定义面会绿，算法面会红。
//!
//! 两面的对象都按出现形式判：注释里的 `fn repo_root()` 不是定义，登记表里的
//! `"CARGO_MANIFEST_DIR"` 字符串不是算法。

use std::path::{Path, PathBuf};

mod common;

use common::routes::repo_root;
use common::rust_production::{all_crate_files, crate_files_of, relative_name};

/// 这一份普查的人口：`tests/` 下的全部 `.rs`。
const TESTS: &str = "crates/cogneva/tests/";

/// 算根的唯一定义。
const THE_ONE: &str = "crates/cogneva/tests/common/routes.rs";

/// 人口下限：这个目录今天有 190 余份文件，读到的比这少说明遍历没走到。
const MIN_FILES: usize = 100;

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

fn computes_the_root(line: &str) -> bool {
    line.contains("env!(\"CARGO_MANIFEST_DIR\")")
}

fn sites(root: &Path, files_in: &[PathBuf]) -> (Vec<Site>, Vec<Site>, usize) {
    let mut defs = Vec::new();
    let mut envs = Vec::new();
    let mut files = 0usize;
    for path in files_in {
        let name = relative_name(root, path);
        if !name.starts_with(TESTS) {
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
        "这些文件自己拿 `env!(\"CARGO_MANIFEST_DIR\")` 拼路径：{env_outside:?}\n\
         换了个函数名不等于换了个答案——根仍然是从第二个地方算出来的。"
    );
    assert_eq!(
        envs.len(),
        1,
        "`env!(\"CARGO_MANIFEST_DIR\")` 应当只出现在算根的那一处，读到 {} 处：{envs:?}",
        envs.len()
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
    assert!(!computes_the_root(
        "    (\"CARGO_MANIFEST_DIR\", \"构建期由工具链注入\"),"
    ));
}

/// 消融对照：把一份本地定义放回一棵假树，两个面都必须红。
///
/// 只断真树的那条判据证明不了它还会响——真树今天恰好干净，而干净与瞎同形。
#[test]
fn a_local_copy_put_back_reddens_both_faces() {
    let root = std::env::temp_dir().join(format!("census-repo-root-{}", std::process::id()));
    let dir = root.join(TESTS);
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

    std::fs::remove_dir_all(&root).ok();
}

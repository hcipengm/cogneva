//! 能杀工作的界，必须先登记才许存在。
//!
//! `bounds_registry_test` 判的是登记簿与代码不脱节：登记过的界必须还在代码里。
//! 它不判反方向——新加一条界、没有登记，那扇门是绿的。实测过：给每条沙箱命令写一个
//! `memory.max`，整套门 203 步全绿，而它的效果是在命令正干活时把它 OOM 杀掉。
//!
//! 这条门补的就是反方向：cgroup 的限值文件（`memory.max`、`memory.high`、`cpu.max`、
//! `pids.max`）在生产的 Rust 代码里出现一次，就必须在
//! `deploy/helm/cogneva/files/cogneva.json` 的 `bounds.kept` 里有一条点名这个文件的
//! 登记。登记不是形式：那里的 `why` 要说清它结束的东西为什么不是「正在产出的工作」。
//!
//! 今天一处也没有，这正是平台该有的样子：装不下就排队（队列与占槽是读数），不杀。
//! 哪天要加，先生成一条登记——也就是先回答「这条界结束的为什么不是正在干的活」。

use crate::bounds_registry_test::{kept, read_registry};
use crate::common;

use common::root::repo_root;
use common::rust_production::{production_sources, production_text, sources_of_tree};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// cgroup v2 里真正会切断一个进程的文件。`memory.current`、`memory.peak` 这些
/// 只是读数，读数不在这条门的题目里。
const LIMIT_FILES: [&str; 4] = ["memory.max", "memory.high", "cpu.max", "pids.max"];

/// 一个字符串常量如果就是限值文件名，它写出去的时候和字面量一样会切断进程，
/// 所以扫描要把这类名字也当成针：`const MEMORY_MAX_FILE: &str = "memory.max";`
/// 之后那一行 `fs::write(dir.join(MEMORY_MAX_FILE), ..)` 与直接写字符串同罪。
fn const_needles(production: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in production.lines() {
        let code = line.split("//").next().unwrap_or("");
        if !(code.trim_start().starts_with("const ") || code.trim_start().starts_with("pub const "))
        {
            continue;
        }
        for limit in LIMIT_FILES {
            if code.contains(&format!("\"{limit}\"")) {
                let name = code
                    .trim_start()
                    .trim_start_matches("pub ")
                    .trim_start_matches("const ")
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if !name.is_empty() {
                    out.push((name, limit.to_string()));
                }
            }
        }
    }
    out
}

/// 生产的 Rust 源码里，哪些文件把限值**写下去**了，写的是哪一个。
///
/// 读是一条读数，写是一次切断：`read_to_string(root.join("memory.max"))` 不该在这条
/// 门的题目里，`fs::write(dir.join("memory.max"), ..)` 必须。判据是同一行里既有某个
/// 写调用、又有那个名字——把读、常量声明与注释都留在门外。
fn ceiling_sites(root: &Path, files: &[PathBuf]) -> Vec<(String, String)> {
    let mut sites = Vec::new();
    for path in files {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        // 门只判生产代码：夹具里写一个限值文件是为了验解析，不是为了切断谁。
        // 切在哪里的那一份算法住在 `common::rust_production`，调用点只挑投影。
        let production = production_text(&text);
        let mut needles: Vec<(String, String)> = LIMIT_FILES
            .iter()
            .map(|limit| (format!("\"{limit}\""), limit.to_string()))
            .collect();
        needles.extend(const_needles(&production));
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path)
            .display()
            .to_string();
        for line in production.lines() {
            let code = line.split("//").next().unwrap_or("");
            if !code.contains("write") {
                continue;
            }
            for (needle, limit) in &needles {
                if code.contains(needle.as_str()) {
                    sites.push((rel.clone(), limit.clone()));
                }
            }
        }
    }
    sites.sort();
    sites.dedup();
    sites
}

/// 有落点、却没有一条点名它的登记。这就是这条门的红。
fn unregistered(root: &Path, files: &[PathBuf], registry: &Value) -> Vec<String> {
    let registered: Vec<String> = kept(registry)
        .iter()
        .filter_map(|entry| entry.get("file").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    ceiling_sites(root, files)
        .into_iter()
        .filter(|(file, _)| !registered.contains(file))
        .map(|(file, limit)| format!("{file}：写了 {limit}，bounds.kept 里没有这一份文件的登记"))
        .collect()
}

/// 今天一处也没有。加一处就会红，红的判词点名文件与限值文件。
#[test]
fn nothing_in_production_cuts_a_process_off_without_a_registered_bound() {
    let root = repo_root();
    let violations = unregistered(&root, &production_sources(&root), &read_registry());
    assert!(
        violations.is_empty(),
        "这些地方会给一个正在干活的进程设界，而登记簿里没有它们。\
         要么拿掉这条界（装不下就排队才是本平台的答案），要么在 bounds.kept 里登记并\
         写清它结束的东西为什么不是正在产出的工作：\n  {}",
        violations.join("\n  ")
    );
}

/// 扫描器本身不许是假的：给它一个真写了限值文件的目录，它必须报出来。
#[test]
fn the_scan_reports_a_site_it_is_given() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("crates").join("cog-fixture").join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("limiter.rs"),
        "fn cage(dir: &std::path::Path) {\n    std::fs::write(dir.join(\"memory.max\"), \"1\").unwrap();\n}\n",
    )
    .unwrap();

    let sites = ceiling_sites(dir.path(), &sources_of_tree(dir.path()));
    assert_eq!(
        sites,
        vec![(
            "crates/cog-fixture/src/limiter.rs".to_string(),
            "memory.max".to_string()
        )],
        "扫描器没看见摆在它面前的界"
    );

    let empty = serde_json::json!({ "kept": [] });
    assert_eq!(
        unregistered(dir.path(), &sources_of_tree(dir.path()), &empty).len(),
        1,
        "没有登记的界必须判红"
    );

    // 换个写法绕不过去：把文件名放进常量、写的时候用常量，写出去的还是同一次切断。
    std::fs::write(
        src.join("named.rs"),
        "pub const MEMORY_MAX_FILE: &str = \"memory.max\";\n\
         pub fn cage(dir: &std::path::Path) {\n    \
         std::fs::write(dir.join(MEMORY_MAX_FILE), \"1\").unwrap();\n}\n",
    )
    .unwrap();
    let through_a_constant = ceiling_sites(dir.path(), &sources_of_tree(dir.path()));
    assert!(
        through_a_constant
            .iter()
            .any(|(file, limit)| file.ends_with("named.rs") && limit == "memory.max"),
        "常量拼出来的写躲过了扫描：{through_a_constant:?}"
    );
}

/// 只写进夹具、只被解析的那种出现不算：那是读数，不是切断。
#[test]
fn a_limit_file_named_only_in_a_test_module_is_not_a_site() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("crates").join("cog-fixture").join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("reader.rs"),
        "pub fn peak(dir: &std::path::Path) -> Option<u64> { read(dir.join(\"memory.peak\")) }\n\n\
         #[cfg(test)]\nmod tests {\n    #[test]\n    fn reads() { std::fs::write(\"memory.max\", \"1\"); }\n}\n",
    )
    .unwrap();

    assert!(
        ceiling_sites(dir.path(), &sources_of_tree(dir.path())).is_empty(),
        "夹具里的限值文件被当成了生产代码里的界"
    );
}

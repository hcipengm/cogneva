//! principle: feedback_a_prohibitive_gate_needs_a_counterexample_census_first
//!
//! 一处超时不能是当场typed-in的一个数字：它必须是一个**有名字的值**，而那个名字旁边
//! 写着它在等什么、等不到留下什么、能不能续跑。
//!
//! 为什么是「有名字」这一步先立、而不是一步到位要求三问都写完：普查的读数（2026-11-20）
//! 是 171 处调用点里 19 处传裸字面量（11%），只落在四个值上。先禁字面量，这道门一开
//! 就是零处红；先要求三问全写，它一开就红 171 处，而一条一开就红的判据最后一定会被
//! `--no-verify` 绕过去，绕过之后就再也没人看它。
//!
//! 名字那一半由人来写（每个常量的文档块里三问一行），因为「等不到时留下什么事件」
//! 只有写那段代码的人答得出；机器能查的是**有没有名字**，以及那个名字是不是常量。
//!
//! 唯一的例外写在行上：测试自己的等待（`// wall: test`）。用例里的等待是夹具的一部分，
//! 不是这个系统的界——把它也钉成常量，只会让一条用例读起来像产品代码。

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the crate sits two levels below the repository root")
        .to_path_buf()
}

fn production_files(root: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|n| n.to_str()) != Some("tests") {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("rs")
                && path.components().any(|c| c.as_os_str() == "src")
            {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    out.push((path, text));
                }
            }
        }
    }
    out
}

/// `.timeout(` 后面紧跟一个写死的数，无论有没有 `Duration::from_*` 包一层。
fn literal_after_timeout(line: &str) -> bool {
    // 注释里写着 `.timeout(30)` 是在讲这件事，不是在做这件事。
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") || trimmed.starts_with('*') {
        return false;
    }
    let Some(at) = line.find(".timeout(") else {
        return false;
    };
    let arg = line[at + ".timeout(".len()..].trim_start();
    // `def.timeout()` 是一个取值的调用，不是设界：它的参数位置是空的，后面跟的东西
    // 属于同一行的别的表达式。
    if arg.starts_with(')') {
        return false;
    }
    let bare = |s: &str| s.starts_with(|c: char| c.is_ascii_digit());
    if bare(arg) {
        return true;
    }
    for ctor in [
        "Duration::from_secs(",
        "Duration::from_millis(",
        "Duration::from_secs_f64(",
    ] {
        if let Some(rest) = arg.strip_prefix(ctor) {
            return bare(rest.trim_start());
        }
    }
    // `std::time::Duration::from_secs(…)` 与上面同一条规则，只是路径更长。
    if let Some(pos) = arg.find("Duration::from_") {
        let after = &arg[pos..];
        if let Some(open) = after.find('(') {
            return bare(after[open + 1..].trim_start());
        }
    }
    false
}

/// 每一处超时都传一个有名字的值；测试自己的等待在行上标 `// wall: test`。
#[test]
fn no_timeout_passes_a_written_number() {
    let root = repo_root();
    let mut offenders: Vec<String> = Vec::new();
    let mut seen = 0usize;
    for (path, text) in production_files(&root) {
        for (index, line) in text.lines().enumerate() {
            if !line.contains(".timeout(") {
                continue;
            }
            seen += 1;
            if !literal_after_timeout(line) || line.contains("// wall: test") {
                continue;
            }
            offenders.push(format!(
                "{}:{}  {}",
                path.strip_prefix(&root).unwrap_or(&path).display(),
                index + 1,
                line.trim()
            ));
        }
    }
    assert!(
        seen > 30,
        "只扫到 {seen} 处 `.timeout(`：走树的这一段坏了，不是树干净"
    );
    assert!(
        offenders.is_empty(),
        "这些超时当场写了一个数字：把它提成一个具名常量，并在它的文档块里写清\
         「在等什么 / 等不到留下什么事件 / 能不能续跑」。用例自己的等待可以在行尾标 \
         `// wall: test`：\n{}",
        offenders.join("\n")
    );
}

/// 判据本身要被判：包装过的字面量与裸字面量一样拦住，具名常量放行。
#[test]
fn the_rule_sees_through_a_duration_wrapper() {
    assert!(literal_after_timeout("            .timeout(30);"));
    assert!(literal_after_timeout(".timeout(Duration::from_secs(5))"));
    assert!(literal_after_timeout(
        ".timeout(std::time::Duration::from_millis(250))"
    ));
    assert!(!literal_after_timeout(".timeout(REQUEST_TIMEOUT_SECS)"));
    assert!(!literal_after_timeout(".timeout(self.timeout_secs)"));
    assert!(!literal_after_timeout(".timeout(lease.remaining_secs())"));
    assert!(!literal_after_timeout("// .timeout(30) 只是注释里的例子"));
    // `def.timeout()` 是取值不是设界：参数位置是空的，同行后面的数字不属于它。
    assert!(!literal_after_timeout(
        "assert_eq!(def.timeout(), Duration::from_millis(5000));"
    ));
}

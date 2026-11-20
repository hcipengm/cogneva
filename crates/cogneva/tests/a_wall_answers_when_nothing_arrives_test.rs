//! principle: feedback_a_prohibitive_gate_needs_a_counterexample_census_first
//!
//! 一个叫「超时／deadline／墙」的常量，要在自己的文档块里回答三件事：
//!
//!   1. **在等什么**——一次往返？一帧？一个子进程？
//!   2. **等不到时留下什么事件**——一次错误？一条放弃记录？什么都没有？
//!   3. **能不能续跑**——重来一遍是新的调用，还是从断点接着读？
//!
//! 第三问是最要紧的一条：一个答不出「等不到时留下什么」的等待，到了那一刻系统只会
//! 安静地少做一件事，而读数上看不出少的是什么。
//!
//! **为什么是棘轮而不是一次立满。** 普查（2026-11-20）是 70 个数字型的墙常量，11 个
//! 已经答了，59 个没有。要求 59 条立刻写全，只会逼出一批「等不到时什么也不留」的假
//! 答案——写它的人未必知道，而假答案比空白更难查。所以这张表记的是**每文件还欠几条**：
//! 数字只许降，不许升；答掉一条就把那一行改小，改到零就把那一行删掉。一个新写的墙常量
//! 会让某个文件的数变大，这道门当场红。
//!
//! **为什么按名字筛的时候还要看类型。** 按名字一刀切得到的第一个数是 93，里面混着
//! 结局词（`PROGRESS_DEADLINE_EXCEEDED`）、环境变量名（`AUDIT_BUDGET_ENV`）、token
//! 预算（`DEFAULT_EXTRACTION_INPUT_BUDGET_TOKENS`）和哨兵值。所以入册的条件是
//! **名字像墙 且 初值是数字或 `Duration`**——只看名字会一开就红一堆不是墙的东西。
//!
//! 行上的答案用一行写：`// wall: 在等什么 / 等不到留下什么 / 能不能续跑`。它和
//! `// cap:` 那一行并列，两行说的是两件事：`cap` 说这个数从哪儿来，`wall` 说这条等待
//! 结束时世界剩下什么。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// 每文件还欠几条三问。只许降：答掉一条就把数改小，清零就删掉这一行。
const UNANSWERED: &[(&str, u32)] = &[
    ("crates/bootstrap/src/apt.rs", 1),
    ("crates/bootstrap/src/main.rs", 1),
    ("crates/cog-agent/src/archive.rs", 1),
    ("crates/cog-agent/src/plugin.rs", 1),
    ("crates/cog-core/src/contract/hook.rs", 1),
    ("crates/cog-extension/src/hostdocs_assist.rs", 1),
    ("crates/cog-extension/src/runtime/remote.rs", 2),
    ("crates/cog-gateway/src/cluster.rs", 1),
    ("crates/cog-gateway/src/git_mirror.rs", 5),
    ("crates/cog-gateway/src/git_routing.rs", 2),
    ("crates/cog-gateway/src/llm_admin.rs", 2),
    ("crates/cog-gateway/src/readings.rs", 1),
    ("crates/cog-gateway/src/security_gateway.rs", 6),
    ("crates/cog-github/src/discovery_loop.rs", 1),
    ("crates/cog-memory/src/extractor.rs", 1),
    ("crates/cog-memory/src/vector_summary_backend.rs", 1),
    ("crates/cog-reflection/src/mainline_deployer.rs", 1),
    ("crates/cog-reflection/src/promotion_eval.rs", 1),
    ("crates/cog-reflection/src/volume_capacity.rs", 1),
    ("crates/cog-reflection/src/workload_resources.rs", 1),
    ("crates/cog-storage/src/backends/object_backends.rs", 1),
    ("crates/cog-storage/src/postgres/metrics_backend.rs", 1),
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the crate sits two levels below the repository root")
        .to_path_buf()
}

fn production_files(root: &Path) -> Vec<PathBuf> {
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
                out.push(path);
            }
        }
    }
    out
}

/// `const NAME: T = value;` 里 NAME 像一道墙、T 是一个数、value 里写着数。
fn wall_constant(line: &str) -> Option<&str> {
    let rest = line.trim_start();
    // `pub const` 与 `pub(crate) const` 都是常量声明：漏掉后一种，它下面的墙就永远
    // 不在册，而「不在册」与「答完了」在读数上是同一句话。
    let rest = match rest.strip_prefix("pub ") {
        Some(tail) => tail,
        None => match rest.strip_prefix("pub(") {
            Some(tail) => match tail.split_once(')') {
                Some((_, after)) => after.trim_start(),
                None => return None,
            },
            None => rest,
        },
    };
    let rest = match rest.strip_prefix("const ") {
        Some(r) => r,
        None => return None,
    };
    let (name, tail) = rest.split_once(':')?;
    let name = name.trim();
    if !(name.contains("TIMEOUT") || name.contains("DEADLINE") || name.contains("WALL")) {
        return None;
    }
    let (ty, value) = tail.split_once('=')?;
    let ty = ty.trim();
    let numeric = ["u8", "u16", "u32", "u64", "usize", "i64", "f64", "Duration"]
        .iter()
        .any(|k| ty.contains(k));
    if !numeric {
        return None;
    }
    if !value.chars().any(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(name)
}

/// 这一条三问。`// wall: …` 写在常量上面那一块里。
const ANSWER: &str = "// wall:";

#[test]
fn every_wall_constant_answers_what_is_left_when_nothing_arrives() {
    let root = repo_root();
    let baseline: BTreeMap<&str, u32> = UNANSWERED.iter().copied().collect();
    let mut actual: BTreeMap<String, u32> = BTreeMap::new();
    let mut seen_walls = 0usize;

    for path in production_files(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .display()
            .to_string();
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if wall_constant(line).is_none() {
                continue;
            }
            seen_walls += 1;
            let from = index.saturating_sub(12);
            let block = lines[from..index].join("\n");
            if !block.contains(ANSWER) {
                *actual.entry(rel.clone()).or_insert(0) += 1;
            }
        }
    }

    assert!(
        seen_walls > 40,
        "只认到 {seen_walls} 个墙常量：扫描写坏了，不是树干净了"
    );

    let mut grew = Vec::new();
    let mut stale = Vec::new();
    for (file, count) in &actual {
        match baseline.get(file.as_str()) {
            Some(allowed) if count > allowed => grew.push(format!(
                "{file}: 还欠 {count} 条，表里写着 {allowed}——新写的墙常量要带一行 `// wall: …`"
            )),
            Some(allowed) if count < allowed => stale.push(format!(
                "{file}: 还欠 {count} 条，表里写着 {allowed}——把这一行改小"
            )),
            None => grew.push(format!(
                "{file}: 这个文件不在表里，现在有 {count} 条墙常量没有三问"
            )),
            _ => {}
        }
    }
    for (file, allowed) in &baseline {
        if !actual.contains_key(*file) {
            stale.push(format!(
                "{file}: 表里写着 {allowed} 条，实际一条都不欠了——删掉这一行"
            ));
        }
    }

    assert!(
        grew.is_empty(),
        "这些墙常量没有回答「在等什么 / 等不到留下什么事件 / 能不能续跑」。这条棘轮只许降：\n{}",
        grew.join("\n")
    );
    assert!(
        stale.is_empty(),
        "表比树旧了。答完一条就把数改小、清零就删行，否则这张表会慢慢变成一段没人读的历史：\n{}",
        stale.join("\n")
    );
}

/// 入册的判据本身要被判：名字像墙但不是墙的东西不该进来。
#[test]
fn the_scan_tells_a_wall_from_a_word_that_looks_like_one() {
    assert_eq!(
        wall_constant("const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);"),
        Some("CONNECT_TIMEOUT")
    );
    assert_eq!(
        wall_constant("pub const READ_TIMEOUT_SECS: u64 = 60;"),
        Some("READ_TIMEOUT_SECS")
    );
    // 结局词：它是「哪一种失败」，不是「等多久」。
    assert_eq!(
        wall_constant("const PROGRESS_DEADLINE_EXCEEDED: &str = \"exceeded\";"),
        None
    );
    // 环境变量名：一个键，不是一个时长。
    assert_eq!(
        wall_constant("const AUDIT_BUDGET_ENV: &str = \"AUDIT_BUDGET\";"),
        None
    );
    // 没有数字的初值不是时长。
    assert_eq!(wall_constant("const WALL_KIND: &str = DEADLINE;"), None);
}

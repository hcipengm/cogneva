//! 一天的名字只有一个来源：本进程时钟。
//!
//! 平台里按日切的东西有四处——配额日切、用量历史日键、原始日志文件名、分区维护选月——
//! 它们必须落在同一个午夜上。各自读一次钟时，最先分家的是时区：一处 `Utc::now()`、
//! 一处 `Local::now()`，在 UTC 机器上永远相等，在这台 +08 的机器上差八小时。症状不是
//! 报错：配额在历史行写下的那一天之外重置，日志按写入方的那一天切、由读取方的另一天
//! 去找，而两边读数都各自正常。
//!
//! 判据：`crates/` 下非测试代码里，取「今天」只许经过 `cog_core::day`（`day_start_unix`
//! / `local_date_naive` / `seconds_to_next_day` 那条投影）。唯一允许自己读钟的地方是
//! 投影本身——`crates/cog-core/src/day.rs`，它读一次并把偏移交出去。
//!
//! 为什么是文本判据而不是行为判据：行为上的差别只在本机时区不为 0 时出现。CI 跑在
//! UTC 上，两处读钟在那里恰好相等——那样的测试在 CI 上永远是绿的，抓不到它要抓的东西。
//! 这里判的是形式本身：`Utc::now().date_naive()` 与 `Local::now().date_naive()` 这两种
//! 写法不再出现在投影之外。
//!
//! 看不见什么，写在明处：它不追 `DateTime::from_timestamp(...).date_naive()` 这类绕路，
//! 也不判 `chrono::NaiveDate::from_ymd_opt(...)` 拼出来的日期——那些要么是夹具，要么
//! 显式带着日期分量，读到的人立刻看得出是哪一个时区。它只钉住「此刻是哪一天」这一个
//! 问题的两种最常见答案。
//!
//! 内联测试不在人口里：一个 `#[cfg(test)] mod tests` 里构造的日期是夹具，不是平台的
//! 日界。所以每个文件只读到第一个 `#[cfg(test)]` 为止。

use std::path::{Path, PathBuf};

use crate::common;

use common::routes::repo_root;

/// 允许自己读钟的那一处：日界投影的定义。
const PROJECTION: &str = "crates/cog-core/src/day.rs";

/// 两种「自己回答今天是哪一天」的写法。
const OWN_CLOCK_FORMS: &[&str] = &["Utc::now().date_naive()", "Local::now().date_naive()"];

/// 人口下限：扫不到东西与全都干净在输出上同形。
const MIN_FILES: usize = 200;

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// 文件里 `#[cfg(test)]` 之前的那部分：平台的代码，不含内联测试。
fn production_lines(text: &str) -> Vec<&str> {
    text.lines()
        .take_while(|line| !line.trim_start().starts_with("#[cfg(test)]"))
        .collect()
}

#[test]
fn only_the_day_projection_reads_the_clock() {
    let root = repo_root();
    let crates = root.join("crates");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates)
        .unwrap_or_else(|e| panic!("读不到 {}: {e}", crates.display()))
        .flatten()
    {
        let src = entry.path().join("src");
        if src.is_dir() {
            rust_sources(&src, &mut files);
        }
    }
    assert!(
        files.len() >= MIN_FILES,
        "只扫到 {} 份源文件，人口不足 {MIN_FILES}——目录名写错时这条门会静默变绿",
        files.len()
    );

    let mut offenders = Vec::new();
    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        if rel == PROJECTION {
            continue;
        }
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("读不到 {}: {e}", path.display()));
        for (n, line) in production_lines(&text).iter().enumerate() {
            if OWN_CLOCK_FORMS.iter().any(|f| line.contains(f)) {
                offenders.push(format!("{rel}:{}: {}", n + 1, line.trim()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "这些地方自己回答了「今天是哪一天」，绕开了那条投影，改参考时区时它们不会跟着变：\n{}",
        offenders.join("\n")
    );
}

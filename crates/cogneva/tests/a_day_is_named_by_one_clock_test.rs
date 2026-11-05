//! 一天的名字只有一个来源：本进程时钟。
//!
//! 平台里按日切的东西有四处——配额日切、用量历史日键、原始日志文件名、分区维护选月——
//! 它们必须落在同一个午夜上。各自读一次钟时，最先分家的是时区：一处 `Utc::now()`、
//! 一处 `Local::now()`，在 UTC 机器上永远相等，在这台 +08 的机器上差八小时。症状不是
//! 报错：配额在历史行写下的那一天之外重置，日志按写入方的那一天切、由读取方的另一天
//! 去找，而两边读数都各自正常。
//!
//! 判据：`crates/` 下随产物跑的源码里，取「今天」只许经过 `cog_core::day`（`day_start_unix`
//! / `local_date_naive` / `seconds_to_next_day` 那条投影）。唯一允许自己读钟的地方是
//! 投影本身——`crates/cog-core/src/day.rs`，它读一次并把偏移交出去。
//!
//! 为什么是文本判据而不是行为判据：行为上的差别只在本机时区不为 0 时出现。CI 跑在
//! UTC 上，两处读钟在那里恰好相等——那样的测试在 CI 上永远是绿的，抓不到它要抓的东西。
//! 这里判的是形式本身：`Utc::now().date_naive()` 与 `Local::now().date_naive()` 这两种
//! 写法不再出现在投影之外。
//!
//! 看不见什么，写在明处：它不追 `DateTime::from_timestamp(...).date_naive()` 这类绕路，
//! 也不判 `NaiveDate::from_ymd_opt(...)` 拼出来的日期——那些要么是夹具，要么显式带着
//! 日期分量，读到的人立刻看得出是哪一个时区。它只钉住「此刻是哪一天」这一个问题的两种
//! 最常见答案。
//！
//! 内联测试不在人口里：一个只给用例用的 `mod` 里构造的日期是夹具，不是平台的日界。
//! 切口用共享的那一份算（`common::rust_production`），不在这里再切一次——切错的形状
//! 是「文件中间一个 test-only 项之后全被丢掉」，那一半产出会静默退出普查。

use crate::common;

use common::routes::repo_root;
use common::rust_production::{production_numbered_lines, production_sources, relative_name};

/// 允许自己读钟的那一处：日界投影的定义。
const PROJECTION: &str = "crates/cog-core/src/day.rs";

/// 两种「自己回答今天是哪一天」的写法。
const OWN_CLOCK_FORMS: &[&str] = &["Utc::now().date_naive()", "Local::now().date_naive()"];

#[test]
fn only_the_day_projection_reads_the_clock() {
    let root = repo_root();

    let mut offenders = Vec::new();
    for path in production_sources(&root) {
        let rel = relative_name(&root, &path);
        if rel == PROJECTION {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("读不到 {}: {e}", path.display()));
        for (number, line) in production_numbered_lines(&text) {
            if OWN_CLOCK_FORMS.iter().any(|form| line.contains(form)) {
                offenders.push(format!("{rel}:{number}: {}", line.trim()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "这些地方自己回答了「今天是哪一天」，绕开了那条投影，改参考时区时它们不会跟着变：\n{}",
        offenders.join("\n")
    );
}

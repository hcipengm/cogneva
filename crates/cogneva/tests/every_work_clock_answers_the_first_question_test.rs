//! 每一个能取消一段「活」的时钟构造，都在它所在的那一行回答四问。
//!
//! 判的顺序是固定的：**第一问是「能不能删」**；不能删才说理由，说清之后才轮到
//! 三问——**在等什么 / 等不到留下什么 / 能不能续跑**。连接、一次往返级别的等待是
//! 唯一的例外，但它们不许沉默：写明是哪一次往返（`// roundtrip: …`），因为「这是
//! 一次往返」本身也是一条判词，读的人要能看出来它不是一段活的墙。
//!
//! 为什么判调用点而不只判常量：`// wall:` 的棘轮只盖得住**写下了**墙的那几行常量，
//! 而 `tokio::time::timeout(...)` 可以直接包住一个 future——包住的是「等一个消费者
//! 退场」「等一次上游流的下一段」还是「等一次 TCP 握手」，常量那一侧看不出来。于是
//! 一段可续的活在调用点被按钟切掉时，账上是干净的：常量有判词，切的那一处没有。
//!
//! 2026-11-27 实测：生产代码里 47 处 `tokio::time::timeout(`，只有 5 处旁边有 `// wall:`。

use crate::common::routes::repo_root;
use crate::common::rust_production::{
    production_numbered_lines_keeping_comments, production_texts,
};

/// 判词可以写在调用行的上面几行（多行参数把调用拆开时，判词落在第一行之前）。
const LOOKBACK: usize = 6;

/// 一次时钟构造在它旁边写下的那条判词。
#[derive(Debug, PartialEq, Eq)]
enum Account<'a> {
    Wall {
        waits: bool,
        leftover: bool,
        resumes: bool,
    },
    RoundTrip(&'a str),
}

fn account_beside<'a>(lines: &[&'a str], index: usize) -> Option<Account<'a>> {
    let start = index.saturating_sub(LOOKBACK);
    for line in lines[start..=index].iter().rev() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("// wall:") {
            return Some(Account::Wall {
                waits: rest.contains("等="),
                leftover: rest.contains("等不到="),
                resumes: rest.contains("续跑="),
            });
        }
        if let Some(rest) = trimmed.strip_prefix("// roundtrip:") {
            return Some(Account::RoundTrip(rest.trim()));
        }
    }
    None
}

fn calls_a_work_clock(line: &str) -> bool {
    let trimmed = line.trim_start();
    !trimmed.starts_with("//") && line.contains("tokio::time::timeout(")
}

/// 判据自己先证明会响：无判词要红，三问缺一项要红，往返例外要写明是哪一次。
#[test]
fn the_predicate_names_the_shapes_it_forbids() {
    let none = ["let x = tokio::time::timeout(window, next()).await;"];
    assert_eq!(account_beside(&none, 0), None);

    let partial = [
        "// wall: 等=上游的下一个事件 / 等不到=这一跳收尾",
        "let x = tokio::time::timeout(window, next()).await;",
    ];
    assert_eq!(
        account_beside(&partial, 1),
        Some(Account::Wall {
            waits: true,
            leftover: true,
            resumes: false
        })
    );

    let full = [
        "// wall: 等=上游的下一个事件 / 等不到=这一跳收尾 / 续跑=能，新开一次调用",
        "let x = tokio::time::timeout(window, next()).await;",
    ];
    assert_eq!(
        account_beside(&full, 1),
        Some(Account::Wall {
            waits: true,
            leftover: true,
            resumes: true
        })
    );

    let roundtrip = [
        "// roundtrip: 一次 TCP 握手",
        "let x = tokio::time::timeout(window, connect()).await;",
    ];
    assert_eq!(
        account_beside(&roundtrip, 1),
        Some(Account::RoundTrip("一次 TCP 握手"))
    );

    // 注释里的字不算调用。
    assert!(!calls_a_work_clock("// tokio::time::timeout(x, y)"));
    assert!(calls_a_work_clock("    tokio::time::timeout(x, y).await"));
}

/// 生产代码里每一处能取消一段活的时钟构造都写了判词：要么四问，要么写明它只是一次
/// 往返。没有这次改动这条会红：新加一处不带判词的 `timeout(...)` 不会让任何命令失败。
#[test]
fn every_work_clock_answers_the_first_question() {
    let root = repo_root();
    let mut problems = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        let numbered = production_numbered_lines_keeping_comments(&text);
        let lines: Vec<&str> = numbered.iter().map(|(_, line)| *line).collect();
        for (index, (number, line)) in numbered.iter().enumerate() {
            if !calls_a_work_clock(line) {
                continue;
            }
            match account_beside(&lines, index) {
                Some(Account::Wall {
                    waits: true,
                    leftover: true,
                    resumes: true,
                }) => {}
                Some(Account::Wall { .. }) => problems.push(format!(
                    "{relative}:{number}: 墙的判词三问不全（要 等= / 等不到= / 续跑=）：{}",
                    line.trim()
                )),
                Some(Account::RoundTrip(reason)) if !reason.is_empty() => {}
                Some(Account::RoundTrip(_)) => problems.push(format!(
                    "{relative}:{number}: `// roundtrip:` 后面要写明是哪一次往返：{}",
                    line.trim()
                )),
                None => problems.push(format!(
                    "{relative}:{number}: 这一处时钟旁边没有判词。第一问是「能不能删」；不能删就写 \
                     `// wall: 等=… / 等不到=… / 续跑=…`，只是连接或一次往返就写 \
                     `// roundtrip: <哪一次往返>`：{}",
                    line.trim()
                )),
            }
        }
    }
    assert!(
        problems.is_empty(),
        "有时钟构造没有回答问题：\n  {}",
        problems.join("\n  ")
    );
}

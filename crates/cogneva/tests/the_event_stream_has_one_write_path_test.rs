//! principle: feedback_a_switch_that_exists_before_its_reading_is_a_decision_nobody_measured
//!
//! 事件流在**四条触发读数都为假**的时候，只能有一条写路径：直接写 PostgreSQL。
//!
//! 换存储不是「PG 换 NATS」，而是写入口前移（JetStream 收追加、物化器批量写 PG、PG 仍是
//! 正本）。那条路只有在下面四条之一量到为真时才允许存在：
//!
//!   1. 追加 p99 持续大于 UI 刷新周期的一半
//!   2. `event_stream_lag_seconds{kind}` 持续上升
//!   3. vacuum / 最老事务年龄追不上
//!   4. `store_growth` 的余量除以增速，短于保留目标
//!
//! 今天这四条一条都还没有发布，所以**切换项不许先在树里**：一个还没被读数要出来的
//! 开关，是有人先拍了板、再去找理由。NATS 作为进程间总线已经存在，那是另一件事——
//! 它不做 `cogneva_event` 的写入口。
//!
//! 判据是树判据，不是运行读数：`cogneva_event` 的写语句只出现在那一个 store 里，且
//! 没有任何地方用 JetStream 发布这一类事件。

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

/// `cogneva_event` 的插入只允许出现在一个文件里：换存储之前，写入口只有一处。
///
/// 这条判据报的不是「谁写了」，是「有几条写路径」：第二处出现就是第二条路径，无论它当时
/// 被哪个开关挡着——开关默认关着的写路径，与没有这条路径，在树里长得不一样，而在读数上
/// 一样（都不写），所以只判树。
#[test]
fn the_event_stream_has_exactly_one_insert_site() {
    let root = repo_root();
    let mut inserts: Vec<String> = Vec::new();
    for (path, text) in production_files(&root) {
        let upper = text.to_uppercase();
        if upper.contains("INSERT INTO COGNEVA_EVENT") {
            inserts.push(
                path.strip_prefix(&root)
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }
    assert_eq!(
        inserts.len(),
        1,
        "事件流的写入口应当只有一处（换存储之前），实得有 {} 处：{inserts:?}",
        inserts.len()
    );
}

/// 没有 JetStream 发布 `cogneva_event` 的路径。
///
/// 这条判据与上一条分开写：一个把追加塞进总线、再由物化器写库的实现，可以只有一处
/// `INSERT`，却已经有两条写路径了。
#[test]
fn no_bus_publishes_the_event_stream() {
    let root = repo_root();
    let mut publishers: Vec<String> = Vec::new();
    for (path, text) in production_files(&root) {
        if !text.contains("jetstream") && !text.contains("JetStream") {
            continue;
        }
        if text.contains("cogneva_event") || text.contains("COGNEVA_EVENT") {
            publishers.push(
                path.strip_prefix(&root)
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }
    assert!(
        publishers.is_empty(),
        "这些文件同时提到 JetStream 与事件流：切换项不能在四条触发读数之前先存在：{publishers:?}"
    );
}

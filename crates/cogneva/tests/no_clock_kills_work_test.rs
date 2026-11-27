//! 部署面不许再把「到点杀工作」的固定钟加回来。
//!
//! 时钟界只在三种形状下允许存在：等待器（到点只让位）、带归因的 stall 杀、
//! 外部硬限。`activeDeadlineSeconds` 是第一种之外的东西：它按钟杀掉整件活，且不区分
//! 「在动」与「空转」。本门判部署清单里没有它（注释除外）。
//!
//! 没有这次改动这条会红：没有它时，把 `activeDeadlineSeconds` 加回一个 CronJob 不会
//! 有任何命令失败。

use crate::common::routes::repo_root;

fn production_lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines()
        .filter(|line| !line.trim_start().starts_with('#'))
}

#[test]
fn no_deployed_job_is_killed_by_a_clock() {
    let root = repo_root();
    let mut offenders = Vec::new();
    for base in ["deploy"] {
        let dir = root.join(base);
        let mut stack = vec![dir];
        while let Some(path) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let suffix = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or(Default::default());
                if !matches!(suffix, "yaml" | "yml") {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                for (index, line) in production_lines(&text).enumerate() {
                    if line.contains("activeDeadlineSeconds") {
                        offenders.push(format!(
                            "{}:{}",
                            path.strip_prefix(&root).unwrap_or(&path).display(),
                            index + 1
                        ));
                    }
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "部署清单里出现了到点杀整件活的固定钟：{}",
        offenders.join(", ")
    );
}

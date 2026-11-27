//! 到点不许杀掉**不可续**的活：`续跑=不能` 只允许出现在点名登记的那几处协议语义上。
//!
//! `// wall:` 的三问里，「能不能续跑」是最要紧的一问：答「能」的墙只是让位（下一次
//! 从同一个持久位置接着做）；答「不能」的墙到点就意味着那一份活到此为止。前者可以
//! 留，后者只有当**协议里根本没有「接着做」这个动作**时才留得住：上游的一条流断开
//! 只能新开一次调用，一个 MCP 会话结束只能新开一次会话——那不是平台把活杀了，是
//! 那种等待本来就没有续读的形状。
//!
//! 所以这张表是**闭集**，不是一张允许名单：新增一处「不能续」的墙会让这条门红，
//! 要留下来就得在这里写下理由；登记的那一处消失了也要红（账不许比树大）。
//!
//! 没有这条门：三问棘轮接受任何一个答案，包括「不能续」——它证明的是「话说了」，
//! 不是「不许杀可续的活」。

use crate::common::routes::repo_root;
use crate::common::rust_production::{production_lines_keeping_comments, production_texts};

/// （文件，墙那一行里的辨识片段，为什么只有它能「不能续」）
const NON_RESUMABLE: &[(&str, &str, &str)] = &[
    (
        "crates/cog-llm/src/provider/mod.rs",
        "上游流里的下一个事件",
        "一次流里的下一个事件等不到时，这一跳收尾并记账；要从断点续读只能新开一次上游调用——协议里没有「接着读同一跳」的动作",
    ),
    (
        "crates/cogneva/src/eval_mcp_in_pod.rs",
        "MCP 服务端在一次会话内的下一次答复",
        "MCP 会话超时后会话本身结束；续跑只能新开一次会话，已拿到的结果留着",
    ),
];

#[test]
fn only_the_registered_protocol_walls_may_end_work_they_cannot_resume() {
    let root = repo_root();
    let mut found: Vec<(String, String)> = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        for line in production_lines_keeping_comments(&text) {
            if !line.contains("// wall:") {
                continue;
            }
            if !(line.contains("续跑=不能") || line.contains("续跑=不")) {
                continue;
            }
            found.push((relative.clone(), line.trim().to_string()));
        }
    }

    let mut problems = Vec::new();
    for (path, line) in &found {
        let registered = NON_RESUMABLE
            .iter()
            .any(|(known_path, needle, _)| known_path == path && line.contains(needle));
        if !registered {
            problems.push(format!(
                "{path}: 这一处墙答「续跑=不能」，但不在登记表里：{line}"
            ));
        }
    }
    // 账不许比树大：登记的每一处都必须在树里找得到一个「不能续」的墙。
    for (path, needle, _) in NON_RESUMABLE {
        let hits = found
            .iter()
            .filter(|(found_path, line)| found_path == path && line.contains(needle))
            .count();
        if hits != 1 {
            problems.push(format!(
                "{path}: 登记里说有一处「{needle}」的不可续墙，树里找到 {hits} 处"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "到点杀掉了不可续的活：\n  {}",
        problems.join("\n  ")
    );
}

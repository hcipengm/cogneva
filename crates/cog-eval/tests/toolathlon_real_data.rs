//! 拿真实取数产物跑一遍 Toolathlon 的数据适配器。
//!
//! 单元测试用的是夹具，跑得动但证明不了「取数脚本真解开的那棵树读得进来」。这条测试读
//! [`DATA_ROOT_ENV`] 指的目录（取数脚本的 `--dest`），只在树真在的时候跑；不在时打印一行
//! SKIP 而不是静默变绿——「没数据」与「读通了」是两件事，不能同形。
//!
//! 这棵树里除了 503 道题还带着 `.utils` 这种辅助目录，题量断言就是「没被静默少读几题」
//! 的那道门。

use cog_eval::adapters::toolathlon::TOOLATHLON_TASKS_DIR;
use cog_eval::{CaseSource, ToolathlonCaseSource, DATA_ROOT_ENV};

#[test]
fn the_real_fetch_output_reads_and_is_shaped_like_toolathlon() {
    let Ok(root) = std::env::var(DATA_ROOT_ENV) else {
        eprintln!("SKIP: {DATA_ROOT_ENV} is not set, the real Toolathlon tree was not read");
        return;
    };
    let root = std::path::PathBuf::from(root);
    if !root.join(TOOLATHLON_TASKS_DIR).is_dir() {
        eprintln!(
            "SKIP: no {} under {root:?}; run the fetch script to extract the tarball here",
            TOOLATHLON_TASKS_DIR
        );
        return;
    }

    let cases = ToolathlonCaseSource
        .cases(&root)
        .expect("the real Toolathlon tree must read");
    // 题量是取数时就钉住的（503 道题；树里另有一个 `.utils` 目录不是题）。
    assert_eq!(cases.len(), 503, "Toolathlon GYM has 503 tasks");

    // id 唯一、题序稳定（两次读回来的顺序必须一致，否则种子对不上同一题）。
    let mut ids: Vec<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    let before = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(before, ids.len(), "duplicate Toolathlon ids");
    let again = ToolathlonCaseSource.cases(&root).unwrap();
    let order: Vec<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    let order2: Vec<&str> = again.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(order, order2, "case order is not stable");

    // 每道题都要有任务陈述、一条以上声明的 MCP server、且那台服务器是真的能跑到的路径。
    let mut servers = std::collections::BTreeSet::new();
    for c in &cases {
        assert!(
            c.input
                .get("task")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty()),
            "{} has no task statement",
            c.id
        );
        let declared: Vec<String> = serde_json::from_str(
            c.metadata
                .get("needed_mcp_servers")
                .map(String::as_str)
                .unwrap_or("[]"),
        )
        .unwrap_or_else(|e| panic!("{} needed_mcp_servers: {e}", c.id));
        assert!(
            !declared.is_empty(),
            "{} declares no MCP server, so its tool face is undefined",
            c.id
        );
        for s in declared {
            servers.insert(s);
        }
        // 判分与环境都要回到这个目录：它必须真的在。
        let dir = c.metadata.get("task_dir").expect("task_dir").clone();
        assert!(
            std::path::Path::new(&dir)
                .join("evaluation/main.py")
                .is_file(),
            "{} has no evaluation/main.py",
            c.id
        );
    }
    // 公开集用到 25 台不同的 MCP server；少于这个数说明有题被漏读或字段读空了。
    assert_eq!(servers.len(), 25, "distinct MCP servers: {servers:?}");
}

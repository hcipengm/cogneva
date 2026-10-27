//! principle: feedback_no_hardcoding_config_surface
//!
//! 同一行数据被两处各读一遍，而中间没有任何东西比较这两份读法。
//!
//! `PostgresStateBackend` 与 `PostgresObservabilityGateway` 读同一张
//! `cog_agent_states`、同一张 `cog_checkpoints`——观测面展示的就是状态面写下的
//! 东西，所以两处各写了一份 SELECT，也各写了一份「一行 → 结构体」的映射。两份
//! 都能编译、都能跑，于是没有任何东西会失败；漂移只表现为读数悄悄换了意思。
//!
//! 这种漂移在这一族读取上格外安静：`sqlx::query_as` 把一行按**位置**塞进元组，
//! 投影里两个同类型列对调之后照样编译、照样返回，只是把 `snapshot_id` 塞进
//! `event_offset`、把偏移量塞进快照名——读者拿到一个从没停过的位置，去恢复一个
//! 任务，而这件事与「检查点本来就存在那儿」在读数上同形。
//!
//! 判据（普查的是 `crates/cog-storage/src` 的生产行）：
//!
//! 1. [`SPELT_ONCE`] 里每一条写法在该 crate 的生产代码里**恰好出现一次**。
//!    第二次出现要么改为引用那一处，要么在这里说明它为什么是同形而不同的东西。
//! 2. 普查自己要有读数：读到的源文件太少时本门红，免得「一个都没读到」与
//!    「一处都没重复」是同一个结论。
//! 3. 判据要认得出第二处：给两行同形文本必须数出二，给一行注释必须数出零。
//!
//! 判不了什么：
//!
//! - 含义相同但写法不同的两条语句（`SELECT state` 与 `SELECT *`）。本门按写法
//!   判，不按语义判；能拦住的是抄写，不是重写。
//! - 结构体在别的 crate 里的构造（`cog-core` 的类型定义、`crates/*/tests/` 的
//!   夹具）：那不是「读一行」，也不该被同一条判据管。
//! - 列顺序本身。判据钉的是「这句话只有一处」，它读得出两处、读不出一处写错了
//!   列——那一半由 `state_reads` 自己的单元测试与恢复测试覆盖。

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::routes::{repo_root, walk};
use common::rust_production::production_lines;

/// 只许出现一次的写法，以及它是哪一件事。
///
/// 每条只写到投影那一截：`WHERE` 怎么写、后面接不接排序，都不改变「这是哪一次
/// 读取」。反过来，投影本身一个字都不许差——它是位置映射的依据。
const SPELT_ONCE: &[(&str, &str)] = &[
    (
        "SELECT state FROM cog_agent_states",
        "一个 agent 存下来的状态怎么读",
    ),
    (
        "SELECT snapshot_id, event_offset, timestamp FROM cog_checkpoints",
        "一个 task 的检查点怎么读",
    ),
    ("TaskCheckpoint {", "一行检查点怎么变成结构体"),
    (
        "AgentState = serde_json::from_str",
        "一份存下来的状态文档怎么解码",
    ),
];

/// 普查下限：测到的那一天该目录有 60 余个源文件。低于下限说明本门读得太少，
/// 而这时的「恰好一次」什么也不证明。
const MIN_FILES: usize = 30;

/// 生产行里 `needle` 出现的次数。一行写两次也算两次，跨行写的算零次——
/// 零次会被「恰好一次」判红，方向是对的：本门读不到，就不许说它只有一处。
fn occurrences(text: &str, needle: &str) -> usize {
    production_lines(text)
        .into_iter()
        .map(|line| line.matches(needle).count())
        .sum()
}

/// 该 crate 生产代码里每条写法的全部落点，按文件与行记下来。
type Sites = BTreeMap<&'static str, Vec<(String, String)>>;

fn census(root: &Path) -> (usize, Sites) {
    let mut files = Vec::new();
    walk(&root.join("crates/cog-storage/src"), &["rs"], &mut files);
    files.sort();
    let mut sites: Sites = SPELT_ONCE
        .iter()
        .map(|(needle, _)| (*needle, Vec::new()))
        .collect();
    for path in &files {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        for line in production_lines(&text) {
            for (needle, hits) in sites.iter_mut() {
                for _ in 0..line.matches(needle).count() {
                    hits.push((file.clone(), line.trim().to_string()));
                }
            }
        }
    }
    (files.len(), sites)
}

#[test]
fn a_row_two_stores_read_is_mapped_once() {
    let (files, sites) = census(&repo_root());
    assert!(
        files >= MIN_FILES,
        "只读到 {files} 个源文件（下限 {MIN_FILES}），这条判据没有题目"
    );

    let mut findings: Vec<String> = Vec::new();
    for (needle, what) in SPELT_ONCE {
        let hits = &sites[*needle];
        if hits.len() == 1 {
            continue;
        }
        let mut rendered = if hits.is_empty() {
            format!("  {what}：一处也读不到（写法是 {needle:?}）")
        } else {
            format!("  {what}：{} 处", hits.len())
        };
        for (file, line) in hits {
            rendered.push_str(&format!("\n      {file}: {line}"));
        }
        findings.push(rendered);
    }

    assert!(
        findings.is_empty(),
        "这些读取在多处各写了一遍。两处都能编译、都能返回，所以两边分叉之后\
         没有任何东西会失败：投影里对调两列，位置映射照样把结果塞进结构体，\
         读的人拿到一个从没发生过的位置。把它们收进 `postgres::state_reads`，\
         其余地方引用那一处：\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_census_counts_a_second_spelling_and_reads_nothing_out_of_prose() {
    let needle = "SELECT state FROM cog_agent_states";
    assert_eq!(
        occurrences(
            "let sql = \"SELECT state FROM cog_agent_states WHERE agent_id = $1\";\n",
            needle
        ),
        1
    );
    // 第二处就是第二处，哪怕两条一模一样。
    assert_eq!(
        occurrences(
            concat!(
                "let a = \"SELECT state FROM cog_agent_states WHERE agent_id = $1\";\n",
                "let b = \"SELECT state FROM cog_agent_states WHERE agent_id = $1\";\n",
            ),
            needle
        ),
        2
    );
    // 注释里提到这句话不是第二处读取。
    assert_eq!(
        occurrences(
            concat!(
                "// 这里是 SELECT state FROM cog_agent_states 的说明\n",
                "let a = 1;\n",
            ),
            needle
        ),
        0
    );
}

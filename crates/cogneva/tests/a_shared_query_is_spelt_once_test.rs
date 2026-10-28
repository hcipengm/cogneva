//! principle: feedback_no_hardcoding_config_surface
//!
//! 同一句话被写了两遍（或更多遍），而中间没有任何东西比较它们。
//!
//! 本门管这一族里的三个形状。
//!
//! **形状一：同一行数据被两处各读一遍。** `PostgresStateBackend` 与
//! `PostgresObservabilityGateway` 读同一张 `cog_agent_states`、同一张
//! `cog_checkpoints`——观测面展示的就是状态面写下的东西，所以两处各写了一份
//! SELECT，也各写了一份「一行 → 结构体」的映射。两份都能编译、都能跑，于是没有
//! 任何东西会失败；漂移只表现为读数悄悄换了意思。这一族格外安静：
//! `sqlx::query_as` 把一行按**位置**塞进元组，投影里两个同类型列对调之后照样编译、
//! 照样返回，只是把 `snapshot_id` 塞进 `event_offset`、把偏移量塞进快照名——读者
//! 拿到一个从没停过的位置，去恢复一个任务，而这件事与「检查点本来就存在那儿」在
//! 读数上同形。
//!
//! **形状二：同一把尺子被三个地方各写一遍。** 三个容量清扫器各量一次「这张表在盘
//! 上占多少」，三条字节读数在同一个面板上被并排比较。三份拷贝里的一处改成不含
//! TOAST 的 `pg_table_size`，另外两条不报错、也不变慢，面板上只是有一条线的量纲
//! 悄悄不同——「这类日志比那一类占得少」这句话从此不成立，且没有任何东西会说不
//! 成立。同形的地方还包括「表还没建出来」的处置：三份拷贝各自把它记成一句「拿不到
//! 大小」，于是「名字写错了」「还没建表」「库连不上」三种现场同形。
//!
//! **形状三：同一把尺子的另一半——「这张表有多少行」。** 四个清扫器
//! （checkpoint、指标样本、可观测事件、任务事件归档）各发布一条行数读数，四条线
//! 也被并排比较。它们与形状二成对：字节是「占多大」，行数是「有多少」，任一处被
//! 加上 `WHERE` 只数一个分区、或换了 `ONLY`、或从 `count(*)` 换成 `count(1)`，
//! 另外三条都不失败，只是那一条的口径变了——而「这类日志比那一类多」这句话与
//! 「这一类刚被清理过」在读数上同形。表名拼进语句前的引号规则同样是这把尺子的
//! 一半：一处按点分段、一处整体加引号，两种都「能跑」，遇到带 schema 的名字就
//! 只有一种是对的。
//!
//! 判据（普查的是整棵树里会随产物跑的 Rust 源码）：
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
//! - 类型定义本身：`pub struct TaskCheckpoint {` 是一个类型，不是一次构造，所以
//!   构造那一条钉的是构造点的写法而不是类型名；`crates/*/tests/` 的夹具同理，
//!   它们不是生产行。
//! - 列顺序本身。判据钉的是「这句话只有一处」，它读得出两处、读不出一处写错了
//!   列——那一半由 `state_reads` 自己的单元测试与恢复测试覆盖。

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::routes::repo_root;
use common::rust_production::{production_lines, production_sources, relative_name};

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
    // 钉的是构造点，不是类型定义：`pub struct TaskCheckpoint {` 也是这几个字符，
    // 但它不构造任何东西。整棵树里这条写法只许有一处。
    ("Ok(TaskCheckpoint {", "一行检查点怎么变成结构体"),
    (
        "AgentState = serde_json::from_str",
        "一份存下来的状态文档怎么解码",
    ),
    (
        "pg_total_relation_size(to_regclass($1))",
        "一张表在盘上占多少（含索引与 TOAST）",
    ),
    ("SELECT count(*) FROM {}", "一条关系里有多少行"),
];

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

fn census(root: &Path) -> Sites {
    let mut sites: Sites = SPELT_ONCE
        .iter()
        .map(|(needle, _)| (*needle, Vec::new()))
        .collect();
    for path in production_sources(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, &path);
        for line in production_lines(&text) {
            for (needle, hits) in sites.iter_mut() {
                for _ in 0..line.matches(needle).count() {
                    hits.push((file.clone(), line.trim().to_string()));
                }
            }
        }
    }
    sites
}

#[test]
fn a_shared_read_is_spelt_once() {
    let sites = census(&repo_root());

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
         读的人拿到一个从没发生过的位置；又或者同名的三条读数换了量纲，面板上\
         照常并排显示。把共用的一处收成一个模块（`postgres::state_reads`、\
         `relation_bytes`、`relation_rows`），其余地方引用它：\n{}",
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

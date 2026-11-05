//! principle: feedback_a_pg_integration_test_owns_its_schema_so_it_cannot_drop_a_deployments_tables
//!
//! What a test that points at a real PostgreSQL must never do is name a table of
//! the deployment it was pointed at. A test that takes the raw URL builds, seeds
//! and finally drops `cog_metrics_samples` -- the table production creates -- so
//! the one variable that decides which server it talks to also decides whether
//! the deployment keeps its own table.
//!
//! 规则：对着真库跑的集成测试必须把表建在**每轮自己的 schema** 里——schema 名由
//! `own_schema` 取、跟着连接串进去（`schema_pool`／`schema_pool_with` 把
//! `search_path` 指过去），跑完 `drop_schema` 删掉。这样语句里不带前缀的表名落在
//! 这一轮的 schema 里，而不是部署的 `public` 里：URL 指错了库，也不再是删掉了别人
//! 的表。
//!
//! 本门读的是这棵树上的测试源码：一段以 `DROP TABLE` / `TRUNCATE` 开头、而且表名
//! 写死（不带 `{}` 插值）的 SQL 字面量，只允许出现在自己开 schema 的文件里。写死的
//! 表名正是「删掉部署那张表」的形状——名字由生产代码决定，用例这一侧没有任何东西
//! 能保证它指向本轮的库。
//!
//! 判不了什么：
//!
//! - 表名由 `format!` 插出来的语句。名字是这一轮现取的（`probe_…`），删的是自己刚
//!   建的那张。
//! - `DELETE FROM` / `INSERT INTO`。那是行，不是表；删错库也只是删错数据，本门只读
//!   建表与删表，因为把部署的表删掉是唯一一条修不回来的路。
//! - 运行期。门只读源码里 `own_schema`、`schema_pool*`、`drop_schema` 三处都在，不看
//!   它们在 `--ignored` 的那一轮里跑得通不通。

use crate::common;

use std::path::Path;

use common::routes::{read, repo_root, walk_files};
use common::rust_production::relative_name;

/// 本门自己：它源码里的字面量不是一份对着真库跑的用例。
const GATE: &str = "crates/cogneva/tests/a_pg_integration_test_owns_its_schema_test.rs";

/// 承载这条原则的用例，以及它文件头里那一行声明。
const CARRIER: &str = "crates/cog-storage/tests/schema_lock_bound.rs";
const PRINCIPLE: &str = "principle: feedback_a_pg_integration_test_owns_its_schema";

/// 认题的记号：每个对着真库跑的用例都在被跳过的那一行里写着它。
const LIVE_ENV: &str = "COGNEVA_TEST_DATABASE_URL";

/// 以这两条语句开头、表名写死的字面量，是本门唯一的题目。
const DESTRUCTIVE: [&str; 2] = ["DROP TABLE", "TRUNCATE"];

/// 这棵树里每份测试源码的仓库相对名与全文。
fn test_sources(root: &Path) -> Vec<(String, String)> {
    let mut paths = Vec::new();
    walk_files(&root.join("crates"), &["rs"], &["target"], &mut paths);
    paths
        .iter()
        .filter(|path| path.components().any(|part| part.as_os_str() == "tests"))
        .map(|path| (relative_name(root, path), read(path)))
        .collect()
}

/// 写死了表名的破坏性语句的条数。
fn fixed_relation_statements(text: &str) -> usize {
    let mut found = 0;
    for line in text.lines() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        for keyword in DESTRUCTIVE {
            let mut rest = line;
            while let Some(at) = rest.find(keyword) {
                let quoted = rest[..at].trim_end().ends_with('"');
                let tail = &rest[at..];
                let statement = tail.split('"').next().unwrap_or(tail);
                if quoted && !statement.contains('{') {
                    found += 1;
                }
                rest = &tail[keyword.len()..];
            }
        }
    }
    found
}

/// 这份文件有没有把连接放进自己的一轮 schema 里，并把它删掉。
fn owns_a_schema(text: &str) -> bool {
    text.contains("own_schema(")
        && (text.contains("schema_pool(") || text.contains("schema_pool_with("))
        && text.contains("drop_schema(")
}

#[test]
fn a_live_database_test_builds_and_drops_its_tables_in_its_own_schema() {
    let root = repo_root();
    let sources = test_sources(&root);
    assert!(
        sources.len() >= 300,
        "只读到 {} 份测试源码，少于这棵树已知的 300 份：读法坏了，下面每条断言都会从空集上通过",
        sources.len()
    );

    let live: Vec<(String, String)> = sources
        .iter()
        .filter(|(name, text)| name != GATE && text.contains(LIVE_ENV))
        .cloned()
        .collect();
    assert!(
        live.len() >= 25,
        "只找到 {} 份对着真库跑的用例，少于已知的 25 份：认出题目的那条判据坏了",
        live.len()
    );

    let mut owning = 0;
    let mut offenders = Vec::new();
    for (name, text) in &live {
        if owns_a_schema(text) {
            owning += 1;
        }
        let fixed = fixed_relation_statements(text);
        if fixed > 0 && !owns_a_schema(text) {
            offenders.push(format!("{name}: {fixed} 处"));
        }
    }
    assert!(
        owning >= 2,
        "自己开 schema 的用例只剩 {owning} 份：`own_schema` 被改名或搬走，下面那条断言只会在空集上通过"
    );
    assert!(
        offenders.is_empty(),
        "这些文件对着真库跑，却按写死的表名建表或删表，而且没有自己的一轮 schema：\
         {offenders:?}。连接串指向哪个库，这些语句就落在哪个库的 `public` 里；\
         一次指错的 URL 就是一次删掉部署的表。连接走 `cog_test_support::own_schema` \
         ＋ `schema_pool`／`schema_pool_with`，收尾 `drop_schema`。"
    );

    let carried = sources
        .iter()
        .find(|(name, _)| name == CARRIER)
        .map(|(_, text)| text.as_str())
        .unwrap_or_else(|| panic!("{CARRIER} 不在了：这条原则没有承载它的用例，本门没有题目"));
    assert!(
        owns_a_schema(carried),
        "{CARRIER} 又回到裸连接上了：它是这条原则的承载者，自己得先守"
    );
    let header = carried.find(PRINCIPLE);
    let first_use = carried.find("\nuse ").unwrap_or(usize::MAX);
    let declared = header.is_some_and(|at| at < first_use);
    assert!(
        declared,
        "{CARRIER} 的文件头里没有 `{PRINCIPLE}`，或者写在了第一个 `use` 之后"
    );
}

//! 任务账本的表结构写了两遍：一遍在迁移里（升级路径），一遍在启动 schema 里
//! （全新安装）。两遍都用 `IF NOT EXISTS`，所以哪一遍先跑、另一遍就静默跳过——
//! 两份不一样的时候，升级上来的库与刚装出来的库各长一个样子，而没有一条读数
//! 会说它们不同。
//!
//! 这个差值不需要数据库就能读出来：把两份文本按启动器同一把刀切成语句，挑出
//! 属于这四张表的建表、索引与触发器，去掉注释、压平空白，逐条比。多一列、少一条
//! 索引、丢一个触发器，都会在这里红。
//!
//! 它比的是**这一组语句**，不是整份文本：启动 schema 里还有 `task_plan` 之类的
//! 表，迁移本来就不负责它们。
//!
//! 账本建出来之后，后面的迁移还可能改它的列名（099 把 `timeout_seconds` 改成
//! `declared_duration_seconds`，启动 schema 用的是改名后的名字）。所以「迁移建出来的
//! 账本」不是 095 一份文本说的，是 095 按顺序走完后面那些改名之后的样子——改名从
//! 迁移目录里读，不在这里抄一份。

use cog_storage::postgres::state_backend::ddl;
use cog_storage::schema_ddl::split_statements;

const MIGRATION: &str = include_str!("../migrations/postgres/095_task_ledger.up.sql");

/// 095 之后落在账本上的改名：`(旧列名, 新列名)`，按版本顺序。
///
/// 读目录而不是抄一份：抄的那一份会在下一次改名时静默停在旧名字上，而这条门要判的
/// 正是「两种装法长出同一个账本」。
fn renames_after_the_ledger() -> Vec<(String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/postgres");
    let mut files: Vec<(u32, std::path::PathBuf)> = std::fs::read_dir(&dir)
        .expect("the migrations directory is part of this crate")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?.to_string();
            let (version, rest) = name.split_once('_')?;
            if !rest.ends_with(".up.sql") {
                return None;
            }
            let version: u32 = version.parse().ok()?;
            (version > 95).then_some((version, path))
        })
        .collect();
    files.sort();
    let mut out = Vec::new();
    for (_, path) in files {
        let text = std::fs::read_to_string(&path).expect("a migration file is readable");
        for statement in split_statements(&text).unwrap_or_default() {
            let text = normalize(&statement);
            let text = text.trim_end_matches(';').trim();
            let Some(rest) = text.strip_prefix("alter table ") else {
                continue;
            };
            let mut words = rest.split_whitespace();
            let Some(table) = words.next() else { continue };
            if !LEDGER_TABLES.contains(&table) {
                continue;
            }
            let (Some("rename"), Some("column"), Some(old), Some("to"), Some(new)) = (
                words.next(),
                words.next(),
                words.next(),
                words.next(),
                words.next(),
            ) else {
                continue;
            };
            out.push((old.to_string(), new.to_string()));
        }
    }
    out
}

/// 095 的文本，按顺序走完后面那些改名。
fn migration_ledger_text() -> String {
    let mut text = MIGRATION.to_string();
    for (old, new) in renames_after_the_ledger() {
        text = text.replace(&old, &new);
    }
    text
}

/// 账本的四张表。`task_plan` 不在其中：它是启动 schema 自己的，迁移不建它。
const LEDGER_TABLES: [&str; 4] = [
    "task_defined",
    "task_transition",
    "task_attempt",
    "task_current",
];

/// 去掉行注释与多余空白，按 PostgreSQL 的标识符大小写不敏感比较。
fn normalize(statement: &str) -> String {
    let without_comments: String = statement
        .lines()
        .map(|line| match line.find("--") {
            Some(at) => &line[..at],
            None => line,
        })
        .collect::<Vec<_>>()
        .join(" ");
    without_comments
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

/// 这条语句是不是四张账本表的定义之一。
fn is_ledger_shape(statement: &str) -> bool {
    let text = normalize(statement);
    let text = text.trim_end_matches(';').trim();
    for table in LEDGER_TABLES {
        if text.starts_with(&format!("create table if not exists {table} (")) {
            return true;
        }
        let index = format!("create index if not exists idx_{table}_");
        if text.starts_with(&index) || text.starts_with(&format!("create unique {index}")) {
            return true;
        }
        let trigger = format!("create or replace trigger {table}_");
        if text.starts_with(&trigger) {
            return true;
        }
    }
    // 追加保护是两条：一条把咨询锁拿到手（整批一次隐式事务，锁活到批尾），
    // 一条在锁后面查缺、缺才建。两条都是条件代码而不是可被目录回答的 DDL，
    // 所以按它们建的那个东西认它们。
    (text.starts_with("do $$") && text.contains("task_ledger_history_is_append_only"))
        || text.starts_with(
            "select pg_advisory_xact_lock(hashtext('cogneva:task-ledger:append-only'))",
        )
}

fn ledger_shapes(text: &str) -> Vec<String> {
    let mut shapes: Vec<String> = split_statements(text)
        .expect("the runner splits both texts with the same splitter")
        .iter()
        .filter(|statement| is_ledger_shape(statement))
        .map(|statement| {
            normalize(statement)
                .trim_end_matches(';')
                .trim()
                .to_string()
        })
        .collect();
    shapes.sort();
    shapes
}

#[test]
fn the_migration_and_the_startup_schema_build_the_same_ledger() {
    let from_migration = ledger_shapes(&migration_ledger_text());
    let from_startup = ledger_shapes(&ddl());

    // 先确认两边都不是空的：一个挑不出任何语句的比较，在「句式变了、过滤条件
    // 全部落空」时会以「两边一样（都空）」通过。
    assert!(
        from_migration.len() >= 4 + 12,
        "迁移里挑出的账本语句太少，过滤条件可能已经对不上它了：{} 条",
        from_migration.len()
    );
    assert_eq!(
        from_migration,
        from_startup,
        "升级路径与全新安装建出来的账本必须逐条相同；这个差就是两种安装方式长出的\
         两种表结构\n只在一侧：{:?}\n只在另一侧：{:?}",
        from_migration
            .iter()
            .filter(|s| !from_startup.contains(s))
            .collect::<Vec<_>>(),
        from_startup
            .iter()
            .filter(|s| !from_migration.contains(s))
            .collect::<Vec<_>>()
    );
}

/// 历史不可改写这件事落在两张表上，两张都要有触发器。
///
/// 单独再判一次，是因为上面的逐条比较只保证「两边一样」：两边同时漏掉一个
/// 触发器，它照样绿。这一条判的是「这两张表各有一次拒绝改写的触发器」。
#[test]
fn both_history_tables_refuse_a_rewrite() {
    for (name, text) in [("migration", MIGRATION), ("startup schema", ddl().as_str())] {
        let shapes = ledger_shapes(text);
        let guard = shapes
            .iter()
            .find(|shape| shape.contains("task_ledger_history_is_append_only"))
            .unwrap_or_else(|| panic!("{name} must install the append-only guard"));
        for table in ["task_defined", "task_transition"] {
            assert!(
                guard.contains(&format!("create trigger {table}_is_append_only"))
                    && guard.contains(&format!("before update on {table}")),
                "{name} must refuse a rewrite of {table}: {guard}"
            );
        }
        // 尝试表故意没有：`ended_at` 就是「这次跑完了」这个事实本身，关一次就是改
        // 这一行。定义与迁移那两张是历史，尝试是记录中的当前状态。
        assert!(
            !guard.contains("task_attempt_is_append_only"),
            "an attempt row is closed in place; it is not a history row"
        );
    }
}

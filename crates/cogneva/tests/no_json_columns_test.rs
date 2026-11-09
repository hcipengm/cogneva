//! principle: feedback_no_hardcoding_config_surface
//!
//! 库只存列：`json` / `jsonb` 列的棘轮。
//!
//! 存储终态的第二条不变式是「全库没有 `json`/`jsonb` 列」：开集载荷存 `text`，
//! 语法上不可查询；要按某个键查，就给它加列。理由不是风格——jsonb 列上的读法
//! 是 `task->>'goal_id'` 这类字符串，键名打错返回 NULL 而不报错，判据用的字段
//! 建不了索引，改不同字段的两个写者照样互相覆盖；派生量被冻进文档之后，再没有
//! 任何东西重算它。
//!
//! 那份方案是一段讨论。讨论不会自己变红，也不会自己变短：一张表今天还在，明天
//! 又长出一列，读的人无从知道总数是在降还是在涨。这条门把它变成一个数——基线在
//! 下面，一行一列，**只能减不能增**。
//!
//! 三个判词：
//!
//! * 树上有基线没点名的列 → 红。新的 jsonb 列不是一个可以顺手加的东西，它是一份
//!   存储决定，要么换成列，要么改基线并在这里写下为什么；
//! * 基线里点名、树上已经没有的列 → 也红。谁把一列改成 `text` 或拆成列，就顺手把
//!   这一行删掉——留着它，基线就从「还剩多少」变成「曾经有多少」，而这个数唯一
//!   的用处就是回答「还剩多少」；
//! * 基数下限：一次至少读到 [`MIN_COLUMNS`] 行。读空与「全清完了」在判词上同形，
//!   而读空只可能来自走错了树。
//!
//! 口径是声明点，不是运行中的库：迁移的 `.up.sql`（重放时建表）与 store 自己的
//! `DDL` 常量（首启时建表）都算；`.down.sql` 不算，它描述的是回滚到旧形状，不是
//! 这棵树想去的终态。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::common;

use common::routes::repo_root;
use common::rust_production::production_sources;

/// 一次普查至少该读到多少列。
///
/// 2026-11-09 实测 66 行：`crates/*/src` 的 DDL 常量 25 行，`migrations/postgres`
/// 的 `.up.sql` 21 行，`migrations/mysql` 的 `.up.sql` 20 行——两种方言各建一次表，
/// 同一个列在两边各算一行，因为一边先清了另一边还建着，那也是一句要清的债。
/// 下限取 30：它挡的是「走错树读成空」，不是「清得太多」——清到零的那天这条门应该
/// 绿，那时把下限跟着基线一起删掉。
const MIN_COLUMNS: usize = 30;

/// 还剩下的 `json`/`jsonb` 列，键是 `文件:表.列`。
///
/// 删掉一行只允许一件事：那一列在树上真的已经不是 jsonb 了。删之前先把改动做完。
const BASELINE: &[&str] = &[
    "crates/cog-memory/src/postgres_entry_store.rs:summary_entries.related_schema_ids",
    "crates/cog-memory/src/postgres_entry_store.rs:summary_entries.sparse_embedding",
    "crates/cog-memory/src/postgres_schema.rs:schema_entries.properties",
    "crates/cog-observability/src/alert_store.rs:alerts.labels",
    "crates/cog-observability/src/analytics.rs:{table}.properties",
    "crates/cog-observability/src/usage_store.rs:gateway_llm_usage.pace_upstreams",
    "crates/cog-storage/migrations/mysql/002_user_auth_methods.up.sql:user_auth_methods.extra",
    "crates/cog-storage/migrations/mysql/003_workspaces.up.sql:workspaces.settings",
    "crates/cog-storage/migrations/mysql/004_tasks.up.sql:tasks.blocked_by",
    "crates/cog-storage/migrations/mysql/004_tasks.up.sql:tasks.blocks",
    "crates/cog-storage/migrations/mysql/004_tasks.up.sql:tasks.input",
    "crates/cog-storage/migrations/mysql/004_tasks.up.sql:tasks.result",
    "crates/cog-storage/migrations/mysql/005_agents.up.sql:agents.capabilities",
    "crates/cog-storage/migrations/mysql/005_agents.up.sql:agents.metadata",
    "crates/cog-storage/migrations/mysql/006_memory.up.sql:memory_raw_sources.tags",
    "crates/cog-storage/migrations/mysql/006_memory.up.sql:memory_schema_entries.properties",
    "crates/cog-storage/migrations/mysql/006_memory.up.sql:memory_summary_entries.related_schema_ids",
    "crates/cog-storage/migrations/mysql/008_chat_sessions.up.sql:chat_sessions.context",
    "crates/cog-storage/migrations/mysql/009_messages.up.sql:messages.file_attachments",
    "crates/cog-storage/migrations/mysql/009_messages.up.sql:messages.tool_calls",
    "crates/cog-storage/migrations/mysql/016_notifications.up.sql:notifications.data",
    "crates/cog-storage/migrations/mysql/017_audit_logs.up.sql:audit_logs.details",
    "crates/cog-storage/migrations/mysql/021_explainability.up.sql:explainability.data",
    "crates/cog-storage/migrations/mysql/028_change_ledger.up.sql:change_ledger.sources",
    "crates/cog-storage/migrations/mysql/050_resource_events.up.sql:resource_events.detail",
    "crates/cog-storage/migrations/mysql/056_token_face_beat.up.sql:token_face_beat.measures",
    "crates/cog-storage/migrations/postgres/002_user_auth_methods.up.sql:user_auth_methods.extra",
    "crates/cog-storage/migrations/postgres/003_workspaces.up.sql:workspaces.settings",
    "crates/cog-storage/migrations/postgres/004_tasks.up.sql:tasks.blocked_by",
    "crates/cog-storage/migrations/postgres/004_tasks.up.sql:tasks.blocks",
    "crates/cog-storage/migrations/postgres/004_tasks.up.sql:tasks.input",
    "crates/cog-storage/migrations/postgres/004_tasks.up.sql:tasks.result",
    "crates/cog-storage/migrations/postgres/005_agents.up.sql:agents.capabilities",
    "crates/cog-storage/migrations/postgres/005_agents.up.sql:agents.metadata",
    "crates/cog-storage/migrations/postgres/006_memory.up.sql:memory_raw_sources.tags",
    "crates/cog-storage/migrations/postgres/006_memory.up.sql:memory_schema_entries.properties",
    "crates/cog-storage/migrations/postgres/006_memory.up.sql:memory_summary_entries.related_schema_ids",
    "crates/cog-storage/migrations/postgres/008_chat_sessions.up.sql:chat_sessions.context",
    "crates/cog-storage/migrations/postgres/009_messages.up.sql:messages.file_attachments",
    "crates/cog-storage/migrations/postgres/009_messages.up.sql:messages.tool_calls",
    "crates/cog-storage/migrations/postgres/016_notifications.up.sql:notifications.data",
    "crates/cog-storage/migrations/postgres/017_audit_logs.up.sql:audit_logs.details",
    "crates/cog-storage/migrations/postgres/021_explainability.up.sql:explainability.data",
    "crates/cog-storage/migrations/postgres/023_explainability_partition.up.sql:explainability_part.data",
    "crates/cog-storage/migrations/postgres/028_change_ledger.up.sql:change_ledger.sources",
    "crates/cog-storage/migrations/postgres/050_resource_events.up.sql:resource_events.detail",
    "crates/cog-storage/migrations/postgres/056_token_face_beat.up.sql:token_face_beat.measures",
    "crates/cog-storage/src/postgres/audit_recorder.rs:cog_guard_audit_log.messages",
    "crates/cog-storage/src/postgres/audit_recorder.rs:cog_guard_audit_log.tool_call",
    "crates/cog-storage/src/postgres/hook_archive.rs:cog_hook_events.payload",
    "crates/cog-storage/src/postgres/metrics_backend.rs:cog_metric_counter_totals.labels",
    "crates/cog-storage/src/postgres/metrics_backend.rs:cog_metric_histogram_buckets.labels",
    "crates/cog-storage/src/postgres/metrics_backend.rs:cog_metric_histogram_sums.labels",
    "crates/cog-storage/src/postgres/metrics_backend.rs:cog_metrics_samples.labels",
    "crates/cog-storage/src/postgres/notification_store.rs:notifications.data",
    "crates/cog-storage/src/postgres/observability_gateway.rs:cog_observability_events.payload",
    "crates/cog-storage/src/postgres/observability_gateway.rs:cog_observability_logs.metadata",
    "crates/cog-storage/src/postgres/observability_gateway.rs:cog_observability_squads.agents",
    "crates/cog-storage/src/postgres/pge_stage_store.rs:pge_stage_product.change_identity",
    "crates/cog-storage/src/postgres/pge_stage_store.rs:pge_stage_product.product",
    "crates/cog-storage/src/postgres/snapshot_store.rs:cog_snapshots.agent_state",
    "crates/cog-storage/src/postgres/snapshot_store.rs:cog_snapshots.context_window",
    "crates/cog-storage/src/postgres/state_backend.rs:cog_dag_state.state",
    "crates/cog-storage/src/postgres/state_backend.rs:cog_events.payload",
    "crates/cog-storage/src/postgres/taste_intent_store.rs:cog_taste_intents.evidence_refs",
    "crates/cog-storage/src/postgres/taste_intent_store.rs:cog_taste_intents.payload",
];

#[test]
fn a_json_column_the_baseline_does_not_name_is_a_new_storage_decision() {
    let root = repo_root();
    let found = census(&root);
    let named: BTreeSet<String> = BASELINE.iter().map(|s| s.to_string()).collect();
    let new: Vec<&String> = found.difference(&named).collect();
    assert!(
        new.is_empty(),
        "这几列是 json/jsonb，而基线里没有它们——加一列 jsonb 不是一次普通改动，是把一段\
         「可查询但没有 schema」的状态又放回库里。要按它查就给它加列；只是搬运就存 text。\
         确实要加，就在 BASELINE 里点名并写清为什么：\n{}",
        new.iter()
            .map(|r| format!("  {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn a_baseline_row_whose_column_is_gone_has_to_be_deleted_too() {
    let root = repo_root();
    let found = census(&root);
    let named: BTreeSet<String> = BASELINE.iter().map(|s| s.to_string()).collect();
    let stale: Vec<&String> = named.difference(&found).collect();
    assert!(
        stale.is_empty(),
        "基线点名、树上已经没有的条目：不删的话，基线就从「还剩多少」变成「曾经有多少」，\
         而它唯一的用处是回答还剩多少。改完这一列的那一笔里顺手把这一行删掉：\n{}",
        stale
            .iter()
            .map(|r| format!("  {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_census_reads_the_tree_and_not_an_empty_set() {
    let root = repo_root();
    let found = census(&root);
    assert!(
        found.len() >= MIN_COLUMNS,
        "只读到 {} 列 json/jsonb（下限 {MIN_COLUMNS}）：读空与「全清完了」在判词上同形，\
         而读空通常只意味着这份普查走错了树",
        found.len()
    );
}

/// 树上声明的每一个 json/jsonb 列，键是 `文件:表.列`。
fn census(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        scan(&rel(root, &path), &text, &mut out);
    }
    for path in migration_files(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        scan(&rel(root, &path), &text, &mut out);
    }
    out
}

fn rel(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

/// `crates/*/migrations/**/*.up.sql`：重放时会建表的那些。
fn migration_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let crates = root.join("crates");
    let Ok(entries) = std::fs::read_dir(&crates) else {
        return out;
    };
    for entry in entries.flatten() {
        let migrations = entry.path().join("migrations");
        if migrations.is_dir() {
            collect_sql(&migrations, &mut out);
        }
    }
    out
}

fn collect_sql(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}：{e}（少读一块与「那一块都合规」同形）", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_sql(&path, out);
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".up.sql"))
        {
            out.push(path);
        }
    }
}

/// 一份 DDL 文本里的 json/jsonb 列。
///
/// 只认表体里的列声明：`CREATE TABLE … (` 之后、配平的右括号之前，以及
/// `ALTER TABLE … ADD COLUMN`。整行匹配而不是全文本正则，是因为 `FORMAT JSON`
/// 这类语句尾巴会与「列名 类型」同形——`analytics.rs` 里 ClickHouse 的建表语句
/// 就有一行只写着 `FORMAT JSON`，它把自己的名字读成了一列。
fn scan(file: &str, text: &str, out: &mut BTreeSet<String>) {
    if !text.to_ascii_uppercase().contains("JSON") {
        return;
    }
    let mut table: Option<String> = None;
    let mut inside = false;
    let mut depth: i32 = 0;
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower
            .trim_start()
            .strip_prefix("create table if not exists ")
        {
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"');
            table = Some(name.to_string());
            depth = parens(line);
            inside = depth > 0;
            continue;
        }
        if let Some(rest) = lower.trim_start().strip_prefix("alter table ") {
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"');
            table = Some(name.to_string());
            inside = false;
            let line_lower = line.to_ascii_lowercase();
            if let Some(rest) = line_lower
                .split_once("add column if not exists ")
                .map(|(_, r)| r)
            {
                if let Some(column) = column_type(rest) {
                    out.insert(format!("{file}:{}.{column}", name.trim_matches('"')));
                }
            }
            continue;
        }
        if inside {
            if let Some(column) = column_type(line) {
                if let Some(name) = &table {
                    out.insert(format!("{file}:{name}.{column}"));
                }
            }
            depth += parens(line);
            if depth <= 0 {
                inside = false;
            }
        }
    }
}

fn parens(line: &str) -> i32 {
    line.matches('(').count() as i32 - line.matches(')').count() as i32
}

/// `<列名> JSON(B)` 的那一行，返回列名。
fn column_type(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let name_end = trimmed
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(trimmed.len());
    if name_end == 0 {
        return None;
    }
    let name = &trimmed[..name_end];
    let rest = trimmed[name_end..].trim_start();
    let rest_lower = rest.to_ascii_lowercase();
    for kind in ["jsonb", "json"] {
        if let Some(after) = rest_lower.strip_prefix(kind) {
            let boundary = after
                .chars()
                .next()
                .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
            if boundary {
                return Some(name.to_string());
            }
        }
    }
    None
}

//! principle: feedback_no_hardcoding_config_surface
//! A role name is a string the configuration, the code and the stored
//! identifiers all have to agree on. `squad:{task}-{role}` is a key in the
//! checkpoint board; `schema_for(actor)` is a lookup in the PGE schema map; the
//! actor list in the configuration names the same five words. Spelling the word
//! at each use site means a rename edits some of them.
//!
//! What makes this worth a gate is how the disagreement reads. An unknown actor
//! is not an error: `schema_for` answers `None`, and every caller is written
//! `if let Some(schema) = ...` and carries on. A checkpoint under the old role
//! name is simply never found, which reads as "that task has no checkpoint yet".
//! So a squad that has been renamed runs to completion with the wrong prompt
//! structure and no checkpoint resume, and both look like ordinary behaviour.
//!
//! The names live in [`cog_core::roles`]. This counts production lines, so a
//! second spelling is caught the day it is written rather than the day someone
//! renames something.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

mod common;

use common::rust_production::production_lines;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Where the names are declared, relative to the repository root.
const HOME: &str = "crates/cog-core/src/roles.rs";

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Every complete string literal on the line, with the text before it.
///
/// Whole literals only: `"the planner decides"` is prose and
/// `"planner_v2"` is a different role, and neither is a second spelling of
/// `"planner"`.
fn literals(line: &str) -> Vec<(&str, &str)> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find('"') {
        let before = &rest[..at];
        let after = &rest[at + 1..];
        let Some(end) = after.find('"') else {
            break;
        };
        out.push((&after[..end], before));
        rest = &after[end + 1..];
    }
    out
}

/// Whether the literal is the label of a `Debug` struct field rather than data.
///
/// `debug_struct("..").field("moderator", &self.moderator.is_some())` prints
/// the word `moderator`; it never reaches the configuration, never names an
/// actor and never becomes an id. Reading it as a second spelling of the role
/// would be a true statement about the wrong thing, so the reader skips it.
fn is_debug_label(before: &str) -> bool {
    before.trim_end().ends_with(".field(")
}

/// Everything in [`cog_core::roles`] that is a name, read from its declaration.
///
/// `ALL` is what consumers use, so it is what this gate follows: a role added
/// there is counted here without anyone remembering to edit this file.
fn declared() -> Vec<&'static str> {
    cog_core::roles::ALL.to_vec()
}

fn sites(root: &Path, names: &[&str]) -> BTreeMap<String, Vec<String>> {
    let mut crates: Vec<PathBuf> = std::fs::read_dir(root.join("crates"))
        .expect("crates/ 不在仓库里")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    crates.sort();
    let mut found: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for krate in crates {
        let src = krate.join("src");
        if !src.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        walk(&src, &mut files);
        files.sort();
        for path in files {
            let file = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if file == HOME {
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            for line in production_lines(&text) {
                for (quote, before) in literals(line) {
                    if is_debug_label(before) {
                        continue;
                    }
                    if names.contains(&quote) {
                        found
                            .entry(quote.to_string())
                            .or_default()
                            .push(format!("{file}: {}", line.trim()));
                    }
                }
            }
        }
    }
    found
}

#[test]
fn every_role_name_is_spelled_in_the_place_it_is_declared() {
    let names = declared();
    assert!(
        names.len() >= 5,
        "只从 cog_core::roles::ALL 读到 {} 个名字，这条判据没有题目",
        names.len()
    );

    let mut rendered = Vec::new();
    let mut total = 0;
    for (name, hits) in sites(&repo_root(), &names) {
        total += hits.len();
        let mut block = format!("  {name}（{} 处）", hits.len());
        for hit in hits.iter() {
            block.push_str("\n      ");
            block.push_str(hit);
        }
        rendered.push(block);
    }

    assert!(
        total < 200,
        "数出 {total} 处角色名，比这棵树里可能有的多得多，说明读错了段落"
    );
    assert!(
        rendered.is_empty(),
        "这些行把角色名又写了一遍。改名时改一处不会报错：按旧名字取 schema 的\
         那一侧拿到 `None`，按旧名字找 checkpoint 的那一侧什么也找不到，两者\
         都不算失败。改成读 `cog_core::roles` 里的声明：\n{}",
        rendered.join("\n")
    );
}

#[test]
fn the_declaration_list_covers_every_name_declared_in_the_module() {
    let path = repo_root().join(HOME);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let consts = production_lines(&text)
        .into_iter()
        .filter(|line| line.trim_start().starts_with("pub const "))
        .filter(|line| line.contains(": &str = "))
        .count();
    assert_eq!(
        consts,
        declared().len(),
        "roles 模块声明了 {consts} 个角色名，ALL 里只有 {} 个：漏掉的那个没有\
         任何消费者读得到，也没人拦得住别人重新拼一遍",
        declared().len()
    );
}

#[test]
fn the_reader_ignores_prose_and_longer_names() {
    let values = |line: &str| -> Vec<String> {
        literals(line)
            .into_iter()
            .map(|(value, _)| value.to_string())
            .collect()
    };
    assert_eq!(values(r#"let a = "planner";"#), vec!["planner"]);
    // 整条字面量才算：句子和更长的名字都不是同一处拼写。
    assert!(!values(r#"let a = "the planner decides";"#).contains(&"planner".to_string()));
    assert!(!values(r#"let a = "planner_v2";"#).contains(&"planner".to_string()));
    // `Debug` 的字段标签不是角色名的第二处拼写。
    let label = r#".field("moderator", &self.moderator.is_some())"#;
    let (quote, before) = literals(label)[0];
    assert_eq!(quote, "moderator");
    assert!(is_debug_label(before));
    // 注释行不参与统计。
    let text = "// let role = \"planner\";\nfn f() { let role = \"planner\"; }\n";
    let counted: Vec<&str> = production_lines(text)
        .into_iter()
        .flat_map(literals)
        .filter(|(value, before)| *value == "planner" && !is_debug_label(before))
        .map(|(value, _)| value)
        .collect();
    assert_eq!(counted, vec!["planner"]);
}

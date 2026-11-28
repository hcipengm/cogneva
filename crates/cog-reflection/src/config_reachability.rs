//! Which defaults a change writes that no deployment can read.
//!
//! The reading is a criterion of reflection: it decides whether a change has any
//! effect at all, and its only caller is the change pipeline in this crate. It
//! lives here rather than in `cog-core` because the base keeps the shapes that
//! cross crates -- the traits and the types -- and a reader of Rust sources and
//! JSON documents with one consumer belongs to that consumer.
//!
//! What stays in the base is the half more than one crate needs: the diff
//! readers (`parse_diff_affected_files`, `diff_file_entries`, `failure_digest`)
//! are called by the landing and collaboration crates too. Moving this module
//! did not touch them.

use std::collections::{BTreeMap, BTreeSet};

/// A line this change writes whose only effect is a default no deployment
/// reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableDefault {
    pub file: String,
    pub line: u64,
    /// The field the line assigns, as the document would spell it.
    pub key: String,
}

/// The written lines whose value nothing can reach, empty when the change has
/// an effect anywhere.
///
/// The criterion is deliberately all-or-nothing, and every input it cannot
/// read answers "no". A change that also writes a line outside a
/// `Default`-impl block is not a no-op, and neither is one whose written line
/// is not a literal — a call, a constructor, a path. The documents are what
/// make the default unreachable: the loader deserializes a document into the
/// type and only falls back to the type's `Default` for a key the document
/// leaves out, so the criterion needs the key written by **every** document
/// and not merely by one. A section a document does not carry at all is a
/// document that does not write the key, so the refusal is not reached.
///
/// `written` is the line numbers the change adds, in the applied file;
/// `sources` is those files as the applied tree holds them, keyed the same way.
/// A file `sources` has nothing for, and an empty document list, both answer
/// "no" rather than convicting on a partial reading.
pub fn unreachable_defaults(
    written: &BTreeMap<String, BTreeSet<u64>>,
    sources: &BTreeMap<String, String>,
    documents: &[serde_json::Value],
) -> Vec<UnreachableDefault> {
    if written.is_empty() || documents.is_empty() {
        return Vec::new();
    }
    let mut found = Vec::new();
    for (file, lines) in written {
        let Some(source) = sources.get(file) else {
            return Vec::new();
        };
        let blocks = default_impl_blocks(source);
        let text: Vec<&str> = source.lines().collect();
        for &line in lines {
            let Some(block) = blocks.iter().find(|b| b.start <= line && line <= b.end) else {
                return Vec::new();
            };
            let Some(key) = text
                .get(line as usize - 1)
                .and_then(|text| literal_field_assignment(text))
            else {
                return Vec::new();
            };
            if !documents
                .iter()
                .all(|doc| section_writes(doc, &block.section, &key))
            {
                return Vec::new();
            }
            found.push(UnreachableDefault {
                file: file.clone(),
                line,
                key,
            });
        }
    }
    found
}

/// One `impl Default for <Name>Config` block: the lines it spans, and the
/// top-level section the document spells that struct as.
struct DefaultImpl {
    start: u64,
    end: u64,
    section: String,
}

/// The `Default`-impl blocks a file carries, by line range.
///
/// Braces are counted without regard for string literals or comments: a brace
/// inside either would move an end. That can only cut a block short, and a line
/// falling outside every block answers "no" above — the reading that costs a
/// round is the one that fires on a change it cannot place, not the one that
/// stays quiet.
fn default_impl_blocks(source: &str) -> Vec<DefaultImpl> {
    let mut blocks = Vec::new();
    let lines: Vec<&str> = source.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let Some(rest) = line.trim_start().strip_prefix("impl Default for ") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let Some(section) = name.strip_suffix("Config").map(snake_case) else {
            continue;
        };
        if section.is_empty() {
            continue;
        }
        let mut depth: i64 = 0;
        let mut opened = false;
        let mut end = None;
        for (offset, body) in lines[index..].iter().enumerate() {
            for c in body.chars() {
                match c {
                    '{' => {
                        depth += 1;
                        opened = true;
                    }
                    '}' => depth -= 1,
                    _ => {}
                }
            }
            if opened && depth <= 0 {
                end = Some(index as u64 + offset as u64 + 1);
                break;
            }
        }
        if let Some(end) = end {
            blocks.push(DefaultImpl {
                start: index as u64 + 1,
                end,
                section,
            });
        }
    }
    blocks
}

/// The field a line assigns, when the line assigns a literal and nothing else.
fn literal_field_assignment(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with("//") {
        return None;
    }
    let (key, value) = line.split_once(':')?;
    let key = key.trim();
    if key.is_empty() || !key.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let value = value.trim().trim_end_matches(',').trim();
    // `500_000` is a Rust literal and not a number `f64::from_str` takes, so the
    // separators come out before the value is asked whether it is one.
    let literal = value.replace('_', "").parse::<f64>().is_ok()
        || matches!(value, "true" | "false" | "None")
        || (value.len() >= 2 && value.starts_with('"') && value.ends_with('"'));
    literal.then(|| key.to_string())
}

/// Whether this document writes the key anywhere inside the section.
///
/// Inside rather than at the top of it: a section's reader may take a value out
/// by pointer, and the key's own path is what the reader reads either way.
fn section_writes(document: &serde_json::Value, section: &str, key: &str) -> bool {
    document
        .get(section)
        .is_some_and(|section| subtree_has_key(section, key))
}

fn subtree_has_key(value: &serde_json::Value, key: &str) -> bool {
    match value {
        serde_json::Value::Object(map) => map
            .iter()
            .any(|(name, child)| name == key || subtree_has_key(child, key)),
        _ => false,
    }
}

/// `SelfEvolution` → `self_evolution`: the rule the document's section names
/// follow the struct names by.
fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (index, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusal only fires on a change that can have no effect anywhere, and
    /// each half of that sentence is a thing the reading has to establish.
    #[test]
    fn only_a_default_no_document_leaves_open_is_unreachable() {
        let source = "\
impl MetricsConfig {
    fn budget(&self) -> u64 { self.sample_max_rows }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            sample_max_rows: 500_000,
            log_at_floor: false,
        }
    }
}
";
        let sources =
            BTreeMap::from([("crates/cog-core/src/config.rs".to_string(), source.into())]);
        let written = BTreeMap::from([(
            "crates/cog-core/src/config.rs".to_string(),
            BTreeSet::from([8]),
        )]);
        let both = vec![
            serde_json::json!({ "metrics": { "sample_max_rows": 200000 } }),
            serde_json::json!({ "metrics": { "sample_max_rows": 200000, "log_at_floor": false } }),
        ];

        let found = unreachable_defaults(&written, &sources, &both);
        assert_eq!(found.len(), 1, "这一笔的全部效果就是这个读不到的默认值");
        assert_eq!(found[0].key, "sample_max_rows");
        assert_eq!(found[0].line, 8);

        // 少一份文档写这个键 ⇒ 那份部署的进程读的就是这个默认值，它有读者。
        let one_writes_it = vec![
            serde_json::json!({ "metrics": { "sample_max_rows": 200000 } }),
            serde_json::json!({ "metrics": { "log_at_floor": false } }),
        ];
        assert!(
            unreachable_defaults(&written, &sources, &one_writes_it).is_empty(),
            "只有全部文档都写着这个键，默认值才不可达"
        );

        // 键在别的段里不算：读它的是那一段的类型，不是这个默认值。
        let elsewhere = vec![
            serde_json::json!({ "metrics": { "log_at_floor": false }, "memory": { "sample_max_rows": 1 } }),
            serde_json::json!({ "metrics": { "log_at_floor": false }, "memory": { "sample_max_rows": 1 } }),
        ];
        assert!(unreachable_defaults(&written, &sources, &elsewhere).is_empty());

        // 文档一份都读不到时不是「查过，没有」。
        assert!(unreachable_defaults(&written, &sources, &[]).is_empty());

        // 改到 impl 之外的行（第 2 行是那行函数体）⇒ 这笔变更有别的效果。
        let outside = BTreeMap::from([(
            "crates/cog-core/src/config.rs".to_string(),
            BTreeSet::from([2]),
        )]);
        assert!(unreachable_defaults(&outside, &sources, &both).is_empty());

        // 写的不是字面量（调用、构造、路径）就不是这一类。
        let call = "\
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            budget: default_budget(),
        }
    }
}
";
        let call_sources =
            BTreeMap::from([("crates/cog-core/src/config.rs".to_string(), call.into())]);
        let call_written = BTreeMap::from([(
            "crates/cog-core/src/config.rs".to_string(),
            BTreeSet::from([4]),
        )]);
        let call_docs = vec![serde_json::json!({ "metrics": { "budget": 1 } })];
        assert!(unreachable_defaults(&call_written, &call_sources, &call_docs).is_empty());

        // 读不到被写的那份文件 ⇒ 判不了，不出口。
        let unreadable = BTreeMap::from([("nope.rs".to_string(), BTreeSet::from([1]))]);
        assert!(unreachable_defaults(&unreadable, &sources, &both).is_empty());
    }

    /// 那笔实测的 no-op 就是这一形：结构名到段名的对应要按文档的写法来。
    #[test]
    fn a_section_name_is_the_struct_name_without_its_config_suffix() {
        assert_eq!(snake_case("SelfEvolution"), "self_evolution");
        assert_eq!(snake_case("Metrics"), "metrics");
        assert_eq!(snake_case("DagExecutor"), "dag_executor");
    }
}

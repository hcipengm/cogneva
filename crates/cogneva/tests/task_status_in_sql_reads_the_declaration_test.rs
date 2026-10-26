//! principle: feedback_no_hardcoding_config_surface
//! `TaskStatus::as_str` is documented as the single place a task's state becomes
//! a string, and the rows stored under it are read back by hand-written SQL.
//! Those two facts meet badly: a query spells the word itself, so the column
//! and the enum agree only as long as nobody renames a variant.
//!
//! What makes this worth a gate is that the disagreement is silent. A variant
//! renamed on the writing side leaves the query matching the old spelling, and
//! a query that matches nothing is indistinguishable from a table that is
//! empty: `open_face_bound` answers "no face has work", the coverage reading
//! says the platform is idle, and the handover applies nothing — every one of
//! them a plausible state, none of them an error. The database never complains,
//! the compile never complains, and the rows are right there.
//!
//! So the words may enter SQL only through `as_str`. This reads the production
//! half of every crate and fails on a comparison against `status` whose
//! operand spells a state `TaskStatus::parse` recognises.

use std::path::{Path, PathBuf};

mod common;

use common::rust_production::production_text;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

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

fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && (b[i] as char).is_whitespace() {
        i += 1;
    }
    i
}

/// Whether `word` stands at `i` as a whole word, in either case.
fn word_at(b: &[u8], i: usize, word: &str) -> bool {
    b.len() >= i + word.len()
        && b[i..i + word.len()].eq_ignore_ascii_case(word.as_bytes())
        && b.get(i + word.len()).is_none_or(|c| !is_ident(*c))
}

/// `'pending'` at `i`, and where it ends.
fn quoted(b: &[u8], i: usize) -> Option<(String, usize)> {
    if b.get(i) != Some(&b'\'') {
        return None;
    }
    let start = i + 1;
    let mut j = start;
    while j < b.len() && b[j] != b'\'' {
        j += 1;
    }
    if j >= b.len() {
        return None;
    }
    Some((String::from_utf8_lossy(&b[start..j]).into_owned(), j + 1))
}

/// The quoted words of a parenthesised list, `('a', 'b')` or `['a', 'b']`.
///
/// A list that opens nothing is still read from `i`, because `= 'a'` is the
/// same operand shape as `IN ('a')` with the brackets spelled by the grammar.
fn quoted_list(b: &[u8], mut i: usize) -> Vec<String> {
    if matches!(b.get(i), Some(b'(') | Some(b'[')) {
        i = skip_ws(b, i + 1);
    }
    let mut out = Vec::new();
    loop {
        match quoted(b, i) {
            Some((word, next)) => {
                out.push(word);
                i = skip_ws(b, next);
            }
            None => break,
        }
        if b.get(i) == Some(&b',') {
            i = skip_ws(b, i + 1);
        } else {
            break;
        }
    }
    out
}

/// The words a comparison starting at `i` is written against.
///
/// `= 'x'`, `IN ('x', 'y')`, `NOT IN (...)`, `= ANY($5)` (no words, bound) and
/// `= ANY(ARRAY['x'])` are all one shape to a reader that only wants the
/// spelled words.
fn compared_words(b: &[u8], mut i: usize) -> Option<Vec<String>> {
    i = skip_ws(b, i);
    if b.get(i) == Some(&b'=') {
        // `ANY(ARRAY['x'])` 的包装层可以套几层，一层一层剥到词表为止。
        let mut j = skip_ws(b, i + 1);
        loop {
            if word_at(b, j, "ANY") {
                j = skip_ws(b, j + 3);
            } else if word_at(b, j, "ARRAY") {
                j = skip_ws(b, j + 5);
            } else if matches!(b.get(j), Some(b'(') | Some(b'[')) {
                j = skip_ws(b, j + 1);
            } else {
                break;
            }
        }
        return Some(quoted_list(b, j));
    }
    let mut j = i;
    if word_at(b, j, "NOT") {
        j = skip_ws(b, j + 3);
    }
    if word_at(b, j, "IN") {
        let k = skip_ws(b, j + 2);
        if b.get(k) != Some(&b'(') {
            return None;
        }
        return Some(quoted_list(b, k));
    }
    None
}

/// Every comparison against a `status` column or key, with the words it names
/// and the text the comparison is written as.
///
/// The subject is the column, not the word: `outcome = 'running'` and
/// `state = 'pending'` are different vocabularies with their own homes, and
/// reading them as task states would be a true statement about the wrong
/// thing. The word may arrive quoted, as a SQL identifier or a JSON key
/// (`task ->> 'status' = 'completed'`), and may carry a cast.
fn status_predicates(sql: &str) -> Vec<(String, Vec<String>)> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = sql[from..].find("status") {
        let at = from + rel;
        from = at + "status".len();
        if at > 0 && is_ident(b[at - 1]) {
            continue;
        }
        let mut j = at + "status".len();
        if j < b.len() && is_ident(b[j]) {
            continue;
        }
        if matches!(b.get(j), Some(b'\'') | Some(b'"')) {
            j += 1;
        }
        j = skip_ws(b, j);
        if b.get(j) == Some(&b':') && b.get(j + 1) == Some(&b':') {
            j += 2;
            while j < b.len() && is_ident(b[j]) {
                j += 1;
            }
        }
        if let Some(words) = compared_words(b, j) {
            // 报错的上下文取自这一处查询本身：折掉空白后一行放得下，也能直接
            // 拿去 grep 原文。
            let mut end = at;
            for (offs, ch) in sql[at..].char_indices() {
                if offs > 70 {
                    break;
                }
                end = at + offs + ch.len_utf8();
            }
            let context = sql[at..end]
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            out.push((context, words));
        }
    }
    out
}

/// One query that names a task state in its own text.
struct Hit {
    file: String,
    word: String,
    context: String,
}

fn hits(root: &Path) -> (Vec<Hit>, usize) {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root.join("crates"))
        .expect("crates/ 不在仓库里")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    files.sort();
    let mut found = Vec::new();
    let mut compared = 0usize;
    for krate in files {
        let src = krate.join("src");
        if !src.is_dir() {
            continue;
        }
        let mut paths = Vec::new();
        walk(&src, &mut paths);
        paths.sort();
        for path in paths {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let production = production_text(&text);
            let file = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            for (context, words) in status_predicates(&production) {
                compared += 1;
                for word in words {
                    if cog_core::TaskStatus::parse(&word).is_none() {
                        continue;
                    }
                    found.push(Hit {
                        file: file.clone(),
                        word,
                        context: context.clone(),
                    });
                }
            }
        }
    }
    (found, compared)
}

#[test]
fn every_task_state_in_a_query_comes_from_the_declaration() {
    let (found, compared) = hits(&repo_root());
    // 这个数只用来证明读到了查询本身：重写后的模板仍然写着 status IN (...)
    // 的占位符，所以「一处都没看见」不等于「一处都没有」。
    assert!(
        compared >= 3,
        "全树只读到 {compared} 处 status 比较，比实际存在的少得多，说明读错了段落"
    );
    assert!(
        found.is_empty(),
        "这些查询自己拼了任务状态词。改一位枚举的名字不会报错：查询照旧匹配\
         旧的拼写，一条也选不中，读起来像「表里没有这种活」。改成 `.bind(...)` \
         或 `TaskStatus::X.as_str()`：\n{}",
        found
            .iter()
            .map(|hit| format!("  {}: {} {}", hit.file, hit.word, hit.context))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_reader_sees_every_shape_a_query_writes_a_state_in() {
    let words = |sql: &str| -> Vec<String> {
        status_predicates(sql)
            .into_iter()
            .flat_map(|(_, words)| words)
            .collect()
    };
    assert_eq!(words("WHERE status = 'completed'"), vec!["completed"]);
    assert_eq!(
        words("WHERE status IN ('pending', 'scheduled', 'running')"),
        vec!["pending", "scheduled", "running"]
    );
    assert_eq!(words("WHERE status NOT IN ('failed')"), vec!["failed"]);
    assert_eq!(words("WHERE task ->> 'status' = 'failed'"), vec!["failed"]);
    assert_eq!(words("WHERE t.status::text = 'running'"), vec!["running"]);
    assert_eq!(
        words("WHERE status = ANY(ARRAY['pending'])"),
        vec!["pending"]
    );
    // 绑定参数与 JSON 路径不是拼写：没有词可读，也不该读出错词。
    assert!(words("WHERE status = $3").is_empty());
    assert!(words("WHERE status = ANY($5)").is_empty());
    assert!(words("jsonb_set(task, '{status}', to_jsonb($2))").is_empty());
    // 主体是 status 这一列：别的列的同名单词是另一套词表。
    assert!(words("WHERE outcome = 'running'").is_empty());
    assert!(words("WHERE state = 'pending'").is_empty());
}

//! principle: feedback_no_hardcoding_config_surface
//! A closed vocabulary is declared once: `TaskStatus::as_str`,
//! `ChangeState::as_str`, `RetireReason::as_str` each name every variant, and
//! the rows stored under those names are read back by hand-written SQL. Those
//! two facts meet badly: a query spells the word itself, so the column and the
//! enum agree only as long as nobody renames a variant.
//!
//! What makes this worth a gate is that the disagreement is silent. A query
//! matching the old spelling returns no rows, and no rows is what these tables
//! return when there is nothing to say: `open_face_bound` answers "no face has
//! work", `outstanding` answers "nothing is owed", the retire census answers
//! "nothing was retired", and every caller reads those as ordinary states. The
//! database never complains, the compile never complains, and the rows are
//! right there.
//!
//! So the words may enter SQL only through `as_str`. This reads the production
//! half of every crate and fails on a comparison against one of these columns
//! whose operand spells a word the vocabulary's own parser accepts.

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

fn parses_as_task_status(word: &str) -> bool {
    cog_core::TaskStatus::parse(word).is_some()
}

fn parses_as_change_state(word: &str) -> bool {
    word.parse::<cog_core::ChangeState>().is_ok()
}

fn parses_as_retire_reason(word: &str) -> bool {
    word.parse::<cog_core::RetireReason>().is_ok()
}

fn parses_as_principle_state(word: &str) -> bool {
    cog_core::PrincipleState::parse(word).is_some()
}

fn parses_as_principle_check_kind(word: &str) -> bool {
    cog_core::PrincipleCheckKind::parse(word).is_some()
}

fn parses_as_blocked_reason(word: &str) -> bool {
    word.parse::<cog_core::BlockedReason>().is_ok()
}

fn parses_as_user_status(word: &str) -> bool {
    cog_core::UserStatus::ALL
        .iter()
        .any(|status| status.as_str() == word)
}

fn parses_as_user_type(word: &str) -> bool {
    cog_core::UserType::ALL
        .iter()
        .any(|tier| tier.as_str() == word)
}

fn parses_as_backup_outcome(word: &str) -> bool {
    cog_core::BackupOutcome::parse(word).is_some()
}

fn parses_as_promotion_status(word: &str) -> bool {
    cog_core::PromotionStatus::parse(word).is_some()
}

fn parses_as_notification_type(word: &str) -> bool {
    cog_core::NotificationType::ALL
        .iter()
        .any(|kind| kind.as_str() == word)
}

fn parses_as_alert_state(word: &str) -> bool {
    cog_core::AlertState::parse(word).is_some()
}

fn parses_as_metric_type(word: &str) -> bool {
    cog_core::MetricType::ALL
        .iter()
        .any(|kind| kind.as_str() == word)
}

/// One closed set of words and the columns a query asks about it with.
struct Vocabulary {
    /// The column names this vocabulary is stored under. The subject is the
    /// column, not the word: `outcome = 'running'` and `state = 'pending'` are
    /// different vocabularies with their own homes, and reading them as this
    /// one would be a true statement about the wrong thing.
    subjects: &'static [&'static str],
    /// Whether a word belongs to the set. The vocabulary's own parser is the
    /// judge, so a variant added to the enum is covered without editing here.
    parses: fn(&str) -> bool,
    /// The type that declares the words, for the failure message.
    home: &'static str,
    /// Files where this column name holds a *different* declared set.
    ///
    /// Two ledgers both keep a `state` column, and both call a row `retired`:
    /// the change ledger means "this change no longer belongs to the account",
    /// the principle ledger means "this principle's carrier left the roster".
    /// The column name alone cannot say which set a query means; the table can,
    /// and these are the two files that own one each. A file listed here is
    /// still read -- by the other vocabulary's entry -- so the exemption moves
    /// the check rather than dropping it.
    except_in: &'static [&'static str],
}

const CHANGE_LEDGER: &str = "crates/cog-storage/src/postgres/change_ledger_store.rs";
const PRINCIPLE_LEDGER: &str = "crates/cog-storage/src/postgres/principle_ledger_store.rs";

const VOCABULARIES: &[Vocabulary] = &[
    Vocabulary {
        subjects: &["status"],
        parses: parses_as_task_status,
        home: "cog_core::TaskStatus",
        except_in: &[],
    },
    Vocabulary {
        // `to_state` / `from_state` are the event log's columns for the same
        // set; a query on them drifts the same way and reads as "this change
        // never moved".
        subjects: &["state", "to_state", "from_state"],
        parses: parses_as_change_state,
        home: "cog_core::ChangeState",
        except_in: &[PRINCIPLE_LEDGER],
    },
    Vocabulary {
        subjects: &["retire_reason"],
        parses: parses_as_retire_reason,
        home: "cog_core::RetireReason",
        except_in: &[],
    },
    Vocabulary {
        subjects: &["state"],
        parses: parses_as_principle_state,
        home: "cog_core::PrincipleState",
        except_in: &[CHANGE_LEDGER],
    },
    Vocabulary {
        subjects: &["check_kind"],
        parses: parses_as_principle_check_kind,
        home: "cog_core::PrincipleCheckKind",
        except_in: &[],
    },
    Vocabulary {
        // The backup account is read by counting this column's values, so a
        // stale word makes one count read zero: "no run ever ended that way".
        subjects: &["outcome"],
        parses: parses_as_backup_outcome,
        home: "cog_core::BackupOutcome",
        except_in: &[],
    },
    Vocabulary {
        // A third set under the `status` column name. `pending` and `failed`
        // belong to both this one and `TaskStatus`; neither is spelled in a
        // query today, and if one comes back the hit is a real one either way.
        subjects: &["status"],
        parses: parses_as_promotion_status,
        home: "cog_core::PromotionStatus",
        except_in: &[],
    },
    Vocabulary {
        // The closed set the table's own constraint lists. A constraint that
        // drifts from the enum refuses the writer's rows on the first
        // notification after a rename.
        subjects: &["ntype"],
        parses: parses_as_notification_type,
        home: "cog_core::NotificationType",
        except_in: &[],
    },
    Vocabulary {
        // The wait beside the state. A stale spelling here does not fail the
        // write: the stamp overwrites a block somebody else set, and the clear
        // releases rows it was never meant to touch.
        subjects: &["blocked_reason"],
        parses: parses_as_blocked_reason,
        home: "cog_core::BlockedReason",
        except_in: &[],
    },
    Vocabulary {
        // Two sets share the `status` column name: a task's progress and an
        // account's standing. The words do not overlap, so each parser accepts
        // its own and the file-level exemption the two `state` columns needed
        // is not needed here.
        subjects: &["status"],
        parses: parses_as_user_status,
        home: "cog_core::UserStatus",
        except_in: &[],
    },
    Vocabulary {
        subjects: &["user_type"],
        parses: parses_as_user_type,
        home: "cog_core::UserType",
        except_in: &[],
    },
    Vocabulary {
        // The alert lifecycle. A guard that names a word the enum no longer
        // produces stops closing rows: the alert stays open, and an operator
        // reads "nothing to resolve" rather than a stuck row.
        subjects: &["state"],
        parses: parses_as_alert_state,
        home: "cog_core::AlertState",
        except_in: &[],
    },
    Vocabulary {
        // The samples table is read back by kind, so a stale kind word answers
        // "this series has no samples yet".
        subjects: &["metric_type"],
        parses: parses_as_metric_type,
        home: "cog_core::MetricType",
        except_in: &[],
    },
];

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
    while let Some((word, next)) = quoted(b, i) {
        out.push(word);
        i = skip_ws(b, next);
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
/// `= 'x'`, `IN ('x', 'y')`, `NOT IN (...)`, `IS DISTINCT FROM 'x'`,
/// `= ANY($5)` (no words, bound) and `= ANY(ARRAY['x'])` are all one shape to a
/// reader that only wants the spelled words.
fn compared_words(b: &[u8], mut i: usize) -> Option<Vec<String>> {
    i = skip_ws(b, i);
    let op = if word_at(b, i, "IS") {
        let mut j = skip_ws(b, i + 2);
        if word_at(b, j, "NOT") {
            j = skip_ws(b, j + 3);
        }
        if !word_at(b, j, "DISTINCT") {
            return None;
        }
        j = skip_ws(b, j + 8);
        if !word_at(b, j, "FROM") {
            return None;
        }
        Some(skip_ws(b, j + 4))
    } else if b.get(i) == Some(&b'=') {
        Some(skip_ws(b, i + 1))
    } else {
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
        return None;
    };
    // `ANY(ARRAY['x'])` 的包装层可以套几层，一层一层剥到词表为止。
    let mut j = op?;
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
    Some(quoted_list(b, j))
}

/// Every comparison against one of `subjects`, with the words it names and the
/// text the comparison is written as.
///
/// The subject may arrive plain (`status`), quoted as a SQL identifier or a
/// JSON key (`task ->> 'status' = 'completed'`), qualified (`l.state`), or
/// carrying a cast (`t.status::text`).
fn predicates(sql: &str, subjects: &[&str]) -> Vec<(String, Vec<String>)> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    for subject in subjects {
        let mut from = 0;
        while let Some(rel) = sql[from..].find(subject) {
            let at = from + rel;
            from = at + subject.len();
            if at > 0 && is_ident(b[at - 1]) {
                continue;
            }
            let mut j = at + subject.len();
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
                // 报错的上下文取自这一处查询本身：折掉空白后一行放得下，也能
                // 直接拿去 grep 原文。
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
    }
    out
}

/// One comparison that names a word the declaration already decides.
struct Hit {
    file: String,
    home: &'static str,
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
            for vocabulary in VOCABULARIES {
                if vocabulary.except_in.contains(&file.as_str()) {
                    continue;
                }
                for (context, words) in predicates(&production, vocabulary.subjects) {
                    compared += 1;
                    for word in words {
                        if !(vocabulary.parses)(&word) {
                            continue;
                        }
                        found.push(Hit {
                            file: file.clone(),
                            home: vocabulary.home,
                            word,
                            context: context.clone(),
                        });
                    }
                }
            }
        }
    }
    (found, compared)
}

#[test]
fn every_closed_vocabulary_in_a_query_comes_from_its_declaration() {
    let (found, compared) = hits(&repo_root());
    // 这个数只用来证明读到了查询本身：重写后的模板仍然写着 status IN (...)
    // 之类的占位符，所以「一处都没看见」不等于「一处都没有」。
    assert!(
        compared >= 3,
        "全树只读到 {compared} 处状态比较，比实际存在的少得多，说明读错了段落"
    );
    assert!(
        found.is_empty(),
        "这些查询自己拼了状态词。改一位枚举的名字不会报错：查询照旧匹配旧的\
         拼写，一条也选不中，读起来像「表里没有这样的行」。改成 `.bind(...)`、\
         模板占位符或 `X::as_str()`：\n{}",
        found
            .iter()
            .map(|hit| format!(
                "  {}: {} 属于 {} — {}",
                hit.file, hit.word, hit.home, hit.context
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_reader_sees_every_shape_a_query_writes_a_word_in() {
    let words = |sql: &str| -> Vec<String> {
        predicates(sql, &["status", "state", "to_state", "retire_reason"])
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
    assert_eq!(
        words("WHERE l.state IS DISTINCT FROM 'landed'"),
        vec!["landed"]
    );
    assert_eq!(words("WHERE e.to_state = 'landed'"), vec!["landed"]);
    assert_eq!(
        words("WHERE retire_reason = 'surface_moved'"),
        vec!["surface_moved"]
    );
    // 绑定参数与 JSON 路径不是拼写：没有词可读，也不该读出错词。
    assert!(words("WHERE status = $3").is_empty());
    assert!(words("WHERE status = ANY($5)").is_empty());
    assert!(words("jsonb_set(task, '{status}', to_jsonb($2))").is_empty());
    // 主体是列名：别的列的同名单词是另一套词表。
    assert!(words("WHERE outcome = 'running'").is_empty());
    assert!(words("WHERE blocked_reason = 'pool_unavailable'").is_empty());
    // `to_state` 里的 `state` 不是一个裸的 `state` 列。
    assert!(words("SELECT to_state FROM change_ledger_event").is_empty());
}

#[test]
fn an_exemption_still_stands_for_something() {
    // 豁免过期了必须有人看见：一个不再排除任何东西的豁免，是给下一个人留的
    // 免费通行证。
    let root = repo_root();
    for vocabulary in VOCABULARIES {
        for file in vocabulary.except_in {
            let path = root.join(file);
            assert!(path.is_file(), "豁免指向的文件不存在：{file}");
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let found = predicates(&production_text(&text), vocabulary.subjects);
            assert!(
                !found.is_empty(),
                "{file} 里已经读不到 {} 的任何一列，这条豁免可以删了",
                vocabulary.home
            );
        }
    }
}

#[test]
fn each_vocabulary_reads_its_own_declaration() {
    // 判据不能是空集：三个解析函数都得认得自己那一族里的词，也得拒绝别人家的
    // 词，否则这条门对着一整族词永远是绿的。
    for vocabulary in VOCABULARIES {
        assert!(
            !vocabulary.subjects.is_empty(),
            "{} 没有列名，这条门对它没有题目",
            vocabulary.home
        );
    }
    assert!(parses_as_task_status("completed"));
    assert!(!parses_as_task_status("landed"));
    assert!(parses_as_change_state("landed"));
    assert!(!parses_as_change_state("completed"));
    assert!(parses_as_retire_reason("surface_moved"));
    assert!(!parses_as_retire_reason("landed"));
    assert!(parses_as_principle_state("unenforced"));
    assert!(parses_as_principle_check_kind("judgement"));
    for state in cog_core::ChangeState::ALL {
        assert!(parses_as_change_state(state.as_str()));
    }
    for reason in cog_core::RetireReason::ALL {
        assert!(parses_as_retire_reason(reason.as_str()));
    }
    for state in cog_core::PrincipleState::ALL {
        assert!(parses_as_principle_state(state.as_str()));
    }
    for kind in cog_core::PrincipleCheckKind::ALL {
        assert!(parses_as_principle_check_kind(kind.as_str()));
    }
    for reason in cog_core::BlockedReason::ALL {
        assert!(parses_as_blocked_reason(reason.as_str()));
    }
    for status in cog_core::UserStatus::ALL {
        assert!(parses_as_user_status(status.as_str()));
    }
    for tier in cog_core::UserType::ALL {
        assert!(parses_as_user_type(tier.as_str()));
    }
    for kind in cog_core::MetricType::ALL {
        assert!(parses_as_metric_type(kind.as_str()));
    }
    for state in cog_core::AlertState::ALL {
        assert!(parses_as_alert_state(state.as_str()));
    }
    assert!(!parses_as_alert_state("landed"));
    for outcome in cog_core::BackupOutcome::ALL {
        assert!(parses_as_backup_outcome(outcome.as_str()));
    }
    for status in cog_core::PromotionStatus::ALL {
        assert!(parses_as_promotion_status(status.as_str()));
    }
    for kind in cog_core::NotificationType::ALL {
        assert!(parses_as_notification_type(kind.as_str()));
    }
    assert!(!parses_as_backup_outcome("firing"));
    assert!(!parses_as_notification_type("promoted"));
    // A word from another set is not this set's: the judge has to be a set, not
    // a shape.
    assert!(!parses_as_user_status("pending"));
    assert!(!parses_as_metric_type("landed"));
    assert!(!parses_as_blocked_reason("superseded"));
}

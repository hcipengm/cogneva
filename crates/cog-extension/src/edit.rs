//! Exact-string replacement, performed where the file lives.
//!
//! Both backends that can touch a file -- the in-process one and the executor
//! pod -- go through this one function, so "the anchor appears twice" is
//! answered the same way on both. A file is never written unless the whole
//! edit is unambiguous: the caller states which text to replace, and getting
//! that text wrong on a file the caller cannot see is exactly the mistake that
//! must cost an error and not a silent second guess.

use std::path::Path;

/// What one edit did, in the terms the caller can check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditReport {
    pub replacements: usize,
    /// 1-based line of each replacement, in file order.
    pub lines: Vec<usize>,
}

impl EditReport {
    /// The sentence an agent reads back: how many sites moved, and where.
    pub fn describe(&self, path: &str) -> String {
        let lines: Vec<String> = self.lines.iter().map(|n| n.to_string()).collect();
        if self.replacements == 1 {
            format!("edited {path}: 1 replacement at line {}", lines[0])
        } else {
            format!(
                "edited {path}: {} replacements at lines {}",
                self.replacements,
                lines.join(", ")
            )
        }
    }
}

/// 1-based line number of a byte offset, which callers only ever pass from
/// `match_indices` and is therefore a character boundary.
fn line_at(text: &str, offset: usize) -> usize {
    text[..offset].bytes().filter(|b| *b == b'\n').count() + 1
}

/// Replace `old` with `new` in the file at `path`.
///
/// Exactly one occurrence is expected unless `replace_all` is set; anything
/// else is refused with the sites named, before the file is touched. The
/// refusal has to carry the line numbers, because the caller cannot see the
/// file and has no other way to tell which of the sites it meant.
pub async fn apply(
    path: &Path,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Result<EditReport, String> {
    if old.is_empty() {
        return Err(
            "`old_string` is empty: an empty anchor matches everywhere, so it names no site to \
             change"
                .into(),
        );
    }
    if old == new {
        return Err(
            "`old_string` and `new_string` are identical, so the file stays as it is".into(),
        );
    }
    let text = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let sites: Vec<(usize, usize)> = text
        .match_indices(old)
        .map(|(start, matched)| (start, matched.len()))
        .collect();
    let lines: Vec<usize> = sites
        .iter()
        .map(|(start, _)| line_at(&text, *start))
        .collect();
    match sites.len() {
        0 => {
            return Err(format!(
                "`old_string` does not appear in {} -- read the file again and copy the text \
                 exactly, indentation included",
                path.display()
            ))
        }
        1 => {}
        n if !replace_all => {
            let named: Vec<String> = lines.iter().map(|n| n.to_string()).collect();
            return Err(format!(
                "`old_string` appears {n} times in {} (at lines {}); include more surrounding \
                 text to name one site, or set `replace_all` to change all of them",
                path.display(),
                named.join(", ")
            ));
        }
        _ => {}
    }

    let mut out = String::with_capacity(text.len() + new.len().saturating_sub(old.len()));
    let mut cursor = 0;
    for (index, (start, len)) in sites.iter().enumerate() {
        if !replace_all && index > 0 {
            break;
        }
        out.push_str(&text[cursor..*start]);
        out.push_str(new);
        cursor = start + len;
    }
    out.push_str(&text[cursor..]);
    tokio::fs::write(path, out)
        .await
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    Ok(EditReport {
        replacements: if replace_all { sites.len() } else { 1 },
        lines: if replace_all { lines } else { vec![lines[0]] },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn write(dir: &tempfile::TempDir, body: &str) -> std::path::PathBuf {
        let path = dir.path().join("f.txt");
        tokio::fs::write(&path, body).await.unwrap();
        path
    }

    #[tokio::test]
    async fn a_unique_anchor_is_replaced_and_the_line_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "one\ntwo\nthree\n").await;
        let report = apply(&path, "two", "TWO", false).await.unwrap();
        assert_eq!(report.replacements, 1);
        assert_eq!(report.lines, vec![2]);
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "one\nTWO\nthree\n"
        );
        assert_eq!(
            report.describe("f.txt"),
            "edited f.txt: 1 replacement at line 2"
        );
    }

    #[tokio::test]
    async fn an_ambiguous_anchor_is_refused_by_line_and_the_file_is_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "a\nx = 1;\nb\nx = 1;\n").await;
        let error = apply(&path, "x = 1;", "x = 2;", false).await.unwrap_err();
        assert!(error.contains("appears 2 times"), "{error}");
        assert!(error.contains("at lines 2, 4"), "{error}");
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "a\nx = 1;\nb\nx = 1;\n"
        );
    }

    #[tokio::test]
    async fn replace_all_moves_every_site_and_says_how_many() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "a\nx = 1;\nb\nx = 1;\n").await;
        let report = apply(&path, "x = 1;", "x = 2;", true).await.unwrap();
        assert_eq!(report.replacements, 2);
        assert_eq!(report.lines, vec![2, 4]);
        assert_eq!(
            report.describe("f.txt"),
            "edited f.txt: 2 replacements at lines 2, 4"
        );
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "a\nx = 2;\nb\nx = 2;\n"
        );
    }

    #[tokio::test]
    async fn a_missing_anchor_is_refused_and_the_file_is_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "one\ntwo\n").await;
        let error = apply(&path, "three", "3", false).await.unwrap_err();
        assert!(error.contains("does not appear"), "{error}");
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "one\ntwo\n"
        );
    }

    #[tokio::test]
    async fn an_empty_or_pointless_anchor_is_refused_before_the_file_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.txt");
        let empty = apply(&path, "", "x", false).await.unwrap_err();
        assert!(empty.contains("empty"), "{empty}");
        let same = apply(&path, "x", "x", false).await.unwrap_err();
        assert!(same.contains("identical"), "{same}");
    }

    #[tokio::test]
    async fn a_multiline_anchor_is_replaced_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "fn a() {}\n}\nfn b() {}\n").await;
        let report = apply(&path, "fn a() {}\n}\n", "fn a() {\n    work();\n}\n", false)
            .await
            .unwrap();
        assert_eq!(report.lines, vec![1]);
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "fn a() {\n    work();\n}\nfn b() {}\n"
        );
    }
}

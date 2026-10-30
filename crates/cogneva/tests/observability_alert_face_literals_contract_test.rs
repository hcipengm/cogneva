//! principle: feedback_a_declaration_of_who_acts_rides_the_row_and_stays_out_of_its_key
//! principle: feedback_the_half_that_is_easy_to_enumerate_is_not_the_half_that_fires
//! Every face a writer in this tree names is a face the rule carriers declare.
//!
//! Alert rows reach the `alerts` table from more than two places. Besides the
//! rules -- the chart's, evaluated in-process, and the manifests', evaluated by
//! Prometheus -- there are writers that have no rule at all: the gateway's pool
//! sync, the config delivery checks, the convergence check, the canary
//! blindness check and the bridge that turns supervisor events into rows. Each
//! of them writes the label that answers "which face acts on this", and each of
//! them spells the face as a literal, because a condition about the platform
//! itself has nowhere in the rule library to declare one.
//!
//! Those literals are the only copy of that answer. Rename a face in the rule
//! carriers and leave one of these behind, and nothing here fails: the row is
//! still written, still fires, and simply names a face nobody owns. The reading
//! that watches ownership does catch it -- as `face_unknown`, one of its three
//! unowned reasons -- but only once that row is firing, which for a condition
//! about a rarely broken thing can be weeks after the rename landed.
//!
//! So this compares the two populations directly: the faces the carriers
//! declare, and the faces the source spells. The carriers are read from the
//! tree rather than copied into a list here, because a copy would be a second
//! answer to "which faces exist" and would drift exactly like the literals do.
//! The carriers themselves are tied to the roster by
//! `deploy/scripts/check-alert-rule-faces.sh` (declared or not) and by the
//! ownership reading (declared face has a name on the roster and an owner);
//! this test is the third leg, the one that keeps the code from being the only
//! place a renamed face survives.
//!
//! The ceiling: only literals the tree can see are on the denominator -- a
//! `concat!`-built value or one assembled at run time has no literal to parse.
//! Each shape below is also required to match at least once, so a helper that
//! is refactored out of the shape this test reads fails loudly instead of
//! passing with an empty population.

use std::collections::BTreeSet;
use std::path::Path;

use crate::common;

use common::routes::{read_repo, repo_root, walk};

use common::rust_production::{all_crate_files, relative_name};

/// This file's own name, so the scan does not report the strings it parses
/// with as the literals it is looking for.
const SELF_FILE: &str = "observability_alert_face_literals_contract_test.rs";

/// One face literal found in the source, with where it was found.
struct CodeFace {
    file: String,
    line: usize,
    value: String,
    shape: &'static str,
}

fn read_at(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// 载体清单文件数下限。声明面是两处载体并起来的：chart 里的进程内规则与
/// `PrometheusRule` 清单。后一半读不出来时，并集还剩 chart 的 15 个面，
/// `declared.len() > 10` 照旧成立——一半载体消失不会让任何一句判词变红。
const MIN_MANIFESTS: usize = 10;

/// The faces the two rule carriers declare: the chart's in-process rules (a
/// `face` field per rule) and the PrometheusRule manifests (the label the rule
/// carries, which the mirror copies into the row).
fn declared_faces() -> BTreeSet<String> {
    let mut faces = BTreeSet::new();

    let chart = read_repo("deploy/helm/cogneva/files/cogneva.json");
    let cfg: serde_json::Value =
        serde_json::from_str(&chart).expect("chart config is not valid JSON");
    let rules = cfg["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("chart config has no observability.infra_watch.rules array");
    for rule in rules {
        if let Some(face) = rule.get("face").and_then(|f| f.as_str()) {
            let face = face.trim();
            if !face.is_empty() {
                faces.insert(face.to_string());
            }
        }
    }

    let mut manifests = Vec::new();
    walk(
        &repo_root().join("deploy/k3s/observability/manifests"),
        &["yaml"],
        &mut manifests,
    );
    assert!(
        manifests.len() >= MIN_MANIFESTS,
        "deploy/k3s/observability/manifests 下只读到 {} 份 .yaml（下限 {MIN_MANIFESTS}）：\
         声明面的清单那一半没了，chart 里的 15 个面单独就能让下面的下限成立",
        manifests.len()
    );
    for path in manifests {
        for value in label_values(&read_at(&path), "cogneva_io_face:") {
            faces.insert(value);
        }
    }

    faces
}

/// The value after `key` on every line that carries it, quotes stripped.
fn label_values(text: &str, key: &str) -> Vec<String> {
    let mut values = Vec::new();
    for line in text.lines() {
        let Some(at) = line.find(key) else { continue };
        let rest = line[at + key.len()..].trim();
        let value = rest
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_matches('"')
            .trim_matches('\'');
        if !value.is_empty() {
            values.push(value.to_string());
        }
    }
    values
}

/// The literal assigned by `const <NAME>_FACE: &str = "...";`.
fn const_face(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let decl = trimmed
        .strip_prefix("pub const ")
        .or_else(|| trimmed.strip_prefix("const "))?;
    let name = decl.split(':').next()?;
    if !name.ends_with("_FACE") {
        return None;
    }
    let at = line.find("= \"")?;
    let rest = &line[at + 3..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// The literal filled in for the face label directly, as
/// `ALERT_FACE_LABEL: "..."`.
fn inline_face(line: &str) -> Option<String> {
    let at = line.find("ALERT_FACE_LABEL: \"")?;
    let rest = &line[at + "ALERT_FACE_LABEL: \"".len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// The literals returned by a helper whose name ends in `_face` -- the shape a
/// dispatch table takes when one function maps several rule names to faces.
fn dispatch_faces(text: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(rel) = text[from..].find("fn ") {
        let start = from + rel;
        let line_end = text[start..].find('\n').map_or(text.len(), |n| start + n);
        let signature = &text[start..line_end];
        from = start + 3;
        let Some(name_at) = signature.find("fn ") else {
            continue;
        };
        let name: String = signature[name_at + 3..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.ends_with("_face") {
            continue;
        }
        // Walk to the matching close brace so only this helper's body is read.
        let mut depth = 0i32;
        let mut opened = false;
        let mut at = start;
        let body_end = loop {
            if at >= bytes.len() {
                break bytes.len();
            }
            match bytes[at] {
                b'{' => {
                    depth += 1;
                    opened = true;
                }
                b'}' => {
                    depth -= 1;
                    if opened && depth == 0 {
                        break at;
                    }
                }
                _ => {}
            }
            at += 1;
        };
        let body = &text[start..body_end];
        let line_no = text[..start].lines().count();
        let mut cursor = 0;
        while let Some(rel) = body[cursor..].find("Some(\"") {
            let lit_at = cursor + rel + "Some(\"".len();
            let Some(end) = body[lit_at..].find('"') else {
                break;
            };
            let literal = &body[lit_at..lit_at + end];
            if !literal.contains('\n') && !literal.is_empty() {
                let offset = line_no + body[cursor..lit_at].lines().count();
                found.push((offset, literal.to_string()));
            }
            cursor = lit_at + end;
        }
    }
    found
}

/// A file can only be writing a face if it names the label at all: either
/// through the shared constant or by spelling the key itself. Anything else is
/// a different `face` -- a test fixture's, an unrelated helper's -- and reading
/// it here would make the verdict about those instead of about alert rows.
fn writes_the_alert_face(text: &str) -> bool {
    text.contains("ALERT_FACE_LABEL") || text.contains("cogneva_io_face")
}

fn code_faces() -> Vec<CodeFace> {
    let root = repo_root();
    let mut found = Vec::new();
    for path in all_crate_files(&root) {
        // This file spells both the label and the shapes it parses, so it would
        // otherwise report itself as the writer it is looking for.
        if path.file_name().is_some_and(|n| n == SELF_FILE) {
            continue;
        }
        let rel = relative_name(&root, &path);
        let text = read_at(&path);
        if !writes_the_alert_face(&text) {
            continue;
        }
        for (i, line) in text.lines().enumerate() {
            if let Some(value) = const_face(line) {
                found.push(CodeFace {
                    file: rel.clone(),
                    line: i + 1,
                    value,
                    shape: "const",
                });
            }
            if let Some(value) = inline_face(line) {
                found.push(CodeFace {
                    file: rel.clone(),
                    line: i + 1,
                    value,
                    shape: "inline",
                });
            }
        }
        for (line, value) in dispatch_faces(&text) {
            found.push(CodeFace {
                file: rel.clone(),
                line,
                value,
                shape: "dispatch",
            });
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every face the source spells is one the rule carriers declare. A name
    /// that survives a rename only in Rust writes rows whose face nobody owns,
    /// and nothing fails until one of those rows is firing.
    #[test]
    fn every_face_the_source_spells_is_one_the_rule_carriers_declare() {
        let declared = declared_faces();
        assert!(
            declared.len() > 10,
            "只从载体里读出 {} 个面——载体解析坏了，这一格就没在判任何东西: {declared:?}",
            declared.len()
        );

        let code = code_faces();
        let strays: Vec<&CodeFace> = code
            .iter()
            .filter(|f| !declared.contains(&f.value))
            .collect();
        assert!(
            strays.is_empty(),
            "这些写者给行挂了一个载体里没有的面（那一行 firing 时读作 face_unknown，等于无主）:\n{}",
            strays
                .iter()
                .map(|f| format!(
                    "  {}:{} [{}] {:?}",
                    f.file, f.line, f.shape, f.value
                ))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    /// The two shapes the writers use today must still match something. A
    /// helper refactored into a shape this test does not parse would leave the
    /// population empty and the verdict above green for the wrong reason.
    /// `inline` is read but not required: a writer may fill the label in
    /// directly, and its absence today is not a sign the scan went blind.
    #[test]
    fn the_scan_still_matches_every_shape_it_reads() {
        let code = code_faces();
        for shape in ["const", "dispatch"] {
            let n = code.iter().filter(|f| f.shape == shape).count();
            assert!(
                n > 0,
                "「{shape}」这种写法一处都没扫到——多半是代码改成了另一种形状，\
                 而那意味着这一格对它是瞎的；把新形状加进 code_faces() 再让它绿"
            );
        }
    }

    /// 载体那一半读不出来时必须炸。旧写法读不出来就 `return`，清单那一半
    /// 悄悄变空，而 chart 的 15 个面单独就把 `declared.len() > 10` 顶住了——
    /// 正是这条判据最怕的那种「变弱但照旧绿」。
    #[test]
    fn a_manifest_directory_that_cannot_be_read_is_refused() {
        let missing = std::env::temp_dir().join(format!(
            "cogneva-face-manifests-absent-{}",
            std::process::id()
        ));
        let mut files = Vec::new();
        let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            walk(&missing, &["yaml"], &mut files)
        }));
        assert!(
            got.is_err(),
            "清单目录不存在时载体解析交了一张空表——声明面只剩 chart 那一半，\
             判据照旧绿，而它想守的那一半已经没人读了"
        );
    }

    /// A face literal carries no surrounding space. A trailing space is how a
    /// rename lands halfway: the row looks owned to a human and unknown to the
    /// reading, which compares the stored label exactly.
    #[test]
    fn no_face_literal_is_padded() {
        for face in code_faces() {
            assert_eq!(
                face.value.trim(),
                face.value,
                "{}:{} 的面名两头带空白: {:?}",
                face.file,
                face.line,
                face.value
            );
        }
    }
}

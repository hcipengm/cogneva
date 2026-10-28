//! principle: feedback_no_hardcoding_config_surface
//! principle: feedback_a_whole_tree_census_reads_shapes_so_new_artifacts_must_declare_themselves
//! The chart's `values.yaml` is enumerated: every numeric leaf there has a row
//! saying whether a measurement moves it. The templates are not. A number typed
//! straight into a template is invisible to that registry -- it is not a leaf, so
//! no row is missing, and it renders into the deployed object exactly like a
//! value that a deployment can set. That is the shape this test closes: a
//! workload port, a retention count, a git timeout written twice.
//!
//! The census counts every number a template writes outside comments -- in the
//! document body and inside `{{ }}` actions alike -- and holds it to
//! `deploy/helm/cogneva/template-literals.tsv`, one row per (file, value) with an
//! occurrence count, so a new site in a file that already writes that number is a
//! count mismatch rather than an absence. An action is mostly a value being read,
//! but `randAlphaNum 32` is a length somebody typed, and a number a template
//! computes and a number somebody typed are the same number on the page.
//! Each row says which of two things the number is:
//!
//! - `fixed`: an identity that is not a dial -- a protocol port (443, 53), the
//!   UID an image runs as, a file mode, a shell exit status. The note names the
//!   agreement, because "fixed by agreement" and "nobody has looked at this" are
//!   the same number on the page.
//! - `dial`: a number a deployment may want to change, which the chart has not
//!   given a home in `values.yaml`. This is the debt, and the ceiling is zero:
//!   the ports, timeouts and replica counts that used to live here now read a
//!   `values.yaml` key, so a new `dial` row is a red test rather than a budget.
//!
//! What is deliberately not counted, because it is not a number anybody chose:
//! anything inside `{{/* ... */}}` or a `#` comment (prose), a width handed to
//! `indent`/`nindent` (punctuation), a number inside a quoted string in an action
//! (the content of a string the template compares or prints -- the substitution
//! table's patterns are exactly that, and the render fails loudly when one stops
//! matching), numbers carrying a unit or a version (e.g. `1000m`, `80Gi`,
//! `v0.5.10`, `apps/v1`), an address or a date (`10.0.0.5`, `2026-10-20`), a
//! shell file descriptor (`2>/dev/null`), a positional parameter (`$1`), and a
//! range (`8080-8081`). The register carries the judgement; the census only
//! insists that every number reaches it.

mod common;

use common::routes::read;

use common::routes::repo_root;

use std::collections::BTreeMap;
use std::path::Path;

/// The directory whose files the census reads.
const TEMPLATES: &str = "deploy/helm/cogneva/templates";

/// The register: one row per (file, value) pair the templates write.
const REGISTRY: &str = "deploy/helm/cogneva/template-literals.tsv";

/// Hardcoded dials still in the templates, counted as sites rather than rows so
/// that a number written eight times costs what it costs.
///
/// Zero: every dial the register once carried now reads from `values.yaml`, and
/// the register keeps only `fixed` rows. Raising this number is a decision to
/// ship a new hardcoded dial -- the thing this test exists to make visible -- so
/// it stays at zero and a new dial means a new `values.yaml` key, not a new row.
const DIAL_SITE_CEILING: u64 = 0;

/// Which of the two things a registered number is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// An identity that is not a dial: a protocol port, a UID, a file mode.
    Fixed,
    /// A number a deployment may want to change, with no home in `values.yaml`.
    Dial,
    /// A row the census wrote and nobody has judged yet.
    Unreviewed,
}

impl Kind {
    fn parse(word: &str) -> Option<Self> {
        match word {
            "fixed" => Some(Kind::Fixed),
            "dial" => Some(Kind::Dial),
            "unreviewed" => Some(Kind::Unreviewed),
            _ => None,
        }
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Fixed => "fixed",
            Kind::Dial => "dial",
            Kind::Unreviewed => "unreviewed",
        }
    }
}

/// One registered number: where it is written, what it is, and why.
#[derive(Debug, Clone)]
struct Row {
    file: String,
    value: String,
    sites: usize,
    kind: Kind,
    why: String,
}

/// The file names under [`TEMPLATES`], sorted so the census is reproducible.
fn template_files() -> Vec<String> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("a directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();
            let rel = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if entry.file_type().expect("a file type").is_dir() {
                walk(&path, &rel, out);
            } else {
                out.push(rel);
            }
        }
    }
    let mut out = Vec::new();
    walk(&repo_root().join(TEMPLATES), "", &mut out);
    out.sort();
    out
}

/// What the scanner carries from one line to the next: a `{{` action and a
/// `{{/*` comment both outlive the line they open on.
#[derive(Default)]
struct ScanState {
    in_action: bool,
    in_block_comment: bool,
}

fn starts_with(chars: &[char], at: usize, needle: &str) -> bool {
    needle
        .chars()
        .enumerate()
        .all(|(offset, c)| chars.get(at + offset) == Some(&c))
}

/// Everything on `line` a template writes literally: the `{{ }}` actions and the
/// comments removed, the rest kept.
///
/// Quotes are tracked so that a `#` inside a quoted value does not start a
/// comment; they do not span lines, which is true of every scalar in this chart.
fn strip_line(line: &str, state: &mut ScanState) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::new();
    let mut quote: Option<char> = None;
    let mut i = 0;
    while i < chars.len() {
        if state.in_action {
            if starts_with(&chars, i, "*/}}") {
                state.in_action = false;
                state.in_block_comment = false;
                i += 4;
                continue;
            }
            // A block comment that was opened is only closed by its own `*/}}`:
            // prose inside it may carry `}}` and it is still prose.
            if !state.in_block_comment && starts_with(&chars, i, "}}") {
                state.in_action = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if starts_with(&chars, i, "{{/*") {
            state.in_action = true;
            state.in_block_comment = true;
            i += 4;
            continue;
        }
        if starts_with(&chars, i, "{{") {
            state.in_action = true;
            i += 2;
            continue;
        }
        let c = chars[i];
        if let Some(open) = quote {
            out.push(c);
            if c == open {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == '"' || c == '\'' {
            quote = Some(c);
            out.push(c);
            i += 1;
            continue;
        }
        if c == '#' && (i == 0 || chars[i - 1] == ' ' || chars[i - 1] == '\t') {
            break;
        }
        // `$1` is a positional parameter, not a number the template chose.
        if c == '$' && chars.get(i + 1).is_some_and(char::is_ascii_digit) {
            i += 1;
            while chars.get(i).is_some_and(char::is_ascii_digit) {
                i += 1;
            }
            out.push(' ');
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// The numbers written literally in `text`, in the order they appear.
fn literals(text: &str) -> Vec<String> {
    let mut state = ScanState::default();
    let mut found = Vec::new();
    for line in text.lines() {
        let kept = strip_line(line, &mut state);
        let chars: Vec<char> = kept.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if !chars[i].is_ascii_digit() {
                i += 1;
                continue;
            }
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            // A digit run that touches a word, a dot, a slash, a dollar or a dash
            // on the left, or a word, a dot, a unit or a redirect on the right, is
            // part of something longer: a version, an address, a quantity, a
            // path segment, a file descriptor.
            let blocked_before = start.checked_sub(1).is_some_and(|at| {
                let c = chars[at];
                c.is_ascii_alphanumeric()
                    || c == '_'
                    || c == '.'
                    || c == '$'
                    || c == '/'
                    || c == '-'
            });
            let blocked_after = chars.get(i).is_some_and(|&c| {
                c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '>' || c == '-'
            });
            if !blocked_before && !blocked_after {
                found.push(chars[start..i].iter().collect());
            }
        }
    }
    found
}

/// The numbers written inside `{{ }}` actions, except the two shapes that are
/// not parameters at all.
///
/// An action is mostly a value being read, but not always: `randAlphaNum 32`
/// asks for a secret of a stated length, and a template that grew
/// `someHelper 500` would be a dial written where the census used to look away.
/// Two shapes are skipped because they are not a number anybody chose about the
/// deployment: a width handed to `indent`/`nindent` is punctuation, and a digit
/// inside a quoted string is part of a string the template compares or prints
/// (the substitution table's patterns are exactly that, and the render fails
/// loudly when one of them stops matching).
fn literals_in_actions(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut found = Vec::new();
    let mut in_action = false;
    let mut in_block_comment = false;
    let mut quote: Option<char> = None;
    let mut prev_word = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if !in_action {
            if starts_with(&chars, i, "{{/*") {
                in_action = true;
                in_block_comment = true;
                i += 4;
                continue;
            }
            if starts_with(&chars, i, "{{") {
                in_action = true;
                prev_word.clear();
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if starts_with(&chars, i, "*/}}") {
                in_action = false;
                in_block_comment = false;
                i += 4;
                continue;
            }
            i += 1;
            continue;
        }
        if starts_with(&chars, i, "*/}}") {
            in_action = false;
            in_block_comment = false;
            i += 4;
            continue;
        }
        // `{{- /*` opens the same comment one dash and a space later.
        if starts_with(&chars, i, "/*") {
            in_block_comment = true;
            i += 2;
            continue;
        }
        if starts_with(&chars, i, "}}") {
            in_action = false;
            prev_word.clear();
            i += 2;
            continue;
        }
        if let Some(open) = quote {
            if c == open {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == '"' || c == '\'' {
            quote = Some(c);
            prev_word.clear();
            i += 1;
            continue;
        }
        if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            let blocked_before = start.checked_sub(1).is_some_and(|at| {
                let c = chars[at];
                c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '$' || c == '/'
            });
            let blocked_after = chars
                .get(i)
                .is_some_and(|&c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
            let formatting = prev_word == "indent" || prev_word == "nindent";
            if !blocked_before && !blocked_after && !formatting {
                found.push(chars[start..i].iter().collect());
            }
            continue;
        }
        if c.is_ascii_alphanumeric() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            prev_word = chars[start..i].iter().collect();
            continue;
        }
        if !c.is_whitespace() {
            prev_word.clear();
        }
        i += 1;
    }
    found
}

/// How many times each (file, value) pair is written under [`TEMPLATES`].
fn census() -> BTreeMap<(String, String), usize> {
    let root = repo_root().join(TEMPLATES);
    let mut out = BTreeMap::new();
    for file in template_files() {
        let text = read(&root.join(&file));
        let written = literals(&text)
            .into_iter()
            .chain(literals_in_actions(&text));
        for value in written {
            *out.entry((file.clone(), value)).or_insert(0) += 1;
        }
    }
    out
}

fn parse_registry(text: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end();
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(
            fields.len(),
            5,
            "{REGISTRY}:{}: a row is file, value, sites, kind, why -- a tab inside \
             the reason would split it: {line:?}",
            number + 1
        );
        let sites: usize = fields[2].parse().unwrap_or_else(|e| {
            panic!(
                "{REGISTRY}:{}: {e}: the sites column counts occurrences",
                number + 1
            )
        });
        let kind = Kind::parse(fields[3]).unwrap_or_else(|| {
            panic!(
                "{REGISTRY}:{}: {:?} is not one of fixed, dial, unreviewed",
                number + 1,
                fields[3]
            )
        });
        rows.push(Row {
            file: fields[0].to_string(),
            value: fields[1].to_string(),
            sites,
            kind,
            why: fields[4].to_string(),
        });
    }
    rows
}

fn registry() -> Vec<Row> {
    parse_registry(&read(&repo_root().join(REGISTRY)))
}

#[test]
fn every_number_written_into_a_chart_template_is_registered_once() {
    let census = census();
    let rows = registry();

    let mut registered: BTreeMap<(String, String), (usize, usize)> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let key = (row.file.clone(), row.value.clone());
        let entry = registered.entry(key.clone()).or_insert((0, index));
        entry.0 += 1;
        if entry.0 > 1 {
            panic!(
                "{REGISTRY}: {} {} has more than one row",
                row.file, row.value
            );
        }
    }

    let mut missing = Vec::new();
    let mut unread = Vec::new();
    for (key, count) in &census {
        match registered.get(key) {
            None => missing.push(format!("{} {} ({count} sites)", key.0, key.1)),
            Some((_, index)) => {
                if rows[*index].sites != *count {
                    unread.push(format!(
                        "{} {}: register says {} sites, the template writes {count}",
                        key.0, key.1, rows[*index].sites
                    ));
                }
            }
        }
    }
    let mut unknown = Vec::new();
    for key in registered.keys() {
        if !census.contains_key(key) {
            unknown.push(format!("{} {}", key.0, key.1));
        }
    }

    assert!(
        missing.is_empty(),
        "these numbers are written into a chart template and no row says whether \
         a deployment should be able to change them -- add a row, or read the \
         number from values.yaml and let the literal go: {missing:#?}"
    );
    assert!(
        unread.is_empty(),
        "these rows no longer match what the template writes, so the count that \
         made the number visible stopped being a reading: {unread:#?}"
    );
    assert!(
        unknown.is_empty(),
        "these rows name numbers no template writes, so the register is \
         describing a chart that is not here: {unknown:#?}"
    );
}

#[test]
fn a_registered_number_says_which_kind_of_number_it_is() {
    let mut unjudged = Vec::new();
    for row in registry() {
        let where_ = format!("{} {}", row.file, row.value);
        if row.kind == Kind::Unreviewed {
            unjudged.push(where_);
            continue;
        }
        assert!(
            row.why.trim().len() >= 20,
            "{where_} ({}) has a reason too short to argue with: {:?} -- say what \
             agreement the number encodes, or what the fix is",
            row.kind.word(),
            row.why
        );
        if row.kind == Kind::Dial {
            assert!(
                row.why.contains("values.yaml"),
                "{where_} is registered as a dial, so the row has to name the home \
                 the number is going to: say which values.yaml key carries it and \
                 read it here"
            );
        }
    }
    assert!(
        unjudged.is_empty(),
        "these rows were written by the census and nobody has judged them: \
         {unjudged:#?}"
    );
}

#[test]
fn the_hardcoded_dials_do_not_grow() {
    let dial_sites: usize = registry()
        .iter()
        .filter(|row| row.kind == Kind::Dial)
        .map(|row| row.sites)
        .sum();
    assert!(
        dial_sites as u64 <= DIAL_SITE_CEILING,
        "{dial_sites} numbers are written straight into chart templates with no \
         home in values.yaml, and the ceiling is {DIAL_SITE_CEILING}. A deployment \
         cannot change any of them without editing the chart, and two files that \
         write the same number disagree the moment one moves. Move the number to \
         values.yaml and read it, or register why it is not a dial."
    );
}

#[test]
fn the_scanner_reads_actions_comments_and_values_the_way_the_chart_writes_them() {
    let sample = "\
# a comment with 4 numbers
kind: Service
  port: 8080
  image: registry.local/cogneva:v0.5.10
  cpu: 1000m
  api: apps/v1
  ip: 10.0.0.5
  seen: 2026-10-20
  range: 8080-8081
  fd: 2>/dev/null
  arg: $1
  quoted: \"note with # and 42\"
  action: {{ .Values.port | default 9999 }}
  {{- if and (gt 3 2) (eq .Values.mode \"5\") }}
  counted: 7
  {{- end }}
  {{/* a block comment 11
     that carries 12 on a prose line and }} inside it
  */}}
  after: 640
";
    // `42` is counted: a number inside a quoted value is still a number the
    // template wrote, and `value: "8080"` is exactly the shape that hides one.
    assert_eq!(literals(sample), vec!["8080", "42", "7", "640"]);

    // Inside an action, the number a template writes is the length it asks for
    // or the fallback it names -- both are typed, and neither can be set. The
    // width handed to `nindent` is punctuation, a digit inside a quoted string
    // is that string's content, and a block comment is prose.
    let actions = "\
  width: {{ .Values.x | nindent 4 }}
  entropy: {{ randAlphaNum 32 }}
  fallback: {{ .Values.port | default 9999 }}
  pattern: {{ printf \"postgres.%s:5432\" .Values.ns }}
  prose: {{/* the reading was 42 */}}
  {{- /* and 43 here */}}
";
    assert_eq!(literals_in_actions(actions), vec!["32", "9999"]);
}

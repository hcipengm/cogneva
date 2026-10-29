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
//! document body and inside `{{ }}` actions alike -- and every one of them has to
//! carry its own reason, on the line above where it is written:
//!
//! ```text
//! {{- /* lit: <value> fixed|dial|unreviewed — <why> */}}
//! ```
//!
//! The dash after `{{` and none before `}}` is the only spelling that leaves the
//! rendered manifests untouched: it eats the marker line's own indentation, while
//! the newline after it survives. `-}}` would eat that newline too and glue the
//! marker's line onto the line below it -- which parses as broken YAML, or worse,
//! is silently inside a block scalar and becomes part of a shell command.
//!
//! The reason used to live in a separate table keyed by (file, value), with an
//! occurrence count beside it. That table was a second copy of what the census
//! computes -- the file and the value it already had -- and the count was the
//! part that went stale every time a line moved. Worse, a judgement written one
//! file away does not travel with the number: the row could name a reason the
//! template no longer wrote, and the only reading of that was a red test that had
//! to be reconciled by hand. Beside the number, the two shapes left are the two
//! that mean something -- a number with no reason, and a reason for a number this
//! file does not write -- and the count is always the census's own.
//!
//! Each reason says which of two things the number is:
//!
//! - `fixed`: an identity that is not a dial -- a protocol port (443, 53), the
//!   UID an image runs as, a file mode, a shell exit status. The note names the
//!   agreement, because "fixed by agreement" and "nobody has looked at this" are
//!   the same number on the page.
//! - `dial`: a number a deployment may want to change, which the chart has not
//!   given a home in `values.yaml`. This is the debt, and the ceiling is zero:
//!   the ports, timeouts and replica counts that used to live here now read a
//!   `values.yaml` key, so a new `dial` reason is a red test rather than a budget.
//!
//! What is deliberately not counted, because it is not a number anybody chose:
//! anything inside `{{/* ... */}}` or a `#` comment (prose), a width handed to
//! `indent`/`nindent` (punctuation), a number inside a quoted string in an action
//! (the content of a string the template compares or prints -- the substitution
//! table's patterns are exactly that, and the render fails loudly when one stops
//! matching), numbers carrying a unit or a version (e.g. `1000m`, `80Gi`,
//! `v0.5.10`, `apps/v1`), an address or a date (`10.0.0.5`, `2026-10-20`), a
//! shell file descriptor (`2>/dev/null`), a positional parameter (`$1`), and a
//! range (`8080-8081`). The marker carries the judgement; the census only
//! insists that every number has one.

mod common;

use common::routes::{read, walk_all};

use common::routes::repo_root;
use common::rust_production::relative_name;

use std::collections::BTreeMap;

/// The directory whose files the census reads.
const TEMPLATES: &str = "deploy/helm/cogneva/templates";

/// The comment a reason is written in, above the number it is about.
///
/// A template comment (`{{/* ... */}}`) rather than a YAML `#` one: the chart's
/// rendered manifests are committed and diffed against a fresh render, and a
/// `#` comment is copied into them verbatim -- so the reason would show up as a
/// change to every profile's artifacts while saying nothing about the deployed
/// object. The template comment is stripped at render and reads the same here.
///
/// The marker stands on its own line above the number, in the `{{- /* ... */}}`
/// form: trim on the left only. The left dash removes the line's indentation,
/// the unterminated right side keeps the newline, so the line disappears from the
/// render instead of becoming a whitespace-only line. Trimming on the right as
/// well would join this line to the next one.
const MARKER: &str = "lit:";

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

/// One declared number: where it is written, what it is, and why.
#[derive(Debug, Clone)]
struct Row {
    file: String,
    value: String,
    kind: Kind,
    why: String,
}

/// The file names under [`TEMPLATES`], sorted so the census is reproducible.
fn template_files() -> Vec<String> {
    let root = repo_root().join(TEMPLATES);
    let mut paths = Vec::new();
    walk_all(&root, &mut paths);
    let mut out: Vec<String> = paths
        .iter()
        .map(|path| relative_name(&root, path))
        .collect();
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

/// How long a closing comment delimiter is at `at`, or `None`.
///
/// Helm lets a comment end as `*/}}`, `*/ -}}` or `*/-}}`, and the trimmed form
/// is the one that keeps the line out of the rendered manifests -- so a scanner
/// that only knows the first spelling reads every later line as prose and stops
/// counting the rest of the file. Only whitespace and one `-` are allowed between
/// the two halves; anything else is not a closing delimiter.
fn comment_close_len(chars: &[char], at: usize) -> Option<usize> {
    if !starts_with(chars, at, "*/") {
        return None;
    }
    let mut i = at + 2;
    while chars.get(i).is_some_and(|c| c.is_whitespace()) {
        i += 1;
    }
    if chars.get(i) == Some(&'-') {
        i += 1;
    }
    starts_with(chars, i, "}}").then(|| i + 2 - at)
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
            let close = if state.in_block_comment {
                comment_close_len(&chars, i)
            } else {
                None
            };
            if let Some(len) = close {
                state.in_action = false;
                state.in_block_comment = false;
                i += len;
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
            let close = comment_close_len(&chars, i);
            if let Some(len) = close {
                in_action = false;
                in_block_comment = false;
                i += len;
                continue;
            }
            i += 1;
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

/// The prose inside `{{/* ... */}}` when the whole line is that comment.
///
/// Both spellings are accepted -- `{{/*` and `{{- /*` with the trailing dash --
/// because the trimmed form is what keeps the line out of the rendered
/// manifests, and a marker that leaves a blank line behind would show up as an
/// artifact change in every profile.
fn comment_body(trimmed: &str) -> Option<&str> {
    let rest = trimmed.strip_prefix("{{")?;
    let rest = rest.strip_prefix('-').unwrap_or(rest).trim_start();
    let rest = rest.strip_prefix("/*")?;
    let rest = rest.strip_suffix("}}")?;
    let rest = rest.strip_suffix('-').unwrap_or(rest);
    rest.trim_end().strip_suffix("*/")
}

/// The reason lines in one template: `{{/* lit: <value> <kind> — <why> */}}`.
///
/// A marker has to stand on its own line: one opened inside a multi-line action
/// is inside that action's prose, and one written as a YAML `#` comment would
/// travel into the rendered manifests. [`strip_line`] tracks both states across
/// lines, which is why this scan runs it rather than looking at raw text.
fn parse_markers(file: &str, text: &str) -> (Vec<Row>, Vec<String>) {
    let mut rows = Vec::new();
    let mut problems = Vec::new();
    let mut state = ScanState::default();
    for (number, line) in text.lines().enumerate() {
        let inside_action = state.in_action;
        strip_line(line, &mut state);
        let where_ = format!("{file}:{}", number + 1);
        let trimmed = line.trim();
        let body = if let Some(body) =
            comment_body(trimmed).and_then(|b| b.trim_start().strip_prefix(MARKER))
        {
            body
        } else if trimmed.starts_with('#') && trimmed.contains(MARKER) {
            problems.push(format!(
                "{where_}: this reason is a YAML `#` comment, so it would be copied \
                 into the rendered manifests; write it as a template comment \
                 `{{{{- /* {MARKER} <value> <kind> — <why> */}}}}` instead"
            ));
            continue;
        } else {
            continue;
        };
        if inside_action {
            problems.push(format!(
                "{where_}: `{MARKER}` is written inside a multi-line `{{{{ }}}}` \
                 action, so it is part of that action's prose rather than a line of \
                 its own"
            ));
            continue;
        }
        let body = body.trim();
        let (head, why) = match body.split_once(" — ") {
            Some((head, why)) => (head, why.trim()),
            None => {
                problems.push(format!(
                    "{where_}: the reason is missing -- say what agreement the number \
                     encodes after ` — `, because a number with a name and no reason \
                     reads exactly like one nobody has looked at"
                ));
                continue;
            }
        };
        let mut parts = head.split_whitespace();
        let (Some(value), Some(word)) = (parts.next(), parts.next()) else {
            problems.push(format!(
                "{where_}: a reason line is `<value> <kind> — <why>`, got {head:?}"
            ));
            continue;
        };
        if parts.next().is_some() {
            problems.push(format!(
                "{where_}: {head:?} has more than a value and a kind before the ` — `"
            ));
            continue;
        }
        let Some(kind) = Kind::parse(word) else {
            problems.push(format!(
                "{where_}: {word:?} is not one of fixed, dial, unreviewed"
            ));
            continue;
        };
        if why.is_empty() {
            problems.push(format!("{where_}: the reason after ` — ` is empty"));
            continue;
        }
        rows.push(Row {
            file: file.to_string(),
            value: value.to_string(),
            kind,
            why: why.to_string(),
        });
    }
    (rows, problems)
}

fn registry() -> Vec<Row> {
    let root = repo_root().join(TEMPLATES);
    let mut rows = Vec::new();
    let mut problems = Vec::new();
    for file in template_files() {
        let (found, bad) = parse_markers(&file, &read(&root.join(&file)));
        rows.extend(found);
        problems.extend(bad);
    }
    assert!(
        problems.is_empty(),
        "these reason lines cannot be read as one: {problems:#?}"
    );
    rows
}

#[test]
fn every_number_written_into_a_chart_template_carries_a_reason() {
    let census = census();
    let rows = registry();

    let mut declared: BTreeMap<(String, String), usize> = BTreeMap::new();
    for row in &rows {
        let key = (row.file.clone(), row.value.clone());
        let seen = declared.entry(key).or_insert(0);
        *seen += 1;
    }

    let mut missing = Vec::new();
    for (key, count) in &census {
        if !declared.contains_key(key) {
            missing.push(format!("{} {} ({count} sites)", key.0, key.1));
        }
    }
    let mut doubled = Vec::new();
    for (key, seen) in &declared {
        if *seen > 1 {
            doubled.push(format!("{} {} ({} reasons)", key.0, key.1, seen));
        }
    }
    let mut unknown = Vec::new();
    for key in declared.keys() {
        if !census.contains_key(key) {
            unknown.push(format!("{} {}", key.0, key.1));
        }
    }

    assert!(
        missing.is_empty(),
        "these numbers are written into a chart template and nothing says whether \
         a deployment should be able to change them -- write `{{{{- /* lit: <value> \
         fixed|dial — <why> */}}}}` on the line above, or read the number from \
         values.yaml and let the literal go: {missing:#?}"
    );
    assert!(
        doubled.is_empty(),
        "these numbers carry more than one reason, so a reader cannot tell which \
         one is in force: {doubled:#?}"
    );
    assert!(
        unknown.is_empty(),
        "these reasons name numbers the template does not write, so they are \
         describing a chart that is not here -- a stale reason is worse than a \
         missing one, because it reads as a decision somebody made: {unknown:#?}"
    );
}

#[test]
fn a_declared_number_says_which_kind_of_number_it_is() {
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
                "{where_} is declared as a dial, so the reason has to name the home \
                 the number is going to: say which values.yaml key carries it and \
                 read it here"
            );
        }
    }
    assert!(
        unjudged.is_empty(),
        "these numbers are declared but nobody has judged them: {unjudged:#?}"
    );
}

#[test]
fn the_hardcoded_dials_do_not_grow() {
    let dial: Vec<(String, String)> = registry()
        .into_iter()
        .filter(|row| row.kind == Kind::Dial)
        .map(|row| (row.file, row.value))
        .collect();
    // The sites come from the census, not from the reason: a number written eight
    // times costs what it costs, and nobody has to keep a count in step.
    let dial_sites: usize = census()
        .into_iter()
        .filter(|(key, _)| dial.contains(key))
        .map(|(_, count)| count)
        .sum();
    assert!(
        dial_sites as u64 <= DIAL_SITE_CEILING,
        "{dial_sites} numbers are written straight into chart templates with no \
         home in values.yaml, and the ceiling is {DIAL_SITE_CEILING}. A deployment \
         cannot change any of them without editing the chart, and two files that \
         write the same number disagree the moment one moves. Move the number to \
         values.yaml and read it, or say why it is not a dial."
    );
}

#[test]
fn a_reason_line_that_cannot_be_read_is_not_silently_a_missing_reason() {
    let (rows, problems) = parse_markers(
        "sample.yaml",
        "\
kind: Service
{{- /* lit: 443 fixed — https itself, the port the protocol defines and nobody renames */}}
  port: 443
{{/* lit: 53 — dns itself, the port the protocol defines and nobody renames */}}
  dns: 53
{{/* lit: 8080 hardcoded — a port somebody typed, with a word that is not a kind */}}
  other: 8080
{{/* lit: 9090 fixed */}}
  none: 9090
  # lit: 1 fixed — a YAML comment would be copied into the rendered manifests
{{/* a block comment
{{/* lit: 2 fixed — a marker inside another action is that action's prose */}}
   that carries }} on a prose line */}}
",
    );
    assert_eq!(rows.len(), 1, "{rows:#?}");
    assert_eq!(rows[0].value, "443");
    let matched = |needle: &str| problems.iter().filter(|p| p.contains(needle)).count();
    assert_eq!(matched("a reason line is"), 1, "{problems:#?}");
    assert_eq!(matched("not one of fixed"), 1, "{problems:#?}");
    assert_eq!(matched("reason is missing"), 1, "{problems:#?}");
    assert_eq!(matched("YAML `#` comment"), 1, "{problems:#?}");
    assert_eq!(matched("multi-line `{{ }}` action"), 1, "{problems:#?}");
    assert_eq!(problems.len(), 5, "{problems:#?}");
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
  {{- /* a trimmed comment with 13 */ -}}
  trimmed: 641
";
    // `42` is counted: a number inside a quoted value is still a number the
    // template wrote, and `value: "8080"` is exactly the shape that hides one.
    // `641` is what proves the trimmed close was read: a scanner that only knows
    // `*/}}` never leaves the comment above it and stops counting the file.
    assert_eq!(literals(sample), vec!["8080", "42", "7", "640", "641"]);

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
  {{- /* and 44 here */ -}}
  entropy: {{ randAlphaNum 64 }}
";
    assert_eq!(literals_in_actions(actions), vec!["32", "9999", "64"]);
}

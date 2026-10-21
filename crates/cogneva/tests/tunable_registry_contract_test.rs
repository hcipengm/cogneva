//! principle: feedback_a_whole_tree_census_reads_shapes_so_new_artifacts_must_declare_themselves
//! Every number a deployment ships is one of three things: something a
//! measurement sets, something a deployment names, or something that is not a
//! dial at all. Only the first moves when the world it reads moves -- and the
//! other two look exactly like it, because a number a loop computed and a number
//! somebody typed are the same number on the page.
//!
//! The registry (`deploy/helm/cogneva/tunables.yaml`) is where that judgement is
//! written down, one row per numeric leaf of the chart. This test holds it to the
//! chart and to the code in four directions:
//!
//! - every numeric leaf has exactly one row, and every row names a numeric leaf.
//!   The set is read from `values.yaml` on every run, so a value that is added
//!   without a row is a hole this test names rather than an absence nobody
//!   notices -- "nobody has looked at this yet" has to be a missing row, not a
//!   silently unjudged number.
//! - every row says something. For `override`/`fixed` that is the reason the
//!   value is not derived; for `derived` it is what the value reads.
//! - a `derived` row names the reading its number lands on and a test that shows
//!   the number following its input. A comment claiming a value tracks the world
//!   is not evidence that it does; the test is the part of the claim a machine
//!   can refuse.
//! - a row that lands on a reading uses a word the reading can carry, and the
//!   file it names publishes that metric and that label. A value registered as
//!   derived while its publisher never says so reads as a number that tracks the
//!   world and is not one -- the failure this is here to catch, since a cap that
//!   stopped tracking its volume is a steady number either way.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use cog_core::build_cache_readings::{
    BuildCacheReadings, CapSource, BUILD_TARGET_CAP_METRIC, SOURCE_LABEL,
};
use cog_core::Observable;
use serde::Deserialize;

/// The registry: one row per numeric leaf of the chart.
const REGISTRY: &str = "deploy/helm/cogneva/tunables.yaml";

/// The chart whose numeric leaves the registry has to cover.
const VALUES: &str = "deploy/helm/cogneva/values.yaml";

/// The metric identifier the `source`-labelled cache readings are published
/// under. Rows that name it are the ones a bounded cache can be checked against.
const CAP_METRIC: &str = "BUILD_TARGET_CAP_METRIC";

/// Where a value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    /// A measurement loop sets it, so it moves when the input it reads moves.
    Derived,
    /// The deployment names it, so it stays where it was pinned.
    Override,
    /// Not a dial: a protocol number or a practice, which no measurement should
    /// move. Registered so that "fixed by agreement" and "nobody has derived
    /// this yet" are told apart by the row rather than by whoever reads it next.
    Fixed,
}

impl Source {
    /// The word this source is published under, or `None` for a value that
    /// reaches no reading.
    ///
    /// These are the label's words, not this enum's names: the domain of the
    /// label is the subset of these that can appear on a reading, and a value
    /// that lands on none never says anything.
    fn as_label(self) -> Option<&'static str> {
        match self {
            Source::Derived => Some("derived"),
            Source::Override => Some("override"),
            Source::Fixed => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    version: u32,
    rows: Vec<Row>,
}

/// One dial this repository ships.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    /// The chart key a deployment sets, spelled as a dotted path into
    /// `values.yaml`.
    key: String,
    /// Where the value comes from today.
    source: Source,
    /// Why it is not derived. A derived row says what input it reads instead.
    why: String,
    /// The publisher and the reading, for a value whose number reaches one.
    #[serde(default)]
    published: Option<Published>,
    /// For a derived row: the test that shows the value following its input.
    #[serde(default)]
    proof: Option<Proof>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Published {
    /// The file that builds the value for this key.
    owner: String,
    /// The file that publishes the reading.
    file: String,
    /// The metric constant the reading is published under.
    metric: String,
    /// The label on that reading that carries where the value came from.
    label: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proof {
    file: String,
    test: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn registry() -> Registry {
    let text = read(&repo_root().join(REGISTRY));
    let registry: Registry = serde_yaml::from_str(&text)
        .unwrap_or_else(|e| panic!("{REGISTRY} does not parse as a registry: {e}"));
    assert_eq!(
        registry.version, 1,
        "{REGISTRY}: this test reads version 1, and the version is here so that a \
         change to the row shape is a version bump rather than a silent reinterpretation"
    );
    registry
}

/// `text` without its line comments.
///
/// A call is being searched for, and a file that mentions one in prose without
/// making it would answer for code it does not have, so what is searched is the
/// code. Cutting at the first `//` on a line is enough here: what it gets wrong
/// is a `//` inside a string literal, which would have to be a URL for a call to
/// hide behind it.
fn without_line_comments(text: &str) -> String {
    text.lines()
        .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Whether the code in `file` says the cap it publishes came from a loop.
fn says_the_cap_is_derived(file: &str) -> bool {
    without_line_comments(&read(&repo_root().join(file)))
        .contains(".with_cap_source(CapSource::Derived)")
}

/// Whether `text` holds `fn <name>(` and the attribute immediately above it is a
/// test attribute. A function doesn't prove anything unless something runs it.
fn has_test(text: &str, name: &str) -> bool {
    let needle = format!("fn {name}(");
    let Some(at) = text.find(&needle) else {
        return false;
    };
    let above = &text[..at];
    above
        .rsplit_once('\n')
        .map(|(_, line)| line.trim())
        .is_some_and(|line| line == "#[test]" || line == "#[tokio::test]")
        || above
            .rsplit('\n')
            .take(2)
            .any(|line| line.trim() == "#[test]" || line.trim() == "#[tokio::test]")
}

/// Whether `text` is a number the chart would read as a value: `123`, `0.5`,
/// `500m`, `80Gi`, with the decimal and binary suffixes Kubernetes accepts.
fn is_quantity(text: &str) -> bool {
    let text = text.trim();
    let text = text.strip_prefix('-').unwrap_or(text);
    const SUFFIXES: [&str; 11] = ["Ki", "Mi", "Gi", "Ti", "Pi", "k", "M", "G", "T", "P", "m"];
    let core = SUFFIXES
        .iter()
        .find_map(|suffix| text.strip_suffix(suffix))
        .unwrap_or(text);
    let mut parts = core.split('.');
    let Some(whole) = parts.next() else {
        return false;
    };
    let fraction = parts.next();
    if parts.next().is_some() {
        return false;
    }
    let digits = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
    digits(whole) && fraction.is_none_or(digits)
}

/// Collect the dotted path of every numeric leaf under `value`.
fn numeric_leaves(value: &serde_yaml::Value, path: &mut Vec<String>, out: &mut BTreeSet<String>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                let key = match key {
                    serde_yaml::Value::String(s) => s.clone(),
                    other => format!("{other:?}"),
                };
                path.push(key);
                numeric_leaves(child, path, out);
                path.pop();
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for (index, child) in items.iter().enumerate() {
                path.push(index.to_string());
                numeric_leaves(child, path, out);
                path.pop();
            }
        }
        serde_yaml::Value::Number(_) => {
            out.insert(path.join("."));
        }
        // `auto` is a number written as a word: a cap the walks derive is still
        // a number the deployment ships, and a value that dropped out of the
        // numeric set would read as prose rather than as a dial.
        serde_yaml::Value::String(s) if is_quantity(s) || s.trim().eq_ignore_ascii_case("auto") => {
            out.insert(path.join("."));
        }
        _ => {}
    }
}

#[test]
fn every_numeric_leaf_of_the_chart_has_exactly_one_row() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read(&repo_root().join(VALUES)))
        .unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    let mut leaves = BTreeSet::new();
    numeric_leaves(&values, &mut Vec::new(), &mut leaves);

    let registry = registry();
    let mut rows: BTreeMap<&str, usize> = BTreeMap::new();
    for row in &registry.rows {
        *rows.entry(row.key.as_str()).or_default() += 1;
    }

    let mut missing = Vec::new();
    for leaf in &leaves {
        if !rows.contains_key(leaf.as_str()) {
            missing.push(leaf.as_str());
        }
    }
    let mut unknown = Vec::new();
    for (key, count) in &rows {
        if *count > 1 {
            unknown.push(format!("`{key}` has {count} rows"));
        } else if !leaves.contains(*key) {
            unknown.push(format!("`{key}` is not a numeric leaf of {VALUES}"));
        }
    }

    assert!(
        missing.is_empty(),
        "these chart values have no row, so nobody has said whether a measurement \
         should move them: {missing:#?}"
    );
    assert!(
        unknown.is_empty(),
        "these rows describe something the chart does not ship as a number: \
         {unknown:#?}"
    );
}

/// The registry is only as wide as the file it enumerates: it reads
/// `values.yaml`. A container's `resources:` block written straight into a
/// template is the same dial with no row anywhere, and no reading built on the
/// registry can see it -- which is how a memory limit below the data the store
/// already held reached the cluster without any account being wrong.
///
/// The check is scoped by shape, because other things called `resources:` are
/// legitimate literals: a PersistentVolumeClaim's `resources:` holds `storage`,
/// and a Role's holds an inline list. A block that carries `cpu:` or `memory:`
/// is a container bound, and that block has to come from values.
#[test]
fn container_resource_bounds_in_templates_come_from_values() {
    let dir = repo_root().join("deploy/helm/cogneva/templates");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("template directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
        .collect();
    entries.sort();

    let mut literals = Vec::new();
    for path in entries {
        let text = read(&path);
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim() != "resources:" {
                continue;
            }
            let indent = line.len() - line.trim_start().len();
            let mut block = Vec::new();
            let mut next = index + 1;
            while next < lines.len() {
                let child = lines[next];
                if child.trim().is_empty() {
                    next += 1;
                    continue;
                }
                if child.len() - child.trim_start().len() <= indent {
                    break;
                }
                block.push(child.trim());
                next += 1;
            }
            let carries_bounds = block
                .iter()
                .any(|child| child.starts_with("cpu:") || child.starts_with("memory:"));
            if !carries_bounds {
                continue;
            }
            let by_reference = block
                .iter()
                .any(|child| child.contains("toYaml") || child.contains("include"));
            if !by_reference {
                let shown = path
                    .strip_prefix(repo_root())
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|_| path.clone());
                literals.push(format!("{}:{}", shown.display(), index + 1));
            }
        }
    }

    assert!(
        literals.is_empty(),
        "these container resource bounds are written into the template, so they \
         are dials with no row in {REGISTRY} and no reading can see them: {literals:#?}"
    );
}

#[test]
fn every_row_says_why_it_is_not_derived() {
    let mut silent = Vec::new();
    for row in &registry().rows {
        if row.why.trim().is_empty() {
            silent.push(format!("`{}` ({:?})", row.key, row.source));
        }
    }
    assert!(
        silent.is_empty(),
        "these values are registered without a reason, which leaves a reader \
         unable to tell a value fixed by agreement from one nobody has derived \
         yet: {silent:#?}"
    );
}

#[test]
fn a_derived_row_names_the_test_that_moves_its_input() {
    let mut wrong = Vec::new();
    for row in &registry().rows {
        if row.source != Source::Derived {
            continue;
        }
        if row.published.is_none() {
            wrong.push(format!(
                "`{}` is registered as derived and names no reading, so a reader \
                 cannot see where the number went",
                row.key
            ));
        }
        let Some(proof) = &row.proof else {
            wrong.push(format!(
                "`{}` is registered as derived and names no test, so nothing shows \
                 the value following the input it claims to read",
                row.key
            ));
            continue;
        };
        let text = read(&repo_root().join(&proof.file));
        if !has_test(&text, &proof.test) {
            wrong.push(format!(
                "`{}` is registered as derived and its proof `{}` is not a test in \
                 `{}`",
                row.key, proof.test, proof.file
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "a value registered as derived without a test that moves its input is a \
         constant with a comment: {wrong:#?}"
    );
}

#[test]
fn a_value_that_lands_on_a_reading_has_a_word_the_reading_can_carry() {
    let registry = registry();
    let mut unnameable = Vec::new();
    for row in &registry.rows {
        if row.published.is_some() && row.source.as_label().is_none() {
            unnameable.push(row.key.as_str());
        }
    }
    assert!(
        unnameable.is_empty(),
        "these values are registered as landing on a reading, and the source \
         they are registered with is one the reading's label cannot take, so the \
         reading would have to say something else about where the number came \
         from: {unnameable:#?}"
    );
}

#[test]
fn a_published_row_names_a_file_that_publishes_its_metric_and_label() {
    let mut wrong = Vec::new();
    for row in &registry().rows {
        let Some(published) = &row.published else {
            continue;
        };
        let text = without_line_comments(&read(&repo_root().join(&published.file)));
        if !text.contains(&published.metric) {
            wrong.push(format!(
                "`{}` lands on `{}`, and `{}` never mentions it",
                row.key, published.metric, published.file
            ));
        }
        let quoted = format!("\"{}\"", published.label);
        let constant = format!("{}_LABEL", published.label.to_uppercase());
        if !text.contains(&quoted) && !text.contains(&constant) {
            wrong.push(format!(
                "`{}` lands on a reading labelled `{}`, and `{}` carries neither \
                 the word nor `{constant}`",
                row.key, published.label, published.file
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "a row naming a reading that does not publish it is a claim about a \
         number nobody can look up: {wrong:#?}"
    );
}

/// The value at a dotted path, if the chart has one.
fn leaf_at<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    let mut current = value;
    for part in key.split('.') {
        current = match current {
            serde_yaml::Value::Mapping(map) => {
                map.get(serde_yaml::Value::String(part.to_string()))?
            }
            serde_yaml::Value::Sequence(items) => items.get(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

#[test]
fn a_cap_rows_owner_does_not_publish_it_as_derived_unless_it_says_so() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read(&repo_root().join(VALUES)))
        .unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    let mut wrong = Vec::new();
    for row in &registry().rows {
        let Some(published) = &row.published else {
            continue;
        };
        if published.metric != CAP_METRIC {
            continue;
        }
        let owner = without_line_comments(&read(&repo_root().join(&published.owner)));
        if !owner.contains(".with_cap(") {
            wrong.push(format!(
                "`{}` names `{}` as the file that builds the cap, and that file \
                 never sets one",
                row.key, published.owner
            ));
        }
        // Which of the two the reading says is decided by what the chart
        // ships: `auto` makes the publisher derive the cap and say so, a number
        // makes it publish the number. The row has to describe the reading the
        // shipped chart actually produces, and a publisher that cannot read
        // `auto` would ignore the sentinel in silence.
        let ships_auto = leaf_at(&values, &row.key)
            .is_some_and(|v| matches!(v, serde_yaml::Value::String(s) if s.trim() == "auto"));
        if ships_auto && !says_the_cap_is_derived(&published.owner) {
            wrong.push(format!(
                "`{}` ships `auto`, and `{}` has no branch that derives a cap, so \
                 the sentinel would be read as a value nobody configured",
                row.key, published.owner
            ));
        }
        if ships_auto != (row.source == Source::Derived) {
            wrong.push(format!(
                "`{}` is registered as {:?}, and the chart ships {}, so the reading \
                 would carry the other word",
                row.key,
                row.source,
                if ships_auto { "`auto`" } else { "a number" }
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "a value registered as one thing and published as another reads as \
         derived to everyone who only sees the reading: {wrong:#?}"
    );
}

#[test]
fn the_registrys_words_are_the_labels_words() {
    assert_eq!(
        Source::Derived.as_label(),
        Some(CapSource::Derived.as_str())
    );
    assert_eq!(
        Source::Override.as_label(),
        Some(CapSource::Override.as_str())
    );
}

#[tokio::test]
async fn every_reading_a_tunable_lands_on_says_where_its_value_came_from() {
    // A bounded cache, which is the state the cap family describes: a cache with
    // no cap publishes no cap reading at all, so the reading a row names is only
    // there to be checked while one is in force.
    let dir = tempfile::tempdir().unwrap();
    let bounded = || BuildCacheReadings::new(dir.path()).with_cap(4096, 300);
    let words = [CapSource::Derived.as_str(), CapSource::Override.as_str()];

    let mut wrong = Vec::new();

    // The label has to be able to say both words, or a row registering one the
    // reading cannot take would pass the structural check and mean nothing.
    for (built, expected) in [
        (bounded(), CapSource::Override),
        (
            bounded().with_cap_source(CapSource::Derived),
            CapSource::Derived,
        ),
    ] {
        let metrics = built.collect_metrics("").await.unwrap();
        let Some(cap) = metrics.iter().find(|m| m.name == BUILD_TARGET_CAP_METRIC) else {
            wrong.push(format!(
                "a bounded cache publishes no `{BUILD_TARGET_CAP_METRIC}` reading, so \
                 nothing says which cache a cap belongs to or where it came from"
            ));
            continue;
        };
        match cap.labels.get(SOURCE_LABEL) {
            Some(value) if value == expected.as_str() => {}
            Some(value) => wrong.push(format!(
                "a cap published from {expected:?} reads `{SOURCE_LABEL}=\"{value}\"`, \
                 outside the words this registry registers {words:?}"
            )),
            None => wrong.push(format!(
                "the cap reading carries no `{SOURCE_LABEL}` label, so a number that \
                 stopped tracking the volume it sits on reads like one that never \
                 tracked anything"
            )),
        }
    }

    // And the reading each cap row names is one of those. A row that lands on a
    // reading this test cannot build is not checked here; the file-level test
    // above is what holds those.
    let mut checked = 0;
    let metrics = bounded().collect_metrics("").await.unwrap();
    for row in &registry().rows {
        let Some(published) = &row.published else {
            continue;
        };
        if published.metric != CAP_METRIC {
            continue;
        }
        if !metrics.iter().any(|m| m.name == BUILD_TARGET_CAP_METRIC) {
            wrong.push(format!(
                "`{}` lands on `{}`, which a bounded cache does not publish",
                row.key, published.metric
            ));
            continue;
        }
        checked += 1;
    }

    assert!(wrong.is_empty(), "{wrong:#?}");
    assert!(
        checked > 0,
        "no row lands on a published reading, so this test checked nothing"
    );
}

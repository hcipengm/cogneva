//! principle: feedback_a_whole_tree_census_reads_shapes_so_new_artifacts_must_declare_themselves
//! principle: feedback_a_chart_value_without_a_registry_row_stops_every_later_rev_not_just_its_own
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

/// Ports a script outside the chart names, and where the chart keeps the same
/// number.
///
/// `restore-from-package.sh` builds a Job of its own rather than rendering the
/// chart, so it carries the data-plane ports a second time. A deployment that
/// moves postgres's port leaves this path dialling the old one, and the failure
/// it gets -- connection refused -- reads the same as a broken backup package.
/// The script takes the port from an env var with the chart's value as the
/// default; this test reads both so the default cannot drift away from the
/// chart while every other gate stays green.
#[test]
fn a_script_outside_the_chart_defaults_to_the_charts_port() {
    const SCRIPT: &str = "deploy/scripts/restore-from-package.sh";
    let values: serde_yaml::Value = serde_yaml::from_str(&read(&repo_root().join(VALUES)))
        .unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    let script = read(&repo_root().join(SCRIPT));

    // The env var the script reads, and the chart leaf holding the same port.
    let pairs = [
        ("COGNEVA_PG_PORT", "backends.postgres.port"),
        ("COGNEVA_QDRANT_PORT", "backends.qdrant.port"),
    ];
    let mut wrong = Vec::new();
    for (var, key) in pairs {
        let Some(expected) = leaf_at(&values, key).and_then(serde_yaml::Value::as_u64) else {
            wrong.push(format!("{key} is not a numeric leaf of {VALUES}"));
            continue;
        };
        // `VAR="${VAR:-5432}"` in any formatting: the default is what has to
        // agree, and the script is free to write the assignment its own way.
        let marker = format!("{var}:-");
        let default = script.lines().find_map(|line| {
            let rest = line.split(marker.as_str()).nth(1)?;
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse::<u64>().ok()
        });
        match default {
            Some(port) if port == expected => {}
            Some(port) => wrong.push(format!(
                "{SCRIPT}: {var} defaults to {port} while {key} is {expected} -- a \
                 deployment that moves the port would restore through the old one"
            )),
            None => wrong.push(format!(
                "{SCRIPT}: {var} no longer reads a `${{VAR:-<port>}}` default, so \
                 nothing here can tell whether it still points at {key} ({expected})"
            )),
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The three places a listened-on port has to agree: the process that binds it,
/// the chart that opens it, and the static manifests that mirror the chart.
const GATEWAY_CODE: &str = "crates/cog-gateway/src/security_gateway.rs";
const GATEWAY_TEMPLATE: &str = "deploy/helm/cogneva/templates/security-gateway.yaml";
const GATEWAY_K3S: &str = "deploy/k3s/gateway-deployment.yaml";

/// The value written on the line after `- name: <env>` in `text`, if the entry
/// reads a chart key. `None` means the entry is absent or writes a literal.
fn env_value_line<'a>(text: &'a str, env: &str) -> Option<&'a str> {
    let needle = format!("- name: {env}");
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() != needle {
            continue;
        }
        // `value:`, `valueFrom:`, or a comment above the value: the entry is a
        // handful of lines at most, and reading further would let a neighbouring
        // entry answer for this one.
        return lines
            .by_ref()
            .take(3)
            .find(|candidate| candidate.trim_start().starts_with("value:"));
    }
    None
}

/// The dotted `values.yaml` path an env entry reads, e.g. `.Values.gateway.httpPort`.
fn values_path(value_line: &str) -> Option<String> {
    let rest = value_line.split(".Values.").nth(1)?;
    let path: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
        .collect();
    let path = path.trim_end_matches('.').to_string();
    (!path.is_empty()).then_some(path)
}

/// A port the process listens on is opened by the chart and bound by the
/// process, and those are two different numbers in two different places: the
/// container port, the Service and the network policy are chart facts, while the
/// binding is whatever the process is told. Moving the chart side alone leaves
/// every connection refused by a gateway that is running perfectly -- the
/// failure reads as "the gateway is down", and nothing on the page says the
/// process never heard about the move.
///
/// So: every port the security gateway can be told about has to be told about,
/// from a `values.yaml` key -- a literal in the template would just move the
/// disagreement inside the chart -- and the static manifest has to carry the
/// same number, because it is the parity baseline.
#[test]
fn a_port_the_gateway_listens_on_is_told_to_the_process_and_not_only_opened() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read(&repo_root().join(VALUES)))
        .unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    let code = read(&repo_root().join(GATEWAY_CODE));
    let template = read(&repo_root().join(GATEWAY_TEMPLATE));
    let k3s = read(&repo_root().join(GATEWAY_K3S));

    // Every port the process reads from the environment, with the default it
    // falls back to when the chart says nothing.
    let mut wrong = Vec::new();
    let mut seen = 0;
    for line in code.lines() {
        let Some(rest) = line.split("env_u16(\"").nth(1) else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !name.starts_with("COGNEVA_") {
            continue;
        }
        seen += 1;
        let Some(value_line) = env_value_line(&template, &name) else {
            wrong.push(format!(
                "{GATEWAY_TEMPLATE} never passes {name}: the process binds its code \
                 default, so a values key that moves this port moves the Service and \
                 the policy and leaves the listener behind"
            ));
            continue;
        };
        let Some(key) = values_path(value_line) else {
            wrong.push(format!(
                "{GATEWAY_TEMPLATE}: {name} is passed as the literal `{}` rather than \
                 read from {VALUES} -- the chart would then disagree with itself, \
                 opening the container port from a key and binding a different number",
                value_line.trim()
            ));
            continue;
        };
        let Some(port) = leaf_at(&values, &key).and_then(serde_yaml::Value::as_u64) else {
            wrong.push(format!(
                "{GATEWAY_TEMPLATE}: {name} reads `{key}`, which is not a numeric leaf \
                 of {VALUES}"
            ));
            continue;
        };
        match env_value_line(&k3s, &name) {
            Some(line) if line.trim() == format!("value: \"{port}\"") => {}
            Some(line) => wrong.push(format!(
                "{GATEWAY_K3S}: {name} is `{}` while {key} is {port} -- the parity \
                 baseline would deploy a gateway listening somewhere else",
                line.trim()
            )),
            None => wrong.push(format!(
                "{GATEWAY_K3S} never passes {name}, so the static manifests bind \
                 whatever the image defaults to while the chart binds {key}"
            )),
        }
    }

    assert!(
        seen > 0,
        "{GATEWAY_CODE} no longer reads a port from the environment, so this test \
         stopped reading anything -- check that the ports are still env-driven \
         before deleting it"
    );
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// An endpoint the process cannot run without, where it is read, and the values
/// leaf the chart has to fill it from.
///
/// These are the envs whose absence is now an error rather than a guess: the code
/// stopped carrying a default address, because a wrong guess and "nothing there"
/// answer identically on every surface that reads them. That makes the chart the
/// only thing that can turn them on -- delete an entry from the ConfigMap and the
/// board reports `not_configured` in production while every gate stays green.
const REQUIRED_ENDPOINTS: &[(&str, &str, &str)] = &[
    (
        "crates/cog-gateway/src/readings.rs",
        "COGNEVA_PROMETHEUS_URL",
        "observability.infraWatch.prometheusUrl",
    ),
    (
        "crates/cog-gateway/src/contribution_admin.rs",
        "COGNEVA_SECURITY_GATEWAY_BASE",
        "securityGateway.llmProxyPort",
    ),
];

/// The env ConfigMap the workloads take through `envFrom`.
const ENV_CONFIGMAP: &str = "deploy/helm/cogneva/templates/configmap.yaml";

/// The static copy of that ConfigMap, which is the parity baseline.
const ENV_CONFIGMAP_K3S: &str = "deploy/k3s/configmap.yaml";

/// The value written for `env` in a ConfigMap whose entries are `KEY: value`.
fn configmap_value<'a>(text: &'a str, env: &str) -> Option<&'a str> {
    let needle = format!("{env}:");
    text.lines()
        .find(|line| line.trim_start().starts_with(&needle))
        .map(str::trim)
}

/// An endpoint with no default in the code has to be declared by the chart, from
/// a values leaf, and the static baseline has to carry the same address: the two
/// paths are the same deployment fact, and only one of them is rendered.
#[test]
fn an_endpoint_the_code_requires_is_filled_by_the_chart() {
    let values: serde_yaml::Value = serde_yaml::from_str(&read(&repo_root().join(VALUES)))
        .unwrap_or_else(|e| panic!("{VALUES} does not parse: {e}"));
    let template = read(&repo_root().join(ENV_CONFIGMAP));
    let k3s = read(&repo_root().join(ENV_CONFIGMAP_K3S));

    let mut wrong = Vec::new();
    for (code_path, env, leaf) in REQUIRED_ENDPOINTS {
        let code = read(&repo_root().join(code_path));
        if !code.contains(&format!("env::var(\"{env}\")")) {
            wrong.push(format!(
                "{code_path} no longer reads {env}, so this row is stale -- either the \
                 endpoint is declared somewhere else now (point this table at it) or the \
                 requirement went away (delete the row)"
            ));
            continue;
        }
        let Some(value_line) = configmap_value(&template, env) else {
            wrong.push(format!(
                "{ENV_CONFIGMAP} never passes {env}: {code_path} treats its absence as an \
                 error, so every Pod with this ConfigMap answers `not_configured`"
            ));
            continue;
        };
        if !value_line.contains(&format!(".Values.{leaf}")) {
            wrong.push(format!(
                "{ENV_CONFIGMAP}: {env} is `{value_line}` rather than a read of \
                 `{leaf}` -- moving the values key would then move one reader and not \
                 the other"
            ));
            continue;
        }
        let Some(declared) = leaf_at(&values, leaf) else {
            wrong.push(format!(
                "{VALUES} has no `{leaf}`, which {ENV_CONFIGMAP} reads for {env}"
            ));
            continue;
        };
        let Some(static_value) = configmap_value(&k3s, env) else {
            wrong.push(format!(
                "{ENV_CONFIGMAP_K3S} never passes {env}, so the static path leaves the \
                 process with no endpoint at all -- it is the baseline parity compares, \
                 not a manifest that can skip the key"
            ));
            continue;
        };
        // The static file carries the value, not an expression: it has to spell out
        // what the chart computes from the leaf, and a mismatch here is exactly the
        // drift parity cannot see (it compares the chart to this file).
        let expected = match declared {
            serde_yaml::Value::String(url) => url.clone(),
            serde_yaml::Value::Number(port) => format!(":{port}"),
            other => {
                wrong.push(format!(
                    "{VALUES}: `{leaf}` is {other:?}, which this test does not know how \
                     to compare with the static manifest"
                ));
                continue;
            }
        };
        let trimmed = static_value
            .trim_start_matches(&format!("{env}:"))
            .trim()
            .trim_matches('"');
        let agrees = match declared {
            serde_yaml::Value::String(_) => trimmed == expected,
            _ => trimmed.ends_with(&expected),
        };
        if !agrees {
            wrong.push(format!(
                "{ENV_CONFIGMAP_K3S}: {env} is `{trimmed}` while {VALUES} gives `{expected}` \
                 for `{leaf}` -- the static path would point the process somewhere the \
                 chart does not"
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The profile values files: what each deployment形态 changes relative to the
/// chart's defaults.
const PROFILES: &str = "deploy/helm/cogneva/profiles";

/// Where a values leaf may be read from when no template names it: the scripts
/// that project the chart's numbers (the resource governance projection reads
/// `resourceGovernance.clusterNodes`, and no manifest can).
const DEPLOY_SCRIPTS: &str = "deploy/scripts";

/// Every dotted path a template reads through `.Values.`.
fn paths_the_templates_read() -> BTreeSet<String> {
    fn walk(dir: &Path, out: &mut BTreeSet<String>) {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("a template directory entry");
            let path = entry.path();
            if entry.file_type().expect("a file type").is_dir() {
                walk(&path, out);
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let mut rest = text.as_str();
            while let Some(at) = rest.find(".Values.") {
                rest = &rest[at + ".Values.".len()..];
                let path: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
                    .collect();
                let path = path.trim_end_matches('.').to_string();
                if !path.is_empty() {
                    out.insert(path);
                }
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(&repo_root().join("deploy/helm/cogneva/templates"), &mut out);
    out
}

/// Every leaf path of a parsed values document, empty mappings included.
fn leaf_paths(value: &serde_yaml::Value, prefix: &mut Vec<String>, out: &mut BTreeSet<String>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if map.is_empty() && !prefix.is_empty() {
                out.insert(prefix.join("."));
            }
            for (key, child) in map {
                let key = match key {
                    serde_yaml::Value::String(s) => s.clone(),
                    other => format!("{other:?}"),
                };
                prefix.push(key);
                leaf_paths(child, prefix, out);
                prefix.pop();
            }
        }
        _ => {
            if !prefix.is_empty() {
                out.insert(prefix.join("."));
            }
        }
    }
}

/// The text of the scripts that may read a chart value the templates do not.
fn deploy_scripts_text() -> String {
    let mut out = String::new();
    let dir = repo_root().join(DEPLOY_SCRIPTS);
    let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.expect("a script directory entry");
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if !matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("sh" | "py" | "bash")
        ) {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(&path) {
            out.push_str(&text);
            out.push('\n');
        }
    }
    out
}

/// A profile's job is to say what changes; a key nothing reads changes nothing.
///
/// Helm is silent about this in both directions: a `--set` of a path no template
/// reads creates the path and renders as if it had not been given, and a profile
/// carrying a stale key is simply carried. The deployment then reads as
/// configured ("we set the address, the logs must be going there") while the
/// value sits in the merge result and moves nothing.
///
/// Templates are the readers here, matched by path prefix so that a `toYaml` of
/// a parent covers its subtree. The projection scripts are the other reader: a
/// number that sizes a quota is not in any manifest and cannot be read by one,
/// so a leaf whose last segment a script names by hand counts as read.
#[test]
fn a_profile_leaf_nobody_reads_is_a_knob_the_deployment_cannot_turn() {
    let read_ = paths_the_templates_read();
    let scripts = deploy_scripts_text();
    assert!(
        !read_.is_empty() && !scripts.is_empty(),
        "nothing was read from the chart templates or the deploy scripts, so this \
         test would pass on any profile -- the paths moved"
    );

    let dir = repo_root().join(PROFILES);
    let mut unread = Vec::new();
    let mut checked = 0;
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("a profile directory entry").path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .collect();
    files.sort();
    for file in files {
        let name = file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parsed: serde_yaml::Value = serde_yaml::from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display())),
        )
        .unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        let mut leaves = BTreeSet::new();
        leaf_paths(&parsed, &mut Vec::new(), &mut leaves);
        for leaf in leaves {
            checked += 1;
            let read_by_template = read_
                .iter()
                .any(|read| leaf == *read || leaf.starts_with(&format!("{read}.")));
            let last = leaf.rsplit('.').next().unwrap_or(&leaf);
            let read_by_script = ["\"", "'"]
                .iter()
                .any(|quote| scripts.contains(&format!("{quote}{last}{quote}")));
            if !read_by_template && !read_by_script {
                unread.push(format!("{name}: {leaf}"));
            }
        }
    }

    assert!(checked > 0, "{PROFILES} holds no key at all");
    assert!(
        unread.is_empty(),
        "these profile keys are set by a deployment and read by nothing -- no \
         template, no projection script -- so the value is carried in the merge \
         result and changes nothing while the deployment reads as configured: \
         {unread:#?}"
    );
}

//! Every number a deployment ships is one of three things: something a
//! measurement sets, something a deployment names, or something that is not a
//! dial at all. Only the first moves when the world it reads moves -- and the
//! other two look exactly like it, because a number a loop computed and a number
//! somebody typed are the same number on the page.
//!
//! So the three are registered here, one row per dial, together with the reason
//! each is not the first. The table is held to what the tree actually does in
//! two directions: the row's declared source has to be what the code publishing
//! that value does, and the reading the value lands on has to say where it came
//! from. A value registered as derived while its publisher never says so reads
//! as a number that tracks the world and is not one -- which is the failure this
//! is here to catch, since a cap that stopped tracking its volume is a steady
//! number either way.
//!
//! A key with no row is the same hole as a key with no loop: the registry is
//! what makes "nobody has looked at this yet" visible as a row rather than as an
//! absence. Rows for other families land as their loops land.

use std::path::{Path, PathBuf};

use cog_core::build_cache_readings::{
    BuildCacheReadings, CapSource, BUILD_TARGET_CAP_METRIC, SOURCE_LABEL,
};
use cog_core::Observable;

/// Where a value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// A measurement loop sets it, so it moves when the input it reads moves.
    Derived,
    /// The deployment names it, so it stays where it was pinned.
    Override,
    /// Not a dial: a protocol number or a practice, which no measurement should
    /// move. Registered so that "fixed by agreement" and "nobody has derived
    /// this yet" are told apart by the row rather than by whoever reads it next.
    #[allow(dead_code)]
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

/// The file whose code decides a value's source, and the reading the value
/// lands on.
type Published = (&'static str, &'static str);

/// One dial this repository ships.
struct Tunable {
    /// The chart key a deployment sets.
    key: &'static str,
    /// Where the value comes from today.
    source: Source,
    /// Why it is not derived. A derived row says what input it reads instead.
    why: &'static str,
    /// The publisher and the reading, for a value whose number reaches one.
    published: Option<Published>,
}

/// The build-cache family: two publishers of one cache shape, each holding a cap
/// of its own, and both caps a number somebody typed.
const TUNABLES: &[Tunable] = &[
    Tunable {
        key: "evolution.workspaces.targetMaxBytes",
        source: Source::Override,
        why: "80 GiB, named by the chart. The value a derived cap would read is \
              the peak of the working set the cache walk already measures, held \
              under the free space of the volume the directory sits on; the walk \
              runs and publishes the size per layer, and nothing closes that back \
              into the cap, so the number stays the deployment's to name. It is \
              worth a row rather than a comment because a cap below the working \
              set is reclaimed every pass -- the cache is emptied and rebuilt each \
              round, and its size reads the same either way.",
        published: Some((
            "crates/cog-reflection/src/plugin.rs",
            BUILD_TARGET_CAP_METRIC,
        )),
    },
    Tunable {
        key: "sandboxExecutor.buildCacheMaxBytes",
        source: Source::Override,
        why: "3 GiB, the shared default for an executor that names no cap. The \
              value a derived cap would read is the free space of the volume its \
              own directory sits on, which this process can measure; nothing feeds \
              one back into the number yet. A separate row from the one above \
              rather than one knob spelled twice: the two directories are the same \
              string under two different claims, so a bound that follows the path \
              does not follow the volume.",
        published: Some((
            "crates/cog-extension/src/build_cache.rs",
            BUILD_TARGET_CAP_METRIC,
        )),
    },
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
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

#[test]
fn every_tunable_says_why_its_value_is_not_derived() {
    let mut silent = Vec::new();
    for t in TUNABLES {
        if t.why.trim().is_empty() {
            silent.push(format!("`{}` ({:?})", t.key, t.source));
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
fn every_tunable_names_a_key_the_chart_sets() {
    let values = read(&repo_root().join("deploy/helm/cogneva/values.yaml"));
    let mut unknown = Vec::new();
    for t in TUNABLES {
        let leaf = t.key.rsplit('.').next().unwrap_or(t.key);
        let set = values.lines().any(|line| {
            line.trim_start()
                .split_once(':')
                .is_some_and(|(key, rest)| key == leaf && !rest.trim().is_empty())
        });
        if !set {
            unknown.push(t.key);
        }
    }
    assert!(
        unknown.is_empty(),
        "these rows name a key the chart does not set a value for, so the \
         registry describes a dial no deployment has: {unknown:#?}"
    );
}

#[test]
fn a_tunable_is_registered_as_the_source_its_publisher_uses() {
    let mut wrong = Vec::new();
    for t in TUNABLES {
        let Some((file, _)) = t.published else {
            continue;
        };
        let published_as_derived = says_the_cap_is_derived(file);
        if published_as_derived != (t.source == Source::Derived) {
            wrong.push(format!(
                "`{}` is registered as {:?}, and `{file}` publishes its cap {} \
                 saying where the cap came from",
                t.key,
                t.source,
                if published_as_derived {
                    "while"
                } else {
                    "without"
                }
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
fn a_value_that_lands_on_a_reading_has_a_word_the_reading_can_carry() {
    let mut unnameable = Vec::new();
    for t in TUNABLES {
        if t.published.is_some() && t.source.as_label().is_none() {
            unnameable.push(t.key);
        }
    }
    assert!(
        unnameable.is_empty(),
        "these values are registered as landing on a reading, and the source \
         they are registered with is one the reading's label cannot take, so the \
         reading would have to say something else about where the number came \
         from: {unnameable:?}"
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

    // And the reading each row names is one of those.
    let mut checked = 0;
    let metrics = bounded().collect_metrics("").await.unwrap();
    for t in TUNABLES {
        let Some((_, metric)) = t.published else {
            continue;
        };
        if !metrics.iter().any(|m| m.name == metric) {
            wrong.push(format!(
                "`{}` lands on `{metric}`, which a bounded cache does not publish",
                t.key
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

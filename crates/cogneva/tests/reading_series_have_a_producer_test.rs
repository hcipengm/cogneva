//! Gate: every database series a reading names must be one the exporter we ship
//! actually emits, and every flag we pass it must be one it actually answers to.
//!
//! A reading is only honest if something produces its series. Two ways to get
//! that wrong, and neither shows up as an error:
//!
//! * the manifest names a flag the image does not know — the sidecar exits at
//!   startup with "unknown long flag", and because it shares a pod with postgres
//!   the whole pod stays NotReady: the database is up, the Service has no
//!   endpoint, and every client reads it as "the database is gone";
//! * a reading names a series no collector of that image emits — the board shows
//!   "no series" where a healthy system would show a number, and that reads like
//!   a missing exporter rather than like a reading that was never wired.
//!
//! A previous version of this gate asserted that the manifest named a particular
//! collector. It passed while the reading had no producer at all, because naming
//! a collector is not the same fact as that collector emitting the series. So the
//! mapping is not written here from memory: it is read from
//! `deploy/images/postgres-exporter.series.yaml`, which is measured off the
//! pinned image (the commands are in its header) and carries the tag it was
//! measured on.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// What the pinned exporter image actually does, measured rather than assumed.
struct Inventory {
    image: String,
    /// Flag spellings the image's `--help` lists.
    collector_flags: Vec<String>,
    /// Series (or `prefix*`) the image emits with no collector flags passed.
    series: Vec<String>,
}

fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{} cannot be read: {e}", path.display()))
}

/// The inventory is a flat file written for humans and read by this test: `key:`
/// lines, then indented `- item` lists under `collector_flags:` / `series:`.
fn inventory() -> Inventory {
    let path = repo_root().join("deploy/images/postgres-exporter.series.yaml");
    let text = read_file(&path);
    let mut image = String::new();
    let mut collector_flags = Vec::new();
    let mut series = Vec::new();
    let mut list: Option<&mut Vec<String>> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("- ") {
            let item = rest.trim().trim_matches('"').to_string();
            match list.as_deref_mut() {
                Some(target) => target.push(item),
                None => panic!("{}: list item outside a list: {trimmed}", path.display()),
            }
            continue;
        }
        list = None;
        if let Some(rest) = trimmed.strip_prefix("image:") {
            image = rest.trim().to_string();
        } else if trimmed == "collector_flags:" {
            list = Some(&mut collector_flags);
        } else if trimmed == "series:" {
            list = Some(&mut series);
        }
    }
    assert!(!image.is_empty(), "{} has no image: line", path.display());
    assert!(
        !collector_flags.is_empty() && !series.is_empty(),
        "{} has an empty flag or series list",
        path.display()
    );
    Inventory {
        image,
        collector_flags,
        series,
    }
}

/// The `args:` of the postgres exporter sidecar, as the single-node profile
/// renders them.
fn postgres_exporter_args() -> Vec<String> {
    let path = repo_root().join("deploy/rendered/k3s-single/42-statefulset-postgres.yaml");
    let text = read_file(&path);
    let indent_of = |line: &str| line.len() - line.trim_start().len();
    let mut args = Vec::new();
    let mut container_indent: Option<usize> = None;
    let mut in_args = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if container_indent.is_none() {
            if trimmed == "- name: postgres-exporter" {
                container_indent = Some(indent_of(line));
            }
            continue;
        }
        let base = container_indent.unwrap_or(0);
        // A list item at the container's own indentation is the next container.
        if indent_of(line) == base && trimmed.starts_with("- ") {
            break;
        }
        if indent_of(line) == base + 2 && trimmed == "args:" {
            in_args = true;
            continue;
        }
        if in_args {
            if let Some(rest) = trimmed.strip_prefix("- ") {
                args.push(rest.trim_matches('"').to_string());
            } else {
                in_args = false;
            }
        }
    }
    args
}

/// The image the chart pins, as the values file spells it.
fn pinned_exporter_image() -> String {
    let path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let text = read_file(&path);
    text.lines()
        .find_map(|line| line.trim().strip_prefix("exporterImage:"))
        .map(|rest| rest.trim().to_string())
        .unwrap_or_else(|| panic!("{} has no exporterImage: line", path.display()))
}

/// The mapping is only true of the image it was measured on, so the two have to
/// name the same tag: a bump without a fresh measurement would leave the gate
/// vouching for a version nobody looked at.
#[test]
fn the_pinned_image_is_the_one_the_inventory_was_measured_on() {
    let inventory = inventory();
    let pinned = pinned_exporter_image();
    assert_eq!(
        pinned, inventory.image,
        "values.yaml pins {pinned} but deploy/images/postgres-exporter.series.yaml \
         was measured on {}; re-run the commands in that file's header against the \
         new image and record what it answers",
        inventory.image
    );
}

/// A flag the image does not answer to is not a missing reading: it is the
/// sidecar exiting at startup and taking the database's only Service endpoint
/// with it.
#[test]
fn every_flag_the_sidecar_passes_is_one_the_pinned_image_answers_to() {
    let inventory = inventory();
    for arg in postgres_exporter_args() {
        if !arg.starts_with("--") {
            continue;
        }
        // `--no-collector.x` is the same flag spelled off.
        let spelling = arg
            .strip_prefix("--no-")
            .map(|rest| format!("--{rest}"))
            .unwrap_or_else(|| arg.clone());
        assert!(
            inventory.collector_flags.contains(&spelling),
            "the postgres exporter is started with {arg}, which {} does not know; \
             it would exit with \"unknown long flag\" and leave the pod NotReady",
            inventory.image
        );
    }
}

/// The reading that watches the oldest open transaction is the reason this gate
/// exists: it reads `pg_stat_activity_max_tx_duration`, and an exporter version
/// that does not emit it answers "no series" forever — which on a board is
/// indistinguishable from a database whose transactions are all short.
#[test]
fn every_pg_series_a_reading_names_is_one_the_pinned_image_emits() {
    let inventory = inventory();
    let mut checked = 0;
    for spec in cog_gateway::readings::READINGS {
        if !spec.series.starts_with("pg_") {
            continue;
        }
        checked += 1;
        let covered = inventory
            .series
            .iter()
            .any(|entry| match entry.strip_suffix('*') {
                Some(prefix) => spec.series.starts_with(prefix),
                None => entry == spec.series,
            });
        assert!(
            covered,
            "reading {} names {}, which {} does not emit with the flags we pass; \
             either point the reading at a series the image publishes, or add the \
             collector flag and re-measure deploy/images/postgres-exporter.series.yaml",
            spec.id, spec.series, inventory.image
        );
    }
    assert!(
        checked > 0,
        "no database readings found — this gate would be vacuous"
    );
}

/// The readings gate above covers what our own workspace publishes; the board
/// names its postgres series separately, and a panel is read by a person just
/// as the reading is. Same inventory, same judgement.
#[test]
fn the_board_names_the_same_producer_for_its_postgres_panels() {
    let inventory = inventory();
    let path = repo_root().join("crates/cogneva/tests/observability_dashboard_contract_test.rs");
    let text = read_file(&path);
    let mut checked = 0;
    for line in text.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("(\"") else {
            continue;
        };
        let Some((name, tail)) = rest.split_once('"') else {
            continue;
        };
        if !name.starts_with("pg_") || !tail.contains("postgres_exporter") {
            continue;
        }
        let covered = inventory
            .series
            .iter()
            .any(|entry| match entry.strip_suffix('*') {
                Some(prefix) => name.starts_with(prefix),
                None => entry == name,
            });
        checked += 1;
        assert!(
            covered,
            "the board reads {name} from the postgres exporter, but {} does not \
             emit it; the panel would be blank where a healthy database shows a number",
            inventory.image
        );
    }
    assert!(
        checked > 0,
        "no postgres series found in the board contract — this gate would be vacuous"
    );
}

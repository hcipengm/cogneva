//! Gate: every reading that names a database series must ask for the collector
//! that emits it.
//!
//! A reading is only honest if something produces its series. The exporter we
//! ship enables a default set of collectors and everything else is opt-in, so a
//! reading can name a series that no process will ever emit — and the failure is
//! silent in the worst way: the board shows "no series" where a healthy system
//! would show a number, and an operator reads that as a missing exporter rather
//! than as the reading never having been wired.
//!
//! The check reads the manifest that is actually applied on the single-node
//! profile, not the template: what matters is what the process is started with.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The `args:` of the postgres exporter sidecar, as the single-node profile
/// renders them.
fn postgres_exporter_args() -> Vec<String> {
    let path = repo_root().join("deploy/rendered/k3s-single/42-statefulset-postgres.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} cannot be read: {e}", path.display()));
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

#[test]
fn the_oldest_transaction_reading_has_the_collector_that_emits_it() {
    // `stat_activity` is an opt-in family: without it the sidecar never emits
    // `pg_stat_activity_max_tx_duration`, and the reading that watches the
    // oldest open transaction answers "no series" forever.
    let args = postgres_exporter_args();
    assert!(
        args.iter().any(|a| a == "--collector.stat_activity"),
        "the postgres exporter is started with {args:?}, which does not enable the \
         stat_activity collector — the oldest-transaction reading would have no producer"
    );
}

#[test]
fn every_pg_series_a_reading_names_has_a_collector_it_depends_on() {
    // The mapping is the reason this gate exists: a reading names a series, the
    // sidecar names collectors, and nothing else connects the two. Only the
    // series that come from opt-in families are listed — the default ones are
    // on without being named.
    let opt_in: [(&str, &str); 1] = [("pg_stat_activity_", "--collector.stat_activity")];
    let args = postgres_exporter_args();
    for spec in cog_gateway::readings::READINGS {
        for (prefix, collector) in opt_in {
            if spec.series.starts_with(prefix) {
                assert!(
                    args.iter().any(|a| a == collector),
                    "{} reads {} but the exporter does not pass {collector}",
                    spec.id,
                    spec.series
                );
            }
        }
    }
}

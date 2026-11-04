//! The outward loop exists in code, and the deployment can turn it on.
//!
//! Two halves have to agree for this capability to be real: the process reads
//! one environment variable, and the chart is the only thing that can set it.
//! If either half drifts, nothing fails — the loop simply never starts, which
//! reads exactly like a deployment that chose not to run it. That is the same
//! silence a switch with no reader produces, so the agreement is asserted here
//! instead of being described in two files that each look correct alone.
//!
//! The other half of the claim is the default: the shipped configuration names
//! no queries, so a deployment that has not asked for this makes no outbound
//! call. Turning it on is a decision with a cost — every result becomes work —
//! and it has to be taken deliberately, by setting a value.

use std::path::Path;

/// The repository root, from this crate's manifest directory.
fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/<crate> sits two levels under the root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The chart sets the variable the loop reads — the same string, not a spelling
/// that happens to look similar.
#[test]
fn the_deployment_sets_the_variable_the_loop_reads() {
    let var = cog_orchestrator::outward::QUERIES_ENV;
    let template = read("deploy/helm/cogneva/templates/gateway.yaml");
    assert!(
        template.contains(&format!("- name: {var}")),
        "the outward loop reads {var}, and the chart never sets it: {template_file} \
         would carry the capability dormant in every deployment",
        template_file = "deploy/helm/cogneva/templates/gateway.yaml"
    );
}

/// The switch is under the operator's hand: a values key names the queries, and
/// the template only sets the variable when that key is non-empty.
#[test]
fn the_switch_is_a_value_whose_default_is_off() {
    let values = read("deploy/helm/cogneva/values.yaml");
    assert!(
        values.contains("outward:\n  queries: []"),
        "outward.queries must default to an empty list: every result the loop finds \
         becomes work somebody is asked to do, so a deployment that has not asked for \
         this must not make the outbound calls"
    );
    let template = read("deploy/helm/cogneva/templates/gateway.yaml");
    assert!(
        template.contains("{{- if .Values.outward.queries }}"),
        "the variable is set unconditionally, so an empty value still reaches the \
         process and the deployment cannot tell 'asked for nothing' from 'the switch \
         is wired'"
    );
    assert!(
        template.contains(".Values.outward.queries | toJson"),
        "values are a YAML list and the process parses JSON: rendering the list with \
         anything but toJson hands the loop a string it cannot read, and the readings \
         would say 'configured and unparsable' for a configuration that looks right \
         in the values file"
    );
}

/// The one profile this platform actually runs on turns the loop on.
///
/// This is a decision on record, not a mechanism: the chart default stays empty so
/// that whoever deploys decides for themselves. Our own single-node deployment
/// decided, and a capability that is built but dark in every deployment has no
/// reading that ever says it works -- the configuration cell sits at "unset" and
/// nobody is told the system is blind to everything outside it. Turning it back
/// off is fine; doing it silently is what this assertion refuses.
#[test]
fn the_profile_we_run_names_something_to_look_for() {
    let profile = read("deploy/helm/cogneva/profiles/k3s-single.yaml");
    assert!(
        profile.contains("\n  queries:\n") || profile.contains("\n  queries: ["),
        "the k3s-single profile names no outward queries, so the loop this platform \
         runs is dark there: nothing outside the tree is ever read"
    );
    assert!(
        profile.contains("query: \""),
        "the profile has a queries key but no subject in it: an empty list still \
         leaves the loop dark"
    );
}

/// The loop is registered where the other resident loops are, next to the
/// publishing of its own readings.
#[test]
fn the_plugin_registers_the_loop_and_publishes_its_readings() {
    let plugin = read("crates/cog-orchestrator/src/plugin.rs");
    assert!(
        plugin.contains("spawn_outward_loop("),
        "nothing starts the outward loop, so the module is a library no process calls"
    );
    assert!(
        plugin.contains("OutwardCensus::new()"),
        "the readings are never published: the configuration cell is what tells \
         'not configured' apart from 'configured and found nothing'"
    );
    assert!(
        plugin.contains("record_config("),
        "the configuration decision is not recorded, so a deployment that turned this \
         on and a deployment that did not are the same absence at the scrape"
    );
}

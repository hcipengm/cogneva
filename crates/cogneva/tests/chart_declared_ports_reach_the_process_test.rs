//! principle: feedback_no_hardcoding_config_surface
//! Two addresses in the code name a port the chart owns, and neither had a
//! route from `values.yaml` to the process that uses it:
//!
//!   - the container self-report target (`http://cogneva:<internalPort>/...`),
//!     read by every container running this image;
//!   - the verified-webhook forward base (`http://cogneva:<webhookPort>`), read
//!     by the security gateway after it attaches the internal HMAC.
//!
//! Both are `std::env::var(...).unwrap_or_else(<literal>)`, which reads like a
//! configurable knob and is not one: `COGNEVA_PROCESS_REPORT_URL` and
//! `COGNEVA_WEBHOOK_FORWARD_URL` were set nowhere in `deploy/`, so the literal
//! was the only source. Move `gateway.internalPort` or `gateway.webhookPort` and
//! the container port, the Service, the NetworkPolicy and `cogneva.json` all
//! follow the new value while these two keep posting to the old one. What comes
//! back is a connection refused or a timeout, which every caller in this tree
//! reports as "the other side is down" -- the incident starts on the wrong
//! machine, and the process-report reading shows the same empty cell it shows
//! for a third-party image that never reported at all.
//!
//! What this asserts, in order of how far it reaches:
//!
//!   1. the chart sets each variable, and the value it sets is derived from the
//!      `values.yaml` key rather than typed as a port;
//!   2. the compiled default agrees with that key, so the fallback is not a
//!      second opinion about the same port;
//!   3. the static manifest carries the rendered value, so the GitOps pull path
//!      and the helm path hand the process the same address.
//!
//! The ports are read from `values.yaml`, not written here: a gate that repeats
//! the number it guards would red on the change that moves the port, which is
//! the change it exists to permit.

use crate::common;

use common::routes::{read_repo, repo_root};

/// The `values.yaml` number under `gateway.<key>`, read from the chart.
fn gateway_value(key: &str) -> u64 {
    let path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let parsed: serde_yaml::Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    parsed
        .get("gateway")
        .and_then(|gateway| gateway.get(key))
        .and_then(|value| value.as_u64())
        .unwrap_or_else(|| panic!("deploy/helm/cogneva/values.yaml has no gateway.{key}"))
}

/// The rendered `- name: <env>` entry and the line under it, or a panic naming
/// the file. Every env entry in this chart is name-then-value; an entry that
/// takes its value from a Secret has no `value:` line and is returned as such.
fn env_entry<'a>(text: &'a str, file: &str, env: &str) -> &'a str {
    let needle = format!("- name: {env}");
    let start = text.find(&needle).unwrap_or_else(|| {
        panic!(
            "{file} never sets {env}: the code's fallback is then the only source, and it \
             carries a port no `--set` can move"
        )
    });
    let rest = &text[start + needle.len()..];
    let end = rest.find("- name: ").unwrap_or(rest.len());
    &rest[..end]
}

fn assert_derived_from(values_path: &str, body: &str, file: &str) {
    assert!(
        body.contains(values_path),
        "{file}: the env value does not read `{values_path}`, so moving that key moves the \
         Service and the NetworkPolicy but not the address the process posts to. Entry was:\n{body}"
    );
}

#[test]
fn the_report_target_follows_the_chart_instead_of_its_own_constant() {
    let port = gateway_value("internalPort");
    let chart = read_repo("deploy/helm/cogneva/templates/gateway.yaml");
    let body = env_entry(
        &chart,
        "deploy/helm/cogneva/templates/gateway.yaml",
        "COGNEVA_PROCESS_REPORT_URL",
    );
    assert_derived_from(
        ".Values.gateway.internalPort",
        body,
        "deploy/helm/cogneva/templates/gateway.yaml",
    );

    let default = cogneva::process_report::DEFAULT_REPORT_URL;
    assert!(
        default.contains(&format!(":{port}/")),
        "the compiled fallback says `{default}` while the chart declares internalPort={port}: \
         with the variable unset the reporter would post to a port the Service does not publish"
    );

    let static_manifest = read_repo("deploy/k3s/deployment.yaml");
    let body = env_entry(
        &static_manifest,
        "deploy/k3s/deployment.yaml",
        "COGNEVA_PROCESS_REPORT_URL",
    );
    assert!(
        body.contains(&format!(":{port}/")),
        "deploy/k3s/deployment.yaml carries a different port than values.yaml ({port}); the \
         GitOps pull path would hand the process another address than helm does. Entry was:\n{body}"
    );
}

#[test]
fn the_webhook_forward_base_follows_the_chart_instead_of_its_own_constant() {
    let port = gateway_value("webhookPort");
    let chart = read_repo("deploy/helm/cogneva/templates/security-gateway.yaml");
    let body = env_entry(
        &chart,
        "deploy/helm/cogneva/templates/security-gateway.yaml",
        "COGNEVA_WEBHOOK_FORWARD_URL",
    );
    assert_derived_from(
        ".Values.gateway.webhookPort",
        body,
        "deploy/helm/cogneva/templates/security-gateway.yaml",
    );

    let source = read_repo("crates/cog-gateway/src/security_gateway.rs");
    let default = format!("http://cogneva:{port}");
    assert!(
        source.contains(&default),
        "the compiled fallback in crates/cog-gateway/src/security_gateway.rs is not `{default}` \
         while the chart declares webhookPort={port}: with the variable unset the gateway would \
         forward verified webhooks to a closed port and the upstream would see a timeout"
    );

    let static_manifest = read_repo("deploy/k3s/gateway-deployment.yaml");
    let body = env_entry(
        &static_manifest,
        "deploy/k3s/gateway-deployment.yaml",
        "COGNEVA_WEBHOOK_FORWARD_URL",
    );
    assert!(
        body.contains(&format!(":{port}")),
        "deploy/k3s/gateway-deployment.yaml carries a different port than values.yaml ({port}). \
         Entry was:\n{body}"
    );
}

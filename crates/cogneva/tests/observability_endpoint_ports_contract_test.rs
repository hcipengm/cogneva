//! principle: feedback_no_hardcoding_config_surface
//! The chart points cogneva at the observability stack by address, and the stack
//! opens those addresses in its own manifests under `deploy/k3s/observability/`.
//! Two files, one number each: nothing in either one can tell that the other
//! moved.
//!
//! The failure is the quiet kind. Loki opens on the port the stack's manifest
//! says; the chart still names the old one; the app's pushes go into a socket
//! nobody is listening on. Nothing crashes, no probe fails -- the log pipeline
//! simply has one fewer process in it, and a pipeline missing a process reads
//! exactly like a quiet week.
//!
//! So the endpoint the chart names is read back against the Service the stack
//! actually declares. A service the stack does not declare here (the Prometheus
//! one comes from the monitoring release, whose service name is the chart's
//! convention rather than a manifest in this tree) has to be named in
//! [`EXEMPT`] with the reason, so that "not checked" is a decision on the page
//! rather than a gap.

use crate::common;

use common::routes::{read_repo, repo_root};

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

/// The chart's values, where the endpoints are named.
const VALUES: &str = "deploy/helm/cogneva/values.yaml";

/// The stack's own manifests, where the ports are opened.
const MANIFESTS: &str = "deploy/k3s/observability/manifests";

/// The endpoints the chart must not disagree with the stack about, spelled as
/// dotted paths into [`VALUES`].
const ENDPOINTS: &[&str] = &[
    "observability.loki.endpoint",
    "observability.clickhouse.baseUrl",
    "observability.infraWatch.prometheusUrl",
];

/// Services whose port this tree cannot read, and why that is not a hole.
const EXEMPT: &[(&str, &str)] = &[(
    "cog-observability-prometheus",
    "created by the monitoring release, not by a manifest here: its service name \
     and port are that chart's conventions. The chart's own scrape and datasource \
     settings name the same address, so the release's values file is the place \
     that would have to change with it.",
)];

/// One Service the observability stack declares.
#[derive(Debug, Clone)]
struct Service {
    namespace: String,
    name: String,
    ports: Vec<u64>,
}

fn services() -> Vec<Service> {
    let dir = repo_root().join(MANIFESTS);
    let mut out = Vec::new();
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.expect("a manifest directory entry").path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .collect();
    entries.sort();
    for path in entries {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for document in serde_yaml::Deserializer::from_str(&text) {
            let value: serde_yaml::Value = match serde_yaml::Value::deserialize(document) {
                Ok(value) => value,
                // A manifest this test cannot read is not a licence to skip it:
                // the endpoint it might have declared would then go unchecked.
                Err(e) => panic!("{}: {e}", path.display()),
            };
            let Some(map) = value.as_mapping() else {
                continue;
            };
            let get = |key: &str| map.get(serde_yaml::Value::String(key.into()));
            if get("kind").and_then(|k| k.as_str()) != Some("Service") {
                continue;
            }
            let Some(meta) = get("metadata").and_then(|m| m.as_mapping()) else {
                continue;
            };
            let field = |key: &str| {
                meta.get(serde_yaml::Value::String(key.into()))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            };
            let ports = map
                .get(serde_yaml::Value::String("spec".into()))
                .and_then(|s| s.as_mapping())
                .and_then(|s| s.get(serde_yaml::Value::String("ports".into())))
                .and_then(|p| p.as_sequence())
                .map(|list| {
                    list.iter()
                        .filter_map(|entry| {
                            entry
                                .as_mapping()?
                                .get(serde_yaml::Value::String("port".into()))?
                                .as_u64()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            out.push(Service {
                namespace: field("namespace"),
                name: field("name"),
                ports,
            });
        }
    }
    out
}

/// The `(service, namespace, port)` an endpoint URL names.
fn endpoint_address(url: &str) -> Option<(String, String, u64)> {
    let without_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = without_scheme.split('/').next()?;
    let (host, port) = host.rsplit_once(':')?;
    let port: u64 = port.parse().ok()?;
    let mut labels = host.split('.');
    let service = labels.next()?.to_string();
    let namespace = match labels.next() {
        Some(label) if label != "svc" => label.to_string(),
        _ => String::new(),
    };
    Some((service, namespace, port))
}

/// The dotted path's value in a parsed `values.yaml`.
fn value_at<'a>(root: &'a serde_yaml::Value, path: &str) -> Option<&'a serde_yaml::Value> {
    let mut current = root;
    for part in path.split('.') {
        current = current
            .as_mapping()?
            .get(serde_yaml::Value::String(part.into()))?;
    }
    Some(current)
}

#[test]
fn the_chart_points_at_the_ports_the_stack_opens() {
    let values: serde_yaml::Value =
        serde_yaml::from_str(&read_repo(VALUES)).unwrap_or_else(|e| panic!("{VALUES}: {e}"));
    let services = services();
    assert!(
        !services.is_empty(),
        "{MANIFESTS} declares no Service, so nothing here can be read back -- if the \
         stack moved, point this test at where its manifests live now"
    );

    let mut wrong = Vec::new();
    let mut verified = 0;
    for path in ENDPOINTS {
        let Some(url) = value_at(&values, path).and_then(|v| v.as_str()) else {
            wrong.push(format!(
                "{VALUES} no longer carries `{path}` as a string, so the endpoint this \
                 test reads is gone -- update the path here rather than deleting the \
                 check"
            ));
            continue;
        };
        let Some((service, namespace, port)) = endpoint_address(url) else {
            wrong.push(format!(
                "{path} = {url:?} is not a `scheme://host:port` address, so nothing can \
                 be compared with the stack"
            ));
            continue;
        };
        let declared: Vec<&Service> = services
            .iter()
            .filter(|s| s.name == service && (namespace.is_empty() || s.namespace == namespace))
            .collect();
        match declared.as_slice() {
            [] => match EXEMPT.iter().find(|(name, _)| *name == service) {
                Some(_) => {}
                None => wrong.push(format!(
                    "{path} points at {service}.{namespace}:{port}, and {MANIFESTS} \
                     declares no Service by that name -- nothing here can tell whether \
                     the two agree. Add the manifest this stack installs it from, or \
                     exempt it with the reason it cannot be read"
                )),
            },
            [only] => {
                if only.ports.contains(&port) {
                    verified += 1;
                } else {
                    wrong.push(format!(
                        "{path} points at {service}:{port}, but the Service declares \
                         {:?} -- a deployment that changes one of them leaves the app \
                         pushing into a closed port, and it fails silently",
                        only.ports
                    ));
                }
            }
            many => {
                let by_ns: BTreeMap<&str, &Vec<u64>> = many
                    .iter()
                    .map(|s| (s.namespace.as_str(), &s.ports))
                    .collect();
                wrong.push(format!(
                    "{path} points at {service}.{namespace} and more than one namespace \
                     declares that name: {by_ns:?} -- address it with the namespace, or \
                     the comparison is a guess"
                ));
            }
        }
    }

    assert!(
        !wrong.is_empty() || verified > 0,
        "no endpoint was compared with a Service the stack declares, and none failed \
         either -- this test is reading nothing"
    );
    assert!(wrong.is_empty(), "{wrong:#?}");
}

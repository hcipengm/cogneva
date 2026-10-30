//! The database's Service must keep publishing endpoints while the pod is not
//! Ready.
//! principle: feedback_an_observer_dying_must_not_take_the_observed_away
//!
//! The metrics sidecar shares its pod with the database. On 2026-10-25 that
//! sidecar exited at startup over a flag its image does not know, so the pod
//! never went Ready — and because `postgres` is a **single-replica headless
//! Service**, "not Ready ⇒ no endpoint" reads to every client as "the database
//! does not exist". Postgres itself was serving the whole time; the gateway
//! failed its own migrations against the empty service name, and the pod could
//! not even be replaced, since a rolling update only proceeds from a Ready pod.
//!
//! With one replica there is nothing to fail over to, so withholding the
//! address buys no safety: it only converts one container's crash into the
//! disappearance of the database. Publishing the address instead leaves the
//! failure where it belongs — a client that connects while postgres is really
//! down gets `connection refused`, which names the thing that is broken.
//!
//! The cluster already answers this shape this way elsewhere: the registry
//! Service publishes not-ready addresses for the same reason, so this is the
//! rule being applied rather than a new one.
//!
//! Reading the carriers rather than the template alone: what gets applied is
//! the rendered output, and a field that exists in `values`/templates but never
//! reaches the render is a different way to have the same outage. The template
//! is read too, because a re-render must not drop the field back out.

use crate::common;

use common::routes::read_repo;

/// Every carrier of the database Service: the chart template that is the
/// authority, the static copy the in-cluster puller applies, and the three
/// rendered profiles.
const CARRIERS: &[&str] = &[
    "deploy/helm/cogneva/templates/postgres.yaml",
    "deploy/k3s/postgres-service.yaml",
    "deploy/rendered/k3s-single/30-service-postgres.yaml",
    "deploy/rendered/k3s-multi/30-service-postgres.yaml",
    "deploy/rendered/k8s-standard/30-service-postgres.yaml",
];

/// The YAML documents in a file, split the way `kubectl apply` splits them.
fn documents(text: &str) -> Vec<&str> {
    text.split("\n---\n").collect()
}

/// Whether a document is the database Service. Matched on whole lines: the
/// substring `kind: Service` also occurs in `kind: ServiceMonitor`, and
/// `name: postgres` occurs in the StatefulSet that shares these files.
fn is_postgres_service(doc: &str) -> bool {
    let lines: Vec<&str> = doc.lines().map(str::trim).collect();
    lines.contains(&"kind: Service") && lines.contains(&"name: postgres")
}

/// Whether the Service publishes addresses of pods that are not Ready yet.
fn publishes_not_ready_addresses(doc: &str) -> bool {
    doc.lines().any(|line| {
        line.trim()
            .strip_prefix("publishNotReadyAddresses:")
            .is_some_and(|value| value.trim().trim_matches('"').eq_ignore_ascii_case("true"))
    })
}

#[test]
fn the_database_service_publishes_not_ready_addresses_in_every_carrier() {
    let mut found = 0;
    let mut failures = Vec::new();
    for carrier in CARRIERS {
        let text = read_repo(carrier);
        let services: Vec<&str> = documents(&text)
            .into_iter()
            .filter(|doc| is_postgres_service(doc))
            .collect();
        if services.is_empty() {
            failures.push(format!("{carrier}: no postgres Service document"));
            continue;
        }
        found += services.len();
        for doc in services {
            if !publishes_not_ready_addresses(doc) {
                failures.push(format!(
                    "{carrier}: the postgres Service does not publish not-ready addresses; \
                     an unready metrics sidecar in the same pod would take the database's \
                     only endpoint with it"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // Control: the scan must have found the object it is judging. A renamed or
    // moved Service would otherwise leave this test vacuously green.
    assert_eq!(
        found,
        CARRIERS.len(),
        "expected exactly one postgres Service per carrier, found {found}"
    );
}

#[test]
fn the_matcher_tells_a_publishing_service_from_a_withholding_one() {
    // Control for the predicate itself: it must reject a Service that differs
    // from the accepted one in this field only.
    let publishing = "apiVersion: v1\nkind: Service\nmetadata:\n  name: postgres\nspec:\n  \
                      publishNotReadyAddresses: true\n";
    let withholding = "apiVersion: v1\nkind: Service\nmetadata:\n  name: postgres\nspec:\n  \
                       clusterIP: None\n";
    let other_service = "apiVersion: v1\nkind: Service\nmetadata:\n  name: redis\nspec:\n  \
                         publishNotReadyAddresses: true\n";
    let statefulset =
        "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: postgres\nspec:\n  \
                       replicas: 1\n";

    assert!(is_postgres_service(publishing) && publishes_not_ready_addresses(publishing));
    assert!(is_postgres_service(withholding) && !publishes_not_ready_addresses(withholding));
    assert!(!is_postgres_service(other_service));
    assert!(!is_postgres_service(statefulset));
}

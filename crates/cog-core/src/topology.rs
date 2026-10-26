//! The names a deployment gives its workloads, and the namespace it puts them in.
//!
//! These are the names the chart creates (`deploy/helm/cogneva/templates/`) and
//! the names in-cluster code asks for -- the ConfigMaps a workload mounts, the
//! two Secrets, the workloads themselves: a deployer rolling `deployment/cogneva`,
//! the gateway reading `deployment/cogneva-security-gateway`, the evolution loop
//! tagging all four base workloads. Every one of those is the same string, and a
//! second copy of it is a copy the chart cannot rename.
//!
//! It fails the way a missing name always fails: `kubectl get deployment <old>`
//! exits 1 with NotFound, a rollout watches a workload that is not there, and a
//! reading of "which revision is live" comes back empty -- none of which says
//! "the chart renamed this workload two commits ago". The rename itself is
//! silent, because the chart and the code never meet at build time.
//!
//! `gate`: `crates/cogneva/tests/workload_names_contract_test.rs` reads the
//! chart and fails if a constant here names a workload the chart does not
//! create, or if any other production source writes one of these names.
//!
//! The namespace is declared in the same place for the same reason, with one
//! difference: `deploy/helm/cogneva/values.yaml` is readable at install time
//! (bootstrap reads it), but not from inside a pod -- so [`namespace`] asks the
//! process first (downward-API env, then the service-account namespace file) and
//! only then falls back to [`DEFAULT_NAMESPACE`].

/// The main application's Deployment, also its Service name.
pub const MAIN_DEPLOYMENT: &str = "cogneva";
/// The container inside [`MAIN_DEPLOYMENT`]. A deployment and a container are
/// two facts that happen to share a name; splitting them at the declaration
/// keeps a divergence between the two a one-line change here instead of a
/// search through every `kubectl set image`.
pub const MAIN_CONTAINER: &str = "cogneva";
/// The credential-holding gateway (the only workload with upstream keys).
pub const SECURITY_GATEWAY: &str = "cogneva-security-gateway";
/// The sandbox executor that runs changes.
pub const SANDBOX_EXECUTOR: &str = "cogneva-sandbox-executor";
/// The evolution loop's Deployment.
pub const EVOLUTION: &str = "cogneva-evolution";
/// The registry/images server the installer creates to seed offline nodes.
pub const IMAGE_SERVER: &str = "cogneva-image-server";
/// The DaemonSet that imports the image on every node.
pub const IMAGE_DISTRIBUTOR: &str = "cogneva-image-distributor";

/// The ConfigMap the running configuration is mounted from (`cogneva.json`).
pub const CONFIG_CONFIGMAP: &str = "cogneva-json";
/// The ConfigMap the prompts are mounted from.
pub const PROMPTS_CONFIGMAP: &str = "cogneva-prompts";
/// The ConfigMap carrying the evolution loop's own settings.
pub const EVOLUTION_CONFIG_CONFIGMAP: &str = "cogneva-evolution-config";
/// The ConfigMap carrying the process's environment (non-secret settings the
/// workloads read as env rather than as a mounted file).
pub const ENV_CONFIGMAP: &str = "cogneva-config";
/// The internal Secret the installer generates (database, redis, internal
/// signing). Business pods read only their own keys from it.
pub const SECRET_INTERNAL: &str = "cogneva-secrets";
/// The out-of-band Secret: the only object carrying real upstream credentials,
/// and the only one mounted into the security gateway alone.
pub const SECRET_OOB: &str = "cogneva-secrets-oob";

/// The namespace the chart declares (`global.namespace`). In-cluster code
/// cannot read the chart, so this is the compiled-in copy; the installer reads
/// the real value and hands it to every command it runs.
pub const DEFAULT_NAMESPACE: &str = "cogneva";

/// Where the service account namespace file lives in every pod.
const SA_NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

/// The namespace this process is deployed in.
///
/// The downward-API env wins (a workload can be given `COGNEVA_NAMESPACE` from
/// `metadata.namespace` even where the service-account token is not mounted),
/// then the service-account file every pod has by default, then the compiled
/// default. Asking the cluster is better than assuming: under
/// `--set global.namespace=<other>` only the first two are right, and a wrong
/// namespace does not fail loudly -- `get` returns an empty list.
pub fn namespace() -> String {
    if let Ok(from_env) = std::env::var("COGNEVA_NAMESPACE") {
        let trimmed = from_env.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Ok(from_file) = std::fs::read_to_string(SA_NAMESPACE_FILE) {
        let trimmed = from_file.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    DEFAULT_NAMESPACE.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four workloads the evolution loop tags and rolls together. Named
    /// here as a list because the reading that compares revisions asks about
    /// them as a set, not one at a time.
    #[test]
    fn the_base_workloads_are_the_declared_constants() {
        let base = [
            MAIN_DEPLOYMENT,
            SECURITY_GATEWAY,
            SANDBOX_EXECUTOR,
            EVOLUTION,
        ];
        assert_eq!(base.len(), 4);
        let mut unique = base.to_vec();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 4, "四个主负载的名字必须互不相同");
    }

    #[test]
    fn the_namespace_is_never_empty() {
        // 三层来源都没给值时也不能拿空串当名字：`kubectl -n ""` 读的是
        // 默认命名空间，会安静地回答一个不是这个问题的问题。
        assert!(!namespace().trim().is_empty());
    }
}

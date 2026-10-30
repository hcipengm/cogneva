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
//! `gate`: `crates/cogneva/tests/deployment_object_names_contract_test.rs` reads
//! the chart and fails if a constant here names a workload the chart does not
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
/// The in-cluster container registry the base workloads pin their image to. Its
/// Deployment and its Service carry the same name; the installer waits on the
/// Deployment and reads the node port off the Service.
pub const REGISTRY: &str = "cogneva-registry";

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
/// The namespace's ResourceQuota. The eval bench's Pods are written into it, so
/// the name is one the code has to know rather than one it merely observes.
pub const QUOTA: &str = "cogneva-quota";

/// The ConfigMap carrying the database-role bootstrap script, mounted by the
/// Job of the same name. Nothing in this crate addresses it by name -- the
/// chart mounts it and the Job runs it -- but a product-named object the
/// manifests create has to be declared, or the next reader that needs to find
/// it writes the string a second time and the two copies drift.
pub const DB_ROLE_BOOTSTRAP_CONFIGMAP: &str = "cogneva-db-role-bootstrap";

/// The namespace the chart declares (`global.namespace`). In-cluster code
/// cannot read the chart, so this is the compiled-in copy; the installer reads
/// the real value and hands it to every command it runs.
pub const DEFAULT_NAMESPACE: &str = "cogneva";

/// The label keys, and the values the workloads' pods answer to.
///
/// Code that asks "are the gateway's pods ready" or "restart the workload that
/// serves the images" selects by these two labels. A selector naming a value no
/// manifest carries matches nothing, and `kubectl get -l` treats an empty list
/// as an ordinary answer: the health reading says "no pods yet", the restart
/// says it succeeded, and neither one distinguishes a workload that has not
/// started from a label that was renamed in the chart. The name label is shared
/// by the main app, the gateway and the executor, so it takes the component
/// label as well — one label alone crosses deployments.
pub const LABEL_NAME: &str = "app.kubernetes.io/name";
/// See [`LABEL_NAME`].
pub const LABEL_COMPONENT: &str = "app.kubernetes.io/component";
/// The `LABEL_NAME` value of the workloads [`MAIN_DEPLOYMENT`],
/// [`SECURITY_GATEWAY`] and [`SANDBOX_EXECUTOR`] all answer to.
pub const WORKLOAD_NAME: &str = "cogneva";
/// The main application's `LABEL_COMPONENT` value (its Deployment is
/// [`MAIN_DEPLOYMENT`], which is why the value and the name differ).
pub const COMPONENT_GATEWAY: &str = "gateway";
/// The credential gateway's `LABEL_COMPONENT` value.
pub const COMPONENT_SECURITY_GATEWAY: &str = "security-gateway";
/// The sandbox executor's `LABEL_COMPONENT` value.
pub const COMPONENT_SANDBOX_EXECUTOR: &str = "sandbox-executor";
/// The evolution loop's `LABEL_COMPONENT` value; its name label is
/// [`EVOLUTION`], not [`WORKLOAD_NAME`].
pub const COMPONENT_EVOLUTION: &str = "evolution";
/// The cluster registry's `LABEL_COMPONENT` value. The restart that makes its
/// garbage collection run selects by this, so a rename in the chart turns the
/// reclaim into a no-op that reports success.
pub const COMPONENT_REGISTRY: &str = "cluster-registry";

/// The annotation a dispatched Job carries its trace id in.
///
/// Two writers put it on the Jobs they create -- the change job and the rollout
/// job -- and one reader collects them back into a chain. A key spelled in three
/// places is three chances to rename one of them, and the result of renaming one
/// is not an error: a stage simply stops appearing in the trace, so the chain
/// reads as shorter rather than as broken.
pub const TRACEPARENT_ANNOTATION: &str = "cogneva.ai/traceparent";

/// The directory every pod's service account is mounted at.
///
/// It is the platform's name rather than this deployment's, and it is declared
/// here because five places had written it out. A wrong copy does not fail
/// loudly: the namespace read falls back to the compiled default, and every
/// list a wrong namespace answers with is empty. The three constants below are
/// one declaration split by file, so the directory is written once.
pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// A file under [`SERVICE_ACCOUNT_DIR`], spelled as one literal so the path
/// cannot be built at run time and then forgotten by a census that reads
/// literals.
macro_rules! service_account_file {
    ($file:literal) => {
        concat!("/var/run/secrets/kubernetes.io/serviceaccount", "/", $file)
    };
}

/// The file that answers "which namespace am I in".
pub const SERVICE_ACCOUNT_NAMESPACE_FILE: &str = service_account_file!("namespace");

/// The projected token: the other half of an in-cluster client config, next to
/// the API endpoint the environment carries.
pub const SERVICE_ACCOUNT_TOKEN_FILE: &str = service_account_file!("token");

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
    if let Ok(from_file) = std::fs::read_to_string(SERVICE_ACCOUNT_NAMESPACE_FILE) {
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

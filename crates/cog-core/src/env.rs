//! Names the platform sets in the environment and more than one crate reads.
//!
//! A name is a contract between whoever sets it (the chart, a supervisor, the
//! installer) and whoever reads it. Two crates that each spell the name
//! themselves are two contracts that happen to look the same today; rename one
//! and the other keeps reading a variable nobody sets. `env::var` answers a
//! missing name with `Err`, and nearly every caller turns that into the default
//! it would have used anyway -- so the knob quietly stops working, and the
//! reading is indistinguishable from "no one configured it".
//!
//! Only names read from more than one crate live here. A name read in one place
//! is that place's business, and moving it here would grow this crate for no
//! shared fact. The path overrides (`COGNEVA_DATA_DIR`, `COGNEVA_CONFIG_PATH`,
//! `COGNEVA_APP_DIR`) are declared next to the paths themselves in
//! [`cog_core::paths`].
//!
//! `gate`: `crates/cogneva/tests/env_names_are_declared_once_test.rs` counts how
//! many production lines spell each name and fails on any second one.

/// The database the process stores everything in. Read by storage, by
/// observability's own alert/usage/skip stores, and by the CLI's backup and
/// governance commands.
pub const DATABASE_URL: &str = "COGNEVA_DATABASE_URL";

/// This pod's own name, as the cluster gave it. Identity and change execution
/// both need it to answer "am I the pod that was asked".
pub const POD_NAME: &str = "COGNEVA_POD_NAME";

/// The node this process runs on.
///
/// Injected by the chart into every workload. The host readings and the process
/// census attach it to what they report so a row can be traced back to a
/// machine; a second spelling would leave one of them reporting an empty node,
/// and an empty node reads as "this reading is not from anywhere" rather than
/// as "the variable moved".
pub const NODE_NAME: &str = "COGNEVA_NODE_NAME";

/// The fingerprint the installer injects. Agent naming and repository identity
/// both read it; absent, each falls back to something that only holds until the
/// next rollout, which is exactly what the fingerprint exists to stop.
pub const INSTANCE_FINGERPRINT: &str = "COGNEVA_INSTANCE_FINGERPRINT";

/// The deployment profile the configuration is selected for. The installer asks
/// it before deploying; the loader and the watcher read the same value to pick
/// which section of the document applies.
pub const RUN_ENV: &str = "COGNEVA_ENV";

/// The name of the deployment this process belongs to, as the manifest gives
/// it. Every retention loop that writes its readings into a table several
/// deployments share puts it on the row, so a row can say which deployment's
/// declaration it is.
///
/// Read by the checkpoint capacity sweep, the prompt-and-response archive's
/// retention and the task event archive's retention. A second spelling would
/// leave one of them publishing under an empty name, which the store reads as
/// "not from anywhere" and answers with whichever deployment wrote last —
/// exactly the confusion the label exists to prevent.
pub const DEPLOYMENT_NAME_ENV: &str = "COGNEVA_DEPLOYMENT_NAME";

/// Where a backup run puts the package it just wrote when somebody asked for
/// that package by hand, so the process serving the web UI can offer it.
///
/// Two parties have to agree on this name and neither writes the other's code:
/// the export Job the API creates carries it as an env value, and the API reads
/// the directory it names to list and stream what came back. A second spelling
/// would mean the Job writes where nobody looks -- and "no packages to
/// download" is exactly what a directory nobody writes looks like.
pub const BACKUP_PUBLISH_DIR: &str = "COGNEVA_BACKUP_PUBLISH_DIR";

/// The passphrase a backup package is encrypted under.
///
/// Set by whoever schedules the run and read by two crates: the binary that
/// encrypts the package, and the gateway that starts an export with it. A second
/// spelling would leave one side looking for a variable nobody sets, and both
/// sides report that as "no passphrase given" — which is also how a run that
/// should be encrypted but is not would look.
pub const BACKUP_PACKAGE_KEY: &str = "COGNEVA_BACKUP_PACKAGE_KEY";

/// The Redis the gateway keeps sessions and rate limits in.
///
/// The gateway dials it; the config loader maps this name to
/// `dag_executor.redis_url` so the same declaration reaches the executor. A
/// second spelling would let the loader document a variable the gateway no
/// longer reads, and both sides answer a missing name with their own default --
/// one process on a fresh server, the other on one holding nothing.
pub const REDIS_URL: &str = "COGNEVA_REDIS_URL";

/// How often the gateway refreshes a contributor's OAuth token, and how close to
/// expiry it starts.
///
/// Read by the gateway that refreshes, and mapped to config keys by the loader
/// that documents what is tunable. Two spellings would leave the knob listed as
/// tunable while the refresh kept its built-in interval.
pub const CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS: &str =
    "COGNEVA_CONTRIBUTION_OAUTH_REFRESH_INTERVAL_SECS";
pub const CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS: &str =
    "COGNEVA_CONTRIBUTION_OAUTH_REFRESH_THRESHOLD_SECS";

/// The repository the GitOps side clones: the gateway serves it as the project
/// repo, the puller reads it as its source of truth.
pub const GITOPS_REPO_URL: &str = "COGNEVA_GITOPS_REPO_URL";

/// The secret the platform's own webhook callers sign with.
///
/// Written by the gateway's secret writer, verified by the GitHub/Gitee plugin,
/// and read by the gateway into its own config. Three readers that must agree on
/// one name: a second spelling verifies against nothing, and every signature
/// fails as "invalid signature" rather than as "this secret is not the one being
/// written".
pub const WEBHOOK_INTERNAL_SECRET: &str = "COGNEVA_WEBHOOK_INTERNAL_SECRET";

/// The secret upstream webhooks are signed with, as the plugin declares it and
/// the gateway reads it.
pub const GITHUB_WEBHOOK_SECRET: &str = "COGNEVA_GITHUB_WEBHOOK_SECRET";

/// How often the LLM upstream pool is checked for health.
///
/// The gateway reads it into its pool config; the supervisor's own pool check
/// reads the same number. Two defaults would let the two checks drift to
/// different intervals while both look configured.
pub const LLM_POOL_CHECK_SECS: &str = "COGNEVA_LLM_POOL_CHECK_SECS";

/// Where the git proxy in front of upstream hosting listens.
///
/// The reflection side resolves it when it clones through the proxy; the gateway
/// serves it as the project repo endpoint. A second spelling would point one of
/// them at an address nobody is serving.
pub const GIT_PROXY_BASE: &str = "COGNEVA_GIT_PROXY_BASE";

/// The commit a build was made from, and how far it sits from the last release.
///
/// Two binaries name these and neither can read the other's constant: the
/// builder injects them into the compile of the main binary, and the deployer
/// passes them into the build it starts. A second spelling would leave an image
/// stamped with a name nobody set -- and an unstamped image answers `unknown`,
/// which reads exactly like a build that genuinely had no `.git`.
pub const GIT_REVISION: &str = "COGNEVA_GIT_REVISION";
pub const VERSION_ID: &str = "COGNEVA_VERSION_ID";

/// The log level the gateway's subscriber reads before falling back to
/// `RUST_LOG`.
///
/// The gateway reads it; the config loader maps it to `app.log_level` so the
/// same declaration reaches the main application. Two spellings would let the
/// loader advertise a level variable the gateway no longer consults, and both
/// sides answer "unset" by picking their own default.
pub const LOG_LEVEL: &str = "COGNEVA_LOG_LEVEL";

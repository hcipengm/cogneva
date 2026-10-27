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

/// The fingerprint the installer injects. Agent naming and repository identity
/// both read it; absent, each falls back to something that only holds until the
/// next rollout, which is exactly what the fingerprint exists to stop.
pub const INSTANCE_FINGERPRINT: &str = "COGNEVA_INSTANCE_FINGERPRINT";

/// The deployment profile the configuration is selected for. The installer asks
/// it before deploying; the loader and the watcher read the same value to pick
/// which section of the document applies.
pub const RUN_ENV: &str = "COGNEVA_ENV";

/// Where a backup run puts the package it just wrote when somebody asked for
/// that package by hand, so the process serving the web UI can offer it.
///
/// Two parties have to agree on this name and neither writes the other's code:
/// the export Job the API creates carries it as an env value, and the API reads
/// the directory it names to list and stream what came back. A second spelling
/// would mean the Job writes where nobody looks -- and "no packages to
/// download" is exactly what a directory nobody writes looks like.
pub const BACKUP_PUBLISH_DIR: &str = "COGNEVA_BACKUP_PUBLISH_DIR";

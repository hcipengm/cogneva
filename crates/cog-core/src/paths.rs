//! Where the image puts the process's own files.
//!
//! This root is not something a deployment can move: the image installs the
//! binary at `APP_DIR/cogneva`, the built web assets at `APP_DIR/web` and the
//! shipped skills at `APP_DIR/skills`, and the process finds them by their
//! position under itself. The single declaration is the Dockerfile's
//! `SF_APP_DIR`; every other path in the tree is derived from [`APP_DIR`] here
//! instead of being spelled out again. A crate that copies the string writes a
//! second declaration that neither the image nor the chart can move, and it
//! goes wrong only when the layout is refactored -- as a file-not-found from
//! whichever component happens to look first.
//!
//! `gate`: `crates/cogneva/tests/image_layout_literals_contract_test.rs` reads
//! the Dockerfile and fails if any production source spells the root out.
//!
//! The three roots the deployment mounts are declared here for the same reason,
//! with the chart as the artifact that declares them: the data volume at
//! [`DATA_DIR`], the configuration at [`CONFIG_DIR`], the runtime socket
//! directory at [`RUN_DIR`], and the bare git repository at [`TREE_REPO`]. A
//! component that spells one of these out reads a directory the chart never
//! mounted -- and that surfaces as "no data yet" rather than as a wrong path.

/// Where the deployment mounts the data volume.
pub const DATA_DIR: &str = "/var/lib/cogneva-data";
/// Where the deployment mounts the configuration (`cogneva.json`, `prompts/`).
pub const CONFIG_DIR: &str = "/etc/cogneva";
/// The configuration document inside [`CONFIG_DIR`].
pub const CONFIG_FILE_NAME: &str = "cogneva.json";
/// The runtime directory (sockets, pids) the deployment mounts.
pub const RUN_DIR: &str = "/run/cogneva";
/// Where the deployment mounts the bare git repository the evolution loop reads
/// and the change jobs clone from.
pub const TREE_REPO: &str = "/host-git";
/// The host directory the cluster-wide build slot is locked in (`claimStore` in
/// the chart's values). One lock file per host, not per pod: two builds on one
/// machine contend for the same CPU.
pub const BUILD_GATE_LOCK_DIR: &str = "/var/lib/cogneva-build-gate";
/// The host directory the security gateway mirrors git repositories into.
pub const GIT_MIRROR_ROOT: &str = "/var/lib/cogneva-git-mirror";

/// The chart's name for the data directory override.
pub const DATA_DIR_ENV: &str = "COGNEVA_DATA_DIR";
/// The chart's name for the configuration file override.
pub const CONFIG_PATH_ENV: &str = "COGNEVA_CONFIG_PATH";
/// The image's name for the installation root override.
pub const APP_DIR_ENV: &str = "COGNEVA_APP_DIR";

/// The data directory as this process sees it: the chart passes
/// [`DATA_DIR_ENV`], and a host install may point it elsewhere.
pub fn data_dir() -> String {
    std::env::var(DATA_DIR_ENV).unwrap_or_else(|_| DATA_DIR.to_string())
}

/// [`data_dir`] joined with a relative path.
pub fn data_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(&data_dir()).join(relative)
}

/// The configuration file as this process sees it: [`CONFIG_PATH_ENV`] from
/// the chart, [`CONFIG_DIR`]/[`CONFIG_FILE_NAME`] otherwise.
pub fn config_file() -> String {
    std::env::var(CONFIG_PATH_ENV).unwrap_or_else(|_| format!("{CONFIG_DIR}/{CONFIG_FILE_NAME}"))
}

/// The installation root, as the image lays it out (`SF_APP_DIR`).
pub const APP_DIR: &str = "/opt/cogneva";

/// A path under the root, as a `&'static str` — for the places that need one at
/// compile time: `const` items, array literals, `match` patterns.
///
/// The root is written once, here. `concat!` cannot take [`APP_DIR`], so a macro
/// is the only way to derive a compile-time path from one declaration; the price
/// is that the expansion is invisible to a grep, which is why the layout is also
/// listed as named constants below.
#[macro_export]
macro_rules! app_path_literal {
    ($($segment:literal),*) => { concat!("/opt/cogneva", $("/", $segment),*) };
}

/// The main binary's file name. Same fact as [`APP_BINARY`]'s last segment,
/// written once for the callers that need the name alone -- a systemd unit, a
/// sidecar's exec line, `cargo build --bin`.
pub const APP_BINARY_NAME: &str = "cogneva";
/// The main binary, as the image installs it.
pub const APP_BINARY: &str = app_path_literal!("cogneva");
/// The built web assets.
pub const APP_WEB_DIR: &str = app_path_literal!("web");
/// The shipped skills.
pub const APP_SKILLS_DIR: &str = app_path_literal!("skills");
/// The record the overlay writes of what it copied into the image.
pub const APP_RUNTIME_ASSET_MANIFEST: &str = app_path_literal!("runtime-assets.json");
/// The SQL migrations the process applies on start-up.
pub const MIGRATIONS_DIR: &str = app_path_literal!("crates/cog-storage/migrations");

/// A path under the installation root. Callers name the relative segment,
/// e.g. `app_path("sandbox/workspaces")`.
pub fn app_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(APP_DIR).join(relative)
}

/// The same path as a string, for command lines, manifests and config
/// defaults.
pub fn app_path_str(relative: &str) -> String {
    format!("{APP_DIR}/{relative}")
}

/// The installation root as this process sees it: the image exports
/// [`APP_DIR_ENV`] and the chart's `app.app_dir` maps onto the same variable,
/// so a container that was given a different root reads it here. Absent that,
/// the compiled-in layout is the answer.
pub fn app_dir() -> String {
    std::env::var(APP_DIR_ENV).unwrap_or_else(|_| APP_DIR.to_string())
}

/// [`app_path`] under the root this process was told to use.
pub fn process_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(&app_dir()).join(relative)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_named_path_is_under_the_one_root() {
        for path in [
            APP_BINARY,
            APP_WEB_DIR,
            APP_SKILLS_DIR,
            APP_RUNTIME_ASSET_MANIFEST,
            MIGRATIONS_DIR,
        ] {
            assert!(
                path.strip_prefix(APP_DIR)
                    .is_some_and(|rest| rest.starts_with('/')),
                "{path} 不在安装根 {APP_DIR} 下"
            );
        }
    }

    #[test]
    fn the_binary_name_is_the_last_segment_of_the_binary_path() {
        assert_eq!(
            APP_BINARY.rsplit('/').next(),
            Some(APP_BINARY_NAME),
            "二进制名与镜像里的路径指向的不是同一个文件"
        );
    }

    #[test]
    fn every_path_is_under_the_one_root() {
        assert_eq!(app_path_str("sandbox"), format!("{APP_DIR}/sandbox"));
        assert_eq!(app_path("web"), std::path::Path::new(APP_DIR).join("web"));
    }
}

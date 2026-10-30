//! principle: feedback_no_hardcoding_config_surface
//! The service account mount is where a pod learns which namespace it is in and
//! gets the token it acts with. It is the platform's path, the same in every
//! cluster, so it is not something a `--set` should move — but it was written
//! out in five places across four crates, and a copy that drifts does not fail
//! loudly. The namespace read falls back to the compiled default, and every list
//! the wrong namespace answers with is empty; a token read from a path that is
//! not there reads as "not in a cluster", so the code takes its out-of-cluster
//! branch and the cluster it was deployed in is never asked.
//!
//! One declaration, in `cog_core::topology` next to the namespace the same file
//! answers, and the file paths under it are built from it. Everything else reads
//! that declaration: this gate is what stops the next copy from being written.
//!
//! Comments and fixtures are not scanned. A doc comment that spells the path is
//! explaining where the value comes from; a test module may name whatever it
//! likes to build a specimen.

use crate::common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
/// The one file allowed to spell the mount.
const DECLARATION: &str = "crates/cog-core/src/topology.rs";

/// The directory itself rather than one of the three full paths: the two files
/// under it were each written out separately, and matching only the full
/// literals would leave a hand-built `format!` of the directory unread.
const MOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

#[test]
fn only_the_declaration_spells_the_service_account_mount() {
    let mut hits = Vec::new();
    for path in production_sources(&repo_root()) {
        let rel = relative_name(&repo_root(), &path);
        if rel == DECLARATION {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for line in production_lines(&text) {
            if line.contains(MOUNT) {
                hits.push(format!("{rel}: {}", line.trim()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "代码里又写了一遍服务账号挂载路径。它在 {DECLARATION} 里声明一次\
         （`SERVICE_ACCOUNT_DIR` 与它下面的两个文件），其他地方读那份声明：\
         抄错的那一份不会报错，命名空间读不到就退回编译期默认值，返回的列表全是空的。\n{}",
        hits.join("\n")
    );
}

/// The declaration has to name all three paths a pod gets, or the callers it
/// replaced would still be holding their own copy of the pieces it left out.
#[test]
fn the_declaration_names_the_directory_and_the_two_files_under_it() {
    assert_eq!(cog_core::topology::SERVICE_ACCOUNT_DIR, MOUNT);
    assert_eq!(
        cog_core::topology::SERVICE_ACCOUNT_NAMESPACE_FILE,
        format!("{MOUNT}/namespace"),
        "命名空间文件必须就挂在声明目录下，读它的代码与探测那个目录的代码指的是同一处"
    );
    assert_eq!(
        cog_core::topology::SERVICE_ACCOUNT_TOKEN_FILE,
        format!("{MOUNT}/token"),
        "令牌文件必须就挂在声明目录下：两件套分开写就会出现一半对一半错的进程"
    );
}

/// The census above can only be trusted if it reads code and drops the two kinds
/// of text that legitimately mention the path. Both directions are pinned here,
/// because a census that silently reads nothing passes exactly like a clean tree.
#[test]
fn the_census_reads_code_and_not_comments_or_fixtures() {
    let text = format!(
        "//! 文档里解释路径：{MOUNT}/namespace\n\
         pub fn read() {{ let p = \"{MOUNT}/namespace\"; }}\n\
         // 行注释里的 {MOUNT}\n\
         #[cfg(test)]\n\
         mod tests {{ fn fixture() -> &'static str {{ \"{MOUNT}\" }} }}\n"
    );
    let kept: Vec<&str> = production_lines(&text)
        .into_iter()
        .filter(|line| line.contains(MOUNT))
        .collect();
    assert_eq!(
        kept.len(),
        1,
        "判据应当只留下那行代码（注释与夹具都不算），实得 {kept:?}"
    );
    assert!(
        kept[0].contains("pub fn read()"),
        "留下的应当是代码行，实得 {kept:?}"
    );
}

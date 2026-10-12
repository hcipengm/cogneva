//! Locating a project's kind declaration on its tree.
//!
//! What the declaration means is a contract and lives in the contract crate
//! (`cog_core::project_kind`); the file access is here, so that no crate which
//! merely passes the type around inherits a filesystem dependency from it.
//!
//! A document that is not there and a document that could not be read are two
//! different answers and are kept apart. Only `NotFound` is absence; every
//! other error is reported as a declaration that exists and could not be read,
//! because collapsing a permission error or a directory into "the project
//! declared nothing" would dispatch the project under the built-in on the
//! strength of a read that never happened — and the reading would say the
//! project had chosen the built-in.

use std::path::Path;

use cog_core::{project_kind_declaration, ProjectKindDeclaration, PROJECT_KIND_DOCUMENT};

/// The kind the project rooted at `project_root` declares, if any.
pub(crate) fn declaration_in(project_root: &Path) -> ProjectKindDeclaration {
    let path = project_root.join(PROJECT_KIND_DOCUMENT);
    match std::fs::read_to_string(&path) {
        Ok(text) => project_kind_declaration(Some(&text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => ProjectKindDeclaration::Absent,
        Err(e) => ProjectKindDeclaration::Unusable {
            reason: format!("`{}` exists and could not be read: {e}", path.display()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tree_without_the_document_declares_nothing() {
        let root = tempfile::tempdir().expect("temp dir");
        assert_eq!(declaration_in(root.path()), ProjectKindDeclaration::Absent);
    }

    #[test]
    fn a_tree_carrying_the_document_is_read_at_the_project_root() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            root.path().join(PROJECT_KIND_DOCUMENT),
            r#"{"kind": "rust"}"#,
        )
        .expect("write");
        assert_eq!(
            declaration_in(root.path()),
            ProjectKindDeclaration::Declared(cog_core::ProjectKind::Rust)
        );
    }

    /// A document that is there and cannot be read is not a document that is
    /// not there. The directory is the cheapest way to make the read fail on a
    /// path that exists.
    #[test]
    fn a_document_that_cannot_be_read_is_not_absence() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir(root.path().join(PROJECT_KIND_DOCUMENT)).expect("mkdir");
        match declaration_in(root.path()) {
            ProjectKindDeclaration::Unusable { reason } => {
                assert!(!reason.trim().is_empty(), "unusable with no reason");
            }
            other => panic!("an unreadable document read as {other:?}"),
        }
    }
}

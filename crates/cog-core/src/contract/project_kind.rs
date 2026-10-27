//! The landing recipe a project lands by, as the project itself declares it.
//!
//! The platform holds no table from project to kind. Such a table would put
//! project names inside the platform and make every new target an edit to it,
//! which is the shape both "no hardcoded configuration" and "no per-project
//! branch on the business side" forbid. What the platform owns is the *closed
//! set* of recipes it actually implements: a project is free to name itself,
//! and only a name in that set can be dispatched to.
//!
//! The declaration therefore lives in the project's own configuration document.
//! The project says what it is; the dispatch point reads it out of the project
//! tree it already has. The name of that document is a constant here rather
//! than an argument, for the same reason the set is closed: a position passed
//! in from outside would let a caller name a document the project does not
//! carry, and a search that finds nothing is indistinguishable from a project
//! that declared nothing.
//!
//! Three states, and the middle one is the point of the whole surface: a
//! project that carries a declaration and cannot say what it is does not get to
//! land. Without that state, "declared a kind this platform cannot dispatch to"
//! and "declared nothing" would both fall back to the built-in, and a project
//! silently landing as a Rust workspace is exactly the reading this surface
//! exists to make impossible.

use std::fmt;
use std::str::FromStr;

/// The document a project declares its kind in, relative to the project's own
/// root.
///
/// One name for every project cogneva lands changes into, cogneva itself
/// included, so the platform names no project and adding the next target is not
/// a change here. The word itself is [`crate::paths::CONFIG_FILE_NAME`]: this
/// reads it, it does not respell it.
pub const PROJECT_KIND_DOCUMENT: &str = crate::paths::CONFIG_FILE_NAME;

/// The one key read out of that document.
pub const PROJECT_KIND_KEY: &str = "kind";

/// The recipe used when a project declares nothing.
///
/// A project that says nothing is not refused: refusing would make every
/// existing project — none of which carries a declaration yet — unlandable.
/// What it does not get is silence: the dispatch records that the built-in was
/// used, so "this project never declared" stays readable and distinct from
/// "this project declared the built-in".
pub const BUILT_IN_PROJECT_KIND: ProjectKind = ProjectKind::Rust;

/// A landing recipe this platform implements.
///
/// The members are recipes the landing path can actually carry out, not
/// languages: adding a member here without the branch that runs it would route
/// changes into a path that does not exist.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ProjectKind {
    /// A Rust workspace: the change is applied to the tree, the workspace's
    /// suite is run in it, and a landed revision is built and switched to.
    Rust,
}

impl ProjectKind {
    /// Every recipe this platform implements, so a reader publishes the axis
    /// instead of the values it happens to have seen.
    pub const ALL: &'static [ProjectKind] = &[Self::Rust];

    /// The wire form, for the declaration document, label values and records.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Rust => "rust",
        }
    }
}

impl fmt::Display for ProjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A name a project used that no recipe here answers.
///
/// Carries the declared name and, in its message, every name that would have
/// been accepted: a project that reads "unknown kind" and has to guess which
/// ones exist will guess wrong, and the set is small enough to print.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UnknownProjectKind {
    declared: String,
}

impl UnknownProjectKind {
    /// The name the project used, verbatim.
    pub fn declared(&self) -> &str {
        &self.declared
    }
}

impl fmt::Display for UnknownProjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no landing recipe named `{}`; this platform implements: {}",
            self.declared,
            ProjectKind::ALL
                .iter()
                .map(ProjectKind::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl std::error::Error for UnknownProjectKind {}

impl FromStr for ProjectKind {
    type Err = UnknownProjectKind;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ProjectKind::ALL
            .iter()
            .copied()
            .find(|kind| kind.as_str() == s)
            .ok_or_else(|| UnknownProjectKind {
                declared: s.to_string(),
            })
    }
}

/// What a project's own configuration said about how it lands.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ProjectKindDeclaration {
    /// Nothing was declared: the key is absent, or the project carries no
    /// declaration document at all.
    Absent,
    /// The project declared a recipe this platform implements.
    Declared(ProjectKind),
    /// The project carries a declaration the platform cannot read a kind out
    /// of. `reason` says which way it failed and names the accepted set where
    /// that is the failure.
    Unusable { reason: String },
}

/// Which of the three states the project's declaration is in.
///
/// `document` is the bytes of [`PROJECT_KIND_DOCUMENT`], or `None` when the
/// project tree does not carry it — the caller owns locating it, this owns
/// reading it, so there is one place where "could not be read" is decided.
///
/// Nothing declared and something undeclared are different failures and are
/// kept apart here: a document that is not JSON, or whose `kind` is not a
/// string, or is empty, or names a recipe that does not exist, is `Unusable`
/// rather than `Absent`. Reading any of them as "declared nothing" would let a
/// project land as the built-in while its own document says otherwise, and a
/// declaration that cannot be read is not a declaration that was not made.
pub fn project_kind_declaration(document: Option<&str>) -> ProjectKindDeclaration {
    let Some(text) = document else {
        return ProjectKindDeclaration::Absent;
    };
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(e) => {
            return ProjectKindDeclaration::Unusable {
                reason: format!("`{PROJECT_KIND_DOCUMENT}` is not JSON: {e}"),
            }
        }
    };
    let Some(object) = value.as_object() else {
        return ProjectKindDeclaration::Unusable {
            reason: format!("`{PROJECT_KIND_DOCUMENT}` is not a JSON object"),
        };
    };
    let Some(declared) = object.get(PROJECT_KIND_KEY) else {
        return ProjectKindDeclaration::Absent;
    };
    let Some(name) = declared.as_str() else {
        return ProjectKindDeclaration::Unusable {
            reason: format!("`{PROJECT_KIND_KEY}` is not a string"),
        };
    };
    let name = name.trim();
    if name.is_empty() {
        return ProjectKindDeclaration::Unusable {
            reason: format!("`{PROJECT_KIND_KEY}` is declared but empty"),
        };
    }
    match name.parse::<ProjectKind>() {
        Ok(kind) => ProjectKindDeclaration::Declared(kind),
        Err(e) => ProjectKindDeclaration::Unusable {
            reason: e.to_string(),
        },
    }
}

/// Which kind a dispatch is to use, and where that kind came from.
///
/// The source is what keeps the fallback honest: a dispatch that used the
/// built-in because the project declared nothing and a dispatch that used the
/// built-in because the project declared it are the same recipe and different
/// facts, and only the second is the project having chosen.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProjectKindSource {
    /// The project's own declaration named it.
    Declared,
    /// The project declared nothing, so the built-in was used.
    BuiltIn,
}

impl ProjectKindSource {
    /// Every source, so a reader seeds the whole axis.
    pub const ALL: &'static [ProjectKindSource] = &[Self::Declared, Self::BuiltIn];

    /// The wire form, for label values and records.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Declared => "declared",
            Self::BuiltIn => "builtin",
        }
    }
}

impl fmt::Display for ProjectKindSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ProjectKindDeclaration {
    /// The kind to dispatch to and where it came from, or why this project's
    /// declaration cannot be dispatched on at all.
    ///
    /// The single place the three states become "carry on" or "refuse", so no
    /// caller can collapse two of them — in particular so that `Absent` cannot
    /// be produced by the error path.
    pub fn resolved(&self) -> Result<(ProjectKind, ProjectKindSource), &str> {
        match self {
            Self::Declared(kind) => Ok((*kind, ProjectKindSource::Declared)),
            Self::Absent => Ok((BUILT_IN_PROJECT_KIND, ProjectKindSource::BuiltIn)),
            Self::Unusable { reason } => Err(reason.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The closed set is what the declaration is judged against, so a member
    /// that cannot be written or cannot be read back would make every
    /// declaration of it unusable, and one that reads back as another member
    /// would dispatch a project to the wrong recipe.
    #[test]
    fn every_recipe_writes_and_reads_back_as_itself() {
        for kind in ProjectKind::ALL {
            let wire = kind.as_str();
            assert_eq!(
                wire.parse::<ProjectKind>().as_ref(),
                Ok(kind),
                "`{wire}` does not read back as the member that writes it"
            );
            assert_eq!(kind.to_string(), wire);
        }
    }

    /// The built-in must be a member: a fallback outside the set is a recipe
    /// no declaration could ever name, and the two would drift.
    #[test]
    fn the_built_in_is_a_member_of_the_set() {
        assert!(
            ProjectKind::ALL.contains(&BUILT_IN_PROJECT_KIND),
            "the built-in {BUILT_IN_PROJECT_KIND} is not in the closed set"
        );
    }

    /// The failure has to name what would have been accepted. A project told
    /// only "unknown kind" has to read this module to learn the options, and
    /// the one place it can look from the outside is the message.
    #[test]
    fn an_unknown_name_is_refused_and_the_message_names_the_whole_set() {
        let err = "cobol".parse::<ProjectKind>().expect_err("not a recipe");
        assert_eq!(err.declared(), "cobol");
        let message = err.to_string();
        for kind in ProjectKind::ALL {
            assert!(
                message.contains(kind.as_str()),
                "the refusal does not name `{kind}`, so a project cannot tell \
                 what it should have written: {message}"
            );
        }
    }

    #[test]
    fn a_project_with_no_document_declares_nothing() {
        assert_eq!(
            project_kind_declaration(None),
            ProjectKindDeclaration::Absent
        );
    }

    /// The document name and the key are pinned as literals rather than read
    /// off the constants: a fixture built from the constant would agree with
    /// any constant, and the wire form is the one thing a project writes by
    /// hand.
    #[test]
    fn a_document_without_the_key_declares_nothing() {
        assert_eq!(
            project_kind_declaration(Some(r#"{"llm": {"model": "x"}}"#)),
            ProjectKindDeclaration::Absent
        );
    }

    #[test]
    fn a_document_naming_a_recipe_declares_it() {
        assert_eq!(
            project_kind_declaration(Some(r#"{"kind": "rust"}"#)),
            ProjectKindDeclaration::Declared(ProjectKind::Rust)
        );
    }

    /// A project that carries a declaration surface and cannot say what it is
    /// is refused rather than defaulted. Every case below has to come back as
    /// `Unusable`; each of them read as `Absent` is a project landing as the
    /// built-in while its own document says something else.
    #[test]
    fn a_declaration_that_cannot_be_read_is_not_a_declaration_that_was_not_made() {
        for (document, why) in [
            ("not json at all", "not JSON"),
            ("[1, 2, 3]", "not an object"),
            (r#"{"kind": 7}"#, "not a string"),
            (r#"{"kind": ""}"#, "empty"),
            (r#"{"kind": "   "}"#, "blank"),
            (r#"{"kind": "cobol"}"#, "unknown"),
        ] {
            match project_kind_declaration(Some(document)) {
                ProjectKindDeclaration::Unusable { reason } => {
                    assert!(
                        !reason.trim().is_empty(),
                        "`{document}` is unusable ({why}) with no reason"
                    );
                }
                other => panic!("`{document}` ({why}) read as {other:?}"),
            }
        }
    }

    /// The reason is what an operator reads off a refusal, so it has to say
    /// which way it failed and, when the failure is a name, which names work.
    #[test]
    fn an_unknown_declared_name_is_reported_with_the_set() {
        let ProjectKindDeclaration::Unusable { reason } =
            project_kind_declaration(Some(r#"{"kind": "cobol"}"#))
        else {
            panic!("an unknown name has to be unusable");
        };
        assert!(
            reason.contains("cobol"),
            "the reason drops the name: {reason}"
        );
        for kind in ProjectKind::ALL {
            assert!(
                reason.contains(kind.as_str()),
                "the reason does not name `{kind}`: {reason}"
            );
        }
    }

    /// The three states have to stay three through the one function that turns
    /// them into a decision.
    #[test]
    fn only_an_unusable_declaration_refuses_and_only_absence_falls_back() {
        assert_eq!(
            ProjectKindDeclaration::Declared(ProjectKind::Rust).resolved(),
            Ok((ProjectKind::Rust, ProjectKindSource::Declared))
        );
        assert_eq!(
            ProjectKindDeclaration::Absent.resolved(),
            Ok((BUILT_IN_PROJECT_KIND, ProjectKindSource::BuiltIn))
        );
        assert_eq!(
            ProjectKindDeclaration::Unusable {
                reason: "why".to_string()
            }
            .resolved(),
            Err("why")
        );
    }

    /// Both sources reach the wire, and a reader seeds from this list: a member
    /// missing from it is a cell that stays absent forever, which reads as
    /// "never happened" rather than "never fired".
    #[test]
    fn both_sources_write_distinct_non_empty_cells() {
        let cells: Vec<&str> = ProjectKindSource::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(cells.len(), 2);
        assert!(cells.iter().all(|c| !c.is_empty()));
        assert_ne!(cells[0], cells[1]);
    }
}

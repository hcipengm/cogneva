//! The spend-verdict roster is a closed set over two vocabularies, and this is
//! its census.
//!
//! The verdict face judges a cell `(call site × window)` and can only judge the
//! call sites someone declared. That makes two censuses necessary, over the two
//! vocabularies the roster sits between:
//!
//! - **the actor labels** the gateway admits (`cog_gateway::actor_labels`): a
//!   new producer arriving without a roster entry would spend tokens nobody
//!   judges, and its spend would look exactly like a site someone decided to
//!   leave alone. Every admitted value is therefore either registered (its
//!   silence is a defect) or waived with a reason.
//! - **the stages the skipping call sites pass** (`cog_collaboration`): the
//!   roster joins a site to its savings by naming the same stage string the
//!   recording call site names. A stage that is recorded under a name no cell
//!   reads is a saving that lands on the realtime series and never in the
//!   account at all — the one direction this whole face exists to close, and
//!   the one a silent rename would open. So the literals the call sites pass
//!   are enumerated here too, and a new one has to be registered or declared
//!   unread.
//!
//! A stage named through `cog_core::roles` is resolved from that declaration
//! before it is counted, because the call sites spell the role once and read it
//! here, and a census that only understood string literals would go on passing
//! while measuring nothing.
//!
//! What it cannot judge, in the open: a stage passed as a variable rather than
//! a literal is invisible to a text census, and so is a call site that reaches
//! `record_self_review_skip` with a name built at run time. Both are readable
//! only at run time, as the loop's own warning about an undeclared stage, and
//! the reader for that is the log rather than this test. Same division as the
//! actor-label census: the text bounds the vocabulary, the runtime reading
//! shows the omissions.

use std::collections::BTreeSet;
use std::path::PathBuf;

use cog_gateway::actor_labels::{ACTOR_LABELS, ACTOR_LABEL_PREFIXES, UNKNOWN_ACTOR};
use cog_observability::spend_verdict::{
    skip_stage_is_declared, skip_stage_is_read, REGISTERED_SITES, UNREAD_SKIP_STAGES,
    UNREGISTERED_SITES,
};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The two entry points a skipping call site goes through, and which argument
/// of the call is the stage.
///
/// The stage is the *first* argument of the helper a stage calls for one
/// decision, and the *fourth* of the shared review helper — the difference is
/// what the stage means in each: one names the stage whose calls were saved,
/// the other names the stage whose output is being reviewed.
const STAGE_ARGUMENTS: &[(&str, usize)] = &[
    ("note_self_review_skip(", 0),
    ("crate::actors::maybe_self_review(", 3),
];

/// A roster entry for an actor is one of three shapes, and only these.
fn roster_covers(actor: &str) -> bool {
    if REGISTERED_SITES.iter().any(|site| site.actor == actor) {
        return true;
    }
    // Waived entries are written either as the exact value or as the family
    // they belong to (`agent:*`), which is how a configured role that is not
    // one of the named stages is covered without enumerating configuration.
    UNREGISTERED_SITES.iter().any(|site| {
        site.actor == actor
            || site
                .actor
                .strip_suffix('*')
                .is_some_and(|prefix| actor.starts_with(prefix))
    })
}

/// Every actor the gateway admits has a decision: registered, or waived with a
/// reason. Nothing in between, and nothing twice.
#[test]
fn every_actor_the_gateway_admits_is_either_registered_or_waived() {
    let mut undecided = Vec::new();
    for actor in ACTOR_LABELS.iter().copied().chain([UNKNOWN_ACTOR]) {
        if !roster_covers(actor) {
            undecided.push(actor);
        }
    }
    assert!(
        undecided.is_empty(),
        "the gateway admits these actors and the roster decides nothing about them: {undecided:?}\n\
         Register each one whose silence should be a defect, or waive it with the \
         reason its silence carries no verdict."
    );

    let mut seen = BTreeSet::new();
    for site in REGISTERED_SITES {
        assert!(
            seen.insert(site.actor),
            "{} is registered twice",
            site.actor
        );
        assert!(!site.why.trim().is_empty(), "{}: no reason", site.actor);
        assert!(
            legal_actor_value(site.actor),
            "{} is registered but is not a value the gateway admits",
            site.actor
        );
    }
    let mut waived = BTreeSet::new();
    for site in UNREGISTERED_SITES {
        assert!(waived.insert(site.actor), "{} is waived twice", site.actor);
        assert!(!site.why.trim().is_empty(), "{}: no reason", site.actor);
        // A waived entry is either a legal value or a family of them.
        assert!(
            site.actor.ends_with('*') || legal_actor_value(site.actor),
            "{} is waived but is neither an admitted value nor a family",
            site.actor
        );
    }
    assert!(
        REGISTERED_SITES
            .iter()
            .all(|site| !waived.contains(site.actor)),
        "an actor is both registered and waived, so its silence has two readings"
    );
}

/// A concrete actor value or a role family, per the gateway's own vocabulary.
fn legal_actor_value(actor: &str) -> bool {
    ACTOR_LABELS.contains(&actor)
        || actor == UNKNOWN_ACTOR
        || ACTOR_LABEL_PREFIXES
            .iter()
            .any(|prefix| actor.starts_with(prefix) && actor.len() > prefix.len())
}

/// A site's savings are joined to its spend by one string, and the roster has
/// to name it the way the ledger names the site.
///
/// The self-review family is `self_review:{role}`, so the stage whose output is
/// reviewed and the reviewer's role have to be the same word for the join to
/// hold. A roster that paired `self_review:planner` with the stage
/// `"moderator"` would attribute one stage's savings to another's spend, and
/// every cell would still read as a plausible verdict.
#[test]
fn a_registered_review_site_names_its_stage_in_its_own_actor() {
    for site in REGISTERED_SITES {
        match site.skip_stage {
            Some(stage) => {
                assert_eq!(
                    site.actor,
                    format!("self_review:{stage}"),
                    "{} names the stage {stage:?}, which its actor does not carry",
                    site.actor
                );
            }
            None => assert!(
                !site.actor.starts_with("self_review:"),
                "{} is a review site with no stage, so its savings can never join",
                site.actor
            ),
        }
    }
}

/// Every stage a call site records is read by a registered site or declared
/// unread — exactly, so a new recording site forces a decision here.
#[test]
fn every_stage_a_call_site_records_is_read_or_declared_unread() {
    let recorded = stages_recorded_in_source(&repo_root());
    let declared: BTreeSet<String> = REGISTERED_SITES
        .iter()
        .filter_map(|site| site.skip_stage.map(str::to_string))
        .chain(UNREAD_SKIP_STAGES.iter().map(|u| u.stage.to_string()))
        .collect();
    let undeclared: Vec<&String> = recorded.difference(&declared).collect();
    assert!(
        undeclared.is_empty(),
        "these stages are recorded and no list reads them: {undeclared:?}\n\
         Register the site whose spend they save, or declare the stage unread \
         with the reason nothing joins it."
    );
    let orphaned: Vec<&String> = declared.difference(&recorded).collect();
    assert!(
        orphaned.is_empty(),
        "these stages are declared and no call site records them: {orphaned:?}\n\
         A declaration that outlives its call site names a saving nothing can make."
    );
    for stage in &declared {
        assert!(
            skip_stage_is_declared(stage),
            "{stage} is enumerated here but the loop's own guard does not know it"
        );
    }
    assert!(
        UNREAD_SKIP_STAGES
            .iter()
            .all(|u| !skip_stage_is_read(u.stage)),
        "a stage declared unread is read by a registered site now; drop the entry"
    );
}

/// The stage the collaboration call sites name, read out of the source. A
/// variable is skipped rather than guessed at; see the module doc.
fn stages_recorded_in_source(root: &std::path::Path) -> BTreeSet<String> {
    let mut stages = BTreeSet::new();
    let roles = role_constants(root);
    let actors_dir = root.join("crates/cog-collaboration/src/actors");
    let Ok(entries) = std::fs::read_dir(&actors_dir) else {
        panic!("{} is not readable", actors_dir.display());
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("actor source is readable");
        for (call, index) in STAGE_ARGUMENTS {
            for args in call_arguments(&text, call) {
                let Some(arg) = args.get(*index) else {
                    continue;
                };
                if let Some(lit) = arg.strip_prefix('"').and_then(|a| a.split('"').next()) {
                    stages.insert(lit.to_string());
                } else if let Some(name) = role_constant_name(arg) {
                    // A role is spelled as a constant since the names moved to
                    // one declaration. Resolving it rather than skipping it is
                    // what keeps this a census: the five production call sites
                    // all pass a constant now, so skipping them would leave the
                    // set empty and every undeclared stage invisible.
                    let value = roles.get(name).unwrap_or_else(|| {
                        panic!(
                            "the call sites name cog_core::roles::{name}, which \
                             crates/cog-core/src/roles.rs does not declare in the shape \
                             this census reads -- fix the read rather than the call site"
                        )
                    });
                    stages.insert(value.clone());
                }
            }
        }
    }
    assert!(
        !stages.is_empty(),
        "the census found no stage literal at all, so it is reading the wrong shape \
         rather than the tree being empty"
    );
    stages
}

/// Every role `cog_core::roles` declares, by name.
///
/// Read from the declaration rather than restated here: a second list would be
/// the fourth home this module's own doc says not to keep, and it would answer
/// "what the roles are" with what someone typed in a test.
fn role_constants(root: &std::path::Path) -> std::collections::BTreeMap<String, String> {
    let path = root.join("crates/cog-core/src/roles.rs");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is not readable: {e}", path.display()));
    let mut roles = std::collections::BTreeMap::new();
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix("pub const ") else {
            continue;
        };
        let Some((name, value)) = rest.split_once(':') else {
            continue;
        };
        let Some(value) = value.split_once('=').map(|(_, v)| v.trim()) else {
            continue;
        };
        if let Some(lit) = value
            .strip_suffix(';')
            .and_then(|v| v.trim().strip_prefix('"'))
            .and_then(|v| v.split('"').next())
        {
            roles.insert(name.trim().to_string(), lit.to_string());
        }
    }
    assert!(
        !roles.is_empty(),
        "{} declares no `pub const NAME: &str = \"...\"`, so this census would \
         resolve no stage at all",
        path.display()
    );
    roles
}

/// The role a stage argument names, when it names one by constant.
///
/// Both spellings the call sites use: the fully qualified path, and the module
/// path a `use` would let them write.
fn role_constant_name(arg: &str) -> Option<&str> {
    arg.strip_prefix("cog_core::roles::")
        .or_else(|| arg.strip_prefix("roles::"))
        .map(str::trim)
}

/// The top-level comma-separated arguments of every call to `call` in `text`.
fn call_arguments(text: &str, call: &str) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find(call) {
        rest = &rest[at + call.len()..];
        // `call` carries its opening paren, so `rest` begins inside the
        // argument list and the scan looks for the `)` that closes it.
        let Some(close) = argument_list_end(rest) else {
            continue;
        };
        let args: Vec<String> = split_top_level(&rest[..close])
            .into_iter()
            .map(|a| a.trim().to_string())
            .collect();
        out.push(args);
        rest = &rest[close..];
    }
    out
}

/// The offset, inside `rest`, of the `)` that closes the argument list this
/// slice starts in — the slice is taken from just after the opening paren.
fn argument_list_end(rest: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, c) in rest.char_indices() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '(' | '[' | '{' => depth += 1,
            ')' if depth == 0 => return Some(i),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    None
}

/// An argument list split on the commas that are not inside a call or a string.
fn split_top_level(args: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut start = 0usize;
    for (i, c) in args.char_indices() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&args[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&args[start..]);
    parts
}

/// The reverse control: a reader that cannot tell a covered actor from an
/// uncovered one would pass the census above by never complaining.
#[test]
fn the_census_reports_an_actor_no_roster_entry_covers() {
    assert!(!roster_covers("a-label-nobody-declared"));
    // ...and does not complain about one that is covered, so it is not merely
    // always red.
    assert!(roster_covers("agent:planner"));
    assert!(
        roster_covers("agent:some-configured-role"),
        "the family waives it"
    );
    assert!(
        roster_covers(UNKNOWN_ACTOR),
        "the unknown cell is a decision"
    );
}

/// The same control for the stage census: the shape the call sites actually
/// use is the shape the reader recognises.
#[test]
fn the_stage_reader_recognises_the_call_shape() {
    let sample = r#"
        crate::actors::note_self_review_skip(
            "planner",
            crate::observable::SELF_REVIEW_SKIP_DISABLED,
        )
    "#;
    let found = call_arguments(sample, "note_self_review_skip(");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0][0], "\"planner\"");

    // And a multi-line review call, where the stage is the fourth argument
    // after two nested calls and a reference.
    let review = r#"
        crate::actors::maybe_self_review(
            self.agent.as_ref(),
            &self.self_review,
            &output_str,
            "moderator",
            review_basis,
        )
    "#;
    let found = call_arguments(review, "crate::actors::maybe_self_review(");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0][3], "\"moderator\"");
}

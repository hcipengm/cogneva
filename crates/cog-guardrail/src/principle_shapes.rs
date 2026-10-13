//! The sentence shapes this project's principles forbid, in one place.
//!
//! Every shape below is a form the project's own working memory has refused at
//! least once, and the set is data rather than prose for a reason: "how much of
//! what we hold is actually checkable" has to be a count anyone can re-run, and
//! a paragraph saying a check covers the principles reads the same whether it
//! covers six or six hundred.
//!
//! One definition, two readers. The host-side hook that gates a session and the
//! in-process judge that gates an agent's tool call are the same judgement, so a
//! shape added to one and not the other would silently narrow whichever still
//! ran. The host reads the literals out of this file rather than keeping its own
//! copy, which is why the layout below is an interchange format as much as it is
//! Rust: a single-line shape is `pub const NAME: &str = r"…";`, an array is one
//! element per line between `&[` and `];`, so a line reader can take the file
//! apart without a Rust parser. A shape written across several lines, or built
//! by `concat!`, would be invisible to the host and the host would quietly check
//! one shape fewer.
//!
//! The patterns are POSIX extended regexes: the host hands them to `grep -E`, the
//! in-process judge hands them to `regex`. Everything below is a form both
//! dialects accept and read the same way, which is what makes "the same
//! definition" true rather than nominal.

/// Deferral shapes: a person made the object of a decision -- waited on, asked,
/// told to decide.
///
/// The axis is the person as the *object*, not the person as a word. An earlier
/// list anchored most of its entries on the word for the owner and so caught the
/// wording of the last correction while missing the next one.
pub const DEFERRAL_SHAPES: &[&str] = &[
    r"等[^，。；、]{0,10}(你|您|用户)",
    r"(需要|要|请|由|交给|交由|留给|听)[^，。；、]{0,6}(你|您)",
    r"归(你|您|用户)",
    r"用户(拍|定|判|决定|确认|批准)",
    r"归.*属主",
    r"属主(拍|定|判)",
    r"不由我拍",
    r"不要自拍",
    r"不是.*的自决项",
];

/// Lines that merely name the two frozen ledgers, or that quote the rule in
/// order to strike it. Kept apart from the shapes so a reader can tell "handed
/// over" from "talking about handing over", and so the exemptions stay visible
/// rather than becoming a silently narrower search.
pub const DEFERRAL_EXEMPT: &str = r"待拍板清单|任务树跟踪表|\.md";

/// A line that quotes the deferral rule in order to strike it is not a deferral.
pub const DEFERRAL_STRIKING: &str = r"作废|不许|不复述|否掉|不存在「|划掉|反面锚";

/// Circled numerals, forbidden outright -- quoting included.
///
/// One literal per element, matched by equality rather than as a bracket
/// expression. A bracket expression holding multibyte characters is a set of
/// *bytes*, and since a circled numeral shares its leading bytes with ordinary
/// CJK text a class like `[①-⑳]` would match a great many characters that are
/// not circled numerals at all -- a gate that found the forbidden thing
/// everywhere instead of nowhere.
pub const CIRCLED_CHARS: &[char] = &[
    '①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩', '⑪', '⑫', '⑬', '⑭', '⑮', '⑯', '⑰', '⑱', '⑲',
    '⑳', '㈠', '㈡', '㈢', '㈣', '㈤', '㈥', '㈦', '㈧', '㈨', '㈩',
];

/// A sweeping `git add` folds another writer's work into this commit, which
/// makes "who changed this" unanswerable. The tree is written by two sessions at
/// once, so a narrow add is a discipline rather than neatness.
pub const NARROW_ADD_RE: &str = r"git[[:space:]]+add[[:space:]]+(-A|--all|\.)([[:space:]]|$)";

/// A destructive git command is allowed only when the owner asked for that exact
/// action.
pub const DESTRUCTIVE_GIT_RE: &str = r"reset[[:space:]]+--hard|push[[:space:]].*(--force|--force-with-lease|[[:space:]]-f)([[:space:]]|$)|branch[[:space:]]+-D|clean[[:space:]]+-[a-z]*f|checkout[[:space:]]+--[[:space:]]";

/// An AI-attribution trailer is forbidden in every commit, including this
/// repository's. A habit of emitting it is not a lesser violation for carrying a
/// placeholder name: the trailer's presence is what the rule forbids.
pub const AI_TRAILER_RE: &str = r"^[[:space:]]*Co-[Aa]uthored-[Bb]y:|^[[:space:]]*Generated with .*[Cc]laude|noreply@anthropic\.com";

/// A frozen document is append-only by new dated document, so editing one in
/// place is refused.
pub const FROZEN_DOC_RE: &str = r"internal-docs/.*\.md$";

/// The shape-bearing constants, keyed by the names the host's own registry uses.
///
/// The host maps a name to the document that states the rule; the tree owns the
/// name-and-pattern half so the two sides cannot drift. A name here that no host
/// line keys is inert, and a host line whose name is absent here is a shape the
/// host silently lost -- which is why the guard's test pins both directions.
pub const ALL_SHAPES: &[(&str, &str)] = &[
    ("NARROW_ADD_RE", NARROW_ADD_RE),
    ("DESTRUCTIVE_GIT_RE", DESTRUCTIVE_GIT_RE),
    ("AI_TRAILER_RE", AI_TRAILER_RE),
    ("FROZEN_DOC_RE", FROZEN_DOC_RE),
];

/// The name of the first shape `text` matches, or `None` when it is clean.
///
/// The name is what a refusal is reported under, so a caller that blocks on this
/// can say which rule it blocked on rather than only that it blocked. This is a
/// net, not a proof: it reads sentence forms, and the exemptions the host applies
/// per line (a frozen ledger merely named, a rule quoted in order to strike it)
/// are the caller's to apply on the text before it reaches here. A pattern that
/// will not compile is a build-time failure in the test below and can never
/// reach a caller; were one to, it is treated as a shape that cannot fire rather
/// than as a match, since blocking on a broken regex would refuse every call.
pub fn first_match(text: &str) -> Option<&'static str> {
    if DEFERRAL_SHAPES.iter().any(|p| matches(p, text)) {
        return Some("DEFERRAL_SHAPES");
    }
    for (name, pattern) in ALL_SHAPES {
        if matches(pattern, text) {
            return Some(name);
        }
    }
    if text.chars().any(|c| CIRCLED_CHARS.contains(&c)) {
        return Some("CIRCLED_CHARS");
    }
    None
}

/// Whether `text` names a frozen ledger, or quotes the deferral rule to strike
/// it, so the deferral shapes must not fire on the line carrying it.
pub fn deferral_exempt(line: &str) -> bool {
    matches(DEFERRAL_EXEMPT, line) || matches(DEFERRAL_STRIKING, line)
}

fn matches(pattern: &str, text: &str) -> bool {
    regex::Regex::new(pattern).is_ok_and(|re| re.is_match(text))
}

/// A guardrail that refuses a tool call whose name or arguments carry a shape a
/// principle forbids.
///
/// This is the reader that makes the table a live judgement rather than data
/// nothing consults: the same definition the host hook reads, applied to an
/// agent's tool call inside the process. It judges the call's own text, so a
/// refusal names the shape in `rule` and is attributable to a rule rather than
/// to "the guardrail", which is the difference between a count that can be acted
/// on and one that cannot.
///
/// Only the tool call is judged. Input and output are text the guardrail sees
/// before it is a commit, a command or a file, and the shapes are about those
/// artefacts, not about a sentence that names them.
pub struct PrincipleShapeGuard;

#[async_trait::async_trait]
impl cog_core::Guardrail for PrincipleShapeGuard {
    async fn check_input(&self, _messages: &[cog_core::Message]) -> cog_core::GuardResult {
        cog_core::GuardResult::Pass
    }

    async fn check_output(&self, _response: &str) -> cog_core::GuardResult {
        cog_core::GuardResult::Pass
    }

    async fn check_tool_call(&self, tool: &cog_core::ToolCall) -> cog_core::GuardResult {
        let mut subject = String::from(tool.name.as_str());
        subject.push(' ');
        subject.push_str(&tool.arguments.to_string());
        match first_match(&subject) {
            Some(shape) => cog_core::GuardResult::Block {
                reason: format!("工具调用带着形状表禁止的一项（{shape}）"),
                rule: format!("principle_shapes:{shape}"),
            },
            None => cog_core::GuardResult::Pass,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every pattern in the table has to compile in the dialect the in-process
    /// judge uses. The host compiles with `grep -E`; a pattern this crate cannot
    /// compile would leave the two readers disagreeing about what the one
    /// definition says, with the host still refusing and the judge silently
    /// passing.
    #[test]
    fn every_shape_compiles_under_the_regex_crate() {
        for pattern in DEFERRAL_SHAPES {
            regex::Regex::new(pattern)
                .unwrap_or_else(|e| panic!("deferral shape {pattern:?} does not compile: {e}"));
        }
        for (name, pattern) in ALL_SHAPES {
            regex::Regex::new(pattern)
                .unwrap_or_else(|e| panic!("shape {name} ({pattern:?}) does not compile: {e}"));
        }
        for pattern in [DEFERRAL_EXEMPT, DEFERRAL_STRIKING] {
            regex::Regex::new(pattern)
                .unwrap_or_else(|e| panic!("pattern {pattern:?} does not compile: {e}"));
        }
    }

    /// The names the host keys its registry by are exactly the names this table
    /// publishes. The host line the tree does not name is a shape the host lost
    /// silently, which is the failure the single definition exists to prevent.
    #[test]
    fn the_published_names_are_the_hosts_own_keys() {
        let names: Vec<&str> = ALL_SHAPES.iter().map(|(n, _)| *n).collect();
        for expected in [
            "NARROW_ADD_RE",
            "DESTRUCTIVE_GIT_RE",
            "AI_TRAILER_RE",
            "FROZEN_DOC_RE",
        ] {
            assert!(
                names.contains(&expected),
                "the host keys a shape {expected} that this table does not publish"
            );
        }
    }

    /// The layout the header promises: every pattern sits complete on one line,
    /// so a reader that walks the file line by line still finds it.
    ///
    /// The host reads this file whole, so *today* a wrapped literal would still
    /// be found. This test is not for today's reader; it is for the promise the
    /// header makes -- "a line reader can take the file apart without a Rust
    /// parser" -- and for the case where a pattern is written across lines or
    /// built by `concat!`. Either of those is invisible to any reader that is not
    /// the compiler, and the shape it hides is one the host silently stops
    /// checking, with the count and every other reading still green. The
    /// compiler cannot catch it because the code compiles perfectly; only asking
    /// the file about its own text can.
    #[test]
    fn every_pattern_sits_complete_on_one_line_of_this_file() {
        let source = include_str!("principle_shapes.rs");
        let mut patterns: Vec<&str> = DEFERRAL_SHAPES.to_vec();
        for (_, pattern) in ALL_SHAPES {
            patterns.push(pattern);
        }
        patterns.push(DEFERRAL_EXEMPT);
        patterns.push(DEFERRAL_STRIKING);
        for pattern in patterns {
            assert!(
                source.lines().any(|line| line.contains(pattern)),
                "the pattern {pattern:?} is not written complete on any single line, so a \
                 reader that walks the file line by line would miss it and the host would \
                 quietly check one shape fewer"
            );
        }
    }

    #[test]
    fn each_shape_still_fires_on_a_form_it_was_written_for() {
        assert_eq!(
            first_match("git add -A ."),
            Some("NARROW_ADD_RE"),
            "a sweeping add is what the shape is for"
        );
        assert_eq!(
            first_match("git push --force origin main"),
            Some("DESTRUCTIVE_GIT_RE")
        );
        assert_eq!(
            first_match("Co-authored-by: someone"),
            Some("AI_TRAILER_RE")
        );
        assert_eq!(first_match("a ① b"), Some("CIRCLED_CHARS"));
    }

    /// The negative half, said out loud: the shapes are nets, and a net that
    /// fires on clean text is uninstalled and then catches nothing.
    #[test]
    fn clean_text_matches_nothing() {
        assert_eq!(first_match("git add crates/x/src/y.rs"), None);
        assert_eq!(first_match("git commit -m 'fix a bug'"), None);
        assert_eq!(first_match("see item one first"), None);
    }

    /// The exemption covers exactly the two ways a line can mention the deferral
    /// rule without being a deferral, and leaves an act unexempted.
    #[test]
    fn the_deferral_exemptions_are_narrow() {
        assert!(deferral_exempt("见 待拍板清单"));
        assert!(deferral_exempt("这条规则作废，不复述"));
        assert!(
            !deferral_exempt("归属主定。"),
            "an act has to stay unexempted or the exemption eats the rule"
        );
    }

    /// The guard the cluster actually runs: a tool call carrying a forbidden
    /// shape is refused and the refusal names which shape, and a clean call
    /// passes. Without this the table would be data no path consults.
    #[tokio::test]
    async fn the_guard_refuses_by_shape_and_names_it() {
        use cog_core::Guardrail;

        let guard = PrincipleShapeGuard;
        let blocked = cog_core::ToolCall {
            id: "1".into(),
            name: "bash".into(),
            arguments: serde_json::json!({"command": "git add -A ."}),
        };
        match guard.check_tool_call(&blocked).await {
            cog_core::GuardResult::Block { rule, .. } => {
                assert_eq!(rule, "principle_shapes:NARROW_ADD_RE");
            }
            other => panic!("a sweeping add must be refused, got {other:?}"),
        }

        let clean = cog_core::ToolCall {
            id: "2".into(),
            name: "bash".into(),
            arguments: serde_json::json!({"command": "git add crates/x/src/y.rs"}),
        };
        assert_eq!(
            guard.check_tool_call(&clean).await,
            cog_core::GuardResult::Pass
        );
    }
}

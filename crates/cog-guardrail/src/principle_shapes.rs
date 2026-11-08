//! principle: feedback_meta_autonomy_decide_from_principles
//! principle: feedback_shared_tree_add_only_your_own_paths
//! principle: feedback_no_claude_attribution
//! principle: feedback_an_extracted_document_is_frozen_new_rounds_get_a_new_dated_doc
//! principle: feedback_numbering_and_plain_language
//!
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
    // 否定不是交接. `不要拿你以为的维度` hands nothing over -- it strikes a
    // premise -- and the bare 要 branch read it as a handover. Excluding one
    // character in front is narrower than a list of negations that would keep
    // growing with the corpus; 要 keeps matching after 需, so `需要你确认` is
    // still a deferral. The guard now covers 请 and 由 too, which were bare
    // branches: `出现不由你决定` states what the agent cannot decide, the
    // opposite of handing the decision over, and it was refused as one.
    r"((^|[^不需])要|(^|[^不])需要|(^|[^不])请|(^|[^不])由|交给|交由|留给|听)[^，。；、]{0,6}(你|您)",
    // 归属不是交接. `工作树归你一个人用` says whose tree it is, not whose decision
    // it is, and the bare branch refused a legitimate note about worktree
    // ownership. The deferral sense always names the act -- `归你定`, `归你拍`
    // -- so the branch requires one; the tree spells the bare form only inside
    // quotes, where it is already exempt.
    r"归(你|您|用户)(拍|定|决|裁)",
    // 一条记录不是一次交接. `（经用户确认）` says who decided something that is
    // already decided; on a decision record that shape is the norm rather than
    // the exception. Excluding 经 and 已 drops the record and keeps `由用户拍板`.
    r"(^|[^已经])用户(拍|定|判|决定|确认|批准)",
    // The gap stays inside one clause. `.*` reached across a full stop:
    // `归因分析的目标` followed by `属主 2026-10-31 提出…` read as handing a
    // decision to the owner, and that sentence is attribution, a noun.
    r"归[^，。；、]{0,4}属主",
    // Three branches rather than one alternation, because the compounds that
    // made this match nouns differ per verb. `属主判据` and `属主判定` name the
    // mechanism -- the owner criterion, the owner determination -- and a bare
    // three-character alternation reads them as handing the decision over. The
    // tree measured 183 matches for the bare form; 78 of them are those two
    // compounds and every one of the 78 is a noun. `属主拍` takes no tail: 拍定
    // and 拍板 are both the act, so narrowing that branch would drop the
    // handover to keep the noun. The cost is the verb reading of 属主判定, which
    // no pattern can separate from the noun; the tree holds no occurrence of it.
    r"属主拍",
    r"属主定($|[^义])",
    r"属主判($|[^据定])",
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
///
/// What a sweep looks like was measured rather than guessed: 4175 `git add`
/// invocations across the session transcripts, and the forms that stage more
/// than the files they name are three. `-A` / `--all` / `.`, which the shape was
/// first written for. A directory written with a trailing slash -- `git add
/// memory/`, six times, and `projects/cogneva/specs/`, `crates/cogneva/debt/`,
/// `projects/cogneva/specs/ledger/inputs/` once each -- which the first shape
/// missed and which is the form that actually blocked two sessions' commits. And
/// a broad target reached through a flag, `git add -N .`, fourteen times.
///
/// The trailing slash separates the directory case, which is safe to add, from
/// the bare name, which is not: `memory/` can only be a directory, while
/// `memory` may be a directory or a file with no extension, and a shape that
/// cannot tell them apart would refuse `git add LICENSE`. So the slash is in and
/// the bare name stays out -- the half this pattern does not check, written down
/// rather than left to look covered. A quoted path is the other uncaught form,
/// and deliberately so: the host strips quoted spans before matching, the same
/// exemption that keeps a rule's own name from tripping it.
///
/// What the flag allowance costs, said plainly: `git add -n .` is a dry run, and
/// a regex cannot separate it from `git add -N .`. The dry-run spelling does not
/// occur in the corpus and intent-to-add of the whole tree does, so refusing a
/// harmless spelling beat leaving the whole tree unwatched.
pub const NARROW_ADD_RE: &str = r"git[[:space:]]+add([[:space:]]+-[^[:space:]]+)*[[:space:]]+(-A|--all|\.|[^[:space:]]*/)([[:space:]]|$)";

/// A destructive git command is allowed only when the owner asked for that exact
/// action.
///
/// The force-push branch carries an argument tail and nothing else: `.*` here
/// crossed command separators, so `git push origin main; rm -f <draft>` -- the
/// ordinary way a session ends a push, since the draft is a scratch message file
/// -- read as a force push, and 38 of the 43 refusals this shape produced in one
/// session were that. The tail stops at `;`, `&`, `|` and a control character --
/// the ways one command ends and the next begins -- so a `-f` belonging to a
/// neighbour is no longer read as belonging to the push. Control characters
/// rather than a literal newline because two engines read this table: the host's
/// `grep` sees one line at a time and is never handed a newline, while the
/// in-process judge sees the whole command and would let a bracket expression
/// walk over one. The cost is that a push whose words are separated by tabs is
/// not read as a force push either; nobody spells it that way, and a shape that
/// misses an exotic spelling is cheaper than one that fires on the ordinary one.
///
/// The same rewrite closed a hole: `push[[:space:]].*` consumed the space before
/// the flag, so the `[[:space:]]-f` alternative could never match the short form
/// -- `git push -f origin main`, the spelling people actually use, was not
/// refused while `--force` was.
pub const DESTRUCTIVE_GIT_RE: &str = r"reset[[:space:]]+--hard|push([[:space:]]+[^;&|[:cntrl:]]*)?[[:space:]]+(-f|--force|--force-with-lease)([[:space:]]|$)|branch[[:space:]]+-D|clean[[:space:]]+-[a-z]*f|checkout[[:space:]]+--[[:space:]]";

/// An AI-attribution trailer is forbidden in every commit, including this
/// repository's. A habit of emitting it is not a lesser violation for carrying a
/// placeholder name: the trailer's presence is what the rule forbids.
pub const AI_TRAILER_RE: &str = r"^[[:space:]]*Co-[Aa]uthored-[Bb]y:|^[[:space:]]*Generated with .*[Cc]laude|noreply@anthropic\.com";

/// A commit that names no paths (`git commit` with no `-- <path>` tail) commits
/// whatever is in the index, and the index is shared: two sessions add to the
/// same one, so a bare commit folds the other writer's staged work into this
/// commit and "who changed this" stops being answerable. The narrow add is only
/// half the discipline; the commit has to name the same paths.
///
/// Why the pattern is shaped the way it is: the hazard is the *absence* of a
/// separator, and neither `grep -E` nor the `regex` crate has a lookahead to ask
/// for that, so the absence is read as a positive form instead -- the arguments
/// tail may be built from anything that is not a standalone `--`. A `--flag`
/// (`--amend`, `--no-edit`) is not a separator, so `--` followed by a non-space
/// is allowed; only `--` with a space or the line end after it ends the match.
/// That is what separates `git commit --amend` (still bare, refused) from
/// `git commit -m x -- crates/` (names its paths, allowed).
///
/// The word has to end there. `git commit-tree` is plumbing: it builds a commit
/// object from a tree handed to it and never reads an index, so there is nothing
/// for `-- <paths>` to narrow and the flag is a syntax error rather than a
/// safer spelling. Matching `git[[:space:]]+commit` by prefix read it as the
/// porcelain command and refused work the rule cannot apply to.
pub const BARE_COMMIT_RE: &str =
    r"git[[:space:]]+commit([[:space:]]([^-]|-[^-]|--[^[:space:]])*)?$";

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
    ("BARE_COMMIT_RE", BARE_COMMIT_RE),
    ("FROZEN_DOC_RE", FROZEN_DOC_RE),
];

/// The principle each shape belongs to, keyed by the name a refusal reports.
///
/// A refusal that carries only this table's own rule name tells an operator which
/// internal pattern fired and nothing about what was protected: the count lands
/// on a string only this crate knows, and the account cannot file it against a
/// principle. The second column is the name the account files it under, so the
/// refusal can say it too.
///
/// The values are not free text. Every one but the first entry is a slug the
/// platform's declaration reader accepts, and this file declares each of them in
/// its own header; those header lines are how the account answers "which file
/// goes red" without asking the host. The exception is `DESTRUCTIVE_GIT_RE`: its
/// rule lives in the host's personal rules file, whose account row is keyed
/// `CLAUDE`, and the platform's slug rule (lowercase, digits, underscore) cannot
/// hold that word. Declaring it in the header would make the platform read its
/// own header as an unreadable declaration on every pass, so it is deliberately
/// left out and the test below pins it as the one such entry rather than letting
/// the gap be discovered as a warning nobody expected.
pub const SHAPE_PRINCIPLES: &[(&str, &str)] = &[
    (
        "DEFERRAL_SHAPES",
        "feedback_meta_autonomy_decide_from_principles",
    ),
    (
        "NARROW_ADD_RE",
        "feedback_shared_tree_add_only_your_own_paths",
    ),
    (
        "BARE_COMMIT_RE",
        "feedback_shared_tree_add_only_your_own_paths",
    ),
    ("DESTRUCTIVE_GIT_RE", "CLAUDE"),
    ("AI_TRAILER_RE", "feedback_no_claude_attribution"),
    (
        "FROZEN_DOC_RE",
        "feedback_an_extracted_document_is_frozen_new_rounds_get_a_new_dated_doc",
    ),
    ("CIRCLED_CHARS", "feedback_numbering_and_plain_language"),
];

/// The principle a shape belongs to, if the table names it.
pub fn principle_of(shape: &str) -> Option<&'static str> {
    SHAPE_PRINCIPLES
        .iter()
        .find(|(name, _)| *name == shape)
        .map(|(_, slug)| *slug)
}

/// The name of the first shape `text` matches, or `None` when it is clean.
///
/// The name is what a refusal is reported under, so a caller that blocks on this
/// can say which rule it blocked on rather than only that it blocked. This is a
/// net, not a proof: it reads sentence forms. The host's reader strips quoted
/// spans and exempts two kinds of line before it judges a deferral, and this
/// reader does the same -- the exemptions are part of the definition, not the
/// caller's business, or the two readers disagree about the one table they
/// share. A pattern that will not compile is a build-time failure in the test
/// below and can never reach a caller; were one to, it is treated as a shape
/// that cannot fire rather than as a match, since blocking on a broken regex
/// would refuse every call.
pub fn first_match(text: &str) -> Option<&'static str> {
    first_span(text).map(|(shape, _)| shape)
}

/// The first shape `text` matches, together with the text it matched.
///
/// The same reader as [`first_match`], by construction rather than by agreement:
/// the name is taken from this function, so a refusal and the evidence recorded
/// beside it cannot name different shapes. The matched run is the gate's own
/// words -- the phrase the host hook would have refused -- which is what makes a
/// refusal row replayable long after the call that produced it is gone.
///
/// It is the text the gate *judged*, which for every shape but the AI trailer is
/// the text with quoted interiors removed. A replay has to read it the same way:
/// `git commit -m ''` is what a bare-commit refusal carries when the message it
/// pasted was quoted, and searching the refusal's span in the original call
/// would miss a row that is telling the truth.
///
/// A circled numeral is its own span: the shape is the character, not the
/// sentence around it.
pub fn first_span(text: &str) -> Option<(&'static str, String)> {
    if let Some(span) = deferral_span(text) {
        return Some(("DEFERRAL_SHAPES", span));
    }
    let named = strip_named_spans(text);
    for (name, pattern) in ALL_SHAPES {
        // A literal byte run is judged as written: the host reads a command
        // un-stripped for the trailer, and a trailer inside quotes is still a
        // trailer in the commit it is pasted into. Every other shape reads the
        // stripped text, which is what the host does so that a command merely
        // named in a sentence is not refused.
        let subject = if *name == "AI_TRAILER_RE" {
            text
        } else {
            named.as_str()
        };
        if let Some(found) = compile(pattern).ok().and_then(|re| re.find(subject)) {
            return Some((name, found.as_str().to_string()));
        }
    }
    // Circled numerals are judged on the original text: the owner forbade the
    // character outright, quoting included.
    if let Some(c) = text.chars().find(|c| CIRCLED_CHARS.contains(c)) {
        return Some(("CIRCLED_CHARS", c.to_string()));
    }
    None
}

/// Whether a deferral shape survives the host's two per-line exemptions.
///
/// Judged a line at a time: a line that names a frozen ledger and a line that
/// quotes the rule in order to strike it are not deferrals, while a line beside
/// them is judged on its own. Whole-text matching refused exactly the messages
/// that report this gate broken, because such a message spells the rule out.
/// The patterns are unanchored and `[^…]` runs across newlines, so the host's
/// line-at-a-time reading is also what keeps `等` on one line from being joined
/// to `你` on the next.
fn deferral_match(text: &str) -> bool {
    deferral_span(text).is_some()
}

/// The first deferral phrase on a line the exemptions do not clear.
///
/// The companion to [`deferral_match`] rather than a second reader: the bool is
/// this function's `is_some`, so a line the host would let through can never
/// produce the evidence for a refusal it does not make.
fn deferral_span(text: &str) -> Option<String> {
    let stripped = strip_named_spans(text);
    for line in stripped.lines() {
        if deferral_exempt(line) {
            continue;
        }
        for pattern in DEFERRAL_SHAPES {
            if let Some(found) = compile(pattern).ok().and_then(|re| re.find(line)) {
                return Some(found.as_str().to_string());
            }
        }
    }
    None
}

/// The two shapes the host judges on a finished turn: a decision handed to a
/// person, and a circled numeral.
///
/// Kept apart from `first_match` because the host's finished-turn check is
/// narrower than its tool-call check -- a reply is not a command or a path, so
/// those shapes have nothing to read there. A caller judging a reply uses this,
/// not `first_match`, or it would refuse a reply that merely mentions a file.
pub fn sentence_form_match(text: &str) -> Option<&'static str> {
    if deferral_match(text) {
        return Some("DEFERRAL_SHAPES");
    }
    if text.chars().any(|c| CIRCLED_CHARS.contains(&c)) {
        return Some("CIRCLED_CHARS");
    }
    None
}

/// The quoting pairs a phrase is wrapped in when it is being *named* rather
/// than used, in the order the host strips them.
const NAMED_PAIRS: [(char, char); 6] = [
    ('「', '」'),
    ('『', '』'),
    ('“', '”'),
    ('`', '`'),
    ('"', '"'),
    ('\'', '\''),
];

/// `text` with the interior of every quoted span removed, the pair kept.
///
/// Same six pairs and the same non-greedy reading as the host's stripper: each
/// opening mark pairs with the nearest closing one, a mark with no mate stays
/// as it is, and the pairs are applied one after another to the text the
/// previous pair left. Two readers that strip differently accept different
/// documents while both cite the same table.
pub fn strip_named_spans(text: &str) -> String {
    let mut out = text.to_string();
    for (open, close) in NAMED_PAIRS {
        out = strip_pairs(&out, open, close);
    }
    out
}

fn strip_pairs(text: &str, open: char, close: char) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != open {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        match (i + 1..chars.len()).find(|j| chars[*j] == close) {
            Some(j) => {
                out.push(open);
                out.push(close);
                i = j + 1;
            }
            None => {
                out.push(chars[i]);
                i += 1;
            }
        }
    }
    out
}

/// Whether `text` names a frozen ledger, or quotes the deferral rule to strike
/// it, so the deferral shapes must not fire on the line carrying it.
pub fn deferral_exempt(line: &str) -> bool {
    matches(DEFERRAL_EXEMPT, line) || matches(DEFERRAL_STRIKING, line)
}

/// Whether one pattern matches, in the dialect the two readers agree on.
///
/// The patterns are written for the host's reader, which is `grep -E`: grep
/// matches a line at a time, so `^` and `$` are the ends of a line. The regex
/// crate here matches a whole string at a time, so the same anchors mean the ends
/// of the *string* -- and two shapes (`AI_TRAILER_RE`, `FROZEN_DOC_RE`) are
/// anchored. Compiled without the mode below, those two could only fire on a
/// whole subject that began or ended with the forbidden text, which is almost
/// never, so the in-process judge would pass what the host refused while both
/// read the same table. The `(?m)` mode restores per-line anchors and is what
/// makes "the same definition" true across the two readers rather than nominal.
fn matches(pattern: &str, text: &str) -> bool {
    compile(pattern).is_ok_and(|re| re.is_match(text))
}

/// One pattern, compiled the way every reader here reads it. Kept apart from
/// `matches` so a test can assert a pattern *compiles* rather than only that it
/// failed to block.
fn compile(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&format!("(?m){pattern}"))
}

/// The strings a tool call is made of, one per field the call carries them in.
///
/// Every shape is judged against each of these and any match is a match. Why not
/// the call's JSON as one string, which is the obvious subject: serialising it
/// escapes newlines to the two characters `\n` and puts the whole call on one
/// line, so a shape anchored to a line -- the AI trailer's `^`, a path's `$` --
/// could never fire, and the in-process judge would pass exactly the calls the
/// host's hook refuses. The fields are taken as the strings they are instead: the
/// shell's `command`, an edit's `file_path` and its `content` or `new_string`, a
/// message's `message`. Objects and arrays are walked so a field the caller names
/// differently is still read, and the tool's own name is included because a
/// forbidden tool spelled in the name is a shape too. Only when a call carries no
/// string at all does the serialised form stand in, so a payload shaped unlike
/// any of these is still judged rather than skipped.
fn subjects(tool: &cog_core::ToolCall) -> Vec<String> {
    let mut out = vec![tool.name.clone()];
    collect_strings(&tool.arguments, &mut out);
    if out.len() == 1 {
        out.push(tool.arguments.to_string());
    }
    out
}

/// The path suffix of the file that defines this table.
///
/// A write to the definition is not a write that hands a decision over: the
/// examples the table exists for have to appear in it, so judging the content
/// refuses the only edit that can add or narrow a shape. The host's gate exempts
/// this path for exactly that reason and did so first; the in-process judge had
/// no such exemption, so the platform refused its own definition file while the
/// host accepted it -- the asymmetry that this shared table exists to rule out.
///
/// A suffix rather than a whole path because the working directory differs per
/// caller: a tool call carries whatever the model wrote, absolute or relative.
const SELF_EDIT_PATH_SUFFIX: &str = "cog-guardrail/src/principle_shapes.rs";

/// The path a write call is about, whichever field carries it.
fn written_path(tool: &cog_core::ToolCall) -> Option<&str> {
    ["file_path", "notebook_path"]
        .iter()
        .find_map(|key| tool.arguments.get(*key).and_then(|value| value.as_str()))
}

fn collect_strings(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::String(text) => out.push(text.clone()),
        serde_json::Value::Array(items) => items.iter().for_each(|i| collect_strings(i, out)),
        serde_json::Value::Object(fields) => {
            fields.values().for_each(|f| collect_strings(f, out));
        }
        _ => {}
    }
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
/// A finished reply is judged by the host's own narrower rule (a decision
/// handed to a person, a circled numeral); the shapes about commands and paths
/// are not applied to prose, which would refuse a reply that merely names a
/// file. Incoming messages are not judged: the host does not judge them either,
/// and a message quoting a rule to have it fixed must reach the reader.
pub struct PrincipleShapeGuard {
    /// Where a refusal goes, resolved on first refusal rather than at
    /// construction: the ledger is published by another plugin and this guard
    /// is built into the composite during this one's `init`, so a read there
    /// would be a read whose timing decides whether the account ever hears
    /// about a refusal.
    ledger: Option<cog_core::LateService<dyn cog_core::PrincipleLedgerStore>>,
    /// Set once, at the first refusal the handle could not answer. A guard built
    /// with no handle at all says nothing: that is a caller which never asked to
    /// record. A guard built with one that resolved to nothing is a deployment
    /// whose account is missing, and it says so exactly once.
    unrecorded: std::sync::Once,
}

impl PrincipleShapeGuard {
    /// A guard that refuses without recording. Kept for the callers that hold
    /// no account -- an embedded run, a test -- rather than as a default that
    /// quietly applies to a deployment.
    pub fn new() -> Self {
        Self {
            ledger: None,
            unrecorded: std::sync::Once::new(),
        }
    }

    /// A guard that records every refusal it makes against `ledger`.
    pub fn reporting_to(ledger: cog_core::LateService<dyn cog_core::PrincipleLedgerStore>) -> Self {
        Self {
            ledger: Some(ledger),
            unrecorded: std::sync::Once::new(),
        }
    }

    /// Report one refusal, without letting the report change the verdict.
    ///
    /// The refusal has already been decided by the time this runs: a ledger
    /// that is absent, slow or failing must not turn a blocked call into a
    /// permitted one. So every failure here is a log line, and the block that
    /// the caller returns is the same one it would have returned anyway. What
    /// the log is for: "the gate refused nothing" and "the gate refused and the
    /// account never heard" are identical in every count the ledger serves.
    async fn report(&self, shape: &str, span: &str) {
        let Some(handle) = self.ledger.as_ref() else {
            return;
        };
        let Some(store) = handle.get() else {
            self.unrecorded.call_once(|| {
                tracing::warn!(
                    "the shape gate made a refusal and no principle ledger was published, so \
                     this refusal and every one after it go uncounted; the call was refused \
                     all the same"
                );
            });
            return;
        };
        record_refusal(store.as_ref(), shape, span).await;
    }
}

impl Default for PrincipleShapeGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Write one refusal, and say whether it landed.
///
/// The bool is what a caller can act on; nothing here can. A write that fails is
/// logged with the row's own slug and rule so the loss is at least attributable,
/// and the refusal stands either way -- a store outage turning a blocked call
/// into a permitted one is the one outcome the gate may not have.
async fn record_refusal<S: RefusalLedger + ?Sized>(sink: &S, shape: &str, span: &str) -> bool {
    let event = refusal_for(shape, span);
    match sink.report_refusals(std::slice::from_ref(&event)).await {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(
                slug = %event.slug,
                rule = %event.criterion,
                "a principle-shaped refusal was made and could not be recorded; the call was \
                 refused, but the account will not count it: {e}"
            );
            false
        }
    }
}

/// The one row a refusal reports, built from what the gate saw.
///
/// A free function rather than a method so the shape of the row can be read and
/// tested without a ledger: the slug is the principle the shape table attributes
/// the shape to, the criterion is the shape's own name, and the span is the
/// matched text. A shape the table attributes to no principle is filed under the
/// shape itself rather than dropped -- an unattributed refusal is still a refusal
/// somebody should be able to count.
pub fn refusal_for(shape: &str, span: &str) -> cog_core::PrincipleRefusal {
    cog_core::PrincipleRefusal {
        slug: principle_of(shape).unwrap_or(shape).to_string(),
        refused_at: chrono::Utc::now(),
        source: REFUSAL_SOURCE.to_string(),
        count: 1,
        criterion: shape.to_string(),
        span: span.to_string(),
    }
}

/// What a runtime refusal is attributed to.
///
/// The guard judges a call and a reply, and neither carries the task it belongs
/// to, so the row says "the runtime" and stops there. A source that guessed at a
/// task id would be worse than one that admits it does not know: the count is
/// still attributable to the gate that made it.
pub const REFUSAL_SOURCE: &str = "runtime";

/// The half of the ledger a refusal needs.
///
/// Narrower than [`cog_core::PrincipleLedgerStore`] on purpose: a guard that
/// refuses has evidence for exactly one row, and a port that also carried the
/// reads would make every test of the refusal path stand up a whole account.
/// The blanket impl is what keeps the production wiring honest -- anything that
/// is a ledger is a sink, so a deployment cannot end up with a guard that writes
/// to a second, private path.
#[async_trait::async_trait]
pub trait RefusalLedger: Send + Sync {
    async fn report_refusals(
        &self,
        events: &[cog_core::PrincipleRefusal],
    ) -> cog_core::SFResult<usize>;
}

#[async_trait::async_trait]
impl<T: cog_core::PrincipleLedgerStore + ?Sized> RefusalLedger for T {
    async fn report_refusals(
        &self,
        events: &[cog_core::PrincipleRefusal],
    ) -> cog_core::SFResult<usize> {
        cog_core::PrincipleLedgerStore::report_refusals(self, events).await
    }
}

#[async_trait::async_trait]
impl cog_core::Guardrail for PrincipleShapeGuard {
    async fn check_input(&self, _messages: &[cog_core::Message]) -> cog_core::GuardResult {
        cog_core::GuardResult::Pass
    }

    /// The finished-turn half of the host gate.
    ///
    /// The host refuses a turn whose text hands a decision to a person or uses
    /// a circled numeral, and it refuses it at the same moment the model would
    /// otherwise send it. A platform that only judges tool calls lets a reply
    /// through the host would have stopped, which is the half that reads as
    /// "the gate is ported" while the last word is unguarded. The check is
    /// deliberately narrower than the tool-call one: a reply is not a command
    /// or a path, so those shapes have nothing to read there.
    async fn check_output(&self, response: &str) -> cog_core::GuardResult {
        match sentence_form_match(response) {
            Some(shape) => {
                let span = first_span(response)
                    .map(|(_, span)| span)
                    .unwrap_or_else(|| shape.to_string());
                self.report(shape, &span).await;
                cog_core::GuardResult::Block {
                    reason: match principle_of(shape) {
                        Some(slug) => format!("回复里带着形状表禁止的一项（{shape}，原则 {slug}）"),
                        None => format!("回复里带着形状表禁止的一项（{shape}）"),
                    },
                    rule: format!("principle_shapes:{shape}"),
                }
            }
            None => cog_core::GuardResult::Pass,
        }
    }

    async fn check_tool_call(&self, tool: &cog_core::ToolCall) -> cog_core::GuardResult {
        let judged = match written_path(tool) {
            Some(path) if path.ends_with(SELF_EDIT_PATH_SUFFIX) => vec![path.to_string()],
            _ => subjects(tool),
        };
        let matched = judged.into_iter().find_map(|subject| first_span(&subject));
        match matched {
            Some((shape, span)) => {
                self.report(shape, &span).await;
                cog_core::GuardResult::Block {
                    reason: match principle_of(shape) {
                        Some(slug) => {
                            format!("工具调用带着形状表禁止的一项（{shape}，原则 {slug}）")
                        }
                        None => format!("工具调用带着形状表禁止的一项（{shape}）"),
                    },
                    rule: format!("principle_shapes:{shape}"),
                }
            }
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
            compile(pattern)
                .unwrap_or_else(|e| panic!("deferral shape {pattern:?} does not compile: {e}"));
        }
        for (name, pattern) in ALL_SHAPES {
            compile(pattern)
                .unwrap_or_else(|e| panic!("shape {name} ({pattern:?}) does not compile: {e}"));
        }
        for pattern in [DEFERRAL_EXEMPT, DEFERRAL_STRIKING] {
            compile(pattern)
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
            "BARE_COMMIT_RE",
            "FROZEN_DOC_RE",
        ] {
            assert!(
                names.contains(&expected),
                "the host keys a shape {expected} that this table does not publish"
            );
        }
    }

    /// Every name a refusal can carry belongs to a principle, and the principles
    /// this file declares in its own header are exactly the ones the table names.
    ///
    /// Two answers have to agree for the account to be able to say anything: the
    /// table says which principle a fired shape belongs to, and the header says
    /// which principles this file gates. A shape added with a pattern but no
    /// entry here would be refused under a name the account cannot file; an entry
    /// with no header line would be a principle the platform never reports as
    /// gated by this file. Neither shows up as a failure at run time -- the gate
    /// keeps refusing -- so this test is the only place they can be caught.
    #[test]
    fn every_shape_names_the_principle_it_stands_for() {
        let mut names: Vec<&str> = ALL_SHAPES.iter().map(|(n, _)| *n).collect();
        names.push("DEFERRAL_SHAPES");
        names.push("CIRCLED_CHARS");
        let mut slugs: Vec<&str> = names
            .iter()
            .map(|name| {
                principle_of(name).unwrap_or_else(|| panic!("shape {name} names no principle"))
            })
            .collect();

        let source = include_str!("principle_shapes.rs");
        let declared: Vec<&str> = source
            .lines()
            .filter_map(|line| line.trim_start().strip_prefix("//! principle:"))
            .map(str::trim)
            .collect();

        // `CLAUDE` is the one account key the platform's slug rule cannot hold,
        // so it is deliberately not declared here (see `SHAPE_PRINCIPLES`). It
        // stays in this test rather than out of it: the day that key becomes
        // declarable, this expectation is what says so.
        slugs.sort_unstable();
        slugs.dedup();
        let undeclared: Vec<&str> = slugs
            .iter()
            .copied()
            .filter(|slug| !declared.contains(slug))
            .collect();
        assert_eq!(
            undeclared,
            vec!["CLAUDE"],
            "these principles fire in this file but the header does not declare them"
        );

        let stale: Vec<&str> = declared
            .iter()
            .copied()
            .filter(|slug| !slugs.contains(slug))
            .collect();
        assert!(
            stale.is_empty(),
            "the header declares principles no shape in this table belongs to: {stale:?}"
        );
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
            first_match("git add memory/"),
            Some("NARROW_ADD_RE"),
            "a directory named with a trailing slash stages everything under it, \
             which is the claim `-A` makes and the form that blocked two commits"
        );
        assert_eq!(
            first_match("git add -A memory/"),
            Some("NARROW_ADD_RE"),
            "a flag in front of the directory does not narrow it"
        );
        assert_eq!(
            first_match("git add -- crates/"),
            Some("NARROW_ADD_RE"),
            "the same directory behind a path separator is still the whole tree"
        );
        assert_eq!(
            first_match("git add -N ."),
            Some("NARROW_ADD_RE"),
            "intent-to-add of the whole tree is reached through a flag, and the \
             target is still every file"
        );
        assert_eq!(
            first_match("git push --force origin main"),
            Some("DESTRUCTIVE_GIT_RE")
        );
        // The short flag is the spelling people type, and the tail used to
        // swallow the space in front of it: `--force` was refused while `-f`
        // went through.
        assert_eq!(
            first_match("git push -f origin main"),
            Some("DESTRUCTIVE_GIT_RE")
        );
        assert_eq!(
            first_match("git push origin main -f"),
            Some("DESTRUCTIVE_GIT_RE"),
            "the flag after the remote is the same act"
        );
        assert_eq!(
            first_match("Co-authored-by: someone"),
            Some("AI_TRAILER_RE")
        );
        assert_eq!(first_match("a ① b"), Some("CIRCLED_CHARS"));
        // A commit with no pathspec is the whole hazard; a flag that merely
        // opens with `--` (`--amend`) does not name a path and must still be
        // read as bare rather than excused by the two dashes.
        assert_eq!(
            first_match("git commit -m 'fix a bug'"),
            Some("BARE_COMMIT_RE")
        );
        assert_eq!(
            first_match("git commit --amend --no-edit"),
            Some("BARE_COMMIT_RE"),
            "a long flag is not a path separator, so an amend is still bare"
        );
    }

    /// The negative half, said out loud: the shapes are nets, and a net that
    /// fires on clean text is uninstalled and then catches nothing.
    #[test]
    fn clean_text_matches_nothing() {
        assert_eq!(first_match("git add crates/x/src/y.rs"), None);
        assert_eq!(
            first_match("git add projects/cogneva/specs/ledger.tsv"),
            None,
            "a path that merely contains slashes is still one file, so the \
             directory branch must not fire on the last segment being a file"
        );
        assert_eq!(
            first_match("git add Cargo.lock crates/cog-guardrail/src/lib.rs"),
            None,
            "a list of files is the discipline, not a sweep"
        );
        assert_eq!(
            first_match("git commit -m 'fix a bug' -- crates/x/src/y.rs"),
            None,
            "a commit that names its paths is the discipline this shape is for"
        );
        // One command's flag is not its neighbour's. Both of these were refused
        // for a force push nobody wrote, and the second is how a session ends
        // every push -- the draft it deletes is the message file it just read.
        for nearby in [
            "git push origin main; rm -f /tmp/draft",
            "git push origin main && rm -f /tmp/draft",
            "git push origin main | tail -3; rm -f /tmp/draft",
            "git push origin main\nrm -f /tmp/draft",
        ] {
            assert_eq!(first_match(nearby), None, "no force push in: {nearby}");
        }
        assert_eq!(
            first_match(r#"git commit-tree "$tree" -p HEAD -m "x""#),
            None,
            "plumbing builds a commit from a tree it is given and never reads \
             an index, so there is no pathspec to narrow"
        );
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

    /// 否定与记录不是交接: the two branches that read ordinary prose as a
    /// deferral, pinned beside the acts that must survive the narrowing.
    ///
    /// Measured on the host's own planning and design documents on 2026-11-03:
    /// over 1359 of them four of the five hits were these branches reading a
    /// state name and a decision record. Each branch narrows on a
    /// grammatical fact -- a negation, a tense -- and the acts stay beside the
    /// prose, because narrowing a shape until a corpus is quiet is how a rule
    /// ends up narrower than the principle it stands for.
    #[test]
    fn a_negation_and_a_record_are_not_handovers() {
        for prose in [
            "不要拿你以为的维度去读这一行。",
            "不需要你确认，我自己拍。",
            "**决策人**：Claude Code（经用户确认）",
            "已经用户确认过了。",
        ] {
            assert_eq!(first_match(prose), None, "not a handover: {prose}");
        }
        for act in [
            "需要你确认之后才能继续。",
            "这一步要你拍板。",
            "由用户拍板，我不动。",
            "用户拍板选项 B。",
        ] {
            assert_eq!(
                first_match(act),
                Some("DEFERRAL_SHAPES"),
                "an act must stay caught: {act}"
            );
        }
    }

    /// A phrase inside quotes is being named, not used, and a striking line is
    /// exempt even unquoted -- both are the host's own exemptions, and the
    /// in-process judge used to refuse what the host accepted. That is not a
    /// harmless asymmetry: the text it refused included the reports that
    /// explained the gate, which are the readings that get it fixed.
    #[test]
    fn a_named_phrase_is_not_a_use() {
        for named in [
            r#"原则里写着「归你定」这三个字"#,
            r#"规则原文是 `git add -A .`，这里只是引用"#,
            r#"“这事由你决定”是反例"#,
        ] {
            assert_eq!(
                first_match(named),
                None,
                "a quoted phrase is named: {named}"
            );
        }
        assert_eq!(
            first_match("这条规则作废，不复述：归你定。"),
            None,
            "a line striking the rule is exempt even without quotes"
        );
    }

    /// The exemption is per line and per shape, not a switch that turns the
    /// judge off: a striking line does not cover the line beside it, and
    /// quoting a trailer does not make it not one.
    #[test]
    fn the_exemptions_do_not_spread() {
        assert_eq!(
            first_match("这条规则作废，不复述：归你定。\n归属主定。"),
            Some("DEFERRAL_SHAPES"),
            "the striking exemption covers its own line only"
        );
        assert_eq!(
            first_match("\"a quoted block\nCo-authored-by: someone\""),
            Some("AI_TRAILER_RE"),
            "the trailer is judged as written, quotes or no quotes -- stripping a \
             multi-line quote would delete the line the anchored shape reads"
        );
    }

    /// The three lines the shape table refused while their author was writing
    /// something the principles allow. Each is kept verbatim: they came out of
    /// the refusal ledger, so they are the corpus rather than a fixture written
    /// to agree with the pattern.
    ///
    /// The last two narrow a branch that reaches too far rather than add an
    /// exemption, because the exemption would have to name every phrase that
    /// follows the shape without being one.
    #[test]
    fn a_possessive_a_negation_and_a_noun_are_not_handovers() {
        for own in [
            // 归 + person with no act after it says whose thing it is.
            "工作树归你一个人用时，一次设好（只改这棵工作树，不动共享 config）：",
            // 由 preceded by 不 says what the agent cannot decide.
            "出现不由你决定。",
            // 归因 (attribution) and 属主 in the next clause are two nouns.
            "写给接手界面与归因那一半的会话。属主 2026-10-31 提出七条界面问题 ＋ 归因分析的目标；",
        ] {
            assert_eq!(
                first_match(own),
                None,
                "this line hands nothing over: {own}"
            );
        }
        // The narrowed branches still catch the act they exist for, one line
        // each, so a pattern that stopped matching cannot pass as a narrowing.
        for act in ["这条归你定。", "由你拍板。", "归属主定。"] {
            assert_eq!(
                first_match(act),
                Some("DEFERRAL_SHAPES"),
                "the act must stay caught: {act}"
            );
        }
    }

    /// Both sides of the owner shape, because a pattern that only narrows is a
    /// pattern that may have stopped catching anything. The nouns on the left
    /// are the wording this repository actually writes when it names the owner
    /// check; the acts on the right are what the shape exists for.
    #[test]
    fn the_owner_shape_catches_handovers_and_not_the_nouns() {
        for noun in [
            "属主判据是 `executor_enabled` 这一个配置位",
            "循环的属主判定改读该配置位",
            "这条属主判定优先用显式配置",
        ] {
            assert_eq!(
                first_match(noun),
                None,
                "a noun naming the mechanism is not a handover: {noun}"
            );
        }
        for act in [
            "归属主定。",
            "这事由属主拍板",
            "仍归部署属主定）",
            "判据形状仍归属主拍板",
            // 拍定 and 拍板 are both the act: a tail on the 拍 branch would have
            // dropped this pair while keeping the noun it was aimed at.
            "这条等属主拍定",
            "属主拍板",
        ] {
            assert_eq!(
                first_match(act),
                Some("DEFERRAL_SHAPES"),
                "an act must stay caught: {act}"
            );
        }
    }

    /// The guard the cluster actually runs: a tool call carrying a forbidden
    /// shape is refused and the refusal names which shape, and a clean call
    /// passes. Without this the table would be data no path consults.
    #[tokio::test]
    async fn the_guard_refuses_by_shape_and_names_it() {
        use cog_core::Guardrail;

        let guard = PrincipleShapeGuard::new();
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

        let by_directory = cog_core::ToolCall {
            id: "2".into(),
            name: "bash".into(),
            arguments: serde_json::json!({"command": "git add memory/"}),
        };
        match guard.check_tool_call(&by_directory).await {
            cog_core::GuardResult::Block { rule, .. } => {
                assert_eq!(rule, "principle_shapes:NARROW_ADD_RE");
            }
            other => panic!("a directory add must be refused, got {other:?}"),
        }

        let clean = cog_core::ToolCall {
            id: "3".into(),
            name: "bash".into(),
            arguments: serde_json::json!({"command": "git add crates/x/src/y.rs"}),
        };
        assert_eq!(
            guard.check_tool_call(&clean).await,
            cog_core::GuardResult::Pass
        );
    }

    /// A write to the definition file is judged on its path, not its text.
    ///
    /// The pair matters more than either half: the same text must still be
    /// refused when the call writes anything else, or the exemption would be a
    /// way to launder a handover into a file by naming the definition in the
    /// path. The refusal the exemption answers is on record -- an agent writing
    /// the comment that explains this table was stopped by this table.
    #[tokio::test]
    async fn a_write_to_the_definition_file_is_not_judged_on_its_text() {
        use cog_core::Guardrail;
        let guard = PrincipleShapeGuard::new();

        let defines = cog_core::ToolCall {
            id: "1".into(),
            name: "Edit".into(),
            arguments: serde_json::json!({
                "file_path": "crates/cog-guardrail/src/principle_shapes.rs",
                "new_string": "    // 需要你确认 is what the branch exists for.\n",
            }),
        };
        assert_eq!(
            guard.check_tool_call(&defines).await,
            cog_core::GuardResult::Pass,
            "the file that defines the shapes has to be able to spell them"
        );

        let elsewhere = cog_core::ToolCall {
            id: "2".into(),
            name: "Edit".into(),
            arguments: serde_json::json!({
                "file_path": "crates/cog-guardrail/src/lib.rs",
                "new_string": "    // 需要你确认 is what the branch exists for.\n",
            }),
        };
        match guard.check_tool_call(&elsewhere).await {
            cog_core::GuardResult::Block { rule, .. } => {
                assert_eq!(rule, "principle_shapes:DEFERRAL_SHAPES")
            }
            other => panic!("the same text in another file must be refused, got {other:?}"),
        }
    }

    /// The finished-turn half, through the same entry the platform calls.
    ///
    /// This is the check the host runs when a reply is about to be sent, and
    /// the one an in-process evaluator used to answer `Pass` unconditionally --
    /// a turn that handed a decision to a person went through. The negative
    /// cases are the host's exemptions: a reply that quotes the rule, names a
    /// frozen ledger, or merely mentions a file path is not refused, because
    /// the shapes about commands and paths do not apply to prose.
    #[tokio::test]
    async fn the_guard_judges_a_finished_reply_by_the_turns_own_shapes() {
        use cog_core::Guardrail;
        let guard = PrincipleShapeGuard::new();

        for (reply, rule) in [
            ("归属主定。", "principle_shapes:DEFERRAL_SHAPES"),
            ("先等你了。", "principle_shapes:DEFERRAL_SHAPES"),
            ("a ① b", "principle_shapes:CIRCLED_CHARS"),
        ] {
            match guard.check_output(reply).await {
                cog_core::GuardResult::Block { rule: got, .. } => assert_eq!(got, rule),
                other => {
                    panic!("a finished turn carrying {reply:?} must be refused, got {other:?}")
                }
            }
        }

        for clean in [
            "这条规则作废，不复述：归属主定。",
            "原则里写着「归你定」，我照做了别的。",
            "见 待拍板清单。",
            "改好了 crates/cog-guardrail/src/principle_shapes.rs，git add -A 没有用。",
        ] {
            assert_eq!(
                guard.check_output(clean).await,
                cog_core::GuardResult::Pass,
                "a reply the host accepts must pass here: {clean}"
            );
        }
    }

    /// Every shape has to fire on a real tool call, not only on a string handed
    /// straight to `first_match`.
    ///
    /// Those are two different paths, and the difference hid a real leak: a
    /// call's arguments serialised into one string escape their newlines, so a
    /// shape anchored to a line -- the AI trailer's `^`, a path's `$` -- could
    /// never fire through the guard, while a test that fed the pattern its own
    /// sample stayed green. So every case below starts from the call an agent
    /// would actually make and names the shape it must refuse on, and the last
    /// group is the negative half: the same call shapes with clean text pass.
    #[tokio::test]
    async fn every_shape_fires_on_the_call_it_was_written_for() {
        use cog_core::Guardrail;
        let guard = PrincipleShapeGuard::new();
        let call = |name: &str, arguments: serde_json::Value| cog_core::ToolCall {
            id: "x".into(),
            name: name.into(),
            arguments,
        };
        let rule_of = |result: cog_core::GuardResult| match result {
            cog_core::GuardResult::Block { rule, .. } => rule,
            other => panic!("expected a block, got {other:?}"),
        };

        // A frozen document edited in place: the shape is anchored to the path,
        // and the path is a field of the call.
        assert_eq!(
            rule_of(
                guard
                    .check_tool_call(&call(
                        "Edit",
                        serde_json::json!({
                            "file_path": "internal-docs/2026-01-01_x.md",
                            "old_string": "a",
                            "new_string": "b"
                        })
                    ))
                    .await
            ),
            "principle_shapes:FROZEN_DOC_RE"
        );

        // An AI trailer on a line of a commit command: the shape is anchored to
        // the start of a line, and the line is the message's second.
        assert_eq!(
            rule_of(
                guard
                    .check_tool_call(&call(
                        "Bash",
                        serde_json::json!({
                            "command": "git commit -F - <<'MSG'\nwhy it changed\n\nCo-authored-by: someone\nMSG"
                        })
                    ))
                    .await
            ),
            "principle_shapes:AI_TRAILER_RE"
        );

        // A commit naming no paths: the command is the field, and the shape has
        // to survive the guard reading the call rather than a bare string.
        assert_eq!(
            rule_of(
                guard
                    .check_tool_call(&call(
                        "Bash",
                        serde_json::json!({"command": "git commit -m 'a fix'"})
                    ))
                    .await
            ),
            "principle_shapes:BARE_COMMIT_RE"
        );

        // A deferral written into a file's content.
        assert_eq!(
            rule_of(
                guard
                    .check_tool_call(&call(
                        "Write",
                        serde_json::json!({"file_path": "/tmp/x.md", "content": "归属主定。"})
                    ))
                    .await
            ),
            "principle_shapes:DEFERRAL_SHAPES"
        );

        // A circled numeral in the text being written.
        assert_eq!(
            rule_of(
                guard
                    .check_tool_call(&call(
                        "Write",
                        serde_json::json!({"file_path": "/tmp/x.md", "content": "one is ① and two is ②"})
                    ))
                    .await
            ),
            "principle_shapes:CIRCLED_CHARS"
        );

        for clean in [
            call(
                "Edit",
                serde_json::json!({"file_path": "crates/x/src/y.rs", "new_string": "let a = 1;"}),
            ),
            call(
                "Bash",
                serde_json::json!({"command": "git commit -m 'fix the thing' -- crates/x/src/y.rs"}),
            ),
            call(
                "Write",
                serde_json::json!({"file_path": "/tmp/x.md", "content": "trigger: the queue is non-empty"}),
            ),
        ] {
            assert_eq!(
                guard.check_tool_call(&clean).await,
                cog_core::GuardResult::Pass
            );
        }
    }

    /// The span a refusal carries is text the gate actually matched, and the
    /// shape it is filed under is the one the verdict names.
    ///
    /// Both halves matter for the same reason: a row whose span is not in the
    /// refused text cannot be replayed, and a row filed under a shape the
    /// verdict does not name sends its reader to fix the wrong rule. A
    /// synthesised span -- the shape's name printed where the phrase belongs --
    /// would satisfy neither while looking like evidence.
    #[test]
    fn a_refusal_span_is_text_the_gate_matched() {
        for text in [
            "等你确认之后再改",
            "git commit -m 'x'",
            "Co-Authored-By: Claude <noreply@anthropic.com>",
            "one is ① and two is ②",
        ] {
            let (shape, span) =
                first_span(text).unwrap_or_else(|| panic!("{text:?} carries a shape"));
            assert_eq!(
                first_match(text),
                Some(shape),
                "{text:?}: 判词与证据指的不是同一个形状"
            );
            assert!(
                text.contains(&span) || strip_named_spans(text).contains(&span),
                "{text:?}: 记录下来的证据 {span:?} 既不在原文里，也不在门真正读的那一版里，\
                 这一行没法回放"
            );
            assert!(!span.is_empty(), "{text:?}: 空的证据不是证据");
        }
    }

    /// A refusal is filed under the principle the shape table attributes the
    /// shape to, not under the shape's own name.
    ///
    /// The ledger is read by principle: a row filed as `DEFERRAL_SHAPES` would
    /// sit beside the slugs and never be counted against the rule it broke,
    /// which is the whole reason the table carries both columns.
    #[test]
    fn a_refusal_names_the_principle_that_owns_the_shape() {
        let event = refusal_for("NARROW_ADD_RE", "git add -A");
        assert_eq!(
            principle_of("NARROW_ADD_RE"),
            Some(event.slug.as_str()),
            "拒绝记在了形状名下，而不是它属于的那条原则下"
        );
        assert_eq!(event.criterion, "NARROW_ADD_RE");
        assert_eq!(event.span, "git add -A");
        assert_eq!(event.count, 1);
        assert_eq!(event.source, REFUSAL_SOURCE);
    }

    /// A shape the table attributes to no principle is still recorded, under the
    /// shape itself.
    ///
    /// Dropping it would be the same silence as never writing the row: the gate
    /// refused, and the account would show nothing. The slug column then holds
    /// something that is not a slug, which is what tells the reader this row's
    /// rule has no owner yet.
    #[test]
    fn an_unattributed_shape_is_recorded_rather_than_dropped() {
        let event = refusal_for("SOME_SHAPE_WITH_NO_RULE", "the phrase");
        assert_eq!(event.slug, "SOME_SHAPE_WITH_NO_RULE");
        assert_eq!(event.criterion, "SOME_SHAPE_WITH_NO_RULE");
    }

    use std::sync::Mutex;

    struct RecordingLedger {
        written: Mutex<Vec<cog_core::PrincipleRefusal>>,
        refusal: Option<String>,
    }

    #[async_trait::async_trait]
    impl RefusalLedger for RecordingLedger {
        async fn report_refusals(
            &self,
            events: &[cog_core::PrincipleRefusal],
        ) -> cog_core::SFResult<usize> {
            if let Some(reason) = &self.refusal {
                return Err(cog_core::SFError::Database(reason.clone()));
            }
            self.written.lock().unwrap().extend(events.iter().cloned());
            Ok(events.len())
        }
    }

    fn recording() -> RecordingLedger {
        RecordingLedger {
            written: Mutex::new(Vec::new()),
            refusal: None,
        }
    }

    #[tokio::test]
    async fn a_refusal_that_is_written_reaches_the_ledger_with_its_evidence() {
        let ledger = recording();
        assert!(record_refusal(&ledger, "CIRCLED_CHARS", "①").await);
        let written = ledger.written.lock().unwrap().clone();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].criterion, "CIRCLED_CHARS");
        assert_eq!(written[0].span, "①");
    }

    /// A ledger that refuses the write is a lost count, not a permitted call.
    ///
    /// `record_refusal` is the whole of what a failing store can reach: the
    /// verdict is returned by the caller after this returns, so the only thing
    /// this can do is say the row did not land.
    #[tokio::test]
    async fn a_write_that_fails_does_not_turn_into_a_pass() {
        let ledger = RecordingLedger {
            written: Mutex::new(Vec::new()),
            refusal: Some("ledger is gone".into()),
        };
        assert!(!record_refusal(&ledger, "CIRCLED_CHARS", "①").await);
        assert!(ledger.written.lock().unwrap().is_empty());
    }
}

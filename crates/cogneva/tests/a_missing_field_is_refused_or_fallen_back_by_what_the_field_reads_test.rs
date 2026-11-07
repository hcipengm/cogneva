//! principle: feedback_a_missing_field_is_refused_or_fallen_back_by_what_the_field_reads
//! Gate: a missing field is refused or fallen back by what the field reads.
//!
//! Serde gives a reader two ways past a key it was not handed, and both are
//! right somewhere: `#[serde(default)]` answers a value, and a field with no
//! default refuses the document. Which one belongs on a field follows from what
//! the field reads -- not from taste, and not from whether refusing is
//! inconvenient at the call site.
//!
//! - A field that reads **one act**: a stored verdict, a measurement, a decision
//!   a pass reached, the reason recorded against a row. Such a field is an
//!   answer somebody wrote down, and a document that does not carry the key did
//!   not say "nothing" -- it said nothing at all. A default there invents a
//!   value out of an absence, and the invention is indistinguishable on every
//!   surface from a value that was recorded: a row nobody looked at reads as
//!   `unenforced`, a pass nobody ran reads as `proceed`. Those types refuse: no
//!   `Default` in the derive list, no variant marked `#[default]`, no
//!   `impl Default for`. A default declared on the type is reached by every
//!   field of that type at once, so it is precisely the place a field-level
//!   review never looks.
//! - A field that reads **what a document said about itself**: how urgent the
//!   change is, which face it files it under. A document written before that
//!   column existed was never asked, and refusing to read it would lose an
//!   answer the account already holds. So it falls back to the member that
//!   spells "no document said", which is the same reading as the absence and
//!   invents nothing.
//!
//! The gate answers both directions off the two ledgers' contracts. Every
//! verdict type they declare is declared here with the side it chose; every
//! struct field whose attributes let serde answer a missing key is declared here
//! with the type that answers it; and a side is only accepted from a declaration
//! that carries the shape that side claims. Neither table may hold an entry the
//! scan no longer finds, so the population grows with the tree instead of with
//! someone remembering: a new verdict, or a new `#[serde(default)]`, is red until
//! it says which side it is on.
//!
//! Its ceiling, stated once: the census is the two files named below. The same
//! shape sits on every `#[serde(default)]` over a verdict anywhere in the
//! workspace, and that census has not been taken. Widening this gate without it
//! would red on fall-backs whose fields read documents nobody has enumerated --
//! a question about those documents, and not about the act this rule rules out.
//!
//! Tier: tree -- every answer comes from the shape of a declaration, never from
//! a run.
//!
//! principle: feedback_a_missing_field_is_refused_or_fallen_back_by_what_the_field_reads

use crate::common;

use common::lex::{blank, Dialect};
use common::routes::repo_root;

/// The two contracts whose verdicts this gate answers for.
const CHANGE_LEDGER: &str = "crates/cog-core/src/contract/change_ledger.rs";
const PRINCIPLE_LEDGER: &str = "crates/cog-core/src/contract/principle_ledger.rs";

/// The census, as one list so a reader sees the whole of it.
const CONTRACTS: [&str; 2] = [CHANGE_LEDGER, PRINCIPLE_LEDGER];

/// Which side a verdict type chose, and -- when it falls back -- the member that
/// states "nothing was stated".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Choice {
    FallsBack(&'static str),
    Refused,
}

/// The shape a declaration carries its side in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// A closed set: `pub enum X` plus `pub const ALL: [X; ...]`, the marker this
    /// tree already uses for "a reader can enumerate this". The side is carried
    /// by `Default` in the derive list and `#[default]` on the member.
    ClosedSet,
    /// A type whose side is carried by a hand-written `impl Default for`, which
    /// names the member it answers.
    Impl,
}

/// One verdict declared here: the side it chose, and where that side is written.
struct DeclaredVerdict {
    name: &'static str,
    choice: Choice,
    shape: Shape,
}

/// A closed-set verdict that answers a missing field with a value.
const fn closed_set(name: &'static str, choice: Choice) -> DeclaredVerdict {
    DeclaredVerdict {
        name,
        choice,
        shape: Shape::ClosedSet,
    }
}

/// A verdict that answers a missing field from its own `impl Default`.
const fn hand_written(name: &'static str, member: &'static str) -> DeclaredVerdict {
    DeclaredVerdict {
        name,
        choice: Choice::FallsBack(member),
        shape: Shape::Impl,
    }
}

/// Every verdict the two contracts declare, and the side it chose. Both
/// populations are set-compared against the scan, so this table cannot lag the
/// tree in either direction.
const VERDICTS: &[DeclaredVerdict] = &[
    // The answer a look wrote down: a stored word this contract does not know is
    // a writer and a reader disagreeing, and reading it as `undecided` would
    // turn that disagreement into a settled answer.
    closed_set("ChangeState", Choice::Refused),
    // It explains one retirement recorded by one measurement. A row whose reason
    // cannot be read is a retirement that did not say why, which is the thing
    // the column exists to forbid.
    closed_set("RetireReason", Choice::Refused),
    // It says what one row is waiting on right now. An unknown word would become
    // "nothing is waiting" while the row sits blocked.
    closed_set("BlockedReason", Choice::Refused),
    // The decision one dispatch pass reached. `proceed` names a hand-over, and a
    // pass that handed nothing over must not be readable as one that did.
    closed_set("DispatchDecision", Choice::Refused),
    // What one snapshot said, including that there was none. `absent` is a
    // reading a snapshot produced, not a value to hand a document that named no
    // snapshot at all.
    closed_set("PoolReading", Choice::Refused),
    // The verdict of one look at the tree. An absent look is carried as
    // `Option`, never as `unenforced`: "no look settled it" and "a debt nobody
    // is paying" must not read the same.
    closed_set("PrincipleState", Choice::Refused),
    // It tells the runnable check from the platform-side declaration. An unknown
    // kind would decide which gate ran, from a word nobody wrote.
    closed_set("PrincipleCheckKind", Choice::Refused),
    // What a document said about urgency. A document written before the band
    // column existed was never asked, and refusing to read it would lose an
    // answer the account already holds; `Unstated` is that absence, stated.
    closed_set("ChangePriority", Choice::FallsBack("Unstated")),
    // The same field-shape: a document that predates the face column named no
    // face, and `none` is the member that spells "no document named one", so the
    // fall-back invents nothing.
    hand_written("ChangeBlock", "NONE"),
];

/// Every struct field in the two contracts whose attributes let serde answer a
/// missing key, with the type it declared, verbatim.
///
/// Set-compared against the scan in both directions: a `#[serde(default)]` the
/// scan finds and this table does not name is red, so the answer for a new
/// column cannot be decided by leaving it out.
const DEFAULTED_FIELDS: &[(&str, &str, &str)] = &[
    ("ChangeSource", "priority", "ChangePriority"),
    ("ChangeSource", "block", "ChangeBlock"),
    ("PrincipleReading", "priority", "Option<ChangePriority>"),
];

/// Everything the gate reads off the two contracts, taken in one pass so that no
/// two assertions can disagree about which text they read.
struct Declarations {
    verdicts: Vec<Verdict>,
    fields: Vec<DefaultedField>,
    impls: Vec<ManualDefault>,
}

impl Declarations {
    fn read() -> Self {
        let mut verdicts = Vec::new();
        let mut fields = Vec::new();
        let mut impls = Vec::new();
        for path in CONTRACTS {
            let text = read_contract(path);
            let code = blank(&text, Dialect::Rust);
            verdicts.extend(closed_set_verdicts(path, &code));
            fields.extend(defaulted_fields(path, &code));
            impls.extend(manual_default_impls(path, &code));
        }
        Self {
            verdicts,
            fields,
            impls,
        }
    }

    fn verdict(&self, name: &str) -> Option<&Verdict> {
        self.verdicts.iter().find(|verdict| verdict.name == name)
    }

    fn default_impl(&self, name: &str) -> Option<&ManualDefault> {
        self.impls
            .iter()
            .find(|declared| declared.type_name == name)
    }
}

/// One closed-set verdict type as the file declares it.
struct Verdict {
    file: &'static str,
    name: String,
    line: usize,
    /// The `#[...]` lines directly above the declaration, joined.
    attributes: String,
    /// The variant carrying `#[default]`, if one does.
    default_member: Option<String>,
}

/// One struct field whose attributes let serde answer a missing key.
struct DefaultedField {
    file: &'static str,
    structure: String,
    name: String,
    /// The declared type, exactly as the field spells it.
    ty: String,
    line: usize,
}

/// One hand-written `impl Default for X` and the member it answers.
struct ManualDefault {
    file: &'static str,
    type_name: String,
    member: String,
}

/// The text of one contract, read relative to the root the workspace declares.
fn read_contract(path: &str) -> String {
    let full = repo_root().join(path);
    std::fs::read_to_string(&full)
        .unwrap_or_else(|error| panic!("{}: unreadable: {error}", full.display()))
}

/// Every `pub enum X { ... }` in `code` that also declares `pub const ALL: [X;`.
///
/// `code` is the contract with comments and literal contents blanked, so a
/// declaration quoted in prose is not read as one.
fn closed_set_verdicts(file: &'static str, code: &str) -> Vec<Verdict> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(found) = code[at..].find("pub enum ") {
        let decl = at + found;
        at = decl + "pub enum ".len();
        let name = identifier_start(&code[at..]);
        if !code.contains(&format!("pub const ALL: [{name};")) {
            continue;
        }
        let open = code[at..]
            .find('{')
            .map(|offset| at + offset)
            .unwrap_or_else(|| panic!("{file}: `{name}` is declared but has no body"));
        out.push(Verdict {
            file,
            name: name.to_string(),
            line: line_of(code, decl),
            attributes: attributes_above(code, decl),
            default_member: default_member(balanced(code, open, b'{', b'}')),
        });
    }
    out
}

/// Every struct field in `code` carrying a `#[serde(default...)]` attribute.
fn defaulted_fields(file: &'static str, code: &str) -> Vec<DefaultedField> {
    let lines: Vec<&str> = code.lines().collect();
    let mut out = Vec::new();
    let mut structure = String::new();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed
            .strip_prefix("pub struct ")
            .or_else(|| trimmed.strip_prefix("struct "))
        {
            structure = identifier_start(rest).to_string();
        }
        if !trimmed.starts_with("#[serde(default") {
            continue;
        }
        // The attribute sits on its own line above the field it answers.
        let declaration = lines[index + 1..]
            .iter()
            .map(|next| next.trim())
            .find(|candidate| !candidate.is_empty() && !candidate.starts_with("#["))
            .unwrap_or_else(|| {
                panic!(
                    "{file}:{}: a `#[serde(default...)]` attribute with no field \
                     under it: `{trimmed}`",
                    index + 1
                )
            });
        let (name, ty) = field_declaration(declaration).unwrap_or_else(|| {
            panic!(
                "{file}:{}: `{declaration}` is not a field declaration, so the \
                 type this attribute answers is not readable",
                index + 1
            )
        });
        out.push(DefaultedField {
            file,
            structure: structure.clone(),
            name,
            ty,
            line: index + 1,
        });
    }
    out
}

/// Every `impl Default for X` in `code`, with the member it answers.
fn manual_default_impls(file: &'static str, code: &str) -> Vec<ManualDefault> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(found) = code[at..].find("impl Default for ") {
        let after = at + found + "impl Default for ".len();
        at = after;
        let type_name = identifier_start(&code[after..]).to_string();
        let open = code[after..]
            .find('{')
            .map(|offset| after + offset)
            .unwrap_or_else(|| panic!("{file}: `impl Default for {type_name}` has no body"));
        let member = member_answered(balanced(code, open, b'{', b'}'), &type_name)
            .unwrap_or_else(|| panic!("{file}: `impl Default for {type_name}` answers no member"));
        out.push(ManualDefault {
            file,
            type_name,
            member,
        });
    }
    out
}

/// The member an `impl Default` body answers: `Self::MEMBER`, or `X::MEMBER`.
///
/// The first one written is taken. An impl that answers different members on
/// different branches is not a shape this gate pretends to read.
fn member_answered(body: &str, type_name: &str) -> Option<String> {
    let qualified = format!("{type_name}::");
    for needle in ["Self::", qualified.as_str()] {
        let Some(found) = body.find(needle) else {
            continue;
        };
        let member = identifier_start(&body[found + needle.len()..]);
        if !member.is_empty() {
            return Some(member.to_string());
        }
    }
    None
}

/// The `#[...]` lines directly above `decl`, joined.
///
/// Doc comments are already blanked in `code`, so the run ends at the first line
/// that is not itself an attribute: the prose above a type is never read as one.
fn attributes_above(code: &str, decl: usize) -> String {
    let mut run: Vec<&str> = Vec::new();
    for line in code[..decl].trim_end().lines().rev() {
        let trimmed = line.trim();
        if !trimmed.starts_with("#[") {
            break;
        }
        run.push(trimmed);
    }
    run.reverse();
    run.join(" ")
}

/// Whether a run of attributes derives `Default`.
///
/// Read as a token, not as a substring: `#[serde(default)]` is a field-level
/// answer and is not the type-level one this asks about.
fn attributes_derive_default(attributes: &str) -> bool {
    attributes
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|token| token == "Default")
}

/// The variant marked `#[default]` in one enum body, if one is.
fn default_member(block: &str) -> Option<String> {
    let lines: Vec<&str> = block.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        if line.trim() != "#[default]" {
            continue;
        }
        for next in &lines[index + 1..] {
            let candidate = next.trim();
            if candidate.is_empty() || candidate.starts_with("#[") {
                continue;
            }
            return Some(identifier_start(candidate).to_string());
        }
    }
    None
}

/// `pub name: Type,` as the name and the type, with the comma and the padding
/// taken off the type but nothing else touched.
fn field_declaration(line: &str) -> Option<(String, String)> {
    let rest = line.strip_prefix("pub ").unwrap_or(line);
    let (name, ty) = rest.split_once(':')?;
    let ty = ty.trim().trim_end_matches(',').trim();
    if name.trim().is_empty() || ty.is_empty() {
        return None;
    }
    Some((identifier_start(name).to_string(), ty.to_string()))
}

/// The type inside one wrapping `Option<...>`, or the type itself.
fn strip_option(ty: &str) -> &str {
    let trimmed = ty.trim();
    trimmed
        .strip_prefix("Option<")
        .and_then(|rest| rest.strip_suffix('>'))
        .map(str::trim)
        .unwrap_or(trimmed)
}

/// The side this gate declares for a type.
fn choice_of(type_name: &str) -> Option<Choice> {
    VERDICTS
        .iter()
        .find(|declared| declared.name == type_name)
        .map(|declared| declared.choice)
}

/// The identifier `text` starts with, or the empty string.
fn identifier_start(text: &str) -> &str {
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    &text[..end]
}

/// Which line `byte` sits on, counted from one.
fn line_of(code: &str, byte: usize) -> usize {
    code[..byte].bytes().filter(|b| *b == b'\n').count() + 1
}

/// The text from the opening delimiter at `open` through its matching close.
///
/// Called on the blanked body, so a brace written in prose or inside a string is
/// not one of these.
fn balanced(code: &str, open: usize, open_b: u8, close_b: u8) -> &str {
    let bytes = code.as_bytes();
    assert_eq!(
        bytes[open], open_b,
        "balanced() is called at an opening delimiter"
    );
    let mut depth = 0i32;
    let mut at = open;
    while at < bytes.len() {
        let c = bytes[at];
        if c == open_b {
            depth += 1;
        } else if c == close_b {
            depth -= 1;
            if depth == 0 {
                return &code[open..=at];
            }
        }
        at += 1;
    }
    panic!("unbalanced {} at byte {open}", open_b as char);
}

#[test]
fn every_absence_answer_is_declared_and_every_declaration_is_a_site() {
    let declarations = Declarations::read();

    for verdict in &declarations.verdicts {
        assert!(
            VERDICTS.iter().any(|declared| {
                declared.name == verdict.name && declared.shape == Shape::ClosedSet
            }),
            "{}:{}: `{}` is a closed-set verdict and this gate declares no side \
             for it: say whether a missing field of that type is refused or falls \
             back, and to which member",
            verdict.file,
            verdict.line,
            verdict.name
        );
    }
    for declared in VERDICTS.iter().filter(|d| d.shape == Shape::ClosedSet) {
        assert!(
            declarations.verdict(declared.name).is_some(),
            "`{}` is declared here with a side, but neither contract declares it \
             as a closed-set verdict any more: the entry claims a side for a type \
             nothing reads",
            declared.name
        );
    }

    for found in &declarations.impls {
        assert!(
            VERDICTS.iter().any(|declared| {
                declared.name == found.type_name && declared.shape == Shape::Impl
            }),
            "{}: `impl Default for {}` answers `{}`, and no entry here declares \
             that type: a default declared on a type is reached by every field of \
             it at once, so it has to be said whether that answer is one a \
             missing key may have",
            found.file,
            found.type_name,
            found.member
        );
    }
    for declared in VERDICTS.iter().filter(|d| d.shape == Shape::Impl) {
        assert!(
            declarations.default_impl(declared.name).is_some(),
            "`{}` is declared here as a hand-written fall-back, and neither \
             contract writes `impl Default for {}` any more",
            declared.name,
            declared.name
        );
    }

    for field in &declarations.fields {
        let declared = DEFAULTED_FIELDS
            .iter()
            .find(|(structure, name, _)| *structure == field.structure && *name == field.name);
        let Some((_, _, ty)) = declared else {
            panic!(
                "{}:{}: `{}::{}` lets serde answer a missing key and this gate \
                 declares nothing about it: say which kind of reading the field is, \
                 and whether the absence it answers is carried as a member or as \
                 `Option`",
                field.file, field.line, field.structure, field.name
            );
        };
        assert_eq!(
            *ty, field.ty,
            "{}:{}: `{}::{}` is declared here as `{ty}` and declares `{}`",
            field.file, field.line, field.structure, field.name, field.ty
        );
    }
    for &(structure, name, _) in DEFAULTED_FIELDS {
        assert!(
            declarations
                .fields
                .iter()
                .any(|field| field.structure == structure && field.name == name),
            "`{structure}::{name}` is declared here as a defaulted field, but \
             neither contract answers a missing key for it any more"
        );
    }
}

#[test]
fn a_falls_back_type_carries_its_own_stated_nothing() {
    let declarations = Declarations::read();
    let mut checked = 0usize;
    for declared in VERDICTS {
        let Choice::FallsBack(member) = declared.choice else {
            continue;
        };
        checked += 1;
        match declared.shape {
            Shape::ClosedSet => {
                let verdict = declarations.verdict(declared.name).unwrap_or_else(|| {
                    panic!(
                        "`{}` is declared here as a closed-set fall-back and the \
                         scan does not find it",
                        declared.name
                    )
                });
                assert!(
                    attributes_derive_default(&verdict.attributes),
                    "{}:{}: `{}` falls back to `{member}`, so `Default` has to be \
                     in its derive list (found `{}`)",
                    verdict.file,
                    verdict.line,
                    declared.name,
                    verdict.attributes
                );
                assert_eq!(
                    verdict.default_member.as_deref(),
                    Some(member),
                    "`{}` falls back to `{member}`, and the member marked \
                     `#[default]` is what a defaulted field of it resolves to",
                    declared.name
                );
            }
            Shape::Impl => {
                let found = declarations.default_impl(declared.name).unwrap_or_else(|| {
                    panic!(
                        "`{}` falls back to `{member}`, and nothing in either \
                         contract says how: no `Default` in a derive list, no \
                         `impl Default for {}`",
                        declared.name, declared.name
                    )
                });
                assert_eq!(
                    found.member, member,
                    "{}: `impl Default for {}` answers `{}`, and the member this \
                     gate declares is `{member}`",
                    found.file, declared.name, found.member
                );
            }
        }
    }
    assert!(
        checked >= 1,
        "no verdict in the table falls back, so this test asserted nothing: the \
         rule it reads has two sides and this is one of them"
    );
}

#[test]
fn a_refused_type_cannot_fall_back() {
    let declarations = Declarations::read();
    let mut checked = 0usize;
    for declared in VERDICTS {
        if declared.choice != Choice::Refused {
            continue;
        }
        checked += 1;
        if let Some(found) = declarations.default_impl(declared.name) {
            panic!(
                "{}: `impl Default for {}` answers `{}` for a type that refuses a \
                 missing field: a verdict nobody recorded must not come back as \
                 one that was",
                found.file, declared.name, found.member
            );
        }
        let Some(verdict) = declarations.verdict(declared.name) else {
            continue;
        };
        assert!(
            !attributes_derive_default(&verdict.attributes),
            "{}:{}: `{}` refuses a missing field, so `Default` cannot be in its \
             derive list",
            verdict.file,
            verdict.line,
            declared.name
        );
        assert!(
            verdict.default_member.is_none(),
            "{}:{}: `{}` refuses a missing field, and `{}` is marked `#[default]`",
            verdict.file,
            verdict.line,
            declared.name,
            verdict.default_member.clone().unwrap_or_default()
        );
    }
    assert!(
        checked >= 1,
        "no verdict in the table refuses, so this test asserted nothing: the rule \
         it reads has two sides and this is one of them"
    );
}

#[test]
fn a_refusing_column_is_not_given_a_serde_default() {
    let declarations = Declarations::read();
    let mut checked = 0usize;
    for field in &declarations.fields {
        checked += 1;
        let inner = strip_option(&field.ty);
        let Some(choice) = choice_of(inner) else {
            panic!(
                "{}:{}: `{}::{}` is answered by serde and `{inner}` is not a \
                 verdict this gate declares, so nothing here can say which side \
                 the absence is on",
                field.file, field.line, field.structure, field.name
            );
        };
        match choice {
            Choice::Refused => {
                // `Option<...>` is the one place a serde default produces the
                // absence itself -- `None` is what "nothing was recorded" looks
                // like, so nothing is invented. A bare verdict is not that.
                let carried_as_absence = field.ty.trim() != inner;
                assert!(
                    carried_as_absence,
                    "{}:{}: `{}::{}` reads one act and is typed `{}`: a serde \
                     default on it invents the verdict of an act nobody recorded. \
                     Carry the absence as `Option<{inner}>` with nothing defaulted, \
                     or make the key required",
                    field.file, field.line, field.structure, field.name, field.ty
                );
            }
            Choice::FallsBack(member) => {
                assert!(
                    DEFAULTED_FIELDS.iter().any(|&(structure, name, _)| {
                        structure == field.structure && name == field.name
                    }),
                    "{}:{}: `{}::{}` falls back to `{member}` and is not declared in \
                     this gate's table with the population it reads",
                    field.file,
                    field.line,
                    field.structure,
                    field.name
                );
            }
        }
    }
    assert!(
        checked >= 1,
        "no `#[serde(default...)]` site was read at all, so every field of a \
         refusing type passed by not being looked at"
    );
}

#[test]
fn the_scanner_reads_both_shapes_off_a_snippet() {
    const SNIPPET: &str = r#"
#[derive(Debug, Clone, Default)]
pub enum FellBack {
    #[default]
    Unstated,
    Named,
}

impl FellBack {
    pub const ALL: [FellBack; 2] = [FellBack::Unstated, FellBack::Named];
}

#[derive(Debug, Clone)]
pub enum Refused {
    One,
    Two,
}

impl Refused {
    pub const ALL: [Refused; 2] = [Refused::One, Refused::Two];
}

pub struct Holder {
    /// Prose that names `#[serde(default)]` is not a field's attribute.
    pub stated: String,
    #[serde(default)]
    pub verdict: Refused,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guess: Option<FellBack>,
}
"#;
    let code = blank(SNIPPET, Dialect::Rust);

    let verdicts = closed_set_verdicts("snippet.rs", &code);
    assert_eq!(
        verdicts.len(),
        2,
        "the snippet declares two closed sets, and a scan that read one of them \
         would answer half the question"
    );
    let fell_back = verdicts
        .iter()
        .find(|verdict| verdict.name == "FellBack")
        .expect("FellBack is declared in the snippet");
    assert!(
        attributes_derive_default(&fell_back.attributes),
        "the deriving enum is read as deriving `Default`"
    );
    assert_eq!(
        fell_back.default_member.as_deref(),
        Some("Unstated"),
        "the marked variant is read as the member the fall-back answers"
    );
    let refused = verdicts
        .iter()
        .find(|verdict| verdict.name == "Refused")
        .expect("Refused is declared in the snippet");
    assert!(
        !attributes_derive_default(&refused.attributes),
        "an enum deriving no `Default` is read as carrying none"
    );
    assert!(
        refused.default_member.is_none(),
        "an enum marking no variant is read as marking none"
    );

    let fields = defaulted_fields("snippet.rs", &code);
    assert_eq!(
        fields.len(),
        2,
        "two fields let serde answer a missing key; the doc line above them is \
         prose and is not a third"
    );
    let bare = fields
        .iter()
        .find(|field| field.name == "verdict")
        .expect("the bare column is read");
    assert_eq!(bare.structure, "Holder");
    assert_eq!(
        bare.ty, "Refused",
        "a bare column is read with the verdict type it declares, which is what \
         the refusing-column check looks up"
    );
    let wrapped = fields
        .iter()
        .find(|field| field.name == "guess")
        .expect("the wrapped column is read");
    assert_eq!(wrapped.ty, "Option<FellBack>");
    assert_eq!(
        strip_option(&wrapped.ty),
        "FellBack",
        "one `Option<...>` is taken off, and the type under it is read"
    );
}

#[test]
fn the_reading_found_both_sides() {
    let declarations = Declarations::read();
    assert!(
        declarations.verdicts.len() >= 4,
        "the scan read {} closed-set verdicts out of the two ledgers' contracts, \
         fewer than the four this gate is drawn around: the scan has gone wrong \
         and every assertion above it passed over an empty set",
        declarations.verdicts.len()
    );
    let falls_back = VERDICTS
        .iter()
        .filter(|declared| matches!(declared.choice, Choice::FallsBack(_)))
        .count();
    assert!(
        falls_back >= 1,
        "no verdict here falls back, so a fall-back that invented a value would \
         have nothing to be compared against"
    );
    assert!(
        VERDICTS.len() - falls_back >= 1,
        "no verdict here refuses, so a refusal turned into a default would have \
         nothing to be compared against"
    );
}

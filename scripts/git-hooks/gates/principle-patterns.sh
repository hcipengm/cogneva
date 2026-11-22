#!/usr/bin/env bash
# shellcheck disable=SC2034  # 这份文件是被 source 的库：下面这些名字是给调用方用的
# The sentence shapes the permanent-memory principles forbid, read from their
# single definition in the cogneva tree.
#
# Sourced, never executed. More than one reader: `check-ownership-deferral.sh`
# (reads a document), `principle-gate.sh` (gates a tool call), and the in-process
# judge the cluster runs on an agent's tool call. They are the same judgement,
# and the shapes used to be defined here and copied into the tree -- which is the
# failure this file now avoids. A shape added to one copy and not the other
# silently narrows whichever still ran, and only the copy that runs is visible.
#
# So the literals live in exactly one place:
#   crates/cog-guardrail/src/principle_shapes.rs
# and this file reads them out of it. The names below stay here because they are
# this side's own keys; the tree publishes the names and the patterns together,
# so a name the tree does not know is a shape this side would silently lose and
# the gate's own test pins that.
#
# Each pattern is an extended regex (grep -E). Adding a shape is done in the
# tree; every entry there is a form some tree has actually produced, so a new one
# is evidence a principle is still leaking rather than noise to be tuned out.
#
# The locale is pinned to C, and nothing here is ever put inside `[...]`. A
# bracket expression holding a multibyte character is a set of *bytes* under that
# locale, and since a circled numeral shares its leading bytes with ordinary CJK
# text such a class matches a great many characters that are not circled numerals
# at all -- a gate that found the forbidden thing everywhere instead of nowhere.
# Literal strings and `case` globs byte-match correctly and are what this file
# uses.
export LC_ALL=C

# Where the single definition lives. The env override lets the hook be pointed at
# a checkout that is not a sibling of this repository; the default is that
# sibling, derived from this file's own location rather than written as an
# absolute path, since the checkout location differs from machine to machine.
# The root comes from the checkout itself, not from counting `..`: the count
# changes every time a reader in this file moves, and a wrong count reads as an
# empty table -- which the gate cannot tell apart from "nothing is forbidden".
_COGNEVA_BRAIN="$(env -u GIT_DIR -u GIT_WORK_TREE git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel 2>/dev/null)" \
    || _COGNEVA_BRAIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Where the shapes are read from. The default is the **HEAD** of the cogneva
# checkout, not its working tree: that tree is shared with other sessions and
# other machines' teams, and a half-finished edit there would change what this
# gate refuses for a commit that never touched it. The sample that produced this
# rule: a commit on the other side was refused twice because a file in the sibling tree was
# being written at that moment, and the same command run by hand came back
# green. Reading HEAD makes the gate a function of what was committed, which is
# the only thing a verdict can be attributed to.
#
# `COGNEVA_SHAPES_FILE` still wins when set: fixtures point it at a file, and a
# checkout that is not a git repository (or a caller that pinned bytes) needs
# the old path. `COGNEVA_SHAPES_REPO` exists for tests to substitute a repository.
_SHAPES_REL="crates/cog-guardrail/src/principle_shapes.rs"
if [ -n "${COGNEVA_SHAPES_FILE:-}" ]; then
    _COGNEVA_SHAPES="$COGNEVA_SHAPES_FILE"
    _COGNEVA_SHAPES_MODE="file"
    _COGNEVA_SHAPES_DESC="$_COGNEVA_SHAPES"
else
    _COGNEVA_SHAPES_REPO="${COGNEVA_SHAPES_REPO:-$_COGNEVA_BRAIN/../cogneva}"
    _COGNEVA_SHAPES="$_COGNEVA_SHAPES_REPO/$_SHAPES_REL"
    _COGNEVA_SHAPES_MODE="head"
    _COGNEVA_SHAPES_DESC="$_COGNEVA_SHAPES_REPO@HEAD:$_SHAPES_REL"
fi

# Read the shapes out of the tree. The literals are Rust raw strings and the file
# is formatted, so the reader walks the whole file rather than a line: a shape
# the formatter split across lines is still one literal, and a line reader that
# missed it would drop a check without a word.
#
# The defaults are set before the read so the check below can tell an empty table
# from an unset variable, and so a `set -u` consumer that reads a missing table
# sees nothing rather than aborting the whole script.
DEFERRAL_SHAPES=()
CIRCLED_CHARS=()
eval "$(
python3 - "$_COGNEVA_SHAPES_MODE" "${_COGNEVA_SHAPES_REPO:-}" "$_COGNEVA_SHAPES" <<'PY'
import re, subprocess, sys

mode, repo, where = sys.argv[1], sys.argv[2], sys.argv[3]
if mode == "head":
    # The committed bytes, not the checkout's: see the comment where the mode is set.
    try:
        got = subprocess.run(["git", "-C", repo, "show", "HEAD:" + "crates/cog-guardrail/src/principle_shapes.rs"],
                             capture_output=True, text=True)
    except OSError:
        sys.exit(0)
    if got.returncode != 0:
        sys.exit(0)  # unreadable table -> empty -> PRINCIPLE_SHAPES_OK=0, never a fallback
    src = got.stdout
else:
    try:
        src = open(where, encoding="utf-8").read()
    except OSError:
        sys.exit(0)


def const_str(name):
    m = re.search(r'pub const %s:\s*&str\s*=\s*r"([^"]*)"' % re.escape(name), src, re.S)
    return m.group(1) if m else None


def const_arr(name):
    m = re.search(r'pub const %s:\s*&\[&str\]\s*=\s*&\[(.*?)\];' % re.escape(name), src, re.S)
    return re.findall(r'r"([^"]*)"', m.group(1)) if m else []


def const_chars(name):
    m = re.search(r"pub const %s:\s*&\[char\]\s*=\s*&\[(.*?)\];" % re.escape(name), src, re.S)
    return re.findall(r"'([^']*)'", m.group(1)) if m else []


def q(v):
    # Every value is emitted single-quoted; a value carrying a single quote would
    # break the shell out of the string, so it is refused rather than emitted.
    if "'" in v:
        sys.exit("principle-patterns: shape contains a single quote: %r" % v)
    return "'" + v + "'"


out = []
vals = const_arr("DEFERRAL_SHAPES")
if vals:
    out.append("DEFERRAL_SHAPES=(%s)" % " ".join(q(v) for v in vals))
for name in (
    "DEFERRAL_EXEMPT",
    "DEFERRAL_STRIKING",
    "NARROW_ADD_RE",
    "DESTRUCTIVE_GIT_RE",
    "AI_TRAILER_RE",
    "BARE_COMMIT_RE",
    "FROZEN_DOC_RE",
):
    v = const_str(name)
    if v is not None:
        out.append("%s=%s" % (name, q(v)))
chars = const_chars("CIRCLED_CHARS")
if chars:
    out.append("CIRCLED_CHARS=(%s)" % " ".join(q(c) for c in chars))
print("\n".join(out))
PY
)"
# Whether the tree's shape table actually came back. An empty table is not "the
# project forbids nothing" -- it is "the definitions could not be read", and the
# two must not look alike to a caller, because one of them is a clean result and
# the other is a gate that would check nothing while reporting nothing found.
# A caller that *gates* has to refuse when this is 0; a caller that merely
# *reports* may warn and carry on. This file sets the flag and takes no side.
# Every name this file can hand to a caller, not just the two arrays. The first
# version of this check asked only whether DEFERRAL_SHAPES and CIRCLED_CHARS came
# back, and a reader that lost one *string* -- an entry whose definition was
# renamed, a `const_str` that stopped matching -- still reported a healthy table
# while the gate's use of that variable aborted under `set -u`. That abort exits
# through the crash handler, which allows the call and warns on a stream the tool
# layer drops at exit 0: one shape stops being checked and nothing says so.
# A shape this list knows and the tree does not is a shape that would be checked
# by nobody, so it is unreadable, not absent.
PRINCIPLE_SHAPE_NAMES=(
    DEFERRAL_SHAPES DEFERRAL_EXEMPT DEFERRAL_STRIKING
    NARROW_ADD_RE DESTRUCTIVE_GIT_RE AI_TRAILER_RE BARE_COMMIT_RE FROZEN_DOC_RE
    CIRCLED_CHARS
)
PRINCIPLE_SHAPES_OK=1
PRINCIPLE_SHAPES_MISSING=()
for _name in "${PRINCIPLE_SHAPE_NAMES[@]}"; do
    case "$_name" in
        # The two tables are read as lists: an empty one is a table that did not
        # arrive, and no caller can tell it apart from "nothing is forbidden".
        DEFERRAL_SHAPES | CIRCLED_CHARS)
            eval "[ \"\${#${_name}[@]}\" -gt 0 ]" || PRINCIPLE_SHAPES_MISSING+=("$_name")
            ;;
        *)
            [ -n "${!_name:-}" ] || PRINCIPLE_SHAPES_MISSING+=("$_name")
            ;;
    esac
done
unset _name
if [ "${#PRINCIPLE_SHAPES_MISSING[@]}" -gt 0 ]; then
    PRINCIPLE_SHAPES_OK=0
    printf 'principle-patterns: 形状表是空的或不全（%s）——不是「没发现」，是「没读到定义」。\n' \
        "$_COGNEVA_SHAPES" >&2
    printf '  缺的是：%s\n' "${PRINCIPLE_SHAPES_MISSING[*]}" >&2
    printf '  设 COGNEVA_SHAPES_FILE 指向 cogneva 检出里的 crates/cog-guardrail/src/principle_shapes.rs。\n' >&2
fi

# --- naming versus using: quoted spans ----------------------------------------
# A phrase inside quotes is being *named*, not *used*. Every reader strips those
# spans before matching, and they do it here rather than in any one of them for
# the same reason the shapes live in one place: a judgement with two definitions
# silently narrows whichever reader still has the older one.
#
# Why it exists, measured three times in one afternoon:
#   - the gate refused the message reporting the gate broken -- the one reading
#     that could get it fixed;
#   - the document reader reported nine quoted phrases in one file as if they
#     were nine plans handing a decision over;
#   - a `grep -B1 -iE "...|reset --hard|..."` was refused as a destructive git
#     command, so the memory behind that very rule could not be looked up.
# Each one is the same mistake: reading a quotation as an act. A rule beats a
# list of quoting styles -- corner and lenticular brackets, curly quotes,
# backticks, and both ASCII quote characters all go -- so a new style costs
# nothing.
#
# What it costs, said plainly: an unpaired quote can pair with a later one and
# swallow what sits between, and a command wrapped as `bash -c "git reset
# --hard"` now reads as a name rather than an act. Both are accepted because the
# plain form is what these checks are nets against, and because the failures
# above are the ones that actually happened.
#
# Circled numerals are deliberately NOT exempted by this: the owner forbade the
# character, quoting included.
#
# Heredoc bodies are blanked before the quotes go, one line out for one line in,
# because they are the same mistake in a shape the quote pass could not reach:
# `cat > notes.md <<'EOF' … EOF` puts file content inside the command string, and
# the body of a commit message written that way is prose about a command rather
# than the command. The quote pass alone does not help -- it pairs the two `'`
# around the delimiter and leaves the body standing -- so 2026-10-30 a commit
# message that merely *mentioned* a wide add was refused as one, and the only way
# past was to reword the message. Line count is preserved so a citation that
# names a line still names the right one.
#
# The terminator line is kept: it is the command's, not the body's, and a body
# with no terminator is blanked to the end, which is what an unterminated heredoc
# would consume anyway.
#
# A delimiter is not always the bare word: `\EOF` and `'EOF'` and `"EOF"` all
# name the same terminator, and `<<$TAG` names one the gate cannot know, because
# the shell is not run to find out. The three literal spellings resolve to the
# word; a variable is resolved from an assignment earlier in the same command
# when there is one, and otherwise the body runs to the first line that is empty
# or that spells the variable back out -- which is what an unset name expands to
# and what a reader usually wrote by hand.
#
# An unresolvable delimiter with no such line is left alone rather than blanked
# to the end. That is the one place this pass declines to guess, and the reason
# is which way the guess would fail: blanking would take a live `git add -A`
# after a heredoc the shell would have terminated, and a false positive costs a
# rewording while a false negative costs the rule.
strip_named_spans() {
    local out
    out="$(printf '%s' "$1" | python3 -c '
import re, sys
t = sys.stdin.read()
lines = t.split("\n")
out = []
WORD = r"[A-Za-z_][A-Za-z0-9_]*"
WORD_RE = re.compile(WORD)
def assignment(name, upto):
    want = re.compile(r"(?:^|[\s;&|(])" + re.escape(name) + r"=(" + WORD + r")")
    value = None
    for k in range(upto):
        for hit in want.finditer(lines[k]):
            value = hit.group(1)
    return value
def ends_at(candidates, start, tabs):
    j = start
    while j < len(lines):
        probe = lines[j].lstrip("\t") if tabs else lines[j]
        if probe.rstrip() in candidates:
            return j
        j += 1
    return None
i = 0
while i < len(lines):
    line = lines[i]
    out.append(line)
    i += 1
    m = re.search(r"<<(-?)[ \t]*([^\s;&|<>()]*)", line)
    if not m:
        continue
    tabs, token = m.group(1) == "-", m.group(2)
    single = len(token) > 1 and token[0] == "\x27" and token[-1] == token[0]
    double = len(token) > 1 and token[0] == "\"" and token[-1] == token[0]
    if single or double:
        token = token[1:-1]
    # Quotes decide whether the *body* is expanded, not whether the delimiter
    # word is: a double-quoted dollar name still expands, a single-quoted one
    # stays the literal text it was written as.
    literal = True
    if not single:
        if token.startswith("\\"):
            token = token[1:]
        elif token.startswith("$"):
            literal = False
            token = token[1:]
            if token.startswith("{") and token.endswith("}"):
                token = token[1:-1]
    if not WORD_RE.fullmatch(token):
        continue
    if literal:
        end = ends_at({token}, i, tabs)
        if end is None:
            end = len(lines) - 1
    else:
        resolved = assignment(token, i)
        if resolved is not None:
            end = ends_at({resolved}, i, tabs)
            if end is None:
                end = len(lines) - 1
        else:
            end = ends_at({"", "$" + token, "${" + token + "}"}, i, tabs)
            if end is None:
                continue
    while i <= end and i < len(lines):
        out.append(lines[i] if i == end else "")
        i += 1
t = "\n".join(out)
for a, b in (("「", "」"), ("『", "』"), ("“", "”"), ("`", "`"), ("\"", "\""), ("\x27", "\x27")):
    t = re.sub(re.escape(a) + ".*?" + re.escape(b), a + b, t, flags=re.S)
sys.stdout.write(t)
' 2>/dev/null)"
    if [ -n "$out" ]; then printf '%s' "$out"; else printf '%s' "$1"; fi
}

# --- 圈码序号: the owner forbade circled numerals outright -------------------
# One literal per element, from the tree's own table, matched with `case` (see
# the locale note above) rather than a bracket expression.
has_circled() {
    local c
    for c in "${CIRCLED_CHARS[@]}"; do
        case "$1" in *"$c"*) return 0 ;; esac
    done
    return 1
}

# --- 冻结文档: internal-docs is append-only by new dated document ------------
# Carried by specs/ledger/inputs/doc-digests.tsv, whose second column is the
# landing moment: non-empty means the document was read, so editing it would
# void the account that reading produced. This is this side's own path, not a
# shape, so it stays here. FROZEN_DOC_RE itself comes from the tree.
DIGESTS_REL='specs/ledger/inputs/doc-digests.tsv'

# --- which rule each judgement above enforces ---------------------------------
# Data rather than prose, because "how much of what we believe is actually
# checkable" has to be a count anyone can re-run -- a header saying "the gate
# covers the principles" reads the same whether it covers six or six hundred.
#
# One line per judgement: the shape it matches, then the document that says the
# rule. The second field is a path relative to this repository and may be a
# memory or a rules file; the coverage reader checks that it exists, so a
# judgement whose rule was renamed away cannot keep running unnoticed.
#
# The names are this side's keys and must be the names the tree publishes --
# `principle_shapes.rs`'s own test pins that direction. Adding a shape without
# adding a line here is the failure this list exists to make visible: the count
# would stay put while the gate grew.
PRINCIPLE_CHECKS=(
    'DEFERRAL_SHAPES|memory/feedback_meta_autonomy_decide_from_principles.md'
    'NARROW_ADD_RE|memory/feedback_shared_tree_add_only_your_own_paths.md'
    'BARE_COMMIT_RE|memory/feedback_shared_tree_add_only_your_own_paths.md'
    'DESTRUCTIVE_GIT_RE|personal/CLAUDE.md'
    'AI_TRAILER_RE|memory/feedback_no_claude_attribution.md'
    'FROZEN_DOC_RE|memory/feedback_an_extracted_document_is_frozen_new_rounds_get_a_new_dated_doc.md'
    'CIRCLED_CHARS|memory/feedback_numbering_and_plain_language.md'
)

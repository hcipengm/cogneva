#!/usr/bin/env bash
#
# Comment cites a document — the prose rule "a code comment does not reference any
# document" turned into a reading.
#
# The rule serves the person at the call site, and a citation is exactly what that
# person cannot use: the private design, audit and record documents live outside
# this repository, so a comment naming one is a dead pointer for anyone who clones
# the tree, and a chapter number points into the same unreadable place. What the
# comment has to carry instead is the reason itself — so the fix for a hit is to
# say the why in place, not to reword the citation.
#
# The reading is taken from the artefact the rule is about: the comment lines of
# the tree's Rust sources. Code is not read, because a `.md` string in a program
# is a path the program means rather than a document a comment points at; the
# tree is full of those (`index.md`, `SKILL.md`, `docs/quickstart.md` in tests) and
# a rule that read them would be noise rather than a judgement.
#
# Three shapes count as a citation, and the set is closed on purpose — a shape
# that is not here is not judged, so the shapes that are here have to be the ones
# a private document can actually arrive as:
#   - `internal-docs`, the directory those documents live in;
#   - a `YYYY-MM-DD_name.md` file name, which is the form every one of them has;
#   - a `§` section sign, which is how a chapter number is written in this tree.
# One exemption, narrowing the strict side: a `§` inside a citation of a published
# external standard (`RFC 7230 §4.1`) names a document the reader can obtain, and
# an implementation of a wire format has no other way to cite the grammar it
# follows. Only those citations are lifted before the `§` shape is judged.
#
# One boundary of the read, stated rather than left to be discovered: a comment
# line is one that opens with `//`, `///`, `//!` or `/*`, or continues a block
# comment with a `*`. A block comment's bare continuation line -- no `*` prefix --
# is therefore not read. Telling those lines from code needs a lexer rather than a
# pattern, and this tree writes `//`, so the boundary is cheap to hold: a citation
# that lands only there is missed, and this is the line that says so.
#
# Usage: check-comment-doc-references.sh [file ...]
#        (default: every Rust source the tree tracks)
set -uo pipefail

fail() { echo "FAIL: $*" >&2; exit 1; }

# Discovery rather than a list on the default path: a hand-kept list is what goes
# quiet the day a file is added to it.
if [ "$#" -gt 0 ]; then
  files=("$@")
else
  command -v git >/dev/null || fail "git is needed to find the tree's Rust sources"
  git rev-parse --show-toplevel >/dev/null 2>&1 || fail "not inside a git work tree"
  mapfile -t files < <(git ls-files '*.rs')
fi
[ "${#files[@]}" -gt 0 ] || fail "no Rust source to read"

# 读不出来不是「没有引用」：这一条判的是命中的存在，而一个读不出的文件交出来的
# 是零命中——与「这个文件干净」在输出上完全同形。所以先把 grep 自己的退出码收下：
# 大于 1 是它读不动（权限、路径是目录、名字不存在），按红算；退出码 1 才是「没有
# 命中」，那正是这条门要找的绿。
comment_lines=""
grep_status=0
comment_lines="$(grep -nH -E '^[[:space:]]*(//|/\*|\*)' "${files[@]}" 2>&1)" || grep_status=$?
if [ "${grep_status}" -gt 1 ]; then
  fail "读不出这些文件（grep 退出 ${grep_status}）：${comment_lines}——没读到就判不了「有没有引用」"
fi

hits="$(
  printf '%s\n' "${comment_lines}" \
    | sed -E 's@\b(RFC|ISO|IEEE|ECMA)[ -]?[0-9A-Za-z._-]*[[:space:]]*§[0-9A-Za-z._-]*@ @g' \
    | grep -E 'internal-docs|[0-9]{4}-[0-9]{2}-[0-9]{2}_[^ ]*\.md|§' || true
)"

if [ -n "${hits}" ]; then
  {
    echo "代码注释引用了文档。文档在仓库之外，读代码的人跟不过去——"
    echo "修法是把 why 就地写清，不是把引用换个说法。命中："
    echo "${hits}"
  } >&2
  exit 1
fi

echo "OK: nothing in a Rust comment cites a document"

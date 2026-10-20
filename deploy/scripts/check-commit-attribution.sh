#!/usr/bin/env bash
#
# Commit attribution — the prose rule "commit messages are English, state why, and
# carry no AI attribution" turned into a reading.
#
# It is the shape of failure that no reading of the tree can reach. The trailer
# lives in the commit message: the diff is exactly what the author meant it to be,
# the files are clean, review reads the diff and not the body, and the one place
# the attribution is visible — the message — is the one nobody opens again. By the
# time anyone notices, the commit is in the history that the release tag and every
# clone carry, and the only repair left is rewriting that history.
#
# So the read is taken while the repair is still a message: the range this push
# adds, not the whole history. A whole-history scan would be green today and
# permanently red after the first slip, and a permanently red gate is one that
# gets deleted — it converts a one-commit mistake into a choice between rewriting
# published history and dropping the check. Judging the new commits costs nothing
# and refuses the thing while it is still cheap to refuse.
#
# The judgement is stated so that a legitimate message cannot trip it, because a
# false positive here is exactly the expensive case the sentence above is about.
# Two anchors do that work:
#   - `Co-authored-by:` counts only as a trailer — at the start of a line — and
#     only when the same line names Claude, Anthropic, or this platform's own
#     self-evolution identity. A commit message that discusses the attribution
#     rule in prose contains the phrase too, and it is not an attribution. The
#     platform's identity is on the list because a bot name in the trailer is
#     the same rule with a different name -- and it was live: the evolution
#     deployer signed every self-generated commit that way until it was fixed.
#   - `Generated with` counts only when the same line names Claude, which is what
#     the generated-file footer does; `Generated with cargo fmt` is a description
#     of how a file was produced, not an author.
# The emoji and the tool's own URL are judged anywhere on a line: neither has any
# other reason to appear in a commit message.
#
# Usage: check-commit-attribution.sh <rev-range> [<rev-range> ...]
#        e.g. check-commit-attribution.sh origin/main..HEAD
#             check-commit-attribution.sh <base-sha>..<head-sha>
set -uo pipefail

# Exit 2 is "this could not be read", exit 1 is "the commits are attributed". The
# two are kept apart because they call for different repairs: one is an
# environment or invocation problem, the other is a message to rewrite.
unreadable() { echo "FAIL: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# No range means no reading, and a gate that reads nothing agrees with everything.
[ "$#" -gt 0 ] || unreadable "no revision range given: pass the commits to judge, e.g. origin/main..HEAD"

# An unreadable range is a refusal rather than a pass -- the same reason.
command -v git >/dev/null || unreadable "git is needed to read commit messages"
git log -1 --format=%H "$@" >/dev/null 2>&1 \
  || unreadable "the range $* cannot be resolved in this repository; refusing rather than passing \
by reading nothing"

# Same reason again, one step further in: a range that resolves and holds no
# commit is a green gate that judged nothing. CI's triggers cannot produce one --
# a push always adds a commit, and a pull request always has one ahead of its
# base -- so a zero here means the range was computed wrong, and the failure is
# worth more than the silence.
shas="$(git log --format=%H "$@")"
[ -n "${shas}" ] \
  || unreadable "the range $* resolves to no commit: nothing would be judged, and a gate that \
reads nothing agrees with everything"

# One line per offending commit, each naming the commit and the trailer it carries.
offenders="$(
  printf '%s\n' "${shas}" | while read -r sha; do
    [ -n "${sha}" ] || continue
    body="$(git log -1 --format=%B "${sha}")"
    hit="$(printf '%s\n' "${body}" \
      | grep -nEi '^[[:space:]]*co-authored-by:.*(claude|anthropic|cogneva self-evolution|self-evolution@cogneva)|^[[:space:]]*generated with.*claude|🤖|claude\.com/claude-code' \
      | head -1)"
    [ -n "${hit}" ] || continue
    printf '%s %s:%s\n' "${sha}" "${hit%%:*}" "${hit#*:}"
  done
)"

if [ -n "${offenders}" ]; then
  {
    echo "提交信息带 AI 署名。署名在 message 里，diff、评审、文件面都看不见，"
    echo "而 history 一旦推出去就只剩重写这一条修法——所以判的是这次推送新增的提交。"
    echo "命中（commit、message 行号、该行）："
    echo "${offenders}"
  } >&2
  exit 1
fi

echo "OK: no commit in the range carries AI attribution"

#!/usr/bin/env bash
# Deterministic gate for "a commit message carries AI attribution".
#
# The failure this pins is silent by construction. The trailer lives in the
# message: the diff is what the author meant, the files are clean, review reads
# the diff, and the message is the one artefact nobody reopens. Nothing fails and
# nothing warns — the attribution is simply in the history that the release tag
# and every clone carry, and the only repair left is rewriting that history.
#
# What is exercised here is the judgement, and the two directions of it are both
# load-bearing. A gate that never fires leaves the rule unenforced; a gate that
# fires on a legitimate message is worse, because the repair it demands is a
# history rewrite, and a check with that price gets deleted rather than fixed.
# So the fixtures pin both: the attribution forms that must be read as hits, and
# the messages that contain the same words without being attributions — prose
# discussing the rule, a tool named after `Generated with`, and the
# `Signed-off-by` trailer that the revert guard's own commits carry.
#
# The range is the second half of the judgement. The commits below are all in one
# history, and the same bad commits are green or red depending only on where the
# range starts; a gate that judged the whole history instead would be permanently
# red after the first slip, which is the state the range exists to avoid. A range
# that cannot be resolved, and the absence of a range, are refusals rather than
# passes — a gate that reads nothing agrees with everything.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-commit-attribution.sh"
fail() { echo "FAIL: $*"; exit 1; }

command -v git >/dev/null || { echo "missing dependency: git" >&2; exit 2; }
[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# A throwaway history, so the messages under judgement are the fixtures and not
# this repository's. Identity is set per-invocation rather than read from the
# machine: a committer who has configured none would otherwise make every case
# below fail for a reason unrelated to what it judges.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
export GIT_AUTHOR_NAME=fixture GIT_AUTHOR_EMAIL=fixture@example.invalid
export GIT_COMMITTER_NAME=fixture GIT_COMMITTER_EMAIL=fixture@example.invalid
git init -q -b main "${work}/hist"
cd "${work}/hist"

# commit <message> — appends an empty commit and prints its sha.
commit() {
  git commit -q --allow-empty -m "$1"
  git rev-parse HEAD
}

# The whole history is built before any verdict is asked for: a range is resolved
# when the checker runs, so a case run against a history that does not yet contain
# the commit it is about would pass without judging anything.
base="$(commit 'feat: land the base')"
prose="$(commit 'docs: state the attribution rule

A commit message must not carry a Co-Authored-By: Claude trailer; this
repository forbids AI attribution anywhere in its history.')"
formatter="$(commit 'chore: import the formatter output

Generated with cargo fmt, nothing else.')"
signoff="$(commit 'fix: undo the bad landing

Signed-off-by: Fixture <fixture@example.invalid>')"
coauthored="$(commit 'feat: add a helper

Co-Authored-By: Claude <noreply@anthropic.com>')"
emoji="$(commit 'feat: add another helper

🤖 Generated with Claude Code')"
generated="$(commit 'feat: add a third helper

Generated with Claude Code (claude.com/claude-code)')"
# The platform signing its own work. This one is not hypothetical: the evolution
# deployer wrote exactly this trailer on every self-generated commit, and a
# pattern that names only the vendor products would have called all of them
# clean -- the rule is about an AI in the trailer, not about one vendor's name.
selfsigned="$(commit 'feat(evolution): apply self-generated change x

Co-Authored-By: Cogneva Self-Evolution <self-evolution@cogneva.ai>')"

# expects <name> <exit-code> [range...]
expect() {
  local name="$1" want="$2"
  shift 2
  local got=0
  "${checker}" "$@" >/dev/null 2>&1 || got=$?
  [ "${got}" -eq "${want}" ] || fail "${name}: exit ${got}, wanted ${want}"
}

# The three attribution forms, each judged on a range that contains it and
# nothing else, so a hit cannot come from a neighbour.
expect trailer_co_authored 1 "${signoff}..${coauthored}"
expect trailer_emoji 1 "${coauthored}..${emoji}"
expect trailer_generated_with 1 "${emoji}..${generated}"
expect trailer_self_evolution 1 "${generated}..${selfsigned}"
# All four at once, which is what a real push reads.
expect all_four_in_one_range 1 "${signoff}..HEAD"

# Same history, same bad commits, a range that stops before them. This pair is
# the whole argument for judging a range rather than the history.
expect commits_outside_the_range_are_not_judged 0 "${base}..${signoff}"

# Each legitimate message judged on its own, for the same reason.
expect prose_mentioning_the_rule 0 "${base}..${prose}"
expect generated_by_a_tool 0 "${prose}..${formatter}"
expect signed_off_by_trailer 0 "${formatter}..${signoff}"

# No range is not "nothing to judge"; an unresolvable range is not a pass; and a
# range that resolves to no commit is the same failure one step further in.
expect no_range_is_refused 2
expect unresolvable_range_is_refused 2 "${signoff}..no-such-branch"
expect empty_range_is_refused 2 "${signoff}..${signoff}"

echo "OK: commit-attribution"

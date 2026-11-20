#!/usr/bin/env bash
# Say whether a commit's bytes have passed CI's own criteria here. Never refuse.
#
# The rule is about what reaches the repository: every revision
# pushed there has passed CI's criteria on this machine first. A commit is not
# a push -- it is a local, amendable, squashable thing that nobody else can see
# yet -- and the push hook asks that question and stops the push when the answer
# is no. This hook used to enforce the rule at the commit, and that was wrong in
# a way that cost whole afternoons: judging takes hours in a build-gate slot, the
# gate stamps the bytes it judged, and a clean-tree run (the shape that records
# all three stamps, including the one a push asks for) requires the commit to
# exist already. Refusing an unjudged commit therefore made the good order
# impossible: commit -> judge -> push became judge -> commit, which leaves the
# tree dirty at judging time and needs `--no-verify` to get the commit in at all.
#
# So this hook reads the same stamp and says what it says -- judged (0), not
# judged or changed since (0, named), cannot tell (0, named) -- and leaves the
# decision to the push, which is where the rule actually bites.
#
# What it still buys: a session that commits sees, at the moment it commits,
# that these bytes have no green stamp, so a forgotten verification is visible
# before it becomes a stack of unjudged commits.
set -u

top="$(git rev-parse --show-toplevel 2>/dev/null)" || top=""
gate="deploy/scripts/check-before-push.sh"

if [ -z "$top" ] || [ ! -f "$top/$gate" ]; then
    printf 'pre-commit（CI 判据）: 这棵树里没有 %s，这一条不适用\n' "$gate" >&2
    exit 0
fi

# The commit asks a different question than the push: "are the bytes I am about
# to commit judged green", not "is HEAD judged green". At commit time the edit is
# still in the working tree, so a HEAD-keyed stamp can never answer it -- and it
# is worse than silence, because a stamp taken on a clean tree keeps answering
# "yes" while the tree changes under it. `--status-content` keys the stamp on the
# bytes the working tree would commit. Trees whose gate predates that question
# are asked the old one, so a checkout in the middle of the rollout is not made
# worse: probe the script's bytes, never run an unknown flag (an older gate runs
# the whole suite on an unrecognised argument).
status_flag=--status
grep -q -- '--status-content' "$top/$gate" && status_flag=--status-content

out="$(
    cd "$top" || exit 1
    bash "$gate" "$status_flag" 2>&1
)"
rc=$?
case "$rc" in
0)
    printf 'pre-commit（CI 判据）: %s\n' "$(printf '%s' "$out" | head -n 1)" >&2
    exit 0
    ;;
1)
    printf '%s\n' "$out" >&2
    printf 'pre-commit（CI 判据）: 这棵树的这份字节还没验过，这一笔照常提交。\n' >&2
    printf '  验证是一步独立的命令，不挂在提交上：提交之后跑一遍整门，它在干净树上\n' >&2
    printf '  一次记齐三把账；推之前 pre-push 会问同一件事，没验过的那一推会被拦下。\n' >&2
    exit 0
    ;;
*)
    printf 'pre-commit（CI 判据）: 这一条今天判不了（门退出码 %s），不拦 —— CI 仍会判推上去的树\n' \
        "$rc" >&2
    exit 0
    ;;
esac

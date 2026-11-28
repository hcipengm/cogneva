#!/usr/bin/env bash
# Refuse a commit that would be signed with this machine's shared git identity
# instead of the name of the digital employee making it.
#
# Why at the commit: `user.name` has exactly one home, the shared `.git/config`,
# and every session on this host commits from a worktree of the same repository.
# Whichever session wrote that file last renames everyone after it -- on
# 2026-10-27 six commits went out as `t <t@example.com>`, the fixture identity a
# branch test had written into the shared config, and the real authors were
# unrecoverable from history. A commit is signed
# by the person who made it. The name is not a table here; it comes from whoever
# is making the commit, in the first form that exists:
#
#   * `COGNEVA_AGENT_NAME` -- the platform's own agents do not run inside Claude
#     Code, so they have neither `CLAUDE_*` variables nor a session record on
#     disk. The side that starts them knows the name and hands it over, which is
#     cheaper than this hook guessing it from somewhere else.
#   * the Claude Code session record -- `~/.claude/sessions/<pid>.json`, pid from
#     `CLAUDE_PID`, cross-checked against `CLAUDE_CODE_SESSION_ID`, the same
#     source used to attribute actions to a session.
#
# "Cannot judge" does not block, and says so: a commit made by the deployer, a
# cron script or the platform's own landing has no Claude session id, and the
# gate is silent there because the question does not apply. A session id that
# cannot be resolved to a record is different -- that prints a named line and
# still passes, because a machine that cannot answer is not a finding about the
# commit.
#
# Escape hatch: `git commit --no-verify`, with the reason in the message.
set -u

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gate="$here/../gates/check-commit-author-is-the-session-employee.sh"
[ -f "$gate" ] || {
    printf 'pre-commit（作者署名）: 这一条今天判不了，不拦 —— 判据脚本不在本树里\n' >&2
    exit 0
}

bash "$gate"
rc=$?
case "$rc" in
0) exit 0 ;;
1) exit 1 ;;
*)
    printf 'pre-commit（作者署名）: 这一条今天判不了，不拦 —— 判据退出码 %s\n' "$rc" >&2
    exit 0
    ;;
esac

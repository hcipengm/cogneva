#!/usr/bin/env bash
# 署名那道门自己的失败标本：共享身份被写坏的时候，红的是**提交**，不是事后的考古。
#
# 那道门判的是「这一笔要用谁的名字署名」：本会话那位数字员工的，不是共享 `.git/config`
# 里最后被谁写进去的那个。它只在**登记过**的项目仓里生效（那才是几个会话共写一份
# config 的现场）——所以这里的夹具把仓登记进 `COGNEVA_PROJECTS_TSV`。
#
# 三格：
#   - 名字对得上 ⇒ 放行；
#   - config 被写成别的名字（2026-10-27 现场：夹具把共享 config 写成 `t`）⇒ 必须拒；
#   - 没有名字可判（不是会话发起、平台也没交名字）⇒ 不阻，而且不印任何话
#     （这条判据在这里不适用，一片静默才是对的）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
gate="$root/scripts/git-hooks/gates/check-commit-author-is-the-session-employee.sh"
[ -f "$gate" ] || { echo "FAIL: 署名门不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/repo"
mkdir -p "$tree"
git -C "$tree" init -q
echo hi >"$tree/a.txt"
git -C "$tree" add -A >/dev/null
git -C "$tree" -c user.email=t@t -c user.name=t commit -qm base

projects="$tmp/projects.tsv"
printf 'fixture\t%s\n' "$tree" >"$projects"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

run() { ( cd "$tree" && env -u GIT_DIR -u GIT_WORK_TREE COGNEVA_PROJECTS_TSV="$projects" "$@" bash "$gate" 2>&1 ); }
set_name() { git -C "$tree" config user.name "$1"; }
git -C "$tree" config user.email t@example.com

# 1. 署名就是这位员工：放行。
set_name "employee-7"
out=$(run COGNEVA_AGENT_NAME=employee-7)
rc=$?
if [ "$rc" -eq 0 ]; then ok "署名对得上时放行"; else bad "署名对得上仍被拒（exit $rc）：$out"; fi

# 2. config 被写成别人（共享身份）：必须拒，并点名两个名字。
set_name "t"
out=$(run COGNEVA_AGENT_NAME=employee-7)
rc=$?
if [ "$rc" -eq 1 ]; then ok "config 被写坏时提交被拒"; else bad "config 被写坏却放行了（exit $rc）：$out"; fi
case "$out" in
    *t*) ok "拒绝时点名了写在 config 里的那个名字" ;;
    *) bad "拒绝时没有说清署名是谁：$out" ;;
esac

# 3. 没有名字可判：不阻，也不印话（这条判据在这里不适用）。
out=$( ( cd "$tree" && env -u GIT_DIR -u GIT_WORK_TREE -u CLAUDE_CODE_SESSION_ID \
    COGNEVA_PROJECTS_TSV="$projects" bash "$gate" ) 2>&1 )
rc=$?
if [ "$rc" -eq 0 ]; then ok "没有名字可判时没有把提交拦下"; else bad "没有名字可判时拦了提交（exit $rc）"; fi
if [ -z "$out" ]; then ok "不适用时一片静默"; else bad "不适用时印了话（读的人会以为判过）：$out"; fi

# 4. 仓没登记：不适用，放行。
unregistered="$tmp/free"
mkdir -p "$unregistered"
git -C "$unregistered" init -q
git -C "$unregistered" -c user.email=t@t -c user.name=someone commit -q --allow-empty -m base
out=$( ( cd "$unregistered" && env -u GIT_DIR -u GIT_WORK_TREE COGNEVA_AGENT_NAME=employee-7 \
    COGNEVA_PROJECTS_TSV="$projects" bash "$gate" ) 2>&1 )
rc=$?
if [ "$rc" -eq 0 ]; then ok "没登记的仓不判也不拦"; else bad "没登记的仓被拦了（exit $rc）：$out"; fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

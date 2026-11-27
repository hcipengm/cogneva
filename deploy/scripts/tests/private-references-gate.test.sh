#!/usr/bin/env bash
# 私有标识门自己的失败标本：名单缺席时它说的是「没看」，不是「通过」。
#
# 那道门守的是「公开树里不许出现私有仓的名字」，名单**从私有仓自己身上推**（目录名与
# origin 仓名），所以名单推不出来时它必须报「判不了」——否则在没有私有仓的机器上它会
# 安静地通过每一次提交，而「一处也没有」与「手里根本没有名单」在输出上一模一样。
#
# 三格：
#   - 没配私有仓 ⇒ 判不了（退 3），并且明说没有被判定；
#   - 配了、暂存区里有私有仓名 ⇒ 拒（退 1）；
#   - 配了、暂存区里干净 ⇒ 放行（退 0）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
gate="$root/scripts/git-hooks/gates/check-private-references.sh"
[ -f "$gate" ] || { echo "FAIL: 私有标识门不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
public="$tmp/publicrepo"
private="$tmp/privatevault"
mkdir -p "$public" "$private"
git -C "$public" init -q
git -C "$private" init -q
printf 'hello\n' >"$public/readme.txt"
git -C "$public" add readme.txt >/dev/null

# 本机配置整份摘掉：判的是这道门，不是这台机器上碰巧配了什么。
isolate=(env -u COGNEVA_PRIVATE_ROOT -u GIT_DIR -u GIT_WORK_TREE
    GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null)

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 1. 没配私有仓：判不了，不是干净。
out=$( ( cd "$public" && "${isolate[@]}" bash "$gate" ) 2>&1 )
rc=$?
if [ "$rc" -eq 3 ]; then ok "名单推不出来时报判不了（退 3）"; else bad "名单推不出来时没有报判不了（exit $rc）：$out"; fi
case "$out" in
    *"没有被判定"*) ok "判不了时明说了这一次没判" ;;
    *) bad "判不了时没有说话，读的人会当成通过：$out" ;;
esac

# 2. 配了私有仓、暂存区里写着它的名字：必须拒。
printf 'see the privatevault repo for the readings\n' >"$public/notes.md"
git -C "$public" add notes.md >/dev/null
out=$( ( cd "$public" && "${isolate[@]}" COGNEVA_PRIVATE_ROOT="$private" bash "$gate" ) 2>&1 )
rc=$?
if [ "$rc" -eq 1 ]; then ok "暂存区里有私有仓名时拒绝"; else bad "暂存区里写着私有仓名却放行了（exit $rc）：$out"; fi
case "$out" in
    *privatevault*) ok "拒绝时点名了命中的那个名字" ;;
    *) bad "拒绝时没有点名命中什么：$out" ;;
esac

# 3. 配了、暂存区干净：放行。
git -C "$public" rm --cached -q notes.md
rm "$public/notes.md"
out=$( ( cd "$public" && "${isolate[@]}" COGNEVA_PRIVATE_ROOT="$private" bash "$gate" ) 2>&1 )
rc=$?
if [ "$rc" -eq 0 ]; then ok "暂存区干净时放行"; else bad "暂存区干净却拒了（exit $rc）：$out"; fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

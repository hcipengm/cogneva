#!/usr/bin/env bash
# 同一份字节只买一次编译：垫片按账本里已有的事实判，不给 `Split` 留口子。
#
# 判的是「改一段编一段」这条习惯在读数上长什么样：同一棵树、同一个 HEAD、同一种
# 编译命令出现第二次 ⇒ 拒，除非头提交写了 `Recompiled: <为什么上一次不够>`。
# `build` 与 `test` 是两种命令，各一次不算重复；`check` 不产出可推的字节，不受限。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
shim="$root/scripts/hooks/cargo"
[ -f "$shim" ] || { echo "FAIL: 垫片不在：$shim" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree"
git -C "$tree" init -q
git -C "$tree" config user.email t@t
git -C "$tree" config user.name t
echo hi >"$tree/a.txt"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base

ledger="$tmp/ledger.tsv"
pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

run() { # $@ = cargo 参数
  ( cd "$tree" && REAL_CARGO=/bin/true COGNEVA_COMPILE_LEDGER="$ledger" bash "$shim" "$@" 2>&1 )
}

out=$(run test); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "第一次 test：放行" || bad "第一次 test 被拒（rc=$rc）：$out"

out=$(run test); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "不再跑第二次"; } \
  && ok "同一 HEAD 上第二次 test：拒，并说清修法" \
  || bad "同一 HEAD 上第二次 test 没被拒（rc=$rc）：$out"

out=$(run build); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "同一 HEAD 上换一种命令（build）：放行" || bad "build 被误拒（rc=$rc）：$out"

out=$(run test -p cog-core); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "同一 HEAD 上另一条命令（不同目标）：放行" || bad "不同目标的 test 被误拒（rc=$rc）：$out"

out=$(run check); rc=$?
out2=$(run check); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "check 不受限：编辑期随时可用" || bad "check 被拒（rc=$rc）：$out2"

git -C "$tree" commit -q --allow-empty -m "another batch"
out=$(run test); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "新 HEAD（先提交）：test 再次放行" || bad "新 HEAD 上的 test 被拒（rc=$rc）：$out"

git -C "$tree" commit -q --allow-empty -m "a retry" -m "Recompiled: 上一次跑的是旧镜像，红的与本批无关"
out=$(run test); rc=$?
out2=$(run test); rc=$?
{ [ "$rc" -eq 0 ]; } && ok "头提交写了 Recompiled：同一 HEAD 再跑一次放行" || bad "Recompiled 没有放行（rc=$rc）：$out2"

printf '\none-batch-one-compile.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

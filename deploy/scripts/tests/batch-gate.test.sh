#!/usr/bin/env bash
# `scripts/hooks/batch-gate.py` 的离线测试：夹具是一棵小仓 + 一张义务清单。
#
# 判的是「默认全批、拆分要举证、而举证要被判真假」：
#   1. 义务没做完、提交里没有 Split ⇒ 红。
#   2. Split: 1 但两侧落在同一批 cargo 包 ⇒ 红（验证面其实相同）。
#   3. Split: 3 但一批里没有一处碰 deploy/migrations/scripts/host ⇒ 红。
#   4. Split: 4 但这一侧义务估算没超过窗口份额 ⇒ 红。
#   5. Split: 2 没点名重叠的测试目标 ⇒ 红；点名了 ⇒ 绿。
#   6. 义务全部有证据 ⇒ 绿，不看 Split。
set -uo pipefail
# 夹具仓通过 `git -C <dir>` 或子 shell 的 `cd` 写，两者都不改调用方的仓：可是外层
# 带进来的绝对 `GIT_DIR` / `GIT_WORK_TREE` / `GIT_INDEX_FILE` 会让每一次调用写进**调用方**
# 的仓。一次清干净，文件作用域，后面的用例都覆盖。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
gate="$root/scripts/hooks/batch-gate.py"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks" "$tree/crates/a/src" "$tree/crates/b/src" "$tree/deploy"
cp "$gate" "$tree/scripts/hooks/batch-gate.py"
git -C "$tree" init -q; git -C "$tree" config user.email t@t; git -C "$tree" config user.name t
echo a >"$tree/crates/a/src/x.rs"; echo b >"$tree/crates/b/src/y.rs"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base; git -C "$tree" branch -f origin/main HEAD

printf '%s\n' "principle: one" "principle: two" "principle: three" >"$tmp/owed.txt"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

commit_with() { # $1 = 提交信息
  echo "# touch" >>"$tree/crates/a/src/x.rs"
  git -C "$tree" commit -q --allow-empty -F - < <(printf '%s\n' "$1")
}
run() { COGNEVA_CONTEXT_WINDOW=${WINDOW:-0} python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main --obligations "$tmp/owed.txt" 2>&1; }
reset() { git -C "$tree" reset -q --hard origin/main; }

commit_with "work on one

principle: one"; out=$(run); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "Split"; } && ok "没做完又没有 Split：红" || bad "没做完没 Split 却没红（rc=$rc）：$out"
reset

commit_with "split by surface

principle: one
Split: 1 — crates/a | crates/b"; out=$(run); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "同一批 cargo 包"; } && ok "Split: 1 但两侧同包：红" || bad "Split: 1 同包没红（rc=$rc）：$out"
reset

commit_with "split by irreversibility

principle: one
Split: 3 — 只动代码"; out=$(run); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "没有一处碰"; } && ok "Split: 3 但没碰不可逆面：红" || bad "Split: 3 没红（rc=$rc）：$out"
reset

WINDOW=100000  # 窗口给足，义务正文远小于份额 ⇒ 第 4 条不成立
commit_with "split by volume

principle: one
Split: 4 — 一批读不完"; out=$(run); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "窗口份额"; } && ok "Split: 4 但量不够：红" || bad "Split: 4 没红（rc=$rc）：$out"
WINDOW=0
reset

commit_with "split by attribution

principle: one
Split: 2 — 会在同一批里互相掩盖"; out=$(run); rc=$?
{ [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "点名会重叠的测试目标"; } && ok "Split: 2 没点名测试目标：红" || bad "Split: 2 没红（rc=$rc）：$out"
reset

commit_with "split by attribution

principle: one
Split: 2 — crates/cog-gateway/tests/foo_test.rs 会同时红"; out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "Split: 2 点名了测试目标：绿" || bad "Split: 2 点名仍被拒（rc=$rc）：$out"
reset

echo "// principle: one" >>"$tree/crates/a/src/x.rs"
echo "// principle: two" >>"$tree/crates/b/src/y.rs"
echo "// principle: three" >>"$tree/deploy/x.sh"
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm "evidence for all three"
out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "义务全部有证据：绿，不看 Split" || bad "全部有证据仍被拒（rc=$rc）：$out"

printf '\nbatch-gate.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

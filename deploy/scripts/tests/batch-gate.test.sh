#!/usr/bin/env bash
# `scripts/hooks/batch-gate.py` 的离线测试：夹具是一棵小仓 + 一份小台账。
#
# 判的是「默认做完整批，没做的要逐条点名」：
#   1. 有未完成待办、提交里没有 Split ⇒ 红，并把它列出来。
#   2. Split 点名了别的待办、没点名它 ⇒ 红（没点名的排除不算排除）。
#   3. Split 点名了它、理由在这一批里成立 ⇒ 绿。
#   4. 本轮的待办都动到了 ⇒ 绿，不看 Split。
#   5. 台账读不到 ⇒ 退 2（没判），不是绿。
set -uo pipefail
# 夹具仓通过 `git -C <dir>` 写，外层带进的绝对 GIT_DIR 会让它写进调用方的仓。
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_COMMON_DIR COGNEVA_COMMIT_INDEX
root=$(git rev-parse --show-toplevel)
gate="$root/scripts/hooks/batch-gate.py"
[ -f "$gate" ] || { echo "FAIL: 门脚本不在：$gate" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tree="$tmp/tree"
mkdir -p "$tree/scripts/hooks" "$tree/crates/a/src" "$tree/crates/b/src" "$tree/deploy"
cp "$gate" "$tree/scripts/hooks/batch-gate.py"
echo a >"$tree/crates/a/src/x.rs"; echo b >"$tree/crates/b/src/y.rs"; echo d >"$tree/deploy/manifest.yaml"
git -C "$tree" init -q; git -C "$tree" config user.email t@t; git -C "$tree" config user.name t
git -C "$tree" add -A >/dev/null; git -C "$tree" commit -qm base; git -C "$tree" branch -f origin/main HEAD

ledger="$tmp/ledger.md"
row() { printf '| %s | 内容 | 未落 | 判据 |｜round: current｜touches: %s\n' "$1" "$2"; }
{ row S1 "crates/a/src/x.rs"; row S2 "crates/b/src/y.rs"; row S3 "deploy/manifest.yaml"; } >"$ledger"

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }
reset() { git -C "$tree" reset -q --hard origin/main; }
run() { COGNEVA_TODO_LEDGER="$ledger" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main 2>&1; }
commit_with() { git -C "$tree" commit -q --allow-empty -F - < <(printf '%s\n' "$1"); }

echo a2 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "touch one

nothing else"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "S2"; } && ok "没做完又没有 Split：红，并点名没做的" || bad "没做完也没 Split 却没红（rc=$rc）：$out"
reset

echo a3 >>"$tree/crates/a/src/x.rs"; git -C "$tree" add -A >/dev/null
commit_with "split, wrong todo

Split: 3 — S1 — 点名了已经做了的那条"; out=$(run); rc=$?
{ [ "$rc" -eq 1 ] && printf '%s' "$out" | grep -q "既没做也没被"; } && ok "Split 没点名真正没做的：红" || bad "点名错对象没红（rc=$rc）：$out"
reset

echo a5 >>"$tree/crates/a/src/x.rs"; echo d2 >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null
commit_with "split by irreversibility

Split: 3 — S2 — 这一批要动 deploy 清单，S2 是纯代码，留着下一批"; out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "Split 点名了真正没做的 S2，且这一批碰了 deploy/：绿" || bad "合格的 Split 被拒（rc=$rc）：$out"
reset

echo a4 >>"$tree/crates/a/src/x.rs"; echo b4 >>"$tree/crates/b/src/y.rs"; echo d3 >>"$tree/deploy/manifest.yaml"
git -C "$tree" add -A >/dev/null; commit_with "touch both"
out=$(run); rc=$?
[ "$rc" -eq 0 ] && ok "本轮待办都动到了：绿，不看 Split" || bad "全做完仍被拒（rc=$rc）：$out"

out=$(COGNEVA_TODO_LEDGER="$tmp/missing.md" python3 "$tree/scripts/hooks/batch-gate.py" --root "$tree" --base origin/main 2>&1); rc=$?
[ "$rc" -eq 2 ] && ok "台账读不到：退 2（没判）" || bad "台账读不到时的行为不对（rc=$rc）：$out"

printf '\nbatch-gate.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

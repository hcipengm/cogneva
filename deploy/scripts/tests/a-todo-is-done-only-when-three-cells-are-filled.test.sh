#!/usr/bin/env bash
# 「做完了没有」只认三格（代码 / 上线 / 验证），不认触点。
#
# 2026-11-21 属主定：不能以「触点发现有改动」就算做完。触点是「这一轮该改哪些文件」的
# 提示，它把三种不同的东西读成同一件事——真做完了、改了一半、只是碰巧落在同一条路径上。
# 三格让这三种分开：`代码`（树里有了实现，点名那条没有它就会红的测试）、`上线`（跑着的
# 那一版里有它）、`验证`（判据对着真东西量过）。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
loader="$root/scripts/hooks/todo_source.py"
[ -f "$loader" ] || { echo "FAIL: 加载器不在：$loader" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

run() { python3 -c "
import sys
sys.path.insert(0, '$root/scripts/hooks')
import todo_source
try:
    items = todo_source.load('md:$1')
except todo_source.Unreadable as e:
    print('REJECTED', e)
    raise SystemExit(2)
for i in items:
    print(i.id, 'missing=' + ','.join(i.missing_cells()) if i.missing_cells() else 'complete')
" 2>&1; }

# 1. 一格都没填：这条待办三格全缺，它没做完。
printf '| S1 | 内容 | 未落 | 判据 | touches: crates/a.rs |\n' >"$tmp/none.md"
out=$(run "$tmp/none.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "S1 missing=代码,上线,验证"; } \
    && ok "一格没填：报三格全缺（不是「做了」）" \
    || bad "一格没填的待办没被判成缺格（rc=$rc）：$out"

# 2. 只有 `代码` 格：还缺上线与验证，不是终态。
printf '| S1 | 内容 | 未落 | 判据 代码: crates/x/tests/y.rs | touches: crates/a.rs |\n' >"$tmp/code.md"
out=$(run "$tmp/code.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "S1 missing=上线,验证"; } \
    && ok "只有代码格：仍缺上线与验证" \
    || bad "只填一格被判成终态（rc=$rc）：$out"

# 3. 三格齐：这条待办可以写成已落。
printf '| S1 | 内容 | 已落 | 判据 代码: crates/x/tests/y.rs 上线: abc1234 验证: 2026-11-21 读数 | touches: crates/a.rs |\n' >"$tmp/all.md"
out=$(run "$tmp/all.md"); rc=$?
{ [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -q "REJECTED" && ! printf '%s' "$out" | grep -q "^S1"; } \
    && ok "三格齐的已落行：不拒、不进批次" \
    || bad "三格齐的已落行被误判（rc=$rc）：$out"

# 4. 写着已落却缺格：不进批次、不是做完，但必须出现在「欠终态」的清单上——
#    否则那一行从此不再被任何门看。
printf '| S1 | 内容 | 已落（2026-11-21） | 判据 | touches: crates/a.rs |\n' >"$tmp/liar.md"
out=$(run "$tmp/liar.md"); rc=$?
{ [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -q "^S1"; } \
    && ok "已落却缺格：不进批次" \
    || bad "已落却缺格进了批次（rc=$rc）：$out"
out=$(python3 -c "
import sys
sys.path.insert(0, '$root/scripts/hooks')
import todo_source
print('DEBT', [i.id for i in todo_source.terminal_debt('md:$tmp/liar.md')])
" 2>&1)
{ printf '%s' "$out" | grep -q "DEBT \['S1'\]"; } \
    && ok "已落却缺格：出现在欠终态的清单上" \
    || bad "欠终态的清单没点名它：$out"

# 5. 英文冒号与 `｜` 分隔也要读得出来：写的人不该被标点卡住。
printf '| S1 | 内容 | 已落 | 判据 ｜ 代码： crates/x/tests/y.rs ｜ 上线： abc1234 ｜ 验证： 读数在 z ｜ touches: crates/a.rs |\n' >"$tmp/wide.md"
out=$(run "$tmp/wide.md"); rc=$?
{ [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -q "REJECTED"; } \
    && ok "全角标点/英文冒号：读得出三格" \
    || bad "全角标点被当成缺格（rc=$rc）：$out"

printf 'a-todo-is-done-only-when-three-cells-are-filled.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

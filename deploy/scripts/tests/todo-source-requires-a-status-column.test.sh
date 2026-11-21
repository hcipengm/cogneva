#!/usr/bin/env bash
# `scripts/hooks/todo_source.py` 读台账：一行以 `| S` 开头却没有状态列时，它必须**拒掉**。
#
# 2026-11-20 现场：台账里两行（S39、S33）少了状态列，加载器 `continue` 跳过了它们。
# 结果是一整条待办从批次里消失，而两道门照样数出一个绿数——「读不到」被当成了「没有」，
# 这正是这一层最要防的形状。
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
print('PARSED', len(items))
" 2>&1; }

# 1. 一行没有状态列：拒掉，并点名是哪一行。
printf '| S1 | 内容 | 未落 | 判据 | touches: crates/a.rs |\n| S2 | 只有两列\n' >"$tmp/broken.md"
out=$(run "$tmp/broken.md"); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "S2"; } \
    && ok "没有状态列的行：拒掉并点名（不再静默跳过）" \
    || bad "没有状态列的行被静默跳过（rc=$rc）：$out"

# 2. 状态词不在闭集：同样拒掉。
printf '| S1 | 内容 | 部分 | 判据 | touches: crates/a.rs |\n' >"$tmp/badword.md"
out=$(run "$tmp/badword.md"); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "闭集"; } \
    && ok "第三个状态词：拒掉" \
    || bad "第三个状态词没被拒（rc=$rc）：$out"

# 3. 两行都齐：正常读出两条。
printf '| S1 | 内容 | 未落 | 判据 | touches: crates/a.rs |\n| S2 | 内容 | 未落 | 判据 | touches: crates/b.rs |\n' >"$tmp/good.md"
out=$(run "$tmp/good.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 2"; } \
    && ok "齐整的台账：读出两条" \
    || bad "齐整的台账没读对（rc=$rc）：$out"

# 4. 已落但没填三格：**不进本轮批次**（那是债），但也不读成做完。
printf '| S1 | 内容 | 已落（2026-11-01） | 判据 | touches: crates/a.rs |\n' >"$tmp/closed.md"
out=$(run "$tmp/closed.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 0"; } \
    && ok "已落却缺三格：不进批次" \
    || bad "已落却缺三格进了批次（rc=$rc）：$out"
out=$(python3 -c "
import sys
sys.path.insert(0, '$root/scripts/hooks')
import todo_source
print('DEBT', [i.id for i in todo_source.terminal_debt('md:$tmp/closed.md')])
" 2>&1)
{ printf '%s' "$out" | grep -q "DEBT \['S1'\]"; } \
    && ok "已落却缺三格：进债的清单（不是做完）" \
    || bad "债的清单里没有那一行：$out"

# 4b. 已落且三格齐：不拒，也不进批次。
printf '| S1 | 内容 | 已落 | 代码: crates/x/tests/y.rs 上线: abc1234 验证: 读数 | touches: crates/a.rs |\n' >"$tmp/closed-ok.md"
out=$(run "$tmp/closed-ok.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 0"; } \
    && ok "已落且三格齐：不拒、不进批次" \
    || bad "已落且三格齐被误判（rc=$rc）：$out"

printf 'todo-source-requires-a-status-column.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

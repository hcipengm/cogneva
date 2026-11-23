#!/usr/bin/env bash
# 五格台账行：`| 序号 | 终态 | 现在 | 差距 | 状态 |`（属主 2026-11-23 定）。
#
# 判六件事：
#   1. 差距非空的 `未落` 行进批次，触点照读；
#   2. 差距为空的 `已落` 行不进批次（终态到了）；
#   3. 写着 `已落` 却还留着差距的行按**未完成**读：跳过它等于承认那个假的终态；
#   4. 没有 `touches:` 的行拒读并点名——留空让它「不在批次里」，那是看起来像做完的最坏形状；
#   5. 序号带 `S` 前缀或带子号（`S20.7`）也按五格读：形状由格数定，不由前缀定；
#   6. 五格行把状态写成散文时拒读并点名，不许静默算作「没有这条」。
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
print('PARSED', len(items), ' '.join(i.id for i in items))
" 2>&1; }

# 1. 未落、差距非空、带触点：进批次。
printf '| CC01 | 读数面分得开 | 网关已收口 | 余下 285 处未分诊 ｜ touches: crates/a/src/x.rs | 未落 |\n' >"$tmp/open.md"
out=$(run "$tmp/open.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 1 CC01"; } \
    && ok "未落且差距非空：读出这一条" \
    || bad "五格未落行没读出来（rc=$rc）：$out"

# 2. 已落、差距为空：不进批次。
printf '| CC03 | 书由台账来源导出 | 已落（971c92db5） | — ｜ touches: scripts/hooks/round.py | 已落 |\n' >"$tmp/closed.md"
out=$(run "$tmp/closed.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 0"; } \
    && ok "已落且差距为空：不进批次" \
    || bad "已落行仍进了批次（rc=$rc）：$out"

# 3. 写着已落却留着差距：按未完成读（进 debt，可被 terminal_debt 读到）。
printf '| CC02 | 编译被看见 | 转发器没装 | 转发器还没装 ｜ touches: scripts/hooks/install.sh | 已落 |\n' >"$tmp/lying.md"
out=$(python3 -c "
import sys
sys.path.insert(0, '$root/scripts/hooks')
import todo_source
debt = todo_source.terminal_debt('md:$tmp/lying.md')
print('DEBT', len(debt), ' '.join(i.id for i in debt))
" 2>&1); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "DEBT 1 CC02"; } \
    && ok "已落但差距非空：按未完成读，进 debt" \
    || bad "假的已落被当成终态（rc=$rc）：$out"

# 4. 没有触点：拒读并点名。
printf '| CC04 | 终态 | 现在 | 差距 | 未落 |\n' >"$tmp/notouch.md"
out=$(run "$tmp/notouch.md"); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "CC04"; } \
    && ok "没有触点：拒读并点名" \
    || bad "没有触点的行被静默丢掉（rc=$rc）：$out"

# 5. 序号带 `S` 前缀、带子号：仍按五格读。按前缀分形状的读法会把它们交给三格
#    分支，于是「现在」那一格成了状态词，整族 S 账读不成。
printf '| S20.7 | 终态 | 现在不是状态词 | 差距 ｜ touches: crates/a/src/x.rs | 未落 |\n| S77 | 终态 | 现在 | 差 ｜ touches: crates/b/src/y.rs | 未落 |\n' >"$tmp/prefixed.md"
out=$(run "$tmp/prefixed.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 2 S20.7 S77"; } \
    && ok "S 前缀与带子号的序号按五格读" \
    || bad "S 开头的五格行被当成三格读（rc=$rc）：$out"

# 6. 五格行把状态写成散文：拒读并点名。静默丢掉的读数与「本条不在台账里」同形。
printf '| N07 | 终态 | 现在 | 差距 ｜ touches: crates/a/src/x.rs | 树里：还没做 |\n' >"$tmp/prose.md"
out=$(run "$tmp/prose.md"); rc=$?
{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "N07"; } \
    && ok "五格行状态写成散文：拒读并点名" \
    || bad "状态写坏的 N 行被静默算成没有（rc=$rc）：$out"

# 7. 做到终态的行不需要触点：它不进任何一批，触点对它没有意义。反过来的顺序会把
#    台账里做得最干净的那几行报成「算不出批次」，整本台账因此读不动。
printf '| N09 | 终态 | 现在 | — | 已落 |\n' >"$tmp/reached.md"
out=$(run "$tmp/reached.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 0"; } \
    && ok "已落且差距为空：没有触点也不拒" \
    || bad "做到终态的行因为没有触点被拒（rc=$rc）：$out"

# 8. 旧形状的三格 S 行仍按三格读：状态在第 3 格，不是「末格」。按形状判的那一版
#    如果把这些行当成五格，末格是判据文案，整行会被当成「状态写成散文」拒掉。
printf '| S99 | 旧格式现在格 | 未落 | 判据 ｜touches: crates/a/src/x.rs\n' >"$tmp/legacy.md"
out=$(run "$tmp/legacy.md"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "PARSED 1 S99"; } \
    && ok "旧三格行：状态仍从第 3 格读" \
    || bad "旧三格行被当成五格读（rc=$rc）：$out"

printf 'todo-source-fixed-format.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

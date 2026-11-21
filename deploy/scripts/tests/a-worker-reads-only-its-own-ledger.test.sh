#!/usr/bin/env bash
# 每个 worker 读自己的台账（S62）：同一台机器上两个 worker 各读各的；
# 某一本的路径没了，工具报「读不到」，**不回落**到一份共享清单。
#
# 回落是最坏的一种：批次在两边都算得出来，读数一样绿，而其中一边的活永远没人做。
set -uo pipefail
root=$(git rev-parse --show-toplevel)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

printf '| S1 | 甲 worker 的活 | 未落 | 判据 | touches: crates/a.rs |\n' >"$tmp/89.md"
printf '| S2 | 乙 worker 的活 | 未落 | 判据 | touches: crates/b.rs |\n' >"$tmp/71.md"

read_ids() { # $1 = ledger path (may be missing)
  env -u COGNEVA_TODO_SOURCE -u COGNEVA_TODO_LEDGER COGNEVA_WORKER_LEDGER="$1" \
    python3 -c "
import sys
sys.path.insert(0, '$root/scripts/hooks')
import todo_source
spec = todo_source.default_spec()
try:
    items = todo_source.load(spec)
except todo_source.Unreadable as e:
    print('UNREADABLE', spec); raise SystemExit(0)
print('IDS', ','.join(i.id for i in items))
" 2>&1
}

out=$(read_ids "$tmp/89.md")
{ printf '%s' "$out" | grep -q "IDS S1$"; } && ok "worker 89 读到自己那本" || bad "worker 89 读错了：$out"

out=$(read_ids "$tmp/71.md")
{ printf '%s' "$out" | grep -q "IDS S2$"; } && ok "worker 71 读到自己那本" || bad "worker 71 读错了：$out"

out=$(read_ids "$tmp/gone.md")
{ printf '%s' "$out" | grep -q "UNREADABLE"; } \
  && ok "台账不在：报读不到，不回落" \
  || bad "台账不在时回落到别处了：$out"

# 两个 worker 同时跑，各自拿到自己的批次（同一台机器、同一次调用之间不共享状态）。
( read_ids "$tmp/89.md" >"$tmp/out-a" ) & pid_a=$!
( read_ids "$tmp/71.md" >"$tmp/out-b" ) & pid_b=$!
wait "$pid_a" "$pid_b"
a=$(cat "$tmp/out-a"); b=$(cat "$tmp/out-b")
{ printf '%s' "$a" | grep -q "S1" && printf '%s' "$b" | grep -q "S2"; } \
  && ok "两个 worker 并行：各拿各的" \
  || bad "并行时读串了：$a / $b"

printf 'a-worker-reads-only-its-own-ledger.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

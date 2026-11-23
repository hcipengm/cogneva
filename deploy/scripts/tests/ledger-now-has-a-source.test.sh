#!/usr/bin/env bash
# 台账的「现在」那一格必须有出处：这是带外读法与带内 API 共用的**一条**判据
# （`scripts/hooks/todo_source.py::now_has_source` 与
# `crates/cog-core/src/contract/ledger_item.rs::now_names_its_source`）。
#
# 为什么要有这一条：「现在」说的是从树上读到的此刻，读的人要能照着它去读同一处。
# 写不出出处的那一格不是「现在」，是一句印象——而印象与读数在台账里长得一样，
# 于是「没读」会被当成「读了、没变化」。
#
# 存量只许减：真实台账里今天还有 N 行没有出处，这个数钉在下面，**只许往下走**。
# 每改好一行就把这个数减一；涨上去就红。数字不是目标，是棘轮。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
loader="$root/scripts/hooks/todo_source.py"
fixture="$root/deploy/scripts/tests/fixtures/now-sources.tsv"
[ -f "$loader" ] || { echo "FAIL: 加载器不在：$loader" >&2; exit 1; }
[ -f "$fixture" ] || { echo "FAIL: 夹具不在：$fixture" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 1. 判据本身：拿出处与没出处的两组标本（**与 Rust 那一侧同一份夹具**），两面各判
#    一次，判词必须一致——两面各认一套写法，就会一边收、一边不收，而两边都不报错。
out=$(python3 -I - "$loader" "$fixture" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("todo_source", sys.argv[1])
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
wrong = []
for line in open(sys.argv[2], encoding="utf-8"):
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    want, text = line.split("\t", 1)
    got = "source" if m.now_has_source(text) else "none"
    if got != want:
        wrong.append(f"{want}!={got}: {text}")
print("WRONG", wrong)
PY
)
printf '%s' "$out" | grep -q "WRONG \[\]" \
    && ok "带外的判词与夹具一致（出处／没出处各判对）" || bad "带外判词与夹具不一致：$out"

# 2. 棘轮：真实台账里没有出处的未完成行，不得超过下面这个数。
ledger="${COGNEVA_TODO_LEDGER:-}"
if [ -z "$ledger" ]; then
  host="$HOME/.config/cogneva/todo-ledger"
  [ -f "$host" ] && ledger=$(head -1 "$host")
fi
if [ -z "$ledger" ] || [ ! -f "$ledger" ]; then
  echo "skip  - 这台机器上没有台账，棘轮这一格不判（不是通过）"
else
  unsourced=$(python3 -I - "$loader" "$ledger" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("todo_source", sys.argv[1])
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
items = m.load("md:" + sys.argv[2])
print(sum(1 for i in items if not m.now_has_source(getattr(i, "now", ""))))
PY
)
  # 2026-11-23 实测：78 条未完成里有 63 条没写出处。只许减。
  max_unsourced=63
  if [ "${unsourced:-0}" -le "$max_unsourced" ]; then
    ok "没有出处的未完成行 ${unsourced} 条，不超过棘轮 ${max_unsourced}"
  else
    bad "没有出处的未完成行涨到 ${unsourced} 条（棘轮 ${max_unsourced}）：先把新写的那些补上出处，或把棘轮往下调"
  fi
fi

printf 'ledger-now-has-a-source.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

#!/usr/bin/env bash
# `round.py plan`：整批必须成为编辑的输入——计划漏一条就拒。
#
# 2026-11-20 现场：批次在编辑前只被印出来，没有谁把它变成「编辑必须消费的输入」，
# 于是「同批编辑」在动手那一刻又退回成「我挑的那几条」。这一步把计划挡在编辑之前：
# 覆盖整批、每条至少两种来源形状（或点名只有一种的理由），否则拒。
set -uo pipefail
root=$(git rev-parse --show-toplevel)
round="$root/scripts/hooks/round.py"
[ -f "$round" ] || { echo "FAIL: round.py 不在：$round" >&2; exit 1; }

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

# 只判校验函数本身：不碰事件流，也不开一轮。
check() { python3 - "$1" <<'PY'
import json, sys, pathlib
sys.path.insert(0, "$ROOT/scripts/hooks".replace("$ROOT", sys.argv[0] and __import__("os").environ["ROOT"]))
PY
}

# 直接调用 plan_defects（导入失败就算红）
run_defects() {
  ROOT="$root" python3 - "$1" <<'PY'
import json, os, sys
sys.path.insert(0, os.path.join(os.environ["ROOT"], "scripts", "hooks"))
import round as r
plan = json.load(open(sys.argv[1]))
ids = ["S1", "S2"]
d = r.plan_defects(ids, plan)
print("DEFECTS", len(d))
PY
}

full='{"input_type":"t","invariants":["i"],"counterexamples":["c"],"items":[{"id":"S1","acceptance":[{"shape":"a"},{"shape":"b"}]},{"id":"S2","acceptance":[{"shape":"a"},{"shape":"b"}]}]}'
echo "$full" >"$tmp/full.json"
out=$(run_defects "$tmp/full.json"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "DEFECTS 0"; } && ok "覆盖整批且每条两种形状：过" || bad "合格计划被拒（rc=$rc）：$out"

partial='{"input_type":"t","invariants":["i"],"counterexamples":["c"],"items":[{"id":"S1","acceptance":[{"shape":"a"},{"shape":"b"}]}]}'
echo "$partial" >"$tmp/partial.json"
out=$(run_defects "$tmp/partial.json"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "DEFECTS 1"; } && ok "漏一条待办：拒" || bad "漏条没被拒（rc=$rc）：$out"

oneshape='{"input_type":"t","invariants":["i"],"counterexamples":["c"],"items":[{"id":"S1","acceptance":[{"shape":"a"}]},{"id":"S2","acceptance":[{"shape":"a"},{"shape":"b"}]}]}'
echo "$oneshape" >"$tmp/one.json"
out=$(run_defects "$tmp/one.json"); rc=$?
{ [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "DEFECTS 1"; } && ok "只有一种形状又没有理由：拒" || bad "单一形状没被拒（rc=$rc）：$out"

printf 'round-plan-covers-the-batch.test.sh: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

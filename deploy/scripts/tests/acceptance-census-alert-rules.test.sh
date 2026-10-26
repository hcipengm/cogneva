#!/usr/bin/env bash
# 验收普查四条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：上面那份 promtool 单测判的是「规则会不会响」，而它自己也可能是一枚
# 橡皮图章——一条永远返回绿的夹具，和没有夹具在读数上完全一样。这里把清单改坏四次，
# 每次都必须让那份单测红，并且红在**它该红的那一格**：
#   1. `> 0` 改成 `>= 0`：负对照（stale 一直是 0）必须红——否则「有数就响」也绿；
#   2. `for: 15m` 改成 `for: 15y`：firing 那一格必须红——否则「永远不会亮」也绿；
#   3. 删掉 absent 那条规则：缺席那一格必须红——否则「读数停了没人管」也绿；
#   4. 后加的那条（landed_unjudgeable）同样把 `> 0` 改成 `>= 0`：它自己的负对照
#      必须红——「新规则配了新夹具」不是自明的事，夹具可以是抄来的、也可以是空的。
# 另有两格证明它**不会**白红：真文件必须绿；拿不到 promtool 时必须退 3（看不了 ≠ 一致），
# 不许把「没跑」报成「通过」。
#
# 跑法：bash deploy/scripts/tests/acceptance-census-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/17-prometheusrule-acceptance-census.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { RULES_MANIFEST="$1" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

# 0) 真文件必须绿。拿不到 promtool 时退 3，这一格按「看不了」记红并说明，不冒充通过。
run "$manifest"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${work}/out")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(grep -m1 FAILED "${work}/out")" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面三格改坏清单的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) `> 0` -> `>= 0`：负对照必须红。
sed 's/verdict="stale"}) > 0/verdict="stale"}) >= 0/' "$manifest" >"${work}/m1.yaml"
run "${work}/m1.yaml"
[ "$rc" -eq 1 ] && ok "界写成 >= 0：单测红（负对照抓得住「有数就响」）" \
    || bad "界写成 >= 0 应红" "1" "$rc"

# 2) `for: 15m` -> `for: 15y`：永远不会亮，必须红。
sed 's/^          for: 15m$/          for: 15y/' "$manifest" >"${work}/m2.yaml"
run "${work}/m2.yaml"
[ "$rc" -eq 1 ] && ok "for 写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 15y 应红" "1" "$rc"

# 3) 删掉 absent 那条规则：缺席那一格必须红。
python3 - "$manifest" "${work}/m3.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
i = s.index("        - alert: CognevaAcceptanceCensusAbsent")
j = s.index("        # 三、")
open(out, "w", encoding="utf-8").write(s[:i] + s[j:])
PY
run "${work}/m3.yaml"
[ "$rc" -eq 1 ] && ok "删掉 absent 规则：单测红（缺席不是靠 stale 那条兜的）" \
    || bad "删掉 absent 规则应红" "1" "$rc"

# 4) landed_unjudgeable 那条规则：`> 0` -> `>= 0`，它自己的负对照必须红。
sed 's/max (cogneva_acceptance_landed_unjudgeable) > 0/max (cogneva_acceptance_landed_unjudgeable) >= 0/' \
    "$manifest" >"${work}/m4.yaml"
run "${work}/m4.yaml"
[ "$rc" -eq 1 ] && ok "landed_unjudgeable 写成 >= 0：单测红（新规则也有负对照）" \
    || bad "landed_unjudgeable 写成 >= 0 应红" "1" "$rc"

# 5) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool bash "$runner" >"${work}/out4" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

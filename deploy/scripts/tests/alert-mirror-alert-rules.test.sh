#!/usr/bin/env bash
# 告警镜像四条判词的 promtool 夹具**自己**能不能红。
#
# 为什么要有这一层：上面那份单测判的是「规则会不会响」，而它自己也可能是一枚橡皮图章
# ——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏三次，每次都必须让
# 那份单测红，并且红在它该红的那一格：
#   1. `> 0` 改成 `>= 0`：负对照（序列一直是 0）必须红——否则「有数就响」也绿；
#   2. `for: 15m` 改成 `for: 15y`：firing 那一格必须红——否则「永远不会亮」也绿；
#   3. 删掉「停住了」那条：它那一格必须红——否则「行停住没人管」也绿。
# 另有两格证明它**不会**白红：真文件必须绿；拿不到 promtool 时必须退 3（看不了 ≠ 一致），
# 不许把「没跑」报成「通过」。
#
# 跑法：bash deploy/scripts/tests/alert-mirror-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/15-prometheusrule-host-readings.yaml"
case_file="${repo}/deploy/k3s/observability/tests/15-prometheusrule-alert-mirror.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { bash "$runner" "$1" "$case_file" >"${work}/out" 2>&1; rc=$?; }

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

# 1) `> 0` -> `>= 0`：负对照必须红。只改缺口那一条，其余三条保持原样。
sed 's/max(cogneva_alert_mirror_missing) > 0/max(cogneva_alert_mirror_missing) >= 0/' \
    "$manifest" >"${work}/m1.yaml"
grep -q 'cogneva_alert_mirror_missing) >= 0' "${work}/m1.yaml" || bad "改坏清单那一格没改到（sed 没匹配）" "已改坏" "原样"
run "${work}/m1.yaml"
[ "$rc" -eq 1 ] && ok "界写成 >= 0：单测红（负对照抓得住「有数就响」）" \
    || bad "界写成 >= 0 应红" "1" "$rc"

# 2) `for: 15m` -> `for: 15y`：永远不会亮，必须红。
sed 's/^          for: 15m$/          for: 15y/' "$manifest" >"${work}/m2.yaml"
grep -q 'for: 15y' "${work}/m2.yaml" || bad "改坏 for 那一格没改到（sed 没匹配）" "已改坏" "原样"
run "${work}/m2.yaml"
[ "$rc" -eq 1 ] && ok "for 写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 15y 应红" "1" "$rc"

# 3) 删掉「停住了」那条规则：它那一格必须红。
python3 - "$manifest" "${work}/m3.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
i = s.index("        - alert: CognevaAlertMirrorRowsStale")
j = s.index("            description: \"开着的行靠", i)
j = s.index("\n", j) + 1
open(out, "w", encoding="utf-8").write(s[:i] + s[j:])
PY
grep -q 'CognevaAlertMirrorRowsStale' "${work}/m3.yaml" && bad "删规则那一格没删掉" "已删除" "还在"
run "${work}/m3.yaml"
[ "$rc" -eq 1 ] && ok "删掉「停住了」规则：单测红（那一格不是靠别的规则兜的）" \
    || bad "删掉「停住了」规则应红" "1" "$rc"

# 4) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool bash "$runner" "$manifest" "$case_file" >"${work}/out4" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

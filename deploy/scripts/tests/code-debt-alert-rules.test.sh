#!/usr/bin/env bash
# 代码欠账那一条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：上面那份 promtool 单测判的是「规则会不会响」，而它自己也可能是一枚
# 橡皮图章——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏四处，
# 每次都必须让那份单测红，并且红在**它该红的那一格**（不只看退出码：改坏之后清单若连
# YAML 都不成立了，promtool 一样退 1，那种红什么都没证明）：
#   1. `> 0` 改成 `>= 0`：`rows` 一直是 0 的那两格（一致的、以及只有树脏的）必须红；
#   2. `for: 30m` 改成 `for: 30y`：永远不亮，第 31 分钟那三格必须红，而「一直是 0」
#      的那几格仍然绿——红的落点必须是对称的；
#   3. `max` 改成 `min`：两台宿主一台 0 一台 2 时判据取到 0，那条多宿主的格子必须红，
#      而且**只有那一格**红（单宿主的格子 min 与 max 同值，它们本来就不该替这一格作证）；
#   4. 删掉 `for:` 那一行：一拍抖动当场就亮，第 5 分钟那一格必须红。
#
# 跑法：bash deploy/scripts/tests/code-debt-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/22-prometheusrule-code-debt.yaml"
case_file="${repo}/deploy/k3s/observability/tests/22-prometheusrule-code-debt.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 夹具改的是**清单的副本**，用例文件不动：规则只有一份，测试读的就是它。
out="${work}/out"
run() { RULES_MANIFEST="$1" RULES_CASE="$case_file" bash "$runner" >"${out}" 2>&1; rc=$?; }

# 红必须红在告警本身：promtool 的失败块以这一行起头。清单被改到读不出来时也会退 1，
# 但那一条红说明的是「改坏的人把文件弄坏了」，不是「判据松了」。
caught() { grep -qF 'alertname: CognevaCodeDebtRowDisagreesWithTree, time:' "${out}"; }
has_time() { grep -qF "alertname: CognevaCodeDebtRowDisagreesWithTree, time: $1," "${out}"; }
blocks() { grep -cF 'alertname: CognevaCodeDebtRowDisagreesWithTree, time:' "${out}"; }

run "$manifest"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${out}")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(grep -m1 -E 'FAILED|error' "${out}")" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面四处改坏的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) 界写成 `>= 0`：一致的树、只脏没欠账的树都必须跟着亮。`time: 1h` 是那两格的求值点。
sed 's/max (cogneva_code_debt_rows) > 0/max (cogneva_code_debt_rows) >= 0/' \
    "$manifest" >"${work}/m1.yaml"
grep -q 'cogneva_code_debt_rows) >= 0' "${work}/m1.yaml" || bad "改坏没落地" "含 >= 0 的清单" "原样"
run "${work}/m1.yaml"
if [ "$rc" -ne 1 ]; then bad "界写成 >= 0 应红" "1" "$rc"
elif ! caught; then bad "界写成 >= 0：红了，但红不在告警上（清单被弄坏？）" "失败块点名告警" "$(head -3 "${out}")"
elif has_time 1h; then ok "界写成 >= 0：单测红（一直是 0 的那几格跟着亮）"
else bad "界写成 >= 0 应红在 1h 那几格" "time: 1h" "$(head -3 "${out}")"; fi

# 2) `for: 30m` -> `for: 30y`：永远不会亮。第 31 分钟那几格必须红，而 `time: 1h` 那几格
#    （期望空）仍然绿——只断言「红了」的话，一条把整份夹具都判红的改坏也过得去。
sed 's/^          for: 30m$/          for: 30y/' "$manifest" >"${work}/m2.yaml"
grep -q 'for: 30y' "${work}/m2.yaml" || bad "改坏没落地" "含 for: 30y 的清单" "原样"
run "${work}/m2.yaml"
if [ "$rc" -ne 1 ]; then bad "for 写成 30y 应红" "1" "$rc"
elif ! caught; then bad "for 写成 30y：红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"
elif has_time 1h; then bad "for 写成 30y 不该动到 1h 那几格" "无 time: 1h" "有"
elif has_time 31m; then ok "for 写成 30y：单测红（抓得住「永远不会亮」）"
else bad "for 写成 30y 应红在 31m 那几格" "time: 31m" "$(head -3 "${out}")"; fi

# 3) `max` -> `min`：多宿主那一格必须红，而且只有它——单宿主的格子 min 与 max 同值，
#    它们替这一格作证就等于没作证（把 `max` 改成 `avg` 也能让它们绿、让这一格红，
#    所以这一格是「取最坏的那台」的唯一证据）。
sed 's/max (cogneva_code_debt_rows)/min (cogneva_code_debt_rows)/' "$manifest" >"${work}/m3.yaml"
grep -q 'min (cogneva_code_debt_rows)' "${work}/m3.yaml" || bad "改坏没落地" "含 min 的清单" "原样"
run "${work}/m3.yaml"
n="$(blocks)"
if [ "$rc" -ne 1 ]; then bad "聚合写成 min 应红" "1" "$rc"
elif ! caught; then bad "聚合写成 min：红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"
elif [ "$n" -ne 1 ]; then bad "聚合写成 min 应只红一格（多宿主那格）" "1 个失败块" "$n 个"
else ok "聚合写成 min：单测红（一台干净不许捂住全队）"; fi

# 4) 删掉 `for:`：一拍抖动当场就亮，第 5 分钟那一格必须红。
awk '! /^          for: 30m$/' "$manifest" >"${work}/m4.yaml"
grep -q '^          for:' "${work}/m4.yaml" && bad "改坏没落地" "不含 for: 的清单" "原样"
run "${work}/m4.yaml"
if [ "$rc" -ne 1 ]; then bad "删掉 for: 应红" "1" "$rc"
elif ! caught; then bad "删掉 for: 红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"
elif has_time 5m; then ok "删掉 for:：单测红（一拍抖动不许报）"
else bad "删掉 for: 应红在 5m 那一格" "time: 5m" "$(head -3 "${out}")"; fi

# 5) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$manifest" RULES_CASE="$case_file" \
    bash "$runner" >"${out}5" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

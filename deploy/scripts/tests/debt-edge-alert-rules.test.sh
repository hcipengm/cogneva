#!/usr/bin/env bash
# 依赖边那两条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：`24-prometheusrule-debt-edge.test.yaml` 判的是「规则会不会响」，而它自己
# 也可能是一枚橡皮图章——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏
# 四处，每次都必须让那份单测红，并且红在**它该红的那一格**（不只看退出码：改坏之后清单若连
# YAML 都不成立了，promtool 一样退 1，那种红什么都没证明）：
#   1. `> 0` 改成 `>= 0`（坏行那条）：一行都没坏的那一格必须红；
#   2. `for: 30m` 改成 `for: 30y`（判不了那条）：第 55 分钟那格必须红；
#   3. `[6h]` 改成 `[10y]`：**只红在 400m 的清场那一格**——这是「响过之后要自己安静」
#      的唯一证据，`> 0` 的写法会在这里红；
#   4. `kind="unmeasured"` 改成 `kind="judged"`：分母被当成判词，响的那几格必须红。
#      它同时证明夹具真的钉住了那个标签选择器，不是只钉住族名。
#
# 跑法：bash deploy/scripts/tests/debt-edge-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/24-prometheusrule-debt-edge.yaml"
case_file="${repo}/deploy/k3s/observability/tests/24-prometheusrule-debt-edge.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 夹具改的是**清单的副本**，用例文件不动：规则只有一份，测试读的就是它。
out="${work}/out"
run() { RULES_MANIFEST="$1" RULES_CASE="$case_file" bash "$runner" >"${out}" 2>&1; rc=$?; }

# 红必须红在告警本身：promtool 的失败块以这一行起头。
red_at() { grep -qF "alertname: $1, time: $2," "${out}"; }
red_any() { grep -qE 'alertname: CognevaDependencyEdge(HookCannotJudge|LedgerUnparsable), time:' "${out}"; }

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

# 1) 坏行那条的界写成 `>= 0`：一行都没坏的那一格（55m）必须跟着亮。
sed 's/max (cogneva_debt_edge_malformed) > 0/max (cogneva_debt_edge_malformed) >= 0/' \
    "$manifest" >"${work}/m1.yaml"
grep -q 'cogneva_debt_edge_malformed) >= 0' "${work}/m1.yaml" || bad "改坏没落地" "含 >= 0 的清单" "原样"
run "${work}/m1.yaml"
if [ "$rc" -ne 1 ]; then bad "界写成 >= 0 应红" "1" "$rc"
elif ! red_any; then bad "界写成 >= 0：红了，但红不在告警上（清单被弄坏？）" "失败块点名告警" "$(head -3 "${out}")"
elif red_at CognevaDependencyEdgeLedgerUnparsable 55m; then ok "界写成 >= 0：单测红（一行没坏的那格跟着亮）"
else bad "界写成 >= 0 应红在 55m 那格" "time: 55m" "$(head -3 "${out}")"; fi

# 2) 判不了那条的 `for:` 写成 30y：第 55 分钟那格必须红。
#    只改这一条的 `for:`，别把坏行那条一起改——一起改的话，红的落点就分不出是哪一条的错。
awk '/alert: CognevaDependencyEdgeHookCannotJudge/ { seen = 1 }
     seen && /^          for: 30m$/ { sub(/30m/, "30y"); seen = 0 }
     { print }' "$manifest" >"${work}/m2.yaml"
grep -q 'for: 30y' "${work}/m2.yaml" || bad "改坏没落地" "含 for: 30y 的清单" "原样"
grep -c '^          for: 30y$' "${work}/m2.yaml" | grep -qx 1 || bad "改坏越界了" "只有一条 for: 30y" "$(grep -c 'for: 30y' "${work}/m2.yaml") 条"
run "${work}/m2.yaml"
if [ "$rc" -ne 1 ]; then bad "for 写成 30y 应红" "1" "$rc"
elif ! red_any; then bad "for 写成 30y：红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"
elif red_at CognevaDependencyEdgeHookCannotJudge 55m; then ok "for 写成 30y：单测红（抓得住「永远不会亮」）"
else bad "for 写成 30y 应红在 55m 那格" "time: 55m" "$(head -3 "${out}")"; fi

# 3) 窗口从 6h 拉到 10y：响过之后再也不安静 ⇒ 只红在 400m 的清场那一格。
#    这一格是「不能写成 > 0」的证据：计数只增不减，界写成 `> 0` 会在这里红。
sed 's/\[6h\]/[10y]/' "$manifest" >"${work}/m3.yaml"
grep -q '\[10y\]' "${work}/m3.yaml" || bad "改坏没落地" "含 [10y] 的清单" "原样"
run "${work}/m3.yaml"
if [ "$rc" -ne 1 ]; then bad "窗口拉长应红" "1" "$rc"
elif ! red_any; then bad "窗口拉长：红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"
# promtool 的失败块印的是人读的时长（400m 写作 `6h40m`，100m 写作 `1h40m`），不是分钟数。
elif red_at CognevaDependencyEdgeHookCannotJudge 6h40m && ! red_at CognevaDependencyEdgeHookCannotJudge 1h40m; then
    ok "窗口拉长：单测红在清场那格（响过之后必须自己安静）"
else bad "窗口拉长应只红在清场那格" "6h40m 红、1h40m 不红" "$(head -3 "${out}")"; fi

# 4) 标签选择器写成分母那一格：判不了那几格必须红。
sed 's/{kind="unmeasured"}/{kind="judged"}/' "$manifest" >"${work}/m4.yaml"
grep -q 'kind="judged"' "${work}/m4.yaml" || bad "改坏没落地" "含 kind=judged 的清单" "原样"
run "${work}/m4.yaml"
if [ "$rc" -ne 1 ]; then bad "读成分母应红" "1" "$rc"
elif ! red_any; then bad "读成分母：红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"
elif red_at CognevaDependencyEdgeHookCannotJudge 55m; then ok "读成分母：单测红（夹具钉住了标签选择器，不只钉住族名）"
else bad "读成分母应红在 55m 那格" "time: 55m" "$(head -3 "${out}")"; fi

# 5) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$manifest" RULES_CASE="$case_file" \
    bash "$runner" >"${out}5" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

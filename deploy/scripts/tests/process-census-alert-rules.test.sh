#!/usr/bin/env bash
# 进程采集器两条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：上面那份 promtool 单测判的是「规则会不会响」，而它自己也可能是一枚
# 橡皮图章——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏四次，
# 每次都必须让那份单测红，并且红在**它该红的那一格**：
#   1. `for: 10m` 改成 `for: 15y`：永远不亮，Down 那一格必须红；
#   2. `<` 改成 `<=`：就绪数与应调度数相等的健康状态也响，负对照那一格必须红；
#   3. `== 0` 改成 `>= 0`：活着的 node-a 跟着响，Silent 那一格必须红；
#   4. 拿不到 promtool 时必须退 3（看不了 ≠ 一致），不许把「没跑」报成「通过」。
#
# 跑法：bash deploy/scripts/tests/process-census-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/20-prometheusrule-process-census.yaml"
case_file="${repo}/deploy/k3s/observability/tests/20-prometheusrule-process-census.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 夹具改的是**清单的副本**，用例文件不动：规则只有一份，测试读的就是它。
run() { RULES_MANIFEST="$1" RULES_CASE="$case_file" bash "$runner" >"${work}/out" 2>&1; rc=$?; }

run "$manifest"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${work}/out")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(grep -m1 -E 'FAILED|error' "${work}/out")" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面三格改坏清单的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) Down 的 `for: 10m` -> `for: 15y`：永远不会亮，必须红。
sed 's/^          for: 10m$/          for: 15y/' "$manifest" >"${work}/m1.yaml"
grep -q 'for: 15y' "${work}/m1.yaml" || bad "改坏没落地" "含 for: 15y 的清单" "原样"
run "${work}/m1.yaml"
[ "$rc" -eq 1 ] && ok "for 写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 15y 应红" "1" "$rc"

# 2) Down 的 `<` -> `<=`：健康状态也响，负对照必须红。
sed 's/^            <$/            <=/' "$manifest" >"${work}/m2.yaml"
grep -q '^            <=$' "${work}/m2.yaml" || bad "改坏没落地" "含 <= 的清单" "原样"
run "${work}/m2.yaml"
[ "$rc" -eq 1 ] && ok "界写成 <=：单测红（健康的 DaemonSet 不许跟着响）" \
    || bad "界写成 <= 应红" "1" "$rc"

# 3) Silent 的 `== 0` -> `>= 0`：活着的节点也响，必须红。
sed 's/            ) == 0$/            ) >= 0/' "$manifest" >"${work}/m3.yaml"
grep -q ') >= 0' "${work}/m3.yaml" || bad "改坏没落地" "含 >= 0 的清单" "原样"
run "${work}/m3.yaml"
[ "$rc" -eq 1 ] && ok "界写成 >= 0：单测红（活着的节点不许跟着响）" \
    || bad "界写成 >= 0 应红" "1" "$rc"

# 4) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$manifest" RULES_CASE="$case_file" \
    bash "$runner" >"${work}/out4" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

#!/usr/bin/env bash
# 构建缓存两条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：上面那份 promtool 单测判的是「规则会不会响」，而它自己也可能是一枚
# 橡皮图章——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏四次，
# 每次都必须让那份单测红，并且红在**它该红的那一格**：
#   1. `> 0` 改成 `>= 0`：负对照（`unmet_bytes` 一直是 0）必须红——否则「有数就响」也绿；
#   2. `for: 15m` 改成 `for: 15y`：永远不亮，够不着那一格必须红；
#   3. `6 *` 改成 `0 *`：只要越界就响，平的负对照必须红——这一格是「界真的是六倍拍距」
#      的唯一证据，结构门只判得出「这条读了拍距」，判不出乘了几；
#   4. 两侧的 `max by (job, dir)` 收成不带 `by` 的 `max`：两份缓存被合成一条，第五格
#      期望的 `job`/`dir` 配不上，必须红。
#
# 跑法：bash deploy/scripts/tests/build-cache-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/21-prometheusrule-build-cache.yaml"
case_file="${repo}/deploy/k3s/observability/tests/21-prometheusrule-build-cache.test.yaml"

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

# 1) 够不着的 `> 0` -> `>= 0`：`unmet_bytes` 一直是 0 的那一格必须红。
sed 's/(cogneva_build_target_unmet_bytes) > 0/(cogneva_build_target_unmet_bytes) >= 0/' \
    "$manifest" >"${work}/m1.yaml"
grep -q 'unmet_bytes) >= 0' "${work}/m1.yaml" || bad "改坏没落地" "含 >= 0 的清单" "原样"
run "${work}/m1.yaml"
[ "$rc" -eq 1 ] && ok "界写成 >= 0：单测红（一直是 0 的那份缓存不许跟着响）" \
    || bad "界写成 >= 0 应红" "1" "$rc"

# 2) 越界计时那条的 `for: 15m` -> `for: 15y`：永远不会亮，必须红。
sed 's/^          for: 15m$/          for: 15y/' "$manifest" >"${work}/m2.yaml"
grep -q 'for: 15y' "${work}/m2.yaml" || bad "改坏没落地" "含 for: 15y 的清单" "原样"
run "${work}/m2.yaml"
[ "$rc" -eq 1 ] && ok "for 写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 15y 应红" "1" "$rc"

# 3) 六倍拍距 -> 零倍：只要越界就响，越界秒数停在 1700 的那一格必须红。
sed 's/> 6 \* max by/> 0 * max by/' "$manifest" >"${work}/m3.yaml"
grep -q '> 0 \* max by' "${work}/m3.yaml" || bad "改坏没落地" "含 0 * 的清单" "原样"
run "${work}/m3.yaml"
[ "$rc" -eq 1 ] && ok "倍数写成 0：单测红（界不是「有数就响」）" \
    || bad "倍数写成 0 应红" "1" "$rc"

# 4) 两侧的 `max by (job, dir)` 收成 `max`：两份缓存被合成一条，身份配不上，必须红。
sed 's/max by (job, dir) (/max (/g' "$manifest" >"${work}/m4.yaml"
grep -q 'max (cogneva_build_target_over_cap_seconds)' "${work}/m4.yaml" \
    || bad "改坏没落地" "含不带 by 的 max" "原样"
run "${work}/m4.yaml"
[ "$rc" -eq 1 ] && ok "聚合丢掉 by：单测红（两份缓存不许被合成一条）" \
    || bad "聚合丢掉 by 应红" "1" "$rc"

# 5) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$manifest" RULES_CASE="$case_file" \
    bash "$runner" >"${work}/out5" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

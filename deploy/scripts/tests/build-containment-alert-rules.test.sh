#!/usr/bin/env bash
# 构建闸外那两条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：上面那份 promtool 单测判的是「规则会不会响」，而它自己也可能是一枚
# 橡皮图章——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏五次，
# 每次都必须让那份单测红，并且红在**它该红的那一格**：
#   1. `CognevaBuildOutsideTheGate` 的 `== 1` 改成 `>= 0`：读数是 0 的那格负对照必须红
#      ——否则「宿主不忙」与「永远在响」在夹具上分不开；
#   2. 同一条的 `for: 30m` 改成 `for: 15y`：firing 那一格必须红——否则「永远不会亮」也绿；
#   3. 同一条去掉 `max()`：两片报信者各响一条，firing 那格（只认一条）必须红——这一格
#      钉的是「报信者标签不进身份」，也是 `check-alert-rule-subjects.sh` 判的同一件事，
#      两处各钉一半：结构门钉写法，这里钉「写法坏了夹具真的会红」；
#   4. `CognevaBuildSliceUnbounded` 的 `== 1` 改成 `>= 0`：负对照那一格必须红；
#   5. 同一条的 `for: 10m` 改成 `for: 15y`：firing 那一格必须红。
# 另有两格证明它**不会**白红：真文件必须绿；拿不到 promtool 时必须退 3（看不了 ≠ 一致），
# 不许把「没跑」报成「通过」。
#
# 改坏的是**清单的副本**，夹具文件不动：规则只有一份，测试读的就是它。改坏的落点按
# `alert:` 名切块定位，不靠 `for: 10m` 这种在整份清单里出现过很多次的裸串。
#
# 跑法：bash deploy/scripts/tests/build-containment-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/15-prometheusrule-host-readings.yaml"
case_file="${repo}/deploy/k3s/observability/tests/15-prometheusrule-build-containment.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

run() { bash "$runner" "$1" "$case_file" >"${work}/out" 2>&1; rc=$?; }

# 按 `- alert: <名字>` 切出那一条规则的块，在块内做**第一次**替换。替换没落地就退 1，
# 让调用方把「改坏没落地」与「改坏了但夹具没红」分开报——两种都是缺陷，但不是同一种。
mutate() { # <src> <dst> <alert> <old> <new>
    python3 -I - "$1" "$2" "$3" "$4" "$5" <<'PY'
import sys

src, dst, alert, old, new = sys.argv[1:6]
lines = open(src, encoding="utf-8").read().splitlines(keepends=True)
out, inside, done = [], False, False
for line in lines:
    stripped = line.lstrip()
    if stripped.startswith("- alert: "):
        inside = stripped == f"- alert: {alert}\n" or stripped == f"- alert: {alert}"
    if inside and not done and old in line:
        out.append(line.replace(old, new, 1))
        done = True
        continue
    out.append(line)
open(dst, "w", encoding="utf-8").write("".join(out))
sys.exit(0 if done else 1)
PY
}

# 0) 真文件必须绿。拿不到 promtool 时退 3，这一格按「看不了」记红并说明，不冒充通过。
run "$manifest"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${work}/out")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(grep -m1 -E 'FAILED|error' "${work}/out")" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面改坏清单的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) 闸外那条的 `== 1` -> `>= 0`：读数为 0 的负对照必须红。
if mutate "$manifest" "${work}/m1.yaml" CognevaBuildOutsideTheGate \
        'max(build_containment_saturated_uncontained) == 1' \
        'max(build_containment_saturated_uncontained) >= 0'; then
    run "${work}/m1.yaml"
    [ "$rc" -eq 1 ] && ok "闸外那条界写成 >= 0：单测红（宿主不忙时不许跟着响）" \
        || bad "闸外那条界写成 >= 0 应红" "1" "$rc"
else
    bad "改坏没落地" "块内含 == 1 的表达式" "原样"
fi

# 2) 闸外那条的 `for: 30m` -> `for: 15y`：永远不会亮，必须红。
if mutate "$manifest" "${work}/m2.yaml" CognevaBuildOutsideTheGate 'for: 30m' 'for: 15y'; then
    run "${work}/m2.yaml"
    [ "$rc" -eq 1 ] && ok "闸外那条 for 写成 15y：单测红（抓得住「永远不会亮」）" \
        || bad "闸外那条 for 写成 15y 应红" "1" "$rc"
else
    bad "改坏没落地" "块内含 for: 30m" "原样"
fi

# 3) 闸外那条去掉 `max()`：两片报信者各响一条，必须红。
if mutate "$manifest" "${work}/m3.yaml" CognevaBuildOutsideTheGate \
        'max(build_containment_saturated_uncontained) == 1' \
        'build_containment_saturated_uncontained == 1'; then
    run "${work}/m3.yaml"
    [ "$rc" -eq 1 ] && ok "闸外那条去掉 max：单测红（每台报信者不许各响一条）" \
        || bad "闸外那条去掉 max 应红" "1" "$rc"
else
    bad "改坏没落地" "块内含 max(...)" "原样"
fi

# 4) slice 那条的 `== 1` -> `>= 0`：读数为 0 的负对照必须红。
if mutate "$manifest" "${work}/m4.yaml" CognevaBuildSliceUnbounded \
        'max(build_containment_slice_unbounded) == 1' \
        'max(build_containment_slice_unbounded) >= 0'; then
    run "${work}/m4.yaml"
    [ "$rc" -eq 1 ] && ok "slice 那条界写成 >= 0：单测红（有配额时不许跟着响）" \
        || bad "slice 那条界写成 >= 0 应红" "1" "$rc"
else
    bad "改坏没落地" "块内含 == 1 的表达式" "原样"
fi

# 5) slice 那条的 `for: 10m` -> `for: 15y`：永远不会亮，必须红。
if mutate "$manifest" "${work}/m5.yaml" CognevaBuildSliceUnbounded 'for: 10m' 'for: 15y'; then
    run "${work}/m5.yaml"
    [ "$rc" -eq 1 ] && ok "slice 那条 for 写成 15y：单测红（抓得住「永远不会亮」）" \
        || bad "slice 那条 for 写成 15y 应红" "1" "$rc"
else
    bad "改坏没落地" "块内含 for: 10m" "原样"
fi

# 6) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool bash "$runner" "$manifest" "$case_file" >"${work}/out6" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

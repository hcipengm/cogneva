#!/usr/bin/env bash
# 「每一面的首读数有没有读者」那条判词的 promtool 夹具**自己**能不能红。
#
# 为什么要有这一层：上面那份单测判的是「规则会不会响」，而它自己也可能是一枚橡皮图章
# ——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏三次，每次都必须
# 让那份单测红，并且红在它该红的那一格：
#   1. `> 0` 改成 `>= 0`（恒真）：第二格负对照必须红——否则「今天没有面缺读者」与
#      「永远在响」在夹具上分不开；
#   2. `for: 2h` 改成 `for: 15y`：第一格 firing 必须红——否则「永远不会亮」也绿；
#   3. 去掉 `max()`：第四格必须红——两片报信者会各响一条，或者把 `job`／`instance`
#      带进身份。这一条钉的是「身份里不许有报信者」，它也是结构门
#      `check-alert-rule-subjects.sh` 判的同一件事，两处各钉一半：结构门钉写法，
#      这里钉「写法坏了夹具真的会红」。
# 另有两格证明它**不会**白红：真文件必须绿；拿不到 promtool 时必须退 3（看不了 ≠ 一致），
# 不许把「没跑」报成「通过」。
#
# 跑法：bash deploy/scripts/tests/face-first-reading-consumers-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/15-prometheusrule-host-readings.yaml"
case_file="${repo}/deploy/k3s/observability/tests/15-prometheusrule-face-consumers.test.yaml"

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

# 1) `> 0` -> `>= 0`：恒真，第二格负对照必须红。
python3 -I - "$manifest" "${work}/m1.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
old = "expr: max(face_first_reading_consumers_unread) > 0"
assert s.count(old) == 1, "锚点不唯一"
open(out, "w", encoding="utf-8").write(s.replace(old, "expr: max(face_first_reading_consumers_unread) >= 0"))
PY
grep -q 'face_first_reading_consumers_unread) >= 0' "${work}/m1.yaml" \
    && ok "改坏那一格改到了（恒真版）" || bad "改坏清单那一格没改到" "已改坏" "原样"
run "${work}/m1.yaml"
[ "$rc" -eq 1 ] && ok "界写成 >= 0：单测红（负对照抓得住「永远在响」）" \
    || bad "界写成 >= 0 应红" "1" "$rc"

# 2) `for: 2h` -> `for: 15y`：永远不会亮，必须红。
python3 -I - "$manifest" "${work}/m2.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
i = s.index("- alert: CognevaFaceFirstReadingUnread")
j = s.index("for: 2h", i)
open(out, "w", encoding="utf-8").write(s[:j] + "for: 15y" + s[j + len("for: 2h"):])
PY
grep -q 'for: 15y' "${work}/m2.yaml" && ok "改坏 for 那一格改到了" || bad "for 那一格没改到" "已改坏" "原样"
run "${work}/m2.yaml"
[ "$rc" -eq 1 ] && ok "for 写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 15y 应红" "1" "$rc"

# 3) 去掉 `max()`：报信者的标签进身份，两片会各响一条，第四格必须红。
python3 -I - "$manifest" "${work}/m3.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
old = "expr: max(face_first_reading_consumers_unread) > 0"
assert s.count(old) == 1, "锚点不唯一"
open(out, "w", encoding="utf-8").write(s.replace(old, "expr: face_first_reading_consumers_unread > 0"))
PY
grep -q 'expr: face_first_reading_consumers_unread > 0' "${work}/m3.yaml" \
    && ok "去掉 max() 那一格改到了" || bad "去掉 max() 那一格没改到" "已改坏" "原样"
run "${work}/m3.yaml"
[ "$rc" -eq 1 ] && ok "去掉 max()：单测红（身份里混进报信者）" \
    || bad "去掉 max() 应红" "1" "$rc"

# 4) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool bash "$runner" "$manifest" "$case_file" >"${work}/out4" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

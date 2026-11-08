#!/usr/bin/env bash
# 评测臂两条判词的 promtool 夹具**自己**能不能红。
#
# 为什么要有这一层：上面那份单测判的是「规则会不会响」，而它自己也可能是一枚橡皮图章
# ——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这里把清单改坏三次，每次都必须
# 让那份单测红，并且红在它该红的那一格：
#   1. `changes(eval_judged_cases[24h]) == 0` 改成 `>= 0`（恒真）：第三格负对照必须红——
#      否则「停住」与「在涨」在夹具上分不开；
#   2. 卡死那一条的 `for: 1h` 改成 `for: 15y`：第一格 firing 必须红——否则「永远不会亮」也绿；
#   3. `eval_bench_unattributed / eval_bench_cases > 0.1` 改成 `>= 0`：第五格负对照必须红
#      ——否则「一条都归得上」也会被报成「丢了一成」；
#   4. 三天没判出来那一条的 `for: 72h` 改成 `for: 15y`：第七格 firing 必须红——那条规则的
#      全部重量都在这个 `for` 上，改没了它与「永远不响」在夹具上同形。
# 另有两格证明它**不会**白红：真文件必须绿；拿不到 promtool 时必须退 3（看不了 ≠ 一致），
# 不许把「没跑」报成「通过」。
#
# 跑法：bash deploy/scripts/tests/eval-arm-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/15-prometheusrule-host-readings.yaml"
case_file="${repo}/deploy/k3s/observability/tests/15-prometheusrule-eval-arm.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 这一批五份输入彼此独立——真文件一份、改坏清单四份，各自一份输入、各自的输出文件，
# 谁也不读谁的结果。
#
# 串行跑是把核闲着。promtool 是**单线程**的（2026-11-08 实测：一次 152.8 秒、CPU 98%，
# 启动 0.04 秒，所以那 153 秒是纯求值），而宿主是 24 核；五份串起来 12.8 分钟，其中
# 12 分钟里 23 个核什么都没干。
#
# 扇出数不写死：它等于这一批要跑的条数，再被核数封顶，两个输入都当场量（`nproc` 与
# 数组长度）。那一行 `读数：扇出 …` 是给人看的——扇出掉到 1 不会让这一格红（那是机器的
# 属性，不是夹具的缺陷），但日志里看得出这一格是并发跑的，还是又悄悄退回串行了。
cores="$(nproc 2>/dev/null || echo 1)"
tags=(base m1 m2 m3 m4)
fanout=$(( ${#tags[@]} < cores ? ${#tags[@]} : cores ))
[ "${fanout}" -ge 1 ] || fanout=1
printf '读数：扇出 %d/%d 条（%d 核）\n' "${fanout}" "${#tags[@]}" "${cores}"

declare -A run_pid run_rc run_manifest
run_manifest[base]="$manifest"
for tag in m1 m2 m3 m4; do run_manifest[$tag]="${work}/${tag}.yaml"; done

# 一批同时起、同时收。`wait` 只等得上一次，所以退出码在这里落进 run_rc，断言的读它，
# 不再碰 pid——重等到一个已经收掉的 pid 拿到的是 127，那会变成一条与夹具无关的红。
run_batch() {
    local tag
    for tag in "$@"; do
        bash "$runner" "${run_manifest[$tag]}" "$case_file" >"${work}/out.${tag}" 2>&1 &
        run_pid[$tag]=$!
    done
    for tag in "$@"; do
        wait "${run_pid[$tag]}"
        run_rc[$tag]=$?
    done
}

# 0) 真文件必须绿。拿不到 promtool 时退 3，这一格按「看不了」记红并说明，不冒充通过。
# 它和下面四条改坏实验同批跑（见 4) 之后那段），断言的顺序仍然先它——早退语义不变。

# 1) `changes(...) == 0` -> `>= 0`：恒真，第三格负对照必须红。
#
# 这条叶子两处出现（池开着那一条，与「三天没判出来」那一条），两处一起改坏才有意义：
# 只改一处时另一处还会拦住负对照，夹具照样红，看上去像这一格在守，其实守的是别处。
# 所以这里数的是两处，不是「唯一」。
python3 - "$manifest" "${work}/m1.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
old = "and max(changes(eval_judged_cases[24h])) == 0"
assert s.count(old) == 2, "锚点不唯一"
open(out, "w", encoding="utf-8").write(s.replace(old, "and max(changes(eval_judged_cases[24h])) >= 0"))
PY
grep -q 'max(changes(eval_judged_cases\[24h\])) >= 0' "${work}/m1.yaml" \
    && ok "改坏那一格改到了（恒真版）" || bad "改坏清单那一格没改到" "已改坏" "原样"

# 2) 卡死那一条的 `for: 1h` -> `for: 15y`：永远不会亮，必须红。
python3 - "$manifest" "${work}/m2.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
i = s.index("- alert: CognevaEvalJudgedStalled")
j = s.index("for: 1h", i)
open(out, "w", encoding="utf-8").write(s[:j] + "for: 15y" + s[j + len("for: 1h"):])
PY
grep -q 'for: 15y' "${work}/m2.yaml" && ok "改坏 for 那一格改到了" || bad "for 那一格没改到" "已改坏" "原样"

# 3) 比值界 `> 0.1` -> `>= 0`：第五格负对照必须红。
python3 - "$manifest" "${work}/m3.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
old = "max(eval_bench_unattributed) / max(eval_bench_cases) > 0.1"
assert s.count(old) == 1, "锚点不唯一"
open(out, "w", encoding="utf-8").write(s.replace(old, "max(eval_bench_unattributed) / max(eval_bench_cases) >= 0"))
PY
grep -q 'max(eval_bench_cases) >= 0' "${work}/m3.yaml" \
    && ok "改坏比值界那一格改到了" || bad "比值界那一格没改到" "已改坏" "原样"

# 4) 「三天没判出来」那一条的 `for: 72h` -> `for: 15y`：永远不会亮，第七格 firing 必须红。
python3 - "$manifest" "${work}/m4.yaml" <<'PY'
import sys
src, out = sys.argv[1], sys.argv[2]
s = open(src, encoding="utf-8").read()
i = s.index("- alert: CognevaEvalArmNoRowsInDays")
j = s.index("for: 72h", i)
open(out, "w", encoding="utf-8").write(s[:j] + "for: 15y" + s[j + len("for: 72h"):])
PY
grep -q 'for: 15y' "${work}/m4.yaml" && ok "改坏三天界那一格改到了" || bad "三天界那一格没改到" "已改坏" "原样"

# 真文件与四条改坏实验都生成好了，一批跑：五份输入各自独立，谁也不读谁的结果。
# 真文件也放进来，是因为它与改坏实验之间同样没有先后——先跑它只是为了让「拿不到
# promtool」时能早退，而那件事这五份会同时给出同一个答案。
batch=()
for tag in "${tags[@]}"; do
    batch+=("${tag}")
    if [ "${#batch[@]}" -ge "${fanout}" ]; then run_batch "${batch[@]}"; batch=(); fi
done
[ "${#batch[@]}" -gt 0 ] && run_batch "${batch[@]}"

case "${run_rc[base]:-127}" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${work}/out.base" 2>/dev/null)" ;;
    *) bad "真文件：单测应为绿" "0" "${run_rc[base]:-127}；$(grep -m1 FAILED "${work}/out.base" 2>/dev/null)" ;;
esac
if [ "${run_rc[base]:-127}" -eq 3 ]; then
    printf '（promtool 拿不到，后面四格改坏清单的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

[ "${run_rc[m1]:-127}" -eq 1 ] && ok "界写成 >= 0：单测红（负对照抓得住「在涨也报」）" \
    || bad "界写成 >= 0 应红" "1" "${run_rc[m1]:-127}；$(tail -1 "${work}/out.m1" 2>/dev/null)"
[ "${run_rc[m2]:-127}" -eq 1 ] && ok "for 写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "for 写成 15y 应红" "1" "${run_rc[m2]:-127}；$(tail -1 "${work}/out.m2" 2>/dev/null)"
[ "${run_rc[m3]:-127}" -eq 1 ] && ok "比值界写成 >= 0：单测红（负对照抓得住「一条都没丢」）" \
    || bad "比值界写成 >= 0 应红" "1" "${run_rc[m3]:-127}；$(tail -1 "${work}/out.m3" 2>/dev/null)"
[ "${run_rc[m4]:-127}" -eq 1 ] && ok "三天界写成 15y：单测红（抓得住「永远不会亮」）" \
    || bad "三天界写成 15y 应红" "1" "${run_rc[m4]:-127}；$(tail -1 "${work}/out.m4" 2>/dev/null)"

# 5) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool bash "$runner" "$manifest" "$case_file" >"${work}/out4" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

#!/usr/bin/env bash
# 变更账本停摆那一条告警的**夹具自己**能不能红。
#
# 为什么要有这一层：上面那份 promtool 单测判的是「规则会不会响」，而它自己也可能是一枚
# 橡皮图章——一条永远返回绿的夹具，与没有夹具在读数上完全一样。这一条是合取，最容易被
# 改松，所以每一处改坏都要求红在**它该红的那一格**（不只看退出码：清单改到读不出来时
# promtool 一样退 1，而那种红什么都没证明）：
#   1. 左半 `> 0` 改成 `>= 0`：账本空着那一格必须红；
#   2. 右半 `== 1` 改成 `>= 0`：池子好好那一格必须红——六格里那一半一直是 0，`>= 0` 恒真；
#   3. `for: 6h` 改成 `for: 6y`：永远不亮，第 6 小时那一格必须红，而第 12 小时那几格仍然绿；
#   4. `and on()` 改成 `or`：合取变析取，账本空着那一格与池子好好那一格都要红；
#   5. `max` 改成 `min`：两个副本一个 0 一个 2110 时判据取到 0，多副本那一格必须红，
#      而且**只有它**红。
#
# 每一处改坏都按「第 12 小时的红有几格、第 6 小时 1 分的红有几格」来判——两个数一起
# 才分得开上面五种改法（它们落点各不相同）。**加了一格用例就要重新推这两个数**，
# 这是这份测试唯一要维护的东西，写在红字里。
#
# 跑法：bash deploy/scripts/tests/change-dispatch-alert-rules.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
runner="${repo}/deploy/scripts/test-acceptance-census-alert-rules.sh"
manifest="${repo}/deploy/k3s/observability/manifests/23-prometheusrule-change-dispatch.yaml"
case_file="${repo}/deploy/k3s/observability/tests/23-prometheusrule-change-dispatch.test.yaml"

pass=0; fail=0
ok()  { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
out="${work}/out"

# 夹具改的是**清单的副本**，用例文件不动：规则只有一份，测试读的就是它。
run() { RULES_MANIFEST="$1" RULES_CASE="$case_file" bash "$runner" >"${out}" 2>&1; rc=$?; }
# 红必须红在告警本身：promtool 的失败块以这一行起头。
caught() { grep -qF 'alertname: CognevaChangeLedgerStoppedMoving, time:' "${out}"; }
blocks_at() { grep -cF "alertname: CognevaChangeLedgerStoppedMoving, time: $1," "${out}"; }

# 判一处改坏：红在告警上，且两个求值点的落点数与预期一致。
expect_red() { # <名字> <期望 12h 红格数> <期望 6h1m 红格数>
    local name="$1" want_day="$2" want_six="$3" day six
    if [ "$rc" -ne 1 ]; then bad "$name 应红" "1" "$rc"; return; fi
    if ! caught; then
        bad "$name：红了，但红不在告警上（清单被弄坏？）" "失败块点名告警" "$(head -3 "${out}")"
        return
    fi
    day="$(blocks_at 12h)"; six="$(blocks_at 6h1m)"
    if [ "$day" -ne "$want_day" ] || [ "$six" -ne "$want_six" ]; then
        bad "$name：红的落点不对（加过用例就要重新推这两个数）" \
            "12h ${want_day} 格 / 6h1m ${want_six} 格" "12h ${day} 格 / 6h1m ${six} 格"
        return
    fi
    ok "$name"
}

# 改坏一处：把改好的副本写到 $2，并确认改动真的落到了文件上（写死的 sed 若哪天不再匹配
# 清单，下面那条断言会静默地判真文件——这正是「改坏没落地」要拦的东西）。
mutate() { # <名字> <目标文件> <sed 表达式> <落地后必须出现的串> <落地后必须消失的串>
    sed "$3" "$manifest" >"$2"
    grep -qF -- "$4" "$2" || { bad "$1：改坏没落地" "含「$4」的清单" "原样"; return 1; }
    if [ -n "${5:-}" ] && grep -qF -- "$5" "$2"; then
        bad "$1：改坏没落地" "不含「$5」的清单" "仍含"; return 1
    fi
    run "$2"
}

run "$manifest"
case "$rc" in
    0) ok "真文件：单测绿" ;;
    3) bad "真文件：拿不到 promtool（看不了，不是一致）" "0" "3；$(tail -1 "${out}")" ;;
    *) bad "真文件：单测应为绿" "0" "$rc；$(grep -m1 -E 'FAILED|error' "${out}")" ;;
esac
if [ "$rc" -eq 3 ]; then
    printf '（promtool 拿不到，后面五处改坏的用例都无从判——总数按 1 格算）\n'
    printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
    exit "$fail"
fi

# 1) 左半的界写成 `>= 0`：账本空着那一格跟着亮（12h，1 格）。
mutate "左半界写成 >= 0" "${work}/m1.yaml" \
    's/change_ledger_pending"}) > 0/change_ledger_pending"}) >= 0/' \
    'change_ledger_pending"}) >= 0' && expect_red "左半界写成 >= 0：单测红（账本空着不许报）" 1 0

# 2) 右半的 `== 1` 写成 `>= 0`：六格全真，池子好好那一格与停摆恢复之后那一格都亮
#    （12h，两格）。账本空着那一格仍然绿——它由左半挡住，所以这一处改坏只有右半看得见。
mutate "右半界写成 >= 0" "${work}/m2.yaml" \
    's/decision="paused_llm_pool_unavailable"}) == 1/decision="paused_llm_pool_unavailable"}) >= 0/' \
    'paused_llm_pool_unavailable"}) >= 0' && expect_red "右半界写成 >= 0：单测红（六格全真即恒报）" 2 0

# 3) `for: 6h` 写成 `for: 6y`：永远不会亮（6h1m，两格）。
mutate "for 写成 6y" "${work}/m3.yaml" 's/^          for: 6h$/          for: 6y/' \
    'for: 6y' 'for: 6h' && expect_red "for 写成 6y：单测红（抓得住「永远不会亮」）" 0 2

# 4) 合取写成析取：三个「不该响」的格子全亮——账本空着、池子好好、停摆已恢复（12h，三格）。
mutate "and 写成 or" "${work}/m4.yaml" 's/^              and on()$/              or on()/' \
    'or on()' 'and on()' && expect_red "and 写成 or：单测红（析取会在不欠账时也报）" 3 0

# 5) `max` 写成 `min`：两个副本里取到干净的那个，多副本那一格必须红，且只有它（6h1m，一格）。
mutate "聚合写成 min" "${work}/m5.yaml" 's/max (/min (/g' 'min (' 'max (' \
    && expect_red "聚合写成 min：单测红（一个副本干净不许捂住全队）" 0 1

# 6) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$manifest" RULES_CASE="$case_file" \
    bash "$runner" >"${work}/out6" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

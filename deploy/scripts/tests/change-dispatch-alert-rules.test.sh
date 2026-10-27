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
#   4. `and on()` 改成 `or`：合取变析取，五个「不该响」的格子一起亮；
#   5. `max` 改成 `min`：两个副本一个 0 一个 2110 时判据取到 0，多副本那一格必须红，
#      而且**只有它**红；
#   6. 两半各有一个写者年龄界，删掉哪一半都只有对应的那一格能看见——这是两处改坏，
#      不是一处：删掉两半里任何一半而两格都安静，说明那半是装饰。
#
# 每一处改坏都按「第 12 小时的红有几格、第 6 小时 1 分的红有几格」来判。这两个数判的是
# 「红在不在该红的求值点上」：`for` 那一类改坏只会在 6h1m 露头，其余只在 12h 露头。它们
# **分不开同一点上的两格**（promtool 的失败块不带用例序号），所以「该亮的是哪一格」由每一
# 条断言的措辞承担，计数只兜住「另一格的求值点上也红了」。**加了一格用例就要重新推这两个
# 数**，这是这份测试唯一要维护的东西，写在红字里。
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
    's/) < 3600)) > 0/) < 3600)) >= 0/' \
    ') < 3600)) >= 0' && expect_red "左半界写成 >= 0：单测红（账本空着不许报）" 1 0

# 2) 右半的 `== 1` 写成 `>= 0`：池子好好那一格跟着亮（12h，一格）。账本空着那一格仍然
#    绿——它由左半挡住，所以这一处改坏只有右半看得见。停摆恢复之后那一格不跟着亮，是因为
#    它的伴生钟与判词同时停下，年龄界先把它摘掉了——写者停下时「恢复」与「冻结」在读数上
#    长得一样，界把两者一起挡在外面，这是界该有的代价，不是漏判。
mutate "右半界写成 >= 0" "${work}/m2.yaml" \
    's/) < 3600)) == 1/) < 3600)) >= 0/' \
    ') < 3600)) >= 0' ')) == 1' && expect_red "右半界写成 >= 0：单测红（六格全真即恒报）" 1 0

# 3) `for: 6h` 写成 `for: 6y`：永远不会亮（6h1m，两格）。
mutate "for 写成 6y" "${work}/m3.yaml" 's/^          for: 6h$/          for: 6y/' \
    'for: 6y' 'for: 6h' && expect_red "for 写成 6y：单测红（抓得住「永远不会亮」）" 0 2

# 4) 合取写成析取：五个「不该响」的格子全亮——账本空着、池子好好、停摆已恢复、写者停在
#    读数一侧、写者停在判词一侧（12h，五格）。析取把「没有读数」与「有活被卡住」接成了一条。
mutate "and 写成 or" "${work}/m4.yaml" 's/^              and on()$/              or on()/' \
    'or on()' 'and on()' && expect_red "and 写成 or：单测红（析取会在不欠账时也报）" 5 0

# 5) `max` 写成 `min`：两个副本里取到干净的那个，多副本那一格必须红，且只有它（6h1m，一格）。
mutate "聚合写成 min" "${work}/m5.yaml" 's/max (/min (/g' 'min (' 'max (' \
    && expect_red "聚合写成 min：单测红（一个副本干净不许捂住全队）" 0 1

# 6) 两半各有一个写者年龄界，缺哪一半都只有对应的那一格能看见：把界删掉，读数冻着而判词
#    新鲜的格子（或者反过来）就跟着亮，规则却还在报「有活被卡住」，而那条读数已经不是账本了。
#    删一半而两格都安静，说明那半的界是装饰；两格全亮，说明判据分不开是哪一半坏了。
expect_leg_removed() { # <名字> <要删的那一段> <删后不许再出现的串> <清单里应仍含的串>
    local name="$1" pat="$2" gone="$3" still="$4"
    local f="${work}/leg.yaml"
    sed "s/$pat//" "$manifest" >"$f"
    if grep -qF -- "$gone" "$f"; then
        bad "$name：改坏没落地" "不再含$gone" "仍含"; return
    fi
    if ! grep -qF -- "$still" "$manifest"; then
        bad "$name：改坏没落地" "清单里仍是那一行" "原样"; return
    fi
    run "$f"
    if [ "$rc" -ne 1 ]; then bad "$name 应红" "1" "$rc"; return; fi
    if ! caught; then bad "$name：红了，但红不在告警上" "失败块点名告警" "$(head -3 "${out}")"; return; fi
    local day six
    day="$(blocks_at 12h)"; six="$(blocks_at 6h1m)"
    if [ "$day" -ne 1 ] || [ "$six" -ne 0 ]; then
        bad "$name：红的落点不对（加过用例就要重新推这两个数）" \
            "12h 1 格 / 6h1m 0 格（写者那一格的求值点在 12h）" "12h ${day} 格 / 6h1m ${six} 格"
        return
    fi
    ok "$name：单测红（冻结的读数不许点亮后果规则）"
}

expect_leg_removed "读数那一半的年龄界去掉" \
    ' and ((time() - cogneva_change_dispatch_readouts_observed_timestamp_seconds) < 3600)' \
    'cogneva_change_dispatch_readouts_observed_timestamp_seconds' \
    'change_ledger_pending"} and ((time()'
expect_leg_removed "判词那一半的年龄界去掉" \
    ' and ((time() - cogneva_change_dispatch_verdict_observed_timestamp_seconds) < 3600)' \
    'cogneva_change_dispatch_verdict_observed_timestamp_seconds' \
    'paused_llm_pool_unavailable"} and ((time()'

# 7) 拿不到 promtool：必须退 3，不许报绿。
PROMTOOL=/nonexistent/promtool RULES_MANIFEST="$manifest" RULES_CASE="$case_file" \
    bash "$runner" >"${work}/out6" 2>&1; rc=$?
[ "$rc" -eq 3 ] && ok "拿不到 promtool：退 3（看不了 ≠ 一致）" || bad "拿不到 promtool 应退 3" "3" "$rc"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]

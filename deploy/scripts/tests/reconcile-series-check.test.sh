#!/usr/bin/env bash
# check-reconcile-series.sh 的定向测试。
#
# 这条门判的是「应到的拍子没到」，所以每个用例都从一份时刻精确可控的序列出发，
# 只改一处：末拍旧了、中间空了、文件没了、行坏了。`--now` 是注入点——不注入就会
# 让测试依赖此刻，明天换个时间跑出来的结论不一样。
#
# 用例：
#   1. 三拍，间距 1800，末拍到此刻 60 秒 -> 0；
#   2. 末拍距此刻 3900 秒（> 两个周期）-> 1，判词点名「应到的拍子没到」；
#   3. 恰好两个周期（3600 秒）不算红——判据是「超过」；
#   4. 中间空了一拍（间距 4000）-> 1，点名第几拍之间；
#   5. 文件不存在 -> 1（一次都没落过不是「都齐了」）；
#   6. 文件是空的 -> 1；
#   7. 有一行的时刻不是数字 -> 2（判不了，不是绿）；
#   8. 末拍在将来 -> 2（时钟对不上，这条读数没有分母）；
#   9. 结局列是 broken 的行也是拍子——门判的是拍子到没到，不是它判得绿不绿 -> 0。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-reconcile-series.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${gate}" ] || fail "门脚本不存在或不可执行：${gate}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

NOW=1792300000

series() { # series <名字> <行…>：每行是「相对 NOW 的秒数<TAB>结局」
    local name="$1"; shift
    local path="${work}/${name}.tsv"
    : >"${path}"
    local line offset verdict
    for line in "$@"; do
        offset="${line%%|*}"
        verdict="${line#*|}"
        printf '%s\t%s\t1\t2\t3\t4\t5\n' "$((NOW - offset))" "${verdict}" >>"${path}"
    done
    printf '%s' "${path}"
}

run() { # run <文件> [额外参数…]：输出进 LAST_OUT，退出码进 RC
    local file="$1"; shift
    set +e
    bash "${gate}" --file "${file}" --now "${NOW}" "$@" >"${work}/out.txt" 2>&1
    RC=$?
    set -e
    LAST_OUT="$(cat "${work}/out.txt")"
}

expect() { # expect <名字> <期望 rc> <必须出现的片段> <序列文件>
    local name="$1" want="$2" needle="$3" file="$4"
    run "${file}"
    [ "${RC}" -eq "${want}" ] || fail "${name}: 期望退出码 ${want}，实得 ${RC}；输出：${LAST_OUT}"
    printf '%s' "${LAST_OUT}" | grep -Fq -- "${needle}" \
        || fail "${name}: 判词里没有「${needle}」；输出：${LAST_OUT}"
    echo "ok   ${name}：退出码 ${want}，判词含「${needle}」"
}

# 1. 正常三拍。
expect fresh 0 "RECONCILE-SERIES 绿" "$(series fresh '3600|ok' '1800|ok' '60|ok')"

# 2. 末拍过旧。
expect stale 1 "应到的拍子没到" "$(series stale '7200|ok' '5400|ok' '3900|ok')"

# 3. 边界：恰好两个周期。
expect boundary 0 "RECONCILE-SERIES 绿" "$(series boundary '5400|ok' '3600|ok')"

# 4. 中间有洞（末拍是新的，洞在前面）。
expect gap 1 "之间空了" "$(series gap '8000|ok' '7600|ok' '3600|ok' '60|ok')"

# 5. 文件不存在。
expect absent 1 "序列文件不存在" "${work}/does-not-exist.tsv"

# 6. 文件是空的。
empty="${work}/empty.tsv"; : >"${empty}"
expect empty 1 "序列文件是空的" "${empty}"

# 7. 坏行。
broken="${work}/broken.tsv"
printf '%s\tok\n' "$((NOW - 60))" >"${broken}"
printf 'not-an-epoch\tok\n' >>"${broken}"
expect malformed 2 "判不了" "${broken}"

# 8. 末拍在将来。
future="${work}/future.tsv"
printf '%s\tok\n' "$((NOW + 7200))" >"${future}"
expect future 2 "时钟对不上" "${future}"

# 9. 红了的那一拍也是拍子：序列在长，门就绿。
expect broken-still-beats 0 "RECONCILE-SERIES 绿" "$(series brokenbeats '3600|broken' '60|ok')"

echo "reconcile-series-check: 9 passed"

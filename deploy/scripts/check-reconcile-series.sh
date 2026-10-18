#!/usr/bin/env bash
# 跨层对账的时序有没有在长：判「应到的那一拍没到」。
#
# 为什么要有这一条：`check-cross-layer-reconcile.sh` 的退出码只回答「跑了的那一拍判出了
# 什么」。它不在、或被 disable 了，退出码根本没有机会出现——而「一直绿」与「一直没跑」
# 在 journal 上长得一样。序列文件把每一拍落成一行，这个门读那份文件，判的是**拍子之间的
# 间距**：相邻两拍（以及最后一拍到此刻）超过两个周期，就是红。
#
# 两个周期不是拍的 2 倍容差，而是「连续缺席一拍」的门槛：30 分钟一拍的定时器允许迟到，
# 但连着两拍不见，说明它已经不在跑了，而不是慢了一点点。
#
# 判词分三种：红（缺拍）、判不了（文件读不了、行坏了、时刻倒退）、绿。
# 「文件不存在」是红不是判不了：一次都没落过，正是最该说话的那一格。
#
# 用法：bash deploy/scripts/check-reconcile-series.sh [--file PATH] [--interval SECS] [--now EPOCH]
# 退出码：0 拍子齐；1 有缺席（文件不存在／空／末拍过旧／中间有洞）；2 判不了。
set -euo pipefail

SERIES="${COGNEVA_RECONCILE_SERIES:-${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/cross-layer-reconcile.tsv}"
INTERVAL=1800
NOW=""

while [ $# -gt 0 ]; do
    case "$1" in
        --file)     [ $# -ge 2 ] || { echo "check-reconcile-series: --file 缺参数" >&2; exit 2; }; SERIES="$2"; shift 2 ;;
        --interval) [ $# -ge 2 ] || { echo "check-reconcile-series: --interval 缺参数" >&2; exit 2; }; INTERVAL="$2"; shift 2 ;;
        --now)      [ $# -ge 2 ] || { echo "check-reconcile-series: --now 缺参数" >&2; exit 2; }; NOW="$2"; shift 2 ;;
        *) echo "check-reconcile-series: 不认识的参数：$1" >&2; exit 2 ;;
    esac
done

case "$INTERVAL" in ''|*[!0-9]*|0) echo "check-reconcile-series: --interval 必须是正整数秒：$INTERVAL" >&2; exit 2 ;; esac
if [ -z "$NOW" ]; then
    NOW="$(date +%s)"
fi
case "$NOW" in ''|*[!0-9]*) echo "check-reconcile-series: --now 必须是 epoch 秒：$NOW" >&2; exit 2 ;; esac

limit=$((INTERVAL * 2))

# 时刻印成人能读的样子；这台机器上没有 date -d 时退回 epoch，不因此判不了。
human() {
    date -d "@$1" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || printf '@%s' "$1"
}

if [ ! -e "$SERIES" ]; then
    printf 'RECONCILE-SERIES 红：序列文件不存在（%s）——一次拍子都没落过，不是「都齐了」。\n' "$SERIES"
    exit 1
fi
if [ ! -r "$SERIES" ]; then
    printf 'RECONCILE-SERIES 判不了：序列文件读不了（%s）。\n' "$SERIES"
    exit 2
fi

# 一行一拍。只读第一列（时刻）；第二列（结局）是给人看的，判绿不靠它——红的那一拍
# 也是一拍，缺席才是不在跑的证据。
prev=""
last=""
rows=0
lineno=0
bad=""
while IFS=$'\t' read -r when _rest; do
    lineno=$((lineno + 1))
    case "$when" in ''|*[!0-9]*) bad="第 ${lineno} 行的时刻不是 epoch 秒：${when:-（空）}"; break ;; esac
    if [ -n "$prev" ] && [ "$when" -lt "$prev" ]; then
        bad="第 ${lineno} 行的时刻早于上一行（${when} < ${prev}）——序列不是只增的"
        break
    fi
    prev="$when"
    last="$when"
    rows=$((rows + 1))
done <"$SERIES"

if [ -n "$bad" ]; then
    printf 'RECONCILE-SERIES 判不了：%s。\n' "$bad"
    exit 2
fi
if [ "$rows" -eq 0 ]; then
    printf 'RECONCILE-SERIES 红：序列文件是空的（%s）——一次拍子都没落过。\n' "$SERIES"
    exit 1
fi

# 中间有洞：相邻两拍的间距超过两个周期。
prev=""
gaps=0
lineno=0
while IFS=$'\t' read -r when _rest; do
    lineno=$((lineno + 1))
    if [ -n "$prev" ]; then
        delta=$((when - prev))
        if [ "$delta" -gt "$limit" ]; then
            printf 'RECONCILE-SERIES 红：第 %d 拍与第 %d 拍之间空了 %d 秒（> %d），%s 到 %s 没有拍子。\n' \
                "$((lineno - 1))" "$lineno" "$delta" "$limit" "$(human "$prev")" "$(human "$when")"
            gaps=$((gaps + 1))
        fi
    fi
    prev="$when"
done <"$SERIES"

# 末拍到此刻：超过两个周期就是红。未来时刻不算红，算判不了（宿主时钟对不上，
# 这条读数就没有分母）。
if [ "$last" -gt $((NOW + INTERVAL)) ]; then
    printf 'RECONCILE-SERIES 判不了：末拍 %s 在将来（此刻 %s）——时钟对不上。\n' \
        "$(human "$last")" "$(human "$NOW")"
    exit 2
fi
age=$((NOW - last))
stale=0
if [ "$age" -gt "$limit" ]; then
    printf 'RECONCILE-SERIES 红：末拍 %s，距此刻 %d 秒（> %d），应到的拍子没到。\n' \
        "$(human "$last")" "$age" "$limit"
    stale=1
fi

if [ "$gaps" -gt 0 ] || [ "$stale" -eq 1 ]; then
    exit 1
fi

printf 'RECONCILE-SERIES 绿：%d 拍，末拍 %s（距此刻 %d 秒），相邻间距都在 %d 秒内。\n' \
    "$rows" "$(human "$last")" "$age" "$limit"

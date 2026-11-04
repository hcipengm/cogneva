#!/usr/bin/env bash
# principle: feedback_a_progress_bound_finer_than_the_persisted_unit_is_what_tells_a_long_call_from_a_wedged_one
# principle: feedback_a_consumer_side_counter_cannot_tell_idle_from_wedged
# 续跑循环的停滞判据回放：两个数都平才判停，io 在动就不许杀。
#
# 为什么值得单独一格：这条判据判错的方向是「把还在流式生成的轮次杀掉」，而这件事在
# 日志上长得和正常收尾一模一样——2026-10-19 夜就是这样连杀了六轮。判据只有上线才验证，
# 就等于没验证；这里拿历史采样回放，把那次误杀变成一条会红的用例。
#
# 样本格式（与 `--dry-run` 一致）：`<epoch> <checkpoint 字节数> <io 计数或 ->`。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOOP="$HERE/eval-registered-rerun.sh"

pass=0
fail=0
report() { # $1 = 实际退出码，$2 = 期望退出码，$3 = 名字
    if [ "$1" = "$2" ]; then
        printf 'ok    %s\n' "$3"
        pass=$((pass + 1))
    else
        printf 'FAIL  %s\n      实际退出码=%s\n      期望退出码=%s\n' "$3" "$1" "$2" >&2
        fail=$((fail + 1))
    fi
}

[ -x "$LOOP" ] || { printf 'FAIL  续跑脚本不可执行：%s\n' "$LOOP" >&2; exit 1; }

work="$(mktemp -d)" || exit 3
trap 'rm -rf "$work"' EXIT

# 逐行写样本：$1 文件，$2 每 20 秒采样一次的起点，其余为 `字节数 io` 交替给出。
emit() {
    local f=$1 t0=$2; shift 2
    local t=$t0
    while [ $# -gt 0 ]; do
        printf '%s %s %s\n' "$t" "$1" "$2" >>"$f"
        t=$((t + 20))
        shift 2
    done
}

run() { # $1 样本文件，$2 STALL
    bash "$LOOP" --dry-run "$1" --stall "$2" >"$work/out" 2>&1
    echo $?
}

# 1. 这一夜被误杀的那种轮次：checkpoint 240 秒一个字节不动，而 io 一直在涨。
#    io 计数取自 2026-10-19 在活 Pod 上实测的 20 秒增量（+632、+316 字节）。
f="$work/generating"; : >"$f"
emit "$f" 1000 \
    1000 1 1000 633 1000 949 1000 1581 1000 1897 1000 2529 \
    1000 3163 1000 3795 1000 4431 1000 5000 1000 5632 1000 6260 1000 6900
report "$(run "$f" 240)" 0 "生成中的轮次不许判停（io 在涨、checkpoint 不动）"
grep -q 'running to' "$work/out" || { printf 'FAIL  判词不是 running：%s\n' "$(cat "$work/out")" >&2; fail=$((fail + 1)); }

# 2. 两个数都平过 STALL：这才是停。
f="$work/wedged"; : >"$f"
emit "$f" 1000 1000 6900 1000 6900 1000 6900 1000 6900 1000 6900 1000 6900 1000 6900 1000 6900
report "$(run "$f" 60)" 1 "两个数都平过界就判停"
grep -q 'stalled at' "$work/out" || { printf 'FAIL  判词不是 stalled：%s\n' "$(cat "$work/out")" >&2; fail=$((fail + 1)); }

# 3. io 问不出来（exec 失败）但 checkpoint 在长：只靠一条腿也仍然是「在动」。
f="$work/no-io"; : >"$f"
emit "$f" 1000 2100 - 2200 - 2300 - 2400 - 2500 -
report "$(run "$f" 240)" 0 "io 读不到时退化到 checkpoint，不把问不出来当没动静"

# 4. io 问不出来且 checkpoint 也平：退化之后仍然要判停，不能因为少一条腿就永远绿。
f="$work/no-io-wedged"; : >"$f"
emit "$f" 1000 2100 - 2100 - 2100 - 2100 - 2100 -
report "$(run "$f" 60)" 1 "io 读不到而 checkpoint 也平，仍判停"

# 5. 边界：正好在 STALL 那一刻判停，早一秒不判。
f="$work/exact"; : >"$f"
emit "$f" 1000 5000 1 5000 1 5000 1
report "$(run "$f" 41)" 0 "差一秒不到界不判停"
f="$work/exact2"; : >"$f"
emit "$f" 1000 5000 1 5000 1 5000 1 5000 1
report "$(run "$f" 40)" 1 "正好到界判停"

if [ "$fail" -ne 0 ]; then
    printf '\n%d 通过，%d 失败\n' "$pass" "$fail" >&2
    exit 1
fi
printf '\n%d 全部通过\n' "$pass"

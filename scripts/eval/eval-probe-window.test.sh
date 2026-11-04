#!/usr/bin/env bash
# principle: feedback_a_binary_gauge_that_merges_spent_with_ramping_hides_a_computable_open_time
# principle: feedback_a_check_that_cannot_run_must_not_answer_absent
# 池子探针的解析器：429 正文里的四个量要读得出来，读不到时**不许印 0**。
#
# 为什么值得单独一格：这一族读数的全部价值在「把 0/1 的可用性拆成能各自处置的形状」。
# 一旦解析器把「正文里没有这几个字段」读成「allowance=0」，斜坡与花光就会再次同形，
# 而且这次是在日志里同形——比原来看 gauge 更难发现。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE="$HERE/eval-probe-window.sh"

pass=0
fail=0
report() { # $1 实际，$2 期望，$3 名字
    if [ "$1" = "$2" ]; then
        printf 'ok    %s\n' "$3"
        pass=$((pass + 1))
    else
        printf 'FAIL  %s\n      实际=%s\n      期望=%s\n' "$3" "$1" "$2" >&2
        fail=$((fail + 1))
    fi
}

[ -x "$PROBE" ] || { printf 'FAIL  探针脚本不可执行：%s\n' "$PROBE" >&2; exit 1; }

work="$(mktemp -d)" || exit 3
trap 'rm -rf "$work"' EXIT

# 2026-10-19T01:46Z 从网关抓到的原样 429 正文（节选；只有 blocked_upstreams 那两串
# 上游 URL 换成了占位名，因为这一族要求成员不出现网络形态——解析器一个字段都不碰它）。
cat >"$work/ramp.json" <<'JSON'
{"blocked_upstreams":["up-a","up-b"],"error":"本窗口的额度已经摊到这里，还不够再开一件任务","quota_window_secs":18000,"retry_after_seconds":1915,"window_allowance_tokens":37531,"window_reserve_tokens":392578,"window_spent_tokens":0,"window_start_unix":1792374146}
JSON

# 同一晚的 503 正文：它只谈下一次尝试，不带这一窗口的算术。
cat >"$work/circuit.json" <<'JSON'
{"error":"所有 LLM 上游当前不可用","quota_window_secs":18000,"retry_after_seconds":9528}
JSON

report "$(bash "$PROBE" --pace "$work/ramp.json")" \
    "allowance:37531,reserve:392578,spent:0,start:1792374146" \
    "429 正文里的四个量原样读出来"

report "$(bash "$PROBE" --pace "$work/circuit.json")" "" \
    "503 正文不带这几个字段时什么都不印，不印 0"

report "$(bash "$PROBE" --pace "$work/nope.json")" "" \
    "正文文件不存在时什么都不印"

# 01:46:16 allowance=37531，01:47:07 allowance=48516 ⇒ 约 215 token/秒；
# 取整到 200 之后 (392578-48516)/200 = 1720.31 ⇒ 向上取整 1721。
report "$(bash "$PROBE" --estimate 48516 392578 200)" "1721" \
    "斜坡反解：差 344062、200 token/秒 ⇒ 1721 秒"

report "$(bash "$PROBE" --estimate 392578 392578 200)" "0" \
    "已经到储备线：还差 0 秒，不是负数"

report "$(bash "$PROBE" --estimate 48516 392578 0)" "unknown" \
    "斜率为 0 时报 unknown，不做除零"

report "$(bash "$PROBE" --estimate "" 392578 200)" "unknown" \
    "缺一个量时报 unknown，不当成 0 算"

if [ "$fail" -ne 0 ]; then
    printf '\n%d 通过，%d 失败\n' "$pass" "$fail" >&2
    exit 1
fi
printf '\n%d 全部通过\n' "$pass"

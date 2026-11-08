#!/usr/bin/env bash
# 评测的开关键：`on` / `off` / `status`。
#
# 为什么不是「杀掉进程就算关了」：拉起值守的那条腿是用户级定时器
# `cogneva-eval-supervisor-ensure.timer`，每十分钟一次，被杀掉的东西它十分钟内就补回来
# （2026-11-08 实测，见 eval-pause-flag.sh 头部）。所以关是写一个盘上的标记，让那条腿
# 自己看见「现在是关着的」，而不是跟它比谁跑得快。
#
# `off` 同时终止正在跑的三条进程：标记只挡下一次启动，挡不住已经起在跑的那一轮，而那一轮
# 正在逐个调用花钱。先写标记再杀——反过来的话，杀与拉起之间有十分钟的窗口，正好够它回来。
#
# 用法：
#   eval-switch.sh off "各方面没有理顺，先不做测评"
#   eval-switch.sh on
#   eval-switch.sh status
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=eval-pause-flag.sh
. "$HERE/eval-pause-flag.sh"
SEEN="$PAUSE_FLAG.seen"
LOGGED="$PAUSE_FLAG.logged"

usage() { echo "usage: $0 on|off \"<理由>\"|status" >&2; exit 2; }

case "${1:-}" in
  off)
    reason=${2:-}
    {
      printf 'off at %s\n' "$(date -u +%FT%TZ)"
      printf 'reason: %s\n' "${reason:-(not stated)}"
    } >"$PAUSE_FLAG"
    rm -f "$LOGGED"
    # 已经起在跑的那一轮：标记挡不住它，它也不读标记。终止是关这个动作的一部分，
    # 不是一个额外的建议——留着它跑完，就是「关着」的状态下还在花钱。
    for p in 'supervise-eval-loop[.]sh' 'eval-registered-rerun[.]sh' 'eval-pool-watch[.]sh'; do
      pkill -f "$p" 2>/dev/null
    done
    printf 'switch=off flag=%s\n' "$PAUSE_FLAG"
    printf 'the three eval processes were asked to stop; the supervisor will not start another round\n'
    ;;
  on)
    rm -f "$PAUSE_FLAG" "$SEEN" "$LOGGED"
    printf 'switch=on flag=%s\n' "$PAUSE_FLAG"
    printf 'the next ensure tick starts a supervisor; nothing here starts one now\n'
    ;;
  status)
    if eval_paused; then
      printf 'switch=off flag=%s\n' "$PAUSE_FLAG"
      sed 's/^/  /' "$PAUSE_FLAG"
      # 值守被关掉之后，「定时器还在跑吗」是另一件事，而它决定恢复时报废了什么：
      # 定时器被 disable 过，`on` 之后不会有人来拉值守。所以读它，而不是假定。
      if command -v systemctl >/dev/null 2>&1; then
        en=$(systemctl --user is-enabled cogneva-eval-supervisor-ensure.timer 2>/dev/null)
        ac=$(systemctl --user is-active cogneva-eval-supervisor-ensure.timer 2>/dev/null)
        printf '  timer: enabled=%s active=%s\n' "${en:-unknown}" "${ac:-unknown}"
      fi
      # ensure 每十分钟来一次；它被关着的时候不写日志（那是 144 行/天），只摸这个文件。
      # 所以「谁在看着开关」这件事只有这里读得到。
      if [ -e "$SEEN" ]; then
        printf '  ensure last looked: %s\n' "$(date -u -r "$SEEN" +%FT%TZ 2>/dev/null || echo unknown)"
      else
        printf '  ensure last looked: never since the switch went off\n'
      fi
    else
      printf 'switch=on flag=%s\n' "$PAUSE_FLAG"
    fi
    ;;
  *) usage ;;
esac

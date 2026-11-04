#!/bin/bash
# 等到上游自己报的恢复时刻再起续跑循环，而不是每三分钟空转一轮。
#
# 为什么需要这个：池子关着时一轮只要 60–100 秒就退（每格秒回 503），循环会照常每
# 180 秒起一个 Pod——两三个小时里起几十个 Pod、每个都要读一遍 /data（一把 600+ MB），
# 换来的 judged 增量为零。真正决定能不能判的是上游的恢复时刻，网关自己把它报在
# `llm_pool_evidenced_recovery_unix` 上；2026-10-19T01:24Z 读到的值是 04:43:18Z。
#
# 用法：eval-rerun-at-window.sh <unix 秒> [额外环境…]
# 到点之后起 eval-registered-rerun.sh；已经有循环在跑时什么都不做（checkpoint 的
# 单写者锁会让第二个循环整轮 bail，没必要靠它发现）。
set -u
at=${1:?usage: eval-rerun-at-window.sh <unix-epoch>}
shift
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
now=$(date +%s)
if [ "$at" -gt "$now" ]; then sleep $(( at - now )); fi
# 匹配要锚在「bash 后面直接跟这个脚本」上：排队中的提交把路径写在参数里，
# 一条裸的 `pgrep -f '...eval-registered-rerun.sh'` 会把它当成循环在跑（2026-10-19 实测：
# 探针刚起来就误报「a loop is running」并退出）。
if pgrep -f '^bash [^ ]*host/eval-registered-rerun\.sh' >/dev/null 2>&1; then
  printf '%s the window arrived and a loop is already running; leaving it alone\n' \
    "$(date -u +%FT%TZ)" >> "${LOG:-$HOME/eval-runs/.runner/rerun.log}"
  exit 0
fi
printf '%s the window arrival was waited for offline; starting the loop\n' \
  "$(date -u +%FT%TZ)" >> "${LOG:-$HOME/eval-runs/.runner/rerun.log}"
exec bash "$here/eval-registered-rerun.sh" "$@"

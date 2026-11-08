#!/usr/bin/env bash
# 值守的宿主腿：宿主重启、或值守被 OOM/误杀之后，把它重新拉起来，不需要有人在。
#
# 为什么需要它：`supervise-eval-loop.sh` 只活在它自己的进程里。2026-10-20 之前，续命的
# 只有会话内的周期卡——会话结束就没了，宿主重启更不用说。而这条臂要等的是两周外的窗口
# （池子的两个恢复时刻是 10-25 与 11-04），中间任何一次重启，都会让窗口打开时没人守。
# 宿主上 k3s 是 `enabled` 的（重启后自己起回来），所以「重启之后这条臂跑不跑」只差这一条腿。
#
# 判「已在跑」的模式必须锚在**第二个 token**：提交命令的 argv 里也会出现这个脚本名
# （`principle-commit.sh … scripts/eval/supervise-eval-loop.sh`），不锚就会把一笔正在
# 排队的提交当成「值守在跑」，于是永远不再起。
#
# 到目标就不起：否则它每 10 分钟起一条、那条立刻判「到了」退出，日志里一天多 144 行。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=eval-pause-flag.sh
. "$HERE/eval-pause-flag.sh"
SEEN="$PAUSE_FLAG.seen"
LOGGED="$PAUSE_FLAG.logged"
SUP=${SUP:-$HERE/supervise-eval-loop.sh}
OUT=${OUT:-$HOME/eval-runs/.runner/supervise.out}
LOG=${LOG:-$HOME/eval-runs/.runner/eval-supervisor-ensure.log}
pattern='^bash ([^ ]*/)?supervise-eval-loop[.]sh( |$)'

[ -r "$SUP" ] || { echo "verdict=missing-supervisor path=$SUP"; exit 1; }

# 判词既上 stdout 也落一行带时刻的文件：用户态 journal 在这台机器上不持久，只印 stdout
# 等于「跑过但没人读得到」——这条腿存在的理由恰恰是**没有人醒着**的时候也在跑。
say() { printf '%s ensure: %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG"; }

# 开关关着就不拉。判在锁之前：关着的时候这件事没有第二条腿会来抢，占锁只是让一条
# 什么都不做的腿在日志里留下 `locked`，而那个词本来是用来说「另一条真在干活」的。
#
# 日志一行都不写才是错的：这条腿每十分钟来一次，写一行就一天 144 行，把真正的事件埋了；
# 可完全不写，读的人就分不出「它在看着开关、只是关着」与「它已经死了」。所以折中是
# 每次摸一次 `.seen`（那是「谁在看着」的读数，`eval-switch.sh status` 读它），而往日志
# 里只在一段关闸里写第一行。
if eval_paused; then
  touch "$SEEN"
  if [ ! -e "$LOGGED" ]; then
    : >"$LOGGED"
    say "verdict=paused flag=$PAUSE_FLAG; no supervisor started"
  fi
  exit 0
fi
rm -f "$LOGGED"

# 两条「ensure」同时进来（定时器与巡检卡撞上）会各自判「没在跑」，然后各起一条值守；
# 值守本身不去重，两条会同时判池、同一下开两轮。锁只护这一小段，拿不到就让给另一条。
exec 9>>"$LOG.lock"
# 判词不能只写 `locked`：同一个词盖着两种相反的处境——另一条 ensure 正在跑（让路是对的），
# 与我上一拍拉起的值守、以及它留下的 `sleep` 还握着这把锁（这是缺陷，得有人来清）。
# 不写持有者，读的人只能登上来手工查；2026-10-20 那次就是这么查出来的，而日志里那一行
# 「格式完全正确的 locked」与正常冲突逐字同形。所以把持有者 pid 一起写进判词。
flock -n 9 2>/dev/null || {
  # 「读不到」不许写成「没有人」：`fuser` 缺失（rc=127）或出错时 stdout 同样是空的，若一律
  # 落成 `none`，判词就在它唯一要回答的那一行上撒谎——查的人照着 `none` 收工，遗留的持有者
  # 继续挡着。`fuser` 只有「找到」才回 0；回 1 且没输出才是真的没有持有者。
  raw=$(fuser "$LOG.lock" 2>/dev/null); rc=$?
  holders=$(printf '%s' "$raw" | tr -s ' ' ' ' | sed 's/^ //;s/ $//')
  if [ "$rc" -eq 0 ] && [ -n "$holders" ]; then :
  elif [ "$rc" -eq 1 ] && [ -z "$holders" ]; then holders=none
  else holders="unreadable(rc=$rc)"; fi
  say "verdict=locked holders=$holders"
  exit 0
}

if pgrep -f "$pattern" >/dev/null 2>&1; then
  say "verdict=running"
  exit 0
fi

# 到没到目标由值守自己那套判据判（按腿，不只看总数），这里不复制一份。
state=$(bash -c 'source "$1"; n=$(lines); if reached "$n"; then echo reached; else echo short; fi' _ "$SUP" 2>/dev/null)
if [ "$state" = reached ]; then
  say "verdict=target-reached"
  exit 0
fi

# `9>&-` 是这条腿的要害，不是随手关个 fd：`flock` 锁的是**打开文件描述**，子进程会连描述
# 一起继承，于是被拉起的值守（以及它 spawn 的每个 sleep）会一直握着这把锁。值守活得越久，
# 后面的 ensure 就全是 `verdict=locked`——**恰好在值守死掉、最需要拉起的时候**这条腿也把自己
# 锁在门外（死值守留下的 sleep 子进程仍握着锁）。2026-10-20 实测：04:42:24 判 `locked`，
# `fuser` 显示持有者正是 04:33:07 拉起的那条值守与它的两个 sleep。
setsid nohup bash "$SUP" >>"$OUT" 2>&1 </dev/null 9>&- &
say "verdict=started pid=$!"

#!/usr/bin/env bash
# 值守：没有评测循环在跑时就起一条；已判到 TARGET 或到期就退出。
#
# 目标为什么默认 198 而不是 240：240 = 40 题 × 3 seed × 2 scaffold，而这个子集里 7 道题
# 带图，骨干看不见图 ⇒ 42 行（7×3×2）永远判不了。带图题由评测台在跑之前摘掉、记
# `skipped`、不进分母、在报表 `## skipped` 段按题号点名。所以口径是
# **198 可判 + 42 按设计跳过（vision=none）**；这不是缩水，是如实。
# 接上 native 视觉骨干并开 `--vision native` 之后，天花板回到 240，这里要跟着改回
# 240；判据是报表 `## skipped` 段为空。目标与天花板的算术由
# `scripts/eval/eval-ceiling-reading.sh` 在跑完的报表上判，不在本文件里重复一遍。
#
# 为什么单独成文件而不是 `bash -c '...'`：后者的命令行里带着循环脚本的路径，
# `pgrep -f` 会把它自己当成一条在跑的循环，于是永远不启动（也不会报错）。
#
# 判「有没有循环在跑」的模式必须允许参数：循环是以 `--target "$TARGET"` 起的，
# 一条锚在 `[.]sh$` 上的模式匹配不到它（2026-10-19 实测：值守每次都判「没在跑」，
# 只因它是同步调用才没真的起出第二条）。模式写成「bash 后面直接跟这个脚本」，
# 路径可以带目录，脚本名之后允许参数或行尾。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 总开关键在 eval-pause-flag.sh 里，三处入口共用那一份默认路径。
# shellcheck source=eval-pause-flag.sh
. "$HERE/eval-pause-flag.sh"
REPO=${REPO:-$(cd "$HERE/../.." && pwd)}
CKPT=${CKPT:-$HOME/eval-runs/20261018_post-recovery.md.cases.jsonl}
LOG=${LOG:-$HOME/eval-runs/.runner/rerun.log}
OUT=${OUT:-$HOME/eval-runs/.runner/rerun.out}
POOL_LOG=${POOL_LOG:-$HOME/eval-runs/.runner/pool.log}
REPORT=${REPORT:-$HOME/eval-runs/20261018_post-recovery.md}
TARGET=${TARGET:-198}
# 「到目标」是按腿判的：这一臂登记了 4 个 scaffold，总数到 198 可能全是 gepa 一条腿
# （2026-10-19 实测那份报表 78 行全是 gepa、agentflow 一行没落过）。只看总数，值守
# 早就算「到了」，而另一条腿仍是零进度。每条腿的分摊是 198 / 腿数 = 99（33 题 × 3 seed）。
LEGS=${LEGS:-gepa,agentflow}
# 池子关着时最多等这么久（秒）再看一次。网关报的下一次尝试可能在三小时后，一次睡到底
# 会让值守看不见「池子提前开了」，所以截断。截多短由**实测的窗口长度**决定，不是拍一个
# 整数：2026-10-20 用集群 Prometheus 把 6 天的 `llm_pool_available` 按 60 秒步长拉出来数，
# 12 段可用里有 5 段短于 600 秒（480/720/900/1260 秒之外还有几条 60 秒的闪），最短那段
# **可用窗口是 480 秒**。取样周期 600 秒只能以 480/600 = 0.8 的概率落进那段窗口，而且
# 落进去之后值守还要再睡满一拍才醒——一次 480 秒的窗口，光是反应就有 10 分钟可等。
# 所以周期与最长等待一起收到 120 秒：凡不短于 120 秒的窗口，取样与反应都必然落在它里面。
# 判据：下面夹具断言 POOL_MAX_WAIT ≤ POOL_WATCH_INTERVAL。
POOL_MAX_WAIT=${POOL_MAX_WAIT:-120}
# 预测已过期、而最近一次采样仍说池子关着时的宽限（秒）：等到下一拍样本再判。
# 过期只说明「网关该试过了」，不说明「试过了就开了」——2026-10-19 实测 23:07:31Z
# 那一拍网关重试失败，把 next_attempt 又推了 2h46m，而 `next <= now` 会让值守立刻
# 起一轮、空转 1815 秒（rerun.log 里 20:31/21:16/22:01/22:46 四轮 judged 0->0）。
# 它等的就是**下一拍样本**，所以它也只能是一拍长——原来写 600 秒，那是「一拍」还等于
# 600 时的形状；周期收到 120 之后这个数若不动，它就成了整条链里最粗的那一项：池子在这
# 600 秒里打开又关掉，值守还在睡，前面把周期收紧一半白做。同一刀在「这一拍没回答」
# （metrics-unreachable）那条分支上也一样：不答也只该等一拍。
POOL_GRACE=${POOL_GRACE:-120}
# 这一臂的请求落在哪一面。`cog-eval` 走网关的 `/v1/chat/completions`，那是 openai 面；
# 池级 `available` 答的是**整个池**有没有容量，不是**这一面**接不接，两者在池里住了两种
# 协议面时会分叉（一面全灭、另一面健康）。面名是协议面的名字，不是厂商。
POOL_FACE=${POOL_FACE:-openai}
# 池读数由一条独立循环每 10 分钟写一行，而那循环**自己会到期**：脚本默认只跑 4 小时
# （HORIZON=14400），在跑的那条是靠环境变量拉到 6.5 天的（INTERVAL=600、HORIZON=561600，
# 2026-10-20 实测）。谁在什么时候起它、起多久，值守这边看不见；它一停，pool.log 最后一行
# 就永远读作「池子关着」，值守于是一直等宽限——池子开了也不起一轮，而这段空白没有任何
# 读数说出来。所以由值守自己保证采样循环在跑，并按自己的到期时间给它 HORIZON。
POOL_WATCH_SCRIPT=${POOL_WATCH_SCRIPT:-$HERE/eval-pool-watch.sh}
POOL_WATCH_LOG=${POOL_WATCH_LOG:-$HOME/eval-runs/.runner/pool-watch.out}
# 取样周期跟采样脚本自己的默认值（120 秒）对齐，别再把它调粗：值守传 INTERVAL 下去，
# 传一个比脚本默认更大的数，等于用「证据每 600 秒来一行」去论证「等 600 秒不算粗」——
# 那条论证是自指的，而实测里最短的可用窗口只有 480 秒（见 POOL_MAX_WAIT 上面那段）。
POOL_WATCH_INTERVAL=${POOL_WATCH_INTERVAL:-120}
# 等着的时候多久往 rerun.log 写一行。周期收到 120 秒之后，「池子还关着」这句话每两分钟
# 都想说一次，一天 720 行、十五天两万五千行，把真正的事件埋了。这条日志的用途是「事后
# 能复述哪一段在等、等的是哪个时刻」，不需要每一拍都在场——真正要紧的边沿由 pool.log
# 自己一拍一行记着。所以按时间预算记：默认半小时一行，等待的理由变了另记。
WAIT_LOG_EVERY=${WAIT_LOG_EVERY:-1800}
# 到期时刻要盖住**证据点名的**下一个窗口，而不是「从现在起 N 小时」。上游自报的恢复时刻
# 可能比任何一次探针尝试远得多（2026-10-20 实测：探针每 3 小时推一次 next_attempt，而
# `llm_pool_evidenced_recovery_unix` 在 15.6 天后），拿「从现在起 N 小时」量它，值守会在
# 窗口打开前先退出，日志上只留一行 `horizon reached`——与「到目标了」同形，读的人分不出来。
# 所以到期取「请求的时长」与「恢复时刻 + 宽限」里更晚的那个，并封顶，免得一条坏样本把
# 值守变成永久进程。
HORIZON_MAX=${HORIZON_MAX:-2678400}   # 31 天
HORIZON_GRACE=${HORIZON_GRACE:-3600}  # 恢复时刻之后再多守一小时
# 方括号让 pgrep 自己的命令行（含这段字面量）不匹配自己；锚在**第二个 token**：一笔排队的
# 提交（`principle-commit.sh … scripts/eval/eval-pool-watch.sh`）的 argv 里也有这个脚本名，
# 不锚就会把提交当成采样循环在跑，于是明明没有采样也不补起——这正是下面 `pattern` 那条
# 注释讲过的形状，`pattern_watch` 当时漏了同一刀（2026-10-20 发现）。
pattern_watch='^bash ([^ ]*/)?eval-pool-watch[.]sh( |$)'
pattern='^bash ([^ ]*/)?scripts/eval/eval-registered-rerun[.]sh( |$)'

say() { printf '%s supervisor: %s\n' "$(date -u +%FT%TZ)" "$*" >>"$LOG"; }

# 先判文件在不在再读：`wc -l <"$CKPT" 2>/dev/null` 挡不住「文件不存在」——重定向在
# 命令启动前就由 shell 做，报错时 `2>` 还没生效，于是每次判都往日志里写一行
# `No such file or directory`（2026-10-19 实测 supervise.out 全是这一句）。
lines() { [ -r "$CKPT" ] && wc -l <"$CKPT" || echo 0; }

# 一条腿已判多少行：检查点每条已判用例带 `"scaffold":"<名字>"` 与 `"benchmark":"<名字>"`。
# 两个坐标都要对上：这一臂同时跑三个基准，只按 scaffold 数会把别处的同名外壳算进 hle
# 这一条腿，而 198 的算术只覆盖 hle（33 题 × 3 seed）。
leg_lines() {
  [ -r "$CKPT" ] || { echo 0; return; }
  grep -F "\"scaffold\":\"$1\"" "$CKPT" 2>/dev/null \
    | grep -c -F "\"benchmark\":\"${LEG_BENCH:-hle}\"" || true
}

# 每条腿各自要多少行：总目标按腿数平摊，向上取整。
leg_target() {
  local n
  n=$(printf '%s' "$LEGS" | tr ',' '\n' | grep -c .)
  [ "$n" -gt 0 ] || n=1
  echo $(( ($1 + n - 1) / n ))
}

# 到没到目标：总数够 **且** 每条腿都够。只判总数时，一条腿把总数顶满就算「到了」，
# 另一条腿零进度也照样停——而这正是「两条腿各 120」这个目标里唯一会出事的方向。
reached() {
  [ "$1" -gt "$TARGET" ] || return 1
  local t leg
  t=$(leg_target "$TARGET")
  for leg in ${LEGS//,/ }; do
    [ "$(leg_lines "$leg")" -ge "$t" ] || return 1
  done
  return 0
}

# 停与不停都要把每条腿的数印在同一行：只印总数时，「到了」和「一条腿在长」同形。
leg_report() {
  local leg out=""
  for leg in ${LEGS//,/ }; do out="$out $leg:$(leg_lines "$leg")"; done
  printf '%s' "${out# }"
}

# 池子关着时不要每半小时起一个 Pod 空转：网关自己报了下一次尝试的时刻，读到它就在
# 那儿等着。读不到、已过期、或值不合理 ⇒ 返回 0（立刻起一轮，别让一个坏读数把值守卡死）。
#
# 输出 `"<秒> <为什么>"`。第二格是**这一拍为什么不算开门**的闭集——`no_sample`（这一拍
# 没回答）／`face_empty`（这一臂要的那一面没有能接的成员）／`next_attempt`（网关报的
# 下一次尝试在未来）／`still_unavailable`（界过了、最近一拍仍说关着）／`open`（该起
# 一轮）。日志只印第一格时，四种原因同形：2026-10-24 实测每一条都写成「per the
# gateway's next attempt」，而当时真正的理由是那一面全灭，`next_attempt` 只是七分钟后
# ——照那句话去查的人会得出「再等一会儿就好」，与「等到十一月也起不来」是两回事。
# 判据不变，只把判词印出来。
pool_wait_seconds() {
  [ -r "$POOL_LOG" ] || { echo "0 no_sample"; return; }
  local line next now w avail
  line=$(tail -1 "$POOL_LOG" 2>/dev/null)
  # 这一拍**没有回答**（采样读不到指标时写的是 `metrics-unreachable`，两个字段都没有）：
  # 不答不等于开门。返回 0 会当场起一轮，而那一轮只会在 503 里空转到被判停；
  # 等下一拍样本再问，代价最多一个宽限，而采样循环由 `ensure_pool_watcher` 保证还在写。
  # 2026-10-19T08:58:44Z 实测出现过一行（497 拍里 1 拍）。
  avail=$(printf '%s' "$line" | sed -n 's/.*llm_pool_available=\([0-9][0-9]*\).*/\1/p')
  [ -n "$avail" ] || { echo "$POOL_GRACE no_sample"; return; }
  # 这一面读 0 就是关门，判在池级那一个数之前：池级读 1 只说明**另一面**还有容量，而这一
  # 臂的每一次调用都会在熔断处回 503。2026-10-24 实测：openai 面唯一那家回了额度周期、
  # anthropic 面健康，池级读 1、`next_attempt` 早已过期，值守于是每半小时起一轮、每轮
  # 零产出，判词一路绿。这一格缺席（旧格式的行、或网关还没发这条指标）就退回只看池级：
  # 读不到不等于关门。
  face=$(printf '%s' "$line" | sed -n "s/.*face_serving\[$POOL_FACE\]=\([0-9][0-9]*\).*/\1/p")
  if [ -n "$face" ] && [ "$face" -eq 0 ]; then echo "$POOL_GRACE face_empty"; return; fi
  next=$(printf '%s' "$line" | sed -n 's/.*llm_pool_next_attempt_unix=\([0-9][0-9]*\).*/\1/p')
  [ -n "$next" ] || { echo "0 open"; return; }
  now=$(date +%s)
  if [ "$next" -gt "$now" ]; then
    w=$(( next - now ))
    [ "$w" -gt "$POOL_MAX_WAIT" ] && w=$POOL_MAX_WAIT
    echo "$w next_attempt"
    return
  fi
  # 界过了，但界过了不等于池子开了：最近一拍样本仍报 available=0 就再等一个宽限，
  # 把「要不要起一轮」交给下一拍采样，而不是拿一条已被证伪的预测去起。
  avail=$(printf '%s' "$line" | sed -n 's/.*llm_pool_available=\([0-9][0-9]*\).*/\1/p')
  if [ -n "$avail" ] && [ "$avail" -eq 0 ]; then echo "$POOL_GRACE still_unavailable"; return; fi
  echo "0 open"
}

# 最后一拍样本有多旧（秒）：`-` 表示读不出时刻，不冒充新鲜也不冒充陈旧。
pool_sample_age() {
  local ts s
  ts=$(tail -1 "$POOL_LOG" 2>/dev/null | awk '{print $1}')
  case "$ts" in
    [0-9][0-9][0-9][0-9]-*T*Z) ;;
    *) echo '-'; return ;;
  esac
  s=$(date -u -d "$ts" +%s 2>/dev/null) || { echo '-'; return; }
  echo $(( $(date +%s) - s ))
}

# 到期时刻：请求的时长与「样本里那条恢复时刻 + 宽限」取更晚的，封顶 HORIZON_MAX。
# 读不出 / 不是数 ⇒ 只用请求的时长，不发明一个远界。
horizon_end() { # $1 = 请求的时长（秒）
  local now end rec cap
  now=$(date +%s); end=$(( now + $1 ))
  rec=$(tail -1 "$POOL_LOG" 2>/dev/null \
        | sed -n 's/.*llm_pool_evidenced_recovery_unix=\([0-9][0-9]*\).*/\1/p')
  case "${rec:-}" in ''|*[!0-9]*) echo "$end"; return ;; esac
  [ "$rec" -gt "$end" ] || { echo "$end"; return; }
  end=$(( rec + HORIZON_GRACE ))
  cap=$(( now + HORIZON_MAX ))
  [ "$end" -gt "$cap" ] && end=$cap
  echo "$end"
}

watcher_running() { pgrep -f "$pattern_watch" >/dev/null 2>&1; }

# 起的这条覆盖到值守自己的到期，而不是用采样脚本里那个 4 小时的默认：默认比值守短一个
# 数量级，到期后同样是沉默。_SCRIPT 可覆盖是为了夹具能塞一个假的，不去真连集群。
start_pool_watcher() {
  local horizon=$1
  [ -r "$POOL_WATCH_SCRIPT" ] || { say "pool sample loop missing at $POOL_WATCH_SCRIPT; the pool reading will go stale"; return 1; }
  INTERVAL="$POOL_WATCH_INTERVAL" HORIZON="$horizon" LOG="$POOL_LOG" CKPT="$CKPT" \
    nohup bash "$POOL_WATCH_SCRIPT" >>"$POOL_WATCH_LOG" 2>&1 &
  say "no pool sample loop was running; started one (interval=${POOL_WATCH_INTERVAL}s horizon=${horizon}s)"
}

ensure_pool_watcher() {
  local horizon=${1:-$POOL_MAX_WAIT}
  watcher_running && return 0
  start_pool_watcher "$horizon"
}

# 每一轮结束后把「跳过这一腿在真报表里成不成立」记一行。主臂那一半过去只有人读报表，
# 没有读数：`## skipped` 在源码里、在诊断臂的产物里都成立，唯独「判了 198 行的那一份
# 报表里有没有这一段」没人量。读不出来记 unread，不记成 holds。
report_leg() {
  [ -r "$REPORT" ] || { echo "unread"; return; }
  local out final named
  out=$(bash "$HERE/eval-skipped-leg-reading.sh" "$REPORT" 2>/dev/null)
  # 读数脚本印两行 verdict：先印具名的错法（no-section / does-not-hold），末行印总结。
  # 只取末行会把「为什么」丢掉，只取首行会把总结丢掉，所以两个都带。
  final=$(printf '%s\n' "$out" | sed -n 's/^verdict=//p' | tail -1)
  named=$(printf '%s\n' "$out" | sed -n 's/^verdict=//p' | grep -v -e '^holds$' -e '^does-not-hold$' | head -1)
  [ -n "$final" ] || { echo "unread"; return; }
  if [ -n "$named" ]; then echo "$final($named)"; else echo "$final"; fi
}

main() {
  local end n w rc now
  # 上一次往 rerun.log 写「池子还关着」的时刻；0 表示还没写过。用它给那条日志做时间预算，
  # 见 WAIT_LOG_EVERY 上面那段。
  local last_wait_said=0
  end=$(horizon_end "${1:-28800}")
  say "start pid=$$ horizon=$(( end - $(date +%s) ))s target=$TARGET"
  while [ "$(date +%s)" -lt "$end" ]; do
    # 开关关着就到此为止：不判池、不起采样、更不起一轮。判在 pgrep 之前是有意的——
    # 一条已经在跑的循环由 `eval-switch.sh off` 终止，这里等的是「关着的时候不要
    # 再起一条」，而池采样那条腿也花钱、也连集群，同样不该起来。
    if eval_paused; then
      say "the eval switch is off ($PAUSE_FLAG); no round started, not judging the pool"
      return 0
    fi
    if ! pgrep -f "$pattern" >/dev/null; then
      n=$(lines)
      if reached "$n"; then say "target reached (judged=$((n-1)) legs=$(leg_report)); stopping"; return 0; fi
      if [ "$n" -gt "$TARGET" ]; then
        say "total is at $((n-1)) but the legs are short (target $(leg_target "$TARGET") each: $(leg_report)); not stopping"
      fi
      # 先保证采样循环在跑：没有新样本时，`pool_wait_seconds` 只能读到最后那一行，
      # 而最后一行说「关着」与「采样停了」在读数上同形。
      ensure_pool_watcher "$(( end - $(date +%s) ))"
      local age age_s
      age=$(pool_sample_age)
      if [ "$age" = '-' ]; then age_s='age=unknown'; else age_s="age=${age}s"; fi
      read -r w why <<<"$(pool_wait_seconds)"
      why=${why:-no_sample}
      if [ "$w" -gt 0 ]; then
        now=$(date +%s)
        if [ "$now" -ge "$(( last_wait_said + WAIT_LOG_EVERY ))" ]; then
          case "$why" in
            face_empty)        reason="face ${POOL_FACE} has no serving member" ;;
            next_attempt)      reason="the gateway's next attempt is in the future" ;;
            still_unavailable) reason="the bound passed but the pool still reads unavailable" ;;
            no_sample)         reason="this sample did not answer" ;;
            *)                 reason="$why" ;;
          esac
          say "pool closed (${reason}; sample ${age_s}); waiting ${w}s before starting a round"
          last_wait_said=$now
        fi
        sleep "$w"
        continue
      fi
      # 池子开了（或判据说该起一轮）：这一次等待到此为止，下一次再等要立刻记一行，
      # 否则「等了很久」与「刚等上，紧接着就开了」在日志上同形。
      last_wait_said=0
      say "no loop is running; starting one (judged=$((n-1)) target=$TARGET)"
      ( cd "$REPO" && STALL=600 ORIGIN=eval bash scripts/eval/eval-registered-rerun.sh --target "$TARGET" >>"$OUT" 2>&1 )
      rc=$?
      say "the loop exited rc=$rc; report skipped-leg=$(report_leg); rechecking in 60s"
    fi
    sleep 60
  done
  say "horizon reached; stopping (judged=$(( $(lines) - 1 )))"
}

# 被 source 时不跑主循环：夹具要能只测 pool_wait_seconds 这一个判据。
if [ "${BASH_SOURCE[0]}" = "$0" ]; then main "$@"; fi

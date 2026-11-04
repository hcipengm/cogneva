#!/bin/bash
# 每隔几分钟朝网关真发一次最小请求，池子一开就起续跑循环；池子关着时只花一个 503。
#
# 为什么不在「上游自己报的恢复时刻」干等到点：那个时刻是上游给的**下界**，不是时刻表。
# 2026-10-19T01:23Z 那次它报 04:43:18Z，而 01:27Z 网关自己放行了 13 条——中间存在几十分钟
# 的瞬时窗口。等到 04:43 才起跑，等于把这类窗口全扔了；而每三分钟发一个小请求，
# 关着的时候网关是**立即**回 503（实测 0.7 ms），不建 Pod、不读 /data、不烧 wall clock。
#
# 反过来也不能每轮都起 Pod 去探：池子关着时一轮要 60–100 秒，且每个 Pod 都要把 /data
# 读一遍。所以探针放在**网关这一层**，探到了才让循环去建 Pod。
#
# 用法：eval-probe-window.sh [--interval 180] [--until <unix>]
set -u
URL=${URL:-http://10.43.158.118:8081/v1/chat/completions}
MODEL=${MODEL:-ark-code-latest}
INTERVAL=${INTERVAL:-180}
UNTIL=${UNTIL:-$(( $(date +%s) + 21600 ))}
LOG=${LOG:-$HOME/eval-runs/.runner/rerun.log}
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 从一份拒绝正文里取出这一窗口的四个量。取不到就什么都不印——**不许印 0**：
# 「正文里没有这几个字段」与「允许额真的是 0」是两件事，后者是一个真实的读数。
pace_of() { # $1 = 正文文件
  local f=${1:?body file}
  [ -r "$f" ] || return 0
  local a r s w
  a=$(grep -oE '"window_allowance_tokens":[0-9]+' "$f" | head -1 | cut -d: -f2)
  r=$(grep -oE '"window_reserve_tokens":[0-9]+' "$f" | head -1 | cut -d: -f2)
  s=$(grep -oE '"window_spent_tokens":[0-9]+' "$f" | head -1 | cut -d: -f2)
  w=$(grep -oE '"window_start_unix":[0-9]+' "$f" | head -1 | cut -d: -f2)
  [ -n "$a" ] || return 0
  printf 'allowance:%s,reserve:%s,spent:%s,start:%s\n' "$a" "${r:-?}" "${s:-?}" "${w:-?}"
}

# 还差多少秒翻过储备线：ceil((reserve-allowance)/rate)。
# 反解只在「这一窗口继续爬坡」的前提下成立；调用方拿到下一条读数（尤其是 503）就该把它丢掉。
estimate() { # $1 allowance $2 reserve $3 rate(tokens/s)
  local a=$1 r=$2 rate=$3
  if ! [[ $a =~ ^[0-9]+$ && $r =~ ^[0-9]+$ && $rate =~ ^[0-9]+$ ]]; then
    echo "unknown"; return 0
  fi
  if [ "$rate" -le 0 ]; then echo "unknown"; return 0; fi
  if [ "$r" -le "$a" ]; then echo 0; return 0; fi
  echo $(( (r - a + rate - 1) / rate ))
}

while [ $# -gt 0 ]; do
  case "$1" in
    --interval) INTERVAL=$2; shift 2;;
    --until) UNTIL=$2; shift 2;;
    # 离线用法：喂一份历史正文，印出四个量（或什么都不印）。测试与人工复盘都走这里。
    --pace) pace_of "$2"; exit 0;;
    # 离线用法：印出还差多少秒翻过储备线。
    --estimate) estimate "$2" "$3" "$4"; exit 0;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG"; }

log "probe start interval=${INTERVAL}s until=$(date -u -d "@$UNTIL" +%FT%TZ) model=$MODEL"
last_code=
while [ "$(date +%s)" -lt "$UNTIL" ]; do
  # 锚在「bash 后面直接跟这个脚本」：排队中的提交把路径写在参数里，裸的路径子串
  # 会把它误读成「循环在跑」。
  if pgrep -f '^bash [^ ]*host/eval-registered-rerun\.sh' >/dev/null 2>&1; then
    log "probe: a loop is running; standing down"
    exit 0
  fi
  body=$(mktemp)
  code=$(curl -s -o "$body" -w '%{http_code}' --max-time 20 \
    -X POST "$URL" -H 'content-type: application/json' \
    -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"ok\"}],\"max_tokens\":4,\"stream\":false}" \
    2>/dev/null)
  if [ "$code" = 200 ]; then
    rm -f "$body"
    log "probe: the gateway answered 200; starting the loop"
    STALL=${STALL:-600} ORIGIN=${ORIGIN:-eval-pod-77} exec bash "$here/eval-registered-rerun.sh" "$@"
  fi
  # 429 的正文里带着判据本身：允许额、储备额、已花额、窗口起点。斜坡期 allowance 从 0
  # 往上爬，爬到 reserve 以上才放行——所以「池子关着」有两种脸：额度真花光了（spent 大），
  # 和这一窗口刚开始、允许额还没爬到储备线（spent=0）。后者会自己开，前者要等下一个窗口。
  pace=$(pace_of "$body")
  rm -f "$body"
  # 401/402/429/503 都是「现在不能用」；只有 200 才算窗口开了。
  # 每个探针一行会把日志淹掉（一天 480 行），所以每 5 次（约 15 分钟）或码变了才写。
  probe_n=$(( ${probe_n:-0} + 1 ))
  if [ "$code" != "$last_code" ] || [ $(( probe_n % 5 )) -eq 0 ]; then
    log "probe: http=$code${pace:+ pace=$pace}; waiting ${INTERVAL}s"
    last_code=$code
  fi
  sleep "$INTERVAL"
done
log "probe: until reached without a usable window"
exit 1

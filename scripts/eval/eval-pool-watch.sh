#!/bin/bash
# 按节奏记一次上游池的读数，让「窗口几点开的」是一行文件而不是事后复述。
#
# 为什么不能只信指标：`llm_pool_available` 两个方向都骗过人（池关着却读 1，开着却读 0）。
# 所以它跟续跑循环的已判例数记在同一条线上，两个数一起看：只有 judged 涨了才算真进了窗口。
# 见 memory/project_all_llm_columns_share_one_five_hour_quota_pool.md。
#
# 为什么要单独一个循环：报表里的 `pool\t…` 行只在**起跑那一刻**写一次，两轮之间（约 3 分钟）
# 没有任何读数；开窗往往就落在这段空档里。
#
# 退出码：0＝至少读到过一次；3＝一次都没读到（网关连不上／指标名变了），不许当 0 读。
set -u

RUNS_DIR=${RUNS_DIR:-$HOME/eval-runs}
OUT=${OUT:-/eval/20261018_post-recovery.md}
NAMESPACE=${NAMESPACE:-cogneva}
PORT=${PORT:-18093}
INTERVAL=${INTERVAL:-120}
HORIZON=${HORIZON:-14400}
LOG=${LOG:-$RUNS_DIR/.runner/pool.log}
CKPT=${CKPT:-$RUNS_DIR/$(basename "$OUT").cases.jsonl}

judged() {
  [ -f "$CKPT" ] || { echo 0; return; }
  local n
  n=$(grep -c . "$CKPT")
  # 首行是 checkpoint 的 run 头，不是用例。
  if head -1 "$CKPT" | grep -q '"kind":"header"'; then echo $((n - 1)); else echo "$n"; fi
}

mkdir -p "$(dirname "$LOG")"
end=$(( $(date +%s) + HORIZON ))
read_any=0
while [ "$(date +%s)" -lt "$end" ]; do
  sudo -n kubectl -n "$NAMESPACE" port-forward svc/cogneva-security-gateway "$PORT":8081 >/dev/null 2>&1 &
  pf=$!
  sleep 2
  # 按面那一格同时记下来：池级 `available` 是**整个池**的容量，池里住着两种协议面时，
  # 一面全灭、另一面健康，它照样读 1，而续跑循环这一侧的每一次调用都在熔断处回 503。
  # 2026-10-24 实测就是这样：openai 面唯一那家回了额度周期、anthropic 面健康，池级读 1，
  # 续跑按它判「窗口开了」，一轮轮起跑、一轮轮零产出。所以这一行里两面都写。
  body=$(curl -s --max-time 4 "http://127.0.0.1:$PORT/metrics" \
    | grep -E '^llm_pool_(available|evidenced_recovery_unix|next_attempt_unix) |^llm_pool_face_serving\{' \
    | awk '{print $1"="$2}' \
    | sed -E 's/^llm_pool_face_serving\{face="([^"]+)"\}=/face_serving[\1]=/' \
    | tr '\n' ' ')
  kill "$pf" 2>/dev/null; wait "$pf" 2>/dev/null
  [ -n "$body" ] && read_any=1
  printf '%s judged=%s %s\n' "$(date -u +%FT%TZ)" "$(judged)" "${body:-metrics-unreachable}" >>"$LOG"
  sleep "$INTERVAL"
done
[ "$read_any" = 1 ] && exit 0 || exit 3

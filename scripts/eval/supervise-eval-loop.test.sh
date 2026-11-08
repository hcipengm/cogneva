#!/usr/bin/env bash
# principle: feedback_a_reader_that_waits_for_the_next_sample_must_keep_the_sampler_alive
# principle: feedback_a_sampling_period_coarser_than_the_shortest_event_reports_absence_as_fact
# principle: feedback_a_sample_i_write_myself_only_tests_the_gate_against_itself
#
# `supervise-eval-loop.sh` 的夹具：不起循环、不起 Pod，只判三件在盘上就能判的事。
#
#   1 「有没有循环在跑」的模式必须匹配**带参数**的循环命令行（2026-10-19 实测：
#     锚在 `[.]sh$` 上的模式匹配不到 `... --target 198`，守卫是死代码）；
#   2 同一个模式不许匹配「路径出现在别处」的命令行（提交把路径写在参数里）；
#   3 值守的 TARGET 必须等于题集行数减去报表里按设计跳过的行数——用一份跑完形状的
#     报表喂 `eval-ceiling-reading.sh` 来判，不在这里抄 198。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/supervise-eval-loop.sh"
CEIL="$HERE/eval-ceiling-reading.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }
[ -f "$CEIL" ] || { echo "FAIL: 读数脚本不在：$CEIL" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

# 模式从脚本里原样取，不在夹具里再写一份——两处各写一份，改了一处另一处会静默失配。
pattern=$(sed -n "s/^pattern='\(.*\)'$/\1/p" "$SCRIPT")
[ -n "$pattern" ] || { echo "FAIL: 脚本里读不到 pattern=" >&2; exit 1; }

# 宽限的期望值同样从脚本里读：写死 600 时，周期收到 120 而宽限没跟上的那一版夹具照样绿，
# 红的是将来的读数而不是夹具。
grace=$(sed -n 's/^POOL_GRACE=${POOL_GRACE:-\([0-9]*\)}$/\1/p' "$SCRIPT")
[ -n "$grace" ] || { echo "FAIL: 脚本里读不到 POOL_GRACE 的默认值" >&2; exit 1; }

# 判模式时只看**造出来的那条命令行**，不看全局进程表：宿主上真有一条循环在跑时，
# `pgrep -f` 总会命中它，负方向就永远判不出来（2026-10-19 实测就是这么假红的）。
# 所以用 `exec -a` 造 argv，再从 /proc 里读它的 cmdline、拿同一个模式去比。
cmdline_of() { # $1 = 想测的整句
  # 不用固定 sleep：宿主被构建压着时 0.3 秒内 `execve` 都可能还没落地，这时读到的
  # `/proc/<pid>/cmdline` 是**空的**，而空串与「模式不匹配」在判词上同形——负方向那格
  # 会假绿、正方向那格会假红（2026-10-29 实测：整树被判红的第二个成员就是它，重跑一次
  # 又绿了）。改成等到 argv[0] 真的等于想测的那一句再读，最多等 5 秒；等不到就照旧读，
  # 让那一格红在它该红的判词上，而不是悄悄放行。
  bash -c 'exec -a "$1" sleep 3' _ "$1" &
  local pid=$! i
  for i in $(seq 1 100); do
    [ "$(tr '\0' '\n' <"/proc/$pid/cmdline" 2>/dev/null | head -1)" = "$1" ] && break
    sleep 0.05
  done
  tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | sed 's/ $//'
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
}
match() { # $1 = 命令行；模式从脚本原样取
  local line=$1
  # shellcheck disable=SC2053
  [[ $line =~ $pattern ]]
}

line=$(cmdline_of 'bash scripts/eval/eval-registered-rerun.sh --target 198')
if match "$line"; then ok "模式匹配带参数的循环命令行"
else bad "模式匹配不到带参数的循环（$line，守卫会永远判「没在跑」）"; fi

line=$(cmdline_of 'bash scripts/principles/principle-commit.sh /tmp/msg.txt scripts/eval/eval-registered-rerun.sh')
if match "$line"; then bad "模式把「路径写在参数里」的命令行也算成循环在跑（$line）"
else ok "模式不匹配只把路径写在参数里的命令行"; fi

# TARGET 与天花板：240 行题集、7 题×2 scaffold×3 seed = 42 跳过 ⇒ 已判 198。
target=$(sed -n 's/^TARGET=${TARGET:-\([0-9]*\)}$/\1/p' "$SCRIPT")
[ -n "$target" ] || { echo "FAIL: 脚本里读不到 TARGET 默认值" >&2; exit 1; }
{
  echo '## per seed'
  for s in gepa agentflow; do for seed in 0 1 2; do
    printf 'cell\thle\t%s\tseed=%s\tresolved=20\ttotal=33\tskipped=7\terrored=0\n' "$s" "$seed"
  done; done
  echo; echo '## skipped'
  for i in 1 2 3 4 5 6 7; do
    printf 'skipped\thle\thle-case-%s\tscaffolds=2\tseeds=3\tthe backbone cannot see images\n' "$i"
  done
} >"$TMP/complete.md"
out=$(bash "$CEIL" "$TMP/complete.md" --target "$target" --sky 240 2>&1); rc=$?
[ "$rc" -eq 0 ] && ok "TARGET=$target 与 240−42 的天花板一致" || bad "TARGET=$target 与天花板不一致（$out）"

# 池子关着时的等待：读网关报的下一次尝试，读到就等，不按 30 分钟一轮空起 Pod。
#
# 判据输出 `"<秒> <为什么>"`（见脚本里那个函数的注释）。这里只取第一格判秒数，第二格由
# `why_for` 单独判：把两格拼在一串里比较，改判词就得改每一条秒数断言，而它们答的不是
# 同一件事。
wait_for() { # $1 = pool.log 内容；其余参数作为环境
  local body=$1; shift
  printf '%s\n' "$body" >"$TMP/pool.log"
  env POOL_LOG="$TMP/pool.log" "$@" bash -c 'source "$1"; pool_wait_seconds' _ "$SCRIPT" | awk '{print $1}'
}
why_for() { # 同一份输入，取第二格（这一拍为什么不算开门）
  local body=$1; shift
  printf '%s\n' "$body" >"$TMP/pool.log"
  env POOL_LOG="$TMP/pool.log" "$@" bash -c 'source "$1"; pool_wait_seconds' _ "$SCRIPT" | awk '{print $2}'
}
now=$(date +%s)
# 这里不能钉死 120：夹具算 `now` 与被测函数自己取 `date +%s` 之间会跨秒，
# 2026-10-20 实测判树里真出现过 119（提交门因此红）。差值只容许 1 秒的时钟漂移。
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=0 llm_pool_next_attempt_unix=$((now + 120))")
[ "$w" -ge 119 ] && [ "$w" -le 120 ] && ok "下一次尝试在 120 秒后 ⇒ 等 119–120 秒（跨秒容许 1 秒）" || bad "等待秒数：期望 119–120，实得 $w"
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=$((now - 120))")
[ "$w" -eq 0 ] && ok "下一次尝试已过期 ⇒ 0（立刻起一轮）" || bad "过期：期望 0，实得 $w"
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=0")
[ "$w" -eq 0 ] && ok "读数里没有 next_attempt ⇒ 0" || bad "缺字段：期望 0，实得 $w"
w=$(wait_for "2026-10-19T00:00:00Z llm_pool_available=0 llm_pool_next_attempt_unix=$((now + 99999))" POOL_MAX_WAIT=1800)
[ "$w" -eq 1800 ] && ok "下一次尝试很远 ⇒ 截到 POOL_MAX_WAIT" || bad "截断：期望 1800，实得 $w"
w=$(env POOL_LOG="$TMP/no-such-pool.log" bash -c 'source "$1"; pool_wait_seconds' _ "$SCRIPT" | awk '{print $1}')
[ "$w" -eq 0 ] && ok "没有 pool.log ⇒ 0" || bad "无 pool.log：期望 0，实得 $w"
# 采样读不到指标时写 `metrics-unreachable`，两个字段都没有：**不答不等于开门**。返回 0 会
# 当场起一轮，那一轮只会在 503 里空转到被判停。2026-10-19T08:58:44Z 实测出现过一行。
w=$(wait_for "2026-10-20T00:00:00Z judged=0 metrics-unreachable")
[ "$w" -eq "$grace" ] && ok "这一拍没回答（metrics-unreachable）⇒ 等下一拍（${grace}s），不当成开门" \
  || bad "未回答：期望 ${grace}，实得 $w"
# 界过了不等于池子开了：网关重试失败会把 next_attempt 再推远，而这一拍样本仍说关着。
# 2026-10-19 实测就是这样空转了四轮 1815 秒，所以过期 + available=0 只等一个宽限，
# 把判据交给下一拍采样；只有说开了（available=1）才立刻起。
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=0 llm_pool_next_attempt_unix=$((now - 120))")
[ "$w" -eq "$grace" ] && ok "界过了但池仍关着 ⇒ 等一个宽限（${grace}s）" || bad "过期仍关：期望 ${grace}，实得 $w"
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=0 llm_pool_next_attempt_unix=$((now - 120))" POOL_GRACE=90)
[ "$w" -eq 90 ] && ok "宽限可由 POOL_GRACE 覆盖" || bad "宽限覆盖：期望 90，实得 $w"

# 池级读 1 而**这一臂要的那一面**读 0：池里住着两种协议面时，另一面还有健康成员就是
# 这个形状。2026-10-24 实测：openai 面唯一那家回了额度周期、anthropic 面健康，池级读 1、
# next_attempt 已过期，值守每半小时起一轮、每轮零产出。面读 0 就是关门，要等下一拍。
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=$((now - 120)) face_serving[openai]=0 face_serving[anthropic]=1")
[ "$w" -eq "$grace" ] && ok "池级开着而这一面全灭 ⇒ 等下一拍（${grace}s），不起一轮" \
  || bad "面全灭：期望 ${grace}，实得 $w"
# 同一行，这一面还有一个成员能接：照旧立刻起一轮。少了这一格，上面那条可以靠「永远等」
# 变绿，而那份改动会让值守在池子真开着的时候停摆。
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=$((now - 120)) face_serving[openai]=1 face_serving[anthropic]=0")
[ "$w" -eq 0 ] && ok "这一面有成员能接 ⇒ 0（立刻起一轮）" || bad "面健康：期望 0，实得 $w"
# 旧格式的行（没有面这一格）退回只看池级：读不到不等于关门。
w=$(wait_for "2026-10-19T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=$((now - 120))")
[ "$w" -eq 0 ] && ok "行里没有面那一格 ⇒ 退回池级读数（0）" || bad "无面字段：期望 0，实得 $w"
# 判词那一格：五种关门原因必须分得开。都印成同一个词等于没印——2026-10-24 的日志把
# 四种原因一律写成「per the gateway's next attempt」，而当时真正的理由是那一面全灭，
# next_attempt 只是七分钟后；照那句话去查的人会得出「再等一会儿就好」。
for pair in \
  "face_empty|2026-10-19T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=$((now - 120)) face_serving[openai]=0 face_serving[anthropic]=1" \
  "next_attempt|2026-10-19T00:00:00Z judged=0 llm_pool_available=0 llm_pool_next_attempt_unix=$((now + 300))" \
  "still_unavailable|2026-10-19T00:00:00Z judged=0 llm_pool_available=0 llm_pool_next_attempt_unix=$((now - 120))" \
  "no_sample|2026-10-19T00:00:00Z judged=0 metrics-unreachable" \
  "open|2026-10-19T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=$((now - 120))" \
; do
  want=${pair%%|*}; body=${pair#*|}
  got=$(why_for "$body")
  [ "$got" = "$want" ] && ok "关门原因印成 ${want}（这一拍为什么不算开门说得出来）" \
    || bad "关门原因：期望 ${want}，实得「${got}」（${body}）"
done

# 等待的粒度不能比证据粗：样本每 POOL_WATCH_INTERVAL 秒来一行，等到比它更细没有新东西可看，
# 比它粗就会在一行「开着」上睡过去。2026-10-18 实测窗口只有 8–15 分钟，而原来截到 1800 秒。
maxw=$(sed -n 's/^POOL_MAX_WAIT=${POOL_MAX_WAIT:-\([0-9]*\)}$/\1/p' "$SCRIPT")
wint=$(sed -n 's/^POOL_WATCH_INTERVAL=${POOL_WATCH_INTERVAL:-\([0-9]*\)}$/\1/p' "$SCRIPT")
[ -n "$maxw" ] && [ -n "$wint" ] || { echo "FAIL: 读不到 POOL_MAX_WAIT / POOL_WATCH_INTERVAL 的默认值" >&2; exit 1; }
[ "$maxw" -le "$wint" ] && ok "等待上限 ${maxw}s 不比采样间隔 ${wint}s 粗" \
  || bad "等待上限 ${maxw}s 粗于采样间隔 ${wint}s：会在「开着」的样本上睡过去"

# 宽限等的是**下一拍样本**，所以它也不能比一拍长：否则整条链里最粗的一项变成它，
# 池子在这段里打开又关掉，值守还在睡——周期收紧那一半就白做了。
[ "$grace" -le "$wint" ] && ok "宽限 ${grace}s 不比采样间隔 ${wint}s 粗" \
  || bad "宽限 ${grace}s 粗于采样间隔 ${wint}s：等下一拍却睡过好几拍"

# 采样周期还得短于**实测最短的那段可用窗口**，否则一段短窗口可能落在两拍之间，一拍都没踩上。
# 2026-10-20 用集群 Prometheus 按 60 秒步长数 6 天：12 段可用里有 5 段短于 600 秒，
# 最短那段可用窗口是 480 秒（10-19T03:10–03:18）。这条钉的是「周期 ≤ 实测最短窗口」，
# 不是钉某个整数——将来数出更短的窗口，就把这个数改小，别把注释改大。
SHORTEST_MEASURED_WINDOW=480
[ "$wint" -le "$SHORTEST_MEASURED_WINDOW" ] \
  && ok "采样间隔 ${wint}s 不粗于实测最短可用窗口 ${SHORTEST_MEASURED_WINDOW}s" \
  || bad "采样间隔 ${wint}s 粗于实测最短可用窗口 ${SHORTEST_MEASURED_WINDOW}s：短窗口可能一拍都没踩上"

# 到目标要按腿判：这一臂登记了 4 个 scaffold，总数到 198 可能全是 gepa 一条腿。
# 2026-10-19 实测那份报表 78 行全是 gepa、agentflow 一行没落过 ⇒ 只看总数会早停。
# 检查点的坐标是四个（基准、外壳、种子、例 id），腿按「基准 + 外壳」数。
mkckpt() { # $1 = gepa 行数，$2 = agentflow 行数
  echo '{"kind":"header","run_id":"test"}' >"$TMP/ckpt.jsonl"
  local i
  for i in $(seq 1 "$1"); do echo "{\"benchmark\":\"hle\",\"scaffold\":\"gepa\",\"case\":\"g$i\"}" >>"$TMP/ckpt.jsonl"; done
  for i in $(seq 1 "$2"); do echo "{\"benchmark\":\"hle\",\"scaffold\":\"agentflow\",\"case\":\"a$i\"}" >>"$TMP/ckpt.jsonl"; done
}
reached_at() { # 用当前 ckpt 判 reached（参数是要喂给 reached 的总行数）
  env CKPT="$TMP/ckpt.jsonl" TARGET="$target" bash -c 'source "$1"; reached "$2" && echo yes || echo no' _ "$SCRIPT" "$(wc -l <"$TMP/ckpt.jsonl")"
}
mkckpt 99 99; v=$(reached_at)
[ "$v" = yes ] && ok "两条腿各 99（198 均摊）⇒ 到目标" || bad "两条腿各 99：期望 yes，实得 $v"
mkckpt 199 0; v=$(reached_at)
[ "$v" = no ] && ok "gepa 一条腿顶满 199、agentflow 零行 ⇒ 不算到目标" || bad "单腿顶满：期望 no，实得 $v"
mkckpt 98 99; v=$(reached_at)
[ "$v" = no ] && ok "总数差一行（197）⇒ 不算到目标" || bad "差一行：期望 no，实得 $v"
lt=$(env TARGET="$target" LEGS=gepa,agentflow bash -c 'source "$1"; leg_target "$TARGET"' _ "$SCRIPT")
[ "$lt" = 99 ] && ok "每条腿的分摊是 198/2=99" || bad "分摊：期望 99，实得 $lt"

# 每轮结束那一行要带「跳过这一腿」的判词：读得到就记判词，读不到记 unread，
# 不许把「没读到」记成绿。
leg_for() { # $1 = 报表路径
  env REPORT="$1" bash -c 'source "$1"; report_leg' _ "$SCRIPT"
}
{
  echo '## per seed'
  echo 'cell	hle	gepa	seed=0	resolved=0	total=0	skipped=1	errored=0'
  echo; echo '## skipped'
  echo 'skipped	hle	hle-x	scaffolds=1	seeds=1	the backbone cannot see images'
} >"$TMP/with-skip.md"
v=$(leg_for "$TMP/with-skip.md")
[ "$v" = holds ] && ok "报表有跳过段 ⇒ 判词 holds" || bad "报表有跳过段：期望 holds，实得 $v"
v=$(leg_for "$TMP/complete.md")
[ "$v" = holds ] && ok "跑完的主臂形状 ⇒ 判词 holds" || bad "主臂形状：期望 holds，实得 $v"
printf '## per seed\n' >"$TMP/no-skip.md"
v=$(leg_for "$TMP/no-skip.md")
[ "$v" = 'does-not-hold(no-section)' ] && ok "报表没有跳过段 ⇒ 判词带出 no-section" || bad "无跳过段：期望 does-not-hold(no-section)，实得 $v"
v=$(leg_for "$TMP/no-such-report.md")
[ "$v" = unread ] && ok "报表读不到 ⇒ unread（不记成 holds）" || bad "读不到：期望 unread，实得 $v"

# 采样循环自己会到期（脚本默认 4 小时）、也会被杀掉。它一停，pool.log 最后一行永远读作
# 「池子关着」⇒ 值守在宽限里无限期打转，池子开了也不起一轮，而这段空白没有任何读数说出来。
# 所以值守每一拍先保证采样循环在跑，并把它起的时长挂到自己的到期上。
watch_pattern=$(sed -n "s/^pattern_watch='\(.*\)'$/\1/p" "$SCRIPT")
[ -n "$watch_pattern" ] || { echo "FAIL: 脚本里读不到 pattern_watch=" >&2; exit 1; }
watch_match() { local l=$1; [[ $l =~ $watch_pattern ]]; }
line=$(cmdline_of 'bash scripts/eval/eval-pool-watch.sh')
if watch_match "$line"; then ok "采样循环的模式匹配它自己的命令行（$line）"
else bad "采样循环模式匹配不到：$line"; fi
line=$(cmdline_of 'bash scripts/eval/eval-registered-rerun.sh --target 198')
if watch_match "$line"; then bad "采样循环模式把评测循环也算进去了：$line"
else ok "采样循环的模式不匹配评测循环（两者是独立的两条）"; fi
# 同一刀对 `pattern` 已经做过一次，`pattern_watch` 当时漏了：一笔排队的提交
# （`principle-commit.sh … scripts/eval/eval-pool-watch.sh`）argv 里也有这个脚本名，
# 不锚就会把提交当成采样循环在跑 ⇒ 明明没有采样也不补起。
line=$(cmdline_of 'bash scripts/principles/principle-commit.sh /tmp/m.txt scripts/eval/eval-pool-watch.sh')
if watch_match "$line"; then bad "采样循环模式把排队的提交当成采样在跑：$line"
else ok "采样循环的模式不匹配只把路径写在参数里的命令行"; fi

# 样本年龄：读不出时刻就印 -，不冒充新鲜——「刚采过」与「采样停了」在读数上同形。
age_for() {
  printf '%s\n' "$1" >"$TMP/age.log"
  env POOL_LOG="$TMP/age.log" bash -c 'source "$1"; pool_sample_age' _ "$SCRIPT"
}
a=$(age_for "$(date -u -d '-3 hours' +%FT%TZ) judged=0 llm_pool_available=0")
[ "$a" -ge 10790 ] && [ "$a" -le 10810 ] && ok "样本年龄读得出来（3 小时 ≈ 10800s）" || bad "年龄：期望 ≈10800，实得 $a"
a=$(age_for "garbage judged=0 llm_pool_available=0")
[ "$a" = '-' ] && ok "首字段不是时刻 ⇒ 年龄印 -" || bad "无时刻：期望 -，实得 $a"

# 到期要盖住证据点名的下一个窗口：上游自报的恢复时刻可能比任何一次探针尝试远得多
# （2026-10-20 实测 15.6 天）——只按「从现在起 N 小时」算，值守会在窗口打开前退出，
# 而日志上那行 `horizon reached` 与「到目标了」同形。封顶防一条坏样本把值守变成永久进程。
horizon_for() { # $1 = pool.log 内容；其余 = 环境
  local body=$1; shift
  printf '%s\n' "$body" >"$TMP/h.log"
  env POOL_LOG="$TMP/h.log" "$@" bash -c 'source "$1"; horizon_end 28800' _ "$SCRIPT"
}
hn=$(date +%s)
h=$(horizon_for "2026-10-20T00:00:00Z llm_pool_available=0 llm_pool_evidenced_recovery_unix=$((hn + 1296000))")
[ "$h" -ge $((hn + 1299600)) ] && [ "$h" -le $((hn + 1300000)) ] \
  && ok "恢复时刻在 15 天后 ⇒ 到期跟着它（+宽限 3600s）" \
  || bad "恢复时刻：期望 ≈now+1300000，实得 $h"
h=$(horizon_for "2026-10-20T00:00:00Z llm_pool_available=0 llm_pool_next_attempt_unix=$((hn + 600))")
[ "$h" -ge $((hn + 28798)) ] && [ "$h" -le $((hn + 28802)) ] \
  && ok "样本里没有恢复时刻 ⇒ 只用请求的时长（28800s）" \
  || bad "无恢复时刻：期望 ≈now+28800，实得 $h"
h=$(horizon_for "2026-10-20T00:00:00Z llm_pool_evidenced_recovery_unix=$((hn + 315360000))")
[ "$h" -le $((hn + 2678402)) ] && [ "$h" -ge $((hn + 2678398)) ] \
  && ok "恢复时刻十年后 ⇒ 封顶 HORIZON_MAX（31 天）" \
  || bad "封顶：期望 ≈now+2678400，实得 $h"
h=$(horizon_for "2026-10-20T00:00:00Z llm_pool_evidenced_recovery_unix=$((hn - 1000))")
[ "$h" -le $((hn + 28802)) ] && [ "$h" -ge $((hn + 28798)) ] \
  && ok "恢复时刻已过去 ⇒ 不缩短，仍用请求的时长" \
  || bad "过去的恢复时刻：期望 ≈now+28800，实得 $h"

# 起采样循环：把 INTERVAL/HORIZON/LOG/CKPT 都传下去，且 HORIZON 跟着值守自己的到期。
cat >"$TMP/fake-watch.sh" <<'EOF'
#!/bin/bash
printf 'interval=%s horizon=%s log=%s ckpt=%s\n' "$INTERVAL" "$HORIZON" "$LOG" "$CKPT" >"$OUT_MARK"
EOF
start_watch() {
  env OUT_MARK="$TMP/seen" POOL_WATCH_SCRIPT="$TMP/fake-watch.sh" POOL_WATCH_LOG="$TMP/w.log" \
      POOL_LOG="$TMP/pool.log" LOG="$TMP/rerun.log" CKPT="$TMP/ck.jsonl" \
      bash -c 'source "$1"; start_pool_watcher 1234' _ "$SCRIPT"
  local i=0
  while [ ! -s "$TMP/seen" ] && [ "$i" -lt 30 ]; do sleep 0.1; i=$((i + 1)); done
  cat "$TMP/seen" 2>/dev/null
}
seen=$(start_watch)
# 间隔期望值从脚本里读，不写死：写死的那个数在间隔调整之后会让这条夹具变红，
# 而红的是夹具不是被测行为——上一版就写死过 600。
case "$seen" in
  "interval=$wint horizon=1234 log=$TMP/pool.log ckpt=$TMP/ck.jsonl") ok "起采样循环时把间隔（$wint）、时长、日志与检查点都传下去" ;;
  *) bad "起采样循环：期望 interval=$wint horizon=1234 与日志/检查点路径，实得 ${seen:-（没有跑起来）}" ;;
esac

# 已经有一条在跑就不再起第二条：单槽的东西起两条会让两行读数互相顶着。
rm -f "$TMP/seen"; mkdir -p "$TMP/bin"
printf '#!/bin/sh\nexit 0\n' >"$TMP/bin/pgrep"; chmod +x "$TMP/bin/pgrep"
env PATH="$TMP/bin:$PATH" OUT_MARK="$TMP/seen" POOL_WATCH_SCRIPT="$TMP/fake-watch.sh" \
    POOL_WATCH_LOG="$TMP/w.log" LOG="$TMP/rerun.log" CKPT="$TMP/ck.jsonl" \
    bash -c 'source "$1"; ensure_pool_watcher 1234' _ "$SCRIPT"
sleep 0.3
[ ! -s "$TMP/seen" ] && ok "已有一条采样循环在跑 ⇒ 不再起第二条" || bad "重复起采样循环：seen=$(cat "$TMP/seen")"

# 开关键：关着的时候一轮都不起。
#
# 这一格要的是**负方向**，而负方向最容易写成永远为真——一个根本没跑起来的值守也「没有
# 起过一轮」。所以正反两跑用同一份夹具，只差标记文件在不在：正跑必须先证明这一份夹具
# 在不关的时候真能起出一轮，否则「关着没起」这一格证明不了任何事。
mkdir -p "$TMP/repo/scripts/eval"
cat >"$TMP/repo/scripts/eval/eval-registered-rerun.sh" <<'EOF'
#!/bin/bash
echo ran >>"$LOOP_MARK"
EOF
printf '{"kind":"header"}\n' >"$TMP/short-ck.jsonl"
printf '2026-11-08T00:00:00Z judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=1\n' >"$TMP/switch-pool.log"
# 「有没有循环在跑」由 pgrep 桩钉死：宿主上真有一条循环在跑时（2026-11-08 实测就有一条
# 手工杀剩的）值守会正确地判「在跑」而不起一轮，那会把这一组断言整组判成「夹具坏了」。
# 桩只说这一件事，别的进程表照旧读真的。
mkdir -p "$TMP/bin-idle"
printf '#!/bin/sh\nexit 1\n' >"$TMP/bin-idle/pgrep"; chmod +x "$TMP/bin-idle/pgrep"
run_supervisor() { # 其余 = 环境；只跑一趟（horizon=5，超时兜住那个 sleep 60）
  env PATH="$TMP/bin-idle:$PATH" \
      REPO="$TMP/repo" CKPT="$TMP/short-ck.jsonl" LOG="$TMP/switch.log" OUT="$TMP/switch.out" \
      POOL_LOG="$TMP/switch-pool.log" POOL_WATCH_SCRIPT="$TMP/fake-watch.sh" \
      POOL_WATCH_LOG="$TMP/switch-w.log" OUT_MARK="$TMP/switch-seen" \
      LOOP_MARK="$TMP/loop-ran" "$@" timeout 20 bash "$SCRIPT" 5 >/dev/null 2>&1
}
rm -f "$TMP/loop-ran" "$TMP/paused"
run_supervisor PAUSE_FLAG="$TMP/paused"
[ -s "$TMP/loop-ran" ] && ok "不关的时候值守真能起出一轮（负方向那一格的前提）" \
  || bad "不关的时候也没起出一轮：整组开关断言都不成立，先查这一份夹具"

rm -f "$TMP/loop-ran" "$TMP/switch.log"
printf 'off at 2026-11-08T00:00:00Z\nreason: 各方面没有理顺\n' >"$TMP/paused"
run_supervisor PAUSE_FLAG="$TMP/paused"
[ ! -s "$TMP/loop-ran" ] && ok "开关关着 ⇒ 一轮都不起" \
  || bad "开关关着还起了一轮（关闸之后仍在花钱）"
grep -qF "the eval switch is off ($TMP/paused)" "$TMP/switch.log" 2>/dev/null \
  && ok "关着的那一跑在日志里点名了标记文件（否则「关着」与「一次都没跑」同形）" \
  || bad "关着的那一跑没在日志里点名标记文件：$(tail -2 "$TMP/switch.log" 2>/dev/null)"

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

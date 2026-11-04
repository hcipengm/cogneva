#!/bin/bash
# reading: producer=eval_evidence
# reading: series=eval_bench_cases,eval_bench_judged=conditional:eval_bench_cases,eval_bench_resolved=conditional:eval_bench_cases,eval_bench_score=conditional:eval_bench_cases,eval_bench_unattributed=conditional:eval_bench_cases,eval_judged_cases,eval_judged_remaining,eval_judged_target,eval_last_progress_age_seconds,eval_last_run_age_seconds,eval_last_run_cause,eval_last_run_failure,eval_last_run_retry_after_seconds,eval_leg_rows,eval_pool_available,eval_runs_by_outcome,eval_runs_flat,eval_verdict
# reading: note=商用交付面：评测判到第几例、目标是几、每条腿各判多少、每个基准此刻的分值（分母是已判数）、上游池开没开、多久没涨（脚本自印判词，退 0/1/3）
# principle: feedback_a_backlog_reading_needs_its_inflow_or_the_wait_has_no_terminator
#
# 评测证据腿的一条读数：已判例数（存量）、最近一次涨的时刻与这一小时的到达率、
# 以及上游池的两条读数。三格一起印，因为「没在长」有两个成因，单看一个数同形：
# 池子关着（窗口没到）与循环跑坏了（窗口到了也没吃上）在 judged 上都表现为不动。
#
# 为什么不直接读值守日志：那里每轮一行、几百行流水，「在长吗」要人肉数。
# 这里只做聚合，不再去敲集群：池读数由 eval-pool-watch.sh 每两分钟写一行。
#
# 退出码：0＝已到目标或在涨；1＝没在长（`verdict=` 分开成因：blocked-on-pool /
#         blocked-on-stale-pool-sample / blocked-on-upstream-quota /
#         blocked-on-incomplete-experiment / working / stalled-unknown）；
#         3＝读不到（检查点或日志缺失／为空），不许当 0 读。
set -u

# 只认两个入口：不带参数（人读的 key=value）与 `--prometheus`（聚合器读的暴露格式）。
# 别的参数一律按用法错退 2——宿主读数登记表的第 8 格对每一份登记脚本跑 `--serve`，要求它
# 退 2：端口只归聚合器，生产者自己开一个端口就是每加一面抢一次稀缺资源。忽略未知参数会
# 让这一格读成「它真去开端口了」。
case "${1:-}" in
  ""|--prometheus) ;;
  *) echo "usage: $0 [--prometheus]" >&2; exit 2 ;;
esac

CKPT=${CKPT:-$HOME/eval-runs/20261018_post-recovery.md.cases.jsonl}
LOG=${LOG:-$HOME/eval-runs/.runner/rerun.log}
POOL=${POOL:-$HOME/eval-runs/.runner/pool.log}
# 最后一轮自己写的那份报表。`--out` 每轮同名，所以它总是最近一轮的现场，而**每格为什么
# 没判出分**只有它 `## failures` 那一段说得清：日志里那一行留的是退出码的成因（「这次跑
# 成不成表」），与「题为什么没判」可以是两件事——2026-11-04 实测那段是
# `failure=diagnostic-arm`，而报表里每一格都是上游 `429`，两个成因的修法完全不同。
REPORT=${REPORT:-${CKPT%.cases.jsonl}}
# 目标按可判口径：198 = 33 题 × 3 seed × 2 scaffold；另 42 行（7 道带图题）在
# `--vision none` 下按设计跳过、不进分母。写成 240 会让这条读数永远报「没到」。
TARGET=${TARGET:-198}
# 到「目标」要按腿判：这一臂登记的网格比目标覆盖的大（3 基准 × 4 外壳 = 1440 行），
# 总数只由 gepa 一条腿累积也能过 198，而 agentflow 一行没落过。2026-10-19 实测那份
# 报表判过的 78 行全是 gepa。每条腿的分摊＝目标 / 腿数（198 / 2 = 99 = 33 题 × 3 seed）。
LEGS=${LEGS:-gepa,agentflow}
WINDOW=${WINDOW:-3600}   # 到达率按这一段时间算，秒
# 「停住」判据的窗口，与到达率的窗口不是同一个数。一轮从起 Pod 到被自己的 horizon 收掉
# 最长 45 分钟（`eval-registered-rerun.sh` 给每轮定的界），所以 1 小时的到达窗口会在
# 每一轮还没跑完时就报一次「窗口内零增量」。2026-11-04T22:54Z 实测过这一刻：有一个评测
# Pod 正在跑，网关记着它 30 分钟里 200 多次成功的模型调用，判词却写着 stalled-unknown，
# 面读数照着它把「商用交付」判红。停住要看的是「轮次跑完了也没落行」，那至少是两轮：
# 两轮都空才算停住，两轮之内只是「这一轮还没跑完」。
STALL_WINDOW=${STALL_WINDOW:-7200}

# `--prometheus`：把同一份判词翻成文本暴露格式，交给宿主读数聚合器（它把各面并到一个
# 端口上，面自己不再开端口、不再加单元）。这里**再调一次自己**再解析，而不是把中间量搬进
# 这一段：两种输出必须永远说同一句话，抄一遍就有两种口径——这一族读数最怕的正是同一件事
# 有两个数（本文件头那三格之所以要一起印，也是这个理由）。
if [ "${1:-}" = "--prometheus" ]; then
  self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  out=$(bash "$self" 2>/dev/null); rc=$?
  case "$rc" in 0|1) ;; *) exit 3 ;; esac   # 读不到（3）与任何意外退出都不印序列
  # 先按空格拆行：`judged=0 target=198` 同在一行，只按行首匹配会只认出第一个。
  num() { printf '%s\n' "$out" | tr ' ' '\n' | sed -n "s/^$1=\([0-9][0-9]*\).*/\1/p" | head -1; }
  j=$(num judged); t=$(num target); rem=$(num remaining); pa=$(num pool_available)
  [ -n "$j" ]   && printf 'eval_judged_cases %s\n' "$j"
  [ -n "$t" ]   && printf 'eval_judged_target %s\n' "$t"
  [ -n "$rem" ] && printf 'eval_judged_remaining %s\n' "$rem"
  [ -n "$pa" ]  && printf 'eval_pool_available %s\n' "$pa"
  # 每条腿一行：总数会说谎（78 行全是 gepa 时总数看起来在动，agentflow 一行没有）。
  # `legs=` 不是行首（`judged=0 … legs=gepa:0 agentflow:0`），所以取「它之后到行尾」。
  printf '%s\n' "$out" | sed -n 's/.*legs=//p' | tr ' ' '\n' | while IFS=: read -r leg n; do
    [ -n "$leg" ] && [ -n "$n" ] && printf 'eval_leg_rows{leg="%s"} %s\n' "$leg" "$n"
  done
  v=$(printf '%s\n' "$out" | sed -n 's/^verdict=\([a-z-]*\).*/\1/p' | head -1)
  [ -n "$v" ] && printf 'eval_verdict{state="%s"} 1\n' "$v"
  # `age_s=` 在两行里都有（最后一格涨了多久、最后一轮跑了多久），`num` 取第一个会拿错；
  # 所以把「最后一轮」那一行单独整行取出来再切。
  age=$(printf '%s\n' "$out" | sed -n 's/^last_progress=.* age_s=\([0-9][0-9]*\).*/\1/p' | head -1)
  [ -n "$age" ] && printf 'eval_last_progress_age_seconds %s\n' "$age"
  lage=$(printf '%s\n' "$out" | sed -n 's/^last_run=.* age_s=\([0-9][0-9]*\).*/\1/p' | head -1)
  [ -n "$lage" ] && printf 'eval_last_run_age_seconds %s\n' "$lage"
  flat=$(printf '%s\n' "$out" | sed -n 's/^runs=[0-9]* runs_grew=[0-9]* runs_flat=\([0-9][0-9]*\).*/\1/p' | head -1)
  [ -n "$flat" ] && printf 'eval_runs_flat %s\n' "$flat"
  # 结局分档：一个标签把三种「没动」拆开，消费者才能在 134 的噪声里看见那几次真正的停住。
  printf '%s\n' "$out" | sed -n 's/^runs_by_outcome=//p' | tr ' ' '\n' | while IFS=: read -r oc n; do
    [ -n "$oc" ] && [ -n "$n" ] && printf 'eval_runs_by_outcome{outcome="%s"} %s\n' "$oc" "$n"
  done
  # 最后一轮为什么结束：`-` 是「没有轮次」，不印样本——那不是一种失败。
  lf=$(printf '%s\n' "$out" | sed -n 's/^last_run=.* failure=\([a-z-]*\).*/\1/p' | head -1)
  [ -n "$lf" ] && [ "$lf" != '-' ] && printf 'eval_last_run_failure{reason="%s"} 1\n' "$lf"
  # 「这次跑成不成表」与「题为什么没判」是两件事，各一格：告警读这一格才知道该去等配额
  # 还是去修执行面（见 failure_cause 那段）。
  lc=$(printf '%s\n' "$out" | sed -n 's/^last_run=.* cause=\([a-z-]*\).*/\1/p' | head -1)
  [ -n "$lc" ] && [ "$lc" != '-' ] && printf 'eval_last_run_cause{reason="%s"} 1\n' "$lc"
  lrt=$(printf '%s\n' "$out" | sed -n 's/^last_run=.* retry_after_s=\([0-9][0-9]*\).*/\1/p' | head -1)
  [ -n "$lrt" ] && printf 'eval_last_run_retry_after_seconds %s\n' "$lrt"
  # 每个基准的得分。`coverage="judged-so-far"` 是这一格的诚实边界：分母是已判数，
  # 不是这个基准的题数——没有那个数之前，它是「到目前为止判过的那部分里答对的比例」。
  # 同时印 judged/resolved 两个计数，读者据此能自己判断这个比例有多少行撑着。
  # 归不到任何基准上的用例单独一条：它不是某个基准的 0 分，是这一面看不见的那些行。
  # 在场证明先印：账本把下面四条标成 `=conditional:eval_bench_cases`，这一条缺席
  # 才算「这一段没跑」。
  bc=$(printf '%s\n' "$out" | sed -n 's/^bench_cases=\([0-9]*\)$/\1/p' | head -1)
  [ -n "$bc" ] && printf 'eval_bench_cases %s\n' "$bc"
  bu=$(printf '%s\n' "$out" | sed -n 's/^bench_unattributed=\([0-9]*\)$/\1/p' | head -1)
  [ -n "$bu" ] && printf 'eval_bench_unattributed %s\n' "$bu"
  printf '%s\n' "$out" | sed -n 's/^bench=//p' | while read -r b rest; do
    [ -n "$b" ] || continue
    bj=$(printf '%s' "$rest" | sed -n 's/^judged=\([0-9]*\).*/\1/p')
    br=$(printf '%s' "$rest" | sed -n 's/.*resolved=\([0-9]*\).*/\1/p')
    bs=$(printf '%s' "$rest" | sed -n 's/.*score=\([0-9.]*\).*/\1/p')
    [ -n "$bj" ] && printf 'eval_bench_judged{bench="%s"} %s\n' "$b" "$bj"
    [ -n "$br" ] && printf 'eval_bench_resolved{bench="%s"} %s\n' "$b" "$br"
    [ -n "$bs" ] && printf 'eval_bench_score{bench="%s",coverage="judged-so-far"} %s\n' "$b" "$bs"
  done
  exit "$rc"
fi
# 池样本多旧算「采样停了」（秒），默认 3 拍（采样循环 600 秒一拍）。采样循环自己会到期，
# 也会被杀掉；它一停，pool.log 最后一行永远说「关着」，而「池关着」与「采样停了」在读数上
# 同形——同形正是这条读数存在的理由（见文件头），所以池那一格要带上样本的年龄。
POOL_STALE_S=${POOL_STALE_S:-1800}


iso() { date -u -d "@$1" +%FT%TZ 2>/dev/null || echo '-'; }
now=$(date +%s)

judged() { # 首行是 run 头，不是用例
  [ -s "$CKPT" ] || { echo ''; return; }
  local n
  n=$(grep -c . "$CKPT")
  if head -1 "$CKPT" | grep -q '"kind":"header"'; then echo $((n - 1)); else echo "$n"; fi
}

j=$(judged)
[ -n "$j" ] || { echo "verdict=unreadable reason=checkpoint-missing-or-empty path=$CKPT"; exit 3; }

# 每条腿已判多少行：检查点每条已判用例带 `"scaffold":"<名字>"` 与 `"benchmark":"<名字>"`。
# 两个坐标都要对上：这一臂同时跑三个基准，只按 scaffold 数时 swe-bench-pro 的同名外壳
# 也算进 hle 那条腿，而 198 的算术只覆盖 hle（33 题 × 3 seed）。
leg_lines() {
  [ -s "$CKPT" ] || { echo 0; return; }
  grep -F "\"scaffold\":\"$1\"" "$CKPT" 2>/dev/null \
    | grep -c -F "\"benchmark\":\"${LEG_BENCH:-hle}\"" || true
}
leg_target() {
  local n
  n=$(printf '%s' "$LEGS" | tr ',' '\n' | grep -c .)
  [ "$n" -gt 0 ] || n=1
  echo $(( (TARGET + n - 1) / n ))
}
legs_report() {
  local leg out=""
  for leg in ${LEGS//,/ }; do out="$out $leg:$(leg_lines "$leg")"; done
  printf '%s' "${out# }"
}
legs_complete() {
  local t leg
  t=$(leg_target)
  for leg in ${LEGS//,/ }; do
    [ "$(leg_lines "$leg")" -ge "$t" ] || return 1
  done
  return 0
}

# 池读数取最后一行；文件不在就是 unreadable（不是 0——「没读到」与「关着」是两件事）。
read_pool() {
  pa='unreadable'; na='-'; er='-'; sa='-'; sf='-'; pf='-'
  [ -s "$POOL" ] || return 0
  local pl n r t ts
  pl=$(grep -E 'llm_pool_available=' "$POOL" | tail -1)
  pa=$(printf '%s\n' "$pl" | sed -n 's/.*llm_pool_available=\([0-9]*\).*/\1/p')
  # 这一臂要的那一面还接不接。池级 `available` 是整个池的容量：池里同时住着两种协议面
  # 时，一面全灭、另一面健康，它照样读 1，而这一臂的每一次调用都在熔断处回 503
  # （2026-10-24 实测）。行里没有这一格（旧格式）就印 `-`，不冒充 1 也不冒充 0。
  pf=$(printf '%s\n' "$pl" | sed -n "s/.*face_serving\[${POOL_FACE:-openai}\]=\([0-9]*\).*/\1/p")
  [ -n "${pf:-}" ] || pf='-'
  n=$(printf '%s\n' "$pl" | sed -n 's/.*llm_pool_next_attempt_unix=\([0-9]*\).*/\1/p')
  r=$(printf '%s\n' "$pl" | sed -n 's/.*llm_pool_evidenced_recovery_unix=\([0-9]*\).*/\1/p')
  [ -n "${n:-}" ] && [ "$n" != 0 ] && na=$(iso "$n")
  [ -n "${r:-}" ] && [ "$r" != 0 ] && er=$(iso "$r")
  [ -n "$pa" ] || pa='unreadable'
  # 样本自身的时刻是这一行的第一个字段；读不出来就印 `-`，不冒充新鲜也不冒充陈旧。
  t=$(printf '%s\n' "$pl" | awk '{print $1}')
  case "${t:-}" in
    [0-9][0-9][0-9][0-9]-*T*Z)
      ts=$(date -u -d "$t" +%s 2>/dev/null) || ts=''
      if [ -n "$ts" ]; then
        sa=$(( now - ts ))
        if [ "$sa" -le "$POOL_STALE_S" ]; then sf=1; else sf=0; fi
      fi ;;
  esac
}
read_pool

# 到目标这件事只看检查点就够：日志缺了也不许把「判完了」读成读不到。
# 但「总数够」不等于「每条腿都够」——总数可以被目标覆盖之外的行顶满（见上面 LEGS）。
if [ "$j" -ge "$TARGET" ]; then
  if legs_complete; then
    printf 'judged=%s target=%s remaining=0 legs=%s\n' "$j" "$TARGET" "$(legs_report)"
    printf 'pool_available=%s pool_face_serving=%s next_attempt=%s evidenced_recovery=%s sample_age_s=%s sample_fresh=%s\n' "$pa" "$pf" "$na" "$er" "$sa" "$sf"
    bench_scores
    echo "verdict=complete"; exit 0
  fi
  printf 'judged=%s target=%s remaining=0 legs=%s short-legs\n' "$j" "$TARGET" "$(legs_report)"
  printf 'pool_available=%s pool_face_serving=%s next_attempt=%s evidenced_recovery=%s sample_age_s=%s sample_fresh=%s\n' "$pa" "$pf" "$na" "$er" "$sa" "$sf"
  echo "verdict=incomplete-legs reason=total-is-past-${TARGET}-but-a-named-leg-has-fewer-than-$(leg_target)"
  exit 1
fi

[ -s "$LOG" ] || { echo "judged=$j target=$TARGET pool_available=$pa verdict=unreadable reason=log-missing-or-empty path=$LOG"; exit 3; }

# 从值守日志的 `judged=A->B` 聚合出「涨了多少 / 涨过几次」。$1 为 ISO 下界（空＝全文件）。
gains() {
  local since=${1:-} line ts pair a b
  while IFS= read -r line; do
    case "$line" in *'judged='*) ;; *) continue ;; esac
    ts=${line%% *}
    if [ -n "$since" ] && ! [[ "$ts" > "$since" ]]; then continue; fi
    pair=${line##*'judged='}
    pair=${pair%%[!0-9>-]*}
    case "$pair" in *'->'*) ;; *) continue ;; esac
    a=${pair%%->*}; b=${pair#*->}
    case "$a$b" in *[!0-9]*) continue ;; esac
    [ "$b" -gt "$a" ] && printf '%s %s\n' "$((b - a))" "$ts"
  done <"$LOG"
}

# token 面：只读总量会漏掉「少数几行吃掉大半」这种形状。2026-10-19 实测——七道带图题
# （题面带 data URL，模型读不了 base64，却每一步重发一次）占 78 行总花费的 91.5%，而总量
# 那一列看起来只是「一题很贵」。判据取前三行占比：> 50% 时这几行就是先要解释的那几行。
token_shape() {
  local vals total top3
  vals=$(grep -o '"tokens":[0-9]*' "$CKPT" 2>/dev/null | cut -d: -f2)
  [ -n "$vals" ] || { echo 'tokens_total=- tokens_top3_share=-'; return; }
  total=$(printf '%s\n' "$vals" | awk '{s+=$1} END{printf "%d", s}')
  [ "${total:-0}" -gt 0 ] || { echo "tokens_total=${total:-0} tokens_top3_share=-"; return; }
  top3=$(printf '%s\n' "$vals" | sort -rn | head -3 | awk '{s+=$1} END{print s+0}')
  printf 'tokens_total=%s tokens_top3_share=%s%%\n' "$total" "$((100 * top3 / total))"
}

# 每个基准的得分：`resolved / judged`，一行一个基准。
#
# 分母是**已判数**，不是这个基准的题数——检查点里没有「计划总数」这一列，拿已判数
# 当分母又不说明，读出来就是「跑了一半的分数被当成整个基准的分数」。所以这一行
# 同时印 judged 与 resolved 两个数，暴露格式那边分值带 `coverage="judged-so-far"`；
# 想让这个数变成「基准得分」，得先有分母，那件事今天还不存在。
#
# 为什么要按基准拆：`judged` 是一条总数，78 行全落在 hle 与三行各落一个基准在总数上
# 同形，而前者能说「hle 上答对了 31/78」、后者什么都说不了。
bench_scores() {
  [ -s "$CKPT" ] || return 0
  python3 -I - "$CKPT" <<'PY'
import json, sys
judged, resolved = {}, {}
unattributed = 0
for line in open(sys.argv[1], encoding="utf-8"):
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        continue
    if d.get("kind") != "case":
        continue
    b = d.get("benchmark")
    if not b:
        # 没有基准名的用例归不到任何一条基准上。造一个 `bench="-"` 只会多出一条
        # 谁也不认识的序列，所以单独计数：漏了多少行要看得见，不能让它们凭空消失。
        unattributed += 1
        continue
    judged[b] = judged.get(b, 0) + 1
    if (d.get("verdict") or {}).get("resolved"):
        resolved[b] = resolved.get(b, 0) + 1
# `bench_cases` 是这一族的**在场证明**：上面那几条按基准的序列只在判过用例时才存在，
# 而检查点刚建好、一条 case 行都还没落时它们一条也不印——那一拍与「这段代码被改坏了、
# 一条也不印」在聚合器眼里同形。所以先印一个总数：0 是量出来的 0，不是没量。
print("bench_cases=%d" % sum(judged.values()))
for b in sorted(judged):
    n, r = judged[b], resolved.get(b, 0)
    print("bench=%s judged=%d resolved=%d score=%.4f" % (b, n, r, r / n))
# 这一条**永远印**，包括 0。
#
# 它原来只在非零时印，而「一条都归不到基准」恰恰是不该发生的那个状态——于是它只在
# 坏的时候出现，在好的时候缺席。对着它的告警因此有两种沉默：真的零条，与这一节压根
# 没跑。人在面板上看不出区别，聚合器也看不出：两种都是「没有这条序列」。印 0 之后
# 缺席只剩一个意思，就是这段读数没生成。
print("bench_unattributed=%d" % unattributed)
PY
}

# 最后一轮为什么结束：只数 `judged` 会说谎——2026-10-18T22:31Z~10-20T06:24Z 那 147 轮里
# 每一轮都退非 0，而理由不是池子：注册的网格点着 toolathlon，评测 Pod 里没有能建 Pod 的
# `kubectl`，于是 `missing` 非空、每轮都判成「不完整的实验」，`judged` 一行没涨。池子在
# 10-18T23:02Z 开过一拍，之后那轮仍是 0 行 ⇒「池关着」这个判词把读者引向了错的门。
# 所以把最后一轮的结局与成因单独读出来，成因是闭集：
#   missing-leg     有一个点名的基准接不上（missing 里带它的接线理由）⇒ 池开着也落不了行
#   diagnostic-arm  只剩 tools-off 那一条（诊断臂的如实记录，不是接不上的依赖）
#   checkpoint-locked 另一个驱动占着检查点
#   other / -       别的理由，或日志里没有轮次
last_run() {
    lr_phase='-'; lr_ts='-'; lr_age='-'; lr_class='-'; lr_cause='-'; lr_retry='-'
    [ -s "$LOG" ] || return 0
    local line
    line=$(grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[^ ]* run .*phase=' "$LOG" 2>/dev/null | tail -1)
    [ -n "$line" ] || return 0
    lr_ts=${line%% *}
    lr_phase=$(printf '%s\n' "$line" | sed -n 's/.*phase=\([A-Za-z]*\).*/\1/p' | head -1)
    case "$line" in
        *'no pod runtime'*|*'swe-bench-pro:'*) lr_class=missing-leg ;;
        *'tools-off diagnostic arm'*) lr_class=diagnostic-arm ;;
        # 起得来、但半句判词都没印就等到停滞判据杀它（2026-10-19T22:01Z 起连续三轮：
        # 1815 秒、`judged=0->0 scaffolds=`、`last=` 只有版本横幅）。它与「池关着」、
        # 「缺一列」都不同形：进程在了，什么都没做。
        *phase=stalled*) lr_class=stalled-no-output ;;
        *'another driver holds the checkpoint'*) lr_class=checkpoint-locked ;;
        *) lr_class=other ;;
    esac
    local s
    s=$(date -u -d "$lr_ts" +%s 2>/dev/null) && [ -n "$s" ] && lr_age=$(( now - s )) || lr_age='-'
    failure_cause
}

# 每一格为什么没判出分：报表 `## failures` 那一段自己的话。它与上面那个 `failure=` 是
# 两个问题——那一个答「这次跑为什么不是一次完整的实验」，这一个答「题为什么没判」。
# 2026-11-04 实测两者不同形：日志留的是 `failure=diagnostic-arm`（退出码的成因），报表里
# 每一格都是上游 `429`、要求等 498106 秒。照日志那一句去查，读者会去改命令行；真正的
# 门是上游配额。成因仍是闭集，过半才算数（少数几格失败不是这一轮的结论）：
#   upstream-refused 过半的失败是上游拒绝（429/rate_limited）⇒ 等配额，改代码没用
#   env-unavailable  过半的失败是起不来环境（容器/Pod 面）⇒ 修执行面
#   case-errors      别的失败 ⇒ 逐格看报表
#   -                报表不在，或这一轮一格失败都没有
failure_cause() {
    [ -s "$REPORT" ] || return 0
    local sec all r429 env
    sec=$(sed -n '/^## failures$/,/^## /p' "$REPORT" 2>/dev/null | grep '^failure')
    [ -n "$sec" ] || return 0
    all=$(printf '%s\n' "$sec" | grep -c .)
    r429=$(printf '%s\n' "$sec" | grep -cE 'rate_limited|HTTP 429|blocked_upstreams')
    env=$(printf '%s\n' "$sec" | grep -c 'cannot acquire env')
    if [ $((r429 * 2)) -ge "$all" ]; then
        lr_cause=upstream-refused
        # 上游自己报的等待：报表里那句「asked for another Ns」。取最大的那个——同一轮里
        # 每一格可能是不同的上游在拒，等哪个都等不到。
        lr_retry=$(printf '%s\n' "$sec" | sed -n 's/.*asked for another \([0-9][0-9]*\)s.*/\1/p' \
                   | sort -n | tail -1)
        [ -n "$lr_retry" ] || lr_retry='-'
    elif [ $((env * 2)) -ge "$all" ]; then
        lr_cause='env-unavailable'
    else
        lr_cause=case-errors
    fi
}

# 跑过却没动的轮次：`judged=A->B` 里 B==A 的那些。只数 B>A 会把「跑了但没进步」并进
# 「没跑」——而这正是 147 轮的样子：每一轮都跑了，每一轮都没动。
run_counts() {
    local grew=0 flat=0 pair a b
    while IFS= read -r pair; do
        a=${pair%%->*}; b=${pair##*->}
        case "$a$b" in ''|*[!0-9]*) continue ;; esac
        if [ "$b" -gt "$a" ]; then grew=$((grew + 1)); else flat=$((flat + 1)); fi
    # 只数 `run` 行：NOTE／start 行里也有 `judged=A->B`（例：手工杀轮次时记下的 73->78），
    # 把它们并进轮次计数会让「几轮」与下面的结局分档对不上，两个数贴在同一张面上打架。
    done < <(grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[^ ]* run .*judged=[0-9]+->[0-9]+' "$LOG" 2>/dev/null \
             | grep -o 'judged=[0-9]*->[0-9]*' | sed 's/^judged=//')
    printf '%s %s' "$grew" "$flat"
}

# 轮次按**结局**分开数：`phase=Failed` 那一格把「容器起来了、什么都没做」「实验面根本没
# 启动（缺一列）」「池关着秒回 503」加在一起，而三者的修法完全不同。147 轮里 134 轮属于
# 「压根没跑成一次实验」，它们与真正停住的那几轮在 `runs_flat` 里同权——一个合并总体。
# 所以这里按每一行自己的证据分档，判词是算出来的，不是人数出来的。
run_outcomes() {
    [ -s "$LOG" ] || return 0
    grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[^ ]* run .*phase=' "$LOG" 2>/dev/null | awk '
      {
        grew = 0
        if (match($0, /judged=[0-9]+->[0-9]+/)) {
          p = substr($0, RSTART + 7, RLENGTH - 7); split(p, ab, "->")
          if (ab[2] + 0 > ab[1] + 0) grew = 1
        }
        if (grew) o = "grew"
        else if ($0 ~ /phase=stalled/) o = "flat-stalled"
        else if ($0 ~ /not a complete experiment|no pod runtime|toolathlon:|swe-bench-pro:/) o = "flat-incomplete"
        else if (match($0, /dur=[0-9]+s/)) {
          d = substr($0, RSTART + 4, RLENGTH - 5) + 0
          o = (d <= 300) ? "flat-fast" : "flat-other"
        }
        else o = "flat-other"
        n[o]++
      }
      END { for (k in n) printf "%s:%d ", k, n[k] }'
}

all=$(gains '')
gained=$(printf '%s\n' "$all" | awk 'NF{n+=$1} END{print n+0}')
events=$(printf '%s\n' "$all" | grep -c . )
last_progress=$(printf '%s\n' "$all" | awk 'NF{t=$2} END{if(t) print t}')

recent=$(gains "$(date -u -d "@$((now - WINDOW))" +%FT%TZ)")
gained_w=$(printf '%s\n' "$recent" | awk 'NF{n+=$1} END{print n+0}')
events_w=$(printf '%s\n' "$recent" | grep -c . )
# 同一个聚合换一个下界：停住判据看两轮，到达率看一小时（见 STALL_WINDOW）。
gained_stall=$(gains "$(date -u -d "@$((now - STALL_WINDOW))" +%FT%TZ)" | awk 'NF{n+=$1} END{print n+0}')

if [ -n "$last_progress" ]; then
  lp_s=$(date -u -d "$last_progress" +%s 2>/dev/null || echo '')
  [ -n "$lp_s" ] && age=$((now - lp_s)) || age='-'
else
  age='-'
fi

# 池读数已在上面取过（到目标那条分支也要印它，所以只取一次）。
last_run
read -r grew_runs flat_runs < <(run_counts)

printf 'judged=%s target=%s remaining=%s legs=%s\n' "$j" "$TARGET" "$((TARGET > j ? TARGET - j : 0))" "$(legs_report)"
printf 'last_progress=%s age_s=%s\n' "${last_progress:--}" "$age"
printf 'last_run=%s@%s age_s=%s failure=%s cause=%s retry_after_s=%s\n' \
  "$lr_phase" "$lr_ts" "$lr_age" "$lr_class" "$lr_cause" "$lr_retry"
# 「跑过」与「跑动过」分开：flat 是跑了但 A==B 的轮次，只数涨的会把这两种并成一个零。
printf 'runs=%s runs_grew=%s runs_flat=%s\n' "$((grew_runs + flat_runs))" "$grew_runs" "$flat_runs"
# 同一个 `runs_flat` 按结局拆开：动不了是三种世界（实验没启动／进程停在横幅／池关着），
# 合并成一行读起来像一件反复发生的事，实际是三种不同的修法。
printf 'runs_by_outcome=%s\n' "$(run_outcomes)"
printf 'arrival_%ss=%s events_%ss=%s since_start_rows=%s rounds=%s\n' \
  "$WINDOW" "$gained_w" "$WINDOW" "$events_w" "$gained" "$events"
printf 'pool_available=%s pool_face_serving=%s next_attempt=%s evidenced_recovery=%s sample_age_s=%s sample_fresh=%s\n' "$pa" "$pf" "$na" "$er" "$sa" "$sf"
printf '%s\n' "$(token_shape)"
bench_scores

if [ "$gained_w" -gt 0 ]; then
  echo "verdict=progressing"; exit 0
fi
# 最后一轮因为「有一个点名的基准接不上」而退非 0：这道门排在池子前面，池开不开它都开着，
# 所以它先于池子回答。它没有上游自报的复位时刻（与池子不同），如实写缺判据，
# 不许拿池子的 recovery 冒充。
# 只认**最近**那一轮：一次很久以前死在缺腿上的尝试不能替今天的配置说话（配置可能已经改过，
# 而下一轮要等池子开才发生）。超过这个窗口就只把 `failure=` 与它的年龄印出来，不改判。
recent_missing_leg=0
if [ "$lr_class" = missing-leg ]; then
  case "$lr_age" in
    ''|*[!0-9]*) ;;
    *) [ "$lr_age" -le "${LAST_RUN_TRUST_S:-21600}" ] && recent_missing_leg=1 ;;
  esac
fi
if [ "$recent_missing_leg" = 1 ]; then
  echo "verdict=blocked-on-incomplete-experiment reason=the-last-attempt-exited-nonzero-because-a-named-benchmark-could-not-be-wired-so-missing-is-not-empty-and-no-row-can-land-even-with-the-pool-open evidenced_recovery=- note=no-upstream-reports-when-this-is-fixed"
  exit 1
fi
# 池子读数说开着，而每一格都被上游拒了：`llm_pool_available=1` 只说明**探测**打通过了
# 一次，真实调用照样可以回 429 + 一个几天长的等待窗口。这时「池关着」是错的门，读者要
# 知道的是「等配额，不是改代码」，以及上游自己报的等待有多长。
if [ "$lr_cause" = upstream-refused ]; then
  case "$lr_age" in
    ''|*[!0-9]*) ;;
    *) if [ "$lr_age" -le "${LAST_RUN_TRUST_S:-21600}" ]; then
         echo "verdict=blocked-on-upstream-quota reason=no-rows-landed-in-${WINDOW}s-and-every-failing-case-in-the-last-run-was-refused-by-the-upstream retry_after_s=${lr_retry:--} note=the-pool-reading-says-open-because-a-probe-passed-while-real-calls-were-429"
         exit 1
       fi ;;
  esac
fi
if [ "$pa" = 0 ]; then
  # 样本老了就不再说「池子关着」：那一句只在有新鲜的「0」时才成立，否则它同时是
  # 「采样停了」的样子。两种成因给两个判词，等的人才知道该去起采样还是接着等。
  if [ "$sf" = 0 ]; then
    echo "verdict=blocked-on-stale-pool-sample reason=no-rows-landed-in-${WINDOW}s-and-the-last-sample-is-${sa}s-old-older-than-${POOL_STALE_S}s-so-a-closed-read-cannot-be-told-from-a-stopped-sampler"
    exit 1
  fi
  echo "verdict=blocked-on-pool reason=no-rows-landed-in-${WINDOW}s-and-the-gateway-reports-the-pool-closed"
  exit 1
fi
# 走到这里：一小时内没落行，池开着，最后一轮也没有可归的成因。这一格曾经直接印
# `stalled-unknown`，而它同时装着两件事——「两轮跑完了都没落行」与「这一轮还在跑」。
# 分开印：两轮之内落过行的是 working（轮次在转，只是比到达窗口慢），两轮都空才是停住。
if [ "$gained_stall" -gt 0 ]; then
  echo "verdict=working reason=no-row-landed-in-${WINDOW}s-but-${gained_stall}-landed-within-${STALL_WINDOW}s-so-the-rounds-are-turning-just-slower-than-the-arrival-window"
  exit 1
fi
echo "verdict=stalled-unknown reason=no-rows-landed-in-${STALL_WINDOW}s-and-pool_available=${pa}"
exit 1

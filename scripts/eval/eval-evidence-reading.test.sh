#!/usr/bin/env bash
# principle: feedback_a_blocked_verdict_must_name_its_cause_and_when_to_come_back
# principle: feedback_a_count_that_adds_not_started_with_started_and_stopped_must_be_split_by_outcome
# principle: feedback_a_backlog_reading_needs_its_inflow_or_the_wait_has_no_terminator
#
# `eval-evidence-reading.sh` 的离线测试：只用夹具，不连集群、不碰真检查点。
# 判的是四件事：涨了是 0、没涨且有池读数分开两个成因、检查点缺失是 3 而不是 0。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-evidence-reading.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# 两条腿交替写：这一臂登记了 4 个 scaffold，而目标是 gepa 与 agentflow 各一半，
# 只写 gepa 的夹具会让「总数够了」与「每条腿都够了」永远同时成立，那条判据就测不到。
ckpt() { # ckpt <路径> <用例数> [只写这一条腿]
  printf '{"kind":"header","run_id":"deadbeef"}\n' >"$1"
  local i=0 legs=(gepa agentflow)
  [ -n "${3:-}" ] && legs=("$3")
  while [ "$i" -lt "$2" ]; do
    printf '{"kind":"case","benchmark":"hle","scaffold":"%s"}\n' "${legs[$((i % ${#legs[@]}))]}" >>"$1"
    i=$((i + 1))
  done
}

# 时间戳：涨的那一行落在窗口内／窗口外，靠相对 now 生成，不写死日期。
t_in()  { date -u -d '-5 minutes' +%FT%TZ; }
t_mid() { date -u -d '-90 minutes' +%FT%TZ; }
t_out() { date -u -d '-5 hours'  +%FT%TZ; }

run() { # run <ckpt> <log> <pool> [env…] ；打印 "rc=<n> verdict=<词>"
  local c=$1 l=$2 p=$3 out rc
  shift 3
  out=$(CKPT="$c" LOG="$l" POOL="$p" TARGET=240 WINDOW=3600 \
        env "$@" bash "$SCRIPT" 2>&1); rc=$?
  printf 'rc=%s verdict=%s\n' "$rc" "$(printf '%s\n' "$out" | sed -n 's/^verdict=\([a-z-]*\).*/\1/p')"
}

# 1. 已到目标：240 例判完 → 0/complete
ckpt "$TMP/ck_ok" 240
: >"$TMP/log_ok"
: >"$TMP/pool_ok"
got=$(run "$TMP/ck_ok" "$TMP/log_ok" "$TMP/pool_ok")
[ "$got" = "rc=0 verdict=complete" ] && ok "判满目标是 complete/0" || bad "判满目标：期望 rc=0 verdict=complete，实得 $got"

# 2. 窗口内有增量 → 0/progressing（哪怕池读数说关着：涨了就是涨了）
ckpt "$TMP/ck_up" 90
{ printf '%s run eval-rerun-1 phase=Failed dur=61s judged=88->88\n' "$(t_out)"
  printf '%s run eval-rerun-2 phase=Failed dur=61s judged=88->90\n' "$(t_in)"
} >"$TMP/log_up"
printf '%s judged=90 llm_pool_available=0 llm_pool_next_attempt_unix=0 llm_pool_evidenced_recovery_unix=0\n' \
  "$(t_in)" >"$TMP/pool_up"
got=$(run "$TMP/ck_up" "$TMP/log_up" "$TMP/pool_up")
[ "$got" = "rc=0 verdict=progressing" ] && ok "窗口内涨了是 progressing/0（池读数不压过实涨）" \
  || bad "窗口内涨了：期望 rc=0 verdict=progressing，实得 $got"

# 3. 窗口内零增量 + 池关着 → 1/blocked-on-pool（成因说得出来）
ckpt "$TMP/ck_blk" 78
printf '%s run eval-rerun-1 phase=Failed dur=61s judged=78->78\n' "$(t_in)" >"$TMP/log_blk"
printf '%s judged=78 llm_pool_available=0 llm_pool_next_attempt_unix=1792407897 llm_pool_evidenced_recovery_unix=1793807999\n' \
  "$(t_in)" >"$TMP/pool_blk"
got=$(run "$TMP/ck_blk" "$TMP/log_blk" "$TMP/pool_blk")
[ "$got" = "rc=1 verdict=blocked-on-pool" ] && ok "零增量＋池关＝blocked-on-pool/1" \
  || bad "零增量＋池关：期望 rc=1 verdict=blocked-on-pool，实得 $got"

# 4. 窗口内零增量 + 池读数是 1 → 1/stalled-unknown（这就是「窗口开着却没吃上」）
ckpt "$TMP/ck_unk" 78
cp "$TMP/log_blk" "$TMP/log_unk"
printf '%s judged=78 llm_pool_available=1 llm_pool_next_attempt_unix=0 llm_pool_evidenced_recovery_unix=0\n' \
  "$(t_in)" >"$TMP/pool_unk"
got=$(run "$TMP/ck_unk" "$TMP/log_unk" "$TMP/pool_unk")
[ "$got" = "rc=1 verdict=stalled-unknown" ] && ok "零增量＋池开着＝stalled-unknown/1" \
  || bad "零增量＋池开：期望 rc=1 verdict=stalled-unknown，实得 $got"

# 4b. 一小时内没落行，但两轮之内落过：轮次在转，只是比到达窗口慢 ⇒ working，不是停住。
# 这一格是判据的地方：`stalled-unknown` 同时装着「两轮跑完都没落行」与「这一轮还在跑」，
# 而后者一秒都不该报停住——一个跑着的 Pod 被读成停住，面读数会照它把「商用交付」判红。
ckpt "$TMP/ck_mid" 78
printf '%s run eval-rerun-1 phase=timeout dur=2704s judged=74->78\n' "$(t_mid)" >"$TMP/log_mid"
got=$(run "$TMP/ck_mid" "$TMP/log_mid" "$TMP/pool_unk")
[ "$got" = "rc=1 verdict=working" ] && ok "涨落在一小时外、两轮内＝working/1" \
  || bad "两轮内落过行：期望 rc=1 verdict=working，实得 $got"

# 4c. 消融：把停住窗口也压到一小时，同一份夹具必须回落到 stalled-unknown——
# 证明上面那一格由 STALL_WINDOW 撑着，不是碰巧。
got=$(run "$TMP/ck_mid" "$TMP/log_mid" "$TMP/pool_unk" STALL_WINDOW=3600)
[ "$got" = "rc=1 verdict=stalled-unknown" ] && ok "停住窗口压回一小时即回落成 stalled-unknown（working 由窗口撑起）" \
  || bad "working 消融：期望 rc=1 verdict=stalled-unknown，实得 $got"

# 4d. 两轮之外也没落行 ⇒ 仍然是停住，窗口不是无限的：working 不许把真停住也盖上。
printf '%s run eval-rerun-1 phase=timeout dur=2704s judged=74->78\n' "$(t_out)" >"$TMP/log_old"
got=$(run "$TMP/ck_mid" "$TMP/log_old" "$TMP/pool_unk")
[ "$got" = "rc=1 verdict=stalled-unknown" ] && ok "五小时前那一次涨救不了现在：仍是 stalled-unknown/1" \
  || bad "两轮之外：期望 rc=1 verdict=stalled-unknown，实得 $got"

# 5. 检查点不在 → 3/unreadable，不许当 0
got=$(run "$TMP/nope" "$TMP/log_blk" "$TMP/pool_blk")
[ "$got" = "rc=3 verdict=unreadable" ] && ok "检查点缺失是 unreadable/3" \
  || bad "检查点缺失：期望 rc=3 verdict=unreadable，实得 $got"

# 6. 池读数文件不在 → 池那一格是 unreadable，且不许读成「池关着」
ckpt "$TMP/ck_np" 78
got=$(run "$TMP/ck_np" "$TMP/log_blk" "$TMP/nope_pool")
case "$got" in
  "rc=1 verdict=stalled-unknown") ok "池读数缺失退到 stalled-unknown，不冒充 blocked-on-pool" ;;
  *) bad "池读数缺失：期望 rc=1 verdict=stalled-unknown，实得 $got" ;;
esac

# 7. token 面：总量读出来，且「少数几行吃掉大半」这个形状要能被读出来。
#    夹具：四行，其中一行 900、其余各 100 ⇒ 前三行 1100/1200 ≈ 91%。
{ printf '{"kind":"header","run_id":"deadbeef"}\n'
  printf '{"kind":"case","tokens":900}\n'
  printf '{"kind":"case","tokens":100}\n'
  printf '{"kind":"case","tokens":100}\n'
  printf '{"kind":"case","tokens":100}\n'
} >"$TMP/ck_tok"
tok=$(CKPT="$TMP/ck_tok" LOG="$TMP/log_blk" POOL="$TMP/pool_blk" TARGET=240 \
      bash "$SCRIPT" 2>&1 | sed -n 's/^\(tokens_total=.*\)$/\1/p')
case "$tok" in
  "tokens_total=1200 tokens_top3_share=91%") ok "token 面：总量与前 N 的占比都读得出来" ;;
  *) bad "token 面：期望 tokens_total=1200 tokens_top3_share=91%，实得 ${tok:-（没有这一行）}" ;;
esac

# 8. 没有 tokens 列（老检查点）→ 印 `-`，不许印 0 冒充「没花过」
got=$(CKPT="$TMP/ck_np" LOG="$TMP/log_blk" POOL="$TMP/pool_blk" TARGET=240 \
      bash "$SCRIPT" 2>&1 | sed -n 's/^\(tokens_total=.*\)$/\1/p')
case "$got" in
  "tokens_total=- tokens_top3_share=-") ok "没有 tokens 列时印 -，不冒充 0" ;;
  *) bad "没有 tokens 列：期望两个 -，实得 ${got:-（没有这一行）}" ;;
esac

# 9. 不给 TARGET 时，默认目标按可判口径是 198（240 会在跳过 42 行之后永远到不了）
ckpt "$TMP/ck_def" 198
: >"$TMP/log_def"; : >"$TMP/pool_def"
out=$(CKPT="$TMP/ck_def" LOG="$TMP/log_def" POOL="$TMP/pool_def" WINDOW=3600 bash "$SCRIPT" 2>&1); rc=$?
verdict=$(printf '%s\n' "$out" | sed -n 's/^verdict=\([a-z-]*\).*/\1/p')
if [ "$rc" = 0 ] && [ "$verdict" = complete ]; then
  ok "默认目标是 198（可判口径），不是 240"
else
  bad "默认目标：198 例应当 complete/0，实得 rc=$rc verdict=${verdict:-无}"
fi

# 10. 总数过了目标、但只有一条腿在长：240 行全是 gepa ⇒ 不许读成 complete。
# 2026-10-19 实测：登记臂 3 基准 × 4 外壳，判过的 78 行全是 gepa，agentflow 一行没落过。
ckpt "$TMP/ck_one_leg" 240 gepa
: >"$TMP/log_one"; : >"$TMP/pool_one"
got=$(run "$TMP/ck_one_leg" "$TMP/log_one" "$TMP/pool_one")
[ "$got" = "rc=1 verdict=incomplete-legs" ] \
  && ok "总数够而一条腿为零 ⇒ incomplete-legs/1" \
  || bad "单腿：期望 rc=1 verdict=incomplete-legs，实得 $got"

# 11. 池样本很旧、而最后一行仍说「关着」：不许再读成 blocked-on-pool。
# 采样循环自己会到期、也会被杀掉；采样停了与池子关着在读数上同形（正是这条读数存在的
# 理由），所以两种成因给两个判词，等人的人才知道该去起采样还是接着等。
t_old() { date -u -d '-2 hours' +%FT%TZ; }
ckpt "$TMP/ck_stale" 78
printf '%s run eval-rerun-1 phase=Failed dur=61s judged=78->78\n' "$(t_in)" >"$TMP/log_stale"
printf '%s judged=78 llm_pool_available=0 llm_pool_next_attempt_unix=1792407897 llm_pool_evidenced_recovery_unix=1793807999\n' \
  "$(t_old)" >"$TMP/pool_stale"
got=$(run "$TMP/ck_stale" "$TMP/log_stale" "$TMP/pool_stale")
[ "$got" = "rc=1 verdict=blocked-on-stale-pool-sample" ] \
  && ok "样本超过 3 拍 ⇒ blocked-on-stale-pool-sample/1（不冒充池子关着）" \
  || bad "陈旧样本：期望 rc=1 verdict=blocked-on-stale-pool-sample，实得 $got"

# 12. 新鲜样本：池那一格带上年龄与新鲜标记（判词仍是 blocked-on-pool）
run_out() { local c=$1 l=$2 p=$3; shift 3; CKPT="$c" LOG="$l" POOL="$p" TARGET=240 WINDOW=3600 env "$@" bash "$SCRIPT" 2>&1 || true; }
line=$(run_out "$TMP/ck_blk" "$TMP/log_blk" "$TMP/pool_blk" | sed -n 's/^\(pool_available=.*\)$/\1/p')
case "$line" in
  *"sample_fresh=1"*) ok "新鲜样本印 sample_fresh=1" ;;
  *) bad "新鲜样本：期望 sample_fresh=1，实得 ${line:-（没有这一行）}" ;;
esac

# 13. 样本那一行没有可读的时刻 ⇒ 年龄印 -、新鲜标记印 -，判词退回 blocked-on-pool：
#     读不出年龄时不发明新鲜度，也不改判成「采样停了」。
printf '%s judged=78 llm_pool_available=0 llm_pool_next_attempt_unix=1792407897 llm_pool_evidenced_recovery_unix=0\n' \
  '' >"$TMP/pool_nots"
got=$(run "$TMP/ck_stale" "$TMP/log_stale" "$TMP/pool_nots")
line=$(run_out "$TMP/ck_stale" "$TMP/log_stale" "$TMP/pool_nots" | sed -n 's/^\(pool_available=.*\)$/\1/p')
if [ "$got" = "rc=1 verdict=blocked-on-pool" ] && [ "${line##* }" = "sample_fresh=-" ]; then
  ok "没有可读时刻 ⇒ 年龄与新鲜度都印 -，判词仍是 blocked-on-pool"
else
  bad "没有可读时刻：期望 rc=1 verdict=blocked-on-pool 且 sample_fresh=-，实得 $got / ${line:-无池行}"
fi

# 14. `--prometheus`：宿主读数聚合器按这条协议跑它（0 绿 / 1 红 / 3 读不到，退 3 时
#     一条序列都不印）。它必须印出与 key=value 同一份判词的数——面读数、面板、告警各读
#     一条序列，两边各说一套就是这一族读数最怕的形状。
ckpt "$TMP/ck_prom" 30
printf '%s run eval-rerun-9 phase=Failed dur=61s judged=30->30\n' "$(t_out)" >"$TMP/log_prom"
printf '%s judged=30 llm_pool_available=0 llm_pool_next_attempt_unix=0 llm_pool_evidenced_recovery_unix=0\n' \
  "$(t_out)" >"$TMP/pool_prom"
pout=$(CKPT="$TMP/ck_prom" LOG="$TMP/log_prom" POOL="$TMP/pool_prom" TARGET=240 WINDOW=3600 \
       bash "$SCRIPT" --prometheus 2>&1); prc=$?
if [ "$prc" = 1 ] \
   && grep -qx 'eval_judged_cases 30' < <(printf '%s\n' "$pout") \
   && grep -qx 'eval_judged_target 240' < <(printf '%s\n' "$pout") \
   && grep -qx 'eval_leg_rows{leg="gepa"} 15' < <(printf '%s\n' "$pout"); then
  ok "--prometheus 印的是同一份判词（总数/目标/每条腿），退出码随判词"
else
  bad "--prometheus：期望 rc=1 且三格与判词一致，实得 rc=$prc 输出=$(printf '%s' "$pout" | tr '\n' ';')"
fi

# 15. 读不到 ⇒ 一条序列都不印（印 0 会被读成「判了 0 个、还差 240」），rc=3。
pout=$(CKPT="$TMP/nope" LOG="$TMP/log_prom" POOL="$TMP/pool_prom" bash "$SCRIPT" --prometheus 2>&1); prc=$?
if [ "$prc" = 3 ] && [ -z "$pout" ]; then
  ok "--prometheus 读不到时一条序列都不印、rc=3"
else
  bad "--prometheus 读不到：期望 rc=3 且无输出，实得 rc=$prc 输出=${pout:-（空）}"
fi

# 16. 最后一轮死在「缺一列」上（点名的基准接不上），且是最近一轮 ⇒ 判词必须说这一道门，
#     而不是池子。2026-10-18~20 实测 147 轮里 134 轮是这句；池子在 10-18T23:02Z 开过一拍
#     之后那轮同样 0 行 ⇒「池关着」把读者引向了错的门。
t_8h() { date -u -d '-8 hours' +%FT%TZ; }
ckpt "$TMP/ck_miss" 78
printf '%s run eval-rerun-1 phase=Failed dur=82s judged=78->78 last=Error: "this run is not a complete experiment: toolathlon: this host has no pod runtime the backend can drive"\n' \
  "$(t_in)" >"$TMP/log_miss"
got=$(run "$TMP/ck_miss" "$TMP/log_miss" "$TMP/pool_blk")
[ "$got" = "rc=1 verdict=blocked-on-incomplete-experiment" ] \
  && ok "最近一轮死在缺一列 ⇒ blocked-on-incomplete-experiment/1（先于池子回答）" \
  || bad "缺一列：期望 rc=1 verdict=blocked-on-incomplete-experiment，实得 $got"

# 17. 同一句但已经是 8 小时前的尝试 ⇒ 不改判（配置可能已改过，下一轮要等池子开），
#     只把那一格与年龄印出来。拿旧尝试替今天说话与「拿旧读数当现状」是同一种错。
ckpt "$TMP/ck_miss_old" 78
printf '%s run eval-rerun-1 phase=Failed dur=82s judged=78->78 last=Error: "this run is not a complete experiment: toolathlon: this host has no pod runtime"\n' \
  "$(t_8h)" >"$TMP/log_miss_old"
got=$(run "$TMP/ck_miss_old" "$TMP/log_miss_old" "$TMP/pool_blk")
line=$(run_out "$TMP/ck_miss_old" "$TMP/log_miss_old" "$TMP/pool_blk" | sed -n 's/^\(last_run=.*\)$/\1/p')
# 这一格判的是「failure= 如实印出来」，不是「它是那一行的最后一格」——行尾后来还长出了
# 成因与上游自报的等待（见 27 号用例），钉成最后一格会让格式一长就红。
if [ "$got" = "rc=1 verdict=blocked-on-pool" ] && grep -q 'failure=missing-leg' < <(printf '%s' "$line"); then
  ok "旧尝试不替今天改判，但 failure= 那一格仍如实印出"
else
  bad "旧尝试：期望 rc=1 verdict=blocked-on-pool 且 failure=missing-leg，实得 $got / ${line:-无}"
fi

# 18. 「跑了但没动」与「没跑」分开：两轮 A==B 记成 runs_flat=2，不是并进「没跑」。
ckpt "$TMP/ck_flat" 78
{ printf '%s run eval-rerun-1 phase=Failed dur=61s judged=78->78\n' "$(t_out)"
  printf '%s run eval-rerun-2 phase=Failed dur=61s judged=78->78\n' "$(t_in)"
} >"$TMP/log_flat"
line=$(run_out "$TMP/ck_flat" "$TMP/log_flat" "$TMP/pool_blk" | sed -n 's/^\(runs=.*\)$/\1/p')
case "$line" in
  "runs=2 runs_grew=0 runs_flat=2") ok "A==B 的轮次单计为 runs_flat" ;;
  *) bad "轮次计数：期望 runs=2 runs_grew=0 runs_flat=2，实得 ${line:-（没有这一行）}" ;;
esac

# 19. 同一份判词在暴露格式里也要有：`eval_runs_flat` 与带成因标签的
#     `eval_last_run_failure`——面板要能直接画「跑了没动」的轮次与最后死因。
pout=$(CKPT="$TMP/ck_miss" LOG="$TMP/log_miss" POOL="$TMP/pool_blk" TARGET=240 WINDOW=3600 \
       bash "$SCRIPT" --prometheus 2>&1); prc=$?
if [ "$prc" = 1 ] \
   && grep -qx 'eval_runs_flat 1' < <(printf '%s\n' "$pout") \
   && grep -qx 'eval_last_run_failure{reason="missing-leg"} 1' < <(printf '%s\n' "$pout") \
   && grep -qx 'eval_verdict{state="blocked-on-incomplete-experiment"} 1' < <(printf '%s\n' "$pout"); then
  ok "--prometheus 也带 flat 轮次与最后死因"
else
  bad "--prometheus 新格：实得 rc=$prc 输出=$(printf '%s' "$pout" | grep -E 'runs_flat|last_run_failure|eval_verdict' | tr '\n' ';')"
fi

# 20. 结局分档：`phase=Failed` 那一格把三种「没动」合成一个数（134 次实验没启动、12 次
#     停在横幅、13 次池关着秒败），修法完全不同。每一行按自己的证据分档，且总数与
#     `runs=` 对得上——两个数贴在同一张面上，不许各算各的。
ckpt "$TMP/ck_out" 78
{ printf '%s run eval-rerun-1 phase=Succeeded dur=61s judged=78->79\n' "$(t_out)"
  printf '%s run eval-rerun-2 phase=stalled dur=1815s judged=79->79 last=cogneva starting: version v0.5.9\n' "$(t_out)"
  printf '%s run eval-rerun-3 phase=Failed dur=82s judged=79->79 last=Error: "this run is not a complete experiment: toolathlon: this host has no pod runtime"\n' "$(t_out)"
  printf '%s run eval-rerun-4 phase=Failed dur=61s judged=79->79\n' "$(t_in)"
} >"$TMP/log_out"
out_lines=$(run_out "$TMP/ck_out" "$TMP/log_out" "$TMP/pool_blk")
line=$(printf '%s\n' "$out_lines" | sed -n 's/^\(runs=.*\)$/\1/p')
ob=$(printf '%s\n' "$out_lines" | sed -n 's/^runs_by_outcome=//p')
sum=$(printf '%s\n' "$ob" | tr ' ' '\n' | sed -n 's/^[a-z-]*:\([0-9][0-9]*\)$/\1/p' | awk '{n+=$1} END{print n+0}')
if [ "$line" = "runs=4 runs_grew=1 runs_flat=3" ] \
   && grep -qx 'grew:1' < <(printf '%s\n' "$ob" | tr ' ' '\n') \
   && grep -qx 'flat-stalled:1' < <(printf '%s\n' "$ob" | tr ' ' '\n') \
   && grep -qx 'flat-incomplete:1' < <(printf '%s\n' "$ob" | tr ' ' '\n') \
   && grep -qx 'flat-fast:1' < <(printf '%s\n' "$ob" | tr ' ' '\n') \
   && [ "$sum" = 4 ]; then
  ok "三种「没动」按结局分档，且分档之和等于 runs"
else
  bad "结局分档：期望 runs=4 runs_grew=1 runs_flat=3 且各档 1（和为 4），实得 ${line:-无} / ${ob:-无}"
fi

# 21. 分档也要进暴露格式：面板要能按 outcome 画，而不是自己再数一遍日志。
pout=$(CKPT="$TMP/ck_out" LOG="$TMP/log_out" POOL="$TMP/pool_blk" TARGET=240 WINDOW=3600 \
       bash "$SCRIPT" --prometheus 2>&1)
if grep -qx 'eval_runs_by_outcome{outcome="flat-stalled"} 1' < <(printf '%s\n' "$pout") \
   && grep -qx 'eval_runs_by_outcome{outcome="flat-incomplete"} 1' < <(printf '%s\n' "$pout"); then
  ok "--prometheus 按 outcome 出序列"
else
  bad "--prometheus 结局分档：实得 $(printf '%s' "$pout" | grep -E 'runs_by_outcome' | tr '\n' ';')"
fi

# 22. 每个基准的得分：分母是**已判数**，不是这个基准的题数。检查点里没有计划总数那一列，
#     所以这一格只能回答「判过的那部分里答对多少」。三个数一起印（judged/resolved/score），
#     比例有多少行撑着由读者自己看得见。
#     两条基准一起放，是因为总数那条线看不出「78 行全在一个基准上」与「两个基准各 39 行」的区别。
{ printf '{"kind":"header","run_id":"deadbeef"}\n'
  for i in $(seq 1 30); do printf '{"kind":"case","benchmark":"hle","verdict":{"resolved":true}}\n'; done
  for i in $(seq 1 18); do printf '{"kind":"case","benchmark":"hle","verdict":{"resolved":false}}\n'; done
  for i in $(seq 1 5);  do printf '{"kind":"case","benchmark":"swe-bench-pro","verdict":{"resolved":true}}\n'; done
  for i in $(seq 1 3);  do printf '{"kind":"case","benchmark":"swe-bench-pro","verdict":{"resolved":false}}\n'; done
} >"$TMP/ck_bench"
# 检查点没到目标时这条读数要读值守日志定判词；给一行「涨了」，免得它按「日志缺失」退 3
# ——那样基准得分那一段根本走不到，这三格就成了永远绿的摆设。
printf '%s run eval-rerun-1 phase=Failed dur=61s judged=55->56\n' "$(t_in)" >"$TMP/log_bench"
: >"$TMP/pool_bench"
bout=$(CKPT="$TMP/ck_bench" LOG="$TMP/log_bench" POOL="$TMP/pool_bench" TARGET=240 \
       bash "$SCRIPT" 2>&1)
if grep -qx 'bench=hle judged=48 resolved=30 score=0.6250' < <(printf '%s\n' "$bout") \
   && grep -qx 'bench=swe-bench-pro judged=8 resolved=5 score=0.6250' < <(printf '%s\n' "$bout") \
   && grep -qx 'bench_cases=56' < <(printf '%s\n' "$bout"); then
  ok "按基准印 judged/resolved/score，并印在场证明 bench_cases"
else
  bad "按基准得分：实得 $(printf '%s' "$bout" | grep '^bench=' | tr '\n' ';')"
fi

# 23. 暴露格式：分值带 `coverage="judged-so-far"`，且 judged/resolved 两个计数一起发。
#     只发一个比例，读者会把它当成「这个基准的分数」——而这批用例是判到哪算哪，
#     判过的行也不是随机抽样（按顺序判），所以比例本身不带分母信息时是误导的。
pbout=$(CKPT="$TMP/ck_bench" LOG="$TMP/log_bench" POOL="$TMP/pool_bench" TARGET=240 \
        bash "$SCRIPT" --prometheus 2>&1)
if grep -qx 'eval_bench_score{bench="hle",coverage="judged-so-far"} 0.6250' < <(printf '%s\n' "$pbout") \
   && grep -qx 'eval_bench_judged{bench="hle"} 48' < <(printf '%s\n' "$pbout") \
   && grep -qx 'eval_bench_resolved{bench="hle"} 30' < <(printf '%s\n' "$pbout"); then
  ok "--prometheus 发分值、分母与分子三条"
else
  bad "--prometheus 基准得分：实得 $(printf '%s' "$pbout" | grep 'eval_bench' | tr '\n' ';')"
fi

# 24. 一行用例都没有：不印任何 bench 行（0 条基准不是「0 分」），也不许因此退错码。
# 用例行没有 benchmark 列（老检查点的形状）：这一格不许凭空造一个 `bench="-"` 的分值。
{ printf '{"kind":"header","run_id":"deadbeef"}\n'
  for i in 1 2 3; do printf '{"kind":"case","tokens":10}\n'; done
} >"$TMP/ck_plain"
printf '%s run eval-rerun-1 phase=Failed dur=61s judged=2->3\n' "$(t_in)" >"$TMP/log_plain"
: >"$TMP/pool_plain"
zout=$(CKPT="$TMP/ck_plain" LOG="$TMP/log_plain" POOL="$TMP/pool_plain" TARGET=240 \
       bash "$SCRIPT" 2>&1); zrc=$?
if ! grep -q '^bench=' < <(printf '%s\n' "$zout") \
   && grep -qx 'bench_unattributed=3' < <(printf '%s\n' "$zout") \
   && grep -qx 'bench_cases=0' < <(printf '%s\n' "$zout") \
   && [ "$zrc" != 3 ]; then
  ok "归不到基准的用例不造分，单独计数，也不退 3"
else
  bad "无用例：实得 rc=$zrc / $(printf '%s' "$zout" | grep -E '^bench' | tr '\n' ';')"
fi

# 25. 一条都判过时，暴露格式里仍要留下**在场证明**：`eval_bench_cases 0`。
#     没有它，聚合器分不清「检查点是新的、还没判过」与「这段代码被改坏了、一条也不印」——
#     两种都是零条按基准的序列，而它们的修法相反（等跑分 / 去改脚本）。
zcases=$(CKPT="$TMP/ck_plain" LOG="$TMP/log_plain" POOL="$TMP/pool_plain" TARGET=240 \
         bash "$SCRIPT" --prometheus 2>&1)
if grep -qx 'eval_bench_cases 0' < <(printf '%s\n' "$zcases") \
   && ! grep -q 'eval_bench_score{' < <(printf '%s\n' "$zcases"); then
  ok "无用例时印 eval_bench_cases 0，且不造分"
else
  bad "无用例的暴露格式：实得 $(printf '%s' "$zcases" | grep 'eval_bench' | tr '\n' ';')"
fi

# 26. 一条都归不到基准时，这一格仍要印 0，不许缺席。
#     `bench_unattributed` 原来只在非零时印，于是它**只在坏状态存在**：对着它的告警
#     在好的时候没有可判的数，在坏的时候才有——两种沉默（真的零条 / 这一节没跑）在
#     面板与聚合器眼里同形。印 0 之后，缺席只剩一个意思。
if grep -qx 'bench_unattributed=0' < <(printf '%s\n' "$bout") \
   && grep -qx 'eval_bench_unattributed 0' < <(printf '%s\n' "$pbout"); then
  ok "全部归到基准时印 bench_unattributed=0，暴露格式同印"
else
  bad "零条无主用例：实得 $(printf '%s\n' "$bout" | grep '^bench_unattributed' || echo '(缺席)') / $(printf '%s\n' "$pbout" | grep 'eval_bench_unattributed' || echo '(缺席)')"
fi

# 27. 每一格都被上游拒（429 + 一个几天长的等待），而池读数报 1：日志里那一行说的是
#     「这次跑为什么不是一次完整的实验」（诊断臂），报表 `## failures` 说的才是「题为什么
#     没判」。两个成因的修法相反，读数必须给后者：等配额，不是改命令行。2026-11-04 实测
#     现场就是这一份：320 轮全空，日志写 `failure=diagnostic-arm`，报表里 2/2 格是
#     `429`、`asked for another 498106s`，判词却只是 `stalled-unknown`。
mkdir -p "$TMP/up"
ckpt "$TMP/up/run.md.cases.jsonl" 0
printf '%s run eval-rerun-1 phase=Failed dur=21s judged=0->0 last=Error: "this run is not a complete experiment: tools: this run used the tools-off diagnostic arm, so it is not a main-table row"\n' \
  "$(t_in)" >"$TMP/log_up"
printf '%s judged=0 llm_pool_available=1 llm_pool_next_attempt_unix=1793744227 llm_pool_evidenced_recovery_unix=1793807999\n' \
  "$(t_in)" >"$TMP/pool_up"
{
  printf '## failures\n'
  printf 'failure\thle\tgepa\tseed=0\tcase-a\tscaffold failed: LLM upstream refused (rate_limited): the upstream asked for another 498106s; this run refuses to sit through more than 600s\n'
  printf 'failure\thle\tagentflow\tseed=0\tcase-a\tscaffold failed: LLM upstream refused (rate_limited): API error (HTTP 429): {"blocked_upstreams":["k3"]}\n'
  printf '\n## skipped\n'
} >"$TMP/up/run.md"
uout=$(CKPT="$TMP/up/run.md.cases.jsonl" LOG="$TMP/log_up" POOL="$TMP/pool_up" TARGET=240 \
       WINDOW=3600 bash "$SCRIPT" 2>&1); urc=$?
uverdict=$(printf '%s\n' "$uout" | sed -n 's/^verdict=\([a-z-]*\).*/\1/p')
ucause=$(printf '%s\n' "$uout" | sed -n 's/^last_run=.* cause=\([a-z-]*\).*/\1/p')
uret=$(printf '%s\n' "$uout" | sed -n 's/^last_run=.* retry_after_s=\([0-9]*\).*/\1/p')
if [ "$urc" = 1 ] && [ "$uverdict" = blocked-on-upstream-quota ] \
   && [ "$ucause" = upstream-refused ] && [ "$uret" = 498106 ]; then
  ok "整轮被上游拒 ⇒ cause=upstream-refused，判词 blocked-on-upstream-quota，带上游自报的等待"
else
  bad "上游拒绝：期望 rc=1 verdict=blocked-on-upstream-quota cause=upstream-refused retry_after_s=498106，实得 rc=$urc verdict=${uverdict:-?} cause=${ucause:-?} retry=${uret:-?}"
fi
uprof=$(CKPT="$TMP/up/run.md.cases.jsonl" LOG="$TMP/log_up" POOL="$TMP/pool_up" TARGET=240 \
        WINDOW=3600 bash "$SCRIPT" --prometheus 2>&1)
if grep -qx 'eval_last_run_cause{reason="upstream-refused"} 1' < <(printf '%s\n' "$uprof") \
   && grep -qx 'eval_last_run_retry_after_seconds 498106' < <(printf '%s\n' "$uprof") \
   && grep -qx 'eval_verdict{state="blocked-on-upstream-quota"} 1' < <(printf '%s\n' "$uprof"); then
  ok "暴露格式里也是同一份判词（成因 + 等待 + 判词）"
else
  bad "上游拒绝的暴露格式：实得 $(printf '%s\n' "$uprof" | grep -E 'eval_last_run|eval_verdict' | tr '\n' ';')"
fi

# 28. 起不来环境（容器/Pod 面）是另一个成因，它的下一步是修执行面，不是等配额。
{
  printf '## failures\n'
  printf 'failure\tswe-bench-pro\tgepa\tseed=0\tcase-b\tcannot acquire env: cannot start a container from x: this execution face cannot unpack image layers\n'
  printf 'failure\tswe-bench-pro\tgepa\tseed=0\tcase-c\tcannot acquire env: no compose backend is wired\n'
} >"$TMP/up/run.md"
eout=$(CKPT="$TMP/up/run.md.cases.jsonl" LOG="$TMP/log_up" POOL="$TMP/pool_up" TARGET=240 \
       WINDOW=3600 bash "$SCRIPT" 2>&1)
ecause=$(printf '%s\n' "$eout" | sed -n 's/^last_run=.* cause=\([a-z-]*\).*/\1/p')
everdict=$(printf '%s\n' "$eout" | sed -n 's/^verdict=\([a-z-]*\).*/\1/p')
if [ "$ecause" = env-unavailable ] && [ "$everdict" = stalled-unknown ]; then
  ok "起不来环境 ⇒ cause=env-unavailable（判词不变，成因另给）"
else
  bad "环境失败：期望 cause=env-unavailable verdict=stalled-unknown，实得 cause=${ecause:-?} verdict=${everdict:-?}"
fi

# 29. 报表不在 / 这一轮没有失败行 ⇒ cause=-，不许把「没读到」印成一个成因。
rm -f "$TMP/up/run.md"
gout=$(CKPT="$TMP/up/run.md.cases.jsonl" LOG="$TMP/log_up" POOL="$TMP/pool_up" TARGET=240 \
       WINDOW=3600 bash "$SCRIPT" 2>&1)
gcause=$(printf '%s\n' "$gout" | sed -n 's/^last_run=.* cause=\([a-z-]*\).*/\1/p')
gverdict=$(printf '%s\n' "$gout" | sed -n 's/^verdict=\([a-z-]*\).*/\1/p')
if [ "$gcause" = '-' ] && [ "$gverdict" = stalled-unknown ]; then
  ok "报表读不到 ⇒ cause=-（不当成因），判词退回 stalled-unknown"
else
  bad "报表缺失：期望 cause=- verdict=stalled-unknown，实得 cause=${gcause:-?} verdict=${gverdict:-?}"
fi

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

#!/bin/bash
# 按登记的 eval-table 命令行反复重跑，直到 checkpoint 里判过的用例数到达目标。
#
# 为什么是「反复重跑」而不是「跑一次」：上游池一次只开几分钟（2026-10-18 实测两次窗口
# 约 8 与 15 分钟），一次跑不可能判完 240 例；而 checkpoint 只记**判过**的用例，未判的
# 下次重跑。于是「已判例数」是唯一随窗口累积的量，判据也写成它。
# 见 memory/feedback_a_terminal_threshold_throws_away_what_a_short_window_proved.md。
#
# 为什么网格只点 hle、只点 gepa+agentflow：名字点多了不会多判一行，却让**每一轮都退非 0**。
# 2026-10-18T22:31Z–10-20T06:24Z 实测 147 轮全部 Failed，其中 134 轮是同一句
# 「not a complete experiment」：这一跑点着 `toolathlon`，而评测 Pod 里没有能建 Pod 的
# `kubectl`（工具还按诊断臂关着），于是 wiring 缺一列 ⇒ 退出码非 0 ⇒ 值守每轮记失败。
# 这一句**不等于「两天一行没落」**：10-19T00:05:12Z 那条 NOTE 记着 judged 73->78（轮
# eval-rerun-10182343，23:43:13Z 起）——退出码非 0 时 `judged=A->B` 照样印，行照样落。
# 真正让计数停住的是另一种形状：1813–1815 秒 `phase=stalled`、`last=` 只有一行版本横幅，
# 以及唯一一次判出行的轮次判到第 5 行就卡死。所以收窄网格是清掉假红，不是解决停住。
# 目标 198 = 40 题 × 3 seed × 2 scaffold
# 减 7 道带图题（×3 seed ×2），算术里本来就没有另两列；把命令行收窄到与目标一致，
# 是让「失败」重新表示真的失败，不是绕过失败。swe-bench-pro 与 toolathlon 的读数
# 不在这一臂的目标里；将来要评它们，应当各自开一次实验，而不是并进这张网格。
#
# 为什么工具臂是 `platform` 而不是 `none`：`--tools none` 按定义**不是主表行**——它把
# 工具面关掉，跑出来的读数是「这个模型不用工具会怎样」，而登记这一臂要的是主表那一行。
# 这条命令行此前跑的是 `none`，而每一轮的退出理由里都写着「this run used the tools-off
# diagnostic arm, so it is not a main-table row」（crates/cogneva/src/eval_table.rs 里那句
# 判词的原文）——判词早就说清楚了，值守读到它却没改命令行，于是半个月的轮次一轮都进不了表。
# HLE 点名的三件（web_search / web_fetch / python）现在是平台已有的实现：前两件就是主应用
# 那两个工具，端点取自这份 Pod 已挂的 cogneva.json（`agent_loop.web_search_base` /
# `web_fetch_base`），第三件走 `system.sandbox_executor_url` 指向的沙箱执行器。
# 没接上时不是悄悄降级：每一格记 `errored` 并说缺哪一件，整列按 `NotWired` 记一条缺席。
#
# 为什么敢盲重跑：池关着时一轮只跑 50–80 秒、一条 checkpoint 都不写（每格秒回 503），
# 比池子自己的指标可靠——`llm_pool_available` 两个方向都骗过人。
# 见 memory/project_all_llm_columns_share_one_five_hour_quota_pool.md。
#
# 为什么要一个停滞判据：没有它，一轮卡住会一直占到 POD_TIMEOUT 才被杀，整段窗口白烧。
# 但**只看 checkpoint 会杀错人**：checkpoint 只在「一格判完」那一刻变，而一格里的模型调用
# 可以流式生成几分钟。2026-10-19T00:41Z 实测：把评测客户端发出的那个请求体原样打到网关，
# 0.6 秒给出 200 + `text/event-stream`，随后一路流了 239 秒、622051 字节，这 239 秒里
# checkpoint 一个字节不动。于是「生成得久」与「请求永远不被唤醒」在 checkpoint 这一列上
# 同形——按那一列杀，杀掉的正是还在干活的轮次（00:27–00:48 那几轮就是这样被杀的）。
# 所以判据读两个数：checkpoint 的字节数**和**进程自己的 io 计数（/proc/1/io 的 rchar+wchar）。
# 两个都不动才算停滞；io 在动说明它还在收流，继续等。
#
# 镜像跟线上走，不钉。2026-11-05 之前这里钉着一个 rev，理由是「checkpoint 的身份里有
# software，换镜像会整轮 bail、已判例数不跟过来」。那条理由在 2026-11-05 修掉了：续跑判据
# 现在是「除软件版本以外逐行相同」（`crates/cog-eval/src/rig.rs::same_experiment`），换版本
# 只换每一例的来源戳，不丢已判的例。理由没了之后，钉住只剩副作用——钉着的 rev 会悄悄烂掉，
# 而它烂掉时**没有任何读数会喊**（2026-11-04 钉的 main-ac2748167396 在 registry 里根本
# 不存在，按它起的 Pod 只会 ImagePullBackOff，一轮都没跑起来过）。所以默认取线上正在跑的
# 那个镜像。见 memory/feedback_a_checkpoint_is_keyed_on_the_software_rev_so_an_image_change_starts_over.md。
#
# 退出码：0＝到目标；1＝时间用尽仍未到目标（读数在日志里）；2＝参数错；
#         3＝前置条件问不出来（kubectl 不可用／checkpoint 读不到），不许当 0 读。
set -u

NAMESPACE=${NAMESPACE:-cogneva}
# 部署器把「线上在跑哪个 rev」写在它的状态文件里，见 deployed_rev。
STATE_POD=${COGNEVA_STATE_POD:-deploy/cogneva-evolution}
# 镜像默认**跟线上走**：取 deployment 的 image 字段（现网在跑哪一个，是那台机器自己的
# 事实，不是这份脚本里的一个常量）。`--image` 仍可显式覆盖——跑登记的某一版，或线上坏了
# 要拿一版已知好的对照。上面那一段讲了为什么钉住的代价大于收益。
#
# 取不到就是 3，不回落：回落成一个写死的 tag 会让「线上是什么」与「这一臂跑的是什么」
# 在日志上重新同形，而那正是这次改动要拆开的东西。
IMAGE=${IMAGE:-}
# 线上镜像从哪个 deployment 取。换名的部署（多副本、另一个 release）用 --live-deployment。
LIVE_DEPLOYMENT=${LIVE_DEPLOYMENT:-cogneva}
OUT=${OUT:-/eval/20261018_post-recovery.md}
RUNS_DIR=${RUNS_DIR:-$HOME/eval-runs}
# 198 = 33 题 × 3 seed × 2 scaffold。写 240 会在跳过 42 行之后永远到不了；
# 判据是报表 `## skipped` 段为空时才回到 240（接上 native 视觉之后）。
TARGET=${TARGET:-198}
HORIZON=${HORIZON:-14400}
# 池关着时的重试间隔。2026-10-19 从 180 秒放宽到 900 秒：两个上游都报配额耗尽，
# Kimi 是周窗（约 10-25T03:13Z 复位）、Ark 是月窗（11-04T15:59:59Z），往后六天每一轮
# 都只会拿到 503。180 秒意味着六天约 2160 个空 Pod；900 秒是 576 个，而真开窗时
# 最多晚 15 分钟进去——周窗实测能烧 20–24 小时，晚进去的代价远小于空转。
PAUSE=${PAUSE:-900}
PAUSE_AFTER_GAIN=${PAUSE_AFTER_GAIN:-30}
POD_TIMEOUT=${POD_TIMEOUT:-2700}
# 一个在跑的 pod 这么久没往 checkpoint 写一个字节，就判它停在未被唤醒的等待上。
STALL=${STALL:-600}
# 行钟：这么久没落一个**判过的用例**，且这段时间的字节涨得比自旋还慢，才判停。
#
# 为什么需要第二条钟：io 这一腿只能证明「管道在动」，而自旋本身就在写字节。
# 2026-10-19 实测——池子关着的那一轮，每个 503 都留下一点 io，45 分钟零判例一次没判 stalled，
# 只靠 POD_TIMEOUT 收场。而 io 这一腿不能删（它挡的是把长流式生成杀掉：实测一格 239 秒、
# 622051 字节）。所以拆成两条：**行**没涨够久就停，**字节**没动够久也停，取先到的那条。
#
# 但只按「行」判停会杀掉正在干活的轮次，2026-10-21 实测到了这个形状：10-18 那批
# `abandoned` 的交换里，上游**已经在下发 reasoning_content**、网关这边却记成客户端放弃，
# 41 条全是同一台上游，时间跨度 16:49–23:53Z；而停住的时刻是 1813–1815 秒——正是这条
# 1800 秒的钟加两拍采样。也就是说：一例生成得比 30 分钟久（这条钟原先的口径是「一例都
# 判不出来时生效」，但代码里它是无条件的），这一轮就被当成自旋杀掉，已经花掉的 token 全丢。
# 分开这两种形状的读数本来就在采样里：**字节涨多快**。实测两个端点是
#   自旋  37 字节/20 秒 ≈ 1.9 B/s（每个 503 留一点）
#   长流  622051 字节/239 秒 ≈ 2600 B/s（推理流每帧都在涨）
# 所以行钟只在字节速率低于 PSTALL_MIN_BYTES_PER_SEC 时才判停；高于它说明上游在出货，
# 交给 STALL 那条钟（字节彻底不动才停）与 POD_TIMEOUT 收场。
PROGRESS_STALL=${PROGRESS_STALL:-1800}
# 上面那条速率门限。取 64 B/s：比自旋实测高 30 倍、比长流实测低 40 倍，两个端点都离得远。
# 问不出 io（exec 失败）时速率按 0 算，也就是退回原来那条**只看行**的钟——那一侧的行为
# 不变，只是不再牵连「io 明明读得出来、且在猛涨」的那些轮次。
PSTALL_MIN_BYTES_PER_SEC=${PSTALL_MIN_BYTES_PER_SEC:-64}
# hostPath 卷只在这一台节点上，pod 必须钉到它；默认取本机名（本机名与节点名同为 cogneva）。
NODE_NAME=${NODE_NAME:-$(hostname)}
LOG=${LOG:-$RUNS_DIR/.runner/rerun.log}
DRY_RUN=${DRY_RUN:-}
# 只打印这台 Pod 的清单、不碰集群。清单里的形状（limits/requests）同时被 chart 的
# `resourceGovernance.runtimeWorkloads` 声明了一次——额度按那一份把天花板加发一份，
# 两边不一致时准入按另一份数算，滚动的重叠额度就会被这台 Pod 借走。测试拿这个开关
# 比对两边，见 `eval-registered-rerun.test.sh`。
PRINT_POD=${PRINT_POD:-}

usage() {
  sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Usage: eval-registered-rerun.sh [options]
  --image <ref>      image ref (default: the image the live deployment runs; see the note by
                     IMAGE for why this follows the deployment instead of a pinned tag)
  --live-deployment <name>
                     which deployment to read the live image from (default: cogneva)
  --out <path in pod> the report path the checkpoint is named after (default: /eval/20261018_post-recovery.md)
  --target <n>       stop once this many cases are judged (default: 198 = 33 x 3 x 2)
  --horizon <secs>   give up after this long (default: 14400)
  --namespace <ns>   namespace the runs are created in (default: cogneva)
  --stall <secs>     kill a running pod that has moved no bytes for this long (default: 600)
  --progress-stall <secs>
                     kill a running pod that has judged no case for this long (default: 1800);
                     a spin over failing calls keeps the byte counter moving, so bytes alone
                     cannot say a pod is alive
  --log <path>       one line per run goes here
  --dry-run <file>   replay past samples (`<epoch> <bytes> <io 或 ->` one per line);
                     prints the verdict and builds nothing
  --print-pod        print the pod manifest this run would create and exit; builds
                     nothing (the quota's runtime-pod budget is declared from it)
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --image) IMAGE=$2; shift 2;;
    --live-deployment) LIVE_DEPLOYMENT=$2; shift 2;;
    --out) OUT=$2; shift 2;;
    --target) TARGET=$2; shift 2;;
    --horizon) HORIZON=$2; shift 2;;
    --namespace) NAMESPACE=$2; shift 2;;
    --stall) STALL=$2; shift 2;;
    --progress-stall) PROGRESS_STALL=$2; shift 2;;
    --pstall-min-bytes-per-sec) PSTALL_MIN_BYTES_PER_SEC=$2; shift 2;;
    --log) LOG=$2; shift 2;;
    --dry-run) DRY_RUN=$2; shift 2;;
    --print-pod) PRINT_POD=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2;;
  esac
done

CKPT=${CKPT:-$RUNS_DIR/$(basename "$OUT").cases.jsonl}
KUBECTL=${KUBECTL:-sudo -n kubectl}

log() {
  mkdir -p "$(dirname "$LOG")"
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG"
}

# 线上正在跑的那个 rev，只为了在每一轮的第一行旁边站一个对照。
#
# 为什么要有这一行：钉住的镜像与线上之间没有任何读数会比。2026-10-19 钉的
# main-2d1fd99239a0 之后，线上换了十几回 rev、修掉的两处恰好都是「这一臂为什么判不动」，
# 而日志里从头到尾只有 `start image=...`——没有对照，读到它的那一轮看的人无从知道
# 自己比线上落后了多久。读不到就是 `-`：这条信息不该拦住一轮。
deployed_rev() {
  local json
  json=$($KUBECTL -n "$NAMESPACE" exec "$STATE_POD" -c cogneva -- \
    cat "${COGNEVA_STATE:-/opt/cogneva/sandbox/mainline/state.json}" 2>/dev/null | tr -d '\n')
  local rev
  rev=$(printf '%s' "$json" \
    | sed -n 's/.*"last_good_rev"[[:space:]]*:[[:space:]]*"\([0-9a-f]\{7,40\}\)".*/\1/p' \
    | cut -c1-12)
  printf '%s\n' "${rev:--}"
}

# 线上正在跑的那个镜像（deployment 的 image 字段）。问不出来就是空串，调用方判 3：
# 这一臂跑的是哪一版，从这里读；读不到时**不许**回落成一个写死的 tag——那会让「线上是
# 什么」与「这一臂跑的是什么」重新变成一个数，而这次改动的全部要点就是把它们分开。
live_image() {
  $KUBECTL -n "$NAMESPACE" get deployment "$LIVE_DEPLOYMENT" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null
}

judged() {
  [ -f "$CKPT" ] || { echo 0; return; }
  local n
  n=$(grep -c . "$CKPT")
  # 首行是 checkpoint 的 run 头，不是用例。把它算进去会让「已判 71」读成 72。
  if head -1 "$CKPT" | grep -q '"kind":"header"'; then echo $((n - 1)); else echo "$n"; fi
}

bytes() {
  stat -c %s "$CKPT" 2>/dev/null || echo 0
}

# 按 scaffold 数已判行。`judged` 一个总数答不了「这一张表的两条腿在不在长」：
# 目标是 agentflow 120 ＋ gepa 120，而今天 78 行全是 gepa、agentflow 一行没落过；
# 总数从 78 涨到 83 时，若涨的全是 gepa，那条腿仍然是零进度，光看总数看不出来。
by_scaffold() {
  [ -f "$CKPT" ] || { echo '-'; return; }
  local out
  out=$(grep -o '"scaffold":"[a-z0-9_-]*"' "$CKPT" 2>/dev/null \
    | sed 's/.*:"//; s/"$//' | sort | uniq -c \
    | awk '{printf "%s:%s ", $2, $1}')
  echo "${out% }"
}

# 进程自己收发的字节数（/proc/1/io 的 rchar + wchar）。
#
# 为什么光看 checkpoint 不够：checkpoint 只在**一格判完**那一刻长一个字节，而一格里的
# 模型调用可以流式生成几分钟。2026-10-19T00:41Z 实测：把评测客户端发出的那个请求体原样
# 打到网关，0.6 秒给出 200 + `text/event-stream`，然后一路流了 239 秒、622051 字节
# （推理内容），整段时间 checkpoint 一个字节没动。于是「生成得久」与「请求卡死」在
# checkpoint 这一列上同形——按那一列杀，杀掉的正是还在干活的轮次。
# rchar 在这段里一直在涨（流里每来一帧就涨一点），卡死时它是平的。所以停滞判据读两个数：
# checkpoint 不动 **且** 进程的 io 也不动。exec 问不出来时返回空，调用方按「没有这一条腿」
# 处理，退化成只看 checkpoint——不许把问不出来读成「没动静」。
io_bytes() {
  $KUBECTL -n "$NAMESPACE" exec "$1" -- sh -c \
    'awk "/^(rchar|wchar)/{s+=\$2} END{print s+0}" /proc/1/io' 2>/dev/null || true
}

# 一次采样的判定：**行**没涨够久且字节涨得比自旋还慢，或**字节**没动够久，都算停。
# 参数：now last_any last_row last_bytes now_bytes last_io now_io io_at_row stall progress_stall min_rate
# 输出：`<新的 last_any> <新的 last_row> <新的 io_at_row> <run|stalled>`
#
# 两条钟各管一件事：`last_any` 认 io 也认 checkpoint（挡长流式生成被误杀），
# `last_row` 只认 checkpoint（挡自旋冒充在跑），而它判停之前还要过一道**速率**：
# 从最后一次落行到现在的字节速率低于 min_rate，才能说这段时间是自旋而不是在出货。
# 单独抽成函数是为了能被回放测试喂历史样本——判停滞的这条循环过去只有上线才验证，
# 而它判错的方向（把在生成的轮次杀掉）在日志上看起来完全正常。
sample_verdict() {
  local now=$1 lc=$2 lr=$3 lb=$4 nb=$5 li=$6 ni=$7 ior=$8 stall=$9 pstall=${10} rate=${11}
  # 空格子在这条返回串上活不过一次 `read`：`read` 按空白分列、连续空白并成一格，
  # io 那一格空着时后面那个词就顶上来——判词被挤到第四格之后（调用方读成空，这一拍
  # 的判停当场丢掉），而 `run`/`stalled` 落进 io 那一格，下一拍被当数做减法。`set -u`
  # 下那是求值失败，而失败发生在 `if` 条件里：这一拍判不出停、`lc`/`lr` 却已经被词
  # 换掉了，此后每拍都判不出——一次 `kubectl exec` 抖动最后让这一轮跑满 POD_TIMEOUT，
  # 日志上只有一行 unbound variable。所以空格子出去时写成 `-`，进来时非数字一律当
  # 「没问到」，与 `io_bytes` 问不出来那一况同一条腿。
  case "$ni" in '' | *[!0-9]*) ni= ;; esac
  case "$ior" in '' | - | *[!0-9]*) ior= ;; esac
  if [ "$nb" != "$lb" ]; then
    lc=$now
    lr=$now
    [ -n "$ni" ] && ior=$ni
  elif [ -n "$ni" ] && [ -n "$li" ] && [ "$ni" != "$li" ]; then
    lc=$now
  fi
  local stalled=0 bps=0
  if [ $(( now - lc )) -ge "$stall" ]; then
    stalled=1
  elif [ $(( now - lr )) -ge "$pstall" ]; then
    # 问不出 io 时 bps 留在 0：退回只看行的那条钟，不拿读不出来的 io 证明还活着。
    if [ -n "$ni" ] && [ -n "$ior" ] && [ "$now" -gt "$lr" ]; then
      bps=$(( (ni - ior) / (now - lr) ))
    fi
    [ "$bps" -lt "$rate" ] && stalled=1
  fi
  if [ "$stalled" -eq 1 ]; then
    printf '%s %s %s stalled\n' "$lc" "$lr" "${ior:--}"
  else
    printf '%s %s %s run\n' "$lc" "$lr" "${ior:--}"
  fi
}

# 回放一份历史采样：每行 `<epoch> <checkpoint 字节数> <io 计数或 ->`。
# io 那一列既是「管道在动」的证据，也是行钟那条速率门的分子：`-` 表示那一刻问不出，
# 与线上一样退化成只看行。
# `-` 表示那一刻问不出 io（exec 失败），与线上一样退化成只看 checkpoint。
# 判停就打印并返回 1；走完没停返回 0。
dry_run() {
  local file=$1 t b i last_t='' lc='' lr='' lb='' li='' ior='' out lc2 lr2 ior2 verdict=''
  [ -r "$file" ] || { echo "dry-run: cannot read $file" >&2; return 2; }
  while read -r t b i; do
    [ -n "${t:-}" ] || continue
    last_t=$t
    case "${i:-}" in -|'') i=;; esac
    if [ -z "${lb:-}" ]; then lc=$t; lr=$t; lb=$b; li=$i; ior=$i; continue; fi
    out=$(sample_verdict "$t" "$lc" "$lr" "$lb" "$b" "${li:-}" "$i" "${ior:-}" "$STALL" "$PROGRESS_STALL" "$PSTALL_MIN_BYTES_PER_SEC")
    read -r lc2 lr2 ior2 verdict <<<"$out"
    lc=$lc2; lr=$lr2
    [ -n "$ior2" ] && ior=$ior2
    lb=$b
    [ -n "$i" ] && li=$i
    if [ "$verdict" = stalled ]; then
      printf 'stalled at %s (last byte movement %s, last row %s)\n' "$t" "$lc" "$lr"
      return 1
    fi
  done <"$file"
  printf 'running to %s (no stall)\n' "${last_t:-<no samples>}"
  return 0
}

if [ -n "$DRY_RUN" ]; then dry_run "$DRY_RUN"; exit $?; fi

# 一条 Pod：与登记跑法逐字相同的参数，只换 pod 名与镜像 tag（登记里写明这两样不属于跑法）。
manifest() {
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NAMESPACE
  labels: {app: eval-table, run: registered-rerun}
spec:
  restartPolicy: Never
  nodeName: $NODE_NAME
  containers:
  - name: eval
    # 空的只会出现在 --print-pod：那一分支是纯渲染、不连集群，所以问不到线上镜像。
    # （这一段是不加引号的 heredoc，反引号会被当成命令替换执行，所以这里不用反引号。）
    image: ${IMAGE:-localhost:30500/cogneva:live}
    env:
    # 网关的 \`llm_calls_total{actor="eval"}\` 不区分是哪一次跑，两边的读数会并进同一个
    # 计数器（2026-10-19 实测：两个会话同时跑，计数只多 1，谁也认不出自己那条）。
    # origin 随请求头走、不进 checkpoint 的 material，所以给它一个固定的名字只花一个标签。
    - {name: COGNEVA_LLM_ORIGIN, value: "${ORIGIN:-eval-pod}"}
    command: ["/opt/cogneva/cogneva","eval-table"]
    args: ["--pins","/eval/pins.tsv","--data-root","/data",
           "--subset","/eval/seed20261011_n40.tsv",
           "--backbone","via-security-gateway@1","--judge","via-security-gateway@1",
           "--benchmarks","hle",
           "--scaffolds","gepa,agentflow",
           "--seeds","0,1,2","--tools","platform","--concurrency-slot","4",
           "--out","$OUT"]
    resources: {requests: {cpu: 500m, memory: 1Gi}, limits: {cpu: "2", memory: 3Gi}}
    volumeMounts:
    - {name: data, mountPath: /data, readOnly: true}
    - {name: runs, mountPath: /eval}
    - {name: cfg, mountPath: /etc/cogneva, readOnly: true}
  volumes:
  - name: data
    hostPath: {path: /srv/cogneva/benchmarks, type: Directory}
  - name: runs
    hostPath: {path: $RUNS_DIR, type: Directory}
  - name: cfg
    configMap: {name: cogneva-json, optional: true}
EOF
}

# 打印就到此为止：下面每一行都会连集群。
if [ -n "$PRINT_POD" ]; then manifest "${PRINT_POD_NAME:-eval-registered-rerun-shape}"; exit 0; fi

# 前置条件问不出来就判 3，别让它跑成一串「0 例」。
# 这一段排在 `--print-pod` **之后**：打印清单是纯渲染，不连集群，也不读检查点。
# 排在前面时，任何一台没有集群的机器（CI、刚克隆下来读形状的人）拿到的都是 3，
# 而不是那份清单——判据看起来最严，实际把这个功能关在了只有集群机器上。
$KUBECTL -n "$NAMESPACE" auth can-i create pods >/dev/null 2>&1 || { log "cannot ask the cluster whether pods may be created"; exit 3; }
[ -f "$CKPT" ] || [ "$(judged)" = 0 ] || { log "the checkpoint cannot be read: $CKPT"; exit 3; }

# 显式给了 `--image` 就钉住不动；没给就每一轮重新问一次线上。
#
# 每一轮重问，是因为部署会在这一跑的几个小时里滚好几次：把第一轮读到的那个 tag 用在
# 第六轮上，跑的就是一台已经不存在的线上。换版本对已判的例没有代价了（见 IMAGE 那一段），
# 所以「跟着线上变」在这里是免费的。
IMAGE_FIXED=${IMAGE:+1}
resolve_image() {
  [ -n "$IMAGE_FIXED" ] && return 0
  local live
  live=$(live_image)
  [ -n "$live" ] || { log "cannot read the image of deployment/$LIVE_DEPLOYMENT in namespace $NAMESPACE"; exit 3; }
  if [ -n "$IMAGE" ] && [ "$live" != "$IMAGE" ]; then
    log "the live image moved: $IMAGE -> $live (cases already judged stay; each case carries its own revision)"
  fi
  IMAGE=$live
}

deadline=$(( $(date +%s) + HORIZON ))
resolve_image
log "start image=$IMAGE deployed=$(deployed_rev) out=$OUT target=$TARGET judged=$(judged)"

# 为什么不拿 `llm_pool_available` 当开闸条件（2026-10-19 明确否掉）：池读数两个方向都骗过人，
# 读出 0 而池子其实开着时不发这一轮，代价是**整段窗口**（今天的窗口只有 29 分钟，往后再没有），
# 而多发一轮的代价是八十秒的 Pod 与一遍 /data 读。两个方向的代价差一个量级，所以宁可空转：
# 空转看得见（judged 不动、dur 短），漏窗看不见（什么都没发生）。
while [ "$(date +%s)" -lt "$deadline" ]; do
  before=$(judged)
  if [ "$before" -ge "$TARGET" ]; then log "reached target: judged=$before"; exit 0; fi
  resolve_image
  name="eval-rerun-$(date -u +%m%d%H%M)"
  $KUBECTL -n "$NAMESPACE" delete pod "$name" --ignore-not-found --wait=false >/dev/null 2>&1
  if ! manifest "$name" | $KUBECTL apply -f - >/dev/null 2>&1; then
    log "the pod could not be created ($name); retrying"
    sleep "$PAUSE"; continue
  fi
  started=$(date +%s)
  last_change=$started
  last_row=$started
  last_bytes=$(bytes)
  # 这一条读不出来就留空：退化成只看 checkpoint，而不是把「问不出来」读成「没动静」。
  last_io=$(io_bytes "$name")
  io_at_row=$last_io
  phase=timeout
  while [ $(( $(date +%s) - started )) -lt "$POD_TIMEOUT" ]; do
    sleep 20
    ph=$($KUBECTL -n "$NAMESPACE" get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null)
    case "$ph" in Succeeded|Failed) phase=$ph; break;; esac
    now_bytes=$(bytes)
    now_io=$(io_bytes "$name")
    out=$(sample_verdict "$(date +%s)" "$last_change" "$last_row" "$last_bytes" "$now_bytes" "$last_io" "$now_io" "${io_at_row:-}" "$STALL" "$PROGRESS_STALL" "$PSTALL_MIN_BYTES_PER_SEC")
    read -r last_change last_row io_at_row verdict <<<"$out"
    last_bytes=$now_bytes
    [ -n "$now_io" ] && last_io=$now_io
    if [ "$verdict" = stalled ]; then phase=stalled; break; fi
  done
  after=$(judged)
  # 跑完才读退出原因：只记 dur 与 judged 时，「进了窗口却一例未判」与「池子关着一例没跑」
  # 是同一个形状（都是 dur 稍长、judged 不动），下一次就分不出该不该提前结束这一轮。
  last=$($KUBECTL -n "$NAMESPACE" logs "$name" --tail=1 2>/dev/null | tr '\t' ' ' | tr -d '\n' | cut -c1-160)
  # 这一行的四个数就是判「有没有进窗口」的全部依据：duration 短且 judged 不动＝池子关着。
  # phase=stalled 是第五种：pod 还在 Running，但它要么这么久没涨过一行（progress-stall），
  # 要么连字节都没动过（stall）——前者抓的是自旋，后者抓的是真死。
  log "run $name phase=$phase dur=$(( $(date +%s) - started ))s judged=$before->$after scaffolds=$(by_scaffold) last=${last:-<no output>}"
  $KUBECTL -n "$NAMESPACE" delete pod "$name" --ignore-not-found --wait=false >/dev/null 2>&1
  if [ "$after" -gt "$before" ]; then sleep "$PAUSE_AFTER_GAIN"; else sleep "$PAUSE"; fi
done

final=$(judged)
log "horizon reached: judged=$final target=$TARGET"
[ "$final" -ge "$TARGET" ] && exit 0 || exit 1

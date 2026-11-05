#!/usr/bin/env bash
# principle: feedback_a_clock_reset_by_any_byte_movement_cannot_tell_a_spin_from_a_long_generation
#
# `eval-registered-rerun.sh --dry-run` 的回放测试：只喂合成采样，不连集群、不建 Pod。
#
# 判的是「判停」这条判据分不分得开三种形状——它们在线上一度同形：
#   1. 自旋：每个失败的调用都写一点字节，checkpoint 一行不涨；
#   2. 真在生成：字节猛涨、checkpoint 长时间不动，但最后确实落了一行；
#   3. 真死了：什么都不动。
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/eval-registered-rerun.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: 脚本不在：$SCRIPT" >&2; exit 1; }

pass=0; fail=0
ok()  { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

T0=1792000000

# 1. 自旋：io 每 20 秒涨一点，checkpoint 始终 94 字节 → 必须在 1800 秒那条钟上停
awk -v t="$T0" 'BEGIN{for(i=0;i<120;i++){printf "%d 94 %d\n", t+i*20, 1000000+i*37}}' >"$TMP/spin"
out=$(bash "$SCRIPT" --dry-run "$TMP/spin" 2>&1); rc=$?
want="stalled at $((T0 + 1800))"
case "$out" in
  "$want"*) ok "自旋（字节在动、行不涨）在 progress-stall 上停" ;;
  *) bad "自旋：期望以「$want」开头，实得「${out}」" ;;
esac
[ "$rc" -eq 1 ] && ok "判停的退出码是 1" || bad "判停：期望 rc=1，实得 $rc"

# 2. 长生成：字节猛涨 400 秒、然后落一行，之后又静下来 → 不许判停
{ awk -v t="$T0" 'BEGIN{for(i=0;i<20;i++){printf "%d 94 %d\n", t+i*20, 1000000+i*31000}}'
  printf '%d 4242 %d\n' "$((T0 + 400))" 1900000
  awk -v t="$T0" 'BEGIN{for(i=21;i<60;i++){printf "%d 4242 %d\n", t+i*20, 1900000+i*7}}'
} >"$TMP/gen"
out=$(bash "$SCRIPT" --dry-run "$TMP/gen" 2>&1); rc=$?
case "$out" in
  running\ to*) ok "长生成落了行之后没有被误杀" ;;
  *) bad "长生成：期望不判停，实得「${out}」" ;;
esac
[ "$rc" -eq 0 ] && ok "不判停的退出码是 0" || bad "不判停：期望 rc=0，实得 $rc"

# 3. 真死了：字节与行都不动 → 600 秒那条钟先到
awk -v t="$T0" 'BEGIN{for(i=0;i<36;i++){printf "%d 94 1000000\n", t+i*20}}' >"$TMP/dead"
out=$(bash "$SCRIPT" --dry-run "$TMP/dead" 2>&1); rc=$?
want="stalled at $((T0 + 600))"
case "$out" in
  "$want"*) ok "完全不动时由 --stall 那条钟判停" ;;
  *) bad "真死了：期望以「$want」开头，实得「${out}」" ;;
esac

# 4. 两个上界都是旋钮：把 progress-stall 抬到 3600，同一份自旋样本必须晚停
#    （样本要比 3600 秒长，否则它只是被样本的长度挡住，测不出旋钮）
awk -v t="$T0" 'BEGIN{for(i=0;i<200;i++){printf "%d 94 %d\n", t+i*20, 1000000+i*37}}' >"$TMP/spin_long"
out=$(bash "$SCRIPT" --dry-run "$TMP/spin_long" --progress-stall 3600 2>&1)
case "$out" in
  "stalled at $((T0 + 3600))"*) ok "--progress-stall 抬得动，判停落在新上界上" ;;
  *) bad "progress-stall 旋钮：期望停在 $((T0 + 3600))，实得「${out}」" ;;
esac

# 6. 长流一直没有落行：字节 1550 B/s 涨了 2400 秒，一行没落。这条在 2026-10-18 被
#    「行钟无条件生效」的旧判据杀过（1813–1815 秒停住、41 条上游已经在下发 reasoning 的
#    交换被记成 abandoned）。速率高于门限就不许判停。
awk -v t="$T0" 'BEGIN{for(i=0;i<120;i++){printf "%d 94 %d\n", t+i*20, 1000000+i*31000}}' >"$TMP/longstream"
out=$(bash "$SCRIPT" --dry-run "$TMP/longstream" 2>&1); rc=$?
case "$out" in
  running\ to*) ok "字节在猛涨、一行没落的长流没有被行钟杀掉" ;;
  *) bad "长流无行：期望不判停，实得「${out}」" ;;
esac
[ "$rc" -eq 0 ] && ok "长流无行的退出码是 0" || bad "长流无行：期望 rc=0，实得 $rc"

# 7. 速率门限是旋钮：同一份长流样本，把门限抬到 4000 B/s 就必须停回行钟上
out=$(bash "$SCRIPT" --dry-run "$TMP/longstream" --pstall-min-bytes-per-sec 4000 2>&1)
case "$out" in
  "stalled at $((T0 + 1800))"*) ok "--pstall-min-bytes-per-sec 抬得动，速率不够时仍停在行钟上" ;;
  *) bad "速率门限旋钮：期望停在 $((T0 + 1800))，实得「${out}」" ;;
esac

# 8. io 有几拍问不出来（exec 抖动）之后又能读了、字节一直不动：这一格过去是空串，
#    返回串里那个空格把「run」顶进 io 那一格，下一拍拿它做减法——`set -u` 下是
#    unbound variable，而失败发生在 if 条件里，判停从此每拍都失效，这一轮要跑满
#    POD_TIMEOUT。要求：不出现这个词，且判停照旧落在速率那条钟上。
awk -v t="$T0" 'BEGIN{for(i=0;i<200;i++){io=(i<10)?"-":500000; printf "%d 94 %s\n", t+i*20, io}}' >"$TMP/noio_then_readable"
out=$(bash "$SCRIPT" --dry-run "$TMP/noio_then_readable" --stall 3600 2>&1); rc=$?
case "$out" in
  *unbound\ variable*) bad "io 抖动之后判停失效：输出里有 unbound variable（${out}）" ;;
  "stalled at $((T0 + 1800))"*) ok "io 有几拍问不出来，判停仍然落在速率那条钟上" ;;
  *) bad "io 抖动：期望停在 $((T0 + 1800))，实得「${out}」" ;;
esac
[ "$rc" -eq 1 ] && ok "io 抖动后判停的退出码是 1" || bad "io 抖动：期望 rc=1，实得 $rc"

# 9. io 从第一拍起就问不出来：io 那一格一直是空串，而判停恰恰要在第二拍落地。
#    过去这一拍的返回串少一列，判词被挤出去，第一次判停读成「没停」（后面几拍
#    靠被污染的词凑够列数才偶然判得出——那不是判据，是巧合）。
awk -v t="$T0" 'BEGIN{for(i=0;i<20;i++){printf "%d 94 -\n", t+i*20}}' >"$TMP/noio_ever"
out=$(bash "$SCRIPT" --dry-run "$TMP/noio_ever" --stall 20 2>&1); rc=$?
case "$out" in
  *unbound\ variable*) bad "io 全程问不出来：输出里有 unbound variable（${out}）" ;;
  "stalled at $((T0 + 20))"*) ok "io 那一格空着时判停也落得下来" ;;
  *) bad "io 全程问不出来：期望停在 $((T0 + 20))，实得「${out}」" ;;
esac
[ "$rc" -eq 1 ] && ok "io 全程问不出来时退出码是 1" || bad "io 全程问不出来：期望 rc=1，实得 $rc"

# 5. 读不出来的样本文件是 2，不是 0（与「跑完了没停」分开）
out=$(bash "$SCRIPT" --dry-run "$TMP/nope" 2>&1); rc=$?
[ "$rc" -eq 2 ] && ok "样本文件不在是 rc=2" || bad "样本文件不在：期望 rc=2，实得 $rc（$out）"

# 6. 这台 Pod 的形状必须与 chart 的 `resourceGovernance.runtimeWorkloads` 逐项相同。
#    额度把这一份形状算进了命名空间天花板（`deploy/helm/cogneva/values.yaml`），两边
#    不一致时准入按另一份数算——**这台 Pod 就会从滚动的重叠额度里借地方**。2026-10-26
#    实测过一次：这台 3Gi 的 Pod 在跑时，`cogneva` 的滚动 surge（12Gi）恰好差 1Gi 装不
#    进去，部署器按设计扣住不动，连续两轮 mainline 一个 rev 都没上去。所以只改一边必须红。
REPO="${COGNEVA_REPO:-$(cd "$HERE/../.." && pwd)}"
VALUES="$REPO/deploy/helm/cogneva/values.yaml"
pod_rendered=1
bash "$SCRIPT" --print-pod >"$TMP/pod.yaml" 2>"$TMP/pod.err"; rc=$?
if [ "$rc" -ne 0 ]; then
  # 清单没渲染出来时 pod.yaml 里是那句错误本身，而第 8 格如果只判「文件在」就会拿这段
  # 文本去喂 YAML——报出来的是 Python 的 TypeError，与真正的原因（清单没生成）无关。
  pod_rendered=0
  bad "--print-pod 退 $rc，拿不到清单（$(head -1 "$TMP/pod.err")）"
elif [ ! -f "$VALUES" ]; then
  bad "找不到 chart values：$VALUES（用 COGNEVA_REPO 指路）"
else
  verdict=$(python3 - "$TMP/pod.yaml" "$VALUES" <<'PY' 2>&1
import sys, yaml

def q(text):
    text = str(text).strip()
    if text.endswith("m"):
        return ("cpu", float(text[:-1]))
    suffix = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "K": 10**3, "M": 10**6, "G": 10**9}
    for k, f in suffix.items():
        if text.endswith(k):
            return ("mem", float(text[: -len(k)]) * f)
    return ("count", float(text))

pod = yaml.safe_load(open(sys.argv[1]))
declared = yaml.safe_load(open(sys.argv[2]))["resourceGovernance"]["runtimeWorkloads"]
if len(declared) != 1:
    print(f"CHART 的 runtimeWorkloads 是 {len(declared)} 条，脚本只建一类 Pod：两边说的不是同一件事")
    sys.exit(0)
want = declared[0]
res = pod["spec"]["containers"][0].get("resources") or {}
pairs = [
    ("limits.cpu", want.get("cpuLimit"), (res.get("limits") or {}).get("cpu")),
    ("limits.memory", want.get("memoryLimit"), (res.get("limits") or {}).get("memory")),
    ("requests.cpu", want.get("cpuRequest"), (res.get("requests") or {}).get("cpu")),
    ("requests.memory", want.get("memoryRequest"), (res.get("requests") or {}).get("memory")),
]
bad_pairs = [f"{k}: chart={c!r} pod={p!r}" for k, c, p in pairs if c is None or p is None or q(c) != q(p)]
print("OK" if not bad_pairs else "；".join(bad_pairs))
PY
)
  case "$verdict" in
    OK) ok "Pod 的形状与 chart 的 runtimeWorkloads 逐项相同" ;;
    *) bad "Pod 形状与 chart 声明不一致：${verdict}" ;;
  esac
fi

# 7. 镜像**跟线上走**，不钉一个 rev。
#
#    2026-11-05 之前这里钉着一个 tag，理由是「检查点的身份里有 software，换镜像会整轮
#    bail」——那条理由在 2026-11-05 修掉了（`crates/cog-eval/src/rig.rs::same_experiment`
#    比材料时跳过 `software` 那一行），钉住的净值只剩副作用：钉着的 rev 会悄悄烂掉，而它
#    烂掉时没有任何读数会喊（2026-11-04 钉的 main-ac2748167396 在 registry 里根本不存在，
#    按它起的 Pod 只会 ImagePullBackOff，一轮都没跑起来过）。
#
#    所以这一格守两件事：默认读线上 deployment 的 image 字段；没有任何一处把 tag 写死
#    在镜像位置上（写死了就等于又钉上了，而它不会再有任何读数替你喊）。
pinline=$(grep -n '^IMAGE=\${IMAGE:-}$' "$SCRIPT" | head -1)
if [ -z "$pinline" ]; then
  bad "IMAGE 的默认值不是「跟线上走」（找不到 \`IMAGE=\${IMAGE:-}\` 那一行）"
elif ! grep -q 'get deployment' "$SCRIPT" || ! grep -q "jsonpath='{.spec.template.spec.containers\[0\].image}'" "$SCRIPT"; then
  bad "脚本没有从 deployment 的 image 字段取线上镜像"
elif grep -nE '^[[:space:]]*(image:|IMAGE=\$\{IMAGE:-[^}]+\})$' "$SCRIPT" | grep -qE 'main-[0-9a-f]{12}'; then
  bad "镜像位置上又写死了一个 main-<rev> tag：$(grep -nE '^[[:space:]]*(image:|IMAGE=\$\{IMAGE:-[^}]+\})$' "$SCRIPT" | grep -E 'main-[0-9a-f]{12}')"
else
  ok "镜像默认取线上 deployment 的那一个，没有写死的 rev"
fi

# 8. 登记的必须是**主表臂**：`--tools platform`，不是 `--tools none`。
#
#    诊断臂把工具面关掉，按 cogneva 自己的定义（`ToolArm::is_table_arm`）它不是主表行，
#    每一轮的退出理由里都写着「this run used the tools-off diagnostic arm, so it is not a
#    main-table row」——判词一直是对的，命令行却一直没改，于是轮次再多也进不了表。
#    （镜像里有没有 HLE 工具面以前靠比 rev 来判；现在跑的就是线上那一版，判它等于判
#    线上能不能出主表行——那是另一条面的读数，不在这份脚本的判据里。）
if [ "${pod_rendered:-0}" -eq 1 ]; then
  arm=$(python3 - "$TMP/pod.yaml" <<'PY' 2>&1
import sys, yaml
pod = yaml.safe_load(open(sys.argv[1]))
args = pod["spec"]["containers"][0]["args"]
try:
    print(args[args.index("--tools") + 1])
except ValueError:
    print("(清单里没有 --tools)")
PY
)
  case "$arm" in
    platform) ok "登记的命令行用的是主表臂 --tools platform" ;;
    *) bad "登记的臂是「$arm」：诊断臂跑多少轮都不会成为主表行" ;;
  esac
fi

printf '%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]

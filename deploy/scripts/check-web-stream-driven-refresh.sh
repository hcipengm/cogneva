#!/usr/bin/env bash
# 「帧到了，读快照的那几面会重读」——观测面对事件流的消费者门禁。
#
# 坏起来完全没有症状：观测面照样读得到，数字照样自洽，只是每一格都停在上一拍。
# 把轮询间隔调成十分钟，屏幕上「在岗 3」就一直是 3，而平台早就换了一拨人——帧
# 一直在总线上走，这一面一件事都不做。读者据此得出的是「平台没变」，不是「这一面
# 没接上总线」。
#
# 三种接法里两种坏：
#   - 每来一帧就重读：忙时一秒三帧，同一份字节连取几十遍，构建与集群同机，被拖慢
#     的是自己的门禁。
#   - 纯尾随去抖（每来一帧把定时器往后推）：常忙时帧不停，定时器永远到不了点，
#     这一面比纯轮询还旧——「帧驱动」做成了「帧饿死」。
#   - 前沿 + 地板：能立刻读就立刻读，两读之间至少隔 `MIN_GAP_MS`，间隔内的帧合并
#     成到期那一刻的一次读。忙时读的频次有上界，闲时第一帧就见效。
#
# 判据：
#   1. 判断是纯的：`store/frameTick.ts` 没有 import，能单文件编译（第 4 段在 node 里
#      按判据实跑）。
#   2. 总线真的报到：`hooks/useEventStream.ts` 在**心跳与正文增量的提前返回之后**
#      调用 `noteFaceTick()`，且只由「平台状态变过」那一位置位——那两种帧每个 token
#      一帧，让它们触发重读等于把轮询换成一种更密的轮询。
#   3. 画面真的接上：`views/ObservabilityView.tsx` 用 `useStreamDrivenRefresh(...)`
#      把概况与监管者/工作负载的 `refresh` 交出去。
#   4. 调度按判据跑：第一帧立刻读、间隔内合并、到点读、在途不重排、时钟回拨不许
#      把这一面静默停住；连打十秒 100 毫秒一帧的突发，既不许一次不读（饿死），也
#      不许按帧数读（风暴）。
#
# 用法：bash deploy/scripts/check-web-stream-driven-refresh.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
command -v node >/dev/null || { echo "缺少依赖：node" >&2; exit 2; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")

def read(rel):
    path = os.path.join(WEB, rel)
    if not os.path.isfile(path):
        sys.exit(f"读不到 web/src/{rel}")
    return open(path, encoding="utf-8").read()

pure = read("store/frameTick.ts")
if re.search(r"^\s*import\b", pure, re.M):
    sys.exit("store/frameTick.ts 有 import：它必须能单文件编译，否则第 4 段的判据跑不起来")
if not re.search(r"export function planRefresh\(", pure):
    sys.exit("store/frameTick.ts 没有 planRefresh：调度判断不是一段能单独跑的东西，第 4 段无从验")

hook_src = read("hooks/useStreamDrivenRefresh.ts")
if not re.search(r"planRefresh\(", hook_src):
    sys.exit(
        "hooks/useStreamDrivenRefresh.ts 没有调 planRefresh：这一面自己另判一套间隔，"
        "第 4 段验的就不是它真正用的那段判断了"
    )
if re.search(r"return\s*\(\)\s*=>\s*window\.clearTimeout\(pending\.current\)", hook_src):
    sys.exit(
        "hooks/useStreamDrivenRefresh.ts 在每次帧到达时都撤掉排在路上的那一发："
        "常忙时帧不停、定时器永远到不了点——「帧驱动」被做成了「帧饿死」，"
        "这一面会比纯轮询还旧"
    )

hook = read("hooks/useEventStream.ts")
bump = re.search(r"if \(changedPolledFace\) noteFaceTick\(\);", hook)
if not bump:
    sys.exit(
        "hooks/useEventStream.ts 没有 `if (changedPolledFace) noteFaceTick();`："
        "总线上变了而没有任何一面被告知，读快照的那几面要等下一拍轮询"
    )
quiet = re.search(
    r"if\s*\(event\.type === 'message_update' \|\| event\.type === 'heartbeat'\)", hook
)
if not quiet:
    sys.exit(
        "hooks/useEventStream.ts 里找不到「心跳与正文增量不入账」那句："
        "这个门禁判断不了上报点是不是在它之后"
    )
if bump.start() < quiet.start():
    sys.exit(
        "hooks/useEventStream.ts 的 `noteFaceTick()` 在提前返回之前："
        "心跳与正文增量每个 token 一帧，它们会把重读打成一种更密的轮询"
    )
# 置位点必须与「平台状态变了」那几处同源，不能被画布上正在流的那条消息带起来：
# 这五处之外的置位，都会让观测面跟着逐帧重读。
setters = len(re.findall(r"changedPolledFace = true;", hook))
if setters != 5:
    sys.exit(
        f"hooks/useEventStream.ts 里 `changedPolledFace = true;` 有 {setters} 处，判据要 5 处"
        "（执行者换态、告警越线、交接推进、项目面任务状态、任务状态帧）："
        "少一处那一面不会重读，多一处则有别的帧带起了重读"
    )

view = read("views/ObservabilityView.tsx")
for pattern, why in (
    (r"useStreamDrivenRefresh\(overview\.refresh\)", "观测面的概况没接到帧上：只靠 20 秒一拍，调成十分钟后这一面就不动了"),
    (
        r"useStreamDrivenRefresh\(\[supervisor\.refresh, components\.refresh\]\)",
        "监管者与工作负载两次读没一起接上：分两次读，画面会先变一半再变另一半",
    ),
):
    if not re.search(pattern, view):
        sys.exit(f"views/ObservabilityView.tsx 缺 {pattern}：{why}")

print("ok   - 调度是纯的、总线报了到、观测面接上了重读")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/store/frameTick.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/store/frameTick.ts 编译不过：调度判据无从检查" >&2
    exit 1
}
mv "$TMP/frameTick.js" "$TMP/frameTick.mjs"

node --input-type=module - "$TMP/frameTick.mjs" <<'JS'
const { MIN_GAP_MS, planRefresh } = await import(process.argv[2]);
const fail = (msg) => {
  console.error(`帧触发重读的调度不对：${msg}`);
  process.exit(1);
};

if (!(MIN_GAP_MS > 0)) fail(`两读之间的地板是 ${MIN_GAP_MS} 毫秒：按帧数读就成了风暴`);
if (MIN_GAP_MS > 30_000) fail(`地板 ${MIN_GAP_MS} 毫秒太宽：闲下来之后第一帧也要等半分钟，这就不叫「帧到了画面动」`);

// 1. 第一帧：还没有过读数，立刻读
let p = planRefresh(10_000, null, false);
if (p.action !== 'now') fail(`第一帧该立刻读：${JSON.stringify(p)}`);

// 2. 地板之内：等剩下的时间，不是直接读、也不是丢掉
p = planRefresh(10_500, 10_000, false);
if (p.action !== 'wait') fail(`间隔内该等：${JSON.stringify(p)}`);
if (p.waitMs !== MIN_GAP_MS - 500) fail(`该等 ${MIN_GAP_MS - 500} 毫秒：${p.waitMs}`);

// 3. 到点：立刻读
p = planRefresh(10_000 + MIN_GAP_MS, 10_000, false);
if (p.action !== 'now') fail(`到点该读：${JSON.stringify(p)}`);

// 4. 已经有一发在路上：不许再排一发
p = planRefresh(10_000 + MIN_GAP_MS + 1, 10_000, true);
if (p.action !== 'skip') fail(`在途时该跳过，否则十秒里能排出几十个定时器一起响：${JSON.stringify(p)}`);

// 5. 时钟往回拨：立刻读，不许等一个负的间隔（那是一个永不到期的定时器）
p = planRefresh(9_000, 10_000, false);
if (p.action !== 'now') fail(`时钟回拨该立刻读：${JSON.stringify(p)}——等下去这一面会静默停住`);

// 6. 读不出上一次的时刻：按「没读过」处理
p = planRefresh(10_000, Number.NaN, false);
if (p.action !== 'now') fail(`上一次时刻读不出来时该立刻读：${JSON.stringify(p)}`);

// 7. 突发：每 100 毫秒一帧，连打十秒。既不许一次不读（饿死），也不许按帧数读（风暴）。
//    这里跑的是 hook 真正用的那段判断：now 就记一次读，wait 就排一发到点读，skip 什么都不做。
const TICK_MS = 100;
const SPAN_MS = 10_000;
let last = null;
let pendingAt = null;
let reads = 0;
for (let t = 0; t <= SPAN_MS; t += TICK_MS) {
  if (pendingAt !== null && t >= pendingAt) {
    pendingAt = null;
    last = t;
    reads += 1;
  }
  const plan = planRefresh(t, last, pendingAt !== null);
  if (plan.action === 'now') {
    last = t;
    reads += 1;
  } else if (plan.action === 'wait') {
    pendingAt = t + plan.waitMs;
  }
}
const floor = Math.floor(SPAN_MS / MIN_GAP_MS);
if (reads < 2) fail(`十秒突发只读了 ${reads} 次：帧把这一面饿死了`);
if (reads > floor + 2) fail(`十秒突发读了 ${reads} 次（地板 ${MIN_GAP_MS} 毫秒最多 ${floor} 次）：按帧数读了`);

// 8. 同一瞬间来一批帧：合并成一发
let burstReads = 0;
last = null;
pendingAt = null;
for (let i = 0; i < 50; i += 1) {
  const plan = planRefresh(5_000, last, pendingAt !== null);
  if (plan.action === 'now') {
    last = 5_000;
    burstReads += 1;
  } else if (plan.action === 'wait') {
    pendingAt = 5_000 + plan.waitMs;
  }
}
if (burstReads !== 1) fail(`同一瞬间的 50 帧该合并成 1 次读：读了 ${burstReads} 次`);

console.log('ok   - 前沿与地板、在途合并、时钟回拨、突发下的读次上界都成立');
JS

echo "check-web-stream-driven-refresh: 通过"

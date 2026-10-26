#!/usr/bin/env bash
# 「连接开着」不等于「总线在说话」——状态条那一格的消费者门禁。
#
# 状态条过去只写「实时流 已连接」。断开看得见，可**连着而一帧都不来**看不见：平台
# 停摆的时候屏幕上一切都是静止的，而没有任何一格说「总线已经十分钟没推来东西了」，
# 读者的结论会是「平台很闲」，不是「发帧的那一端停了」。这两个状态在画面上完全
# 一样，没有一格区分，等于把判断交给了猜。
#
# 判据：
#   1. 折叠函数是纯的：`store/livePulse.ts` 没有 import，能单文件编译（第 4 段在
#      node 里按判据实跑）。
#   2. 记的是**到达**，不是帧里的时间：模块里不出现 `.timestamp` / `Date.parse`。
#      重连后服务端会重放一串旧帧，按帧里的时间算，状态条会把「刚刚在动」读成
#      「半小时没动」——正好把要判断的事说反。
#   3. 分派点在**所有提前返回之前**：`noteArrival(Date.now())` 必须出现在
#      「心跳与正文增量不入账」那句之前。那两种帧正是「平台在干活」最密的一类，
#      漏掉它们，忙碌与停摆都成了「一帧没有」。
#   4. 画面真的读它，而且**有自己的一口钟**：`shell/StatusBar.tsx` 用
#      `pulseVerdict(pulse, now, connectionStatus)` 决定那一格的颜色、正文与悬停
#      解释，`now` 来自 `useNow(`——只靠帧触发的重渲染，在帧不来的时候一次都不
#      重渲染，读数会永远停在「刚刚」，而它正是用来说「已经很久没有帧了」的。
#   5. 四种情形分开说：没连上、连上但一帧没到、连上但超时无帧、有帧；每种都要带
#      一句解释，判词与读数对不上时读者至少知道该去查哪一头。
#
# 用法：bash deploy/scripts/check-web-live-pulse.sh [仓库根目录]
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

pure = read("store/livePulse.ts")
if re.search(r"^\s*import\b", pure, re.M):
    sys.exit("store/livePulse.ts 有 import：它必须能单文件编译，否则第 4 段的判据跑不起来")
# 形状而不是裸子串：注释里写着「不是帧里的 timestamp」是正经解释，不算违规。
if re.search(r"\.timestamp\b|Date\.parse\(", pure):
    sys.exit(
        "store/livePulse.ts 在读帧里的时间（`.timestamp` / `Date.parse`）："
        "这一格数的是到达。重连重放旧帧时按帧里的时间算，会把「刚刚在动」读成「很久没动」"
    )

hook = read("hooks/useEventStream.ts")
arrive = re.search(r"noteArrival\(Date\.now\(\)\)", hook)
if not arrive:
    sys.exit(
        "hooks/useEventStream.ts 没有记帧到达（`noteArrival(Date.now())`）："
        "状态条那一格只能看连接状态，连着而不来帧时与平台真的没事长得一模一样"
    )
quiet = re.search(r"if\s*\(event\.type === 'message_update' \|\| event\.type === 'heartbeat'\)", hook)
if not quiet:
    sys.exit(
        "hooks/useEventStream.ts 里找不到「心跳与正文增量不入账」那句："
        "这个门禁判断不了到达记录是不是在它之前，被提前返回挡掉就少算了最密的那几种帧"
    )
if arrive.start() > quiet.start():
    sys.exit(
        "hooks/useEventStream.ts 的到达记录在提前返回之后："
        "心跳与正文增量这两种帧被 return 挡住，数不到——平台最忙的时候这一格反而最静"
    )

bar = read("shell/StatusBar.tsx")
for pattern, why in (
    (r"useLivePulseStore\(\(", "状态条没有从 live store 取到达记录：记了没人读"),
    (r"pulseVerdict\(pulse, now, connectionStatus\)", "那一格没有按判据算颜色与正文"),
    (r"useNow\(", "那一格没有自己的钟：帧不来时它一次都不重渲染，读数永远停在「刚刚」"),
    (r"stream\.text", "算出判词却不显示"),
    (r"tone=\{stream\.color\}", "判词的颜色没接到那一格上：超时无帧与有帧会同色"),
):
    if not re.search(pattern, bar):
        sys.exit(f"shell/StatusBar.tsx 缺 {pattern}：{why}")

print("ok   - 折叠、到达记录的位置、状态条的钟与判词接线齐全")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/store/livePulse.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/store/livePulse.ts 编译不过：折叠的判据无从检查" >&2
    exit 1
}
mv "$TMP/livePulse.js" "$TMP/livePulse.mjs"

node --input-type=module - "$TMP/livePulse.mjs" <<'JS'
const {
  EMPTY_PULSE,
  foldArrival,
  framesPerMinute,
  humanAge,
  pulseVerdict,
  PULSE_STALE_MS,
} = await import(process.argv[2]);
const fail = (msg) => {
  console.error(`帧到达读数不对：${msg}`);
  process.exit(1);
};

// 1. 第一帧：首末都落在它身上，一共一帧
let p = foldArrival(EMPTY_PULSE, 1_000);
if (p.total !== 1) fail(`第一帧没记上：${p.total}`);
if (p.firstAtMs !== 1_000 || p.lastAtMs !== 1_000) fail(`第一帧没同时当首末：${JSON.stringify(p)}`);

// 2. 再来一帧：首不动、末跟着走
p = foldArrival(p, 11_000);
if (p.total !== 2) fail('第二帧没累加');
if (p.firstAtMs !== 1_000) fail('第二帧把「第一帧」的时刻推后了');
if (p.lastAtMs !== 11_000) fail('第二帧没推「最近一帧」');

// 3. 窗口按到达时刻数，过期的不再算进去
if (framesPerMinute(p, 21_000) !== 2) fail('刚过去 20 秒，两帧都该在窗口里');
if (framesPerMinute(p, 61_001) !== 1) fail('第一帧已经过了 60 秒，窗口里不该还算它');
if (framesPerMinute(p, 200_000) !== 0) fail('早就过期的帧不该留在「最近一分钟」里');

// 4. 窗口外的样本被裁掉，但「一共到了多少帧」不裁
p = foldArrival(p, 200_000);
if (p.recent.length !== 1) fail(`窗口外的样本没裁掉：${p.recent.length}`);
if (p.total !== 3) fail(`总数被窗口一起裁掉了：${p.total}`);

// 5. 时钟往回拨：末次不倒退，总数照加
const back = foldArrival(p, 100_000);
if (back.lastAtMs !== 200_000) fail(`时钟回拨把「最近一帧」拉回去了：${back.lastAtMs}`);
if (back.total !== 4) fail('回拨后的那一帧没计数');

// 6. 读不出时刻的到达不记
const same = foldArrival(p, Number.NaN);
if (same !== p) fail('NaN 不该改动读数');

// 7. 时长说法
if (humanAge(999) !== '刚刚') fail(`不到一秒应说「刚刚」：${humanAge(999)}`);
if (humanAge(1_000) !== '1 秒前') fail(`一秒应说「1 秒前」：${humanAge(1_000)}`);
if (humanAge(125_000) !== '2 分前') fail(`两分多应说「2 分前」：${humanAge(125_000)}`);

// 8. 四种情形分开说
const v = (state, now, connected) => pulseVerdict(state, now, connected);
const colorMatches = (verdict) =>
  verdict.color ===
  { ok: 'var(--ok)', warn: 'var(--warn)', mute: 'var(--muted)' }[verdict.tone];

const closed = v(p, 200_000, 'closed');
if (closed.tone !== 'warn' || closed.text !== '已断开') fail(`断开时该说「已断开」：${closed.text}`);
if (!colorMatches(closed)) fail(`断开时的颜色与判词对不上：${closed.tone} / ${closed.color}`);
if (!closed.title) fail('断开时没有解释');
if (v(p, 200_000, 'connecting').text !== '连接中') fail('连接中没说「连接中」');
if (v(p, 200_000, 'error').text !== '连不上') fail('连不上没说「连不上」');

const none = v(EMPTY_PULSE, 200_000, 'open');
if (none.tone !== 'mute') fail('连上但一帧没到不该是绿的：那会把「没有帧」说成「一切正常」');
if (!none.text.includes('还没有一帧')) fail(`一帧没到时应说「还没有一帧」：${none.text}`);
if (!none.title) fail('一帧没到时没有解释');
if (!colorMatches(none)) fail(`一帧没到时的颜色与判词对不上：${none.tone} / ${none.color}`);

const stale = v(p, 200_000 + PULSE_STALE_MS, 'open');
if (stale.tone !== 'warn') fail('超时无帧时该报警色');
if (!stale.text.includes('最近一帧在')) fail(`超时无帧应说清多久了：${stale.text}`);
if (!stale.text.includes('2 分前')) fail(`两分钟没帧应读成「2 分前」：${stale.text}`);
if (!stale.title.includes('2 分前')) fail('超时无帧的解释里没说多久');
if (!colorMatches(stale)) fail(`超时无帧的颜色与判词对不上：${stale.tone} / ${stale.color}`);

const live = v(p, 200_000 + PULSE_STALE_MS - 1, 'open');
if (live.tone !== 'ok') fail('还差一毫秒到阈值就报警色：阈值不该提前亮');
if (!live.text.includes('帧/分')) fail(`有帧时该报帧率：${live.text}`);
if (!live.title) fail('有帧时没有解释');
if (!colorMatches(live)) fail(`有帧时的颜色与判词对不上：${live.tone} / ${live.color}`);

// 9. 刚连上的窗口还没走满时只报实数，不外推成「帧/分」
const fresh = v(foldArrival(EMPTY_PULSE, 100_000), 110_000, 'open');
if (fresh.text.includes('帧/分')) fail(`刚连上就报「帧/分」是把十秒的帧数说成一分钟的：${fresh.text}`);
if (!fresh.text.includes('刚 10 秒来了 1 帧')) fail(`刚连上该报实数：${fresh.text}`);

console.log('ok   - 到达口径、窗口、回拨、四种判词与阈值边界都成立');
JS

echo "check-web-live-pulse: 通过"

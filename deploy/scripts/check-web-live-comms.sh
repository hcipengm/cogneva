#!/usr/bin/env bash
# 「这一面是事件流的回音，不是每 15 秒拍一张照片」——通信面的消费者门禁。
#
# 坏起来没有症状：交接图照样读得到，边照样画得出，只是它比真相旧最多 15 秒。而让图
# 长出边的那些事——任务完成、任务失败——一直在总线上走，到了这一面却一条边都不动。
# 这一面问的正是「这条链现在走到哪了」，晚一拍就是把「刚交出去的活」显示成还没交。
#
# 判据：
#   1. 折叠函数是纯的：`store/liveComms.ts` 没有 import，能单文件编译（下面第 4 段
#      就是编译出来在 node 里跑的）。
#   2. 帧真的被折进去：`hooks/useEventStream.ts` 的分派点调用 `commsFrame` 与
#      `noteCommsFrame`，而不是只在某处定义了一个没人用的函数。
#   3. 画面真的读它：`views/CommsView.tsx` 从 live store 取行、按
#      `outcomesSinceSnapshot(graph.at, …)` 判「这一帧是不是比这张图晚」、把那些
#      结局渲染出来，并且标出来路——不标的话，读者会去数一个帧报不出的条数。
#   4. 语义按判据实跑：不是结局的状态不折、`hook_event` 不折、重连重放的旧帧不许把
#      时刻翻回去、比快照早的帧不算「图里还没有」。
#
# 用法：bash deploy/scripts/check-web-live-comms.sh [仓库根目录]
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

pure = read("store/liveComms.ts")
if re.search(r"^\s*import\b", pure, re.M):
    sys.exit("store/liveComms.ts 有 import：它必须能单文件编译，否则第 4 段的判据跑不起来")

hook = read("hooks/useEventStream.ts")
if "commsFrame" not in hook or "noteCommsFrame" not in hook:
    sys.exit(
        "hooks/useEventStream.ts 没有把结局帧折进去：总线上有 task_status_change，"
        "交接图上却没有任何东西因此改变"
    )
if not re.search(r"const\s+liveComms\s*=\s*commsFrame\(event\)", hook):
    sys.exit("hooks/useEventStream.ts 定义了却没用 commsFrame(event)：折的是别的什么东西")
if not re.search(r"noteCommsFrame\(liveComms\)", hook):
    sys.exit("hooks/useEventStream.ts 算出帧却不写进 live store")

view = read("views/CommsView.tsx")
# 判的是**调用形状**，不是文件里出现过某个名字：只留 import 和注释时，名字照样在，
# 而屏幕上一格都不会变（2026-10-26 拿删掉调用的变异验过）。
for pattern, why in (
    (r"useLiveCommsStore\(\(", "画面没有从 live store 取行：折进来了没人读"),
    (r"outcomesSinceSnapshot\(graph\.at,", "画面没有拿这张图的时刻去判「这一帧是不是更晚」"),
    (r"<FreshOutcomes rows=\{fresh\}", "算出「图里还没有的那几个结局」却不渲染：读者仍然要等下一拍轮询"),
    (r"比这张图更晚，图还没画", "那一块没有标来路：读者会把它当成图上已经画出来的边"),
):
    if not re.search(pattern, view):
        sys.exit(f"views/CommsView.tsx 缺 {pattern}：{why}")

print("ok   - 折叠、分派、画面三处接线齐全")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/store/liveComms.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/store/liveComms.ts 编译不过：折叠的判据无从检查" >&2
    exit 1
}
mv "$TMP/liveComms.js" "$TMP/liveComms.mjs"

node --input-type=module - "$TMP/liveComms.mjs" <<'JS'
const { commsFrame, foldCommsFrame, pruneComms, outcomesSinceSnapshot } = await import(
  process.argv[2]
);
const fail = (msg) => {
  console.error(`结局帧折叠不对：${msg}`);
  process.exit(1);
};

const T = (s) => `2026-10-26T06:00:${String(s).padStart(2, '0')}Z`;
const ev = (type, payload, at = T(0)) => ({ type, timestamp: at, payload });
const outcome = (task, status, at = T(0), extra = {}) =>
  ev('task_status_change', { task_id: task, status, ...extra }, at);

// 1. 两种结局各折成什么边
let f = commsFrame(outcome('t1', 'completed', T(1)));
if (!f) fail('一条完成帧没折出来');
if (f.key !== 't1::completed') fail(`结局的身份错了：${f.key}`);
if (f.edge !== 'handoff') fail(`完成该带出交接边，折成了 ${f.edge}`);
if (f.agentId !== null) fail('没带 agent_id 时不该编一个出来');
f = commsFrame(outcome('t1', 'failed', T(1)));
if (f.edge !== 'retry_or_dead_letter') fail(`失败该带出重试/死信边，折成了 ${f.edge}`);

// 2. 不产生边的状态一律不折——折了只会让这一块在没人交接时也在动
for (const s of ['pending', 'scheduled', 'running', 'retried', 'cancelled', 'timeout', 'lease_expired']) {
  if (commsFrame(outcome('t1', s, T(1))) !== null) fail(`状态 ${s} 不该折成「图上要长边」`);
}
if (commsFrame(outcome('t1', 'hook_event', T(1))) !== null) fail('hook_event 借这个变体过总线，不是任务在换状态');
if (commsFrame(ev('task_status_change', { status: 'completed' }, T(1))) !== null) {
  fail('没有 task_id 的帧不该折');
}
if (commsFrame(ev('agent_end', { task_id: 't1', status: 'completed' }, T(1))) !== null) {
  fail('不是 task_status_change 的帧不该折');
}

// 3. 第一帧给首末时刻；再来一帧只推最近的那一头
let r = foldCommsFrame(undefined, commsFrame(outcome('t1', 'completed', T(1), { agent_id: 'a1' })));
if (r.firstAt !== T(1) || r.lastAt !== T(1) || r.frames !== 1) fail(`第一帧的记账不对：${JSON.stringify(r)}`);
if (r.agentId !== 'a1') fail('带着执行者的一帧没把它留住');
r = foldCommsFrame(r, commsFrame(outcome('t1', 'completed', T(2))));
if (r.firstAt !== T(1)) fail('再来一帧把「第一次见到」的时刻推后了');
if (r.lastAt !== T(2)) fail('再来一帧没推「最近一次」的时刻');
if (r.frames !== 2) fail(`帧数没累加：${r.frames}`);
if (r.agentId !== 'a1') fail('后一帧没带执行者时不该把先前记的那个抹掉');

// 4. 重连重放：更旧的帧不许把时刻拉回去
const replayed = foldCommsFrame(r, commsFrame(outcome('t1', 'completed', T(0))));
if (replayed.lastAt !== T(2)) fail('旧帧把「最近一次」的时刻拉回去了');
if (replayed.frames !== 2) fail('重放不算新的一帧，帧数却不该动');

// 5. 同一个任务的两种结局是两行：它们带来的是两种不同的边
const both = {
  ...{ [r.key]: r },
  ...{ 't1::failed': foldCommsFrame(undefined, commsFrame(outcome('t1', 'failed', T(3)))) },
};
if (Object.keys(both).length !== 2) fail('完成与失败被并成了一行');

// 6. 裁剪按「最近一次」留
const many = {};
for (let i = 0; i < 5; i += 1) {
  const one = commsFrame(outcome(`t${i}`, 'completed', T(i)));
  many[one.key] = foldCommsFrame(undefined, one);
}
const kept = pruneComms(many, 2);
if (Object.keys(kept).length !== 2) fail('裁剪没按上限留');
if (!kept['t4::completed'] || !kept['t3::completed']) fail(`裁掉的不是最旧的那几个：${Object.keys(kept)}`);

// 7. 「比这张图更晚」才是「图里还没有」
const rows = {
  't1::completed': foldCommsFrame(undefined, commsFrame(outcome('t1', 'completed', T(5)))),
  't2::failed': foldCommsFrame(undefined, commsFrame(outcome('t2', 'failed', T(1)))),
};
let fresh = outcomesSinceSnapshot(T(3), rows);
if (fresh.length !== 1 || fresh[0].key !== 't1::completed') {
  fail(`比快照更晚的没挑对：${JSON.stringify(fresh.map((x) => x.key))}`);
}
fresh = outcomesSinceSnapshot(T(9), rows);
if (fresh.length !== 0) fail('比快照早的帧被当成了「图里还没有」：重读之后它自己就会消失');
fresh = outcomesSinceSnapshot(null, rows);
if (fresh.length !== 2) fail('还没成功读过图时不许把这些结局藏起来');
if (fresh[0].key !== 't1::completed') fail('没有快照时刻时应按最近一次排序，新的在前');
fresh = outcomesSinceSnapshot('不是时间', rows);
if (fresh.length !== 2) fail('快照时刻读不出来时应按「帧更晚」算，而不是把帧扔掉');

console.log('ok   - 两种结局、不产边的状态不折、重放不清旧、两种结局分行、来路判据都成立');
JS

echo "check-web-live-comms: 通过"

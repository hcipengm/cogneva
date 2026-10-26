#!/usr/bin/env bash
# 「这一面是事件流的回音，不是每 15 秒拍一张照片」——执行者面的消费者门禁。
#
# 坏起来没有症状：名单照样读得到，表照样画得出，只是每一格都比真相旧最多 15 秒。
# 一个人刚起来、刚换状态、刚报错，在总线上都到了，在这一面上一件事都不改。这种
# 面看上去完全正常，只有坐在前面等它动的人会发现它从来不动。
#
# 判据：
#   1. 折叠函数是纯的：`store/liveAgents.ts` 没有 import，能单文件编译（下面第 4 段
#      就是编译出来在 node 里跑的）。
#   2. 帧真的被折进去：`hooks/useEventStream.ts` 的分派点调用 `agentFrame` 与
#      `noteAgentFrame`，而不是只在某处定义了一个没人用的函数。
#   3. 画面真的读它：`views/AgentsView.tsx` 从 live store 取行、按 `rosterWithFrames`
#      合成表格，并且把「帧」这个来路标出来——不标的话，读者无法分清一格是刚才
#      那帧说的还是十几秒前那次轮询说的。
#   4. 语义按判据实跑：状态帧改状态、心跳只推「还在」、「重连重放的旧帧不许把状态
#      翻回去」、错误之后再换状态要清掉错误原文、名单里还没有的人单列。
#
# 用法：bash deploy/scripts/check-web-live-frames.sh [仓库根目录]
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

pure = read("store/liveAgents.ts")
if re.search(r"^\s*import\b", pure, re.M):
    sys.exit("store/liveAgents.ts 有 import：它必须能单文件编译，否则第 4 段的判据跑不起来")

hook = read("hooks/useEventStream.ts")
if "agentFrame" not in hook or "noteAgentFrame" not in hook:
    sys.exit(
        "hooks/useEventStream.ts 没有把执行者帧折进去：总线上有这几种帧，"
        "执行者面上却没有任何东西因此改变"
    )
if not re.search(r"const\s+liveAgent\s*=\s*agentFrame\(event\)", hook):
    sys.exit("hooks/useEventStream.ts 定义了却没用 agentFrame(event)：折的是别的什么东西")
if not re.search(r"noteAgentFrame\(liveAgent\)", hook):
    sys.exit("hooks/useEventStream.ts 算出帧却不写进 live store")

view = read("views/AgentsView.tsx")
# 判的是**调用形状**，不是文件里出现过某个名字：只留 import 和注释时，名字照样在，
# 而屏幕上一格都不会变（2026-10-26 拿删掉调用、只留 import 的变异验过）。
for pattern, why in (
    (r"useLiveAgentStore\(\(", "画面没有从 live store 取行：折进来了没人读"),
    (r"rosterWithFrames\(agents,", "画面没有按名单＋帧合成：帧与快照各画一张表"),
    (r"seenAfterSnapshot\(", "画面没有判「这一帧是不是比快照更晚」"),
    (r"比这次轮询读到的快照更晚", "状态格没有标出来路：读者分不清这一格是帧说的还是那次轮询说的"),
):
    if not re.search(pattern, view):
        sys.exit(f"views/AgentsView.tsx 缺 {pattern}：{why}")

print("ok   - 折叠、分派、画面三处接线齐全")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/store/liveAgents.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/store/liveAgents.ts 编译不过：折叠的判据无从检查" >&2
    exit 1
}
mv "$TMP/liveAgents.js" "$TMP/liveAgents.mjs"

node --input-type=module - "$TMP/liveAgents.mjs" <<'JS'
const { agentFrame, foldAgentFrame, rosterWithFrames, seenAfterSnapshot } = await import(
  process.argv[2]
);
const fail = (msg) => {
  console.error(`执行者帧折叠不对：${msg}`);
  process.exit(1);
};

const T = (s) => `2026-10-26T06:00:${String(s).padStart(2, '0')}Z`;
const ev = (type, agent_id, extra = {}, at = T(0)) => ({
  type,
  timestamp: at,
  payload: { agent_id, ...extra },
});

// 1. 五类帧各折成什么
let r = foldAgentFrame(undefined, agentFrame(ev('agent_start', 'a1', {}, T(1))));
if (r.state !== 'started' || r.stateFrame !== 'agent_start') fail(`agent_start 折成 ${JSON.stringify(r)}`);
r = foldAgentFrame(r, agentFrame(ev('state_change', 'a1', { from: 'started', to: 'running' }, T(2))));
if (r.state !== 'running') fail(`state_change 没换状态：${r.state}`);
if (r.seenAt !== T(2) || r.stateAt !== T(2)) fail('状态帧要同时推两个时刻');

// 2. 心跳只推「还在」，不许把状态翻回别的值
r = foldAgentFrame(r, agentFrame(ev('heartbeat', 'a1', {}, T(3))));
if (r.state !== 'running') fail(`心跳把状态改成了 ${r.state}：一条心跳只说明它还在`);
if (r.seenAt !== T(3)) fail('心跳没有推进「最后一次心跳」的时刻');
if (r.stateAt !== T(2)) fail('心跳不该推进状态自己的时刻');

// 3. 重连重放：比状态更旧的帧不许把它翻回去
r = foldAgentFrame(r, agentFrame(ev('state_change', 'a1', { to: 'started' }, T(1))));
if (r.state !== 'running') fail(`旧帧把状态倒回了 ${r.state}——一次从未发生的回退`);
if (r.seenAt !== T(3)) fail('旧帧不该把「还在」的时刻拉回去');

// 4. 错误带原文；之后换状态要把它清掉
r = foldAgentFrame(r, agentFrame(ev('agent_error', 'a1', { severity: 'critical', details: 'upstream 502' }, T(4))));
if (r.state !== 'error') fail('agent_error 没落成 error');
if (!r.detail || !r.detail.includes('upstream 502')) fail(`错误原文没留住：${r.detail}`);
r = foldAgentFrame(r, agentFrame(ev('state_change', 'a1', { to: 'running' }, T(5))));
if (r.state !== 'running') fail('错误之后换状态没生效');
if (r.detail !== null) fail('换过状态之后还挂着上一条错误原文：读者会以为这条错还在');

// 5. 结束之后再启动：要能回来
r = foldAgentFrame(r, agentFrame(ev('agent_end', 'a1', {}, T(6))));
if (r.state !== 'ended') fail('agent_end 没落成 ended');
r = foldAgentFrame(r, agentFrame(ev('agent_start', 'a1', {}, T(7))));
if (r.state !== 'started') fail('重新启动之后状态没回来');

// 6. 不是执行者的帧、没有 agent_id 的帧、没有 to 的 state_change：一律不折
if (agentFrame(ev('tool_execution_start', 'a1', {}, T(1))) !== null) fail('工具帧不该折成执行者状态');
if (agentFrame({ type: 'agent_start', timestamp: T(1), payload: {} }) !== null) fail('没有 agent_id 的帧不该折');
if (agentFrame(ev('state_change', 'a1', { from: 'running' }, T(1))) !== null) {
  fail('没有 to 的 state_change 不该折出一个空状态');
}

// 7. 名单与帧合成：名单顺序不动、名单里的人挂上帧、名单外的人单列（已结束的不单列）
const live = {
  a1: foldAgentFrame(undefined, agentFrame(ev('state_change', 'a1', { to: 'running' }, T(2)))),
  a2: foldAgentFrame(undefined, agentFrame(ev('agent_start', 'a2', {}, T(3)))),
  a3: foldAgentFrame(undefined, agentFrame(ev('agent_end', 'a3', {}, T(4)))),
};
const out = rosterWithFrames([{ agent_id: 'a1' }, { agent_id: 'z9' }], live);
if (out.rows.length !== 2) fail('名单行数被帧改掉了');
if (out.rows[0].row.agent_id !== 'a1' || out.rows[1].row.agent_id !== 'z9') fail('名单顺序被改掉了');
if (!out.rows[0].live || out.rows[0].live.state !== 'running') fail('名单里的那一行没挂上帧');
if (out.rows[1].live !== null) fail('名单里没有帧的行必须是 null，不能编一行');
if (out.unseen.length !== 1 || out.unseen[0].agentId !== 'a2') {
  fail(`名单外的帧单列错了：${JSON.stringify(out.unseen.map((x) => x.agentId))}（已结束的不该单列）`);
}

// 8. 「帧」那个徽记的判据：帧比快照晚才算
if (!seenAfterSnapshot(T(1), live.a1)) fail('更晚的帧没被认出来');
if (seenAfterSnapshot(T(9), live.a1)) fail('比快照早的帧被当成了更新的');
if (seenAfterSnapshot(T(1), null)) fail('没有帧时不许标「帧」');
if (!seenAfterSnapshot('不是时间', live.a1)) fail('快照时刻读不出来时应按帧更晚算，而不是把帧扔掉');

console.log('ok   - 状态/心跳分离、重放不清旧、错误原文、名单合成、来路判据都成立');
JS

echo "check-web-live-frames: 通过"

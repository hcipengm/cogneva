#!/usr/bin/env bash
# 「这一面是事件流的回音，不是每 30 秒拍一张照片」——告警面的消费者门禁。
#
# 坏起来没有症状：表照样读得到，条数照样对，只是每一格都比真相旧最多 30 秒。对
# 告警面来说这 30 秒不是延迟，是代价——它是出事时才有人打开的那一面，晚一拍就是
# 把这一拍还给了出事的那一方。而且总线上的 `resource_alert` 一直在走，这一面上
# 却一件事都不改，那这一面就不是事件流的回音。
#
# 判据：
#   1. 折叠函数是纯的：`store/liveAlerts.ts` 没有 import，能单文件编译（下面第 4 段
#      就是编译出来在 node 里跑的）。
#   2. 帧真的被折进去：`hooks/useEventStream.ts` 的分派点调用 `alertFrame` 与
#      `noteAlertFrame`，而不是只在某处定义了一个没人用的函数。
#   3. 画面真的读它：`views/AlertsView.tsx` 从 live store 取行、按
#      `breachesSinceSnapshot(reader.at, …)` 判「这一帧是不是比快照晚」、把那几条
#      渲染出来，并且标出来路——不标的话，读者会以为那是一行可以去确认的告警，而
#      它背后此刻还没有行。
#   4. 语义按判据实跑：不是 `resource_alert` 的不折、说不清越到哪儿的不折、
#      重连重放的旧帧不许把数值翻回去、比快照早的帧不算「表里没有」。
#
# 用法：bash deploy/scripts/check-web-live-alerts.sh [仓库根目录]
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

pure = read("store/liveAlerts.ts")
if re.search(r"^\s*import\b", pure, re.M):
    sys.exit("store/liveAlerts.ts 有 import：它必须能单文件编译，否则第 4 段的判据跑不起来")

hook = read("hooks/useEventStream.ts")
if "alertFrame" not in hook or "noteAlertFrame" not in hook:
    sys.exit(
        "hooks/useEventStream.ts 没有把越界帧折进去：总线上有 resource_alert，"
        "告警面上却没有任何东西因此改变"
    )
if not re.search(r"const\s+liveAlert\s*=\s*alertFrame\(event\)", hook):
    sys.exit("hooks/useEventStream.ts 定义了却没用 alertFrame(event)：折的是别的什么东西")
if not re.search(r"noteAlertFrame\(liveAlert\)", hook):
    sys.exit("hooks/useEventStream.ts 算出帧却不写进 live store")

view = read("views/AlertsView.tsx")
# 判的是**调用形状**，不是文件里出现过某个名字：只留 import 和注释时，名字照样在，
# 而屏幕上一格都不会变（2026-10-26 拿删掉调用的变异验过）。
for pattern, why in (
    (r"useLiveAlertStore\(\(", "画面没有从 live store 取行：折进来了没人读"),
    (r"breachesSinceSnapshot\(reader\.at,", "画面没有拿这份快照的时刻去判「这一帧是不是更晚」"),
    (r"<FreshBreaches rows=\{fresh\}", "算出「表里还没有的那几次」却不渲染：读者仍然要等下一拍轮询"),
    (r"比这张表更晚，表里还读不到", "那一块没有标来路：读者会把它当成一行可以去确认的告警"),
):
    if not re.search(pattern, view):
        sys.exit(f"views/AlertsView.tsx 缺 {pattern}：{why}")

print("ok   - 折叠、分派、画面三处接线齐全")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/store/liveAlerts.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/store/liveAlerts.ts 编译不过：折叠的判据无从检查" >&2
    exit 1
}
mv "$TMP/liveAlerts.js" "$TMP/liveAlerts.mjs"

node --input-type=module - "$TMP/liveAlerts.mjs" <<'JS'
const { alertFrame, foldAlertFrame, pruneAlerts, breachesSinceSnapshot } = await import(
  process.argv[2]
);
const fail = (msg) => {
  console.error(`越界帧折叠不对：${msg}`);
  process.exit(1);
};

const T = (s) => `2026-10-26T06:00:${String(s).padStart(2, '0')}Z`;
const ev = (type, payload, at = T(0)) => ({ type, timestamp: at, payload });
const breach = (agent, metric, threshold, current, at = T(0), extra = {}) =>
  ev('resource_alert', { agent_id: agent, metric, threshold, current, ...extra }, at);

// 1. 身份：同一个人同一项指标算同一次越界
let f = alertFrame(breach('a1', 'cpu', 0.9, 0.93, T(1)));
if (!f) fail('一条完整的越界帧没折出来');
if (f.key !== 'a1::cpu') fail(`越界的身份错了：${f.key}`);
if (f.threshold !== 0.9 || f.current !== 0.93) fail(`阈值/实测值没留住：${JSON.stringify(f)}`);
if (f.crewId !== null) fail('没带 crew_id 时不该编一个出来');

// 2. 说不清越到哪儿的帧一律不折
if (alertFrame(breach('a1', 'cpu', 0.9, undefined)) !== null) fail('没有实测值的帧不该折');
if (alertFrame(breach('a1', 'cpu', undefined, 0.93)) !== null) fail('没有阈值的帧不该折');
if (alertFrame(ev('resource_alert', { agent_id: 'a1', threshold: 0.9, current: 0.93 })) !== null) {
  fail('没有 metric 的帧不该折');
}
if (alertFrame(ev('resource_alert', { metric: 'cpu', threshold: 0.9, current: 0.93 })) !== null) {
  fail('没有 agent_id 的帧不该折');
}
if (alertFrame(ev('heartbeat', { agent_id: 'a1' }, T(1))) !== null) fail('心跳不该折成越界');
if (alertFrame(ev('agent_error', { agent_id: 'a1' }, T(1))) !== null) fail('错误帧不该折成越界');

// 3. 第一帧给首末时刻；再来一帧只推最近的那一头
let r = foldAlertFrame(undefined, f);
if (r.firstAt !== T(1) || r.lastAt !== T(1) || r.frames !== 1) {
  fail(`第一帧的记账不对：${JSON.stringify(r)}`);
}
r = foldAlertFrame(r, alertFrame(breach('a1', 'cpu', 0.9, 0.97, T(2))));
if (r.firstAt !== T(1)) fail('再来一帧把「第一次越界」的时刻推后了');
if (r.lastAt !== T(2)) fail('再来一帧没推「最近一次」的时刻');
if (r.current !== 0.97) fail(`实测值没更新：${r.current}`);
if (r.frames !== 2) fail(`帧数没累加：${r.frames}`);

// 4. 重连重放：更旧的帧不许把数值翻回去
const replayed = foldAlertFrame(r, alertFrame(breach('a1', 'cpu', 0.9, 0.91, T(0))));
if (replayed.current !== 0.97) fail(`旧帧把实测值倒回了 ${replayed.current}——一次从未发生的变化`);
if (replayed.lastAt !== T(2)) fail('旧帧把「最近一次」的时刻拉回去了');
if (replayed.frames !== 2) fail('重放不算新的一帧，帧数却不该动');

// 5. 裁剪按「最近一次」留
const many = {};
for (let i = 0; i < 5; i += 1) {
  const one = alertFrame(breach(`a${i}`, 'mem', 1, i, T(i)));
  many[one.key] = foldAlertFrame(undefined, one);
}
const kept = pruneAlerts(many, 2);
if (Object.keys(kept).length !== 2) fail('裁剪没按上限留');
if (!kept['a4::mem'] || !kept['a3::mem']) fail(`裁掉的不是最旧的那几个：${Object.keys(kept)}`);

// 6. 「比这张表更晚」才是「表里还没有」
const rows = {
  'a1::cpu': foldAlertFrame(undefined, alertFrame(breach('a1', 'cpu', 0.9, 0.95, T(5)))),
  'a2::mem': foldAlertFrame(undefined, alertFrame(breach('a2', 'mem', 1, 2, T(1)))),
};
let fresh = breachesSinceSnapshot(T(3), rows);
if (fresh.length !== 1 || fresh[0].key !== 'a1::cpu') {
  fail(`比快照更晚的没挑对：${JSON.stringify(fresh.map((x) => x.key))}`);
}
fresh = breachesSinceSnapshot(T(9), rows);
if (fresh.length !== 0) fail('比快照早的帧被当成了「表里还没有」：重读之后它自己就会消失');
fresh = breachesSinceSnapshot(null, rows);
if (fresh.length !== 2) fail('还没成功读过表时不许把这些帧藏起来');
if (fresh[0].key !== 'a1::cpu') fail('没有快照时刻时应按最近一次排序，新的在前');
fresh = breachesSinceSnapshot('不是时间', rows);
if (fresh.length !== 2) fail('快照时刻读不出来时应按「帧更晚」算，而不是把帧扔掉');

console.log('ok   - 身份、缺字段不折、重放不清旧、裁剪、来路判据都成立');
JS

echo "check-web-live-alerts: 通过"

#!/usr/bin/env bash
# 「这一面是事件流的回音，不是每 15 秒拍一张照片」——项目面的消费者门禁。
#
# 坏起来没有症状：四个视图照样读得到，数字照样自洽，只是每一格都比真相旧最多
# 15 秒。项目面回答的正是「这活现在到哪一步了」：一条刚结束的任务还挂在「执行
# 中」列里，挣值的完成量与甘特的实到时刻一起晚一拍。总线上 `task_status_change`
# 一直在走，这一面却一件事都不改，那这一面就不是事件流的回音。
#
# 判据：
#   1. 折叠函数是纯的：`store/liveProjects.ts` 没有 import，能单文件编译（下面第
#      4 段就是编译出来在 node 里跑的）。
#   2. 帧真的被折进去：`hooks/useEventStream.ts` 的分派点调用 `taskStatusFrame` 与
#      `noteProjectTaskFrame`，而不是只在某处定义了一个没人用的函数。
#   3. 画面真的读它：`views/ProjectsView.tsx` 从 live store 取折叠结果、按
#      `useFrameFedFace(project, …)` 判「这一帧是不是比快照晚」并据此重读、
#      把多出来的那几条渲染出来、标出来路，且范围面登记了「哪条任务归哪个项目」——
#      不登记的话任何一条任务换状态都会惊动每一个打开着的项目面。
#   4. 语义按判据实跑：不是状态帧的不折、`hook_event` 不折、缺字段的不折、
#      重连重放的旧帧不许把状态翻回去、不属于本项目的帧不算本项目的版本、
#      比快照早的帧不算「表里还没有」。
#
# 用法：bash deploy/scripts/check-web-live-projects.sh [仓库根目录]
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

pure = read("store/liveProjects.ts")
if re.search(r"^\s*import\b", pure, re.M):
    sys.exit("store/liveProjects.ts 有 import：它必须能单文件编译，否则第 4 段的判据跑不起来")

hook = read("hooks/useEventStream.ts")
if "taskStatusFrame" not in hook or "noteProjectTaskFrame" not in hook:
    sys.exit(
        "hooks/useEventStream.ts 没有把任务状态帧折进去：总线上有 task_status_change，"
        "项目面上却没有任何东西因此改变"
    )
if not re.search(r"const\s+liveProjectTask\s*=\s*taskStatusFrame\(event\)", hook):
    sys.exit("hooks/useEventStream.ts 定义了却没用 taskStatusFrame(event)：折的是别的什么东西")
if not re.search(r"noteProjectTaskFrame\(liveProjectTask\)", hook):
    sys.exit("hooks/useEventStream.ts 算出帧却不写进 live store")

view = read("views/ProjectsView.tsx")
# 判的是**调用形状**，不是文件里出现过某个名字：只留 import 和注释时，名字照样在，
# 而屏幕上一格都不会变。
for pattern, why in (
    (r"useLiveProjectStore\(\(s\) => s\.rows\)", "画面没有从 live store 取折叠结果：折进来了没人读"),
    (r"useFrameFedFace\(project,", "画面没有按「帧到了就重读」这条判据接帧"),
    (r"own\(project\.id,", "范围面没有登记任务归属：任何任务换状态都会惊动每一个项目面"),
    (r"<FreshNodeChanges rows=\{fresh\}", "算出「快照里还没有的那几条」却不渲染：读者仍然要等下一拍轮询"),
    (r"总线上比这张", "那一块没有标来路：读者会把它当成表里本来就有的行"),
):
    if not re.search(pattern, view):
        sys.exit(f"views/ProjectsView.tsx 缺 {pattern}：{why}")

print("ok   - 折叠、分派、画面三处接线齐全")
PYEOF

TSC="$ROOT/web/node_modules/.bin/tsc"
[ -x "$TSC" ] || {
    echo "缺少 web/node_modules/.bin/tsc——先在 web/ 里装依赖（npm ci）" >&2
    exit 2
}
"$TSC" "$ROOT/web/src/store/liveProjects.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/store/liveProjects.ts 编译不过：折叠的判据无从检查" >&2
    exit 1
}
mv "$TMP/liveProjects.js" "$TMP/liveProjects.mjs"

node --input-type=module - "$TMP/liveProjects.mjs" <<'JS'
const {
  taskStatusFrame,
  foldTaskFrame,
  pruneTasks,
  rowsForProject,
  projectRevision,
  changedSinceSnapshot,
} = await import(process.argv[2]);
const fail = (msg) => {
  console.error(`任务状态帧折叠不对：${msg}`);
  process.exit(1);
};

const T = (s) => `2026-10-26T06:00:${String(s).padStart(2, '0')}Z`;
const ev = (type, payload, at = T(0)) => ({ type, timestamp: at, payload });
const move = (task, status, at = T(0)) =>
  ev('task_status_change', { task_id: task, status }, at);

// 1. 身份：一条任务一行，状态是最近一帧说的那个
let f = taskStatusFrame(move('t1', 'running', T(1)));
if (!f) fail('一条完整的状态帧没折出来');
if (f.taskId !== 't1' || f.status !== 'running') fail(`帧的身份错了：${JSON.stringify(f)}`);

// 2. 不是任务状态帧的一律不折
if (taskStatusFrame(ev('heartbeat', { task_id: 't1', status: 'running' }, T(1))) !== null) {
  fail('心跳不该折成状态变化');
}
if (taskStatusFrame(ev('resource_alert', { task_id: 't1', status: 'running' }, T(1))) !== null) {
  fail('越界帧不该折成状态变化');
}
if (taskStatusFrame(move('t1', undefined)) !== null) fail('没有状态值的帧不该折');
if (taskStatusFrame(ev('task_status_change', { status: 'running' }, T(1))) !== null) {
  fail('没有 task_id 的帧不该折');
}

// 3. `hook_event` 借这个变体过总线：它说的是钩子跑完了，不是任务换了状态
if (taskStatusFrame(move('t1', 'hook_event', T(1))) !== null) {
  fail('hook_event 被折成了状态变化：看板上会凭空出现一次没发生的迁移');
}

// 4. 第一帧给首末时刻；再来一帧只推最近的那一头
let r = foldTaskFrame(undefined, f);
if (r.firstAt !== T(1) || r.lastAt !== T(1) || r.frames !== 1) {
  fail(`第一帧的记账不对：${JSON.stringify(r)}`);
}
r = foldTaskFrame(r, taskStatusFrame(move('t1', 'completed', T(2))));
if (r.firstAt !== T(1)) fail('再来一帧把「第一次变化」的时刻推后了');
if (r.lastAt !== T(2)) fail('再来一帧没推「最近一次」的时刻');
if (r.status !== 'completed') fail(`状态没更新：${r.status}`);
if (r.frames !== 2) fail(`帧数没累加：${r.frames}`);

// 5. 重连重放：更旧的帧不许把状态翻回去
const replayed = foldTaskFrame(r, taskStatusFrame(move('t1', 'running', T(0))));
if (replayed.status !== 'completed') {
  fail(`旧帧把状态倒回了 ${replayed.status}——一次从未发生的回退`);
}
if (replayed.lastAt !== T(2)) fail('旧帧把「最近一次」的时刻拉回去了');
if (replayed.frames !== 2) fail('重放不算新的一帧，帧数却不该动');

// 6. 裁剪按「最近一次」留
const many = {};
for (let i = 0; i < 5; i += 1) {
  const one = taskStatusFrame(move(`t${i}`, 'running', T(i)));
  many[one.taskId] = foldTaskFrame(undefined, one);
}
const kept = pruneTasks(many, 2);
if (Object.keys(kept).length !== 2) fail('裁剪没按上限留');
if (!kept.t4 || !kept.t3) fail(`裁掉的不是最旧的那几个：${Object.keys(kept)}`);

// 7. 归属：只算本项目任务的帧，版本号取最近一帧的时刻
const owned = { t1: 'p1', t2: 'p2' };
const rows = {
  t1: foldTaskFrame(undefined, taskStatusFrame(move('t1', 'running', T(5)))),
  t2: foldTaskFrame(undefined, taskStatusFrame(move('t2', 'failed', T(9)))),
  t3: foldTaskFrame(undefined, taskStatusFrame(move('t3', 'running', T(7)))),
};
const mine = rowsForProject('p1', owned, rows);
if (mine.length !== 1 || mine[0].taskId !== 't1') {
  fail(`不属于本项目的帧被算进了这一面：${JSON.stringify(mine.map((x) => x.taskId))}`);
}
const rev = projectRevision('p1', owned, rows);
if (rev !== Date.parse(T(5))) fail(`版本号不是本项目最近一帧的时刻：${rev}`);
if (projectRevision('p9', owned, rows) !== 0) fail('没有归属的项目不该有版本号');

// 8. 「比这份快照更晚」才是「表里还没有」
let fresh = changedSinceSnapshot(T(3), mine);
if (fresh.length !== 1 || fresh[0].taskId !== 't1') {
  fail(`比快照更晚的没挑对：${JSON.stringify(fresh.map((x) => x.taskId))}`);
}
if (changedSinceSnapshot(T(9), mine).length !== 0) {
  fail('比快照早的帧被当成了「表里还没有」：重读之后它自己就会消失');
}
if (changedSinceSnapshot(null, mine).length !== 1) {
  fail('还没成功读过这一面时不许把这些帧藏起来');
}
if (changedSinceSnapshot('不是时间', mine).length !== 1) {
  fail('快照时刻读不出来时应按「帧更晚」算，而不是把帧扔掉');
}

console.log('ok   - 身份、hook_event 不折、重放不回退、裁剪、归属、版本号、来路判据都成立');
JS

echo "check-web-live-projects: 通过"

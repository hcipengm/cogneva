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

# ---- 5. 四张屏没被一墙项目埋掉 ------------------------------------------------
#
# 一览与四张屏在同一个滚动面里：一览铺满，四张屏就在几万像素之外。任务图里 goal
# 与任务常常一比一（2026-10-26 实测 3831 条任务上有 3726 个 goal），所以「全铺」
# 不是罕见情况，是常态。这一条判两处：一览必须先经 `visibleProjects` 折叠再画，
# 四张屏必须紧接在折叠后的一览之下。
strip = read("views/projectStrip.ts")
if re.search(r"^\s*import\b", strip, re.M):
    sys.exit("views/projectStrip.ts 有 import：它必须能单文件编译，否则第 5 段的判据跑不起来")
for pattern, why in (
    (r"visibleProjects\(rows,", "一览没有经 visibleProjects 折叠：几千个 goal 会原样铺开，四张屏被推到墙后面"),
    (r"view\.shown\.map\(", "折叠算出来了却不按它画：铺开的仍然是全部行"),
):
    if not re.search(pattern, view):
        sys.exit(f"views/ProjectsView.tsx 缺 {pattern}：{why}")
if not re.search(r"<ProjectStrip rows=\{rows\}", view):
    sys.exit("views/ProjectsView.tsx 没有把一览接上：折叠函数成了一个没人用的东西")

print("ok   - 一览经折叠后再画，四张屏不被几千行推走")
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

"$TSC" "$ROOT/web/src/views/projectStrip.ts" --target es2020 --module es2020 \
    --outDir "$TMP" >/dev/null 2>&1 || {
    echo "web/src/views/projectStrip.ts 编译不过：一览折叠的判据无从检查" >&2
    exit 1
}
mv "$TMP/projectStrip.js" "$TMP/projectStrip.mjs"

node --input-type=module - "$TMP/projectStrip.mjs" <<'JS'
const { visibleProjects, STRIP_CAP } = await import(process.argv[2]);
const fail = (msg) => {
  console.error(`项目一览折叠不对：${msg}`);
  process.exit(1);
};

// 实测尺度：3726 个 goal，服务端已按任务数从多到少排好。
const rows = Array.from({ length: 3726 }, (_, i) => ({ id: `goal-${i}` }));

// 1. 默认只铺前几行，其余用一个数说清楚
let v = visibleProjects(rows, { selected: null });
if (v.shown.length !== STRIP_CAP) fail(`默认铺了 ${v.shown.length} 行，应只有 ${STRIP_CAP} 行`);
if (v.hidden !== rows.length - STRIP_CAP) fail(`没列出的数不对：${v.hidden}`);
if (v.filteredOut !== 0) fail('没输过滤词却报了被筛掉的行');

// 2. 顺序就是服务端给的那个（任务多的在前），不许在这里重排
if (v.shown[0].id !== 'goal-0' || v.shown[STRIP_CAP - 1].id !== `goal-${STRIP_CAP - 1}`) {
  fail(`铺出来的顺序变了：${JSON.stringify(v.shown.map((r) => r.id))}`);
}

// 3. 选中行永远在场：它在折叠线以下时也要补进来，否则一览与面板说的不是一个项目
v = visibleProjects(rows, { selected: 'goal-3000' });
if (!v.shown.some((r) => r.id === 'goal-3000')) fail('选中的项目被折叠掉了');
if (v.shown.length !== STRIP_CAP + 1) fail(`补选中行补多了：${v.shown.length}`);

// 4. 过滤词说了算：铺满相符的那些，并分别报「筛掉」与「没列出」
v = visibleProjects(rows, { query: 'goal-12' });
const want = rows.filter((r) => r.id.includes('goal-12')).length;
if (v.shown.length !== want) fail(`过滤后铺了 ${v.shown.length} 行，相符的有 ${want} 行`);
if (v.hidden !== 0) fail('过滤之后仍有没列出的行：读者已经在缩小范围，不该再替他砍一刀');
if (v.filteredOut !== rows.length - want) fail(`被筛掉的数不对：${v.filteredOut}`);

// 5. 过滤词大小写不敏感，两侧空白不吃人
if (visibleProjects(rows, { query: '  GOAL-7 ' }).shown.length === 0) {
  fail('过滤词大小写或空白一碰就查不到');
}

// 6. 展开就是全铺
v = visibleProjects(rows, { expanded: true });
if (v.shown.length !== rows.length) fail(`展开了却只铺了 ${v.shown.length} 行`);
if (v.hidden !== 0) fail('展开之后仍报有没列出的行');

// 7. 空名单不出错，也不编一行出来
v = visibleProjects([], { selected: 'goal-1' });
if (v.shown.length !== 0) fail('空名单里凭空补出了一行');

console.log('ok   - 折叠、选中行在场、过滤、展开、空名单都按判据');
JS

echo "check-web-live-projects: 通过"

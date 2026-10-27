#!/usr/bin/env bash
# check-web-project-write-paths.sh 的判据：项目面必须能从界面上被改。
#
# 这条门守的是一整条**每一段都能单独断**的链。断掉的每一段单独看都像「已经做完了」：
# 取数函数在 `client.ts` 里、组件在 `projectWrite.tsx` 里、路由在网关上——三份代码
# 各自都对，而客户在屏上找不到任何一个能按的控件。
#
# 夹具是一条完整的链（两端的取数与路由 ＋ 中间的组件与渲染），然后逐段剪断，每剪一段
# 必须红：
#   - 取数函数少了声明        → 前端无从发起这一次写
#   - 组件 import 了不调用    → 函数声明了，没有人用它
#   - 视图 import 了不渲染    → 屏上不会有这个控件
#   - 写完不调回调            → 写成功之后读数不重读，读者会以为没生效
#   - 客户端不认 cards        → 服务端带回来了，屏上还是没有
#   - 服务端少了 BoardCard    → 一列只有计数，画不出能挪的卡
#   - 服务端少了 cards_hidden → 截断之后的「还有几张」没人说得出来
#   - 网关少一条路由          → 前端在调的这条路不存在
# 每段改动都先确认**落到了文件上**（否则「改动没生效」与「门没抓住」不可区分）。
#
# 最后一条断言在**真树**上跑：夹具全绿而真树红，说明这条门只是在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-project-write-paths.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${checker}" ] || fail "找不到 ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ── 夹具：一条从取数到路由的完整链 ──
fixture() {
  rm -rf "${work}/web" "${work}/crates" "${work}/deploy"
  mkdir -p "${work}/web/src/api" "${work}/web/src/views" "${work}/web/src/projects" \
    "${work}/crates/cog-gateway/src" "${work}/deploy/scripts"
  # 门自己会被按路径引用（A16 的 check 里点了它的名字），夹具里放一份同样的文件名。
  cat >"${work}/deploy/scripts/check-web-project-write-paths.sh" <<'EOF'
cards_hidden
projectWrite
EOF

  cat >"${work}/web/src/api/client.ts" <<'EOF'
export interface BoardCard {
  id: string;
  status: string;
}
export interface BoardColumn {
  status: string;
  cards: BoardCard[];
  cards_hidden: number;
}
export async function createProject(input: unknown): Promise<unknown> {
  return input;
}
export async function createProjectTask(p: string, input: unknown): Promise<unknown> {
  return input;
}
export async function updateTask(id: string, input: unknown): Promise<unknown> {
  return input;
}
export async function saveTaskPlan(id: string, input: unknown): Promise<unknown> {
  return input;
}
export async function moveTask(id: string, status: string): Promise<unknown> {
  return status;
}
export async function getProjectAudit(id: string): Promise<unknown> {
  return id;
}
export async function downloadProjectCsv(id: string): Promise<unknown> {
  return id;
}
EOF

  cat >"${work}/web/src/projects/projectWrite.tsx" <<'EOF'
import { createProject, createProjectTask, updateTask, saveTaskPlan, moveTask } from '@/api/client';

export function NewProjectForm({ onCreated }: { onCreated: () => void }) {
  const submit = async () => {
    await createProject({ name: 'x' });
    onCreated();
  };
  return <button onClick={() => void submit()}>建</button>;
}

export function NewTaskForm({ onCreated }: { onCreated: () => void }) {
  const submit = async () => {
    await createProjectTask('p', { title: 't' });
    onCreated();
  };
  return <button onClick={() => void submit()}>建活</button>;
}

export function MoveCardControl({ onDone }: { onDone: () => void }) {
  const move = async () => {
    await moveTask('t', 'scheduled');
    onDone();
  };
  return <button onClick={() => void move()}>挪</button>;
}

export function ReparentControl({ onDone }: { onDone: () => void }) {
  const apply = async () => {
    await updateTask('t', { parent_task_id: null });
    onDone();
  };
  return <button onClick={() => void apply()}>改</button>;
}

export function PlanForm({ onDone }: { onDone: () => void }) {
  const submit = async () => {
    await saveTaskPlan('t', {});
    onDone();
  };
  return <button onClick={() => void submit()}>排期</button>;
}

export function PlanButton({ onDone }: { onDone: () => void }) {
  return <PlanForm onDone={onDone} />;
}

export function ExportCsvButton() {
  return <button>导出</button>;
}

export function AuditTimeline() {
  return <div />;
}
EOF

  cat >"${work}/web/src/views/ProjectsView.tsx" <<'EOF'
import {
  AuditTimeline,
  ExportCsvButton,
  MoveCardControl,
  NewProjectForm,
  NewTaskForm,
  PlanButton,
  ReparentControl,
} from '@/projects/projectWrite';

export function ProjectsView({ columns }: { columns: { cards: { id: string }[] }[] }) {
  return (
    <div>
      {columns.map((c) => (
        <div key={c.cards.length}>
          {c.cards.map((card) => (
            <MoveCardControl key={card.id} onDone={() => {}} />
          ))}
        </div>
      ))}
      <NewProjectForm onCreated={() => {}} />
      <NewTaskForm onCreated={() => {}} />
      <ReparentControl onDone={() => {}} />
      <PlanButton onDone={() => {}} />
      <ExportCsvButton />
      <AuditTimeline projectId="p" />
    </div>
  );
}
EOF

  cat >"${work}/crates/cog-gateway/src/projects.rs" <<'EOF'
pub struct BoardCard {
    pub id: String,
    pub status: String,
}

pub struct BoardColumn {
    pub status: String,
    pub cards: Vec<BoardCard>,
    pub cards_hidden: usize,
}
EOF
  cat >"${work}/crates/cog-gateway/src/lib.rs" <<'EOF'
fn routes() {
    let _ = "/api/v1/projects";
    let _ = "/api/v1/projects/{id}/tasks";
    let _ = "/api/v1/tasks/{id}/plan";
    let _ = "/api/v1/tasks/{id}/move";
    let _ = "/api/v1/projects/{id}/audit";
}
EOF
}

expect_green() {
  bash "${checker}" "${work}" >/dev/null 2>"${work}/err" \
    || fail "夹具应当绿，实际红：$(cat "${work}/err")"
}

# 剪一段，确认**改动真的落在文件上**，再要求它红。
cut() { # $1 = 文件，$2 = 旧串，$3 = 新串，$4 = 判词
  python3 - "$1" "$2" "$3" <<'PY'
import sys
path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
src = open(path, encoding="utf-8").read()
if old not in src:
    sys.exit(f"剪不动：{path} 里没有 {old!r}")
open(path, "w", encoding="utf-8").write(src.replace(old, new, 1))
PY
  [ $? -eq 0 ] || fail "$4：改动没落到文件上"
  if bash "${checker}" "${work}" >/dev/null 2>&1; then
    fail "$4：门还是绿的"
  fi
}

fixture
expect_green

cut "${work}/web/src/api/client.ts" \
  'export async function moveTask' 'async function moveTaskRemoved' \
  "取数函数少了声明（挪卡）"
fixture

cut "${work}/web/src/projects/projectWrite.tsx" \
  'await createProjectTask(' 'createProjectTask;void (0 as unknown);(' \
  "组件 import 了却不调用"
fixture

cut "${work}/web/src/views/ProjectsView.tsx" \
  '<AuditTimeline projectId="p" />' '<div />' \
  "视图 import 了不渲染"
fixture

cut "${work}/web/src/projects/projectWrite.tsx" \
  'await moveTask('"'"'t'"'"', '"'"'scheduled'"'"');
    onDone();' 'await moveTask('"'"'t'"'"', '"'"'scheduled'"'"');' \
  "写完不调回调"
fixture

cut "${work}/web/src/api/client.ts" \
  'cards: BoardCard[];' 'cardsRemoved: BoardCard[];' \
  "客户端不认 cards"
fixture

cut "${work}/crates/cog-gateway/src/projects.rs" \
  'pub struct BoardCard' 'pub struct BoardCardGone' \
  "服务端少了 BoardCard"
fixture

cut "${work}/crates/cog-gateway/src/projects.rs" \
  'pub cards_hidden: usize,' 'pub cards_left_out: usize,' \
  "服务端少了 cards_hidden"
fixture

cut "${work}/crates/cog-gateway/src/lib.rs" \
  '"/api/v1/tasks/{id}/move"' '"/api/v1/tasks/{id}/moveX"' \
  "网关少一条路由"
fixture

# 真树：夹具全绿而真树红，说明这条门只是在测自己。
bash "${checker}" "${repo}" >/dev/null || fail "真树上这条门是红的"

echo "check-web-project-write-paths：八段剪断各红一次，真树绿"

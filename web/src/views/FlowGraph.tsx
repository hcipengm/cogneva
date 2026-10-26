import type { GoalGroup, ProcessTask } from '@/api/client';

/**
 * 一道意图的流程图：目标是起点，箭头是「谁等谁」，节点是原子任务。
 *
 * 它与下面那张表读的是同一批行，答的是两个问题：表回答「每一条现在是什么状态」，
 * 图回答「这件事长成什么样、卡在哪一段」。分成两个视图不是重复：一张表里十几行
 * 状态看不出形状，一张图里也读不出重试次数与错误原文。
 *
 * 布局只按数据算，不摆位置：一条任务在第几列，是它依赖链的深度（它等的那几条里
 * 最长的那条 + 1）。所以「卡住」在图上就是右边一大片还没人点亮，而左边一条链已经
 * 走到底——这个形状是这一面真正要说的东西。
 */

const NODE_W = 172;
const NODE_H = 34;
const COL_GAP = 56;
const ROW_GAP = 14;
const PAD_X = 16;
const PAD_Y = 16;
/** 目标是图上第一个节点，它比任务宽一点：goal 的 id 要能读全前八位。 */
const GOAL_W = 150;

/** 一行的状态 → 颜色。与下方表格同一套词，同一套色。 */
function statusColor(status: string): string {
  switch (status.toLowerCase()) {
    case 'running':
      return 'var(--ok)';
    case 'failed':
      return 'var(--danger)';
    case 'completed':
      return 'var(--paper-dim)';
    case 'pending':
      return 'var(--warn)';
    default:
      return 'var(--warn)';
  }
}

function isRunning(status: string): boolean {
  return status.toLowerCase() === 'running';
}

interface Placed {
  task: ProcessTask;
  status: string;
  x: number;
  y: number;
  level: number;
}

/**
 * 依赖深度。
 *
 * 环不是不可能（重试过的链、被判退又重排的图都可能留下一条成环的 `blocked_by`），
 * 所以迭代有上界：`tasks.length` 轮之后还定不下来的按它当前的值落位。为一条环
 * 把整张图挂掉不值得——图是给人看形状的，不是求解器。
 */
function levelsOf(tasks: ProcessTask[]): Map<string, number> {
  const known = new Set(tasks.map((task) => task.id));
  const level = new Map<string, number>(tasks.map((task) => [task.id, 0]));
  for (let round = 0; round < tasks.length; round += 1) {
    let changed = false;
    for (const task of tasks) {
      const parents = task.blocked_by.filter((id) => known.has(id));
      if (parents.length === 0) continue;
      const deepest = Math.max(...parents.map((id) => level.get(id) ?? 0));
      if ((level.get(task.id) ?? 0) < deepest + 1) {
        level.set(task.id, deepest + 1);
        changed = true;
      }
    }
    if (!changed) break;
  }
  return level;
}

function layout(goal: GoalGroup, statusOf: (task: ProcessTask) => string): Placed[] {
  const level = levelsOf(goal.tasks);
  const rows = new Map<number, number>();
  const placed: Placed[] = [];
  for (const task of goal.tasks) {
    const column = level.get(task.id) ?? 0;
    const row = rows.get(column) ?? 0;
    rows.set(column, row + 1);
    placed.push({
      task,
      status: statusOf(task),
      level: column,
      x: PAD_X + GOAL_W + COL_GAP + column * (NODE_W + COL_GAP),
      y: PAD_Y + row * (NODE_H + ROW_GAP),
    });
  }
  return placed;
}

export function FlowGraph({
  goals,
  statusOf,
}: {
  goals: GoalGroup[];
  statusOf: (task: ProcessTask) => string;
}) {
  if (goals.length === 0) return null;
  return (
    <div className="flex flex-col divide-y divide-[var(--line)]">
      {goals.map((goal) => (
        <GoalFlow key={goal.goal_id} goal={goal} statusOf={statusOf} />
      ))}
    </div>
  );
}

function GoalFlow({
  goal,
  statusOf,
}: {
  goal: GoalGroup;
  statusOf: (task: ProcessTask) => string;
}) {
  const placed = layout(goal, statusOf);
  const columns = placed.reduce((max, node) => Math.max(max, node.level), -1) + 1;
  // 行数要按「最挤的那一列」算，不能拿总数除以列数：一道目标常常是「一个根任务 +
  // 一串串行的后续」，按平均算出来的高度会让长的那一列压到下面去。
  const rows = Math.max(
    1,
    placed.reduce((max, node) => {
      const sameColumn = placed.filter((other) => other.level === node.level).length;
      return Math.max(max, sameColumn);
    }, 1)
  );
  const width = PAD_X * 2 + GOAL_W + COL_GAP + columns * (NODE_W + COL_GAP) - COL_GAP;
  const height = PAD_Y * 2 + rows * (NODE_H + ROW_GAP) - ROW_GAP;
  const byId = new Map(placed.map((node) => [node.task.id, node]));
  const goalCy = PAD_Y + (rows * (NODE_H + ROW_GAP) - ROW_GAP) / 2 - NODE_H / 2;
  const goalColor =
    goal.failed > 0
      ? 'var(--danger)'
      : goal.total > 0 && goal.completed === goal.total
        ? 'var(--ok)'
        : 'var(--brass)';

  return (
    <div className="overflow-x-auto px-4 py-2">
      <svg
        role="img"
        aria-label={`意图 ${goal.goal_id} 的任务流程图`}
        viewBox={`0 0 ${width} ${Math.max(height, NODE_H + PAD_Y * 2)}`}
        style={{ minWidth: width, height: Math.max(height, NODE_H + PAD_Y * 2) }}
      >
        {placed.map((node) =>
          node.task.blocked_by.map((blocked) => {
            const parent = byId.get(blocked);
            if (!parent) return null;
            const x1 = parent.x + NODE_W;
            const y1 = parent.y + NODE_H / 2;
            const x2 = node.x;
            const y2 = node.y + NODE_H / 2;
            const mid = (x1 + x2) / 2;
            return (
              <path
                key={`${parent.task.id}->${node.task.id}`}
                d={`M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke="var(--line-strong)"
                strokeWidth={1.2}
                className={isRunning(node.status) ? 'flow-edge-active' : undefined}
              />
            );
          })
        )}

        {/* 目标 → 第一列：每一道还没被挡住的任务都直接由它派生。 */}
        {placed
          .filter((node) => node.level === 0)
          .map((node) => {
            const y1 = goalCy + NODE_H / 2;
            const y2 = node.y + NODE_H / 2;
            const x1 = PAD_X + GOAL_W;
            const x2 = node.x;
            const mid = (x1 + x2) / 2;
            return (
              <path
                key={`goal->${node.task.id}`}
                d={`M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke="var(--line)"
                strokeWidth={1}
              />
            );
          })}

        <g>
          <rect
            x={PAD_X}
            y={goalCy}
            width={GOAL_W}
            height={NODE_H}
            rx={3}
            fill="var(--ink-800)"
            stroke={goalColor}
            strokeWidth={1.4}
          />
          <text
            x={PAD_X + 10}
            y={goalCy + 14}
            className="mono"
            style={{ fill: 'var(--paper)', fontSize: 10 }}
          >
            {goal.goal_id.slice(0, 8)}
          </text>
          <text
            x={PAD_X + 10}
            y={goalCy + 27}
            style={{ fill: 'var(--muted)', fontSize: 9 }}
          >
            意图 · {goal.completed}/{goal.total} 收口
          </text>
        </g>

        {placed.map((node) => {
          const color = statusColor(node.status);
          return (
            <g key={node.task.id}>
              <rect
                x={node.x}
                y={node.y}
                width={NODE_W}
                height={NODE_H}
                rx={3}
                fill="var(--ink-800)"
                stroke={color}
                strokeWidth={isRunning(node.status) ? 1.6 : 1}
                className={isRunning(node.status) ? 'flow-node-active' : undefined}
              >
                <title>
                  {`${node.task.id}\n类型 ${node.task.task_type}\n状态 ${node.status}（快照 ${node.task.status}）` +
                    `\n执行者 ${node.task.agent_id ?? '未认领'} · 重试 ${node.task.retry_count}/${node.task.max_retries}` +
                    (node.task.blocked_by.length ? `\n等 ${node.task.blocked_by.join('、')}` : '') +
                    (node.task.error ? `\n错误 ${node.task.error}` : '')}
                </title>
              </rect>
              <circle cx={node.x + 10} cy={node.y + 12} r={3} fill={color} />
              <text
                x={node.x + 20}
                y={node.y + 15}
                className="mono"
                style={{ fill: 'var(--paper-dim)', fontSize: 9 }}
              >
                {node.task.task_type.slice(0, 18)}
              </text>
              <text
                x={node.x + 20}
                y={node.y + 27}
                className="mono"
                style={{ fill: 'var(--muted)', fontSize: 8 }}
              >
                {(node.task.agent_id ?? '未认领').slice(0, 16)} · {node.status}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

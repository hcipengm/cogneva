import { useMemo, useState } from 'react';
import {
  getTaskGraph,
  getTaskSummary,
  listTasks,
  type TaskRow,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import {
  effectiveStatus,
  liveIndex,
  useStreamStore,
  type LiveTaskStatus,
} from '@/store/streamStore';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';
import { Strip } from '@/ui/plots';

/**
 * 任务面：两类问题两个视图。
 *
 * 列表答「这一批活各自是什么状态」；图答「谁在等谁、一次意图被分解成了哪几块」。
 * 两个视图读的是同一个端点族（`/tasks/list` 与 `/tasks/graph`），过滤词也是同一个，
 * 所以「列表里 12 条失败、图上一条红都看不到」这种口径不一致不会发生。
 *
 * 图和列表都能点开一条任务：展开的是它自己的行，不是再发一次请求——图与列表本来
 * 就已经带着完整的一行回来了。
 */
// 过滤词原样进 query string，服务端按 `TaskFilter` 认识它们：`running` 这些是行上
// 携带的状态词，`ready` 是就绪队列本身（未开始、阻塞都已完成）——它不是任何一行的
// 状态，所以由服务端拿队列谓词算，不是按状态比。这里的词与那份声明对不上时，标签
// 会拿到一张空表，而空表读起来就是「没有任务卡在这一步」。
const FILTERS = ['全部', 'running', 'ready', 'completed', 'failed'] as const;

type Mode = 'list' | 'graph';

export function TasksView() {
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>('全部');
  const [mode, setMode] = useState<Mode>('list');
  const [selected, setSelected] = useState<string | null>(null);

  // 过滤词进 `what`：这个 hook 按 `what` 重开读取，而过滤词写在闭包里时它看不见
  // 变化——点了「失败」，屏上还是上一次那批行，标题却已经是新的过滤词，最久要等
  // 一次轮询才对上。
  const summary = useResource('任务汇总', getTaskSummary, 15_000);
  const list = useResource(
    filter === '全部' ? '任务列表' : `任务列表（${filter}）`,
    () => listTasks(filter === '全部' ? { limit: 300 } : { status: filter, limit: 300 }),
    15_000
  );
  const graph = useResource(
    filter === '全部' ? '任务图' : `任务图（${filter}）`,
    () => getTaskGraph(filter === '全部' ? {} : { status: filter }),
    15_000,
    mode === 'graph'
  );
  // 帧在这里不是补丁，是**重读的由头**：列表里那一行的状态由帧直接改写，而表头
  // 的计数和图上的节点来自各自那份快照。少了这一句，一次 `task_status_change`
  // 之后画面上会同时存在两份事实——行说跑完了，表头还数在「运行」里，图上那条
  // 也还连着。等到下一拍 15 秒，两半才对上。三份都重读，是因为它们必须是同一
  // 时刻的那一份；只刷列表，表头只会更久地站在错的那一边。
  useKindTickedRefresh(
    ['tasks', resourceKey('tasks'), resourceKey('projects')],
    [summary.refresh, list.refresh, graph.refresh]
  );

  const tasks = list.data ?? [];
  const nodes = graph.data?.nodes ?? [];
  const counts = summary.data;

  // 帧是轮询快照之间的补丁：`task_status_change` 一到，这一行就不必再等下一次
  // 轮询。没有它，画面上最久会停在十几秒前的状态上——而总线上的状态早就变了。
  const liveTasks = useStreamStore((s) => s.liveTasks);
  const live = useMemo(() => liveIndex(liveTasks), [liveTasks]);
  const fromFrame = tasks.filter((t) => effectiveStatus(t, live).fromFrame).length;
  const picked = useMemo(
    () => tasks.find((t) => t.id === selected) ?? nodes.find((t) => t.id === selected) ?? null,
    [tasks, nodes, selected]
  );

  return (
    <ViewFrame
      title="任务"
      meta={counts ? `共 ${counts.total} · 运行 ${counts.running} · 就绪 ${counts.ready} · 失败 ${counts.failed}` : '按 15 秒重读'}
      actions={
        <div className="flex items-center gap-3">
          <div className="flex gap-1">
            {(['list', 'graph'] as const).map((m) => (
              <button
                key={m}
                onClick={() => setMode(m)}
                className="border px-2 py-1 text-[11px]"
                style={{
                  borderColor: mode === m ? 'var(--brass-dim)' : 'var(--line)',
                  color: mode === m ? 'var(--brass)' : 'var(--paper-dim)',
                }}
              >
                {m === 'list' ? '列表' : '图'}
              </button>
            ))}
          </div>
          <div className="flex gap-1">
            {FILTERS.map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className="border px-2 py-1 text-[11px]"
                style={{
                  borderColor: filter === f ? 'var(--brass-dim)' : 'var(--line)',
                  color: filter === f ? 'var(--brass)' : 'var(--paper-dim)',
                }}
              >
                {f}
              </button>
            ))}
          </div>
        </div>
      }
    >
      {counts && (
        <Readings
          from={summary}
          source="GET /api/v1/tasks/summary"
          caveat="五个数是服务端在那一时刻数的全部任务，不随下面的过滤词变。"
        >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="全部" value={counts.total} />
          <Reading label="运行中" value={counts.running} tone="ok" />
          <Reading label="就绪待领" value={counts.ready} tone={counts.ready > 0 ? 'warn' : 'plain'} />
          <Reading label="已完成" value={counts.completed} />
          <Reading label="失败" value={counts.failed} tone={counts.failed > 0 ? 'danger' : 'plain'} />
          <Reading
            label="帧里更新过"
            value={fromFrame}
            from={list}
            source="这一页任务行与总线上 task_status_change 取新的那个"
            caveat="只算这一页列出来的任务，且只算比这次轮询新的帧；它数的是差，不是总量。"
            frames={liveTasks.slice(0, 20).map((t) => ({
              at: t.at,
              type: 'task_status_change',
              summary: `任务 ${t.taskId} → ${t.status}`,
            }))}
            tone={fromFrame > 0 ? 'warn' : 'plain'}
            note="比这次轮询新"
          />
        </div>
        </Readings>
      )}
      {/* 图上画的是依赖边，画不出「五分之四已经收口」这种比例。同一份 summary 分成
          几段：这批活是刚开始还是快收尾，是这一面最先被问到的一句。 */}
      {counts && counts.total > 0 && (
        <div className="border-b border-[var(--line)]">
          <Strip
            label="全部任务各处在什么状态"
            note="数的是 GET /api/v1/tasks/summary 那一刻的全部任务，不随下面的过滤词变"
            segments={[
              { label: '运行中', value: counts.running, tone: 'ok' },
              { label: '就绪待领', value: counts.ready, tone: 'warn' },
              { label: '已完成', value: counts.completed, tone: 'muted' },
              { label: '失败', value: counts.failed, tone: 'danger' },
            ]}
          />
        </div>
      )}

      {mode === 'list' ? (
        list.error ? (
          <ReadError what="任务列表" error={list.error} onRetry={list.refresh} />
        ) : !list.data && list.loading ? (
          <Loading what="任务列表" />
        ) : tasks.length === 0 ? (
          <Empty>这个过滤条件下没有任务。</Empty>
        ) : (
          <Table<TaskRow>
            columns={[
              { key: 'id', title: 'id', width: '150px', render: (t) => (
                <button
                  onClick={() => setSelected(t.id)}
                  className="mono text-[11px] text-[var(--paper-dim)] underline-offset-2 hover:underline"
                >
                  {t.id.slice(0, 18)}
                </button>
              ) },
              { key: 'type', title: '类型', width: '130px', render: (t) => t.task_type },
              { key: 'status', title: '状态', width: '130px', render: (t) => <LiveStatus row={t} live={live} /> },
              { key: 'owner', title: '归属', width: '150px', render: (t) => (
                <span className="mono text-[11px] text-[var(--muted)]">{t.owner_unit ?? t.agent_id ?? '—'}</span>
              ) },
              { key: 'blocked', title: '等谁', width: '70px', render: (t) => (
                <span className="mono text-[11px]" style={{ color: (t.blocked_by?.length ?? 0) > 0 ? 'var(--warn)' : 'var(--muted)' }}>
                  {t.blocked_by?.length || '—'}
                </span>
              ) },
              { key: 'error', title: '结局', render: (t) =>
                t.error ? (
                  <span className="text-[11px] text-[var(--danger)]">
                    {t.error_cause ? `[${t.error_cause}] ` : ''}
                    {t.error}
                  </span>
                ) : (
                  <span className="text-[11px] text-[var(--muted)]">—</span>
                ) },
              { key: 'updated', title: '更新', width: '100px', render: (t) => <Moment iso={t.updated_at} /> },
            ]}
            rows={tasks}
            rowKey={(t) => t.id}
          />
        )
      ) : graph.error ? (
        <ReadError what="任务图" error={graph.error} onRetry={graph.refresh} />
      ) : !graph.data && graph.loading ? (
        <Loading what="任务图" />
      ) : nodes.length === 0 ? (
        <Empty>这个过滤条件下图里没有节点。</Empty>
      ) : (
        <TaskGraphPanel
          nodes={nodes}
          selected={selected}
          live={live}
          onSelect={(id) => setSelected(id === selected ? null : id)}
        />
      )}

      {picked && (
        <TaskDetail
          task={picked}
          live={live}
          onSelect={setSelected}
          onClose={() => setSelected(null)}
        />
      )}
    </ViewFrame>
  );
}

const COLUMN_WIDTH = 236;
const NODE_WIDTH = 196;
const NODE_HEIGHT = 58;
const ROW_GAP = 74;
const PAD = 18;

/**
 * 任务图：一列一层，边从左往右。
 *
 * 分层按 `blocked_by` 的最长路径算：一条任务落在「它所有依赖都落定之后」的那一列，
 * 所以从左往右读就是执行顺序。算的过程中撞回自己在走的路径，就是一个环——那时
 * 不画一张看起来合理的图，而是把它点出来：环是分解的缺陷，不是布局问题。
 */
function TaskGraphPanel({
  nodes,
  selected,
  live,
  onSelect,
}: {
  nodes: TaskRow[];
  selected: string | null;
  live: Record<string, LiveTaskStatus>;
  onSelect: (id: string) => void;
}) {
  const layout = useMemo(() => computeLayout(nodes), [nodes]);
  const { placed, cycle } = layout;
  const maxDepth = Math.max(0, ...placed.map((p) => p.depth));
  const byLayer: TaskRow[][] = Array.from({ length: maxDepth + 1 }, () => []);
  for (const node of placed) byLayer[node.depth].push(node.task);

  const width = PAD * 2 + (maxDepth + 1) * COLUMN_WIDTH;
  const height = PAD * 2 + Math.max(1, ...byLayer.map((layer) => layer.length)) * ROW_GAP;
  const position = new Map<string, { x: number; y: number }>();
  byLayer.forEach((layer, column) => {
    layer.forEach((task, row) => {
      position.set(task.id, {
        x: PAD + column * COLUMN_WIDTH,
        y: PAD + row * ROW_GAP,
      });
    });
  });

  return (
    <div className="border-b border-[var(--line)]">
      {cycle.length > 0 && (
        <p className="px-4 py-2 text-[11px] text-[var(--danger)]">
          图里有环：{cycle.map((t) => t.id).join('、')}。环上的任务互相等待，永远不会被调度——
          这是分解的缺陷，不是布局的问题。
        </p>
      )}
      <div className="overflow-x-auto">
        <svg width={width} height={height} className="block">
          {placed.map(({ task }) =>
            (task.blocked_by ?? []).map((blocker) => {
              const from = position.get(blocker);
              const to = position.get(task.id);
              if (!from || !to) return null;
              const x1 = from.x + NODE_WIDTH;
              const y1 = from.y + NODE_HEIGHT / 2;
              const x2 = to.x;
              const y2 = to.y + NODE_HEIGHT / 2;
              const mid = (x1 + x2) / 2;
              return (
                <path
                  key={`${blocker}->${task.id}`}
                  d={`M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`}
                  fill="none"
                  stroke="var(--line)"
                  strokeWidth={1}
                />
              );
            })
          )}
          {placed.map(({ task }) => {
            const at = position.get(task.id);
            if (!at) return null;
            const active = task.id === selected;
            const eff = effectiveStatus(task, live);
            const color = statusColor(eff.status);
            return (
              <g key={task.id} onClick={() => onSelect(task.id)} style={{ cursor: 'pointer' }}>
                <rect
                  x={at.x}
                  y={at.y}
                  width={NODE_WIDTH}
                  height={NODE_HEIGHT}
                  fill="var(--paper)"
                  stroke={active ? 'var(--brass)' : 'var(--line)'}
                  strokeWidth={active ? 2 : 1}
                />
                <rect x={at.x} y={at.y} width={3} height={NODE_HEIGHT} fill={color} />
                <text x={at.x + 12} y={at.y + 18} className="mono" fontSize={10} fill="var(--paper-dim)">
                  {task.id.slice(0, 24)}
                </text>
                <text x={at.x + 12} y={at.y + 34} fontSize={11} fill="var(--paper)">
                  {task.task_type}
                </text>
                <text x={at.x + 12} y={at.y + 49} fontSize={10} fill="var(--muted)">
                  {task.owner_unit ?? task.agent_id ?? '未归属'}
                  {' · '}
                  {eff.status}
                  {eff.fromFrame ? '（帧）' : ''}
                  {task.face_id ? ` · ${task.face_id}` : ''}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        一列一层：越往右越晚。边的方向是「等」——左边那条完成后，右边那条才可调度。
        点一个节点看它自己的行。
      </p>
    </div>
  );
}

/** 一条任务的全貌。字段分组按读者的问题排：它是什么、谁负责、等谁、为什么失败、执行到哪。 */
function TaskDetail({
  task,
  live,
  onSelect,
  onClose,
}: {
  task: TaskRow;
  live: Record<string, LiveTaskStatus>;
  onSelect: (id: string) => void;
  onClose: () => void;
}) {
  const leaseExpired =
    task.lease_expires_at != null && new Date(task.lease_expires_at).getTime() < Date.now();

  return (
    <section className="border-t border-[var(--line)]">
      <div className="flex items-center px-4 py-2">
        <span className="mono text-[11px] text-[var(--paper-dim)]">{task.id}</span>
        <LiveStatus row={task} live={live} />
        <button onClick={onClose} className="ml-auto text-[11px] text-[var(--muted)] hover:text-[var(--paper)]">
          收起
        </button>
      </div>
      <dl className="grid grid-cols-[110px_1fr] gap-x-3 gap-y-1 px-4 pb-3 text-[11px]">
        <Field label="类型">{task.task_type}</Field>
        <Field label="归属">
          {task.owner_unit ?? '—'}
          {task.agent_id ? ` · 交付 ${task.agent_id}` : ''}
          {task.bound_by_handover_id ? ` · 交接单 ${task.bound_by_handover_id}` : ''}
        </Field>
        <Field label="服务面">{task.face_id ?? '不服务任何面'}</Field>
        <Field label="目标 / 父">
          {task.goal_id ?? '—'}
          {task.parent_task_id ? ` · 父 ${task.parent_task_id.slice(0, 18)}` : ''}
          {task.sink_task_id ? ` · 收口 ${task.sink_task_id.slice(0, 18)}` : ''}
        </Field>
        <Field label="可调度">{task.is_executable === false ? '否（占位行）' : '是'}</Field>
        <Field label="等谁">
          <Refs ids={task.blocked_by} onSelect={onSelect} empty="不依赖任何任务" />
        </Field>
        <Field label="解锁谁">
          <Refs ids={task.blocks} onSelect={onSelect} empty="没有下游" />
        </Field>
        <Field label="失败">
          {task.error ? (
            <span className="text-[var(--danger)]">
              {task.error_cause ? `[${task.error_cause}] ` : ''}
              {task.error}
            </span>
          ) : (
            '—'
          )}
        </Field>
        <Field label="重试">
          {task.retry_count ?? 0} / {task.max_retries ?? 0}
          {task.retry_not_before ? ` · 不早于 ${fmt(task.retry_not_before)}` : ''}
        </Field>
        <Field label="执行">
          {task.lease_owner ? (
            <span style={{ color: leaseExpired ? 'var(--danger)' : undefined }}>
              {task.lease_owner}
              {task.lease_expires_at ? ` · 租约至 ${fmt(task.lease_expires_at)}` : ''}
              {leaseExpired ? ' · 已过期，持有它的进程可能不在了' : ''}
            </span>
          ) : (
            '没有持有者'
          )}
        </Field>
        <Field label="时间">
          开始 {task.started_at ? fmt(task.started_at) : '未开始'} · 超时 {task.timeout_seconds ?? '—'} 秒
        </Field>
        <Field label="创建 / 更新">
          <Moment iso={task.created_at} /> · <Moment iso={task.updated_at} />
        </Field>
        <Field label="输入">
          <Json value={task.input} />
        </Field>
        {task.result && (
          <Field label="结果">
            <Json value={task.result} />
          </Field>
        )}
      </dl>
    </section>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <>
      <dt className="text-[var(--muted)]">{label}</dt>
      <dd className="text-[var(--paper-dim)]">{children}</dd>
    </>
  );
}

function Refs({
  ids,
  onSelect,
  empty,
}: {
  ids?: string[];
  onSelect: (id: string) => void;
  empty: string;
}) {
  if (!ids || ids.length === 0) return <span className="text-[var(--muted)]">{empty}</span>;
  return (
    <span className="flex flex-wrap gap-1">
      {ids.map((id) => (
        <button
          key={id}
          onClick={() => onSelect(id)}
          className="mono border border-[var(--line)] px-1 text-[10px] hover:border-[var(--brass-dim)]"
        >
          {id.slice(0, 18)}
        </button>
      ))}
    </span>
  );
}

function Json({ value }: { value: Record<string, unknown> }) {
  return (
    <pre className="mono max-h-40 overflow-auto whitespace-pre-wrap break-all text-[10px] text-[var(--muted)]">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

function fmt(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN');
}

/**
 * 状态：快照与帧取新的那个，并标出它是从哪儿读来的。
 *
 * 标记不是装饰：读者要能分清「这是刚才那帧说的」与「这是十几秒前那次轮询说的」，
 * 否则一个比快照新的值会被当成快照的一部分，下一帧把它改掉时看着像读数在跳。
 */
function LiveStatus({
  row,
  live,
}: {
  row: { id: string; status: string; updated_at: string };
  live: Record<string, LiveTaskStatus>;
}) {
  const eff = effectiveStatus(row, live);
  return (
    <span className="inline-flex items-baseline gap-1.5">
      <Status value={eff.status} />
      {eff.fromFrame && (
        <span className="mono text-[10px] text-[var(--brass)]" title="比这次轮询读到的更新">
          帧
        </span>
      )}
    </span>
  );
}

function statusColor(status: string): string {
  switch (status.toLowerCase()) {
    case 'running':
      return 'var(--ok)';
    case 'failed':
      return 'var(--danger)';
    case 'completed':
      return 'var(--paper-dim)';
    default:
      return 'var(--warn)';
  }
}

/**
 * 按最长依赖路径分层，同时点出环。
 *
 * 一个节点在算的过程中沿 `blocked_by` 走回自己正在走的路径，就是环。这时不把深度
 * 写成 0 假装它是一层的起点——那会让一张自相矛盾的图看起来刚刚好。
 */
function computeLayout(nodes: TaskRow[]): { placed: { task: TaskRow; depth: number }[]; depth: Map<string, number>; cycle: TaskRow[] } {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const depth = new Map<string, number>();
  const cycle: TaskRow[] = [];
  const inCycle = new Set<string>();

  const visit = (id: string, stack: Set<string>): number => {
    const known = depth.get(id);
    if (known != null) return known;
    const node = byId.get(id);
    if (!node) return 0;
    if (stack.has(id)) {
      if (!inCycle.has(id)) {
        inCycle.add(id);
        cycle.push(node);
      }
      return 0;
    }
    stack.add(id);
    let level = 0;
    for (const blocker of node.blocked_by ?? []) {
      if (!byId.has(blocker)) continue;
      level = Math.max(level, visit(blocker, stack) + 1);
    }
    stack.delete(id);
    depth.set(id, level);
    return level;
  };

  for (const node of nodes) visit(node.id, new Set());
  return {
    placed: nodes.map((task) => ({ task, depth: depth.get(task.id) ?? 0 })),
    depth,
    cycle,
  };
}

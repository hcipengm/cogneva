import { useMemo, useState } from 'react';
import {
  getProjectBoard,
  getProjectEvm,
  getProjectGantt,
  getProjects,
  getTaskGraph,
  type EvmReading,
  type GanttRow,
  type ProjectRow,
  type ReadingSource,
  type TaskRow,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Status, ViewFrame } from '@/ui/primitives';

/**
 * 项目面：一个项目 ＝ 任务图里的一个 goal 根连同它的后代。
 *
 * 四个视图读的是同一族端点，回答四个不同的问题：
 *   范围：这活被拆成了什么形状；
 *   看板：这些活现在堆在哪个状态上；
 *   甘特：各自什么时候开始、什么时候动过，有没有计划可以对照；
 *   挣值：做得比计划的快还是慢、比预算的省还是费。
 *
 * 没有第二个任务清单。这里的每个数都是任务图的一个投影，现读现算——另存一份
 * 派生值，两份迟早不一致，而读面上分不出哪一份是错的。
 */
type Mode = 'wbs' | 'board' | 'gantt' | 'evm';

const MODES: { id: Mode; label: string }[] = [
  { id: 'wbs', label: '范围' },
  { id: 'board', label: '看板' },
  { id: 'gantt', label: '甘特' },
  { id: 'evm', label: '挣值' },
];

export function ProjectsView() {
  const [mode, setMode] = useState<Mode>('wbs');
  const [picked, setPicked] = useState<string | null>(null);

  const projects = useResource('项目清单', getProjects, 30_000);
  const rows = projects.data?.projects ?? [];

  // 没选过就落在任务最多的那个真项目上。落在未分组桶上会一进门就是一屏
  // 「这桶没有范围结构」，读者会以为整个项目面是坏的。
  const selected =
    picked ?? rows.find((r) => r.kind === 'goal')?.id ?? rows[0]?.id ?? null;
  const current = rows.find((r) => r.id === selected) ?? null;

  return (
    <ViewFrame
      title="项目"
      meta={
        projects.data
          ? `共 ${projects.data.projects.length} 个项目 · ${projects.data.total_tasks} 条任务`
          : '按 30 秒重读'
      }
      actions={
        <div className="flex gap-1">
          {MODES.map((m) => (
            <button
              key={m.id}
              onClick={() => setMode(m.id)}
              className="border px-2 py-1 text-[11px]"
              style={{
                borderColor: mode === m.id ? 'var(--brass-dim)' : 'var(--line)',
                color: mode === m.id ? 'var(--brass)' : 'var(--paper-dim)',
              }}
            >
              {m.label}
            </button>
          ))}
        </div>
      }
    >
      {projects.error ? (
        <ReadError what="项目清单" error={projects.error} onRetry={projects.refresh} />
      ) : !projects.data && projects.loading ? (
        <Loading what="项目清单" />
      ) : rows.length === 0 ? (
        <Empty>任务图里一条任务都没有，所以还没有项目。</Empty>
      ) : (
        <>
          <ProjectStrip rows={rows} selected={selected} onSelect={setPicked} />
          {current && (
            <ProjectPanel mode={mode} project={current} />
          )}
        </>
      )}
    </ViewFrame>
  );
}

/** 项目一览。条数、可执行数、积压分开写：混成一个数读者读不出「有没有活可干」。 */
function ProjectStrip({
  rows,
  selected,
  onSelect,
}: {
  rows: ProjectRow[];
  selected: string | null;
  onSelect: (id: string) => void;
}) {
  return (
    <div className="border-b border-[var(--line)]">
      {rows.map((r) => {
        const active = r.id === selected;
        const ungrouped = r.kind === 'ungrouped';
        return (
          <button
            key={r.id}
            onClick={() => onSelect(r.id)}
            className="flex w-full items-baseline gap-3 border-b border-[var(--line)] px-4 py-1.5 text-left last:border-b-0"
            style={{ background: active ? 'var(--ink-800)' : 'transparent' }}
          >
            <span
              className="mono min-w-0 flex-1 truncate text-[11px]"
              style={{ color: ungrouped ? 'var(--muted)' : 'var(--paper)' }}
              title={r.id}
            >
              {ungrouped ? '未分组（没有 goal_id 的任务）' : r.id}
            </span>
            <span className="mono text-[11px] text-[var(--paper-dim)]">
              {r.tasks} 条
              {r.executables !== r.tasks && (
                <span className="text-[var(--muted)]"> · 可执行 {r.executables}</span>
              )}
            </span>
            <span
              className="mono w-16 text-right text-[11px]"
              style={{ color: r.blocked > 0 ? 'var(--warn)' : 'var(--muted)' }}
            >
              等 {r.blocked}
            </span>
            <span className="mono w-24 text-right text-[11px] text-[var(--muted)]">
              <Moment iso={r.last_updated_at} />
            </span>
          </button>
        );
      })}
    </div>
  );
}

function ProjectPanel({ mode, project }: { mode: Mode; project: ProjectRow }) {
  switch (mode) {
    case 'wbs':
      return <ScopePanel project={project} />;
    case 'board':
      return <BoardPanel project={project} />;
    case 'gantt':
      return <GanttPanel project={project} />;
    default:
      return <EarnedValuePanel project={project} />;
  }
}

/** 一份读数的来路。每个面板都印一行，省得读者去猜这个数字是谁算的。 */
function Provenance({ source }: { source: ReadingSource | null }) {
  if (!source) return null;
  return (
    <div className="border-b border-[var(--line)] px-4 py-1 text-[10px] text-[var(--muted)]">
      读自 <span className="mono">{source.table}</span> via{' '}
      <span className="mono">{source.reader}</span> · <Moment iso={source.read_at} />
    </div>
  );
}

/**
 * 范围：一条任务挂在谁下面。
 *
 * 树按 `parent_task_id` 现拼。父指针指向这一批里不存在的行时，那一行仍然画出来，
 * 但标成「父不在图里」——静默把它提到根上，一棵有断口的树看起来和一棵完整的树
 * 一模一样。
 */
function ScopePanel({ project }: { project: ProjectRow }) {
  const ungrouped = project.kind === 'ungrouped';
  const graph = useResource(
    ungrouped ? '未分组的任务' : `范围树（${project.id}）`,
    () => getTaskGraph(ungrouped ? {} : { goal_id: project.id }),
    15_000
  );

  const nodes = useMemo(() => {
    // 未分组桶只能整图拉回来再挑：这一桶的定义就是「没有 goal_id」，没有可过滤的键。
    const all = graph.data?.nodes ?? [];
    return ungrouped ? all.filter((n) => !n.goal_id) : all;
  }, [graph.data, ungrouped]);

  const tree = useMemo(() => buildTree(nodes), [nodes]);

  if (graph.error) {
    return <ReadError what="范围树" error={graph.error} onRetry={graph.refresh} />;
  }
  if (!graph.data && graph.loading) return <Loading what="范围树" />;
  if (nodes.length === 0) {
    return <Empty>这个项目在任务图里没有任务。</Empty>;
  }

  return (
    <>
      <Readings
        from={graph}
        source={ungrouped ? 'GET /api/v1/tasks/graph' : 'GET /api/v1/tasks/graph?goal_id=' + project.id}
        caveat="树按 parent_task_id 现拼，没有父指针的行按根画；父指针指向图外的那一行会被标出来。"
      >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="节点" value={nodes.length} />
          <Reading label="根" value={tree.roots.length} />
          <Reading
            label="有父指针"
            value={nodes.filter((n) => n.parent_task_id).length}
            tone={nodes.some((n) => n.parent_task_id) ? 'ok' : 'warn'}
            note={nodes.some((n) => n.parent_task_id) ? undefined : '这一批里没有一个拆出层级'}
          />
          <Reading
            label="父在图外"
            value={tree.orphans.length}
            tone={tree.orphans.length > 0 ? 'danger' : 'plain'}
            note={tree.orphans.length > 0 ? '分解产出了指向不存在父节点的行' : undefined}
          />
        </div>
      </Readings>

      <div className="overflow-auto">
        {tree.flattened.map(({ node, depth, orphan }) => (
          <div
            key={node.id}
            className="flex items-baseline gap-2 border-b border-[var(--line)] px-4 py-1"
          >
            <span style={{ width: depth * 14 }} />
            <span className="mono text-[10px] text-[var(--muted)]">{depth === 0 ? '▪' : '└'}</span>
            <span className="mono w-40 shrink-0 truncate text-[11px] text-[var(--paper-dim)]" title={node.id}>
              {node.id.slice(0, 20)}
            </span>
            <span className="min-w-0 flex-1 truncate text-[11px]" title={titleOf(node)}>
              {titleOf(node)}
            </span>
            {orphan && (
              <span className="mono text-[10px] text-[var(--danger)]">父不在图里</span>
            )}
            {node.is_executable === false && (
              <span className="mono text-[10px] text-[var(--muted)]">占位</span>
            )}
            <span className="w-24 shrink-0 text-right">
              <Status value={node.status} />
            </span>
          </div>
        ))}
      </div>
    </>
  );
}

interface FlatNode {
  node: TaskRow;
  depth: number;
  orphan: boolean;
}

/** 按父指针把行拼成树。环与孤儿都不吞掉：环断在一处并标出来，孤儿按根画并标出来。 */
function buildTree(nodes: TaskRow[]): {
  roots: TaskRow[];
  orphans: TaskRow[];
  flattened: FlatNode[];
} {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const children = new Map<string, TaskRow[]>();
  const roots: TaskRow[] = [];
  const orphans: TaskRow[] = [];

  for (const n of nodes) {
    const parent = n.parent_task_id;
    if (!parent) {
      roots.push(n);
      continue;
    }
    if (!byId.has(parent)) {
      roots.push(n);
      orphans.push(n);
      continue;
    }
    const bucket = children.get(parent) ?? [];
    bucket.push(n);
    children.set(parent, bucket);
  }

  const flattened: FlatNode[] = [];
  const seen = new Set<string>();
  const walk = (node: TaskRow, depth: number, orphan: boolean) => {
    // 环：走回自己在走的路径就停在这里，并把这一行标成断口。
    if (seen.has(node.id)) return;
    seen.add(node.id);
    flattened.push({ node, depth, orphan });
    for (const child of children.get(node.id) ?? []) {
      walk(child, depth + 1, false);
    }
  };
  for (const r of roots) walk(r, 0, orphans.includes(r));

  // 一个环上的行可能一次都没被走到（它们没有根）。单独列出来，不静默消失。
  for (const n of nodes) {
    if (!seen.has(n.id)) {
      flattened.push({ node: n, depth: 0, orphan: true });
    }
  }

  return { roots, orphans, flattened };
}

function titleOf(t: TaskRow): string {
  for (const key of ['name', 'task', 'description', 'goal']) {
    const v = t.input?.[key];
    if (typeof v === 'string' && v.trim()) return v.trim().slice(0, 120);
  }
  return t.id;
}

/** 看板：六个状态各堆了多少。列上限还没有出处，所以显式写「没配上限」。 */
function BoardPanel({ project }: { project: ProjectRow }) {
  const board = useResource(`看板（${project.id}）`, () => getProjectBoard(project.id), 15_000);

  if (board.error) {
    return <ReadError what="看板" error={board.error} onRetry={board.refresh} />;
  }
  if (!board.data && board.loading) return <Loading what="看板" />;
  const data = board.data;
  if (!data) return <Empty>读不到这个项目的看板。</Empty>;

  return (
    <>
      <Readings
        from={board}
        source={`GET /api/v1/projects/${project.id}/board`}
        caveat="六列是同一批任务按状态分组的结果，加起来等于总数；列上限还没有出处，所以每列都写「没配上限」。"
      >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="总数" value={data.total} />
        </div>
      </Readings>
      <Provenance source={data.source} />
      <div className="grid grid-cols-6 gap-px border-b border-[var(--line)] p-2">
        {data.columns.map((c) => (
          <div key={c.status} className="border border-[var(--line)] p-2">
            <div className="mb-1 flex items-baseline justify-between">
              <Status value={c.status} />
              <span className="mono text-[14px]">{c.count}</span>
            </div>
            <div className="text-[10px] text-[var(--muted)]">
              {c.wip_limit === null ? '没配上限' : `上限 ${c.wip_limit}`}
            </div>
            {c.over_limit === true && (
              <div className="mt-1 text-[10px] text-[var(--danger)]">已超限</div>
            )}
          </div>
        ))}
      </div>
      <div className="px-4 py-2 text-[11px] text-[var(--muted)]">
        上限是准入控制，不是画在列头的红字。计划基线（期 2）落地之前这里不显示上限，
        因为填一个默认数会让一列看起来在拦派发，而实际上没有任何东西在拦。
      </div>
    </>
  );
}

/** 图上最多画多少行。超出的行数写在下面，不做静默截断。 */
const GANTT_LIMIT = 200;

/** 甘特：实际时间线画得出来，计划基线还没有出处。缺的那一头必须说出来。 */
function GanttPanel({ project }: { project: ProjectRow }) {
  const gantt = useResource(`甘特（${project.id}）`, () => getProjectGantt(project.id), 15_000);

  if (gantt.error) {
    return <ReadError what="甘特" error={gantt.error} onRetry={gantt.refresh} />;
  }
  if (!gantt.data && gantt.loading) return <Loading what="甘特" />;
  const data = gantt.data;
  if (!data) return <Empty>读不到这个项目的甘特。</Empty>;

  const drawn = data.rows.slice(0, GANTT_LIMIT);
  const span = timeSpan(drawn);
  const unplanned = data.rows.filter((r) => r.unplanned).length;

  return (
    <>
      <Readings
        from={gantt}
        source={`GET /api/v1/projects/${project.id}/gantt`}
        caveat="时间轴取这批任务里最早的 created_at 到最晚的 updated_at；条是实际起止，不是计划。"
      >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="行" value={data.rows.length} />
          <Reading
            label="未排期"
            value={unplanned}
            tone={unplanned > 0 ? 'warn' : 'ok'}
            note={unplanned > 0 ? '没有计划基线，画不出计划条' : undefined}
          />
          <Reading
            label="有计划基线"
            value={data.has_plan ? '有' : '没有'}
            tone={data.has_plan ? 'ok' : 'warn'}
          />
        </div>
      </Readings>
      <Provenance source={data.source} />

      {data.plan_error && (
        <div className="border-b border-[var(--line)] px-4 py-2 text-[11px] text-[var(--warn)]">
          读不到计划基线：{data.plan_error}。因此下面每行的「未排期」可能是**读不到**，
          不是没排期——这两件事在这一屏上分不开，所以先说这一句。
        </div>
      )}

      {!data.plan_error && !data.has_plan && (
        <div className="border-b border-[var(--line)] px-4 py-2 text-[11px] text-[var(--warn)]">
          这个项目还没有计划基线：下面的条画的是**实际**起止（创建到最近一次更新），
          不是计划。缺计划的一头留空，不画成零长度——零长度的条会被读成「计划已完成」。
        </div>
      )}

      {data.has_plan && (
        <div className="border-b border-[var(--line)] px-4 py-2 text-[11px] text-[var(--muted)]">
          每行两条：上面细的是**计划**（基线给的起止），下面粗的是**实际**
          （创建到最近一次更新）。两条一起看才看得出偏差。
        </div>
      )}

      <div className="px-4 py-2">
        {drawn.map((r) => (
          <GanttBar key={r.id} row={r} span={span} />
        ))}
      </div>

      {data.rows.length > drawn.length && (
        <div className="border-t border-[var(--line)] px-4 py-2 text-[11px] text-[var(--muted)]">
          只画了前 {drawn.length} 行，还有 {data.rows.length - drawn.length} 行没画。
        </div>
      )}
    </>
  );
}

function timeSpan(rows: GanttRow[]): { start: number; end: number } | null {
  let start = Number.POSITIVE_INFINITY;
  let end = Number.NEGATIVE_INFINITY;
  // 计划的那一头也要算进来：只按实际算时间轴，计划条会被裁到画布外面，
  // 而裁掉的样子（一根短条）看起来像是计划本来就这么短。
  const consider = (v: string | null | undefined) => {
    if (!v) return;
    const t = Date.parse(v);
    if (Number.isFinite(t)) {
      start = Math.min(start, t);
      end = Math.max(end, t);
    }
  };
  for (const r of rows) {
    consider(r.created_at);
    consider(r.updated_at);
    consider(r.planned_start);
    consider(r.planned_end);
  }
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return null;
  return { start, end };
}

/** 把两个时刻换成时间轴上的百分比区间；缺一头就返回 null（不画零长度条）。 */
function barSpan(
  from: string | null,
  to: string | null,
  span: { start: number; end: number } | null,
): { left: number; width: number } | null {
  if (!span || !from || !to) return null;
  const a = Date.parse(from);
  const b = Date.parse(to);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  const total = span.end - span.start;
  return {
    left: ((a - span.start) / total) * 100,
    width: Math.max(((b - a) / total) * 100, 0.4),
  };
}

function GanttBar({ row, span }: { row: GanttRow; span: { start: number; end: number } | null }) {
  const actual = barSpan(row.created_at, row.updated_at, span);
  const planned = barSpan(row.planned_start, row.planned_end, span);

  return (
    <div className="flex items-center gap-2 border-b border-[var(--line)] py-0.5">
      <span className="mono w-40 shrink-0 truncate text-[10px] text-[var(--paper-dim)]" title={row.id}>
        {row.id.slice(0, 18)}
      </span>
      <span className="min-w-0 flex-1 truncate text-[10px] text-[var(--muted)]" title={row.title}>
        {row.title}
      </span>
      <div className="relative h-4 w-64 shrink-0">
        {/* 上轨：计划。没有基线就没有这一条——不画一根零长度的，那会被读成「计划已完成」。 */}
        {planned && (
          <div
            className="absolute top-0 h-1"
            style={{ left: `${planned.left}%`, width: `${planned.width}%`, background: 'var(--brass)' }}
            title={`计划 ${row.planned_start} → ${row.planned_end ?? '（时长未知）'}`}
          />
        )}
        {/* 下轨：实际。 */}
        <div className="absolute bottom-0 h-2.5 w-full bg-[var(--line)]">
          {actual && (
            <div
              className="absolute top-0 h-full"
              style={{
                left: `${actual.left}%`,
                width: `${actual.width}%`,
                background: row.unplanned ? 'var(--brass-dim)' : 'var(--brass)',
              }}
              title={`实际 ${row.created_at} → ${row.updated_at}`}
            />
          )}
        </div>
      </div>
      <span className="w-20 shrink-0 text-right">
        <Status value={row.status} />
      </span>
      <span className="mono w-12 shrink-0 text-right text-[10px] text-[var(--muted)]">
        {row.unplanned ? '未排期' : row.planned_end ? '有计划' : '有起期'}
      </span>
    </div>
  );
}

/**
 * 挣值：三个量都现算，不落表。
 *
 * 「量不了」与「是零」必须分开写：计划价值为 0 是还没开工、成本为 0 是一分没花，
 * 而量不出来是这一部署回答不了。把后者印成 0，读者会当成好消息。
 */
function EarnedValuePanel({
  project,
}: {
  project: ProjectRow;
}) {
  const evm = useResource(`挣值（${project.id}）`, () => getProjectEvm(project.id), 15_000);

  if (evm.error) {
    return <ReadError what="挣值" error={evm.error} onRetry={evm.refresh} />;
  }
  if (!evm.data && evm.loading) return <Loading what="挣值" />;
  const data = evm.data;
  if (!data) return <Empty>读不到这个项目的挣值。</Empty>;

  const pv = evmCell(data.planned_value);
  const ev = evmCell(data.earned_value);
  const ac = evmCell(data.actual_cost);
  const baselinedAc = evmCell(data.baselined_actual_cost);
  const unit = data.unit;

  return (
    <>
      <Readings
        from={evm}
        source={`GET /api/v1/projects/${project.id}/evm`}
        caveat={data.assumption}
      >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading
            label={`计划价值（${unit}）`}
            value={pv.text}
            tone={pv.tone}
            note={pv.note}
          />
          <Reading
            label={`挣值（${unit}）`}
            value={ev.text}
            tone={ev.tone}
            note={ev.note}
          />
          <Reading
            label={`实际成本（${unit}）`}
            value={ac.text}
            tone={ac.tone}
            note={ac.note}
          />
          <Reading
            label={`有基线的成本（${unit}）`}
            value={baselinedAc.text}
            tone={baselinedAc.tone}
            note={baselinedAc.note}
          />
          <Reading
            label="成本绩效"
            value={indexText(data.cost_performance_index)}
            tone={indexTone(data.cost_performance_index)}
            note={
              data.cost_performance_index === null
                ? '挣值或有基线成本量不出来，指数就给不出——不是 1.0'
                : '挣值 / 有基线任务的实际成本；大于 1 是省。分母与分子覆盖同一批任务'
            }
          />
          <Reading
            label="进度绩效"
            value={indexText(data.schedule_performance_index)}
            tone={indexTone(data.schedule_performance_index)}
            note={
              data.schedule_performance_index === null
                ? '挣值或计划价值量不出来，指数就给不出——不是 1.0'
                : '挣值 / 计划价值；大于 1 是快'
            }
          />
        </div>
      </Readings>
      <Provenance source={data.source} />

      <div className="border-b border-[var(--line)] px-4 py-2 text-[11px]">
        <div className="flex items-baseline justify-between">
          <span className="text-[var(--paper-dim)]">无归因用量</span>
          <span className="mono">
            {data.unattributed_tokens === null
              ? '量不了'
              : `${data.unattributed_tokens.toLocaleString()} ${unit}`}
          </span>
        </div>
        <p className="mt-1 text-[var(--muted)]">
          {data.unattributed_tokens === null
            ? '逐笔 token 账读不到，归因缺口也量不出来。'
            : `账上没有 task_id 的用量，不并进实际成本（它不属于任何项目），也不折成 0。窗口 ${
                data.unattributed_window_start
                  ? new Date(data.unattributed_window_start).toLocaleString()
                  : '起点未知'
              } 起至本次读取；换个窗口这个数就变，别把它当常数。这里只报数不报比例：
              这条账没有项目维度，拿它除本项目的花销，项目越小算出的比例越大。`}
        </p>
        <p className="mt-1 text-[var(--muted)]">
          账上有花销的任务 {data.attribution.tasks_with_cost} 条；有基线预算但账上一条花销
          都没有的已完成任务 {data.attribution.baselined_tasks_without_cost} 条——那不是
          「免费」，是这笔账上看不到它。另有 {data.attribution.tasks_outside_baseline} 条
          任务没有基线，它们的预算不在挣值里。
        </p>
        <p className="mt-1 text-[var(--muted)]">{data.assumption}</p>
      </div>
    </>
  );
}

/** 一个量的三种印法：有数、量不了、以及有数但读法要跟着说明走。 */
function evmCell(reading: EvmReading): {
  text: string;
  tone: 'ok' | 'warn' | 'plain';
  note: string;
} {
  if (reading.value === null) {
    return {
      text: '量不了',
      tone: 'warn',
      note: reading.unmeasurable ?? '这一部署回答不了这个量',
    };
  }
  return { text: reading.value.toLocaleString(), tone: 'plain', note: reading.basis };
}

/** 指数只印两位；量不出来时印破折号之外还要在 note 里说清为什么。 */
function indexText(v: number | null): string {
  return v === null ? '—' : v.toFixed(2);
}

function indexTone(v: number | null): 'ok' | 'warn' | 'danger' | 'plain' {
  if (v === null) return 'warn';
  if (v >= 1) return 'ok';
  return v >= 0.8 ? 'warn' : 'danger';
}

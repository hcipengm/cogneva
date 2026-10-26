import { useEffect, useMemo, useState } from 'react';
import {
  getProjectBoard,
  getProjectEvm,
  getProjectGantt,
  getProjects,
  getTaskGraph,
  type BehindRow,
  type EvmReading,
  type GanttRow,
  type OverrunRow,
  type ProjectRow,
  type ReadingSource,
  type TaskRow,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import {
  changedSinceSnapshot,
  projectRevision,
  rowsForProject,
  type LiveTaskRow,
} from '@/store/liveProjects';
import { useLiveProjectStore } from '@/store/liveProjectStore';
import { STRIP_CAP, visibleProjects } from '@/views/projectStrip';
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

/**
 * 项目一览。条数、可执行数、积压分开写：混成一个数读者读不出「有没有活可干」。
 *
 * 默认只铺前几行：goal 与任务在这里常常是一比一（实测 3831 条任务上有 3726 个
 * goal），全铺出来就是几万像素的一墙 UUID，而下面的范围、看板、甘特、挣值四张屏
 * 正好被推到墙后面——视图全在，读者却停在第一屏。剩下多少没列出由数字说清楚，
 * 过滤与展开都由读者决定。
 */
function ProjectStrip({
  rows,
  selected,
  onSelect,
}: {
  rows: ProjectRow[];
  selected: string | null;
  onSelect: (id: string) => void;
}) {
  const [query, setQuery] = useState('');
  const [expanded, setExpanded] = useState(false);
  const view = visibleProjects(rows, { query, expanded, selected });

  return (
    <div className="border-b border-[var(--line)]">
      <div className="flex items-baseline gap-2 border-b border-[var(--line)] px-4 py-1.5">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={`按 goal 过滤（共 ${rows.length} 个）`}
          className="mono min-w-0 flex-1 border px-1.5 py-0.5 text-[11px]"
          style={{ background: 'transparent', borderColor: 'var(--line)', color: 'var(--paper)' }}
        />
        <button
          onClick={() => setExpanded((v) => !v)}
          className="mono shrink-0 border px-1.5 py-0.5 text-[11px]"
          style={{ borderColor: 'var(--line)', color: 'var(--paper-dim)' }}
        >
          {expanded ? `只看前 ${STRIP_CAP} 个` : `显示全部 ${rows.length} 个`}
        </button>
      </div>
      {view.shown.map((r) => {
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
      {(view.hidden > 0 || view.filteredOut > 0) && (
        <div className="px-4 py-1 text-[10px] text-[var(--muted)]">
          {view.hidden > 0 && (
            <>还有 {view.hidden} 个没列出（按任务数从多到少排，上面是最大的那些）。</>
          )}
          {view.filteredOut > 0 && <>过滤词筛掉了 {view.filteredOut} 个。</>}
        </div>
      )}
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
 * 这一面跟着事件流走的那一半。
 *
 * 四个视图都是定时重读的快照，而 `task_status_change` 一直在总线上走。帧到了就
 * 立刻重读这一面，让服务端按新状态把四张屏一起重算——帧是触发，不是真值：帧里
 * 只有「这条任务现在是什么状态」，没有基线、没有预算，拿它去改服务端的数，两份
 * 账就写成了一本。
 *
 * 定时轮询照旧留着当兜底：连接断了、帧丢了，读数仍然会靠它追上来，只是慢一拍。
 *
 * 返回的是「比手上这份快照更晚」的那几条，交给画面标出来。重读成功之后快照时刻
 * 越过它们，这块自己就空了——所以它显示的永远是表里还没有的，不是表里也有、
 * 这里再抄一遍。
 */
function useFrameFedFace(
  project: ProjectRow,
  snapshotAt: string | null,
  refresh: () => void
): LiveTaskRow[] {
  const rows = useLiveProjectStore((s) => s.rows);
  const owned = useLiveProjectStore((s) => s.owned);
  const revision = useMemo(
    () => projectRevision(project.id, owned, rows),
    [project.id, owned, rows]
  );
  useEffect(() => {
    if (!revision) return;
    // 一批任务往往一起换状态（同一轮派发、同一批结束）。逐帧重读会把同一份字节
    // 连取十几遍，而读者要的是它们都到了之后的那一份，所以合并成一次。
    const timer = window.setTimeout(refresh, 600);
    return () => window.clearTimeout(timer);
  }, [revision, refresh]);
  return useMemo(
    () => changedSinceSnapshot(snapshotAt, rowsForProject(project.id, owned, rows)),
    [snapshotAt, project.id, owned, rows]
  );
}

/** 总线上比这一拍更晚的状态变化。表里不可能有这几条，所以单列出来并标来路。 */
function FreshNodeChanges({ rows, what }: { rows: LiveTaskRow[]; what: string }) {
  if (rows.length === 0) return null;
  const shown = rows.slice(0, 8);
  return (
    <div className="border-b border-[var(--line)] px-4 py-2">
      <div className="mb-1 text-[10px] text-[var(--muted)]">
        总线上比这张{what}更晚，表里还读不到——正在重读这一面
      </div>
      {shown.map((r) => (
        <div key={r.taskId} className="flex items-baseline gap-2 text-[11px]">
          <span
            className="mono w-40 shrink-0 truncate text-[var(--paper-dim)]"
            title={r.taskId}
          >
            {r.taskId.slice(0, 20)}
          </span>
          <Status value={r.status} />
          <span className="text-[10px] text-[var(--muted)]">
            <Moment iso={r.lastAt} />
          </span>
        </div>
      ))}
      {rows.length > shown.length && (
        <div className="text-[10px] text-[var(--muted)]">
          还有 {rows.length - shown.length} 条
        </div>
      )}
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

  // 这一面是唯一知道「哪条任务归这个项目」的地方，其余三面据此判断一帧要不要
  // 惊动自己。登记只增不删：归属不随状态变化而变。
  const own = useLiveProjectStore((s) => s.own);
  useEffect(() => {
    if (nodes.length > 0) own(project.id, nodes.map((n) => n.id));
  }, [own, project.id, nodes]);

  const fresh = useFrameFedFace(project, graph.at, graph.refresh);
  const live = useMemo(() => new Map(fresh.map((r) => [r.taskId, r])), [fresh]);

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

      <FreshNodeChanges rows={fresh} what="范围树" />

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
              <Status value={live.get(node.id)?.status ?? node.status} />
            </span>
            {live.has(node.id) && (
              <span className="mono w-12 shrink-0 text-[10px] text-[var(--brass)]">刚变</span>
            )}
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

/** 看板：六个状态各堆了多少，以及每一列的上限拦没拦住新活。 */
function BoardPanel({ project }: { project: ProjectRow }) {
  const board = useResource(`看板（${project.id}）`, () => getProjectBoard(project.id), 15_000);
  const fresh = useFrameFedFace(project, board.at, board.refresh);

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
        caveat="六列是同一批任务按状态分组的结果，加起来等于总数；上限来自部署配置，没配的列写「没配上限」而不是 0。"
      >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="总数" value={data.total} />
        </div>
      </Readings>
      <Provenance source={data.source} />
      <FreshNodeChanges rows={fresh} what="看板" />
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
        上限是准入控制，不是画在列头的红字：到达上限时，这一列的活不再派发，任务留在
        待办里等下一轮，所以「已超限」是这一列正在拦住新活的读数。上限由部署配置给出
        （发布侧与这里读的是同一份），没配的列就是不限。
      </div>
    </>
  );
}

/** 图上最多画多少行。超出的行数写在下面，不做静默截断。 */
const GANTT_LIMIT = 200;

/** 甘特：计划与实际情况各画一条；缺计划基线的那一行留空，不画成零长度。 */
function GanttPanel({ project }: { project: ProjectRow }) {
  const gantt = useResource(`甘特（${project.id}）`, () => getProjectGantt(project.id), 15_000);
  const fresh = useFrameFedFace(project, gantt.at, gantt.refresh);
  const live = useMemo(() => new Map(fresh.map((r) => [r.taskId, r])), [fresh]);

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
      <FreshNodeChanges rows={fresh} what="甘特" />

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
          <GanttBar key={r.id} row={r} span={span} live={live.get(r.id)?.status} />
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

/**
 * 一行甘特。`live` 是总线上比这份快照更晚的那个状态：有它就显示它并标「刚变」，
 * 条形仍按快照的时刻画——帧里没有时刻线，拿它去改条长就是编一根没发生过的条。
 */
function GanttBar({
  row,
  span,
  live,
}: {
  row: GanttRow;
  span: { start: number; end: number } | null;
  live?: string;
}) {
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
        <Status value={live ?? row.status} />
      </span>
      {live !== undefined && live !== row.status && (
        <span className="mono w-10 shrink-0 text-[10px] text-[var(--brass)]">刚变</span>
      )}
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
  const fresh = useFrameFedFace(project, evm.at, evm.refresh);

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
      <FreshNodeChanges rows={fresh} what="挣值" />

      <NodeList
        title="超支的节点"
        empty="一条也没超预算"
        unmeasured="基线或逐笔账读不到，谁超了量不出来"
        rows={nodeRows<OverrunRow>(data.overruns, (row) => ({
          id: row.task_id,
          headline: row.title,
          detail: `超出 ${(row.spent_tokens - row.budget_tokens).toLocaleString()} ${unit}`,
          note: `花 ${row.spent_tokens.toLocaleString()} / 预算 ${row.budget_tokens.toLocaleString()}`,
        }))}
      />
      <NodeList
        title="过了计划止期还没跑完的节点"
        empty="没有节点过了止期还在等或在跑"
        unmeasured="计划基线读不到，落后量不出来"
        rows={nodeRows<BehindRow>(data.behind_schedule, (row) => ({
          id: row.task_id,
          headline: row.title,
          detail: `过了 ${formatOverdue(row.overdue_secs)}`,
          note: `${row.status} · 计划止期 ${new Date(row.planned_end).toLocaleString()}`,
        }))}
      />

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
          账上有花销的任务 {countOf(data.attribution.tasks_with_cost)}；有基线预算但账上一条
          花销都没有的已完成任务 {countOf(data.attribution.baselined_tasks_without_cost)}
          ——那不是「免费」，是这笔账上看不到它。另有{' '}
          {countOf(data.attribution.tasks_outside_baseline)}任务没有基线，它们的预算不在
          挣值里。
        </p>
        <p className="mt-1 text-[var(--muted)]">{data.assumption}</p>
      </div>
    </>
  );
}

/**
 * 一张逐节点表。`null` 与空表是两件事：前者是这一屏回答不了「谁」，后者是
 * 回答得出、答案就是一条也没有。把两者印成同一句，读者会以为系统查过了。
 */
function NodeList({
  title,
  empty,
  unmeasured,
  rows,
}: {
  title: string;
  empty: string;
  unmeasured: string;
  rows: { id: string; headline: string; detail: string; note: string }[] | null;
}) {
  return (
    <div className="border-b border-[var(--line)] px-4 py-2 text-[11px]">
      <div className="flex items-baseline justify-between">
        <span className="text-[var(--paper-dim)]">{title}</span>
        <span className="mono">{rows === null ? '量不了' : `${rows.length} 条`}</span>
      </div>
      {rows === null ? (
        <p className="mt-1 text-[var(--muted)]">{unmeasured}</p>
      ) : rows.length === 0 ? (
        <p className="mt-1 text-[var(--muted)]">{empty}</p>
      ) : (
        <ul className="mt-1 space-y-1">
          {rows.map((row) => (
            <li key={row.id} className="flex flex-wrap items-baseline gap-x-2">
              <span className="truncate">{row.headline}</span>
              <span className="mono text-[var(--warn)]">{row.detail}</span>
              <span className="text-[var(--muted)]">{row.note}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** 把服务端那两张表翻成一行行字；`null` 原样传下去，别折成空表。 */
function nodeRows<T>(
  rows: T[] | null,
  map: (row: T) => { id: string; headline: string; detail: string; note: string },
): { id: string; headline: string; detail: string; note: string }[] | null {
  return rows === null ? null : rows.map(map);
}

/** 过了多久：秒级到天级，读者一眼能分出「差几分钟」和「差几天」。 */
function formatOverdue(secs: number): string {
  if (secs < 60) return `${secs} 秒`;
  if (secs < 3600) return `${Math.round(secs / 60)} 分钟`;
  if (secs < 86400) return `${Math.round(secs / 3600)} 小时`;
  return `${Math.round(secs / 86400)} 天`;
}

/**
 * 计数格。`null` 是「没量过」，不能印成 0——0 在这一屏上是一个具体的结论
 * （「一条都没有」），而没量过是「不知道」。
 */
function countOf(n: number | null): string {
  return n === null ? '量不了' : `${n} 条`;
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

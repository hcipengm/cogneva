import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
  type ProjectView,
  type ReadingSource,
  type TaskRow,
} from '@/api/client';
import {
  AuditTimeline,
  ExportCsvButton,
  MoveCardControl,
  NewProjectForm,
  NewTaskForm,
  PlanButton,
  ReparentControl,
} from '@/projects/projectWrite';
import { useResource } from '@/hooks/useResource';
import { useFrameDrivenRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey, ticksFor } from '@/store/frameTick';
import { useFrameTickStore } from '@/store/frameTickStore';
import {
  changedSinceSnapshot,
  projectRevision,
  rowsForProject,
  type LiveTaskRow,
} from '@/store/liveProjects';
import { useLiveProjectStore } from '@/store/liveProjectStore';
import { STRIP_CAP, visibleProjects } from '@/views/projectStrip';
import {
  Button,
  Empty,
  Loading,
  Moment,
  ReadError,
  Reading,
  Readings,
  Status,
  ViewFrame,
} from '@/ui/primitives';

import { Note } from '@/ui/Note';
/**
 * 项目面：一个项目 ＝ 任务图里的一个 goal 根连同它的后代。
 *
 * 四个视图读的是同一族端点，回答四个不同的问题：
 *   范围（WBS，工作分解结构）：这活被拆成了什么形状、挂在谁下面；
 *   看板（Kanban）：这些活现在堆在哪个状态上、每列的上限拦没拦住新活；
 *   甘特图（Gantt）：各自什么时候开始、什么时候动过，有没有计划可以对照；
 *   挣值（EVM，挣值管理）：做得比计划的快还是慢、比预算的省还是费。
 *
 * 中文名是意译，认不出这是行业里哪张图；英文名是通用叫法，认不出这页在说什么。
 * 两个都写。
 *
 * 没有第二个任务清单。这里的每个数都是任务图的一个投影，现读现算——另存一份
 * 派生值，两份迟早不一致，而读面上分不出哪一份是错的。
 */
type Mode = ProjectView | 'audit';

// 四个按钮各带原本的英文名。中文名是意译，靠它认不出这是行业里那张图；英文名是
// 通用叫法，靠它认不出这页在说什么。两个一起写，读者才不用先在脑子里做一次翻译。
//
// 第五个不是第四张图的同族：前四个回答「这活是什么形状」，它回答「谁把它改成
// 这样的」。放在同一排，是因为客户问的下一句永远是这个。
const MODES: { id: Mode; label: string }[] = [
  { id: 'wbs', label: '范围 WBS' },
  { id: 'board', label: '看板 Kanban' },
  { id: 'gantt', label: '甘特图 Gantt' },
  { id: 'evm', label: '挣值 EVM' },
  { id: 'audit', label: '改动 Audit' },
];

/**
 * 项目清单：分页读，读到的页在本地累起来。
 *
 * 项目数没有上界（一个 goal 一行），所以这一读是分页的；而界面要的是「一共有多少」
 * 与「我手上这些是全部还是前一段」。两件事分别对应响应里的 `total_projects` 与
 * `next_cursor`，缺一个就会出现「读了 1000 个，屏上写 1000 个项目」——那是个假数，
 * 而且是读者无从核对的那一种。
 *
 * 「再多读」按游标追加，不是把 `limit` 调大：服务端每页有上限，调大只会拿到同一页，
 * 而按钮看起来按了、列表一动不动。
 */
function useProjectPages() {
  const [rows, setRows] = useState<ProjectRow[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [totalProjects, setTotalProjects] = useState(0);
  const [totalTasks, setTotalTasks] = useState(0);
  const [loaded, setLoaded] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);

  // 首屏：重读总是从第一页开始，否则刷新之后会把新的一页接到旧的那一串后面。
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getProjects({ limit: 1000 })
      .then((page) => {
        if (cancelled) return;
        setRows(page.projects);
        setNextCursor(page.next_cursor);
        setTotalProjects(page.total_projects);
        setTotalTasks(page.total_tasks);
        setLoaded(true);
        setError(null);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError((e as { message?: string }).message ?? '读项目清单失败');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [revision]);

  // 定时那一拍是兜底：连接断了、帧丢了，读数仍然追得上，只是慢一拍。
  //
  // 只在「已经读全了」的时候定时重读。读到一半还有下一页时，重读是从第一页开始的，
  // 定时器一响就把用户刚点出来的那几页丢掉——看起来像列表自己往回缩。
  // 那种情况下让读者自己按刷新，比一个会吃掉他操作的定时器好。
  const hasMoreRef = useRef(false);
  hasMoreRef.current = nextCursor !== null;
  useEffect(() => {
    const timer = window.setInterval(() => {
      if (!hasMoreRef.current) setRevision((r) => r + 1);
    }, 30_000);
    return () => window.clearInterval(timer);
  }, []);

  const refresh = useCallback(() => setRevision((r) => r + 1), []);

  const loadMore = useCallback(() => {
    if (!nextCursor || loading) return;
    setLoading(true);
    getProjects({ limit: 1000, cursor: nextCursor })
      .then((page) => {
        // 按 id 去重：分页是位置游标，两次读之间顺序可能变，同一个项目会出现在
        // 相邻两页上。重复一行会让 React 的 key 撞车，也会让计数多出来。
        setRows((prev) => {
          const seen = new Set(prev.map((r) => r.id));
          return [...prev, ...page.projects.filter((r) => !seen.has(r.id))];
        });
        setNextCursor(page.next_cursor);
        setTotalProjects(page.total_projects);
        setTotalTasks(page.total_tasks);
        setError(null);
      })
      .catch((e: unknown) => {
        setError((e as { message?: string }).message ?? '读下一页失败');
      })
      .finally(() => setLoading(false));
  }, [nextCursor, loading]);

  return {
    rows,
    nextCursor,
    totalProjects,
    totalTasks,
    loaded,
    loading,
    error,
    refresh,
    loadMore,
  };
}

export function ProjectsView() {
  const [mode, setMode] = useState<Mode>('wbs');
  const [picked, setPicked] = useState<string | null>(null);

  const projects = useProjectPages();
  const rows = projects.rows;
  const totalProjects = projects.totalProjects;

  // 没选过就落在任务最多的那个真项目上。落在未分组桶上会一进门就是一屏
  // 「这桶没有范围结构」，读者会以为整个项目面是坏的。
  const selected =
    picked ?? rows.find((r) => r.kind === 'goal')?.id ?? rows[0]?.id ?? null;
  const current = rows.find((r) => r.id === selected) ?? null;

  return (
    <ViewFrame
      title="项目"
      meta={
        projects.loaded
          ? `共 ${totalProjects} 个项目 · ${projects.totalTasks} 条任务` +
            (projects.nextCursor ? `（这一屏列了 ${rows.length} 个）` : '')
          : '按 30 秒重读'
      }
      actions={
        <div className="flex items-center gap-2">
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
          {current && mode !== 'audit' && (
            <ExportCsvButton
              projectId={current.id}
              view={mode}
            />
          )}
          <NewProjectForm onCreated={projects.refresh} />
        </div>
      }
    >
      {projects.error ? (
        <ReadError what="项目清单" error={projects.error} onRetry={projects.refresh} />
      ) : !projects.loaded && projects.loading ? (
        <Loading what="项目清单" />
      ) : rows.length === 0 ? (
        <Empty>任务图里一条任务都没有，所以还没有项目。</Empty>
      ) : (
        <>
          <ProjectStrip rows={rows} selected={selected} onSelect={setPicked} />
          {projects.nextCursor && (
            <div className="flex items-center gap-2 border-b border-[var(--line)] px-4 py-1.5 text-[11px] text-[var(--muted)]">
              还有 {totalProjects - rows.length} 个项目没读出来——上面这些是按任务数
              排的前 {rows.length} 个，不是全部。
              <Button onClick={projects.loadMore} disabled={projects.loading}>
                再多读 1000 个
              </Button>
            </div>
          )}
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
            <Note
              className="mono min-w-0 flex-1 truncate text-[11px]"
              style={{ color: ungrouped ? 'var(--muted)' : 'var(--paper)' }}
              text={r.id}
            >
              {ungrouped ? '未分组（没有 goal_id 的任务）' : r.id}
            </Note>
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
    case 'audit':
      return <AuditTimeline projectId={project.id} />;
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
  // 这里以前是一个 600 毫秒的尾随去抖：每来一条属于本项目的帧就把定时器往后推。
  // 一批任务一起换状态时它确实只读一次，可平台忙起来帧是**连续**的——每推一次，
  // 这一发的到期时刻就又晚 600 毫秒，于是一面号称「跟着帧走」的视图比纯轮询还旧。
  // 现在走的是与其余各面同一条调度：前沿立刻读、两读之间至少隔一段地板、地板之内
  // 的信号合并成到期的那一发，且**不**把已在路上的那一发往后推。
  // 任务帧由上面那个 revision 数着。REST 侧对这一面的写（改列、改日期、认领）
  // 不产生任务状态帧，只落在 `projects` 前缀下——那一族也要算进同一个信号里，
  // 否则「帧驱动」只对了一半：别处改的东西要等 15 秒那一拍才出现在这一面。
  const written = useFrameTickStore((s) =>
    ticksFor(s.byKind, [resourceKey('projects'), resourceKey('tasks')])
  );
  useFrameDrivenRefresh(revision + written, refresh);
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
          <Note
            className="mono w-40 shrink-0 truncate text-[var(--paper-dim)]"
            text={r.taskId}
          >
            {r.taskId.slice(0, 20)}
          </Note>
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
    ungrouped ? '未分组的任务' : `范围树 WBS（${project.id}）`,
    () => getTaskGraph(ungrouped ? {} : { goal_id: project.id }),
    15_000
  );
  // 正在给哪一片新建子任务：`null` 是没开，`''` 是新建顶层，其余是父任务 id。
  // 一次只开一个——同时开三段输入，读者会分不清哪一段是要提交的那一段。
  const [composing, setComposing] = useState<string | null>(null);

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

      {/* 拆活的地方就在树上：看得见「这一片下面还空着」的那一刻，正是要往里加
          一条的时候。未分组桶不是容器，不放建活的入口——往一个「没有 goal 的行」
          里建东西没有意义。 */}
      {!ungrouped && (
        <div className="border-b border-[var(--line)] px-4 py-2">
          {composing !== '' ? (
            <Button onClick={() => setComposing('')}>＋ 新建顶层活</Button>
          ) : (
            <NewTaskForm
              project={project}
              onCreated={graph.refresh}
              onCancel={() => setComposing(null)}
            />
          )}
        </div>
      )}

      <div className="overflow-auto">
        {tree.flattened.map(({ node, depth, orphan }) => (
          <div key={node.id} className="border-b border-[var(--line)]">
            <div className="flex items-baseline gap-2 px-4 py-1">
              <span style={{ width: depth * 14 }} />
              <span className="mono text-[10px] text-[var(--muted)]">{depth === 0 ? '▪' : '└'}</span>
              <Note className="mono w-40 shrink-0 truncate text-[11px] text-[var(--paper-dim)]" text={node.id}>
                {node.id.slice(0, 20)}
              </Note>
              <Note className="min-w-0 flex-1 truncate text-[11px]" text={titleOf(node)}>
                {titleOf(node)}
              </Note>
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
              <span className="flex w-52 shrink-0 items-baseline justify-end gap-2">
                {!ungrouped && (
                  <button
                    type="button"
                    onClick={() => setComposing(node.id)}
                    className="mono text-[10px] text-[var(--muted)] hover:text-[var(--paper)]"
                  >
                    ＋子活
                  </button>
                )}
                <ReparentControl
                  taskId={node.id}
                  parentTaskId={node.parent_task_id ?? null}
                  onDone={graph.refresh}
                />
                <PlanButton taskId={node.id} onDone={graph.refresh} />
              </span>
            </div>
            {composing === node.id && (
              <div className="px-4 pb-2">
                <NewTaskForm
                  project={project}
                  parentTaskId={node.id}
                  parentTitle={titleOf(node)}
                  onCreated={graph.refresh}
                  onCancel={() => setComposing(null)}
                />
              </div>
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
            {/* 卡片是这一屏的主体：一列只写个数字，读者知道有多少活，不知道是
                哪些活，也就无从挪动它们。最近动过的排前面，与服务端同一顺序。 */}
            <div className="mt-1 flex max-h-96 flex-col gap-1 overflow-auto">
              {c.cards.map((card) => (
                <div key={card.id} className="border border-[var(--line)] px-1 py-0.5">
                  <div className="flex items-baseline gap-1">
                    <Note className="min-w-0 flex-1 truncate text-[10px]" text={card.title}>
                      {card.title}
                    </Note>
                    {card.blocked && (
                      <Note className="mono text-[9px] text-[var(--danger)]" text="依赖还没完成">
                        卡住
                      </Note>
                    )}
                    {!card.has_acceptance && (
                      <Note
                        className="mono text-[9px] text-[var(--warn)]"
                        text="没有验收标准：这条活之后写不了计划基线"
                      >
                        无验收
                      </Note>
                    )}
                  </div>
                  <MoveCardControl card={card} onDone={board.refresh} />
                </div>
              ))}
              {c.cards_hidden > 0 && (
                <div className="text-[10px] text-[var(--muted)]">
                  还有 {c.cards_hidden} 张没列出
                </div>
              )}
              {c.cards.length === 0 && c.count > 0 && (
                <div className="text-[10px] text-[var(--muted)]">这一列的卡片没有带回来</div>
              )}
            </div>
          </div>
        ))}
      </div>
      {/* 六列全写「没配上限」时，这两件事在格子里逐字同形：这一台按设计不限流，
          与这一台的上限没接上。前者不用动，后者要去查部署，所以让来源自己说话。 */}
      <div className="border-b border-[var(--line)] px-4 py-2 text-[11px]">
        <span className="text-[var(--muted)]">上限来源：</span>
        <span
          className={
            data.wip_limits.refused.length > 0 ? 'text-[var(--danger)]' : 'text-[var(--paper-dim)]'
          }
        >
          {data.wip_limits.basis}
        </span>
        {data.wip_limits.refused.length > 0 && (
          <ul className="mt-1 list-disc pl-5 text-[10px] text-[var(--danger)]">
            {data.wip_limits.refused.map((p) => (
              <li key={p}>{p}</li>
            ))}
          </ul>
        )}
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
          <GanttBar
            key={r.id}
            row={r}
            span={span}
            live={live.get(r.id)?.status}
            onPlanChanged={gantt.refresh}
          />
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
  onPlanChanged,
}: {
  row: GanttRow;
  span: { start: number; end: number } | null;
  live?: string;
  onPlanChanged: () => void;
}) {
  const actual = barSpan(row.created_at, row.updated_at, span);
  const planned = barSpan(row.planned_start, row.planned_end, span);

  return (
    <div className="flex items-center gap-2 border-b border-[var(--line)] py-0.5">
      <Note className="mono w-40 shrink-0 truncate text-[10px] text-[var(--paper-dim)]" text={row.id}>
        {row.id.slice(0, 18)}
      </Note>
      <Note className="min-w-0 flex-1 truncate text-[10px] text-[var(--muted)]" text={row.title}>
        {row.title}
      </Note>
      <div className="relative h-4 w-64 shrink-0">
        {/* 上轨：计划。没有基线就没有这一条——不画一根零长度的，那会被读成「计划已完成」。 */}
        {planned && (
          <Note
            className="absolute top-0 h-1"
            style={{ left: `${planned.left}%`, width: `${planned.width}%`, background: 'var(--brass)' }}
            text={`计划 ${row.planned_start} → ${row.planned_end ?? '（时长未知）'}`}
          />
        )}
        {/* 下轨：实际。 */}
        <div className="absolute bottom-0 h-2.5 w-full bg-[var(--line)]">
          {actual && (
            <Note
              className="absolute top-0 h-full"
              style={{
                left: `${actual.left}%`,
                width: `${actual.width}%`,
                background: row.unplanned ? 'var(--brass-dim)' : 'var(--brass)',
              }}
              text={`实际 ${row.created_at} → ${row.updated_at}`}
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
      {/* 计划基线原先只能由编排侧算出来，客户改不动自己的计划。入口就放在
          写着「未排期」的那一格旁边：看见缺的地方，正是要补的那一刻。 */}
      <span className="w-16 shrink-0 text-right">
        <PlanButton taskId={row.id} onDone={onPlanChanged} />
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

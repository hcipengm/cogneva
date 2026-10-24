import { createContext, useContext, useMemo, useState } from 'react';
import { useStreamStore, type ActivityEntry } from '@/store/streamStore';
import { useSystemSurface, type SurfaceBucket } from '@/hooks/useSystemSurface';
import type {
  BackendHealth,
  FaceDriftReading,
  GovernanceFace,
  TaskRow,
} from '@/api/client';

/**
 * 系统面：一块屏幕里把所有能力各自的读数摆开。
 *
 * 每一格单独读、单独报错。合并成一个「加载失败」会让一格坏掉时另外几格
 * 跟着看不见，而**空着与读失败在界面上长得一样**——读的人会以为系统真的
 * 什么都没有。
 */

type Tab = 'activity' | 'faces' | 'tasks' | 'backends' | 'eval';

const TABS: { key: Tab; label: string }[] = [
  { key: 'activity', label: '活动' },
  { key: 'faces', label: '面与读数' },
  { key: 'tasks', label: '任务' },
  { key: 'backends', label: '资源与后端' },
  { key: 'eval', label: '评估' },
];

/** 面上的状态词 → 颜色。红=拦变更，黄=报告不拦，灰=没读到，绿=达标。 */
function stateColor(state: string): string {
  if (state === 'on_target') return 'text-emerald-400';
  if (['drifted_binding', 'no_owner', 'vacancy_expired', 'unsigned_handover'].includes(state))
    return 'text-red-400';
  if (['owner_idle', 'witness_is_owner', 'deputy_overlap'].includes(state)) return 'text-amber-400';
  return 'text-slate-500';
}

function isRed(state: string): boolean {
  return ['drifted_binding', 'no_owner', 'vacancy_expired', 'unsigned_handover'].includes(state);
}

function stamp(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function short(id: string, max = 18): string {
  return id.length > max ? `${id.slice(0, max)}…` : id;
}

/** 一格自己的读数是否可用。读失败与「没数据」写成两句不同的话。 */
function BucketNote<T>({ bucket, what }: { bucket: SurfaceBucket<T>; what: string }) {
  if (bucket.error) {
    return (
      <div className="rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
        {bucket.error}
        <div className="mt-1 text-xs text-red-300/80">
          这一格没有读数——不是「{what}是空的」。
        </div>
      </div>
    );
  }
  if (bucket.loading && !bucket.data) {
    return <p className="text-sm text-slate-400">读取中…</p>;
  }
  return null;
}

function Panel({
  title,
  subtitle,
  action,
  children,
}: {
  title: string;
  subtitle?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-xl border border-slate-800/80 bg-slate-950/40 p-4">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-medium text-slate-200">{title}</h2>
          {subtitle && <p className="mt-0.5 text-xs text-slate-500">{subtitle}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

function ActivityRow({ entry }: { entry: ActivityEntry }) {
  return (
    <li className="flex gap-3 border-t border-slate-800/60 py-1.5 text-xs first:border-0">
      <span className="w-16 shrink-0 tabular-nums text-slate-500">
        {new Date(entry.at).toLocaleTimeString('zh-CN', { hour12: false })}
      </span>
      <span className="w-28 shrink-0 truncate text-sky-300/80" title={entry.agent ?? ''}>
        {entry.agent ?? '—'}
      </span>
      <span className={`w-52 shrink-0 truncate ${entry.failed ? 'text-red-400' : 'text-slate-200'}`}>
        {entry.summary}
      </span>
      <span className="min-w-0 flex-1 truncate text-slate-500" title={entry.detail ?? ''}>
        {entry.detail ?? ''}
      </span>
    </li>
  );
}

function ActivityPanel() {
  const activity = useStreamStore((state) => state.activity);
  const connectionStatus = useStreamStore((state) => state.connectionStatus);
  const clearActivity = useStreamStore((state) => state.clearActivity);
  const agents = useMemo(
    () => Array.from(new Set(activity.map((a) => a.agent).filter((a): a is string => !!a))),
    [activity]
  );

  return (
    <Panel
      title="活动"
      subtitle={
        activity.length === 0
          ? '总线上还没有帧。连着时说「没在干活」之前，先看下面的连接状态——没连上时这里本来就该是空的。'
          : `最近 ${activity.length} 条事件，来自 ${agents.length} 个执行者。这是总线上的原始帧，不是任务状态的投影。`
      }
      action={
        <div className="flex items-center gap-2">
          <span
            className={`rounded px-2 py-0.5 text-xs ${
              connectionStatus === 'open'
                ? 'bg-emerald-500/10 text-emerald-400'
                : 'bg-amber-500/10 text-amber-400'
            }`}
          >
            {connectionStatus === 'open' ? '已连接' : connectionStatus}
          </span>
          <button
            onClick={clearActivity}
            className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
          >
            清空
          </button>
        </div>
      }
    >
      {activity.length === 0 ? (
        <p className="text-sm text-slate-500">
          {connectionStatus === 'open'
            ? '已连上，窗口内还没有事件。'
            : '尚未连上实时流，没有可显示的事件。'}
        </p>
      ) : (
        <ul className="max-h-[60vh] overflow-y-auto">
          {activity.map((entry) => (
            <ActivityRow key={entry.id} entry={entry} />
          ))}
        </ul>
      )}
    </Panel>
  );
}

function FacesPanel() {
  const { faces, drift } = useSystemSurfaceContext();
  const byFace = useMemo(() => {
    const map = new Map<string, FaceDriftReading>();
    for (const r of drift.data ?? []) map.set(r.face_id, r);
    return map;
  }, [drift.data]);

  const rows = useMemo(() => {
    const list = (faces.data ?? []).map((face: GovernanceFace) => ({
      face,
      reading: byFace.get(face.face_id) ?? null,
      state: byFace.get(face.face_id)?.state ?? 'unread',
    }));
    const rank = (s: string) => (isRed(s) ? 0 : s === 'on_target' ? 2 : 1);
    return list.sort((a, b) => rank(a.state) - rank(b.state) || a.face.face_id.localeCompare(b.face.face_id));
  }, [faces.data, byFace]);

  const red = rows.filter((r) => isRed(r.state)).length;
  const green = rows.filter((r) => r.state === 'on_target').length;

  return (
    <Panel
      title="面与读数"
      subtitle={
        faces.data
          ? `${rows.length} 个面；达标 ${green}，红 ${red}。读数是每次请求重算的，不是存下来的旧结论。`
          : undefined
      }
    >
      <div className="mb-3 space-y-2">
        <BucketNote bucket={faces} what="面名册" />
        <BucketNote bucket={drift} what="面的读数" />
      </div>
      {rows.length > 0 && (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-slate-500">
              <th className="pb-2 pr-3 text-left font-medium">面</th>
              <th className="pb-2 pr-3 text-left font-medium">类型</th>
              <th className="pb-2 pr-3 text-left font-medium">责任人</th>
              <th className="pb-2 pr-3 text-left font-medium">读数</th>
              <th className="pb-2 text-left font-medium">依据</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ face, reading, state }) => (
              <tr key={face.face_id} className="border-t border-slate-800/60">
                <td className="py-1.5 pr-3 text-slate-200" title={face.face_id}>
                  {face.spec.display_name || face.face_id}
                </td>
                <td className="py-1.5 pr-3 text-slate-500">{face.spec.kind}</td>
                <td className="py-1.5 pr-3">
                  <span className="text-slate-300">{reading?.owner ?? face.owner_unit ?? '—'}</span>
                  {reading && (
                    <span className="ml-1 text-slate-600">
                      {reading.owner_source === 'measured' ? '(实测)' : '(声明)'}
                    </span>
                  )}
                </td>
                <td className={`py-1.5 pr-3 ${stateColor(state)}`}>{state}</td>
                <td className="py-1.5 text-slate-500" title={reading?.detail ?? ''}>
                  {reading?.cause ? `${reading.cause} · ` : ''}
                  {reading?.detail ?? (reading ? '' : '没有读数——不是达标')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function TasksPanel() {
  const { tasks, taskRows } = useSystemSurfaceContext();
  const [statusFilter, setStatusFilter] = useState<string>('');

  const statuses = useMemo(
    () => Array.from(new Set((taskRows.data ?? []).map((r) => r.status))).sort(),
    [taskRows.data]
  );
  const rows = useMemo(
    () =>
      (taskRows.data ?? []).filter(
        (r: TaskRow) => !statusFilter || r.status.toLowerCase() === statusFilter.toLowerCase()
      ),
    [taskRows.data, statusFilter]
  );

  const counts = tasks.data;

  return (
    <Panel
      title="任务"
      subtitle="计数读的是调度器当前的全量视图；下面的行按更新时间取最近的，过滤在本地做。"
    >
      <div className="mb-3 space-y-2">
        <BucketNote bucket={tasks} what="任务计数" />
        <BucketNote bucket={taskRows} what="任务列表" />
      </div>

      {counts && (
        <div className="mb-4 grid grid-cols-5 gap-2">
          {[
            ['总计', counts.total, 'text-slate-200'],
            ['就绪', counts.ready, 'text-sky-300'],
            ['运行中', counts.running, 'text-emerald-300'],
            ['完成', counts.completed, 'text-slate-400'],
            ['失败', counts.failed, counts.failed > 0 ? 'text-red-400' : 'text-slate-400'],
          ].map(([label, value, cls]) => (
            <div key={label as string} className="rounded-lg bg-slate-900/60 px-3 py-2">
              <div className="text-xs text-slate-500">{label}</div>
              <div className={`text-lg tabular-nums ${cls}`}>{value as number}</div>
            </div>
          ))}
        </div>
      )}

      {rows.length > 0 && (
        <>
          <div className="mb-2 flex flex-wrap gap-1">
            <button
              onClick={() => setStatusFilter('')}
              className={`rounded px-2 py-0.5 text-xs ${
                statusFilter === '' ? 'bg-slate-700 text-slate-100' : 'text-slate-500 hover:text-slate-300'
              }`}
            >
              全部
            </button>
            {statuses.map((s) => (
              <button
                key={s}
                onClick={() => setStatusFilter(s)}
                className={`rounded px-2 py-0.5 text-xs ${
                  statusFilter === s
                    ? 'bg-slate-700 text-slate-100'
                    : 'text-slate-500 hover:text-slate-300'
                }`}
              >
                {s}
              </button>
            ))}
          </div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500">
                <th className="pb-2 pr-3 text-left font-medium">任务</th>
                <th className="pb-2 pr-3 text-left font-medium">类型</th>
                <th className="pb-2 pr-3 text-left font-medium">状态</th>
                <th className="pb-2 pr-3 text-left font-medium">指定</th>
                <th className="pb-2 pr-3 text-left font-medium">执行</th>
                <th className="pb-2 text-left font-medium">更新</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 200).map((row) => (
                <tr key={row.id} className="border-t border-slate-800/60">
                  <td className="py-1.5 pr-3 text-slate-300" title={row.id}>
                    {short(row.id)}
                  </td>
                  <td className="py-1.5 pr-3 text-slate-500">{row.task_type}</td>
                  <td
                    className={`py-1.5 pr-3 ${
                      row.status === 'Failed'
                        ? 'text-red-400'
                        : row.status === 'Running'
                          ? 'text-emerald-400'
                          : 'text-slate-400'
                    }`}
                    title={row.error ?? ''}
                  >
                    {row.status}
                  </td>
                  <td className="py-1.5 pr-3 text-slate-400">{row.owner_unit ?? '—'}</td>
                  <td className="py-1.5 pr-3 text-slate-400">{row.agent_id ?? '—'}</td>
                  <td className="py-1.5 tabular-nums text-slate-500">{stamp(row.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
      {taskRows.data && taskRows.data.length === 0 && (
        <p className="text-sm text-slate-500">任务图里没有任务。</p>
      )}
    </Panel>
  );
}

function BackendsPanel() {
  const { backends } = useSystemSurfaceContext();
  const overall = backends.data?.overall;

  return (
    <Panel
      title="资源与后端"
      subtitle={
        overall
          ? `整体 ${overall}。每次打开面板都重新探测，不是缓存上一次的结论。`
          : '持久化、缓存、消息与记忆四个后端各自探一次。'
      }
    >
      <BucketNote bucket={backends} what="后端健康" />
      {backends.data && (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-slate-500">
              <th className="pb-2 pr-3 text-left font-medium">后端</th>
              <th className="pb-2 pr-3 text-left font-medium">状态</th>
              <th className="pb-2 pr-3 text-right font-medium">耗时</th>
              <th className="pb-2 text-left font-medium">说明</th>
            </tr>
          </thead>
          <tbody>
            {backends.data.backends.map((b: BackendHealth) => (
              <tr key={b.name} className="border-t border-slate-800/60">
                <td className="py-1.5 pr-3 text-slate-300">{b.name}</td>
                <td
                  className={`py-1.5 pr-3 ${
                    b.status === 'up'
                      ? 'text-emerald-400'
                      : b.status === 'down'
                        ? 'text-red-400'
                        : 'text-amber-400'
                  }`}
                >
                  {b.status}
                </td>
                <td className="py-1.5 pr-3 text-right tabular-nums text-slate-400">
                  {b.latency_ms === null ? '—' : `${b.latency_ms} ms`}
                </td>
                <td className="py-1.5 text-slate-500">{b.error ?? stamp(b.last_checked)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function EvalPanel() {
  const { datasets } = useSystemSurfaceContext();
  return (
    <Panel title="评估" subtitle="可跑的评估集，来自网关的评估数据目录。">
      <BucketNote bucket={datasets} what="评估集" />
      {datasets.data && datasets.data.length === 0 && (
        <p className="text-sm text-slate-500">评估目录里没有数据集。这是「一个都没有」，不是读失败。</p>
      )}
      {datasets.data && datasets.data.length > 0 && (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-slate-500">
              <th className="pb-2 pr-3 text-left font-medium">名称</th>
              <th className="pb-2 pr-3 text-right font-medium">用例</th>
              <th className="pb-2 pr-3 text-left font-medium">标签</th>
              <th className="pb-2 text-left font-medium">路径</th>
            </tr>
          </thead>
          <tbody>
            {datasets.data.map((d) => (
              <tr key={d.path} className="border-t border-slate-800/60">
                <td className="py-1.5 pr-3 text-slate-200">{d.name}</td>
                <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">{d.case_count}</td>
                <td className="py-1.5 pr-3 text-slate-500">{d.tags.join(', ')}</td>
                <td className="py-1.5 text-slate-600" title={d.path}>
                  {short(d.path, 60)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

// ── 上下文：一次读，几个面板共用 ──────────────────────────────

type Surface = ReturnType<typeof useSystemSurface>;

/**
 * 一次读、几个面板共用。
 *
 * 每个面板各自 `useSystemSurface` 会在同一秒内把同一批端点各问一遍，
 * 而且各自轮询——四个面板就是四倍流量、四份可能对不上的读数。
 */
const SurfaceContext = createContext<Surface | null>(null);

function useSystemSurfaceContext(): Surface {
  const surface = useContext(SurfaceContext);
  if (!surface) throw new Error('SystemOverlay 未挂载读数上下文');
  return surface;
}

export function SystemOverlay({
  onClose,
  onOpenConversations,
}: {
  onClose: () => void;
  onOpenConversations: () => void;
}) {
  const [tab, setTab] = useState<Tab>('activity');
  const surface = useSystemSurface({ enabled: true, intervalMs: 15000 });

  const { faces, drift, backends } = surface;
  const redFaces = (drift.data ?? []).filter((r) => isRed(r.state)).length;
  const downBackends = (backends.data?.backends ?? []).filter((b) => b.status === 'down').length;

  const badge = (key: Tab): string | null => {
    if (key === 'faces' && redFaces > 0) return String(redFaces);
    if (key === 'backends' && downBackends > 0) return String(downBackends);
    return null;
  };

  return (
    <SurfaceContext.Provider value={surface}>
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 p-6 backdrop-blur-sm">
      <div className="flex h-full max-h-[92vh] w-full max-w-6xl overflow-hidden rounded-2xl border border-slate-700/60 bg-slate-900/90 shadow-2xl">
        <nav className="w-40 shrink-0 border-r border-slate-800/80 p-3">
          <h1 className="mb-3 px-2 text-sm font-semibold text-white">系统</h1>
          {TABS.map((t) => {
            const mark = badge(t.key);
            return (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`mb-0.5 flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-xs transition ${
                  tab === t.key
                    ? 'bg-slate-800 text-slate-100'
                    : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
                }`}
              >
                <span>{t.label}</span>
                {mark && (
                  <span className="rounded bg-red-500/20 px-1.5 text-red-300">{mark}</span>
                )}
              </button>
            );
          })}
          <div className="mt-4 border-t border-slate-800/80 pt-3">
            <button
              onClick={onOpenConversations}
              className="w-full rounded-lg px-2 py-1.5 text-left text-xs text-slate-400 transition hover:bg-slate-800/50 hover:text-slate-200"
            >
              与智能体对话 →
            </button>
            <p className="mt-1 px-2 text-[11px] leading-snug text-slate-600">
              每个在岗的执行者一条会话，读的是落库的往返，不是本地拼的。
            </p>
          </div>
        </nav>

        <div className="flex min-w-0 flex-1 flex-col">
          <div className="flex items-center justify-end gap-2 border-b border-slate-800/80 px-4 py-2">
            <span className="text-xs text-slate-500">
              面 {faces.data?.length ?? '—'} · 后端{' '}
              {backends.data ? backends.data.overall : '—'}
            </span>
            <button
              onClick={surface.refreshAll}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              刷新
            </button>
            <button
              onClick={onClose}
              className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              ✕
            </button>
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-4">
            {tab === 'activity' && <ActivityPanel />}
            {tab === 'faces' && <FacesPanel />}
            {tab === 'tasks' && <TasksPanel />}
            {tab === 'backends' && <BackendsPanel />}
            {tab === 'eval' && <EvalPanel />}
          </div>
        </div>
      </div>
    </div>
    </SurfaceContext.Provider>
  );
}

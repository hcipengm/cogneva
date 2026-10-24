import { createContext, useContext, useMemo, useState } from 'react';
import { useStreamStore, type ActivityEntry } from '@/store/streamStore';
import { useSystemSurface, type SurfaceBucket } from '@/hooks/useSystemSurface';
import type {
  ActorUsage,
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

type Tab =
  | 'activity'
  | 'faces'
  | 'tasks'
  | 'governance'
  | 'resources'
  | 'principles'
  | 'eval';

const TABS: { key: Tab; label: string }[] = [
  { key: 'activity', label: '活动' },
  { key: 'faces', label: '面与读数' },
  { key: 'tasks', label: '任务' },
  { key: 'governance', label: '治理与演化' },
  { key: 'resources', label: '资源与用量' },
  { key: 'principles', label: '原则' },
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

/** 一个数，或者「没答」——`null` 不打印成 0，那是另一种读数。 */
function num(value: number | null | undefined): string {
  return value === null || value === undefined ? '—' : String(value);
}

/**
 * 资源与用量：后端探活、集群计数、三个固定组件、令牌花销。
 *
 * 四份读数分开报错。合成一格会在其中一个接口没权限或没配置时把另外三份
 * 一起藏起来，而「读不到」与「没有」在界面上长得一样。
 */
function ResourcesPanel() {
  const { backends, cluster, components, usage } = useSystemSurfaceContext();
  const overall = backends.data?.overall;
  const c = cluster.data;
  const actors: ActorUsage[] = usage.data?.actors ?? [];
  const calls = actors.reduce((sum, a) => sum + a.calls, 0);
  const failed = actors.reduce((sum, a) => sum + a.failed_calls, 0);
  const tokensIn = actors.reduce((sum, a) => sum + a.tokens_input, 0);
  const tokensOut = actors.reduce((sum, a) => sum + a.tokens_output, 0);

  return (
    <div className="space-y-4">
      <Panel
        title="集群计数"
        subtitle="监管者与任务图各自报的数。空着的格子是「这个来源没答」，不是零。"
      >
        <div className="mb-3 space-y-2">
          <BucketNote bucket={cluster} what="集群概况" />
        </div>
        {c && (
          <>
            <div className="grid grid-cols-4 gap-2">
              {[
                ['执行者', `${num(c.active_agents)} / ${num(c.total_agents)}`, 'text-emerald-300'],
                ['进行中任务', `${num(c.active_tasks)} / ${num(c.total_tasks)}`, 'text-sky-300'],
                ['排队', num(c.queued_tasks), 'text-slate-300'],
                ['失败', num(c.failed_tasks), (c.failed_tasks ?? 0) > 0 ? 'text-red-400' : 'text-slate-400'],
              ].map(([label, value, cls]) => (
                <div key={label} className="rounded-lg bg-slate-900/60 px-3 py-2">
                  <div className="text-xs text-slate-500">{label}</div>
                  <div className={`text-lg tabular-nums ${cls}`}>{value}</div>
                </div>
              ))}
            </div>
            <p className="mt-3 text-xs text-slate-500">
              健康判词{' '}
              <span
                className={
                  c.cluster_health === 'healthy'
                    ? 'text-emerald-400'
                    : c.cluster_health === 'degraded'
                      ? 'text-amber-400'
                      : 'text-slate-400'
                }
              >
                {c.cluster_health}
              </span>
              {' · '}平均任务耗时{' '}
              {c.avg_task_duration_ms === null ? '—' : `${(c.avg_task_duration_ms / 1000).toFixed(1)} s`}
              {' · '}读于 {stamp(c.timestamp)}
            </p>
            <p className="mt-1 text-xs text-slate-600">
              判词的覆盖面只是告警面：healthy 说的是「没有东西在 firing」，不是「每个组件都验过」。
            </p>
          </>
        )}
      </Panel>

      <Panel title="组件" subtitle="一套安装的三个固定工作负载。unknown 是读不到 Kubernetes，不是挂了。">
        <div className="mb-3 space-y-2">
          <BucketNote bucket={components} what="组件状态" />
        </div>
        {components.data && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500">
                <th className="pb-2 pr-3 text-left font-medium">组件</th>
                <th className="pb-2 pr-3 text-left font-medium">状态</th>
                <th className="pb-2 text-left font-medium">副本</th>
              </tr>
            </thead>
            <tbody>
              {components.data.components.map((component) => (
                <tr key={component.id} className="border-t border-slate-800/60">
                  <td className="py-1.5 pr-3 text-slate-300">{component.id}</td>
                  <td
                    className={`py-1.5 pr-3 ${
                      component.status === 'up'
                        ? 'text-emerald-400'
                        : component.status === 'down'
                          ? 'text-red-400'
                          : 'text-amber-400'
                    }`}
                  >
                    {component.status}
                  </td>
                  <td className="py-1.5 tabular-nums text-slate-400">
                    {component.ready_replicas === null && component.desired_replicas === null
                      ? '—'
                      : `${num(component.ready_replicas)} / ${num(component.desired_replicas)}`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      <Panel
        title="令牌花销"
        subtitle={
          usage.data
            ? `按执行者汇总。窗口 ${stamp(usage.data.covered_since)} → ${stamp(usage.data.covered_until)}——这是折叠层的窗口，不是「刚刚」。`
            : '按执行者读耐久汇总，不是逐笔账本。'
        }
      >
        <div className="mb-3 space-y-2">
          <BucketNote bucket={usage} what="令牌花销" />
        </div>
        {usage.data && actors.length === 0 && (
          <p className="text-sm text-slate-500">
            这个窗口里没有折叠行。可能是真的没花，也可能是折叠还没跑到——看上面的窗口终点再判。
          </p>
        )}
        {actors.length > 0 && (
          <>
            <div className="mb-3 grid grid-cols-4 gap-2">
              {[
                ['调用', String(calls), 'text-slate-200'],
                ['失败', String(failed), failed > 0 ? 'text-red-400' : 'text-slate-400'],
                ['输入', String(tokensIn), 'text-sky-300'],
                ['输出', String(tokensOut), 'text-emerald-300'],
              ].map(([label, value, cls]) => (
                <div key={label} className="rounded-lg bg-slate-900/60 px-3 py-2">
                  <div className="text-xs text-slate-500">{label}</div>
                  <div className={`text-lg tabular-nums ${cls}`}>{value}</div>
                </div>
              ))}
            </div>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500">
                  <th className="pb-2 pr-3 text-left font-medium">执行者</th>
                  <th className="pb-2 pr-3 text-left font-medium">来源</th>
                  <th className="pb-2 pr-3 text-right font-medium">调用</th>
                  <th className="pb-2 pr-3 text-right font-medium">失败</th>
                  <th className="pb-2 pr-3 text-right font-medium">输入</th>
                  <th className="pb-2 pr-3 text-right font-medium">输出</th>
                  <th className="pb-2 text-right font-medium">均耗</th>
                </tr>
              </thead>
              <tbody>
                {actors.slice(0, 50).map((a) => (
                  <tr
                    key={`${a.actor}|${a.origin}|${a.upstream}|${a.api_style}`}
                    className="border-t border-slate-800/60"
                  >
                    <td className="py-1.5 pr-3 text-slate-300" title={a.actor}>
                      {short(a.actor, 28)}
                    </td>
                    <td className="py-1.5 pr-3 text-slate-500" title={`${a.origin} · ${a.upstream} · ${a.api_style}`}>
                      {a.origin}
                    </td>
                    <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">{a.calls}</td>
                    <td
                      className={`py-1.5 pr-3 text-right tabular-nums ${
                        a.failed_calls > 0 ? 'text-red-400' : 'text-slate-500'
                      }`}
                    >
                      {a.failed_calls}
                    </td>
                    <td className="py-1.5 pr-3 text-right tabular-nums text-slate-400">{a.tokens_input}</td>
                    <td className="py-1.5 pr-3 text-right tabular-nums text-slate-400">{a.tokens_output}</td>
                    <td className="py-1.5 text-right tabular-nums text-slate-500">
                      {a.avg_latency_ms ? `${a.avg_latency_ms} ms` : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-2 text-xs text-slate-600">
              输入 token 的含义逐协议不同（OpenAI 兼容线路已含缓存，Anthropic 上两者不相交），
              跨协议不要相减或相加算命中率。
            </p>
          </>
        )}
      </Panel>

      <Panel
        title="后端探活"
        subtitle={
          overall
            ? `整体 ${overall}。每次打开面板都重新探测，不是缓存上一次的结论。`
            : '持久化、缓存、消息与记忆各自探一次。'
        }
      >
        <div className="mb-3 space-y-2">
          <BucketNote bucket={backends} what="后端健康" />
        </div>
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
    </div>
  );
}

/**
 * 原则台账：语料判成了什么、按什么档位查、哪几条真的拦下过东西。
 *
 * 三个读数分开印。合成一个「原则 N 条」回答不了读者带来的任何一个问题：
 * 分不出「没有门」与「门从没开过」，而这两件事要做的事相反。
 */
function PrinciplesPanel() {
  const { principles } = useSystemSurfaceContext();
  const p = principles.data;

  return (
    <Panel
      title="原则台账"
      subtitle={
        p
          ? `读于 ${stamp(p.generated_at)}。判读结果、检查档位、拒绝次数三份读数各自独立。`
          : '语料判定结果与拒绝计数，来自平台自己的台账。'
      }
    >
      <div className="mb-3 space-y-2">
        <BucketNote bucket={principles} what="原则台账" />
      </div>
      {p && (
        <>
          <div className="mb-4 grid grid-cols-4 gap-2">
            {[
              ['有门', p.census.enforced, 'text-emerald-300'],
              ['无门', p.census.unenforced, 'text-amber-400'],
              ['未判读', p.census.unmeasured, 'text-slate-400'],
              ['已退役', p.census.retired, 'text-slate-500'],
            ].map(([label, value, cls]) => (
              <div key={label as string} className="rounded-lg bg-slate-900/60 px-3 py-2">
                <div className="text-xs text-slate-500">{label}</div>
                <div className={`text-lg tabular-nums ${cls}`}>{value as number}</div>
              </div>
            ))}
          </div>
          <p className="mb-4 text-xs text-slate-500">
            检查档位：形状 {p.check_kinds.shape} · 树 {p.check_kinds.tree} · 现场 {p.check_kinds.live} · 人判{' '}
            {p.check_kinds.judgement} · 未声明 {p.check_kinds.undeclared}。人判与未声明是两件事：
            前者是「本来就不该有机器的门」，后者是「还没人说过该怎么查」。
          </p>
          <p className="mb-3 text-xs text-slate-500">
            被拒过的原则 {p.refused_principles} 条，累计拒绝 {p.refusals_total} 次。
            下面是次数最多的几条，两个总数是全集口径。
          </p>
          {p.refused.length > 0 && (
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500">
                  <th className="pb-2 pr-3 text-left font-medium">原则</th>
                  <th className="pb-2 pr-3 text-right font-medium">拒绝</th>
                  <th className="pb-2 text-left font-medium">最近一次</th>
                </tr>
              </thead>
              <tbody>
                {p.refused.map((row) => (
                  <tr key={row.slug} className="border-t border-slate-800/60">
                    <td className="py-1.5 pr-3 text-slate-300" title={row.lead ?? row.slug}>
                      {row.lead ? short(row.lead, 60) : short(row.slug, 60)}
                    </td>
                    <td className="py-1.5 pr-3 text-right tabular-nums text-slate-300">
                      {row.refusals}
                    </td>
                    <td className="py-1.5 tabular-nums text-slate-500">
                      {stamp(row.last_refused_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </Panel>
  );
}

function GovernancePanel() {
  const { supervisor, evolution, changes } = useSystemSurfaceContext();
  const queue = changes.data?.queue ?? null;
  const changeRows = changes.data?.changes ?? [];
  const s = supervisor.data;
  const e = evolution.data;

  const tile = (label: string, value: number | string, cls: string) => (
    <div key={label} className="rounded-lg bg-slate-900/60 px-3 py-2">
      <div className="text-xs text-slate-500">{label}</div>
      <div className={`text-lg tabular-nums ${cls}`}>{value}</div>
    </div>
  );

  return (
    <div className="space-y-4">
      <Panel
        title="在岗与调度"
        subtitle="监管者每轮巡检的结论。四个数分开计：健康、失联、可疑、卡住是四种不同的处置。"
      >
        <div className="mb-3 space-y-2">
          <BucketNote bucket={supervisor} what="监管者状态" />
        </div>
        {s && (
          <>
            <div className="grid grid-cols-4 gap-2">
              {tile('健康', s.healthy_agents, 'text-emerald-300')}
              {tile('失联', s.dead_agents, s.dead_agents > 0 ? 'text-red-400' : 'text-slate-400')}
              {tile('可疑', s.suspect_agents, s.suspect_agents > 0 ? 'text-amber-400' : 'text-slate-400')}
              {tile('卡住', s.stuck_agents, s.stuck_agents > 0 ? 'text-amber-400' : 'text-slate-400')}
            </div>
            <p className="mt-3 text-xs text-slate-500">
              第 {s.cycle} 轮 · 待接管 {s.pending_handoffs} · 调度器
              {s.scheduler_paused ? (
                <span className="ml-1 text-amber-400">已暂停</span>
              ) : (
                <span className="ml-1 text-emerald-400">在跑</span>
              )}
              {' · '}上次发出再平衡计划 {stamp(s.last_plan_emitted_at)}
            </p>
            <p className="mt-1 text-xs text-slate-600">
              读于 {stamp(s.timestamp)}。这一格是每次请求现算的，不是上一轮的存值。
            </p>
          </>
        )}
      </Panel>

      <Panel title="演化与变更" subtitle="平台登记的累计计数；它们只增不减，用来对账而不是看趋势。">
        <div className="mb-3 space-y-2">
          <BucketNote bucket={evolution} what="演化计数" />
        </div>
        {e && (
          <div className="grid grid-cols-4 gap-2">
            {tile('事件', e.events_total, 'text-slate-200')}
            {tile('事件失败', e.events_failed, e.events_failed > 0 ? 'text-red-400' : 'text-slate-400')}
            {tile('变更落地', e.changes_applied, 'text-emerald-300')}
            {tile('变更失败', e.changes_failed, e.changes_failed > 0 ? 'text-red-400' : 'text-slate-400')}
          </div>
        )}
      </Panel>

      <Panel
        title="变更队列"
        subtitle={
          queue
            ? `读的是 ${queue.dir}${queue.owner ? '（本进程就是这条队列的执行者）' : '（本进程不消费这条队列）'}。`
            : '列举一条变更实际读的目录。'
        }
      >
        <div className="mb-3 space-y-2">
          <BucketNote bucket={changes} what="变更队列" />
        </div>
        {changes.data && changeRows.length === 0 && (
          <p className="text-sm text-slate-500">
            这条队列里没有变更。空列表也可能是「列举的进程读的不是生成变更的那个目录」——
            看上面那行目录与属主再判。
          </p>
        )}
        {changeRows.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500">
                <th className="pb-2 pr-3 text-left font-medium">变更</th>
                <th className="pb-2 pr-3 text-left font-medium">类型</th>
                <th className="pb-2 pr-3 text-left font-medium">状态</th>
                <th className="pb-2 pr-3 text-left font-medium">差异</th>
                <th className="pb-2 text-left font-medium">生成于</th>
              </tr>
            </thead>
            <tbody>
              {changeRows.slice(0, 100).map((change) => (
                <tr key={change.id} className="border-t border-slate-800/60">
                  <td className="py-1.5 pr-3 text-slate-300" title={change.description}>
                    {short(change.description, 48)}
                  </td>
                  <td className="py-1.5 pr-3 text-slate-500">{change.kind}</td>
                  <td className="py-1.5 pr-3 text-slate-400">{change.status}</td>
                  <td className="py-1.5 pr-3 text-slate-500">{change.diff_summary ?? '—'}</td>
                  <td className="py-1.5 tabular-nums text-slate-500">{stamp(change.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      <ChangeLedgerPanel />
    </div>
  );
}

function ChangeLedgerPanel() {
  const { ledger } = useSystemSurfaceContext();
  const l = ledger.data;

  return (
    <Panel
      title="变更账"
      subtitle={
        l
          ? `读于 ${stamp(l.generated_at)}。状态、优先级带、待折叠三段读数各自独立。`
          : '语料要求的变更、树被量到的状态，与还在排队等着折叠进账的交接。'
      }
    >
      <div className="mb-3 space-y-2">
        <BucketNote bucket={ledger} what="变更账" />
      </div>
      {l && (
        <>
          <div className="mb-4 grid grid-cols-6 gap-2">
            {l.census.map((cell) => (
              <div key={cell.state} className="rounded-lg bg-slate-900/60 px-3 py-2">
                <div className="text-xs text-slate-500">{cell.state}</div>
                <div
                  className={`text-lg tabular-nums ${
                    cell.outstanding && cell.rows > 0 ? 'text-amber-400' : 'text-slate-300'
                  }`}
                >
                  {cell.rows}
                </div>
              </div>
            ))}
          </div>
          <p className="mb-3 text-xs text-slate-500">
            全账 {l.total} 条，其中还欠着 {l.outstanding_total} 条。前四个状态是欠账，landed 与
            retired 是两种结案方式——后两个往往被数成欠账，所以这里每个格子自带它算不算欠账。
          </p>
          <p className="mb-3 text-xs text-slate-500">
            按优先级带：{' '}
            {l.owed_by_band.map((band) => `${band.band} ${band.rows}`).join(' · ')}。
            五个带都在，空给 0；缺一格与「这一带是空的」在界面上长得一样。
          </p>
          <p className="mb-3 text-xs text-slate-500">
            待折叠的读取 {l.pending_declarations} 条、退役 {l.pending_retirements} 条。
            这是「账本被告知过没有」的唯一读数：交接了却还没进账的变更只出现在这里，
            所以只有零才说明账已经追上了。
          </p>
          {l.outstanding.length === 0 ? (
            <p className="text-sm text-slate-500">没有还欠着的变更。</p>
          ) : (
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500">
                  <th className="pb-2 pr-3 text-left font-medium">变更</th>
                  <th className="pb-2 pr-3 text-left font-medium">状态</th>
                  <th className="pb-2 pr-3 text-left font-medium">来源</th>
                  <th className="pb-2 text-left font-medium">最近量于</th>
                </tr>
              </thead>
              <tbody>
                {l.outstanding.slice(0, 100).map((row) => (
                  <tr key={row.identity} className="border-t border-slate-800/60">
                    <td className="py-1.5 pr-3 text-slate-300" title={row.assertion ?? row.identity}>
                      {row.title ? short(row.title, 56) : short(row.identity, 56)}
                    </td>
                    <td className={`py-1.5 pr-3 ${stateColor(row.state)}`}>{row.state}</td>
                    <td className="py-1.5 pr-3 text-slate-500" title={row.sources.join(', ')}>
                      {short(row.sources[0] ?? '—', 32)}
                    </td>
                    <td className="py-1.5 tabular-nums text-slate-500">
                      {row.measured_at ? stamp(row.measured_at) : '还没人看过'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {l.outstanding_total > l.outstanding.length && (
            <p className="mt-2 text-xs text-slate-600">
              本页 {l.outstanding.length} 条，总共欠 {l.outstanding_total} 条。
            </p>
          )}
        </>
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
    if (key === 'resources' && downBackends > 0) return String(downBackends);
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
            {tab === 'governance' && <GovernancePanel />}
            {tab === 'resources' && <ResourcesPanel />}
            {tab === 'principles' && <PrinciplesPanel />}
            {tab === 'eval' && <EvalPanel />}
          </div>
        </div>
      </div>
    </div>
    </SurfaceContext.Provider>
  );
}

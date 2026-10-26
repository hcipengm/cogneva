import { useState } from 'react';
import { getClusterComponents, getClusterOverview, getSupervisorStatus, type ClusterOverview } from '@/api/client';
import { useResource, type Resource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { EventsBoard } from '@/observability/EventsBoard';
import { LogsBoard } from '@/observability/LogsBoard';
import { MetricsBoard } from '@/observability/MetricsBoard';
import { ProbesBoard } from '@/observability/ProbesBoard';
import { ReadingsBoard } from '@/observability/ReadingsBoard';
import { TracesBoard } from '@/observability/TracesBoard';
import { ClusterTopology } from '@/topology/ClusterTopology';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';

/**
 * 观测面：一排工具板，不是一张大表。
 *
 * 每块板回答一种问题，各有自己的读面与刷新节奏：概况（现在怎么样）、拓扑（东西在
 * 哪儿）、指标（平台自己数的数）、日志（盘上有什么可读）、trace（跑过的轨迹）、
 * 事件回放（最近一天发生过什么）、探测（谁通了）。
 *
 * 它们刻意不合成为一个健康分：后端挂了、监管者停了、模型池没快照、任务全失败，
 * 是四种不同的处置，加权成一个数字之后读者还得再拆一次。
 */
type BoardId =
  | 'overview'
  | 'readings'
  | 'topology'
  | 'metrics'
  | 'logs'
  | 'traces'
  | 'events'
  | 'probes';

const BOARDS: { id: BoardId; label: string; what: string }[] = [
  { id: 'overview', label: '概况', what: '集群判词、监管者、工作负载' },
  { id: 'readings', label: '读数', what: '六条要动手的数，各带来源与 Grafana 深链' },
  { id: 'topology', label: '拓扑', what: '集群 → 节点 → Pod → 容器 → 卷与事件' },
  { id: 'metrics', label: '指标', what: '平台自己登记的累计计数' },
  { id: 'logs', label: '日志', what: '归档索引（正文在存储面）' },
  { id: 'traces', label: 'trace', what: '执行轨迹的形状' },
  { id: 'events', label: '事件回放', what: '最近 24 小时的历史事件' },
  { id: 'probes', label: '探测', what: '后端健康矩阵' },
];

export function ObservabilityView() {
  const [board, setBoard] = useState<BoardId>('overview');
  const overview = useResource('集群概况', getClusterOverview, 20_000);
  // 这一面的数（在岗智能体、活动任务、失败数）全是总线上那几种帧算出来的：执行者
  // 一起来、一停，任务一换状态，告警一越线，这里就该变。只靠 20 秒一拍，把间隔
  // 调成十分钟后，屏幕上「在岗 3」会一直是 3，而平台早就换了一拨人。
  //
  // 帧只是触发器：重读的是服务端现算的那一份，不是拿帧去改数。
  useKindTickedRefresh(
    ['tasks', 'agents', 'alerts', resourceKey('cluster')],
    overview.refresh
  );
  const o = overview.data;

  return (
    <ViewFrame
      title="观测"
      meta={
        o
          ? `集群判词 ${o.cluster_health} · ${new Date(o.timestamp).toLocaleTimeString('zh-CN')}`
          : '按 20 秒重读'
      }
    >
      <nav className="flex flex-wrap items-stretch border-b border-[var(--line)]" aria-label="观测工具板">
        {BOARDS.map((entry) => {
          const active = entry.id === board;
          return (
            <button
              key={entry.id}
              type="button"
              onClick={() => setBoard(entry.id)}
              title={entry.what}
              aria-pressed={active}
              className="border-r border-[var(--line)] px-4 py-2 text-[11px] transition-colors hover:bg-[var(--ink-700)] hover:text-[var(--paper)]"
              style={{
                color: active ? 'var(--paper)' : 'var(--muted)',
                background: active ? 'var(--ink-800)' : 'transparent',
                boxShadow: active ? 'inset 0 -2px 0 var(--brass)' : undefined,
              }}
            >
              {entry.label}
            </button>
          );
        })}
      </nav>

      {board === 'overview' && <OverviewBoard overview={overview} />}
      {board === 'readings' && <ReadingsBoard />}
      {board === 'topology' && <ClusterTopology />}
      {board === 'metrics' && <MetricsBoard />}
      {board === 'logs' && <LogsBoard />}
      {board === 'traces' && <TracesBoard />}
      {board === 'events' && <EventsBoard />}
      {board === 'probes' && <ProbesBoard />}
    </ViewFrame>
  );
}

/**
 * 概况板：集群判词、监管者、工作负载三块并排。
 *
 * 模型花销不在这里再列一遍——它有自己的面（按会话、执行者、上游拆开，还带窗口），
 * 两处各算一次的话，同一段时间会读出两个数。
 */
function OverviewBoard({ overview }: { overview: Resource<ClusterOverview> }) {
  const setActivity = useShell((s) => s.setActivity);
  const supervisor = useResource('监管者状态', getSupervisorStatus, 20_000);
  const components = useResource('工作负载存活', getClusterComponents, 30_000);
  // 三块并排，变的原因常常是同一件事（一次派发让监管者多了一批要管的、让工作负载
  // 多起一个 Pod），所以一起重读——分两次读，画面会先变一半再变另一半。
  useKindTickedRefresh(
    ['tasks', 'agents', 'alerts', 'usage', resourceKey('cluster'), resourceKey('supervisor')],
    [supervisor.refresh, components.refresh]
  );

  const o = overview.data;
  const s = supervisor.data;

  return (
    <>
      {overview.error ? (
        <ReadError what="集群概况" error={overview.error} onRetry={overview.refresh} />
      ) : !o && overview.loading ? (
        <Loading what="集群概况" />
      ) : (
        o && (
          <Readings from={overview} source="GET /api/v1/cluster/overview">
            <div className="flex flex-wrap border-b border-[var(--line)] py-2">
              <Reading label="在岗智能体" value={o.active_agents ?? '—'} note={`共 ${o.total_agents ?? '?'}`} />
              <Reading label="活动任务" value={o.active_tasks ?? '—'} note={`就绪 ${o.queued_tasks ?? '?'} · 失败 ${o.failed_tasks ?? '?'}`} />
              <Reading label="平均任务时长" value={o.avg_task_duration_ms != null ? `${Math.round(o.avg_task_duration_ms / 1000)}s` : '—'} />
              <Reading
                label="模型上游池"
                value={o.llm_pool ? (o.llm_pool.available ? '可用' : '不可用') : '无快照'}
                tone={o.llm_pool ? (o.llm_pool.available ? 'ok' : 'danger') : 'warn'}
                note={o.llm_pool ? `池 ${o.llm_pool.pool_size} · 不可用 ${o.llm_pool.unavailable_count}` : '写者没发布过快照'}
              />
              <Reading label="集群判词" value={o.cluster_health} tone={o.cluster_health === 'healthy' ? 'ok' : 'warn'} />
            </div>
          </Readings>
        )
      )}

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">监管者</h2>
        {supervisor.error ? (
          <ReadError what="监管者状态" error={supervisor.error} onRetry={supervisor.refresh} />
        ) : s ? (
          <Readings from={supervisor} source="GET /api/v1/supervisor/status">
            <div className="flex flex-wrap py-2">
              <Reading label="健康" value={s.healthy_agents} tone="ok" />
              <Reading label="失联" value={s.dead_agents} tone={s.dead_agents ? 'danger' : 'plain'} />
              <Reading label="可疑" value={s.suspect_agents} tone={s.suspect_agents ? 'warn' : 'plain'} />
              <Reading label="卡住" value={s.stuck_agents} tone={s.stuck_agents ? 'warn' : 'plain'} />
              <Reading label="待交接" value={s.pending_handoffs} />
              <Reading label="调度器" value={s.scheduler_paused ? '已暂停' : '运行中'} tone={s.scheduler_paused ? 'warn' : 'ok'} />
              <Reading label="巡检轮次" value={s.cycle} note={`上次派发计划 ${s.last_plan_emitted_at ? new Date(s.last_plan_emitted_at).toLocaleTimeString('zh-CN') : '从未'}`} />
            </div>
          </Readings>
        ) : (
          <Empty>读不到监管者状态。</Empty>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">工作负载</h2>
        {components.error ? (
          <ReadError what="工作负载存活" error={components.error} onRetry={components.refresh} />
        ) : components.data ? (
          <Table
            columns={[
              { key: 'id', title: '工作负载', width: '220px', render: (c) => <span className="mono text-[11px]">{c.id}</span> },
              { key: 'status', title: '状态', width: '110px', render: (c) => <Status value={c.status} /> },
              { key: 'replicas', title: '副本', width: '110px', render: (c) => (
                <span className="mono text-[11px]">{c.ready_replicas ?? '?'} / {c.desired_replicas ?? '?'}</span>
              ) },
            ]}
            rows={components.data.components}
            rowKey={(c) => c.id}
          />
        ) : (
          <Loading what="工作负载存活" />
        )}
      </section>

      <section className="flex items-center gap-3 px-4 py-3 text-[11px] text-[var(--muted)]">
        <span>模型花销按会话、执行者与上游拆开在 Token 面，这一面不重复算一遍。</span>
        <Button onClick={() => setActivity('token')}>去 Token 面</Button>
      </section>
    </>
  );
}

import {
  getBackendHealth,
  getClusterComponents,
  getClusterOverview,
  getSupervisorStatus,
  getTokenUsage,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { TokenShare } from '@/observability/TokenShare';
import { ClusterTopology } from '@/topology/ClusterTopology';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';

/**
 * 观测面：集群、后端、监管者、模型用量四个口径并排放。
 *
 * 四块刻意不合成为一个健康分：后端挂了、监管者停了、模型池没快照、任务全失败，
 * 是四种不同的处置，加权成一个数字之后读者还得再拆一次。
 */
export function ObservabilityView() {
  const overview = useResource('集群概况', getClusterOverview, 20_000);
  const backends = useResource('后端健康', getBackendHealth, 20_000);
  const supervisor = useResource('监管者状态', getSupervisorStatus, 20_000);
  const components = useResource('工作负载存活', getClusterComponents, 30_000);
  const usage = useResource('模型用量', () => getTokenUsage(24), 60_000);

  const o = overview.data;
  const s = supervisor.data;

  return (
    <ViewFrame title="观测" meta={o ? `集群判词 ${o.cluster_health} · ${new Date(o.timestamp).toLocaleTimeString('zh-CN')}` : '按 20 秒重读'}>
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
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          拓扑 · 集群 → 节点 → Pod → 容器 → 卷与事件
        </h2>
        <ClusterTopology />
      </section>

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
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          后端健康{backends.data ? ` · 总体 ${backends.data.overall}` : ''}
        </h2>
        {backends.error ? (
          <ReadError what="后端健康" error={backends.error} onRetry={backends.refresh} />
        ) : backends.data ? (
          <Table
            columns={[
              { key: 'name', title: '后端', width: '180px', render: (b) => <span className="mono text-[11px]">{b.name}</span> },
              { key: 'status', title: '状态', width: '110px', render: (b) => <Status value={b.status} /> },
              { key: 'latency', title: '延迟', width: '100px', render: (b) => (
                <span className="mono text-[11px]">{b.latency_ms != null ? `${b.latency_ms} ms` : '—'}</span>
              ) },
              { key: 'checked', title: '最后检查', width: '110px', render: (b) => <Moment iso={b.last_checked} /> },
              { key: 'error', title: '错误', render: (b) => (
                <span className="text-[11px] text-[var(--danger)]">{b.error ?? ''}</span>
              ) },
            ]}
            rows={backends.data.backends}
            rowKey={(b) => b.name}
          />
        ) : (
          <Loading what="后端健康" />
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">工作负载</h2>
        {components.data ? (
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

      <section>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">模型用量（最近 24 小时）</h2>
        {usage.error ? (
          <ReadError what="模型用量" error={usage.error} onRetry={usage.refresh} />
        ) : usage.data ? (
          <Readings
            from={usage}
            source="GET /api/v1/observability/token-usage（按执行者累加的折叠行）"
            caveat="这里的总量是这份返回里各行相加，窗口由「折叠窗口」那一格给出；没有折叠行的窗口读出来是 0，不是「没人用过模型」。"
          >
          <div className="flex flex-wrap py-2">
            <Reading
              label="输入 token"
              value={sum(usage.data.actors, (a) => a.tokens_input).toLocaleString()}
            />
            <Reading
              label="输出 token"
              value={sum(usage.data.actors, (a) => a.tokens_output).toLocaleString()}
            />
            <Reading
              label="调用次数"
              value={sum(usage.data.actors, (a) => a.calls).toLocaleString()}
            />
            <Reading
              label="失败调用"
              value={sum(usage.data.actors, (a) => a.failed_calls).toLocaleString()}
              tone={sum(usage.data.actors, (a) => a.failed_calls) > 0 ? 'warn' : 'plain'}
            />
            <Reading
              label="折叠窗口"
              value={
                usage.data.covered_until
                  ? new Date(usage.data.covered_until).toLocaleTimeString('zh-CN')
                  : '无折叠行'
              }
              note={
                usage.data.covered_since
                  ? `自 ${new Date(usage.data.covered_since).toLocaleString('zh-CN')}`
                  : '窗口里没有折叠行，数是空的'
              }
            />
            {usage.data.actors.slice(0, 4).map((actor) => (
              <Reading
                key={`${actor.actor}|${actor.origin}`}
                label={`${actor.actor} · ${actor.origin}`}
                value={actor.calls.toLocaleString()}
                note={`${actor.tokens_input.toLocaleString()} in / ${actor.tokens_output.toLocaleString()} out`}
              />
            ))}
          </div>
          </Readings>
        ) : (
          <Loading what="模型用量" />
        )}
        {usage.data && usage.data.actors.length > 0 && (
          <>
            <TokenShare actors={usage.data.actors} />
            <Table
              columns={[
                { key: 'actor', title: '执行者', width: '180px', render: (a) => (
                  <span className="mono text-[11px]">{a.actor}</span>
                ) },
                { key: 'origin', title: '来源', width: '140px', render: (a) => (
                  <span className="text-[11px] text-[var(--muted)]">{a.origin}</span>
                ) },
                { key: 'upstream', title: '上游', width: '150px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{a.upstream}</span>
                ) },
                { key: 'style', title: '协议', width: '120px', render: (a) => (
                  <span className="text-[11px]">{a.api_style}</span>
                ) },
                { key: 'calls', title: '调用', width: '80px', render: (a) => (
                  <span className="mono text-[11px]">{a.calls}</span>
                ) },
                { key: 'failed', title: '失败', width: '70px', render: (a) => (
                  <span className="mono text-[11px]" style={{ color: a.failed_calls > 0 ? 'var(--danger)' : 'var(--muted)' }}>
                    {a.failed_calls}
                  </span>
                ) },
                { key: 'in', title: '输入', width: '100px', render: (a) => (
                  <span className="mono text-[11px]">{a.tokens_input.toLocaleString()}</span>
                ) },
                { key: 'out', title: '输出', width: '100px', render: (a) => (
                  <span className="mono text-[11px]">{a.tokens_output.toLocaleString()}</span>
                ) },
                { key: 'cached', title: '缓存命中', width: '100px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{a.tokens_cached.toLocaleString()}</span>
                ) },
                { key: 'latency', title: '均耗', width: '90px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">
                    {a.avg_latency_ms ? `${a.avg_latency_ms} ms` : '—'}
                  </span>
                ) },
              ]}
              rows={usage.data.actors}
              rowKey={(a) => `${a.actor}|${a.origin}|${a.upstream}|${a.api_style}`}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              输入 token 的含义逐协议不同：OpenAI 兼容线路里它已含缓存命中，Anthropic 上两者不相交。
              跨协议不要相减算命中率。
            </p>
          </>
        )}
      </section>
    </ViewFrame>
  );
}

/** 汇总一列。空列表给 0：这里求和的是「返回了几行」，不是「上游有没有配」。 */
function sum<T>(rows: T[], pick: (row: T) => number): number {
  return rows.reduce((total, row) => total + pick(row), 0);
}

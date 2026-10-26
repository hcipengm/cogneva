import {
  getClusterTopology,
  getResourceReport,
  type TopologyClaim,
  type TopologyCluster,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { formatBytes, formatCpu } from '@/topology/ClusterTopology';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

/**
 * 资源面：配额、可调度余量、存储账本。
 *
 * 与拓扑面共用同一份集群读数（`/api/v1/cluster/topology`），不另取一套：同一台
 * 节点的 CPU 在两张页上必须是同一个数，否则「余量不够」和「节点空着」会同时成立。
 * 这一面只补拓扑没有的三格——准入前的配额、调度时的余量、盘上真实的占用。
 */
export function ResourcesView() {
  const topology = useResource('集群读数', getClusterTopology, 15_000);
  const quota = useResource('配额与限额', getResourceReport, 30_000);
  // 这一面的两半都跟着执行者与任务走：一次派发起一批 Pod，一个 agent 收工放掉它
  // 占的额度。总线把这两件事都说出来了（`agent_start` / `agent_end` / 告警越线），
  // 这里只在「说有变化」时重读一次，数仍由服务端从集群现算——帧里没有配额。
  useKindTickedRefresh(
    ['alerts', 'agents', resourceKey('cluster'), resourceKey('quota')],
    [topology.refresh, quota.refresh]
  );

  const cluster = topology.data?.clusters[0] ?? null;
  const claims = collectClaims(cluster);
  const report = quota.data;

  return (
    <ViewFrame
      title="资源"
      meta={
        report
          ? `命名空间 ${report.namespace || '（不在集群里）'} · ${new Date(report.collected_at).toLocaleTimeString('zh-CN')}`
          : '按 30 秒重读'
      }
    >
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          配额与默认限额 · 准入前的账
        </h2>
        {quota.error ? (
          <ReadError what="配额与限额" error={quota.error} onRetry={quota.refresh} />
        ) : !report ? (
          <Loading what="配额与限额" />
        ) : report.error ? (
          <p className="px-4 py-2 text-[11px] text-[var(--warn)]">{report.error}</p>
        ) : report.quotas.length === 0 && report.limits.length === 0 ? (
          <Empty>
            这个命名空间没有 ResourceQuota 也没有 LimitRange：不是「配额是零」，是没有上限。
            申请不会被配额拒绝，请求过大的 Pod 仍会被调度器挡在节点之外。
          </Empty>
        ) : (
          <>
            {report.quotas.map((quotaView) => (
              <div key={quotaView.name} className="border-t border-[var(--line)] first:border-t-0">
                <p className="px-4 py-1 text-[11px] text-[var(--muted)]">
                  ResourceQuota <span className="mono">{quotaView.name}</span>
                </p>
                <Table
                  columns={[
                    { key: 'resource', title: '资源', width: '200px', render: (e) => (
                      <span className="mono text-[11px]">{e.resource}</span>
                    ) },
                    { key: 'hard', title: '上限', width: '110px', render: (e) => (
                      <span className="mono text-[11px]">{e.hard || '—'}</span>
                    ) },
                    { key: 'used', title: '已用', width: '110px', render: (e) => (
                      <span className="mono text-[11px]">{e.used || '—'}</span>
                    ) },
                    { key: 'percent', title: '用掉', width: '160px', render: (e) => (
                      <Meter percent={e.percent} />
                    ) },
                  ]}
                  rows={quotaView.entries}
                  rowKey={(e) => `${quotaView.name}|${e.resource}`}
                />
              </div>
            ))}
            {report.limits.map((limitView) => (
              <div key={limitView.name} className="border-t border-[var(--line)]">
                <p className="px-4 py-1 text-[11px] text-[var(--muted)]">
                  LimitRange <span className="mono">{limitView.name}</span>
                  ：没写 requests/limits 的容器由这里补默认值，超上限的申请在准入时被拒
                </p>
                <Table
                  columns={[
                    { key: 'kind', title: '对象', width: '110px', render: (e) => (
                      <span className="text-[11px]">{e.kind}</span>
                    ) },
                    { key: 'resource', title: '资源', width: '110px', render: (e) => (
                      <span className="mono text-[11px]">{e.resource}</span>
                    ) },
                    { key: 'min', title: '最小', width: '90px', render: (e) => (
                      <span className="mono text-[11px]">{e.min ?? '—'}</span>
                    ) },
                    { key: 'max', title: '最大', width: '90px', render: (e) => (
                      <span className="mono text-[11px]">{e.max ?? '—'}</span>
                    ) },
                    { key: 'default', title: '默认上限', width: '100px', render: (e) => (
                      <span className="mono text-[11px]">{e.default ?? '—'}</span>
                    ) },
                    { key: 'default_request', title: '默认申报', render: (e) => (
                      <span className="mono text-[11px]">{e.default_request ?? '—'}</span>
                    ) },
                  ]}
                  rows={limitView.entries}
                  rowKey={(e) => `${limitView.name}|${e.kind}|${e.resource}`}
                />
              </div>
            ))}
          </>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          节点可调度余量 · 调度时的账
        </h2>
        {topology.error ? (
          <ReadError what="集群读数" error={topology.error} onRetry={topology.refresh} />
        ) : !cluster ? (
          <Loading what="集群读数" />
        ) : !cluster.reachable ? (
          <Empty>{cluster.error ?? '读不到集群。'}</Empty>
        ) : (
          <>
            <Readings
              from={topology}
              source="GET /api/v1/cluster/topology（与观测面共用的同一份集群读数）"
              caveat="「用量来源」说的是实际用量从哪儿来：没有 metrics-server 时只有申报量，实际用量是空的，不是 0。"
            >
            <div className="flex flex-wrap py-2">
              <Reading label="节点" value={cluster.nodes.length} note={`未调度 Pod ${cluster.unscheduled.length}`} />
              <Reading
                label="用量来源"
                value={
                  cluster.usage_source === 'metrics-server'
                    ? 'metrics-server'
                    : cluster.usage_source === 'kubelet-summary'
                      ? 'kubelet summary'
                      : '无（只有申报量）'
                }
                tone={cluster.usage_source ? 'ok' : 'warn'}
              />
              <Reading label="采集于" value={<Moment iso={topology.data?.collected_at} />} />
            </div>
            </Readings>
            <Table
              columns={[
                { key: 'node', title: '节点', width: '180px', render: (n) => (
                  <span className="mono text-[11px]">{n.name}</span>
                ) },
                { key: 'cpu', title: 'CPU（已申报 / 可分配）', width: '230px', render: (n) => (
                  <span className="mono text-[11px]">
                    {formatCpu(n.cpu_requested_millicores)} /{' '}
                    {n.cpu_allocatable_millicores != null ? formatCpu(n.cpu_allocatable_millicores) : '—'}
                  </span>
                ) },
                { key: 'cpu_left', title: 'CPU 余量', width: '90px', render: (n) => (
                  <Left
                    left={
                      n.cpu_allocatable_millicores != null
                        ? n.cpu_allocatable_millicores - n.cpu_requested_millicores
                        : null
                    }
                    low={500}
                  />
                ) },
                { key: 'memory', title: '内存（已申报 / 可分配）', width: '250px', render: (n) => (
                  <span className="mono text-[11px]">
                    {formatBytes(n.memory_requested_bytes)} /{' '}
                    {n.memory_allocatable_bytes != null ? formatBytes(n.memory_allocatable_bytes) : '—'}
                  </span>
                ) },
                { key: 'memory_left', title: '内存余量', width: '100px', render: (n) => (
                  <Left
                    left={
                      n.memory_allocatable_bytes != null
                        ? n.memory_allocatable_bytes - n.memory_requested_bytes
                        : null
                    }
                    format={formatBytes}
                    low={1024 * 1024 * 1024}
                  />
                ) },
                { key: 'pods', title: 'Pod 数', width: '90px', render: (n) => (
                  <span className="mono text-[11px]">
                    {n.pods.length} / {n.pods_allocatable ?? '—'}
                  </span>
                ) },
              ]}
              rows={cluster.nodes}
              rowKey={(n) => n.name}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              余量按申报量算，不按实际用量：调度器只看申报。实际用量低于申报时，节点看着忙
              其实有空间——要不要压缩申报是另一个决定，不在这里猜。
            </p>
          </>
        )}
      </section>

      <section>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          存储账本 · 声明 vs 实测
        </h2>
        {claims.length === 0 ? (
          <Empty>这一面没读到卷声明：不是「没有数据」，是这个命名空间里没有挂卷的 Pod。</Empty>
        ) : (
          <>
            <Table
              columns={[
                { key: 'name', title: '卷声明', width: '200px', render: (c) => (
                  <span className="mono text-[11px]">{c.name}</span>
                ) },
                { key: 'phase', title: '状态', width: '100px', render: (c) => (
                  <span className="text-[11px]">{c.phase}</span>
                ) },
                { key: 'class', title: '存储类', width: '130px', render: (c) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{c.storage_class ?? '—'}</span>
                ) },
                { key: 'capacity', title: '声明', width: '100px', render: (c) => (
                  <span className="mono text-[11px]">
                    {c.capacity_bytes != null ? formatBytes(c.capacity_bytes) : '—'}
                  </span>
                ) },
                { key: 'measured', title: '盘上占用', width: '110px', render: (c) => (
                  <span className="mono text-[11px]">
                    {c.measured_bytes != null ? formatBytes(c.measured_bytes) : '—'}
                  </span>
                ) },
                { key: 'fill', title: '占用', width: '160px', render: (c) => (
                  <Meter
                    percent={
                      c.measured_bytes != null && c.capacity_bytes
                        ? Math.round((c.measured_bytes / c.capacity_bytes) * 1000) / 10
                        : null
                    }
                  />
                ) },
              ]}
              rows={claims}
              rowKey={(c) => c.name}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              「声明」是准入时写下的数字，「盘上占用」是平台自己的走查端量出来的字节。两个
              都空才说明这一卷没有被量到；只画声明量会让一块写满的盘看起来只用了三成。
            </p>
          </>
        )}
      </section>
    </ViewFrame>
  );
}

/** 从拓扑里收齐卷声明。同一个声明被多个 Pod 挂着时只算一行：它是一块盘。 */
function collectClaims(cluster: TopologyCluster | null): TopologyClaim[] {
  const claims = new Map<string, TopologyClaim>();
  for (const node of cluster?.nodes ?? []) {
    for (const pod of node.pods) {
      for (const volume of pod.volumes) {
        if (volume.claim && !claims.has(volume.claim.name)) {
          claims.set(volume.claim.name, volume.claim);
        }
      }
    }
  }
  for (const pod of [...(cluster?.unscheduled ?? []), ...(cluster?.unplaced ?? [])]) {
    for (const volume of pod.volumes) {
      if (volume.claim && !claims.has(volume.claim.name)) {
        claims.set(volume.claim.name, volume.claim);
      }
    }
  }
  return [...claims.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/** 一条用量条。没有百分比就没有条，画一条满格或空格的条都是在编。 */
function Meter({ percent }: { percent: number | null }) {
  if (percent == null) return <span className="text-[11px] text-[var(--muted)]">算不出</span>;
  const clamped = Math.max(0, Math.min(100, percent));
  return (
    <span className="flex items-center gap-2">
      <span className="relative h-1.5 w-24 overflow-hidden bg-[var(--ink-700)]">
        <span
          className="absolute inset-y-0 left-0"
          style={{
            width: `${clamped}%`,
            background:
              clamped >= 90
                ? 'var(--danger)'
                : clamped >= 75
                  ? 'var(--warn)'
                  : 'var(--brass)',
          }}
        />
      </span>
      <span className="mono text-[10px] text-[var(--muted)]">{percent}%</span>
    </span>
  );
}

/** 余量：读数缺失时写「—」，不写 0——0 是一句「已经排满了」。 */
function Left({
  left,
  format = formatCpu,
  low,
}: {
  left: number | null;
  format?: (value: number) => string;
  /** 低于这个数的余量按「快满了」上色，单位与读数相同。 */
  low: number;
}) {
  if (left == null) return <span className="mono text-[11px] text-[var(--muted)]">—</span>;
  return (
    <span
      className="mono text-[11px]"
      style={{ color: left <= 0 ? 'var(--danger)' : left < low ? 'var(--warn)' : undefined }}
    >
      {format(Math.max(0, left))}
    </span>
  );
}

import {
  getClusterTopology,
  getResourceReport,
  type ClusterTopology as ClusterTopologyData,
  type TopologyClaim,
  type TopologyCluster,
} from '@/api/client';
import { useResource, type Resource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { ClusterTopology } from '@/topology/ClusterTopology';
import { clusterName, formatBytes, formatCpu } from '@/topology/format';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { BarList } from '@/ui/plots';

/**
 * 资源面：拓扑树、配额、可调度余量、存储账本。
 *
 * 顶上是「集群 ⊃ 节点 ⊃ Pod ⊃ 容器（展开到容器里的进程）」那棵树：资源这一面回答的
 * 就是「这些东西分别占了什么」，从最大的包含关系往下读，比先读配额再倒推谁在占它更
 * 自然。这棵树与观测面的拓扑板是同一个组件、同一份读数，不是两套画法。
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

  // 每个集群各算各的账。以前这里写死 `clusters[0]`：多集群时第二、第三个集群的
  // 余量与存储账本根本不会出现在这一面，而读者看不出少了谁——一张只有一半机器的
  // 账看起来和一张完整的账一模一样。
  const clusters = topology.data?.clusters ?? [];
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
          集群 → 节点 → Pod → 容器 → 进程 · 包含关系
        </h2>
        {/* 读数由这一面自己的那一份给进去（下面节点余量表算的是同一份），不在这里
            再读一遍：同一屏上同一个接口问两次，两处的数会各自按自己的间隔跳。 */}
        <ClusterTopology reading={topology} />
      </section>

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

      {clusters.length === 0 ? (
        <section className="border-b border-[var(--line)]">
          <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
            节点可调度余量 · 调度时的账
          </h2>
          {topology.error ? (
            <ReadError what="集群读数" error={topology.error} onRetry={topology.refresh} />
          ) : (
            <Loading what="集群读数" />
          )}
        </section>
      ) : (
        clusters.map((cluster, index) => (
          <NodeBudget
            key={`budget-${cluster.id}-${index}`}
            cluster={cluster}
            index={index}
            total={clusters.length}
            source={topology}
          />
        ))
      )}

      {clusters.map((cluster, index) => (
        <StorageLedger
          key={`storage-${cluster.id}-${index}`}
          cluster={cluster}
          index={index}
          total={clusters.length}
        />
      ))}
    </ViewFrame>
  );
}

/**
 * 一个集群的调度余量。
 *
 * 每个集群单开一节，标题写清是哪一个：多集群时把这些数加在一起是没有意义的——
 * 「还剩 3 核」说的是哪台机器上的 3 核，加总之后的数不描述任何一个可以调度的地方。
 */
function NodeBudget({
  cluster,
  index,
  total,
  source,
}: {
  cluster: TopologyCluster;
  index: number;
  total: number;
  source: Resource<ClusterTopologyData>;
}) {
  return (
    <section className="border-b border-[var(--line)]">
      <h2 className="flex flex-wrap items-baseline gap-x-3 px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
        <span>节点可调度余量 · 调度时的账</span>
        <span className="mono normal-case tracking-normal text-[var(--paper-dim)]">
          {clusterName(index, total)} · {cluster.namespace}
        </span>
      </h2>
      {!cluster.reachable ? (
        <Empty>
          {cluster.error ?? '读不到这个集群。'}
          {' '}它的余量这一拍没有数，不是「还剩很多」。
        </Empty>
      ) : (
        <>
          <Readings
            from={source}
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
              <Reading label="采集于" value={<Moment iso={source.data?.collected_at} />} />
            </div>
          </Readings>
          {/* 配额那一半已在表里逐行画了占比；节点这一半没有：余量是一列孤立的
              数字，「这台还剩 3 核、那台还剩 4 核」读不出调度器会往哪台放。按可分配
              量算成百分比排开，谁快满了是第一眼的事。 */}
          <BarList
            label="每台节点已申报的 CPU 占可分配量的比例"
            note="申报量 ÷ 可分配量（GET /api/v1/cluster/topology）；没有可分配量的节点不画长度"
            max={100}
            rows={cluster.nodes.map((n) => ({
              id: n.name,
              label: n.name,
              value:
                n.cpu_allocatable_millicores && n.cpu_allocatable_millicores > 0
                  ? Math.round((n.cpu_requested_millicores / n.cpu_allocatable_millicores) * 100)
                  : 0,
              note:
                n.cpu_allocatable_millicores != null
                  ? `${formatCpu(n.cpu_requested_millicores)} / ${formatCpu(n.cpu_allocatable_millicores)}`
                  : '可分配量读不到',
              tone:
                n.cpu_allocatable_millicores != null &&
                n.cpu_requested_millicores / n.cpu_allocatable_millicores >= 0.9
                  ? ('danger' as const)
                  : undefined,
            }))}
          />
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
  );
}

/** 一个集群的存储账本：卷声明是准入时写下的，盘上占用是平台自己量出来的。 */
function StorageLedger({
  cluster,
  index,
  total,
}: {
  cluster: TopologyCluster;
  index: number;
  total: number;
}) {
  const claims = collectClaims(cluster);
  return (
    <section className="border-b border-[var(--line)]">
      <h2 className="flex flex-wrap items-baseline gap-x-3 px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
        <span>存储账本 · 声明 vs 实测</span>
        <span className="mono normal-case tracking-normal text-[var(--paper-dim)]">
          {clusterName(index, total)} · {cluster.namespace}
        </span>
      </h2>
      {claims.length === 0 ? (
        <Empty>
          {cluster.reachable
            ? '这个集群里没有挂卷的 Pod：不是「没有数据」，是这一拍没有卷声明可读。'
            : '这个集群这次读不到，卷声明也就无从谈起。'}
        </Empty>
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
  );
}

/** 从拓扑里收齐卷声明。同一个声明被多个 Pod 挂着时只算一行：它是一块盘。 */
function collectClaims(cluster: TopologyCluster): TopologyClaim[] {
  const claims = new Map<string, TopologyClaim>();
  for (const node of cluster.nodes) {
    for (const pod of node.pods) {
      for (const volume of pod.volumes) {
        if (volume.claim && !claims.has(volume.claim.name)) {
          claims.set(volume.claim.name, volume.claim);
        }
      }
    }
  }
  for (const pod of [...cluster.unscheduled, ...cluster.unplaced]) {
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

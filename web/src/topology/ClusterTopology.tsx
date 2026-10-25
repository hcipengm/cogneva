import { useState } from 'react';
import {
  getClusterTopology,
  type TopologyCluster,
  type TopologyNode,
  type TopologyPod,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, ReadError } from '@/ui/primitives';

/**
 * 集群拓扑：集群 → 节点 → Pod → 容器，逐层点开。
 *
 * 三层而不是一张总图，是因为这三种对象回答的是不同的问题：「这台机器还剩多少」
 * 需要可分配量与申报量，「这个 Pod 为什么没就绪」需要容器状态与重启次数。把
 * 它们叠在一张图上，读者只能看到一堆点。
 *
 * 每个数字都能顺着点到它的来源：Pod 上的申报量加起来就是节点那一行；容器状态
 * 加起来就是 Pod 的就绪与否。点不开的数字不要画在这里——那会变成装饰。
 */
export function ClusterTopology() {
  const topology = useResource('集群拓扑', getClusterTopology, 10_000);
  const data = topology.data;

  if (topology.error && !data) {
    return (
      <div className="px-4 py-3">
        <ReadError what="集群拓扑" error={topology.error} onRetry={topology.refresh} />
      </div>
    );
  }
  if (!data) return <Loading what="集群拓扑" />;

  if (data.clusters.length === 0) {
    return (
      <div className="px-4 py-3">
        <Empty>
          这个进程不在集群里运行，读不到节点与 Pod。拓扑只在集群内的部署里有数据源。
        </Empty>
      </div>
    );
  }

  return (
    <div className="flex flex-col divide-y divide-[var(--line)]">
      {data.clusters.map((cluster) => (
        <ClusterRow key={cluster.id} cluster={cluster} collectedAt={data.collected_at} />
      ))}
    </div>
  );
}

function ClusterRow({ cluster, collectedAt }: { cluster: TopologyCluster; collectedAt: string }) {
  const pods = cluster.nodes.reduce((sum, node) => sum + node.pods.length, 0);
  const ready = cluster.nodes.filter((node) => node.ready).length;

  return (
    <div className="flex flex-col">
      <header className="flex flex-wrap items-baseline gap-x-4 gap-y-1 px-4 py-2">
        <span className="flex items-center gap-1.5">
          <Dot tone={cluster.reachable ? 'ok' : 'danger'} />
          <span className="mono text-[12px] text-[var(--paper)]">{cluster.id}</span>
        </span>
        {cluster.version && <span className="mono text-[11px] text-[var(--muted)]">{cluster.version}</span>}
        <span className="text-[11px] text-[var(--paper-dim)]">
          节点 {ready}/{cluster.nodes.length} 就绪 · Pod {pods}
          {cluster.unscheduled.length > 0 && ` · 未落节点 ${cluster.unscheduled.length}`}
        </span>
        <span className="mono ml-auto text-[10px] text-[var(--muted)]">
          {new Date(collectedAt).toLocaleTimeString('zh-CN')} 读
        </span>
      </header>

      {cluster.error && (
        <p className="mono px-4 pb-2 text-[11px]" style={{ color: 'var(--warn)' }}>
          {cluster.error}
        </p>
      )}

      {cluster.nodes.length > 0 && (
        <div className="grid grid-cols-1 gap-px bg-[var(--line)] md:grid-cols-2 xl:grid-cols-3">
          {cluster.nodes.map((node) => (
            <NodeCard key={node.name} node={node} />
          ))}
        </div>
      )}

      {cluster.unscheduled.length > 0 && (
        <div className="border-t border-[var(--line)] px-4 py-2">
          <h3 className="mb-1 text-[10px] uppercase tracking-[0.14em]" style={{ color: 'var(--warn)' }}>
            没有落在任何节点上的 Pod
          </h3>
          <div className="flex flex-col gap-0.5">
            {cluster.unscheduled.map((pod) => (
              <PodRow key={`${pod.namespace}/${pod.name}`} pod={pod} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function NodeCard({ node }: { node: TopologyNode }) {
  const [open, setOpen] = useState(false);
  const cpuPercent = ratio(node.cpu_requested_millicores, node.cpu_allocatable_millicores);
  const memoryPercent = ratio(node.memory_requested_bytes, node.memory_allocatable_bytes);

  return (
    <div className="bg-[var(--ink-900)]">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        aria-expanded={open}
        className="flex w-full flex-col gap-1.5 px-3 py-2 text-left transition-colors hover:bg-[var(--ink-800)]"
      >
        <span className="flex items-center gap-2">
          <Dot tone={node.ready ? 'ok' : 'danger'} />
          <span className="mono truncate text-[12px] text-[var(--paper)]">{node.name}</span>
          <span className="mono text-[10px] text-[var(--muted)]">{node.roles.join(' · ')}</span>
          <span className="mono ml-auto text-[10px] text-[var(--muted)]">
            {node.pods.length}
            {node.pods_allocatable != null ? `/${node.pods_allocatable}` : ''} Pod
          </span>
        </span>
        <Bar
          label="CPU"
          used={node.cpu_requested_millicores}
          total={node.cpu_allocatable_millicores}
          format={formatCpu}
        />
        <Bar
          label="内存"
          used={node.memory_requested_bytes}
          total={node.memory_allocatable_bytes}
          format={formatBytes}
        />
      </button>

      {open && (
        <div className="flex flex-col gap-px border-t border-[var(--line)] bg-[var(--line)]">
          {node.pods.length === 0 ? (
            <p className="bg-[var(--ink-900)] px-3 py-2 text-[11px] text-[var(--muted)]">
              这个命名空间里没有 Pod 排在这台机器上。
            </p>
          ) : (
            node.pods.map((pod) => (
              <div key={`${pod.namespace}/${pod.name}`} className="bg-[var(--ink-900)]">
                <PodRow pod={pod} />
              </div>
            ))
          )}
        </div>
      )}
      {!open && (cpuPercent >= 0.9 || memoryPercent >= 0.9) && (
        <p className="px-3 pb-2 text-[10px]" style={{ color: 'var(--warn)' }}>
          申报量已接近可分配量：新 Pod 可能排不进来，点开看是哪一个占的。
        </p>
      )}
    </div>
  );
}

function PodRow({ pod }: { pod: TopologyPod }) {
  const [open, setOpen] = useState(false);
  const tone = pod.phase === 'Running' && pod.ready ? 'ok' : pod.phase === 'Running' ? 'warn' : 'danger';

  return (
    <div className="flex flex-col">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        aria-expanded={open}
        className="flex w-full items-baseline gap-2 px-3 py-1 text-left transition-colors hover:bg-[var(--ink-800)]"
      >
        <Dot tone={tone} />
        <span className="mono min-w-0 flex-1 truncate text-[11px] text-[var(--paper-dim)]">{pod.name}</span>
        <span className="mono shrink-0 text-[10px] text-[var(--muted)]">
          {pod.phase}
          {pod.restarts > 0 && ` · 重启 ${pod.restarts}`}
        </span>
        <span className="mono shrink-0 text-[10px] text-[var(--muted)]">
          {formatCpu(pod.cpu_requested_millicores)} · {formatBytes(pod.memory_requested_bytes)}
        </span>
      </button>

      {open && (
        <div className="flex flex-col gap-0.5 pb-1.5 pl-8 pr-3">
          {pod.containers.map((container) => (
            <div key={container.name} className="flex items-baseline gap-2">
              <Dot tone={container.ready ? 'ok' : container.state.startsWith('waiting') ? 'warn' : 'danger'} />
              <span className="mono text-[11px] text-[var(--paper-dim)]">{container.name}</span>
              <span className="mono truncate text-[10px] text-[var(--muted)]" title={container.image}>
                {container.image}
              </span>
              <span className="mono ml-auto shrink-0 text-[10px] text-[var(--muted)]">
                {container.state}
                {container.restarts > 0 && ` · 重启 ${container.restarts}`}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Bar({
  label,
  used,
  total,
  format,
}: {
  label: string;
  used: number;
  total: number | null;
  format: (value: number) => string;
}) {
  const percent = ratio(used, total);
  const shown = total == null ? `${format(used)}（不知道上限）` : `${format(used)} / ${format(total)}`;
  return (
    <span className="flex items-center gap-2">
      <span className="mono w-8 shrink-0 text-[10px] text-[var(--muted)]">{label}</span>
      <span className="relative h-1 flex-1 overflow-hidden bg-[var(--ink-700)]">
        <span
          className="absolute inset-y-0 left-0"
          style={{
            width: `${Math.min(100, Math.round(percent * 100))}%`,
            background: percent >= 0.9 ? 'var(--danger)' : percent >= 0.75 ? 'var(--warn)' : 'var(--live)',
          }}
        />
      </span>
      <span className="mono shrink-0 text-[10px] text-[var(--muted)]">{shown}</span>
    </span>
  );
}

function Dot({ tone }: { tone: 'ok' | 'warn' | 'danger' }) {
  const color = tone === 'ok' ? 'var(--ok)' : tone === 'warn' ? 'var(--warn)' : 'var(--danger)';
  return <span aria-hidden className="h-1.5 w-1.5 shrink-0 rounded-full" style={{ background: color }} />;
}

function ratio(used: number, total: number | null): number {
  if (total == null || total <= 0) return 0;
  return used / total;
}

function formatCpu(millicores: number): string {
  if (millicores === 0) return '0';
  if (millicores % 1000 === 0) return `${millicores / 1000} 核`;
  return `${(millicores / 1000).toFixed(1)} 核`;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
}

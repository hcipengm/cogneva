import { useState } from 'react';
import {
  getClusterTopology,
  type TopologyCluster,
  type TopologyContainer,
  type TopologyEvent,
  type TopologyNode,
  type TopologyPod,
  type TopologyVolume,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, ReadError } from '@/ui/primitives';

/**
 * 集群拓扑：集群 → 节点 → Pod → 容器 → 挂载与事件。
 *
 * 每多一层，回答的问题就换一个：节点那一层是「这台机器还剩多少」，Pod 那一层是
 * 「哪个东西没就绪」，容器那一层是「它为什么没就绪」——状态只说它在等，事件才说
 * 它在等什么。所以最底层放的是事件、挂载与上限，而不是又一个计数。
 *
 * 申报量与用量是两个数，永远并排：申报量决定排不排得进来，用量决定这台机器累不累。
 * 混成一个数，读者会拿其中一个去回答另一个问题。没有 metrics-server 时用量显示
 * 「—」并在集群那一行说明，不画一条零线——零用量和没有数据是两件事。
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
        {!cluster.metrics_available && (
          <span className="text-[11px]" style={{ color: 'var(--muted)' }}>
            没有 metrics-server：只显示申报量，实际用量未知
          </span>
        )}
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
          requested={node.cpu_requested_millicores}
          used={node.cpu_used_millicores}
          total={node.cpu_allocatable_millicores}
          format={formatCpu}
        />
        <Bar
          label="内存"
          requested={node.memory_requested_bytes}
          used={node.memory_used_bytes}
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
  const [openContainer, setOpenContainer] = useState<string | null>(null);
  const tone = pod.phase === 'Running' && pod.ready ? 'ok' : pod.phase === 'Running' ? 'warn' : 'danger';
  const warnings = pod.events.filter((event) => event.kind === 'Warning').length;

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
        {warnings > 0 && (
          <span className="mono shrink-0 text-[10px]" style={{ color: 'var(--warn)' }}>
            {warnings} 条告警事件
          </span>
        )}
        <span className="mono shrink-0 text-[10px] text-[var(--muted)]">
          {pod.phase}
          {pod.restarts > 0 && ` · 重启 ${pod.restarts}`}
        </span>
        <span className="mono shrink-0 text-[10px] text-[var(--muted)]">
          {formatCpu(pod.cpu_requested_millicores)} · {formatBytes(pod.memory_requested_bytes)}
          {pod.cpu_used_millicores != null && (
            <span style={{ color: 'var(--paper-dim)' }}>
              {' '}
              （实 {formatCpu(pod.cpu_used_millicores)} · {formatBytes(pod.memory_used_bytes ?? 0)}）
            </span>
          )}
        </span>
      </button>

      {open && (
        <div className="flex flex-col gap-2 pb-2 pl-6 pr-3">
          {pod.conditions.some((condition) => !condition.ok) && (
            <section className="flex flex-col gap-0.5">
              <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">没成立的条件</h4>
              {pod.conditions
                .filter((condition) => !condition.ok)
                .map((condition) => (
                  <div key={condition.kind} className="flex items-baseline gap-2 text-[10.5px]">
                    <Dot tone="warn" />
                    <span className="mono shrink-0" style={{ color: 'var(--warn)' }}>
                      {condition.kind}
                    </span>
                    <span className="min-w-0 flex-1 text-[var(--muted)]">
                      {[condition.reason, condition.message].filter(Boolean).join('：')}
                    </span>
                  </div>
                ))}
              <p className="text-[10px] text-[var(--muted)]">
                其余 {pod.conditions.filter((condition) => condition.ok).length} 项条件成立。
              </p>
            </section>
          )}

          <section className="flex flex-col gap-0.5">
            <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">容器</h4>
            {pod.containers.map((container) => (
              <ContainerRow
                key={container.name}
                container={container}
                open={openContainer === container.name}
                onToggle={() =>
                  setOpenContainer((current) => (current === container.name ? null : container.name))
                }
              />
            ))}
          </section>

          {pod.volumes.length > 0 && (
            <section className="flex flex-col gap-0.5">
              <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">卷</h4>
              {pod.volumes.map((volume) => (
                <VolumeRow key={volume.name} volume={volume} />
              ))}
            </section>
          )}

          {pod.events.length > 0 && (
            <section className="flex flex-col gap-0.5">
              <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">最近事件</h4>
              {pod.events.map((event, index) => (
                <EventRow key={`${event.reason}-${index}`} event={event} />
              ))}
            </section>
          )}
        </div>
      )}
    </div>
  );
}

function ContainerRow({
  container,
  open,
  onToggle,
}: {
  container: TopologyContainer;
  open: boolean;
  onToggle: () => void;
}) {
  const tone = container.ready ? 'ok' : container.state.startsWith('waiting') ? 'warn' : 'danger';

  return (
    <div className="flex flex-col">
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={open}
        className="flex w-full items-baseline gap-2 py-0.5 text-left transition-colors hover:bg-[var(--ink-800)]"
      >
        <Dot tone={tone} />
        <span className="mono text-[11px] text-[var(--paper-dim)]">{container.name}</span>
        <span className="mono ml-auto shrink-0 text-[10px] text-[var(--muted)]">
          {container.state}
          {container.restarts > 0 && ` · 重启 ${container.restarts}`}
        </span>
      </button>

      {open && (
        <dl className="mb-1 ml-3 grid grid-cols-[7rem_1fr] gap-x-2 gap-y-0.5 border-l border-[var(--line)] pl-3 text-[10.5px]">
          <dt className="text-[var(--muted)]">镜像</dt>
          <dd className="mono break-all text-[var(--paper-dim)]">{container.image}</dd>

          <dt className="text-[var(--muted)]">申报</dt>
          <dd className="mono text-[var(--paper-dim)]">
            {container.cpu_request_millicores == null && container.memory_request_bytes == null
              ? '没有申报：调度器按 0 算，这台机器会超卖'
              : `CPU ${container.cpu_request_millicores != null ? formatCpu(container.cpu_request_millicores) : '未申报'} · 内存 ${
                  container.memory_request_bytes != null ? formatBytes(container.memory_request_bytes) : '未申报'
                }`}
          </dd>

          <dt className="text-[var(--muted)]">上限</dt>
          <dd className="mono text-[var(--paper-dim)]">
            {container.cpu_limit_millicores == null && container.memory_limit_bytes == null
              ? '没有设上限：节点还剩多少它就能吃多少'
              : `CPU ${container.cpu_limit_millicores != null ? formatCpu(container.cpu_limit_millicores) : '未设'} · 内存 ${
                  container.memory_limit_bytes != null ? formatBytes(container.memory_limit_bytes) : '未设'
                }`}
          </dd>

          {container.cpu_used_millicores != null && (
            <>
              <dt className="text-[var(--muted)]">实际用量</dt>
              <dd className="mono text-[var(--paper-dim)]">
                {formatCpu(container.cpu_used_millicores)} · {formatBytes(container.memory_used_bytes ?? 0)}
                {container.memory_limit_bytes != null && (
                  <span className="text-[var(--muted)]">
                    {' '}
                    （占上限 {percentOf(container.memory_used_bytes ?? 0, container.memory_limit_bytes)}）
                  </span>
                )}
              </dd>
            </>
          )}

          {container.last_termination && (
            <>
              <dt className="text-[var(--muted)]">上次退出</dt>
              <dd className="mono" style={{ color: container.last_termination.exit_code === 0 ? 'var(--paper-dim)' : 'var(--danger)' }}>
                {container.last_termination.reason} · 退出码 {container.last_termination.exit_code}
                {container.last_termination.finished_at && (
                  <span className="text-[var(--muted)]">
                    {' '}
                    · {new Date(container.last_termination.finished_at).toLocaleTimeString('zh-CN')}
                  </span>
                )}
                {container.last_termination.message && (
                  <span className="block text-[var(--muted)]">{container.last_termination.message}</span>
                )}
              </dd>
            </>
          )}

          {container.ports.length > 0 && (
            <>
              <dt className="text-[var(--muted)]">端口</dt>
              <dd className="mono text-[var(--paper-dim)]">
                {container.ports
                  .map((port) => `${port.name ? `${port.name}:` : ''}${port.container_port}/${port.protocol}`)
                  .join(' · ')}
              </dd>
            </>
          )}

          {container.mounts.length > 0 && (
            <>
              <dt className="text-[var(--muted)]">挂载</dt>
              <dd className="mono flex flex-col text-[var(--paper-dim)]">
                {container.mounts.map((mount) => (
                  <span key={`${mount.name}:${mount.mount_path}`}>
                    {mount.mount_path} ← {mount.name}
                    {mount.read_only ? '（只读）' : ''}
                  </span>
                ))}
              </dd>
            </>
          )}

          {container.probes.length > 0 && (
            <>
              <dt className="text-[var(--muted)]">探针</dt>
              <dd className="text-[var(--paper-dim)]">{container.probes.join(' · ')}</dd>
            </>
          )}
        </dl>
      )}
    </div>
  );
}

function VolumeRow({ volume }: { volume: TopologyVolume }) {
  return (
    <div className="flex items-baseline gap-2 text-[10.5px]">
      <span className="mono text-[var(--paper-dim)]">{volume.name}</span>
      <span className="mono text-[var(--muted)]">{volume.kind}</span>
      {volume.claim && (
        <span className="mono min-w-0 flex-1 truncate text-[var(--muted)]" title={volume.claim.volume_name ?? ''}>
          {volume.claim.name} · {volume.claim.phase}
          {volume.claim.capacity_bytes != null && ` · ${formatBytes(volume.claim.capacity_bytes)}`}
          {volume.claim.storage_class && ` · ${volume.claim.storage_class}`}
          {volume.claim.access_modes.length > 0 && ` · ${volume.claim.access_modes.join('/')}`}
        </span>
      )}
    </div>
  );
}

function EventRow({ event }: { event: TopologyEvent }) {
  const warning = event.kind === 'Warning';
  return (
    <div className="flex items-baseline gap-2 text-[10.5px]">
      <Dot tone={warning ? 'warn' : 'ok'} />
      <span className="mono shrink-0 text-[var(--paper-dim)]">{event.reason}</span>
      <span className="min-w-0 flex-1 text-[var(--muted)]">{event.message}</span>
      {event.count > 1 && <span className="mono shrink-0 text-[var(--muted)]">×{event.count}</span>}
      {event.last_seen && (
        <span className="mono shrink-0 text-[var(--muted)]">
          {new Date(event.last_seen).toLocaleTimeString('zh-CN')}
        </span>
      )}
    </div>
  );
}

function Bar({
  label,
  requested,
  used,
  total,
  format,
}: {
  label: string;
  requested: number;
  used: number | null;
  total: number | null;
  format: (value: number) => string;
}) {
  const percent = ratio(requested, total);
  const usedPercent = used == null ? null : ratio(used, total);
  // 用量条压在申报条上面：申报量是「预留」，用量是「真在用」。两条一个颜色分不出
  // 谁是谁，所以用量用实心、申报用同一色的半透明底。
  return (
    <span className="flex items-center gap-2">
      <span className="mono w-8 shrink-0 text-[10px] text-[var(--muted)]">{label}</span>
      <span className="relative h-1.5 flex-1 overflow-hidden bg-[var(--ink-700)]">
        <span
          className="absolute inset-y-0 left-0"
          style={{
            width: `${Math.min(100, Math.round(percent * 100))}%`,
            background:
              percent >= 0.9
                ? 'color-mix(in srgb, var(--danger) 45%, transparent)'
                : percent >= 0.75
                  ? 'color-mix(in srgb, var(--warn) 45%, transparent)'
                  : 'color-mix(in srgb, var(--live) 45%, transparent)',
          }}
        />
        {usedPercent != null && (
          <span
            className="absolute inset-y-0 left-0 opacity-90"
            style={{
              width: `${Math.min(100, Math.round(usedPercent * 100))}%`,
              background: usedPercent >= 0.9 ? 'var(--danger)' : 'var(--live)',
            }}
          />
        )}
      </span>
      <span className="mono shrink-0 text-[10px] text-[var(--muted)]">
        {total == null
          ? `${format(requested)}（不知道上限）`
          : `${format(requested)} / ${format(total)}${used != null ? ` · 实 ${format(used)}` : ''}`}
      </span>
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

function percentOf(part: number, whole: number): string {
  if (whole <= 0) return '—';
  const value = (part / whole) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
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

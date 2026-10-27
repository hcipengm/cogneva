import { useState } from 'react';
import {
  getClusterTopology,
  type ClusterTopology as ClusterTopologyData,
  type TopologyCluster,
  type TopologyNode,
  type TopologyPod,
} from '@/api/client';
import { useResource, type Resource } from '@/hooks/useResource';
import { BreathingTopology } from '@/topology/BreathingTopology';
import { clusterName, formatBytes, formatCpu } from '@/topology/format';
import { ContainerRow, Dot, EventRow, VolumeRow } from '@/topology/rows';
import { Empty, Loading, ReadError } from '@/ui/primitives';

/**
 * 集群拓扑：集群 → 节点 → Pod → 容器。
 *
 * 包含关系到这里为止，再往下是容器里的进程——容器是隔离的运行环境，不是进程本身，
 * 而这个接口不读进程，所以不画。
 *
 * 卷与事件跟容器并列，都不在容器下面：卷是 Pod 级的定义（`spec.volumes`），容器用
 * `volumeMounts` 把其中一卷挂进自己的文件系统，所以「挂载」是容器的一个字段，被挂
 * 的那块盘不是；事件更是独立的对象，靠 `involvedObject` 关联到 Pod 或节点，谁都可
 * 以有事件。把它们写成容器的下级，读者会以为卷跟着容器走、Pod 删了事件就没了。
 *
 * 每多一层，回答的问题就换一个：节点那一层是「这台机器还剩多少」，Pod 那一层是
 * 「哪个东西没就绪」，容器那一层是「它为什么没就绪」——状态只说它在等，容器里展
 * 开的是镜像、上限、探针与挂载，而它在等什么要读 Pod 上那条事件。
 *
 * 申报量与用量是两个数，永远并排：申报量决定排不排得进来，用量决定这台机器累不累。
 * 混成一个数，读者会拿其中一个去回答另一个问题。没有 metrics-server 时用量显示
 * 「—」并在集群那一行说明，不画一条零线——零用量和没有数据是两件事。
 */
export function ClusterTopology({ reading }: { reading?: Resource<ClusterTopologyData> } = {}) {
  // 调用方已经读着同一份读数时（资源页把它挂在顶上，下面还要用自己的那份算余量），
  // 这里就不再单开一条轮询：同一个接口一屏问两次，两个数还会各自按自己的间隔跳。
  const own = useResource('集群拓扑', getClusterTopology, 10_000, reading === undefined);
  const topology = reading ?? own;
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
      {/* 先给图，再给表：图回答「谁在动、哪里坏了」，读到这里的人接着往下才看得懂
          每一行的数为什么是那个数。 */}
      <BreathingTopology data={data} />
      {data.clusters.map((cluster, index) => (
        <ClusterRow
          key={`${cluster.id}-${index}`}
          cluster={cluster}
          index={index}
          total={data.clusters.length}
          collectedAt={data.collected_at}
        />
      ))}
    </div>
  );
}

function ClusterRow({
  cluster,
  index,
  total,
  collectedAt,
}: {
  cluster: TopologyCluster;
  index: number;
  total: number;
  collectedAt: string;
}) {
  const pods = cluster.nodes.reduce((sum, node) => sum + node.pods.length, 0);
  const ready = cluster.nodes.filter((node) => node.ready).length;
  const placed = cluster.nodes.flatMap((node) => node.pods);
  const counts = tally(placed);

  return (
    <div className="flex flex-col">
      <header className="flex flex-wrap items-baseline gap-x-4 gap-y-1 px-4 py-2">
        <span className="flex items-center gap-1.5">
          <Dot tone={cluster.reachable ? 'ok' : 'danger'} />
          <span className="text-[12px] text-[var(--paper)]" title={`接口标识 ${cluster.id}`}>
            {clusterName(index, total)}
          </span>
        </span>
        {cluster.version && <span className="mono text-[11px] text-[var(--muted)]">{cluster.version}</span>}
        <span className="mono text-[11px] text-[var(--muted)]">命名空间 {cluster.namespace}</span>
        <span className="text-[11px] text-[var(--paper-dim)]">
          节点 {ready}/{cluster.nodes.length} 就绪 · Pod {pods}
          {counts.finished > 0 && `（已完成 ${counts.finished}）`}
        </span>
        <UsageSourceNote source={cluster.usage_source} />
        <span className="mono ml-auto text-[10px] text-[var(--muted)]">
          {new Date(collectedAt).toLocaleTimeString('zh-CN')} 读
        </span>
      </header>

      <p className="px-4 pb-1 text-[10px] text-[var(--muted)]">
        Pod 与节点都是整个集群的：别的命名空间（系统组件、监控）也画在它的节点上。
        事件与卷声明只读<span className="mono"> {cluster.namespace} </span>
        这一个命名空间，别的命名空间里的 Pod 那两格是空的，不是没有。
      </p>

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
            调度器还没给它节点（Pending / 没有 nodeName）
          </h3>
          <div className="flex flex-col gap-0.5">
            {cluster.unscheduled.map((pod) => (
              <PodRow key={`${pod.namespace}/${pod.name}`} pod={pod} />
            ))}
          </div>
        </div>
      )}

      {cluster.unplaced.length > 0 && (
        <div className="border-t border-[var(--line)] px-4 py-2">
          <h3 className="mb-1 text-[10px] uppercase tracking-[0.14em]" style={{ color: 'var(--warn)' }}>
            在节点上，但这次没读到那台节点（{cluster.unplaced.length} 个）
          </h3>
          <p className="mb-1 text-[10px] text-[var(--muted)]">
            {cluster.reachable
              ? '这些 Pod 写着自己在某台节点上，那台节点不在这次的节点列表里：可能是刚被删，也可能是这次只读到了一部分节点。不是「没有被调度」。'
              : '这次没读到节点列表（见上面的错误），所以它们挂不到任何节点下——它们写着自己在哪台机器上，只是这一面看不见那台机器。'}
          </p>
          <div className="flex flex-col gap-0.5">
            {cluster.unplaced.map((pod) => (
              <PodRow key={`${pod.namespace}/${pod.name}`} pod={pod} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * 一个阶段该是什么颜色。
 *
 * `Succeeded` 不是失败：Job 跑完了，它就该是这个样子，把它画成红点是这一面最容易
 * 犯的错——一次部署里十几个 Job 跑完，整屏都是红的，真正 Failed 的那两个就淹了。
 */
function phaseTone(pod: TopologyPod): 'ok' | 'warn' | 'danger' | 'idle' {
  if (pod.phase === 'Succeeded') return 'idle';
  if (pod.phase === 'Failed') return 'danger';
  if (pod.phase === 'Running') return pod.ready ? 'ok' : 'warn';
  if (pod.phase === 'Pending') return 'warn';
  return 'danger';
}

/** 跑完的排最后，其余按名字。 */
function byLiveness(a: TopologyPod, b: TopologyPod): number {
  const finished = (pod: TopologyPod) => (pod.phase === 'Succeeded' ? 1 : 0);
  return finished(a) - finished(b) || a.name.localeCompare(b.name);
}

/** 运行中、未就绪、已完成、失败。四个数分开，合起来才是「Pod 有多少」。 */
function tally(pods: TopologyPod[]) {
  return {
    running: pods.filter((pod) => pod.phase === 'Running' && pod.ready).length,
    unfinished: pods.filter((pod) => pod.phase === 'Running' && !pod.ready).length,
    finished: pods.filter((pod) => pod.phase === 'Succeeded').length,
    failed: pods.filter((pod) => pod.phase === 'Failed' || pod.phase === 'Unknown').length,
  };
}

/**
 * 用量的来源要说出来，因为两个来源的读数不是一回事：metrics-server 是它自己按
 * 15 秒采样的结果，kubelet 的 summary 是这台机器当下的读数。读不到的时候说清缺
 * 什么权限，而不是让读者以为「用量是 0」。
 */
function UsageSourceNote({ source }: { source: TopologyCluster['usage_source'] }) {
  if (source === 'metrics-server') {
    return <span className="text-[11px] text-[var(--muted)]">用量来自 metrics-server</span>;
  }
  if (source === 'kubelet-summary') {
    return (
      <span className="text-[11px] text-[var(--muted)]">
        用量来自 kubelet 的 summary 接口（这个集群没装 metrics-server，不影响读数）
      </span>
    );
  }
  return (
    <span className="text-[11px]" style={{ color: 'var(--warn)' }}>
      读不到实际用量：没有 metrics-server，kubelet 的 summary 也读不了（要 nodes/stats 读权限）。下面只有申报量
    </span>
  );
}

/** 压力条件为真是坏消息，Ready 为真才是好消息——同一份条件，两个方向。 */
function pressureWarnings(node: TopologyNode): string[] {
  return node.conditions
    .filter((condition) =>
      condition.kind === 'Ready' ? !condition.ok : condition.kind.endsWith('Pressure') && condition.ok
    )
    .map((condition) =>
      condition.kind === 'Ready'
        ? '节点 NotReady'
        : `${condition.kind}${condition.reason ? `：${condition.reason}` : ''}`
    );
}

function NodeCard({ node }: { node: TopologyNode }) {
  const [open, setOpen] = useState(false);
  const cpuPercent = ratio(node.cpu_requested_millicores, node.cpu_allocatable_millicores);
  const memoryPercent = ratio(node.memory_requested_bytes, node.memory_allocatable_bytes);
  const pressures = pressureWarnings(node);

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
            // 已经跑完的 Job 排在最后：它们还占着 Pod 名额，但没有人要在这一屏上
            // 找它们，而活着的那几个应该在上面。
            [...node.pods].sort(byLiveness).map((pod) => (
              <div key={`${pod.namespace}/${pod.name}`} className="bg-[var(--ink-900)]">
                <PodRow pod={pod} />
              </div>
            ))
          )}
        </div>
      )}
      {pressures.length > 0 && (
        <p className="px-3 pb-1 text-[10px]" style={{ color: 'var(--danger)' }}>
          {pressures.join(' · ')}：这个节点在驱逐 Pod，光看申报量看不出来。
        </p>
      )}
      {!open && (cpuPercent >= 0.9 || memoryPercent >= 0.9) && (
        <p className="px-3 pb-2 text-[10px]" style={{ color: 'var(--warn)' }}>
          申报量已接近可分配量：新 Pod 可能排不进来，点开看是哪一个占的。
        </p>
      )}
      {/* 「这台机器上的容器」与「屏上的容器」不是一个数。差多少要说出来——
          不说的话，屏上看起来就是全量。这一面读的是整个集群的 Pod，所以差出来的
          只剩不经 k8s 起的容器，而它们照样有进程读数。 */}
      {node.unclaimed_containers != null && node.unclaimed_containers > 0 && (
        <div className="px-3 pb-2">
          <p className="text-[10px] text-[var(--muted)]">
            采集器在这台机器上看到 <span className="mono">{node.census_containers}</span>{' '}
            个容器，其中 <span className="mono">{node.unclaimed_containers}</span>{' '}
            个没有任何 Pod 认领（构建容器、宿主上直接起的进程）：它们有进程读数，但没有
            Pod 这一格。下面按进程数从多到少列出是谁——cgroup 只给容器 id，进程名是这一面
            唯一读得到的身份。
          </p>
          <ul className="mt-1 space-y-0.5">
            {node.unclaimed.map((row) => (
              <li key={row.id} className="mono text-[10px] text-[var(--muted)]">
                {row.id} · {row.processes} 个进程
                {row.commands.length > 0 && <> · {row.commands.join(' / ')}</>}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function PodRow({ pod }: { pod: TopologyPod }) {
  const [open, setOpen] = useState(false);
  const [openContainer, setOpenContainer] = useState<string | null>(null);
  const tone = phaseTone(pod);
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
        <span className="mono shrink-0 text-[10px] text-[var(--muted)]">{pod.namespace}/</span>
        <span className="mono min-w-0 flex-1 truncate text-[11px] text-[var(--paper-dim)]">{pod.name}</span>
        {warnings > 0 && (
          <span className="mono shrink-0 text-[10px]" style={{ color: 'var(--warn)' }}>
            {warnings} 条告警事件
          </span>
        )}
        <span className="mono shrink-0 text-[10px] text-[var(--muted)]">
          {pod.phase}
          {pod.restarts > 0 && ` · 重启 ${pod.restarts}`}
          {pod.processes != null && ` · ${pod.processes} 个进程`}
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
                censusAt={pod.process_census_at}
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

function ratio(used: number, total: number | null): number {
  if (total == null || total <= 0) return 0;
  return used / total;
}

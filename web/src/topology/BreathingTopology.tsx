import { useState } from 'react';
import type {
  ClusterTopology as ClusterTopologyData,
  TopologyCluster,
  TopologyNode,
  TopologyPod,
} from '@/api/client';
import { formatBytes, formatCpu, percentOf } from '@/topology/format';
import { ContainerRow, Dot, EventRow, VolumeRow } from '@/topology/rows';

/**
 * 集群这一张「活着」的图：节点是心，Pod 绕着它，谁在呼吸由真实用量决定。
 *
 * 呼吸不是装饰：一次呼吸的**快慢**读的是这个 Pod 真实吃掉的 CPU（`cpu_used` 与它
 * 的申请量之比），**动不动**读的是有没有这份读数。没有 metrics-server 时整张图
 * 停着不呼吸，并写明白为什么——一张照样呼吸的图会让人以为机器很闲，而事实上是
 * 「不知道」。读数缺失与读数为零在图上必须是两个样子。
 *
 * 图只回答三条问题，多了就是装饰：
 * - 这台机器上是谁（节点、Pod 各几个，几个没就绪）；
 * - 谁在干活（呼吸的快慢＝CPU 用的比例）；
 * - 哪里坏了（未就绪的色、Warning 事件的红点、重启次数）。
 * 细节数仍由下面那张卡表给：图是索引，不是账本。
 */

/** Pod 第一圈离节点中心多远，往外每多一圈再加多少。 */
const ORBIT_NEAR = 112;
const ORBIT_STEP = 54;
/** 一圈最多放几个：再多点与点之间就挤到看不清。 */
const PER_RING = 8;

/** 这个节点要画几圈。圈数是算出来的，行高也跟着它长——第二圈压到下一行的字上，
 * 比少画几个 Pod 更没法读。 */
function ringsOf(node: TopologyNode): number {
  return Math.max(1, Math.ceil(node.pods.length / PER_RING));
}

function laneHeightOf(node: TopologyNode): number {
  const outer = ORBIT_NEAR + (ringsOf(node) - 1) * ORBIT_STEP;
  return outer * 2 + 96;
}

interface Placed {
  pod: TopologyPod;
  x: number;
  y: number;
  /** 0–1：CPU 用量与申请量之比；null 表示这次没有这份读数。 */
  util: number | null;
  /** 一次呼吸的秒数。用量越高呼吸越急；没有读数时为空，图不呼吸。 */
  breath: number | null;
  warnings: number;
}

function utilization(pod: TopologyPod): number | null {
  if (pod.cpu_used_millicores == null) return null;
  const base = pod.cpu_requested_millicores > 0 ? pod.cpu_requested_millicores : 0;
  if (base === 0) {
    // 没写申请量时按整机分摊没有意义；用一个固定的参照（一核）说明「它在用多少」。
    return Math.min(1, pod.cpu_used_millicores / 1000);
  }
  return Math.min(1, pod.cpu_used_millicores / base);
}

/**
 * 一次呼吸的秒数。
 *
 * 3.4 秒到 1.1 秒这一段：闲的时候慢到几乎是一条静止的环，满载时急到一眼能看出
 * 在动。比例再往里压就会让「很闲」和「很忙」在视觉上分不出来——那正是这张图要
 * 回答的问题。
 */
function breathSeconds(util: number): number {
  return 3.4 - 2.3 * Math.min(1, Math.max(0, util));
}

function place(node: TopologyNode, cx: number, cy: number): Placed[] {
  const pods = node.pods;
  const placed: Placed[] = [];
  pods.forEach((pod, index) => {
    const ring = Math.floor(index / PER_RING);
    const inRing = Math.min(pods.length - ring * PER_RING, PER_RING);
    const slot = index % PER_RING;
    const radius = ORBIT_NEAR + ring * ORBIT_STEP;
    // 从 -90° 起、绕一整圈：第一个在最上面，读的人从「正上方」开始数最自然。
    const angle = -Math.PI / 2 + (slot / Math.max(1, inRing)) * Math.PI * 2;
    const util = utilization(pod);
    placed.push({
      pod,
      x: cx + Math.cos(angle) * radius,
      y: cy + Math.sin(angle) * radius,
      util,
      breath: util == null ? null : breathSeconds(util),
      warnings: pod.events.filter((event) => event.kind === 'Warning').length,
    });
  });
  return placed;
}

function podTone(pod: TopologyPod): { fill: string; ring: string } {
  if (pod.phase === 'Succeeded') return { fill: 'var(--muted)', ring: 'var(--line)' };
  if (pod.phase === 'Failed') return { fill: 'var(--danger)', ring: 'var(--danger)' };
  if (!pod.ready) return { fill: 'var(--warn)', ring: 'var(--warn)' };
  return { fill: 'var(--ok)', ring: 'var(--ok)' };
}

/** 名字里那串随机后缀在图上读不出来，只留能认人的前段。 */
function shortName(name: string): string {
  const parts = name.split('-');
  if (parts.length <= 2) return name;
  return parts.slice(0, -1).join('-');
}

function hasUsage(cluster: TopologyCluster): boolean {
  return cluster.nodes.some((node) => node.pods.some((pod) => pod.cpu_used_millicores != null));
}

export function BreathingTopology({ data }: { data: ClusterTopologyData }) {
  // 点中的是哪一层，只一个：图上点节点、点 Pod 都落到这里，右侧那一栏跟着换。
  // 选中态是一个名字而不是一个对象——快照每拍换一份，抓着旧对象不放的详情栏会
  // 停在上一次读到的数上，而图已经换了一版。
  const [picked, setPicked] = useState<
    { kind: 'node'; name: string } | { kind: 'pod'; name: string } | null
  >(null);

  const clusters = data.clusters.filter((cluster) => cluster.nodes.length > 0);
  if (clusters.length === 0) return null;

  /**
   * 节点自上而下排开，每一行的高度按它自己有几个 Pod 定。
   *
   * 高度不能是一个常数：超过一圈的节点，第二圈会压到下一行的名字上，而重叠的
   * 标签比少画几个 Pod 更没法读。
   */
  const lanes: { node: TopologyNode; cx: number; cy: number; placed: Placed[] }[] = [];
  let offset = 0;
  for (const cluster of clusters) {
    for (const node of cluster.nodes) {
      const laneHeight = laneHeightOf(node);
      const cx = 120;
      const cy = offset + laneHeight / 2;
      lanes.push({ node, cx, cy, placed: place(node, cx, cy) });
      offset += laneHeight;
    }
  }
  const height = Math.max(200, offset);

  const breathing = clusters.some(hasUsage);

  return (
    <section className="flex min-h-0 flex-col border-b border-[var(--line)]">
      <header className="flex flex-wrap items-baseline gap-x-4 gap-y-1 px-4 py-2">
        <h2 className="text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">
          谁在呼吸 · 每个环的快慢是它真实吃掉的 CPU
        </h2>
        <span className="text-[11px] text-[var(--paper-dim)]">
          {breathing
            ? `用量读数来自 ${clusters.find(hasUsage)?.usage_source ?? '集群'}`
            : '这次没有用量读数（没有 metrics-server，kubelet 也没答）：图不动，不是机器闲着'}
        </span>
      </header>

      <div className="flex flex-col gap-2 px-4 pb-3 lg:flex-row">
        {/* 图本身多高就画多高，外层给一个上限再滚：压扁一张图换来的是一屏读不出
            名字的点，那比要滚一下更贵。 */}
        <div className="max-h-[540px] w-full max-w-[420px] shrink-0 overflow-y-auto">
        <svg
          role="img"
          aria-label="集群里节点与 Pod 的实时图：环的明暗与快慢来自真实用量"
          viewBox={`0 0 400 ${height}`}
          className="w-full"
          style={{ height }}
        >
          {lanes.map(({ node, cx, cy, placed }) => {
            const cpuShare =
              node.cpu_allocatable_millicores && node.cpu_allocatable_millicores > 0
                ? Math.min(1, node.cpu_requested_millicores / node.cpu_allocatable_millicores)
                : 0;
            const cpuUsed =
              node.cpu_used_millicores != null && node.cpu_allocatable_millicores
                ? Math.min(1, node.cpu_used_millicores / node.cpu_allocatable_millicores)
                : null;
            const nodePicked = picked?.kind === 'node' && picked.name === node.name;
            return (
              <g key={node.name}>
                {/* 连线先画：线压在节点与 Pod 底下，读者先看到关系，再看到点。 */}
                {placed.map((point) => (
                  <line
                    key={`edge-${point.pod.name}`}
                    x1={cx}
                    y1={cy}
                    x2={point.x}
                    y2={point.y}
                    stroke="var(--line)"
                    strokeWidth={
                      picked?.kind === 'pod' && picked.name === point.pod.name ? 1.6 : 1
                    }
                    strokeDasharray={point.pod.ready ? undefined : '3 3'}
                  />
                ))}

                {/* 节点的呼吸：整机 CPU 用量的比例。没有读数时是一圈静止的虚线。 */}
                <circle
                  cx={cx}
                  cy={cy}
                  r={54}
                  fill="none"
                  stroke="var(--brass)"
                  strokeWidth={1}
                  className={cpuUsed == null ? undefined : 'topology-pulse'}
                  strokeDasharray={cpuUsed == null ? '2 4' : undefined}
                  style={{
                    opacity: cpuUsed == null ? 0.3 : 0.35,
                    animationDuration: cpuUsed == null ? undefined : `${breathSeconds(cpuUsed)}s`,
                  }}
                />
                {/* 节点本身也能点：点它是「这台机器怎么了」，点 Pod 是「这个部件
                    怎么了」。两层问的不是一件事，图上让它们各自可点，省掉一层菜单。 */}
                <circle
                  cx={cx}
                  cy={cy}
                  r={40}
                  fill="var(--ink-800)"
                  stroke={nodePicked ? 'var(--paper)' : 'var(--line-strong)'}
                  strokeWidth={nodePicked ? 1.6 : 1}
                  onClick={() => setPicked({ kind: 'node', name: node.name })}
                  style={{ cursor: 'pointer' }}
                >
                  <title>{`${node.name}\n${node.ready ? '就绪' : '未就绪'} · ${node.pods.length} 个 Pod\n点这里看这台机器`}</title>
                </circle>
                {/* 申报量是个扇形：它决定「还排不排得进来」。 */}
                <circle
                  cx={cx}
                  cy={cy}
                  r={40}
                  fill="none"
                  stroke="var(--brass)"
                  strokeWidth={3}
                  strokeDasharray={`${cpuShare * 251.3} 251.3`}
                  transform={`rotate(-90 ${cx} ${cy})`}
                  opacity={0.85}
                />
                <text
                  x={cx}
                  y={cy - 2}
                  textAnchor="middle"
                  style={{ fill: 'var(--paper)', fontSize: 11, fontFamily: 'var(--font-ui)' }}
                >
                  {shortName(node.name)}
                </text>
                <text
                  x={cx}
                  y={cy + 13}
                  textAnchor="middle"
                  className="mono"
                  style={{ fill: node.ready ? 'var(--ok)' : 'var(--danger)', fontSize: 9 }}
                >
                  {node.pods.length} Pod · {node.ready ? '就绪' : '未就绪'}
                </text>

                {placed.map((point) => {
                  const tone = podTone(point.pod);
                  const selected = picked?.kind === 'pod' && picked.name === point.pod.name;
                  return (
                    <g
                      key={point.pod.name}
                      onClick={() => setPicked({ kind: 'pod', name: point.pod.name })}
                      style={{ cursor: 'pointer' }}
                    >
                      <title>
                        {`${point.pod.name}\n状态 ${point.pod.phase}${point.pod.ready ? ' · 就绪' : ' · 未就绪'}` +
                          `\nCPU ${point.pod.cpu_used_millicores == null ? '无读数' : formatCpu(point.pod.cpu_used_millicores)}` +
                          `（申请 ${formatCpu(point.pod.cpu_requested_millicores)}）` +
                          `\n内存 ${point.pod.memory_used_bytes == null ? '无读数' : formatBytes(point.pod.memory_used_bytes)}` +
                          `\n重启 ${point.pod.restarts} 次 · 容器 ${point.pod.containers.length} 个` +
                          (point.warnings > 0 ? `\nWarning 事件 ${point.warnings} 条` : '')}
                      </title>
                      {/* 呼吸环：快慢来自这个 Pod 真实用掉的 CPU。没有读数就没有环。 */}
                      {point.breath != null && (
                        <circle
                          cx={point.x}
                          cy={point.y}
                          r={12}
                          fill="none"
                          stroke={tone.ring}
                          strokeWidth={1.4}
                          className="topology-pulse"
                          style={{ animationDuration: `${point.breath}s` }}
                        />
                      )}
                      <circle
                        cx={point.x}
                        cy={point.y}
                        r={selected ? 9 : 7}
                        fill={tone.fill}
                        stroke={selected ? 'var(--paper)' : 'var(--ink-900)'}
                        strokeWidth={selected ? 1.6 : 1}
                        opacity={point.pod.ready ? 0.9 : 0.65}
                      />
                      {point.warnings > 0 && (
                        <circle cx={point.x + 8} cy={point.y - 8} r={2.6} fill="var(--danger)" />
                      )}
                      <text
                        x={point.x}
                        y={point.y + 20}
                        textAnchor="middle"
                        className="mono"
                        style={{ fill: 'var(--muted)', fontSize: 8 }}
                      >
                        {shortName(point.pod.name)}
                      </text>
                    </g>
                  );
                })}
              </g>
            );
          })}
        </svg>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto">
          <Detail picked={picked} clusters={clusters} onPick={(next) => setPicked(next)} />
        </div>
      </div>

      <p className="px-4 pb-2 text-[10px] leading-relaxed text-[var(--muted)]">
        环的快慢＝这个 Pod 真实用掉的 CPU 与它的申请量之比（越忙越急）；没有用量读数时不呼吸，
        画成静止的虚线。节点外那圈扇形是申报量占整机的比例，它决定「还排不排得进来」，
        与「累不累」是两件事。红点＝这个 Pod 有 Warning 事件，小圆点的边是虚线＝还没就绪。
      </p>
    </section>
  );
}

type Picked = { kind: 'node'; name: string } | { kind: 'pod'; name: string } | null;

/**
 * 右侧那一栏：点到了哪一层就讲哪一层。
 *
 * 没选中时不留空白——空白会被读成「这里什么都没有」，而这一栏要说的恰恰是「图上
 * 每一样都能点」。选中时按名字在**这一份**快照里找，找不到就说找不到：抓着上一次
 * 读到的对象不放，会让详情栏停在一个已经不在的 Pod 上。
 */
function Detail({
  picked,
  clusters,
  onPick,
}: {
  picked: Picked;
  clusters: TopologyCluster[];
  onPick: (next: Picked) => void;
}) {
  if (!picked) {
    return (
      <p className="max-w-[52ch] px-1 py-2 text-[11px] leading-relaxed text-[var(--muted)]">
        点图上的节点圆，看这台机器还剩多少、有没有压力；点外面的一个小圆点，看这个 Pod
        的容器、挂载、卷与事件，容器还能再点开一层——镜像、申报、上限、实际用量、上次
        退出码、端口、探针都在那一层。点上是哪一个是你选的，不是它自己跳出来的：一张
        会自己换内容的细节栏，读者没法拿它对着看。
      </p>
    );
  }

  if (picked.kind === 'node') {
    const node = clusters.flatMap((cluster) => cluster.nodes).find((it) => it.name === picked.name);
    if (!node) {
      return <Vanished what={`节点 ${picked.name}`} />;
    }
    return <NodeDetail node={node} onPick={onPick} />;
  }

  const pools = clusters.flatMap((cluster) => [
    ...cluster.nodes.flatMap((node) => node.pods),
    ...cluster.unscheduled,
    ...cluster.unplaced,
  ]);
  const pod = pools.find((it) => it.name === picked.name);
  if (!pod) {
    return <Vanished what={`Pod ${picked.name}`} />;
  }
  return <PodDetail pod={pod} />;
}

/** 选中过的东西不在这一份快照里了：它被删了，或者这一拍没读到它。 */
function Vanished({ what }: { what: string }) {
  return (
    <p className="px-1 py-2 text-[11px] leading-relaxed text-[var(--muted)]">
      {what} 已经不在这一份读数里：它可能刚被删掉，也可能这一拍没读到。下一次重读若它还在，
      这里会自己回来。
    </p>
  );
}

/** 一台机器：能吃多少、已经划出去多少、真在用多少，以及它自己在报什么。 */
function NodeDetail({ node, onPick }: { node: TopologyNode; onPick: (next: Picked) => void }) {
  const pressures = node.conditions.filter((condition) =>
    condition.kind === 'Ready' ? !condition.ok : condition.kind.endsWith('Pressure') && condition.ok
  );
  const requested = node.cpu_allocatable_millicores
    ? percentOf(node.cpu_requested_millicores, node.cpu_allocatable_millicores)
    : '—';
  const used =
    node.cpu_used_millicores != null && node.cpu_allocatable_millicores
      ? percentOf(node.cpu_used_millicores, node.cpu_allocatable_millicores)
      : null;
  const memoryUsed =
    node.memory_used_bytes != null && node.memory_allocatable_bytes
      ? percentOf(node.memory_used_bytes, node.memory_allocatable_bytes)
      : null;

  return (
    <div className="flex flex-col gap-2 px-1 py-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="mono text-[12px] text-[var(--paper)]">{node.name}</span>
        <span className="text-[11px]" style={{ color: node.ready ? 'var(--ok)' : 'var(--danger)' }}>
          {node.ready ? '就绪' : '未就绪'}
        </span>
        {node.roles.length > 0 && (
          <span className="mono text-[10px] text-[var(--muted)]">{node.roles.join(' · ')}</span>
        )}
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-[var(--paper-dim)]">
        <span>
          CPU 申报 <span className="mono">{formatCpu(node.cpu_requested_millicores)}</span>
          <span className="text-[var(--muted)]">（{requested}）</span>
        </span>
        <span>
          CPU 用量{' '}
          <span className="mono">
            {node.cpu_used_millicores == null ? '无读数' : formatCpu(node.cpu_used_millicores)}
          </span>
          {used && <span className="text-[var(--muted)]">（{used}）</span>}
        </span>
        <span>
          内存申报 <span className="mono">{formatBytes(node.memory_requested_bytes)}</span>
        </span>
        <span>
          内存用量{' '}
          <span className="mono">
            {node.memory_used_bytes == null ? '无读数' : formatBytes(node.memory_used_bytes)}
          </span>
          {memoryUsed && <span className="text-[var(--muted)]">（{memoryUsed}）</span>}
        </span>
        <span>
          可分配 CPU{' '}
          <span className="mono">
            {node.cpu_allocatable_millicores == null ? '—' : formatCpu(node.cpu_allocatable_millicores)}
          </span>
        </span>
        <span>
          可分配内存{' '}
          <span className="mono">
            {node.memory_allocatable_bytes == null ? '—' : formatBytes(node.memory_allocatable_bytes)}
          </span>
        </span>
      </div>

      {pressures.length > 0 && (
        <p className="text-[10.5px]" style={{ color: 'var(--danger)' }}>
          {pressures
            .map((condition) =>
              condition.kind === 'Ready'
                ? '节点 NotReady'
                : `${condition.kind}${condition.reason ? `：${condition.reason}` : ''}`
            )
            .join(' · ')}
          ：这个节点在驱逐 Pod，光看申报量看不出来。
        </p>
      )}

      <section className="flex flex-col gap-0.5">
        <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
          排在这台机器上的 Pod（点一个看它下面）
        </h4>
        {node.pods.length === 0 ? (
          <p className="text-[10.5px] text-[var(--muted)]">这一拍没有 Pod 排在这台机器上。</p>
        ) : (
          node.pods.map((pod) => (
            <button
              key={pod.name}
              type="button"
              onClick={() => onPick({ kind: 'pod', name: pod.name })}
              className="flex items-baseline gap-2 py-0.5 text-left transition-colors hover:bg-[var(--ink-800)]"
            >
              <Dot tone={pod.ready ? 'ok' : pod.phase === 'Succeeded' ? 'idle' : 'warn'} />
              <span className="mono min-w-0 flex-1 truncate text-[10.5px] text-[var(--paper-dim)]">
                {pod.name}
              </span>
              <span className="mono shrink-0 text-[10px] text-[var(--muted)]">{pod.phase}</span>
            </button>
          ))
        )}
      </section>
    </div>
  );
}

/** 一个 Pod：它自己、它的容器（可再点开）、它的卷、它的事件。 */
function PodDetail({ pod }: { pod: TopologyPod }) {
  const [openContainer, setOpenContainer] = useState<string | null>(null);
  const usedCpu = pod.cpu_used_millicores;
  const usedMemory = pod.memory_used_bytes;
  return (
    <div className="flex flex-col gap-2 px-1 py-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="mono text-[12px] text-[var(--paper)]">{pod.name}</span>
        <span className="text-[11px]" style={{ color: pod.ready ? 'var(--ok)' : 'var(--warn)' }}>
          {pod.phase}
          {pod.ready ? ' · 就绪' : ' · 未就绪'}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">{pod.node ?? '未调度'}</span>
      </div>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-[var(--paper-dim)]">
        <span>
          CPU 用量 <span className="mono">{usedCpu == null ? '无读数' : formatCpu(usedCpu)}</span>
        </span>
        <span>
          申请 <span className="mono">{formatCpu(pod.cpu_requested_millicores)}</span>
        </span>
        <span>
          内存用量 <span className="mono">{usedMemory == null ? '无读数' : formatBytes(usedMemory)}</span>
        </span>
        <span>
          申请 <span className="mono">{formatBytes(pod.memory_requested_bytes)}</span>
        </span>
        <span>
          重启 <span className="mono">{pod.restarts}</span> 次
        </span>
        <span>
          容器 <span className="mono">{pod.containers.length}</span> 个
        </span>
      </div>

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
        </section>
      )}

      <section className="flex flex-col gap-0.5">
        <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
          容器（点开一层：镜像、申报、上限、用量、挂载、探针）
        </h4>
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
          <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
            卷（卷声明后面那串是盘上的真实占用）
          </h4>
          {pod.volumes.map((volume) => (
            <VolumeRow key={volume.name} volume={volume} />
          ))}
        </section>
      )}

      {pod.events.length > 0 && (
        <section className="flex flex-col gap-0.5">
          <h4 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">事件</h4>
          {pod.events.map((event, index) => (
            <EventRow key={`${event.reason}-${index}`} event={event} />
          ))}
        </section>
      )}
    </div>
  );
}

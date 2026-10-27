import type { TopologyContainer, TopologyEvent, TopologyVolume } from '@/api/client';
import { formatBytes, formatCpu, percentOf } from '@/topology/format';

/**
 * 拓扑最底下那三行：容器、卷、事件。
 *
 * 它们被放在一个单独的模块里，是因为同一份读数有两个读法：拓扑树把它排在展开的
 * Pod 下面，集群图把它放在右侧的详情栏里。两份实现会长成两个版本——一边补了
 * 「上次退出码 0 不算坏消息」，另一边没有，而读者分不出自己看到的是哪一版。
 *
 * 每一行都能再点开一层：容器点开是镜像、申报、上限、用量、上次退出、端口、挂载、
 * 探针、进程；这些正是「它为什么没就绪」的答案，状态词只说它在等。进程表最下面，
 * 因为它是唯一由容器自己推上来的一格（其余都来自 Pod 状态与指标面）。
 */

export function Dot({ tone }: { tone: 'ok' | 'warn' | 'danger' | 'idle' }) {
  const color =
    tone === 'ok'
      ? 'var(--ok)'
      : tone === 'warn'
        ? 'var(--warn)'
        : tone === 'idle'
          ? 'var(--muted)'
          : 'var(--danger)';
  return <span aria-hidden className="h-1.5 w-1.5 shrink-0 rounded-full" style={{ background: color }} />;
}

export function ContainerRow({
  container,
  censusAt,
  open,
  onToggle,
}: {
  container: TopologyContainer;
  /** 这台节点上采集器最后一次报到的时刻（Pod 的 `process_census_at`）。 */
  censusAt: string | null;
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
          {container.processes != null && ` · ${container.processes.length} 个进程`}
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
              <dd
                className="mono"
                style={{
                  color:
                    container.last_termination.exit_code === 0 ? 'var(--paper-dim)' : 'var(--danger)',
                }}
              >
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

          {/* 进程属于容器。名字来自节点上的采集器：它按宿主 cgroup 树找到这个容器，
              把 `cgroup.procs` 里的 pid 逐个读出来——镜像是什么都读得到，所以第三方
              容器也有这一格。跑本平台镜像的容器还会自己再报一份，两个来源落在同一张表上。 */}
          <dt className="text-[var(--muted)]">进程</dt>
          <dd className="mono flex max-h-40 flex-col overflow-y-auto text-[var(--paper-dim)]">
            {container.processes == null ? (
              <span className="text-[var(--muted)]">{noProcessTable(container, censusAt)}</span>
            ) : (
              container.processes.map((process) => (
                <span key={process.pid} className="break-all">
                  {process.command}
                  <span className="text-[var(--muted)]">
                    {' '}
                    · pid {process.pid} · 父 {process.parent_pid} · {formatBytes(process.rss_bytes)}
                  </span>
                </span>
              ))
            )}
          </dd>
        </dl>
      )}
    </div>
  );
}

/**
 * 这一格为什么是空的。三种现场、三种修法，不该读成同一句话：
 *
 * - 容器已经退出：进程随它一起没了，没有「现在的进程表」这回事；
 * - 这台节点上没有采集器（`censusAt` 为空）：没人能读到它，装 DaemonSet 是集群的事；
 * - 采集器在跑而这一拍没有它：刚起、或者正好落在两拍之间，等下一拍。
 */
function noProcessTable(container: TopologyContainer, censusAt: string | null): string {
  if (container.state.startsWith('terminated')) {
    return '容器已经退出，进程随它一起没了——一格里没有「现在的进程表」这回事。';
  }
  if (censusAt == null) {
    return '这台节点上没有进程采集器（process-census），所以没人能读到它里面的进程。这是集群少了一个 DaemonSet，不是这个容器不报。';
  }
  const ageSec = Math.max(0, Math.round((Date.now() - new Date(censusAt).getTime()) / 1000));
  return `采集器 ${ageSec} 秒前刚报过这台节点，但这一拍没有它：容器可能刚起（60 秒一拍），或者正好落在两拍之间。`;
}

export function VolumeRow({ volume }: { volume: TopologyVolume }) {
  return (
    <div className="flex items-baseline gap-2 text-[10.5px]">
      <span className="mono text-[var(--paper-dim)]">{volume.name}</span>
      <span className="mono text-[var(--muted)]">{volume.kind}</span>
      {volume.claim && (
        <span className="mono min-w-0 flex-1 truncate text-[var(--muted)]" title={volume.claim.volume_name ?? ''}>
          {volume.claim.name} · {volume.claim.phase}
          {volume.claim.capacity_bytes != null && ` · ${formatBytes(volume.claim.capacity_bytes)}`}
          {volume.claim.measured_bytes != null && (
            <span style={{ color: 'var(--paper-dim)' }}>
              {` · 已用 ${formatBytes(volume.claim.measured_bytes)}`}
              {volume.claim.capacity_bytes != null &&
                `（${percentOf(volume.claim.measured_bytes, volume.claim.capacity_bytes)}）`}
            </span>
          )}
          {volume.claim.storage_class && ` · ${volume.claim.storage_class}`}
          {volume.claim.access_modes.length > 0 && ` · ${volume.claim.access_modes.join('/')}`}
        </span>
      )}
    </div>
  );
}

export function EventRow({ event }: { event: TopologyEvent }) {
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

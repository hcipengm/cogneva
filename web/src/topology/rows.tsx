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
 * 探针；这些正是「它为什么没就绪」的答案，状态词只说它在等。
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
        </dl>
      )}
    </div>
  );
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

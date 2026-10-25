import { getBackendHealth } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Loading, Moment, ReadError, Status, Table } from '@/ui/primitives';

/**
 * 探测板：后端健康矩阵。
 *
 * 每一行是一次真的探测留下的读数：谁、通不通、多久回、什么时候探的。`unknown` 与
 * `down` 不是一回事——前者是「探过但没结论」（超时、被跳过），后者是「探了，拒绝了」。
 */
export function ProbesBoard() {
  const health = useResource('后端健康', getBackendHealth, 20_000);
  const data = health.data;

  if (health.error) {
    return <ReadError what="后端健康" error={health.error} onRetry={health.refresh} />;
  }
  if (!data) return <Loading what="后端健康" />;

  return (
    <>
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        总体 {data.overall} · 逐行是各后端最近一次探测的读数；`unknown` 是没探出结论，不是通了。
      </p>
      <Table
        columns={[
          { key: 'name', title: '后端', width: '200px', render: (b) => (
            <span className="mono text-[11px]">{b.name}</span>
          ) },
          { key: 'status', title: '状态', width: '110px', render: (b) => <Status value={b.status} /> },
          { key: 'latency', title: '延迟', width: '100px', render: (b) => (
            <span className="mono text-[11px]">{b.latency_ms != null ? `${b.latency_ms} ms` : '—'}</span>
          ) },
          { key: 'checked', title: '最后检查', width: '120px', render: (b) => <Moment iso={b.last_checked} /> },
          { key: 'error', title: '错误', render: (b) => (
            <span className="text-[11px] text-[var(--danger)]">{b.error ?? ''}</span>
          ) },
        ]}
        rows={data.backends}
        rowKey={(b) => b.name}
      />
    </>
  );
}

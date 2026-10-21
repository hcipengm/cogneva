import type { ActiveAlert, ActiveAlerts } from '@/api/client';

function stamp(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function severityClass(severity: string): string {
  switch (severity) {
    case 'critical':
      return 'text-red-400';
    case 'warning':
      return 'text-amber-400';
    default:
      return 'text-slate-400';
  }
}

function AckCell({
  alert,
  pending,
  onAcknowledge,
}: {
  alert: ActiveAlert;
  pending: boolean;
  onAcknowledge: (id: string) => void;
}) {
  // 进程内事件背后没有行：确认没有可以写的地方，所以这一格不是「没人确认」，
  // 是「这一条不可确认」——两者画成一样会让人以为点一下就能清掉。
  if (alert.source !== 'durable') {
    return <td className="py-1.5 text-right text-slate-600">不可确认</td>;
  }
  if (alert.acknowledged_at) {
    return (
      <td className="py-1.5 text-right text-slate-400">
        <span className="text-emerald-400">{alert.acknowledged_by ?? '?'}</span>{' '}
        {stamp(alert.acknowledged_at)}
      </td>
    );
  }
  return (
    <td className="py-1.5 text-right">
      <button
        onClick={() => onAcknowledge(alert.id)}
        disabled={pending}
        className="rounded px-2 py-0.5 text-xs text-amber-300 transition hover:bg-slate-800 hover:text-amber-200 disabled:text-slate-500"
      >
        {pending ? '确认中…' : '确认'}
      </button>
    </td>
  );
}

function Row({
  alert,
  pending,
  onAcknowledge,
}: {
  alert: ActiveAlert;
  pending: boolean;
  onAcknowledge: (id: string) => void;
}) {
  return (
    <tr className="border-t border-slate-800/60 align-top">
      <td className={`py-1.5 pr-3 font-medium ${severityClass(alert.severity)}`}>
        {alert.severity}
      </td>
      <td className="py-1.5 pr-3 text-slate-200">{alert.event_type}</td>
      <td className="py-1.5 pr-3 text-slate-300">{alert.message}</td>
      <td className="py-1.5 pr-3 text-slate-500">{alert.source}</td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-400">
        {stamp(alert.timestamp)}
      </td>
      <td className="py-1.5 pr-3 text-right tabular-nums text-slate-400">
        {stamp(alert.last_seen_at)}
      </td>
      <AckCell alert={alert} pending={pending} onAcknowledge={onAcknowledge} />
    </tr>
  );
}

export function ActiveAlertsOverlay({
  alerts,
  loading,
  error,
  ackError,
  pendingAck,
  acknowledge,
  refresh,
  onClose,
}: {
  alerts: ActiveAlerts | null;
  loading: boolean;
  error: string | null;
  ackError: string | null;
  pendingAck: Record<string, true>;
  acknowledge: (id: string) => void;
  refresh: () => void;
  onClose: () => void;
}) {
  const rows = alerts?.alerts ?? [];
  const durableRead = alerts?.durable_alerts_read;
  const unacknowledged = rows.filter(
    (a) => a.source === 'durable' && !a.acknowledged_at,
  ).length;

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 p-6 backdrop-blur-sm">
      <div className="max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-2xl border border-slate-700/60 bg-slate-900/90 p-8 shadow-2xl">
        <div className="mb-1 flex items-start justify-between">
          <h1 className="text-2xl font-semibold text-white">活跃告警</h1>
          <div className="flex items-center gap-2">
            <button
              onClick={refresh}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              刷新
            </button>
            <button
              onClick={onClose}
              className="rounded-lg px-2 py-1 text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              ✕
            </button>
          </div>
        </div>

        <p className="mb-6 text-sm text-slate-400">
          进程内事件与 PostgreSQL 耐久表的合并视图，每 30 秒刷新。「最后确认」是生产者
          上一次确认这条仍在 firing 的时间；它停住而告警还挂着，说明生产者已经不看了。
          「确认」是人看到之后留的标记，只对耐久行有——告警重新 firing 时会自动清掉。
          {unacknowledged > 0 && (
            <span className="text-amber-300"> 现在有 {unacknowledged} 条没人确认。</span>
          )}
        </p>

        {error && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {error}
            <div className="mt-1 text-xs text-red-300/80">
              这一格没有读数——不是「没有告警」。
            </div>
          </div>
        )}

        {ackError && (
          <div className="mb-4 rounded-lg bg-red-500/10 p-3 text-sm text-red-400">
            {ackError}
          </div>
        )}

        {durableRead === false && (
          <div className="mb-4 rounded-lg bg-amber-500/10 p-3 text-sm text-amber-300">
            耐久告警表这次没读出来，下面只剩本进程内存里的事件。列表比平时短，
            不代表那些告警已经清了。
          </div>
        )}

        {durableRead === null && !error && (
          <div className="mb-4 rounded-lg bg-slate-800/60 p-3 text-sm text-slate-300">
            这个部署没有配置耐久告警源，只显示本进程内存里的事件。
          </div>
        )}

        {loading && !alerts && <p className="text-sm text-slate-400">读取中…</p>}

        {alerts && rows.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500">
                <th className="pb-2 pr-3 text-left font-medium">级别</th>
                <th className="pb-2 pr-3 text-left font-medium">规则</th>
                <th className="pb-2 pr-3 text-left font-medium">说明</th>
                <th className="pb-2 pr-3 text-left font-medium">来源</th>
                <th className="pb-2 pr-3 text-right font-medium">开始</th>
                <th className="pb-2 pr-3 text-right font-medium">最后确认</th>
                <th className="pb-2 text-right font-medium">有人确认</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((alert, i) => (
                <Row
                  key={`${alert.source}\u0000${alert.id}\u0000${i}`}
                  alert={alert}
                  pending={Boolean(pendingAck[alert.id])}
                  onAcknowledge={acknowledge}
                />
              ))}
            </tbody>
          </table>
        )}

        {alerts && rows.length === 0 && !error && (
          <p className="text-sm text-slate-400">
            当前没有 firing 的告警
            {durableRead === true ? '（耐久表与进程内事件都读过）' : ''}。
          </p>
        )}
      </div>
    </div>
  );
}

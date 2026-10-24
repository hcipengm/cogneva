import type { ActiveAlert } from '@/api/client';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, Reading, Status, Table, ViewFrame } from '@/ui/primitives';

/**
 * 告警面：正在 firing 的条件，以及「有人看过没有」。
 *
 * 读数由工作台共享，这一面不另开一个轮询：状态条上的数与这一页上的数必须是
 * 同一个数，否则读者得先决定信哪一个。
 *
 * `durable_alerts_read` 必须显示出来。它在 false 时意味着这一页只剩进程内事件，
 * 列表变短但没有任何东西被清掉——不写这一句，读者会把「读不到」当成「没事了」。
 */
export function AlertsView() {
  const { alerts: reader } = useServices();
  const appendOutput = useShell((s) => s.appendOutput);
  const data = reader.alerts;
  const list = data?.alerts ?? [];

  const acknowledge = async (alert: ActiveAlert) => {
    const outcome = await reader.acknowledge(alert.id);
    // 结局落在输出格里：确认没写进去时这一行仍然是未确认，那一行不能悄悄消失。
    appendOutput(
      outcome.ok
        ? `已确认告警 ${alert.event_type}（${alert.id}）`
        : `确认告警 ${alert.event_type} 未写入：${outcome.message ?? '未知原因'}`,
    );
  };

  return (
    <ViewFrame
      title="告警"
      meta={
        data
          ? `${data.count} 条在 firing${
              data.durable_alerts_read === false ? ' · 耐久源读失败，列表只剩进程内事件' : ''
            }`
          : reader.error
            ? '读不到'
            : '按 30 秒重读'
      }
      actions={<Button onClick={reader.refresh}>重读</Button>}
    >
      {data?.durable_alerts_read === false && (
        <div className="border-b border-[var(--line)] bg-[color-mix(in_srgb,var(--warn)_10%,transparent)] px-4 py-2 text-[11px] text-[var(--warn)]">
          这一次没读到耐久告警表：下面只是进程内推送的事件，缺少的不等于已经清掉。
        </div>
      )}
      {reader.error ? (
        <ReadError what="活跃告警" error={reader.error} onRetry={reader.refresh} />
      ) : !data && reader.loading ? (
        <Loading what="活跃告警" />
      ) : (
        <>
          <div className="flex flex-wrap border-b border-[var(--line)] py-2">
            <Reading label="在 firing" value={list.length} tone={list.length ? 'danger' : 'ok'} />
            <Reading label="critical" value={list.filter((a) => a.severity === 'critical').length} />
            <Reading label="未确认" value={list.filter((a) => !a.acknowledged_at).length} />
            <Reading
              label="耐久源"
              value={
                data?.durable_alerts_read === null
                  ? '未接'
                  : data?.durable_alerts_read
                    ? '已读到'
                    : '读失败'
              }
              tone={data?.durable_alerts_read === false ? 'warn' : 'plain'}
            />
          </div>
          {reader.ackError && (
            <div className="border-b border-[var(--line)] px-4 py-2 text-[11px] text-[var(--danger)]">
              {reader.ackError}
            </div>
          )}
          <AlertTable alerts={list} acknowledge={acknowledge} pending={reader.pendingAck} />
        </>
      )}
    </ViewFrame>
  );
}

/**
 * 列表与确认按钮必须落在同一屏上。
 *
 * 只接列表就是「看得见、动不了」；只接回调就是「点了看不见」。两件事分开成
 * 两个组件时，它们各自的测试都能过，而中间那个「这一条到底是不是同一条」没人问。
 */
function AlertTable({
  alerts,
  acknowledge,
  pending,
}: {
  alerts: ActiveAlert[];
  acknowledge: (alert: ActiveAlert) => Promise<void>;
  pending: Record<string, true>;
}) {
  if (alerts.length === 0) {
    return <Empty>当前没有 firing 的告警。</Empty>;
  }

  return (
    <Table<ActiveAlert>
      columns={[
        { key: 'sev', title: '级别', width: '90px', render: (a) => <Status value={a.severity} /> },
        { key: 'rule', title: '规则', width: '210px', render: (a) => (
          <span className="mono text-[11px]">{a.event_type}</span>
        ) },
        { key: 'msg', title: '说的是什么', render: (a) => <span className="text-[12px]">{a.message}</span> },
        { key: 'src', title: '来源', width: '90px', render: (a) => (
          <span className="text-[11px] text-[var(--muted)]">{a.source}</span>
        ) },
        { key: 'fired', title: '开始', width: '100px', render: (a) => <Moment iso={a.timestamp} /> },
        { key: 'seen', title: '最后确认在 firing', width: '120px', render: (a) => <Moment iso={a.last_seen_at} /> },
        { key: 'ack', title: '确认', width: '130px', render: (a) =>
          a.acknowledged_at ? (
            <span className="text-[11px] text-[var(--muted)]">已确认 · {a.acknowledged_by}</span>
          ) : a.source === 'durable' ? (
            <Button onClick={() => void acknowledge(a)} disabled={pending[a.id] === true}>
              {pending[a.id] ? '确认中…' : '确认'}
            </Button>
          ) : (
            <span className="text-[11px] text-[var(--muted)]">进程内事件，无处可写</span>
          ) },
      ]}
      rows={alerts}
      rowKey={(a) => a.id}
    />
  );
}

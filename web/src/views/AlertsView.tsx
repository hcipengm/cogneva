import type { ActiveAlert } from '@/api/client';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { breachesSinceSnapshot, type LiveAlertRow } from '@/store/liveAlerts';
import { useLiveAlertStore } from '@/store/liveAlertStore';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';
import { BarList } from '@/ui/plots';

import { Note } from '@/ui/Note';
/** 分钟数写成「几小时几分」：图上那一列只有十几格宽，写 137 比写「2 小时 17 分」省地方，但读者换算不了。 */
function howLong(minutes: number): string {
  if (minutes < 60) return `${minutes} 分钟`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0 ? `${hours} 小时` : `${hours} 小时 ${rest} 分`;
}

/**
 * 告警面：正在 firing 的条件，以及「有人看过没有」。
 *
 * 读数由工作台共享，这一面不另开一个轮询：状态条上的数与这一页上的数必须是
 * 同一个数，否则读者得先决定信哪一个。
 *
 * 表本身是每 30 秒重读一次的快照，**但它上面那一块不是**：`resource_alert` 帧一直
 * 在总线上走，帧说「上次读这张表之后又越了什么界」。少了这一块，一次越界要等
 * 下一拍轮询才出现在告警面上——而告警恰恰是「现在就要知道」的那种读数。
 *
 * `durable_alerts_read` 必须显示出来。它在 false 时意味着这一页只剩进程内事件，
 * 列表变短但没有任何东西被清掉——不写这一句，读者会把「读不到」当成「没事了」。
 */
export function AlertsView() {
  const { alerts: reader } = useServices();
  const appendOutput = useShell((s) => s.appendOutput);
  const data = reader.alerts;
  const list = data?.alerts ?? [];
  const liveRows = useLiveAlertStore((s) => s.rows);
  // 帧比这份快照晚，就说明它不可能是这份快照里的：只列那几条，重读之后它们
  // 自己消失，所以同一件事不会在表里再出现一次。
  const fresh = breachesSinceSnapshot(reader.at, liveRows);

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
              fresh.length > 0 ? ` · ${fresh.length} 次越界刚推到、这张表还没读到` : ''
            }${data.durable_alerts_read === false ? ' · 耐久源读失败，列表只剩进程内事件' : ''}`
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
          <Readings
            from={{ what: reader.what, at: reader.at }}
            source="GET /api/v1/alerts/active"
            caveat="表里这几格按 30 秒重读，两格之间不会自己变；表上面那一块是总线上来的，会在两拍之间自己变。耐久源读失败时列表只剩进程内事件，那时的数回答的是「这一进程见过什么」。"
          >
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
          </Readings>
          {/* 这一面第二个问题是「哪一条烧得最久」。表按开始时间排不出这件事——开始
              得早不等于烧得久，何况表里还混着刚推来、没有开始时间的那几条。时长本身
              是个量，量就该按长短排开，一眼看出有没有一条一直没人管。 */}
          <div className="border-b border-[var(--line)]">
            <BarList
              label="每条在 firing 的规则已经烧了多久"
              note="从这一条开始 firing 算到这页渲染的时刻；下一次重读时重新算"
              rows={list
                .map((alert) => {
                  const minutes = Math.max(
                    0,
                    Math.round((Date.now() - Date.parse(alert.timestamp)) / 60_000),
                  );
                  return {
                    id: alert.id,
                    label: alert.event_type,
                    value: minutes,
                    note: howLong(minutes),
                    tone: alert.severity === 'critical' ? ('danger' as const) : undefined,
                    detail: alert.message,
                  };
                })
                .sort((a, b) => b.value - a.value)}
              empty="现在没有 firing 的告警，没有时长可画"
            />
          </div>
          {reader.ackError && (
            <div className="border-b border-[var(--line)] px-4 py-2 text-[11px] text-[var(--danger)]">
              {reader.ackError}
            </div>
          )}
          {fresh.length > 0 && <FreshBreaches rows={fresh} />}
          <AlertTable alerts={list} acknowledge={acknowledge} pending={reader.pendingAck} />
        </>
      )}
    </ViewFrame>
  );
}

/**
 * 总线上推来、而这张表还没读到的越界。
 *
 * 这一块不是表的一份更快拷贝：表里的行有 dedup_key、有谁点过确认，帧里只有这一次
 * 的实测值，两者身份对不上。所以它只说自己能说的——谁、哪项指标、越到哪儿——并且
 * 明写「表里还没有」，免得读者把它当成一条可以去确认的告警：它背后此刻还没有行。
 */
function FreshBreaches({ rows }: { rows: LiveAlertRow[] }) {
  return (
    <div className="border-b border-[var(--line)] bg-[color-mix(in_srgb,var(--danger)_8%,transparent)] py-2">
      <div className="mb-1 text-[11px] text-[var(--paper-dim)]">
        总线刚推来 {rows.length} 次越界（比这张表更晚，表里还读不到）：
      </div>
      {rows.map((row) => (
        <div key={row.key} className="flex flex-wrap items-baseline gap-2 py-0.5 text-[11px]">
          <span className="mono text-[var(--paper)]">{row.agentId}</span>
          <span className="mono text-[var(--muted)]">{row.metric}</span>
          <span className="mono text-[var(--danger)]">
            {row.current.toFixed(2)} 越过 {row.threshold.toFixed(2)}
          </span>
          {row.crewId && <span className="mono text-[var(--muted)]">crew {row.crewId}</span>}
          <span className="text-[var(--muted)]">
            第一次 <Moment iso={row.firstAt} /> · 最近 <Moment iso={row.lastAt} />
          </span>
          <Note
            className="mono text-[10px] text-[var(--brass)]"
            text={`这一格来自 resource_alert 帧（${row.frames} 帧折成一行），不是这次轮询`}
          >
            帧
          </Note>
        </div>
      ))}
    </div>
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

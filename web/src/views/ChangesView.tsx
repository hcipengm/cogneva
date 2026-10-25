import { useState } from 'react';
import {
  getChangeTimeline,
  getEvolutionChanges,
  getEvolutionMetrics,
  getGovernanceChanges,
  type ChangeLedgerRow,
  type ChangeTimelineEvent,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Status, Table, ViewFrame } from '@/ui/primitives';

/**
 * 一条变更的来路：它入账之后走过哪些状态，每一步由哪一次判读、因为什么。
 *
 * 身份按三段发给读面，不把这个行上那个渲染键拆开：字面量里常有冒号，反解出来的
 * 会是另一条身份。拆错的后果不是报错，是一条永远为空的历史——空历史读作「这条行
 * 从来没动过」，而那正是最容易被当成结论的那个答案。
 */
function ChangeHistory({ row, onClose }: { row: ChangeLedgerRow; onClose: () => void }) {
  const timeline = useResource(
    `变更时间线 ${row.identity}`,
    () => getChangeTimeline({ kind: row.kind, scope: row.scope, literal: row.literal }),
    30_000
  );
  const events = timeline.data?.events ?? [];

  return (
    <section className="border-b border-[var(--line)]">
      <div className="flex items-center px-4 py-2">
        <span className="mono text-[11px] text-[var(--paper-dim)]">{row.identity}</span>
        <button onClick={onClose} className="ml-auto text-[11px] text-[var(--muted)] hover:text-[var(--paper)]">
          收起
        </button>
      </div>
      <dl className="grid grid-cols-[110px_1fr] gap-x-3 gap-y-1 px-4 pb-2 text-[11px]">
        <dt className="text-[var(--muted)]">要什么</dt>
        <dd className="text-[var(--paper-dim)]">{row.assertion ?? row.title ?? '—'}</dd>
        <dt className="text-[var(--muted)]">谁要的</dt>
        <dd className="mono text-[var(--paper-dim)]">{row.sources.join(' · ') || '—'}</dd>
        <dt className="text-[var(--muted)]">判读用的是</dt>
        <dd className="mono text-[var(--paper-dim)]">
          {row.measured_rev ?? '还没有人以某个版本看过它'}
        </dd>
      </dl>
      {timeline.error ? (
        <ReadError what="变更时间线" error={timeline.error} onRetry={timeline.refresh} />
      ) : !timeline.data && timeline.loading ? (
        <Loading what="变更时间线" />
      ) : events.length === 0 ? (
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          没有读到任何一步。入账本身会记一步，所以空着只有两种可能：这条行早于事件表，
          或者它从来只被读过、没有被判读过——两种都不是「状态一直没变」的证据。
        </p>
      ) : (
        <ol className="px-4 pb-3">
          {events.map((e, index) => (
            <TimelineStep key={`${e.observed_at}-${index}`} step={e} newest={index === 0} />
          ))}
        </ol>
      )}
    </section>
  );
}

function TimelineStep({ step, newest }: { step: ChangeTimelineEvent; newest: boolean }) {
  return (
    <li className="flex items-baseline gap-3 border-b border-[color-mix(in_srgb,var(--line)_55%,transparent)] py-1 text-[11px]">
      <span className="mono w-36 shrink-0 text-[var(--muted)]">
        <Moment iso={step.observed_at} />
      </span>
      <span className="w-52 shrink-0">
        <span className="mono text-[var(--muted)]">{step.from_state ?? '入账'}</span>
        <span className="text-[var(--muted)]"> → </span>
        <Status value={step.to_state} />
      </span>
      <span className="mono w-28 shrink-0 truncate text-[var(--paper-dim)]" title={step.measured_rev ?? ''}>
        {step.measured_rev ?? '—'}
      </span>
      <span className="min-w-0 flex-1 text-[var(--muted)]">
        {step.retire_reason ? `退役原因 ${step.retire_reason}` : ''}
        {step.retire_reason && step.unsettled_reason ? ' · ' : ''}
        {step.unsettled_reason ? `未决原因 ${step.unsettled_reason}` : ''}
        {!step.retire_reason && !step.unsettled_reason ? '—' : ''}
        {newest ? ' · 最新一步' : ''}
      </span>
    </li>
  );
}

/**
 * 变更面：台账里还欠着的条目，以及流水线上排队的候选。
 *
 * 两半读的是两个端点，刻意不合并成一个数：台账数的是「谁欠一次判读」，队列数的是
 * 「谁在等一次构建」。把两者相加只会得到一个解释不了的总数。
 */
export function ChangesView() {
  const ledger = useResource('变更账', getGovernanceChanges, 30_000);
  const queue = useResource('变更队列', getEvolutionChanges, 20_000);
  const metrics = useResource('演化计数', getEvolutionMetrics, 60_000);
  const [picked, setPicked] = useState<ChangeLedgerRow | null>(null);

  const page = ledger.data;
  const changes = queue.data?.changes ?? [];

  return (
    <ViewFrame
      title="变更"
      meta={page ? `台账 ${page.total} 条 · 欠账 ${page.outstanding_total} · 待声明 ${page.pending_declarations}` : '按 20 秒重读'}
    >
      {page && (
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          {page.census.map((cell) => (
            <Reading
              key={cell.state}
              label={cell.state}
              value={cell.rows}
              tone={cell.outstanding && cell.rows > 0 ? 'warn' : 'plain'}
            />
          ))}
        </div>
      )}
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">台账欠账</h2>
        {ledger.error ? (
          <ReadError what="变更账" error={ledger.error} onRetry={ledger.refresh} />
        ) : !page && ledger.loading ? (
          <Loading what="变更账" />
        ) : !page || page.outstanding.length === 0 ? (
          <Empty>没有还欠着的变更。</Empty>
        ) : (
          <Table
            columns={[
              { key: 'id', title: '身份', width: '190px', render: (r) => (
                <button
                  onClick={() => setPicked(picked?.identity === r.identity ? null : r)}
                  className="mono truncate text-left text-[11px] text-[var(--paper-dim)] underline-offset-2 hover:underline"
                  title={r.identity}
                >
                  {r.identity}
                </button>
              ) },
              { key: 'state', title: '状态', width: '120px', render: (r) => <Status value={r.state} /> },
              { key: 'title', title: '是什么', render: (r) => (
                <span className="text-[12px]">{r.title ?? r.assertion ?? '—'}</span>
              ) },
              { key: 'seen', title: '首次出现', width: '100px', render: (r) => <Moment iso={r.first_seen_at} /> },
              { key: 'measured', title: '上次判读', width: '110px', render: (r) =>
                r.measured_at ? <Moment iso={r.measured_at} /> : (
                  <span className="text-[11px] text-[var(--warn)]">还没有人看过</span>
                ) },
            ]}
            rows={page.outstanding}
            rowKey={(r) => r.identity}
          />
        )}
      </section>
      {picked && (
        <ChangeHistory row={picked} onClose={() => setPicked(null)} />
      )}
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">演化累计</h2>
        {metrics.error ? (
          <ReadError what="演化计数" error={metrics.error} onRetry={metrics.refresh} />
        ) : metrics.data ? (
          <div className="flex flex-wrap py-2">
            <Reading label="事件" value={metrics.data.events_total} />
            <Reading
              label="事件失败"
              value={metrics.data.events_failed}
              tone={metrics.data.events_failed > 0 ? 'warn' : 'plain'}
            />
            <Reading label="变更落地" value={metrics.data.changes_applied} tone="ok" />
            <Reading
              label="变更失败"
              value={metrics.data.changes_failed}
              tone={metrics.data.changes_failed > 0 ? 'danger' : 'plain'}
            />
          </div>
        ) : (
          <Loading what="演化计数" />
        )}
        <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
          这几个数只增不减，用来对账，不用来看趋势。
        </p>
      </section>
      <section>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          流水线队列{queue.data?.queue ? ` · ${queue.data.queue.dir}` : ''}
        </h2>
        {queue.error ? (
          <ReadError what="变更队列" error={queue.error} onRetry={queue.refresh} />
        ) : !queue.data && queue.loading ? (
          <Loading what="变更队列" />
        ) : changes.length === 0 ? (
          <Empty>队列里没有候选变更。</Empty>
        ) : (
          <Table
            columns={[
              { key: 'id', title: 'id', width: '170px', render: (c) => (
                <span className="mono text-[11px]">{c.id}</span>
              ) },
              { key: 'kind', title: '种类', width: '110px', render: (c) => c.kind },
              { key: 'status', title: '状态', width: '120px', render: (c) => <Status value={c.status} /> },
              { key: 'desc', title: '描述', render: (c) => <span className="text-[12px]">{c.description}</span> },
              { key: 'at', title: '创建', width: '100px', render: (c) => <Moment iso={c.created_at} /> },
            ]}
            rows={changes}
            rowKey={(c) => c.id}
          />
        )}
      </section>
    </ViewFrame>
  );
}

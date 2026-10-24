import {
  getEvolutionChanges,
  getEvolutionMetrics,
  getGovernanceChanges,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Status, Table, ViewFrame } from '@/ui/primitives';

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
                <span className="mono text-[11px]">{r.identity}</span>
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

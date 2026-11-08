import { useState } from 'react';
import {
  getChangeFunnel,
  getChangeTimeline,
  getEvolutionChanges,
  getEvolutionMetrics,
  getGovernanceChanges,
  type ChangeFunnelDay,
  type ChangeLedgerRow,
  type ChangeTimelineEvent,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { money } from '@/topology/format';
import { Note } from '@/ui/Note';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';
import { Strip } from '@/ui/plots';

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
  // 一条变更的状态迁移由任务面推着走：判读、落地、回退都是任务换态。历史是服务端
  // 从账里现读的，帧只说明「该再看一眼了」。
  useKindTickedRefresh(
    ['tasks', 'projects', resourceKey('governance'), resourceKey('evolution')],
    timeline.refresh
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
        <dt className="text-[var(--muted)]">这条花了</dt>
        <dd className="mono text-[var(--paper-dim)]">
          {/* 按这条身份自己的交接 id 求和，不是「全队一天的均摊」：均摊回答不了
              任何一条变更花了多少，它把差一百倍的两条读成同一个数。 */}
          {timeline.data
            ? timeline.data.cost.tokens === null
              ? `量不了：${timeline.data.cost.unreadable ?? '没有账'}`
              : `${timeline.data.cost.tokens.toLocaleString()} token（按 ${timeline.data.cost.ids_covered} 个交接号求和）`
            : '—'}
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
      <Note className="mono w-28 shrink-0 truncate text-[var(--paper-dim)]" text={step.measured_rev ?? ''}>
        {step.measured_rev ?? '—'}
      </Note>
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

/** 比率读数：分母是 0 时它不存在，不能印成 0%。 */
function rate(numerator: number, denominator: number): string {
  if (denominator === 0) return '—';
  return `${((numerator / denominator) * 100).toFixed(1)}%`;
}

/**
 * 一天一行的「尝试 → 落地」漏斗。
 *
 * 这条读数是服务端自己算的（GET /api/v1/governance/changes/funnel），不是谁手工跑
 * 一遍 SQL：手工数在没人跑的那天就是空的，而空着正好是「今天是不是没产出」这个问题
 * 最需要回答的时候。两半来自两张表——尝试数智能体自己记的轮次事件，落地数账上真的
 * 落了地的那一头——所以只看一个数看不出是在哪一段丢掉的。
 */
function FunnelPanel() {
  const funnel = useResource('变更漏斗', () => getChangeFunnel(14), 60_000);
  // 两半各有各的写者：尝试事件随任务换态推出来，落地随账换态推出来。
  useKindTickedRefresh(
    ['tasks', resourceKey('governance'), resourceKey('evolution')],
    funnel.refresh
  );
  const days = funnel.data?.days ?? [];
  const totals = days.reduce(
    (acc, d) => ({
      attempted: acc.attempted + d.attempted,
      rounds: acc.rounds + d.rounds,
      passed: acc.passed + d.passed,
      landed: acc.landed + d.landed,
      landed_with_rev: acc.landed_with_rev + d.landed_with_rev,
    }),
    { attempted: 0, rounds: 0, passed: 0, landed: 0, landed_with_rev: 0 }
  );
  // 用量那两列是 `null` 表示「这一天没读到用量表」。合计只有在**每一天都读到**时
  // 才是一个数：有一天没读到就整列印「未读到」，否则「少算了一天」会读成「那天没花钱」。
  const usageMeasured = days.length > 0 && days.every((d) => d.llm_calls !== null);
  const calls = usageMeasured
    ? days.reduce((n, d) => n + (d.llm_calls ?? 0), 0)
    : null;
  const perDelivery =
    calls !== null && totals.landed_with_rev > 0
      ? `${Math.round(calls / totals.landed_with_rev)}`
      : null;
  // 钱这一列比调用数多两条规矩。一是币种：价目表自己带着币种，页面不能替它选一个
  // 符号——把日元印成美元，读者看不出差了几倍。二是下界：有调用没计价的那些天，
  // 合计数只盖住了其余部分，所以它不是「花了多少」，是「至少花了多少」，这个
  // 「至少」必须印在数前面，否则下界会被读成省钱。
  const costRead = days.length > 0 && days.every((d) => d.unpriced_calls !== null);
  const unpricedCalls = days.reduce((n, d) => n + (d.unpriced_calls ?? 0), 0);
  const currencies = new Set(
    days.map((d) => d.cost_currency).filter((c): c is string => c !== null)
  );
  // 有价却印不出来的天（跨币种）：这种天混进合计会让合计沉默地少一块。
  const unpricedOut = days.filter(
    (d) => d.cost_micros === null && (d.unpriced_calls ?? 0) === 0
  );
  const micros =
    costRead && currencies.size === 1 && unpricedOut.length === 0
      ? days.reduce((n, d) => n + (d.cost_micros ?? 0), 0)
      : null;
  const costGap =
    micros !== null
      ? null
      : !costRead
        ? '未读到'
        : currencies.size > 1
          ? '多币种'
          : '未记价';
  const perDeliveryCost =
    micros !== null && currencies.size === 1 && totals.landed_with_rev > 0
      ? `${unpricedCalls > 0 ? '≥ ' : ''}${money(
          Math.round(micros / totals.landed_with_rev),
          [...currencies][0]
        )}`
      : null;

  return (
    <section className="border-b border-[var(--line)]">
      <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">尝试 → 落地</h2>
      {funnel.error ? (
        <ReadError what="变更漏斗" error={funnel.error} onRetry={funnel.refresh} />
      ) : !funnel.data && funnel.loading ? (
        <Loading what="变更漏斗" />
      ) : days.length === 0 ? (
        <Empty>近 14 天没有尝试记录，也没有落地记录。</Empty>
      ) : (
        <>
          <Readings
            from={funnel}
            source="GET /api/v1/governance/changes/funnel?days=14，近 14 天求和"
            caveat="尝试数的是智能体自己的轮次事件，交付数的是账上带着交付版本的那一头——「结为成立」里绝大多数是本来就成立、什么都没交付的条，两者相加会把每天五六笔的交付读成上百笔。合计是把每天的尝试数加起来，跨天重复尝试的变更会被数两次。调用数与成本取自网关自己的用量表，含所有调用方（反思、评估、发现都算），所以这两个比值是整个部署的代价，不只是这条账上尝试的代价；调用数印「未读到」时是没读到用量表，不是零调用。成本按价目表自己的币种印（符号来自那本价目），页上不替它选汇率；数字前面带「≥」的天是那天还有调用没有记价——那是下界，不是花销总额，读成省钱会正好读反。"
          >
            <div className="flex flex-wrap py-2">
              <Reading label="尝试变更" value={totals.attempted} />
              <Reading label="轮次" value={totals.rounds} />
              <Reading
                label="判定通过"
                value={totals.passed}
                tone={totals.attempted > 0 && totals.passed === 0 ? 'danger' : 'plain'}
              />
              <Reading label="通过率" value={rate(totals.passed, totals.rounds)} />
              <Reading label="账上结为成立" value={totals.landed} />
              <Reading
                label="交付了 rev"
                value={totals.landed_with_rev}
                tone={totals.landed_with_rev > 0 ? 'ok' : 'danger'}
              />
              <Reading label="尝试→交付" value={rate(totals.landed_with_rev, totals.attempted)} />
              <Reading
                label="模型调用"
                value={calls === null ? '未读到' : calls}
                tone={calls === null ? 'danger' : 'plain'}
              />
              <Reading
                label="每笔交付调用数"
                value={
                  calls === null ? '未读到' : perDelivery === null ? '无交付' : perDelivery
                }
                tone={calls === null || perDelivery === null ? 'danger' : 'plain'}
              />
              <Reading
                label="每笔交付成本"
                value={
                  micros === null
                    ? costGap
                    : perDeliveryCost === null
                      ? '无交付'
                      : perDeliveryCost
                }
                tone={micros === null || perDeliveryCost === null ? 'danger' : 'plain'}
              />
            </div>
          </Readings>
          <Table
            columns={[
              { key: 'day', title: '日（UTC）', width: '100px', render: (d: ChangeFunnelDay) => (
                <span className="mono text-[11px]">{d.day}</span>
              ) },
              { key: 'attempted', title: '尝试', width: '70px', render: (d: ChangeFunnelDay) => d.attempted },
              { key: 'rounds', title: '轮', width: '70px', render: (d: ChangeFunnelDay) => d.rounds },
              { key: 'passed', title: '通过', width: '70px', render: (d: ChangeFunnelDay) => (
                <span className={d.rounds > 0 && d.passed === 0 ? 'text-[var(--danger)]' : undefined}>
                  {d.passed}
                </span>
              ) },
              { key: 'pass_rate', title: '通过率', width: '80px', render: (d: ChangeFunnelDay) => (
                <span className="mono text-[11px]">{rate(d.passed, d.rounds)}</span>
              ) },
              { key: 'landed', title: '结为成立', width: '80px', render: (d: ChangeFunnelDay) => d.landed },
              { key: 'landed_with_rev', title: '交付 rev', width: '80px', render: (d: ChangeFunnelDay) => (
                <span className={d.landed_with_rev > 0 ? undefined : 'text-[var(--muted)]'}>
                  {d.landed_with_rev}
                </span>
              ) },
              { key: 'land_rate', title: '尝试→交付', render: (d: ChangeFunnelDay) => (
                <span className="mono text-[11px]">{rate(d.landed_with_rev, d.attempted)}</span>
              ) },
              { key: 'llm_calls', title: '调用', width: '80px', render: (d: ChangeFunnelDay) => (
                <span className={d.llm_calls === null ? 'text-[var(--danger)]' : 'mono text-[11px]'}>
                  {d.llm_calls === null ? '未读到' : d.llm_calls}
                </span>
              ) },
              { key: 'calls_per_delivery', title: '调用/交付', width: '90px', render: (d: ChangeFunnelDay) => (
                <span className="mono text-[11px]">
                  {d.llm_calls === null
                    ? '—'
                    : d.landed_with_rev > 0
                      ? Math.round(d.llm_calls / d.landed_with_rev)
                      : '无交付'}
                </span>
              ) },
              { key: 'cost', title: '成本', width: '110px', render: (d: ChangeFunnelDay) => (
                <Note
                  className={
                    d.cost_micros === null ? 'text-[var(--muted)]' : 'mono text-[11px]'
                  }
                  text={
                    (d.unpriced_calls ?? 0) > 0
                      ? `这一天还有 ${d.unpriced_calls} 次调用没有记价，印出来的只是下界`
                      : ''
                  }
                >
                  {d.cost_micros === null || d.cost_currency === null
                    ? '未记价'
                    : `${(d.unpriced_calls ?? 0) > 0 ? '≥ ' : ''}${money(
                        d.cost_micros,
                        d.cost_currency
                      )}`}
                </Note>
              ) },
            ]}
            rows={[...days].reverse()}
            rowKey={(d) => d.day}
          />
        </>
      )}
    </section>
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
  // 三份账各有各的写者：账本身在 governance 前缀下，队列与计数在 evolution 前缀下，
  // 而一条变更的进度是任务换态推出来的。三件都订上，且一次触发三份一起重读——
  // 只刷其中一份，同一屏上就会出现「账说 N 条、计数说 M 条」。
  useKindTickedRefresh(
    ['tasks', 'projects', resourceKey('governance'), resourceKey('evolution')],
    [ledger.refresh, queue.refresh, metrics.refresh]
  );
  const [picked, setPicked] = useState<ChangeLedgerRow | null>(null);

  const page = ledger.data;
  const changes = queue.data?.changes ?? [];

  return (
    <ViewFrame
      title="变更"
      meta={page ? `台账 ${page.total} 条 · 欠账 ${page.outstanding_total} · 待声明 ${page.pending_declarations}` : '按 20 秒重读'}
    >
      {page && page.census.length > 0 && (
        <div className="border-b border-[var(--line)]">
          <Strip
            label="台账里这些变更各处在哪个状态"
            note="按这次读回来的 census 数（GET /api/v1/governance/changes）；黄的那几段是还要人动手的欠账"
            segments={page.census.map((cell) => ({
              label: cell.state,
              value: cell.rows,
              tone: cell.outstanding && cell.rows > 0 ? ('warn' as const) : ('magnitude' as const),
            }))}
          />
        </div>
      )}
      <FunnelPanel />
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
                <Note
                  as="button"
                  onClick={() => setPicked(picked?.identity === r.identity ? null : r)}
                  className="mono truncate text-left text-[11px] text-[var(--paper-dim)] underline-offset-2 hover:underline"
                  text={r.identity}
                >
                  {r.identity}
                </Note>
              ) },
              { key: 'state', title: '状态', width: '120px', render: (r) => <Status value={r.state} /> },
              { key: 'title', title: '是什么', render: (r) => (
                <span className="text-[12px]">{r.title ?? r.assertion ?? '—'}</span>
              ) },
              { key: 'delivered', title: '交付 rev', width: '90px', render: (r) =>
                r.delivered_rev ? (
                  <Note className="mono truncate text-[11px] text-[var(--paper-dim)]" text={r.delivered_rev}>
                    {r.delivered_rev.slice(0, 8)}
                  </Note>
                ) : (
                  <span className="text-[11px] text-[var(--muted)]">—</span>
                ) },
              // 这一条自己的花销，按它的全部交接代际求和。以前这一面只有
              // 「全队一天的花费 ÷ 全队一天的落地数」，那是平均：差一百倍的两条
              // 变更在它上面是同一个数，答不了「这条花了多少」。
              { key: 'cost', title: '花了', width: '110px', render: (r) =>
                r.cost === null ? (
                  <Note
                    className="text-[11px] text-[var(--muted)]"
                    text={page.cost_unreadable ?? '这一页没有账可读'}
                  >
                    量不了
                  </Note>
                ) : (
                  <span className="mono text-[11px] text-[var(--paper-dim)]">
                    {r.cost.toLocaleString()}
                  </span>
                ) },
              { key: 'why', title: '还差什么', width: '150px', render: (r) =>
                r.holds_the_line ? (
                  <Note
                    className="text-[11px] text-[var(--warn)]"
                    text="它已经被交付上线，而判据在树上还不成立——它扣着主线"
                  >
                    扣着主线
                  </Note>
                ) : r.unsettled_reason ? (
                  <Note className="mono truncate text-[11px] text-[var(--warn)]" text={r.unsettled_reason}>
                    {r.unsettled_reason}
                  </Note>
                ) : (
                  <span className="text-[11px] text-[var(--muted)]">—</span>
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
          <Readings from={metrics} source="GET /api/v1/evolution/metrics 的累计一栏" caveat="这些是演化进程自己的累计计数，进程重启后从头算。">
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
          </Readings>
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

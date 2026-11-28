import { useCallback, useState } from 'react';
import {
  applyEvolutionChange,
  approveEvolutionChange,
  getChangeFunnel,
  getChangeTimeline,
  getEvolutionChanges,
  getEvolutionMetrics,
  getGovernanceChanges,
  getPromotionSwitch,
  getPromotionTrend,
  listPromotions,
  rollbackEvolution,
  setPromotionPaused,
  type ChangeFunnelDay,
  type ChangeCells,
  type ChangeLedgerRow,
  type ChangeTimelineEvent,
  type PromotionRecord,
  type PromotionTrendWeek,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { useRecordFocus } from '@/shell/useRecordFocus';
import { useShell } from '@/shell/store';
import { resourceKey } from '@/store/frameTick';
import { money } from '@/topology/format';
import { Note } from '@/ui/Note';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Status, Table, ViewFrame } from '@/ui/primitives';
import { Strip } from '@/ui/plots';


/**
 * 一条变更的三格：代码 / 上线 / 验证。
 *
 * 三格回答三个不同的问题，「已落」只回答了其中一个却常被读成三个答案。空格不
 * 等于没进展——它是「这一步还没有证据」，这一句话比一个状态词长，但它是对的。
 * 代码填了、验证空着的那一行是红的：它读作做完了，实际没人核过。
 */
function Cells({ cells }: { cells: ChangeCells | undefined }) {
  if (!cells) {
    // 平台这一页算不出三格（读不到现网 revision）：说出来，不印三个空格——
    // 三个空格读作「全都没做」，那是另一个结论。
    return <Note className="text-[11px] text-[var(--muted)]" text="这一页量不到三格">量不到</Note>;
  }
  const pieces: { label: string; filled: boolean; tone: 'plain' | 'warn' | 'good' }[] = [
    { label: '码', filled: cells.code, tone: 'plain' },
    { label: '线', filled: cells.live, tone: 'plain' },
    {
      label: '验',
      filled: cells.verified,
      tone: cells.code && !cells.verified ? 'warn' : 'plain',
    },
  ];
  return (
    <Note
      className="mono flex gap-1 text-[11px]"
      text="代码 / 上线 / 验证：树里有实现、跑着的那一版有它、判据对着真东西量过"
    >
      {pieces.map((piece) => (
        <span
          key={piece.label}
          className={
            piece.filled
              ? 'text-[var(--paper-dim)]'
              : piece.tone === 'warn'
                ? 'text-[var(--warn)]'
                : 'text-[var(--muted)]'
          }
        >
          {piece.filled ? piece.label : '□'}
        </span>
      ))}
      {cells.code && !cells.verified && <span className="text-[var(--warn)]">!</span>}
    </Note>
  );
}

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
  // 合计是服务端算的（`FunnelSummary::of`），页面只负责印。这里刻意不留一份「自己再加
  // 一遍」的备份算法：两份算法只要有一天不一致，页面上的数就与 API 上的数对不上，而没有
  // 人会去比。口径——分母是「全部关掉的条」还是「带 rev 关掉的条」——与它一起在服务端测。
  const summary = funnel.data?.summary;
  // 钱说不出来的三种原因由服务端给（`cost_unstated`），页面只把它们翻成中文。这里不
  // 再从 days 里推一遍：同一条判据两处实现，迟早一处改了一处没改，而页面上的字没人复核。
  const costGapText = {
    usage_unread: '未读到',
    mixed_currency: '多币种',
    nothing_priced: '未记价',
  } as const;
  const costGap =
    summary?.cost_micros === null && summary.cost_unstated !== null
      ? costGapText[summary.cost_unstated]
      : null;
  // 有调用没记价时印「≥」：那是下界，不是花销总额，读成省钱会正好读反。
  const moneyReading = (micros: number | null | undefined): string | null =>
    micros == null || summary?.cost_currency == null
      ? null
      : `${(summary.unpriced_calls ?? 0) > 0 ? '≥ ' : ''}${money(micros, summary.cost_currency)}`;
  const perClosureCost = moneyReading(summary?.cost_per_landing_micros);
  const perShippedCost = moneyReading(summary?.cost_per_shipped_change_micros);
  const perShippedCalls =
    summary?.calls_per_shipped_change == null
      ? null
      : `${Math.round(summary.calls_per_shipped_change)}`;

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
            caveat="尝试数的是智能体自己的轮次事件，交付数的是账上带着交付版本的那一头——「结为成立」里绝大多数是本来就成立、什么都没交付的条，两者相加会把每天五六笔的交付读成上百笔。合计是把每天的尝试数加起来，跨天重复尝试的变更会被数两次。调用数与成本取自网关自己的用量表，含所有调用方（反思、评估、发现都算），所以这些比值是整个部署的代价，不只是这条账上尝试的代价；调用数印「未读到」时是没读到用量表，不是零调用。成本按价目表自己的币种印（符号来自那本价目），页上不替它选汇率；数字前面带「≥」的是还有调用没有记价——那是下界，不是花销总额，读成省钱会正好读反。**每笔成本有两个分母，两个都对，答的是两个问题**：「每笔落地成本」除以全部关掉的条，「每笔送出变更成本」只除以带 rev 的那几条——后者才是账上 ¥ 目标所说的那个分母。两者的比就是「带 rev 覆盖率」：覆盖率不到 100% 时，前者看着低，是因为分母里混着大量本来就已经成立、什么都没送出去的条；而后者在覆盖率为 0 时不存在（印「无 rev 交付」），别把它读成一个便宜的数。"
          >
            <div className="flex flex-wrap py-2">
              <Reading label="尝试变更" value={summary?.attempted ?? '—'} />
              <Reading label="轮次" value={summary?.rounds ?? '—'} />
              <Reading
                label="判定通过"
                value={summary?.passed ?? '—'}
                tone={
                  summary !== undefined && summary.attempted > 0 && summary.passed === 0
                    ? 'danger'
                    : 'plain'
                }
              />
              <Reading
                label="通过率"
                value={summary === undefined ? '—' : rate(summary.passed, summary.rounds)}
              />
              <Reading label="账上结为成立" value={summary?.landed ?? '—'} />
              <Reading
                label="交付了 rev"
                value={summary?.landed_with_rev ?? '—'}
                tone={summary === undefined || summary.landed_with_rev === 0 ? 'danger' : 'ok'}
              />
              <Reading
                label="带 rev 覆盖率"
                value={
                  // `null` 只在「一条都没关」时出现：分母为零，比值不存在——印 0% 会把
                  // 「窗口里什么都没发生」读成「发生了但一条都没送出」。
                  summary?.rev_coverage == null
                    ? '—'
                    : `${(summary.rev_coverage * 100).toFixed(1)}%`
                }
                tone={summary !== undefined && summary.rev_coverage === 0 ? 'danger' : 'plain'}
              />
              <Reading
                label="尝试→交付"
                value={
                  summary === undefined ? '—' : rate(summary.landed_with_rev, summary.attempted)
                }
              />
              <Reading
                label="模型调用"
                value={summary == null || summary.llm_calls === null ? '未读到' : summary.llm_calls}
                tone={summary == null || summary.llm_calls === null ? 'danger' : 'plain'}
              />
              <Reading
                label="每笔落地成本"
                // 还没拿到合计时印「—」，不印「无落地」：后者是一个结论（窗口里一条都没关），
                // 而这时页面只是还不知道。
                value={summary === undefined ? '—' : (costGap ?? perClosureCost ?? '无落地')}
                tone={
                  summary === undefined
                    ? 'plain'
                    : costGap !== null || perClosureCost === null
                      ? 'danger'
                      : 'plain'
                }
              />
              <Reading
                label="每笔送出变更成本"
                value={summary === undefined ? '—' : (costGap ?? perShippedCost ?? '无 rev 交付')}
                tone={
                  summary === undefined
                    ? 'plain'
                    : costGap !== null || perShippedCost === null
                      ? 'danger'
                      : 'plain'
                }
              />
              <Reading
                label="每笔送出变更调用数"
                value={
                  summary == null || summary.llm_calls === null
                    ? '未读到'
                    : perShippedCalls ?? '无 rev 交付'
                }
                tone={
                  summary == null || summary.llm_calls === null || perShippedCalls === null
                    ? 'danger'
                    : 'plain'
                }
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

/** 分级判定的类型化结果翻成一句人话；表里那一列是整个晋级门的理由。 */
const GATE_LABEL: Record<string, string> = {
  reject_unmeasured: '拒收：影响面不可确认',
  reject_protected: '拒收：触及受保护文件',
  auto_config: 'L0 配置热更新',
  auto_rollout: 'L1 自动金丝雀',
  approval_diff_over_limit: '转人工：diff 超上限',
  approval_core_path: '转人工：触及核心路径',
  approval_unclassified: '转人工：路径未分类（旧判定）',
  reject_unclassified: '拒收：路径族没有准入条件',
};

const PROMO_STATUS: Record<string, string> = {
  pending: '执行中',
  promoted: '已晋级',
  rolled_back: '已回滚',
  failed: '失败',
  awaiting_approval: '等人工审批',
};

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
  // 晋级那一面读的是 evolution_admin 的台账与开关；没配 evolution_admin 时这三条
  // 答 503，各自落在「读不到」上，不会伪装成空台账。
  const promoSwitch = useResource('晋级开关', getPromotionSwitch, 30_000);
  const promotions = useResource('晋级台账', () => listPromotions(100), 60_000);
  const trend = useResource('晋级周报', getPromotionTrend, 120_000);
  // 这几份账各有各的写者：账本身在 governance 前缀下，队列、计数与晋级在 evolution
  // 前缀下，而一条变更的进度是任务换态推出来的。都订上，且一次触发一起重读——
  // 只刷其中一份，同一屏上就会出现「账说 N 条、计数说 M 条」。
  useKindTickedRefresh(
    ['tasks', 'projects', resourceKey('governance'), resourceKey('evolution')],
    [ledger.refresh, queue.refresh, metrics.refresh, promoSwitch.refresh, promotions.refresh, trend.refresh]
  );
  const [picked, setPicked] = useState<ChangeLedgerRow | null>(null);
  const appendOutput = useShell((s) => s.appendOutput);
  const [acting, setActing] = useState<string | null>(null);

  const page = ledger.data;
  const changes = queue.data?.changes ?? [];
  const promo = promoSwitch.data;

  /**
   * 对一条候选变更动手：应用（落工作树并跑测试）或放行（提交、构建、切换）。
   *
   * 两个动作都不是「改一列状态」：一个会在树上改文件并跑一遍测试，另一个会把
   * 一条变更送上构建并切换运行的二进制。所以按钮只在对应的状态上出现、并且先
   * 要人确认；结局照响应原文念出来，不替它总结成「成功」。
   */
  const act = async (
    label: string,
    id: string,
    action: (id: string) => Promise<unknown>,
    confirmText: string
  ) => {
    if (!window.confirm(confirmText)) return;
    setActing(id);
    try {
      const result = await action(id);
      const text =
        result && typeof result === 'object'
          ? Object.entries(result as Record<string, unknown>)
              .map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`)
              .join(' ')
          : String(result);
      appendOutput(`${label} ${id}：${text || '已返回'}（${queue.what} 会重读）`);
      queue.refresh();
      ledger.refresh();
      metrics.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`${label} ${id} 失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setActing(null);
    }
  };

  const rollback = async () => {
    if (!window.confirm('回滚到上一个部署的二进制并重启？正在跑的活会被打断，且这一下不经过评审。')) return;
    try {
      const result = await rollbackEvolution();
      appendOutput(`回滚：${JSON.stringify(result)}`);
      metrics.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`回滚失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    }
  };

  /**
   * 一键暂停 / 恢复自动晋级。
   *
   * 暂停不撤回任何已生效的变更，它只让「排队中的」全部转人工；恢复也不补跑。
   * 所以两种结局都念出来：改成了什么、配置总开关是什么、运行时标志又是什么——
   * `effective_enabled` 是这两者的相与，单看一个会读错它为什么是 false。
   */
  const flipPromotion = async (pause: boolean) => {
    const what = pause ? '暂停自动晋级' : '恢复自动晋级';
    if (
      !window.confirm(
        `${what}？${
          pause
            ? '排队中的晋级会全部转人工审批，已生效的变更不受影响。'
            : '自动晋级会按分级判定重新推进；这不会补跑暂停期间积压的变更。'
        }`
      )
    )
      return;
    try {
      const info = await setPromotionPaused(pause, pause ? '接管台手动暂停' : '接管台手动恢复');
      appendOutput(
        `${what}：实际生效=${info.effective_enabled}（配置总开关=${info.config_enabled}，暂停标志=${info.paused}）`
      );
      promoSwitch.refresh();
      trend.refresh();
      promotions.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`${what}失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    }
  };

  // 对话里点了一条变更：打开这一行的来路。身份是三段，这里只按整串匹配——
  // 拆开再拼会拼出另一条身份，而错的那一条在账上通常也「存在」，只是不是它。
  const pickChange = useCallback(
    (identity: string) => {
      const row = page?.outstanding.find((item) => item.identity === identity);
      if (!row) return false;
      setPicked(row);
      return true;
    },
    [page]
  );
  useRecordFocus('change', pickChange);

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
      {page?.cells_census && (
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="本页·代码" value={page.cells_census.code} tone="plain" />
          <Reading label="本页·上线" value={page.cells_census.live} tone="plain" />
          <Reading label="本页·验证" value={page.cells_census.verified} tone="plain" />
          <Reading label="本页·三格齐" value={page.cells_census.terminal} tone="plain" />
          <Reading
            label="本页·落地没量过"
            value={page.cells_census.unverified}
            tone={page.cells_census.unverified > 0 ? 'warn' : 'plain'}
          />
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
              { key: 'cells', title: '三格', width: '80px', render: (r) => <Cells cells={r.cells} /> },
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
        <div className="flex items-center justify-between px-4 py-2">
          <h2 className="text-[11px] uppercase tracking-wider text-[var(--muted)]">演化累计</h2>
          <Button onClick={() => void rollback()}>回滚到上一个部署</Button>
        </div>
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

      {/* ---- 晋级（promotion）---- */}
      <section className="border-b border-[var(--line)]">
        <div className="flex items-center justify-between px-4 py-2">
          <h2 className="text-[11px] uppercase tracking-wider text-[var(--muted)]">自动晋级</h2>
          {promo && (
            <Button
              tone={promo.paused ? 'accent' : 'plain'}
              onClick={() => void flipPromotion(!promo.paused)}
            >
              {promo.paused ? '恢复自动晋级' : '暂停自动晋级'}
            </Button>
          )}
        </div>
        {promoSwitch.error ? (
          <ReadError what="晋级开关" error={promoSwitch.error} onRetry={promoSwitch.refresh} />
        ) : promoSwitch.data ? (
          <Readings
            from={promoSwitch}
            source="GET /api/v1/evolution/promotion/switch"
            caveat="实际生效 = 配置总开关 ∧ 运行时暂停标志；暂停只影响排队中的变更，已生效的不回滚。"
          >
            <div className="flex flex-wrap py-2">
              <Reading
                label="实际生效"
                value={promoSwitch.data.effective_enabled ? '开着' : '停着'}
                tone={promoSwitch.data.effective_enabled ? 'ok' : 'warn'}
              />
              <Reading
                label="配置总开关"
                value={promoSwitch.data.config_enabled ? '开' : '关'}
                note="来自配置文件，重启后按它算"
              />
              <Reading
                label="运行时暂停"
                value={promoSwitch.data.paused ? '已暂停' : '未暂停'}
                tone={promoSwitch.data.paused ? 'warn' : 'plain'}
                note="这一下改的是它；重启后回落配置文件值"
              />
              <Reading
                label="上次切换"
                value={
                  promoSwitch.data.updated_at
                    ? new Date(promoSwitch.data.updated_at).toLocaleString('zh-CN')
                    : '从未切换'
                }
                note={promoSwitch.data.note || '没有留备注'}
              />
            </div>
          </Readings>
        ) : (
          <Loading what="晋级开关" />
        )}

        {trend.data && (trend.data.alert || trend.data.stall_alert) && (
          <div className="px-4 pb-2">
            {trend.data.alert && (
              <div className="text-[11px]" style={{ color: 'var(--warn)' }}>
                趋势告警：{trend.data.alert}
              </div>
            )}
            {trend.data.stall_alert && (
              <div className="text-[11px]" style={{ color: 'var(--danger)' }}>
                停摆告警：{trend.data.stall_alert}
              </div>
            )}
            <div className="mt-0.5 text-[10px] text-[var(--muted)]">
              两条判据的证据不同：趋势告警说「有对决胜出、但成功率在降」，停摆告警说
              「连续几个已完成周零晋级」；零晋级时前者没有样本可判，不能拿它代替后者。
            </div>
          </div>
        )}
        {trend.error ? (
          <ReadError what="晋级周报" error={trend.error} onRetry={trend.refresh} />
        ) : trend.data ? (
          trend.data.weeks.length === 0 ? (
            <Empty>
              还没有任何一周的晋级记录。周报是聚合台账生成的；周次为空说明台账里还没有落到
              「已切分到周」的行，不是这一周没问题。
            </Empty>
          ) : (
            <Table
              columns={[
                { key: 'week', title: '周', width: '100px', render: (w: PromotionTrendWeek) => <span className="mono text-[11px]">{w.week}</span> },
                { key: 'promoted', title: '晋级', width: '70px', render: (w: PromotionTrendWeek) => w.promoted },
                { key: 'rolled', title: '回滚', width: '70px', render: (w: PromotionTrendWeek) => w.rolled_back },
                { key: 'failed', title: '失败', width: '70px', render: (w: PromotionTrendWeek) => w.failed },
                { key: 'pending', title: '执行中', width: '80px', render: (w: PromotionTrendWeek) => w.pending },
                {
                  key: 'review',
                  title: '等审批',
                  width: '110px',
                  render: (w: PromotionTrendWeek) => (
                    <Note
                      text={
                        w.awaiting_by_gate > 0
                          ? `其中 ${w.awaiting_by_gate} 条由分级阈值转入人工（diff 超限 ${w.awaiting_over_diff_limit} 条）；这一类多了该调阈值`
                          : '没有一条是由分级阈值拦下来的'
                      }
                    >
                      {w.awaiting_review}
                    </Note>
                  ),
                },
                {
                  key: 'rate',
                  title: '成功率',
                  width: '100px',
                  render: (w: PromotionTrendWeek) =>
                    w.success_rate === null ? (
                      <Note text="这一周没有完结对局（全在审批中），成功率没有分母">—</Note>
                    ) : (
                      <span className="mono text-[11px]">{(w.success_rate * 100).toFixed(0)}%</span>
                    ),
                },
              ]}
              rows={[...trend.data.weeks].reverse()}
              rowKey={(w) => w.week}
            />
          )
        ) : (
          <Loading what="晋级周报" />
        )}

        <h3 className="px-4 pb-1 pt-2 text-[11px] text-[var(--muted)]">晋级台账（最近 100 条）</h3>
        {promotions.error ? (
          <ReadError what="晋级台账" error={promotions.error} onRetry={promotions.refresh} />
        ) : promotions.data ? (
          promotions.data.promotions.length === 0 ? (
            <Empty>台账里还没有任何晋级记录。</Empty>
          ) : (
            <Table
              columns={[
                {
                  key: 'change',
                  title: '变更',
                  width: '150px',
                  render: (p: PromotionRecord) => (
                    <span className="mono text-[11px]">{p.change_id.slice(0, 12)}</span>
                  ),
                },
                { key: 'level', title: '级别', width: '100px', render: (p: PromotionRecord) => <span className="mono text-[11px]">{p.level}</span> },
                {
                  key: 'gate',
                  title: '怎么判的',
                  width: '180px',
                  render: (p: PromotionRecord) =>
                    p.gate_kind ? (
                      <Note text={p.decision_reason} className="text-[11px]">
                        {GATE_LABEL[p.gate_kind] ?? p.gate_kind}
                      </Note>
                    ) : (
                      <Note text={p.decision_reason} className="text-[11px] text-[var(--muted)]">
                        非分级路径
                      </Note>
                    ),
                },
                {
                  key: 'status',
                  title: '状态',
                  width: '110px',
                  render: (p: PromotionRecord) => (
                    <span
                      style={{
                        color:
                          p.status === 'promoted'
                            ? 'var(--ok)'
                            : p.status === 'failed' || p.status === 'rolled_back'
                              ? 'var(--danger)'
                              : p.status === 'awaiting_approval'
                                ? 'var(--warn)'
                                : 'var(--paper-dim)',
                      }}
                    >
                      {PROMO_STATUS[p.status] ?? p.status}
                    </span>
                  ),
                },
                {
                  key: 'cluster',
                  title: '来源',
                  width: '110px',
                  render: (p: PromotionRecord) => <span className="mono text-[11px]">{p.cluster || '—'}</span>,
                },
                {
                  key: 'outcome',
                  title: '结果说明',
                  render: (p: PromotionRecord) => (
                    <span className="text-[11px] text-[var(--paper-dim)]">{p.outcome || '—'}</span>
                  ),
                },
                { key: 'at', title: '更新于', width: '110px', render: (p: PromotionRecord) => <Moment iso={p.updated_at} /> },
              ]}
              rows={promotions.data.promotions}
              rowKey={(p) => p.id}
            />
          )
        ) : (
          <Loading what="晋级台账" />
        )}
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
              {
                key: 'act',
                title: '',
                width: '150px',
                render: (c) => {
                  // 状态是闭集里的词；按钮按它摆——对一条已经放行过的变更再放行
                  // 一次，服务端只会答 400，而读者点之前就该看见「没有这一步」。
                  const canApply = c.status === 'generated' || c.status === 'compile_checked';
                  const canApprove = c.status === 'awaiting_review';
                  if (!canApply && !canApprove) return null;
                  return (
                    <span className="flex gap-1">
                      {canApply && (
                        <Button
                          disabled={acting === c.id}
                          onClick={() =>
                            void act(
                              '应用变更',
                              c.id,
                              applyEvolutionChange,
                              `把 ${c.id} 应用到工作树并跑测试？这一步会改树上的文件。`
                            )
                          }
                        >
                          应用
                        </Button>
                      )}
                      {canApprove && (
                        <Button
                          tone="accent"
                          disabled={acting === c.id}
                          onClick={() =>
                            void act(
                              '放行变更',
                              c.id,
                              approveEvolutionChange,
                              `放行 ${c.id}？它会提交、构建并切换运行中的二进制。`
                            )
                          }
                        >
                          放行
                        </Button>
                      )}
                    </span>
                  );
                },
              },
            ]}
            rows={changes}
            rowKey={(c) => c.id}
          />
        )}
      </section>
    </ViewFrame>
  );
}

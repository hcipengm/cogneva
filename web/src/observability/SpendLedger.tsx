import { useMemo, useState } from 'react';
import { getTokenSpend, type SpendLine, type TokenSpend } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { compact } from '@/observability/usageRows';
import { Loading, ReadError, Readings } from '@/ui/primitives';

/**
 * 花了多少钱：直读逐笔账本，任意起止。
 *
 * 这一块与页面上其他花销读法回答的不是同一个问题。折叠账答的是「过去几个整点
 * 各是谁在花」，最新一个数按构造是一小时前的，而且它**没有钱这一列**——那里只有
 * token。要回答「现在花了多少、这笔钱是谁花的」，只能回到逐笔账本上按调用时刻
 * 求和；这也正是账本存在的理由。
 *
 * 三条读法上的规矩，与后端的判据一一对应：
 *
 * - 起止成对、与「最近 N 小时」互斥。只给一半时另一半只能靠猜，而猜出来的区间
 *   与真区间在响应里长得一模一样。
 * - 上限来自保留期（`COGNEVA_LLM_USAGE_KEEP_DAYS`，默认 30 天），不是一个常量。
 *   写死的上限会在保留期被改的那天开始撒谎：账本留着、接口读不到，两边各自都
 *   正常。
 * - 钱只覆盖「带着价的那几次调用」。`priced_calls` 小于 `calls` 时，总额是一个
 *   下界——把缺口读成零，等于让没定价的调用按免费结算。
 */
export function SpendLedger() {
  // 默认最近 24 小时：一个「今天花了多少」最常问的跨度。
  const [range, setRange] = useState(() => trailingDay());
  const [editing, setEditing] = useState(false);

  const label = `花费（${range.since} – ${range.until}）`;
  const spend = useResource(label, () => getTokenSpend(range), 60_000);

  return (
    <div className="flex flex-col gap-2 px-4 py-3" style={{ userSelect: 'text' }}>
      <div className="flex flex-wrap items-baseline gap-2 text-[11px]">
        <span className="text-[var(--paper-dim)]">读逐笔账本，按调用时刻切区间</span>
        <button
          type="button"
          className="rounded border border-[var(--line)] px-2 py-0.5 text-[var(--paper-dim)] hover:text-[var(--paper)]"
          onClick={() => setEditing((v) => !v)}
        >
          {editing ? '收起' : '改区间'}
        </button>
        <button
          type="button"
          className="rounded border border-[var(--line)] px-2 py-0.5 text-[var(--paper-dim)] hover:text-[var(--paper)]"
          onClick={() => setRange(trailingDay())}
        >
          最近 24 小时
        </button>
      </div>

      {editing && (
        <div className="flex flex-wrap items-baseline gap-2 text-[11px]">
          <label className="flex items-baseline gap-1">
            <span className="text-[var(--paper-dim)]">从</span>
            <input
              type="datetime-local"
              className="mono rounded border border-[var(--line)] bg-transparent px-1"
              value={range.sinceInput}
              onChange={(e) => setRange({ ...range, sinceInput: e.target.value })}
            />
          </label>
          <label className="flex items-baseline gap-1">
            <span className="text-[var(--paper-dim)]">到</span>
            <input
              type="datetime-local"
              className="mono rounded border border-[var(--line)] bg-transparent px-1"
              value={range.untilInput}
              onChange={(e) => setRange({ ...range, untilInput: e.target.value })}
            />
          </label>
          <button
            type="button"
            className="rounded border border-[var(--line)] px-2 py-0.5 text-[var(--paper-dim)] hover:text-[var(--paper)]"
            onClick={() => setRange(applyInputs(range))}
          >
            按这个区间读
          </button>
        </div>
      )}

      {spend.error ? (
        <ReadError what="花费账本" error={spend.error} onRetry={spend.refresh} />
      ) : !spend.data ? (
        <Loading what="花费账本" />
      ) : (
        <Readings
          from={spend}
          source={`GET /api/v1/observability/token-usage/spend?since=${encodeURIComponent(
            spend.data.requested_since,
          )}&until=${encodeURIComponent(spend.data.requested_until)}`}
          caveat="这一读直读逐笔账本（gateway_llm_usage），不是折叠表：折叠每小时落一行、没有钱这一列，读它得到的「现在花了多少」在整点前是错的、整点后是对的——而这种错最危险，因为没人会去复查一个一直对的数。金额是落账那一刻按当时那版价目折算并存下来的，不是前端乘出来的：跨时段、跨调价的价格在读取侧折会算错。"
        >
          <SpendTable spend={spend.data} />
        </Readings>
      )}
    </div>
  );
}

interface RangeState {
  since: string;
  until: string;
  sinceInput: string;
  untilInput: string;
}

function trailingDay(): RangeState {
  const until = new Date();
  const since = new Date(until.getTime() - 24 * 3600_000);
  return {
    since: since.toISOString(),
    until: until.toISOString(),
    sinceInput: localInput(since),
    untilInput: localInput(until),
  };
}

/** 按输入框里的两个时刻重新读。空的那一头保持原值，不猜。 */
function applyInputs(range: RangeState): RangeState {
  const since = range.sinceInput ? new Date(range.sinceInput) : new Date(range.since);
  const until = range.untilInput ? new Date(range.untilInput) : new Date(range.until);
  return {
    since: since.toISOString(),
    until: until.toISOString(),
    sinceInput: localInput(since),
    untilInput: localInput(until),
  };
}

/** `datetime-local` 要的是本地时间、不带时区尾巴的那种串。 */
function localInput(at: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${at.getFullYear()}-${pad(at.getMonth() + 1)}-${pad(at.getDate())}T${pad(
    at.getHours(),
  )}:${pad(at.getMinutes())}`;
}

function SpendTable({ spend }: { spend: TokenSpend }) {
  const rows = useMemo(
    () =>
      [...spend.rows].sort(
        (a, b) =>
          tokenTotal(b) - tokenTotal(a) || a.actor.localeCompare(b.actor),
      ),
    [spend.rows],
  );
  const covered =
    spend.covered_since && spend.covered_until
      ? `${stamp(spend.covered_since)} – ${stamp(spend.covered_until)}`
      : null;
  const swept =
    covered !== null &&
    new Date(spend.covered_since as string).getTime() -
      new Date(spend.requested_since).getTime() >
      60_000;

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-baseline gap-3 text-[11px]">
        <span className="text-[var(--paper-dim)]">
          问的区间 {stamp(spend.requested_since)} – {stamp(spend.requested_until)}
        </span>
        <span className="mono text-[var(--muted)]">
          {covered
            ? `账本里实际有 ${covered}`
            : '这段里账本一行都没有'}
        </span>
        <span className="mono text-[var(--muted)]">保留期 {spend.retention_days} 天</span>
      </div>

      {swept && (
        <p className="text-[11px] text-[var(--muted)]">
          请求的起点比账本里最早的一行还早：那一段已经被保留期清扫删掉了。这里显示的是
          剩下的部分——把它读成零，就是「那段时间没人用模型」，而真相是那段时间已经没法量。
        </p>
      )}

      <div className="flex flex-wrap items-baseline gap-4">
        {spend.totals.by_currency.map((total) => (
          <span key={total.currency} className="flex items-baseline gap-1">
            <span className="text-[11px] text-[var(--paper-dim)]">合计</span>
            <span className="mono text-[15px]">{money(total.cost_micros, total.currency)}</span>
            <span className="mono text-[11px] text-[var(--muted)]">
              {total.priced_calls.toLocaleString()} 次有价
            </span>
          </span>
        ))}
        {spend.totals.by_currency.length === 0 && (
          <span className="text-[11px] text-[var(--muted)]">
            这段区间里没有一次调用被算过价——不是没花钱，是账上没有价目能算。
          </span>
        )}
      </div>

      {spend.totals.unpriced_calls > 0 && (
        <p className="text-[11px] text-[var(--danger)]">
          有 {spend.totals.unpriced_calls.toLocaleString()} 次调用没有价：上面的金额只盖住了
          其余部分，总额是一个下界。没价的原因是上游条目没配价目，或者这些调用落在价目
          没声明的时段里——两种都不是「免费」。
        </p>
      )}

      {rows.length > 0 && (
        <table className="w-full text-[11px]">
          <thead>
            <tr className="text-left text-[var(--muted)]">
              <th className="py-1 pr-2 font-normal">执行者</th>
              <th className="py-1 pr-2 font-normal">上游 · 协议</th>
              <th className="py-1 pr-2 text-right font-normal">调用</th>
              <th className="py-1 pr-2 text-right font-normal">未命中输入</th>
              <th className="py-1 pr-2 text-right font-normal">命中输入</th>
              <th className="py-1 pr-2 text-right font-normal">输出</th>
              <th className="py-1 pr-2 text-right font-normal">命中率</th>
              <th className="py-1 pr-2 text-right font-normal">金额</th>
              <th className="py-1 text-right font-normal">无价调用</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const unpriced = row.calls - row.priced_calls;
              return (
                <tr key={key(row)} className="border-t border-[var(--line)]">
                  <td className="mono py-1 pr-2">{row.actor}</td>
                  <td className="mono py-1 pr-2 text-[var(--muted)]">
                    {shortUpstream(row.upstream)} · {row.api_style}
                  </td>
                  <td className="mono py-1 pr-2 text-right">{row.calls.toLocaleString()}</td>
                  <td className="mono py-1 pr-2 text-right">{compact(row.tokens_input)}</td>
                  <td className="mono py-1 pr-2 text-right">{compact(row.tokens_cached)}</td>
                  <td className="mono py-1 pr-2 text-right">{compact(row.tokens_output)}</td>
                  <td className="mono py-1 pr-2 text-right">{hitRate(row)}</td>
                  <td className="mono py-1 pr-2 text-right">
                    {row.cost_micros === null || row.currency === null
                      ? '没算过'
                      : money(row.cost_micros, row.currency)}
                  </td>
                  <td
                    className="mono py-1 text-right"
                    style={unpriced > 0 ? { color: 'var(--danger)' } : undefined}
                  >
                    {unpriced > 0 ? unpriced.toLocaleString() : '—'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function key(row: SpendLine): string {
  return `${row.actor}|${row.origin}|${row.upstream}|${row.api_style}|${row.price_book ?? ''}`;
}

function tokenTotal(row: SpendLine): number {
  return row.tokens_input + row.tokens_cached + row.tokens_output;
}

/**
 * 命中率的分母是整个 prompt（未命中 + 命中），不是只有输入：
 * 命中量也是流进模型的输入，只是计价比未命中的低。
 */
function hitRate(row: SpendLine): string {
  const prompt = row.tokens_input + row.tokens_cached;
  if (prompt <= 0) return '—';
  return `${((row.tokens_cached / prompt) * 100).toFixed(1)}%`;
}

/**
 * 百万分之一元说成人话。
 *
 * 小额保留四位：一次调用常常是几分钱甚至几厘，两位小数会把它们全印成 ¥0.00，
 * 而那一列正是「这条路径贵不贵」的读数。
 */
function money(micros: number, currency: string): string {
  const symbol = currency === 'CNY' ? '¥' : currency === 'USD' ? '$' : `${currency} `;
  const value = micros / 1_000_000;
  const text = Math.abs(value) >= 1 ? value.toFixed(2) : value.toFixed(4);
  return `${symbol}${text}`;
}

function stamp(iso: string): string {
  const at = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${pad(at.getMonth() + 1)}-${pad(at.getDate())} ${pad(at.getHours())}:${pad(
    at.getMinutes(),
  )}`;
}

function shortUpstream(upstream: string): string {
  try {
    return new URL(upstream).host;
  } catch {
    return upstream;
  }
}

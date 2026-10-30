import { useState } from 'react';
import { getTokenUsage, getTokenUsageSeries, type ActorUsage } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { SpendBars } from '@/observability/SpendBars';
import { TokenShare } from '@/observability/TokenShare';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { percentOf } from '@/topology/format';

/**
 * Token 面：模型花销这一本账的两个粒度，放在同一页上。
 *
 * 上半页读折叠账按窗口分行的那一面，答「什么时候烧的」；下半页读同一本账
 * 按执行者合并的那一面，答「谁烧的」。两问都要从折叠表来，不能合成一个读数：
 * 合计抹掉尖峰，逐窗口看不出总量。
 *
 * 这一面故意不给「预算用了多少」那根进度条：平台里逐用户的每日额度账没有
 * 写者（网关不再由任何中间件扣减它），画出来会是一个永远是 0 的「今日已用」。
 * 花销的真账只有右边这条——网关计量后落进 `gateway_llm_usage`、再由整点折叠
 * 落进 `cog_llm_usage_rollup`。
 */
export function TokenView() {
  const [hours, setHours] = useState(24);
  const series = useResource(`令牌花销（${hours} 小时）`, () => getTokenUsageSeries(hours), 60_000);
  const usage = useResource(`模型用量（${hours} 小时）`, () => getTokenUsage(hours), 60_000);
  // 累计这一读问的是「这账号一共花过多少」，`hours=0` 即「整本折叠账」。它每分钟
  // 重问一次没有意义——一个只在增长的总数，问得越勤只让同一个数字重复出现——所以
  // 它按 5 分钟重读，也不跟 `turn_end` 一起刷新。
  const cumulative = useResource('累计花销（整本折叠账）', () => getTokenUsage(0), 300_000);
  // 这本账的写者是一轮话、一次运行：`turn_end` 与 `agent_end` 就是那一笔落账的时刻。
  // 整点折叠改的是同一本账的另一个粒度，落在 observability 前缀下。
  useKindTickedRefresh(
    ['usage', resourceKey('observability')],
    [series.refresh, usage.refresh]
  );

  const rows = series.data?.rows ?? [];
  const actors = usage.data?.actors ?? [];
  const sum = (pick: (row: (typeof rows)[number]) => number) => rows.reduce((total, row) => total + pick(row), 0);
  const failed = sum((row) => row.failed_calls);
  // 命中率只在协议内算：`input` 在 OpenAI 兼容面已含缓存命中，在 Anthropic 面两者不相交，
  // 所以分母取 `input + cached`，而两种协议仍然分行列出——把两条线并成一个百分比，
  // 就是让一个协议的分母去解释另一个协议的分子。
  const byStyle = styleCells(rows);
  const cumulativeActors = cumulative.data?.actors ?? [];
  const cumulativeSum = (pick: (actor: ActorUsage) => number) =>
    cumulativeActors.reduce((total, actor) => total + pick(actor), 0);
  const cumulativeByStyle = styleCells(cumulativeActors);

  return (
    <ViewFrame
      title="Token"
      meta={`最近 ${hours} 小时 · 按 60 秒重读 · 累计 5 分钟`}
    >
      <div className="flex flex-wrap items-center gap-1 border-b border-[var(--line)] px-4 py-2">
        <span className="mr-2 text-[11px] text-[var(--muted)]">窗口</span>
        {WINDOWS.map((option) => {
          const active = option.hours === hours;
          return (
            <button
              key={option.hours}
              type="button"
              onClick={() => setHours(option.hours)}
              className="mono px-2 py-0.5 text-[11px] transition-colors"
              style={{
                color: active ? 'var(--ink-1000)' : 'var(--paper-dim)',
                background: active ? 'var(--brass)' : 'transparent',
                border: `1px solid ${active ? 'var(--brass)' : 'var(--line)'}`,
              }}
            >
              {option.label}
            </button>
          );
        })}
        <span className="ml-auto text-[11px] text-[var(--muted)]">
          服务端只答它折过的窗口，最长 7 天
        </span>
      </div>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">累计</h2>
        {cumulative.error ? (
          <ReadError what="累计花销" error={cumulative.error} onRetry={cumulative.refresh} />
        ) : !cumulative.data ? (
          <Loading what="累计花销" />
        ) : (
          <Readings
            from={cumulative}
            source="GET /api/v1/observability/token-usage?hours=0"
            caveat="`hours=0` 是「整本折叠账」，端点报出的覆盖起点就是这本账的第一格。聚合面每个执行者一行，所以问全史也不会把整本账拉过来；逐窗口那一面没有这个读法。"
          >
            <div className="flex flex-wrap py-2">
              <Reading label="输入 token" value={cumulativeSum((actor) => actor.tokens_input).toLocaleString()} />
              <Reading label="输出 token" value={cumulativeSum((actor) => actor.tokens_output).toLocaleString()} />
              <Reading label="缓存命中" value={cumulativeSum((actor) => actor.tokens_cached).toLocaleString()} />
              {[...cumulativeByStyle.entries()].map(([style, cell]) => {
                const base = cacheBase(style, cell.input, cell.cached);
                return (
                  <Reading
                    key={`total-hit-${style}`}
                    label={`命中率 · ${style}`}
                    value={base === null ? '—' : percentOf(cell.cached, base)}
                    note={
                      base === null
                        ? '这个协议没有已知的缓存记法'
                        : `缓存 ${compact(cell.cached)} / 分母 ${compact(base)}`
                    }
                  />
                );
              })}
              <Reading label="调用" value={cumulativeSum((actor) => actor.calls).toLocaleString()} />
              <Reading
                label="起始"
                value={
                  cumulative.data.covered_since ? <Moment iso={cumulative.data.covered_since} /> : '无折叠行'
                }
                note="这本账最早折过的那一格，不是平台上线时刻"
              />
              <Reading
                label="截至"
                value={
                  cumulative.data.covered_until ? <Moment iso={cumulative.data.covered_until} /> : '—'
                }
                note="折叠只折已收口的整点，最近那一小时不算在内"
              />
            </div>
          </Readings>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">花销</h2>
        {series.error ? (
          <ReadError what="令牌花销" error={series.error} onRetry={series.refresh} />
        ) : !series.data ? (
          <Loading what="令牌花销" />
        ) : (
          <>
            <Readings
              from={series}
              source={`GET /api/v1/observability/token-usage/series?hours=${hours}`}
              caveat="端点给的是折叠读实际覆盖到的窗口，不是请求的区间：最近那一小时通常还没折，所以它不在覆盖里。覆盖里的一格没有行，才是「那一格真的没花」。"
            >
              <div className="flex flex-wrap py-2">
                <Reading label="输入 token" value={sum((row) => row.tokens_input).toLocaleString()} />
                <Reading label="输出 token" value={sum((row) => row.tokens_output).toLocaleString()} />
                <Reading
                  label="缓存命中"
                  value={sum((row) => row.tokens_cached).toLocaleString()}
                  note="只在同一协议内可比"
                />
                {[...byStyle.entries()].map(([style, cell]) => {
                  const base = cacheBase(style, cell.input, cell.cached);
                  return (
                    <Reading
                      key={`hit-${style}`}
                      label={`命中率 · ${style}`}
                      value={base === null ? '—' : percentOf(cell.cached, base)}
                      note={
                        base === null
                          ? '这个协议没有已知的缓存记法'
                          : base > 0
                            ? `缓存 ${compact(cell.cached)} / 分母 ${compact(base)}`
                            : '这段没有输入 token'
                      }
                    />
                  );
                })}
                <Reading label="调用" value={sum((row) => row.calls).toLocaleString()} />
                <Reading
                  label="失败调用"
                  value={failed.toLocaleString()}
                  tone={failed > 0 ? 'warn' : 'plain'}
                  note="失败行不产 token，靠这个数分开"
                />
                <Reading
                  label="覆盖"
                  value={series.data.covered_until ? <Moment iso={series.data.covered_until} /> : '无折叠行'}
                  note={
                    series.data.covered_since
                      ? `自 ${new Date(series.data.covered_since).toLocaleString('zh-CN')}`
                      : '这段区间一格都没折过'
                  }
                />
                <Reading
                  label="窗口格数"
                  value={new Set(rows.map((row) => row.window_start)).size}
                  note={`${new Set(rows.map((row) => `${row.actor}|${row.api_style}`)).size} 个执行者 × 协议`}
                />
              </div>
            </Readings>
            <SpendBars
              rows={rows}
              since={series.data.covered_since}
              until={series.data.covered_until}
            />
          </>
        )}
      </section>

      <section>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">谁在花</h2>
        {usage.error ? (
          <ReadError what="模型用量" error={usage.error} onRetry={usage.refresh} />
        ) : !usage.data ? (
          <Loading what="模型用量" />
        ) : actors.length === 0 ? (
          <Empty>
            这段覆盖里没有花销行。看上面「覆盖」那一格：它答的是这段区间折过没有，
            这一格答的是折过的窗口里有没有人用模型。
          </Empty>
        ) : (
          <>
            <Readings
              from={usage}
              source={`GET /api/v1/observability/token-usage?hours=${hours}`}
              caveat="和上面的柱是同源两次读：两次之间正好折了一个窗口，就会只出现在一边。这里是把窗口加起来，所以它答不出尖峰。"
            >
              <div className="flex flex-wrap py-2">
                <Reading label="执行者" value={new Set(actors.map((actor) => actor.actor)).size} />
                <Reading
                  label="上游"
                  value={new Set(actors.map((actor) => actor.upstream)).size}
                  note={[...new Set(actors.map((actor) => actor.upstream))].join(' · ') || '—'}
                />
                <Reading
                  label="协议"
                  value={new Set(actors.map((actor) => actor.api_style)).size}
                  note={[...new Set(actors.map((actor) => actor.api_style))].join(' · ') || '—'}
                />
                <Reading
                  label="环境"
                  value={new Set(actors.map((actor) => actor.origin)).size}
                  note={[...new Set(actors.map((actor) => actor.origin))].join(' · ') || '—'}
                />
              </div>
            </Readings>
            <TokenShare actors={actors} />
            <Table
              columns={[
                { key: 'actor', title: '执行者', width: '190px', render: (a) => (
                  <span className="mono text-[11px]">{a.actor}</span>
                ) },
                { key: 'origin', title: '环境', width: '140px', render: (a) => (
                  <span className="text-[11px] text-[var(--muted)]">{a.origin}</span>
                ) },
                { key: 'upstream', title: '上游', width: '150px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{a.upstream}</span>
                ) },
                { key: 'style', title: '协议', width: '110px', render: (a) => (
                  <span className="text-[11px]">{a.api_style}</span>
                ) },
                { key: 'calls', title: '调用', width: '80px', render: (a) => (
                  <span className="mono text-[11px]">{a.calls}</span>
                ) },
                { key: 'failed', title: '失败', width: '70px', render: (a) => (
                  <span className="mono text-[11px]" style={{ color: a.failed_calls > 0 ? 'var(--danger)' : 'var(--muted)' }}>
                    {a.failed_calls}
                  </span>
                ) },
                { key: 'in', title: '输入', width: '100px', render: (a) => (
                  <span className="mono text-[11px]">{a.tokens_input.toLocaleString()}</span>
                ) },
                { key: 'out', title: '输出', width: '100px', render: (a) => (
                  <span className="mono text-[11px]">{a.tokens_output.toLocaleString()}</span>
                ) },
                { key: 'cached', title: '缓存', width: '100px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{a.tokens_cached.toLocaleString()}</span>
                ) },
                { key: 'share', title: '缓存占比', width: '100px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">{hitRate(a)}</span>
                ) },
                { key: 'per', title: '每调用输入', width: '110px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">
                    {a.calls > 0 ? Math.round(a.tokens_input / a.calls).toLocaleString() : '—'}
                  </span>
                ) },
                { key: 'latency', title: '均耗', width: '90px', render: (a) => (
                  <span className="mono text-[11px] text-[var(--muted)]">
                    {a.avg_latency_ms ? `${a.avg_latency_ms} ms` : '—'}
                  </span>
                ) },
              ]}
              rows={actors}
              rowKey={(a) => `${a.actor}|${a.origin}|${a.upstream}|${a.api_style}`}
            />
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              一行是「一个执行者 × 一个环境 × 一个上游 × 一种协议」，四格合起来才是唯一的一行：
              同一个执行者换过环境就是两行。缓存占比的分母逐协议不同——OpenAI 兼容线路里
              `input` 已含缓存命中，分母就是它；Anthropic 上两者不相交，分母是二者之和——
              所以这一列只在同一协议内纵向比；混着算出来的百分比，分子和分母往往不是同一段字节。
            </p>
          </>
        )}
      </section>
    </ViewFrame>
  );
}

const WINDOWS: { hours: number; label: string }[] = [
  { hours: 6, label: '6 小时' },
  { hours: 24, label: '24 小时' },
  { hours: 72, label: '3 天' },
  { hours: 168, label: '7 天' },
  // 「整本折叠账」不在这排按钮里：上面两读里有一读是逐窗口的，按钮选它就会让
  // 60 秒轮询把整本折过的账拉一次（五周就五万多行），而累计那个数只需要按执行者
  // 求和的那一面——它全史也只有几十行。累计在页顶单独一格、单独一条读。
];

/**
 * 缓存命中的分母，按协议取。
 *
 * 同一个词在两条线上指的不是同一段字节：OpenAI 兼容面的 `input` **已经含**缓存
 * 命中（账上逐行 `cached ≤ input` 成立），所以命中率是 `cached / input`；Anthropic
 * 面的 `input` 不含缓存，两者相加才是一次调用的全部输入，分母取 `input + cached`。
 * 拿一个分母套两种记法，必有一边是错的：对 OpenAI 兼容面会把命中份额压到约六成，
 * 对 Anthropic 面则可能算出超过 100%。
 *
 * 认不出的协议返回 `null`——那不是「命中率为 0」，是没有算法可用，得读成不知道。
 */
function cacheBase(style: string, input: number, cached: number): number | null {
  if (style.startsWith('openai')) return input;
  if (style === 'anthropic') return input + cached;
  return null;
}

/** 一行执行者的命中率；分母为 0 时没有比例可言，写 0% 会读成「一点没命中」。 */
function hitRate(actor: ActorUsage): string {
  const base = cacheBase(actor.api_style, actor.tokens_input, actor.tokens_cached);
  if (base === null || base <= 0) return '—';
  const value = (actor.tokens_cached / base) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
}

/**
 * 逐协议的 `input` / `cached` 小计。
 *
 * 两种协议共享同一个词，但 `input` 的含义不同：OpenAI 兼容面它已经含了缓存命中，
 * Anthropic 面两者不相交。小计必须按协议分开，命中率的分母才可能落对。
 */
function styleCells(
  rows: { api_style: string; tokens_input: number; tokens_cached: number }[]
): Map<string, { input: number; cached: number }> {
  const byStyle = new Map<string, { input: number; cached: number }>();
  for (const row of rows) {
    const cell = byStyle.get(row.api_style) ?? { input: 0, cached: 0 };
    cell.input += row.tokens_input;
    cell.cached += row.tokens_cached;
    byStyle.set(row.api_style, cell);
  }
  return byStyle;
}

/** 大数按万/亿缩写：读的人要的是量级，不是每一位都念一遍。 */
function compact(value: number): string {
  if (value >= 100_000_000) return `${(value / 100_000_000).toFixed(1)}亿`;
  if (value >= 10_000) return `${(value / 10_000).toFixed(1)}万`;
  return value.toLocaleString();
}

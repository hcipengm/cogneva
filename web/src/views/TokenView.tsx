import { useState } from 'react';
import { getTokenUsage, getTokenUsageSeries, type ActorUsage } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { SpendBars } from '@/observability/SpendBars';
import { TokenShare } from '@/observability/TokenShare';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

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

  return (
    <ViewFrame title="Token" meta={`最近 ${hours} 小时 · 按 60 秒重读`}>
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
              同一个执行者换过环境就是两行。缓存比的分母是输入，而输入的含义逐协议不同
              ——OpenAI 兼容线路里它已含缓存命中，Anthropic 上两者不相交——所以这一列只在
              同一协议内纵向比，跨协议相减会算出一个不存在的命中率。
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
];

/**
 * 缓存占全部输入的比例，即 `cached / (input + cached)`。
 *
 * 分母要把两段加起来，不能只用 input：两种协议面对同一笔缓存的记法不同——
 * Anthropic 面 `input` 不含缓存的那些 token，OpenAI 兼容面 `input` 已经含了，
 * 只拿 `input` 作分母时前者会算出超过 100%，后者则把命中份额压低。
 * 分母为 0 时没有比例可言——写 0% 会读成「一点没命中」。
 */
function hitRate(actor: ActorUsage): string {
  const input = actor.tokens_input + actor.tokens_cached;
  if (input <= 0) return '—';
  const value = (actor.tokens_cached / input) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
}

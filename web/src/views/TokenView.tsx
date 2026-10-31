import { useState } from 'react';
import { getTokenAccount, getTokenOutcomes, getTokenUsage, getTokenUsageSeries, type ActorUsage, type LlmPaceView } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useServices } from '@/shell/context';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { SpendBars } from '@/observability/SpendBars';
import { DimensionCounts } from '@/observability/DimensionCounts';
import { OutcomeBreakdown } from '@/observability/OutcomeBreakdown';
import { SpendAccount } from '@/observability/SpendAccount';
import { ActorTable } from '@/observability/ActorTable';
import {
  cacheBase,
  cacheWriteNote,
  cacheWriteValue,
  compact,
  styleCells,
} from '@/observability/usageRows';
import { Empty, Loading, Moment, ReadError, Reading, Readings, ViewFrame } from '@/ui/primitives';
import { localZoneLabel, percentOf } from '@/topology/format';

/**
 * Token 面：模型花销这一本账的几个粒度，放在同一页上。
 *
 * 页面分三层，每层的时刻不是同一个：**额度窗**是当刻读数（上游配额窗此刻允许多
 * 少、还剩多少），**累计**是整本折叠账（这账号一共花过多少），下面一整组**按窗口**
 * ——花销、零花销、谁在花。窗口按钮因此长在组头上，而不是页头：它变的是下面那三
 * 块，上面两节各有各的时刻，罩在同一个按钮下会让人以为它们也跟着变。
 *
 * 组里第一块读折叠账按窗口分行的那一面，答「什么时候烧的」；第三块读同一本账按
 * 执行者合并的那一面，答「谁烧的」。两问都要从折叠表来，不能合成一个读数：合计
 * 抹掉尖峰，逐窗口看不出总量。
 *
 * 中间那块答「为什么一分没花」，而它的三种成因来自三个不同的源：逐笔账本的结局
 * （试过、被挡回来）、省下账（决定不发）、判定账（该花却停了）。三者在每个花销
 * 读数上都是同一个零，只有这三块能把它们分开；三块的单位也不同（尝试 / 决定 /
 * 窗口），所以并成一节但不合并成一个数。
 *
 * 这一面故意不给「预算用了多少」那根进度条：平台里逐用户的每日额度账没有
 * 写者（网关不再由任何中间件扣减它），画出来会是一个永远是 0 的「今日已用」。
 * 花销的真账只有右边这条——网关计量后落进 `gateway_llm_usage`、再由整点折叠
 * 落进 `cog_llm_usage_rollup`。
 *
 * 「额度窗」那一节不是那根进度条，别把两者混为一谈。它记的是**上游配额窗**
 * 自己的账：上游不公布额度，网关能算的是「上一个窗烧穿之前它服务了多少」，
 * 把那份实测容量按窗长摊平就是此刻允许的量。花销面只答「花了多少」，答不出
 * 「这一窗允许多少、还剩多少、什么时候换窗」——而订阅制的上游没有单价可乘，
 * 这三格就是成本读数本身。没有量过整窗时它显式说「没有读数」，绝不是零额度。
 */
export function TokenView() {
  const [hours, setHours] = useState(24);
  const series = useResource(`令牌花销（${hours} 小时）`, () => getTokenUsageSeries(hours), 60_000);
  const usage = useResource(`模型用量（${hours} 小时）`, () => getTokenUsage(hours), 60_000);
  // 累计这一读问的是「这账号一共花过多少」，`hours=0` 即「整本折叠账」。它每分钟
  // 重问一次没有意义——一个只在增长的总数，问得越勤只让同一个数字重复出现——所以
  // 它按 5 分钟重读，也不跟 `turn_end` 一起刷新。
  const cumulative = useResource('累计花销（整本折叠账）', () => getTokenUsage(0), 300_000);
  // 这一读从逐笔账本取，答的是「每次尝试怎么结束的」。它和上面两读同窗不同源：
  // 折叠表只留「失败了几次」，说不出一分没花是因为池子空着还是熔断开着。
  const outcomes = useResource(`调用结局（${hours} 小时）`, () => getTokenOutcomes(hours), 60_000);
  // 第三种零花销：根本没发起过。省下账与判定账都是「决定不花」那一侧，
  // 一直在写、以前没有任何读法，于是「本来在花、无声停了」这句话没人读得到。
  const account = useResource(`决定不花（${hours} 小时）`, () => getTokenAccount(hours), 60_000);
  // 这本账的写者是一轮话、一次运行：`turn_end` 与 `agent_end` 就是那一笔落账的时刻。
  // 整点折叠改的是同一本账的另一个粒度，落在 observability 前缀下。
  useKindTickedRefresh(
    ['usage', resourceKey('observability')],
    [series.refresh, usage.refresh, outcomes.refresh, account.refresh]
  );

  const rows = series.data?.rows ?? [];
  const actors = usage.data?.actors ?? [];
  const sum = (pick: (row: (typeof rows)[number]) => number) => rows.reduce((total, row) => total + pick(row), 0);
  const failed = sum((row) => row.failed_calls);
  // 折叠账按行记「调用了几次、其中几次失败」，所以成功是两者之差而不是另一列。
  // 这一格必须单独露出来：一段窗口里 token 合计为 0 有两个完全不同的原因——
  // 没人用模型，和每一次尝试都被挡在门外——而失败数在同一排里，读者要自己减
  // 一次才知道是后者。
  const succeeded = sum((row) => row.calls) - failed;
  // 命中率只在协议内算，而且分母逐协议不同：`input` 在 OpenAI 兼容面已含缓存命中，
  // 分母就是它；Anthropic 面上两者不相交，分母是二者之和。两种协议分行列出——
  // 把两条线并成一个百分比，就是让一个协议的分母去解释另一个协议的分子。
  const byStyle = styleCells(rows);
  const cumulativeActors = cumulative.data?.actors ?? [];
  const cumulativeSum = (pick: (actor: ActorUsage) => number) =>
    cumulativeActors.reduce((total, actor) => total + pick(actor), 0);
  const cumulativeByStyle = styleCells(cumulativeActors);

  return (
    <ViewFrame
      title="Token"
      meta={`按 60 秒重读 · 累计 5 分钟 · 页面时刻为本地 ${localZoneLabel()}`}
    >
      <AllowanceWindow />

      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          累计
          <span className="ml-2 normal-case tracking-normal opacity-70">
            整本折叠账，不随下面的窗口变
          </span>
        </h2>
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
              <Reading
                label="缓存写入"
                value={cacheWriteValue(cumulativeActors)}
                note={cacheWriteNote(cumulativeActors)}
              />
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
              <Reading
                label="调用"
                value={cumulativeSum((actor) => actor.calls).toLocaleString()}
                note="尝试次数，含被熔断挡下的那些"
              />
              <Reading
                label="成功调用"
                value={cumulativeSum((actor) => actor.calls - actor.failed_calls).toLocaleString()}
                note="与「花销」那一段同一口径：成功的那几次才带回 token"
              />
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

      {/* 窗口是这一组三块的属性，不是整个页面的属性：上面两节各有各的时刻——额度窗是
          当刻读数、累计是全史——把它们也罩在按钮下面，读的人会以为按下 6 小时它们也变。
          组头写清「下面哪几块跟、上面哪几块不跟」，是这排按钮唯一的读法。 */}
      <section>
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1 border-b border-[var(--line)] bg-[var(--ink-800)] px-4 py-2">
          <span className="mr-1 text-[11px] uppercase tracking-wider text-[var(--muted)]">按窗口</span>
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
          <span className="ml-2 text-[11px] text-[var(--paper-dim)]">
            最近 {hours} 小时 · 下面三块（花销 / 零花销 / 谁在花）都跟着它变，上面两节不跟
          </span>
          <span className="ml-auto text-[11px] text-[var(--muted)]">
            服务端只答它折过的窗口，最长 7 天
          </span>
        </div>
        <div className="border-l-2 border-[var(--brass-dim)]">
          <div className="border-b border-[var(--line)]">
            <h3 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">花销</h3>
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
                      note="只在同一协议内可比：两个协议的 input 含义不同，读回量本身不跨协议相加"
                    />
                    <Reading
                      label="缓存写入"
                      value={cacheWriteValue(rows)}
                      note={cacheWriteNote(rows)}
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
                    <Reading
                      label="调用"
                      value={sum((row) => row.calls).toLocaleString()}
                      note="尝试次数，含被熔断挡下的那些"
                    />
                    <Reading
                      label="成功调用"
                      value={succeeded.toLocaleString()}
                      tone={succeeded === 0 && failed > 0 ? 'warn' : 'plain'}
                      note={
                        succeeded === 0 && failed > 0
                          ? '一次也没成：上面的 token 合计因此是 0，原因看下面「零花销」'
                          : '只有成功的那些才带回 token'
                      }
                    />
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
                      note={`${new Set(rows.map((row) => `${row.actor}|${row.api_style}`)).size} 个「执行者 · 协议」组合`}
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
          </div>

          {/* 三块说的是同一件事——一分没花——的三种成因，所以合成一节；但它们**不是
              同一种单位**：结局数是尝试次数、省下账数是决定次数、判定账数是窗口个数。
              把它们加成一个「零花销总数」，等于把三种东西加起来。标题里写清三种成因，
              是因为少了任何一块，剩下的两种就会读成同一件事。 */}
          <div className="border-b border-[var(--line)]">
            <h3 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              零花销
              <span className="ml-2 normal-case tracking-normal opacity-70">
                一分没花的三种成因：试过被挡回来 · 决定不发 · 该花却停了
              </span>
            </h3>
            <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
              三段读数各答一种，单位不同，不能相加：第一段数的是**尝试**（被熔断或配速挡回来的也算一次
              尝试，只是没发出去），后两本账一本按**决定**记、一本按**窗口**记。同一个零出现在三段上
              是同一件事的三个侧面，不是三份花销。
            </p>

            <h4 className="px-4 pb-1 pt-2 text-[11px] text-[var(--paper-dim)]">
              试过、被挡回来（逐笔账本的尝试）
            </h4>
            {outcomes.error ? (
              <ReadError what="调用结局" error={outcomes.error} onRetry={outcomes.refresh} />
            ) : !outcomes.data ? (
              <Loading what="调用结局" />
            ) : (
              <Readings
                from={outcomes}
                source={`GET /api/v1/observability/token-usage/outcomes?hours=${hours}`}
                caveat="这一读取逐笔账本，不是折叠表：折叠只留「失败了几次」，说不出被谁挡下来。区间是它实际读的那一段，不是请求的那一段。同一读还答一件事：成功的调用里有多少次上游真的报了用量——没报的那些 token 记成 0，和真的没花钱在账上是同一行，所以上面的花销合计是下界还是测量，看这一格。"
              >
                <OutcomeBreakdown data={outcomes.data} />
              </Readings>
            )}

            <h4 className="px-4 pb-1 pt-2 text-[11px] text-[var(--paper-dim)]">
              根本没发起（省下账按决定记，判定账按窗口记）
            </h4>
            {account.error ? (
              <ReadError what="决定不花" error={account.error} onRetry={account.refresh} />
            ) : !account.data ? (
              <Loading what="决定不花" />
            ) : (
              <Readings
                from={account}
                source={`GET /api/v1/observability/token-usage/account?hours=${hours}`}
                caveat="省下账与判定账是两本账，写的东西不一样：省下账一次决定一行（阶段 + 理由），判定账一个调用点一小时一行（被判成什么）。前者的计数单位是「决定」、后者是「窗口」，两栏各说各的，加起来没有意义。这两本账也是唯一说得出「根本没发起」与「本来在花、现在停了」的读数。"
              >
                <SpendAccount data={account.data} />
              </Readings>
            )}
          </div>

          <div>
            <h3 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">谁在花</h3>
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
                  caveat="和上面的柱是同源两次读：两次之间正好折了一个窗口，就会只出现在一边。这里是把窗口加起来，所以它答不出尖峰。四个计数都点得开，个数就是清单长度——它们来自同一批行，不会一个说十几个、一个只列得出十个。"
                >
                  <DimensionCounts actors={actors} />
                </Readings>
                <ActorTable actors={actors} />
              </>
            )}
          </div>
        </div>
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






/** 秒数说成人话：窗长常常是 30 天，写成 2592000 秒没人会去换算。 */
function spanOf(secs: number): string {
  if (secs <= 0) return '未报';
  if (secs % 86_400 === 0) return `${secs / 86_400} 天`;
  if (secs % 3_600 === 0) return `${secs / 3_600} 小时`;
  if (secs % 60 === 0) return `${secs / 60} 分钟`;
  return `${secs} 秒`;
}

/**
 * 上游配额窗的账：这一窗允许多少、花出去多少、什么时候换窗、池子此刻能不能供。
 *
 * 这一节读的是集群概况里的池快照，不是花销三读的那本折叠账：额度不是花销的
 * 另一种画法，它是花销的上界，而那个上界只存在于持有凭证的网关的内存里，按
 * TTL 发布到共享键值面。四种状态要分得开，界面上各有一句话：
 *
 * - 没有快照：写者没发布过，不能说池子健康；
 * - 有快照但没配速：还没有上游在一个窗里被拒过，没有容量可算，不是额度为 0；
 * - 有配速：允许量、已花、差额、一个整活的预留、窗长与窗尾；
 * - 有配速但各家窗不一样长：允许量照印（它是各家摊平量之和），窗起止三格改说
 *   「没有共同的窗」——取最早起点配最长窗长会印出一个谁都不在的窗尾；
 * - 池不可用：带上游自己说的复位时刻，以及我方下一次去探测的时刻。
 *
 * 允许量是**摊平**后的量，不是官方剩余额度：上游不公布额度，这个数是「上个窗
 * 烧穿前它服务了多少 token」按窗长线性摊到此刻。差额为负不是错误，它表示这一段
 * 花得比摊平位置快，池子会把新的整活压到下一窗再发。
 */
function AllowanceWindow() {
  const { overview } = useServices();
  const pool = overview.data?.llm_pool ?? null;
  const pace: LlmPaceView | null = pool?.pace ?? null;
  const remaining = pace ? pace.allowance_tokens - pace.spent_tokens : 0;
  // 各家窗不一样长时，快照不报窗（`window_secs` 与起点都为零）：允许量是各家
  // 摊平量之和，窗却不是任何东西的和——「最早起点 + 最长窗长」拼出来的那份窗，
  // 一条上游都不在里面。这种时候不印起止，把读者指到池那一行的复位时刻。
  const mixedWindows = pace !== null && pace.allowance_tokens > 0 && pace.window_secs === 0;
  const windowEnd =
    pace?.window_start_at && pace.window_secs > 0
      ? new Date(new Date(pace.window_start_at).getTime() + pace.window_secs * 1000).toISOString()
      : null;

  return (
    <section className="border-b border-[var(--line)]">
      <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
        额度窗
        <span className="ml-2 normal-case tracking-normal opacity-70">
          当刻读数，不随上面的窗口变
        </span>
      </h2>
      {overview.error ? (
        <ReadError what="上游池快照" error={overview.error} onRetry={overview.refresh} />
      ) : !overview.data ? (
        <Loading what="上游池快照" />
      ) : !pool ? (
        <p className="px-4 pb-3 text-[11px] text-[var(--warn)]">
          网关没有发布上游池快照——这是「没有读数」，不是「池子健康」：写者可能没起、
          没配共享键值面，或发布的那条键已过 TTL。
        </p>
      ) : (
        <Readings
          from={overview}
          source="GET /api/v1/observability/cluster-overview → llm_pool"
          caveat="额度窗读的是持有凭证的网关发布的那份快照，不是折叠账。允许量是实测容量按窗长摊平到此刻，不是上游公布的剩余额度——上游不公布。"
        >
          <div className="flex flex-wrap py-2">
            <Reading
              label="池"
              value={pool.available ? '可用' : '不可用'}
              tone={pool.available ? 'ok' : 'danger'}
              note={`${pool.pool_size - pool.unavailable_count} / ${pool.pool_size} 条此刻可供${
                pool.min_healthy !== null ? `，声明下限 ${pool.min_healthy}` : '，未声明下限'
              }`}
            />
            {pace ? (
              <>
                <Reading
                  label="这一窗允许"
                  value={pace.allowance_tokens.toLocaleString()}
                  note={`实测容量按 ${spanOf(pace.window_secs)} 摊平到此刻`}
                />
                <Reading
                  label="这一窗已花"
                  value={pace.spent_tokens.toLocaleString()}
                  tone={remaining < 0 ? 'warn' : 'plain'}
                  note="同一窗内，取自台账"
                />
                <Reading
                  label="差额"
                  value={`${remaining < 0 ? '−' : ''}${Math.abs(remaining).toLocaleString()}`}
                  tone={remaining < 0 || !pool.available ? 'warn' : 'plain'}
                  note={
                    // 池不可用时差额再大也发不出去：额度是「允许多少」，不是
                    // 「现在能发多少」，后者还要过池可用这一关。这一格在最常见
                    // 的故障（配额窗用尽、池子躺着）里差额恰恰很宽裕，不写这句
                    // 读者会当成「还有余量、可以接着跑」。
                    !pool.available
                      ? '池此刻不可用：差额再宽裕也发不出去，看「池」与「上游自述复位」'
                      : remaining < 0
                        ? '越过摊平位置：新的整活会被压到下一窗再发'
                        : '离摊平位置还剩这么多'
                  }
                />
                <Reading
                  label="一个整活"
                  value={pace.reserve_tokens.toLocaleString()}
                  note={
                    pace.reserve_population > 0
                      ? `测自 ${pace.reserve_population} 个已完成的活`
                      : '最近没有可测的整活，预留量按 0 算'
                  }
                />
                <Reading
                  label="窗长"
                  value={mixedWindows ? '没有共同的窗' : spanOf(pace.window_secs)}
                  tone={mixedWindows ? 'warn' : 'plain'}
                  note={
                    mixedWindows
                      ? `${pace.measured_upstreams} 条上游各量各的窗，长度不同：额度是各家摊平量相加的报表，不是某一家的剩余额度`
                      : `${pace.measured_upstreams} 条上游的实测窗合成这一读`
                  }
                />
                <Reading
                  label="窗起点"
                  value={
                    mixedWindows ? (
                      '没有共同起点'
                    ) : pace.window_start_at ? (
                      <Moment iso={pace.window_start_at} />
                    ) : (
                      '未报'
                    )
                  }
                  tone={mixedWindows ? 'warn' : 'plain'}
                />
                <Reading
                  label="窗尾"
                  value={mixedWindows ? '算不出来' : windowEnd ? <Moment iso={windowEnd} /> : '未报'}
                  tone={mixedWindows ? 'warn' : 'plain'}
                  note={
                    mixedWindows
                      ? '各家复位时刻见「上游自述复位」——用最早起点加最长窗长会得到一个谁都不在的日期'
                      : '窗尾即下一窗开始，允许量从这里重新摊'
                  }
                />
              </>
            ) : (
              <Reading
                label="额度"
                value="还没有量过整窗"
                tone="warn"
                note="上游第一次以「这一窗用完了」拒绝之前没有容量可算——这不是额度为 0"
              />
            )}
            <Reading
              label="上游自述复位"
              value={pool.recovery_at ? <Moment iso={pool.recovery_at} /> : '上游没说'}
              note={pool.quota_window_secs > 0 ? `嫌疑上游声明的窗长 ${spanOf(pool.quota_window_secs)}` : undefined}
            />
            <Reading
              label="下次探测"
              value={pool.next_attempt_at ? <Moment iso={pool.next_attempt_at} /> : '无嫌疑上游'}
              note="我方重试的节拍，不是上游的承诺"
            />
            <Reading
              label="最后送出"
              value={pool.last_served_at ? <Moment iso={pool.last_served_at} /> : '这代进程没见过'}
              note="写快照的进程最后一次送出真实请求；上游被探测通了不算"
            />
          </div>
        </Readings>
      )}
    </section>
  );
}

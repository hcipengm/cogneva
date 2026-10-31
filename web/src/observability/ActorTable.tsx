import { Fragment, useState } from 'react';
import type { ActorUsage } from '@/api/client';
import { actorHint, registeredActorNames } from '@/observability/DimensionCounts';
import {
  actorTotals,
  cacheFormula,
  compact,
  hitRate,
  percent,
  splitUpstream,
  type ActorGroup,
} from '@/observability/usageRows';
import { Note } from '@/ui/Note';

/**
 * 谁在花：一张表两级行。
 *
 * 折叠账的一行是「执行者 × 环境 × 上游 × 协议」四格合起来才唯一的，所以拆成两处
 * 画的时候，两处一定对不齐——分摊条按四格行分段，表里也是四格行，读者要自己
 * 做一次分组才知道「那条最宽的其实是同一个执行者的三种协议」。这里只留一份：
 * 一级行是执行者（合起来的那一面，答「是不是有一个人吃掉大半」），点开是它的
 * 四格行（答「他换过几条线路」）。两级共用同一个 `<table>`，所以列宽、对齐、
 * 数值格式都只有一份定义。
 *
 * 零 token 的行照样在表里——一条有调用、有失败、却没有 token 的行不是噪声，
 * 它说的是「这个执行者在试，但一次都没拿到回应」，上游熔断的时候整段窗口就是
 * 这样。把它滤掉，剩下的表只会显示「这段没花多少」，而真实是「这段根本没跑
 * 起来」。闭集里登记了、这段窗口一次也没出现的执行者也列着，灰色、不可展开：
 * 出现过几个不是全集。
 *
 * 一级行的缓存占比只在**这个执行者只有一种协议**时印数。两种协议的 `input`
 * 含义不同（OpenAI 兼容面已含缓存，Anthropic 面不相交），先把两个协议的缓存
 * 加起来再除，就是拿一个协议的分母解释另一个协议的分子；那种情况写「分协议
 * 见下」，比值由展开后的四格行按协议各印一个。
 */
export function ActorTable({ actors }: { actors: ActorUsage[] }) {
  const groups = actorTotals(actors);
  const present = new Set(groups.map((group) => group.actor));
  const absent = registeredActorNames().filter((name) => !present.has(name));
  const [open, setOpen] = useState<Record<string, boolean>>({});

  const grand = groups.reduce((sum, group) => sum + group.input + group.output, 0);
  const totalCalls = groups.reduce((sum, group) => sum + group.calls, 0);
  const totalFailed = groups.reduce((sum, group) => sum + group.failed, 0);
  const spenders = groups.filter((group) => group.input + group.output > 0);
  const toggle = (actor: string) => setOpen((prev) => ({ ...prev, [actor]: !prev[actor] }));

  return (
    <div className="flex flex-col gap-2 px-4 pb-4 pt-1" style={{ userSelect: 'text' }}>
      {grand === 0 ? (
        // 一条都没有宽度：不存在「谁占了大部分」。空着一条底也不是零花销的图。
        <p className="text-[11px]" style={{ color: 'var(--danger)' }}>
          这段有 {totalCalls.toLocaleString()} 次调用、0 token，其中{' '}
          {totalFailed.toLocaleString()} 次失败：不是「没人用模型」，是每次调用都在拿到回应前就结束了。
          下面按执行者列出它们各自试了多少次。
        </p>
      ) : (
        <div className="flex h-2 w-full overflow-hidden bg-[var(--ink-700)]" aria-hidden>
          {spenders.map((group, index) => (
            <span
              key={group.actor}
              style={{
                width: `${((group.input + group.output) / grand) * 100}%`,
                background:
                  index % 2 === 0
                    ? 'var(--brass)'
                    : 'color-mix(in srgb, var(--brass) 55%, var(--ink-600))',
              }}
            />
          ))}
        </div>
      )}

      <table className="grid-table">
        <thead>
          <tr>
            <th style={{ width: W.actor }}>执行者</th>
            <th style={{ width: W.share }}>占比</th>
            <th style={{ width: W.calls }} className="text-right">
              调用
            </th>
            <th style={{ width: W.failed }} className="text-right">
              失败
            </th>
            <th style={{ width: W.input }} className="text-right">
              输入
            </th>
            <th style={{ width: W.output }} className="text-right">
              输出
            </th>
            <th style={{ width: W.cached }} className="text-right">
              缓存
            </th>
            <th style={{ width: W.rate }} className="text-right">
              缓存占比
            </th>
            <th style={{ width: W.per }} className="text-right">
              每调用输入
            </th>
            <th style={{ width: W.latency }} className="text-right">
              均耗
            </th>
          </tr>
        </thead>
        <tbody>
          {groups.map((group) => {
            const total = group.input + group.output;
            const expandable = group.calls > 0;
            const expanded = expandable && !!open[group.actor];
            return (
              <Fragment key={group.actor}>
                <tr style={total === 0 ? { opacity: 0.6 } : undefined}>
                  <td>
                    <Note
                      as={expandable ? 'button' : 'span'}
                      type={expandable ? 'button' : undefined}
                      onClick={expandable ? () => toggle(group.actor) : undefined}
                      aria-expanded={expandable ? expanded : undefined}
                      className="mono flex w-full items-baseline gap-1 text-left text-[11px]"
                      text={groupNote(group)}
                    >
                      <span className="w-2 shrink-0 text-[var(--muted)]">
                        {expandable ? (expanded ? '▾' : '▸') : ''}
                      </span>
                      <span className="min-w-0 truncate">{group.actor}</span>
                    </Note>
                  </td>
                  <td className={NUM}>
                    <Note
                      text={`${compact(total)} / ${compact(grand)}：这一行的「输入+输出」除以全场合计`}
                      className="text-[var(--paper)]"
                    >
                      {percent(total, grand)}
                    </Note>
                  </td>
                  <td className={NUM}>{group.calls.toLocaleString()}</td>
                  <td
                    className={NUM}
                    style={{ color: group.failed > 0 ? 'var(--danger)' : 'var(--muted)' }}
                  >
                    {group.failed.toLocaleString()}
                  </td>
                  <td className={NUM}>{group.input.toLocaleString()}</td>
                  <td className={NUM}>{group.output.toLocaleString()}</td>
                  <td className={NUM} style={{ color: 'var(--muted)' }}>
                    {group.cached.toLocaleString()}
                  </td>
                  <td className={NUM}>
                    {group.styles.length === 1 ? (
                      <Note
                        className="text-[var(--muted)]"
                        text={cacheFormula(group.styles[0], group.input, group.cached)}
                      >
                        {hitRate(group.styles[0], group.input, group.cached)}
                      </Note>
                    ) : (
                      <Note
                        className="text-[10px] text-[var(--warn)]"
                        text={`这个执行者这段走过了 ${group.styles.length} 种协议（${group.styles.join('、')}）。两种协议的 input 含义不同，先把两个协议的缓存加起来再除，是拿一个协议的分母解释另一个协议的分子——所以这一格不印数，展开看每种协议各自的比值。`}
                      >
                        分协议见下
                      </Note>
                    )}
                  </td>
                  <td className={NUM} style={{ color: 'var(--muted)' }}>
                    {group.calls > 0 ? (
                      <Note text={perCallNote('输入', group.input, group.calls)}>
                        {Math.round(group.input / group.calls).toLocaleString()}
                      </Note>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className={NUM} style={{ color: 'var(--muted)' }}>
                    {group.calls > 0 && group.latencyWeighted > 0 ? (
                      <Note
                        text={`Σ(每行均耗 × 该行调用) = ${Math.round(group.latencyWeighted).toLocaleString()} ms·次，除以 ${group.calls.toLocaleString()} 次调用。直接对几行的均耗取平均会让调了一次的那行和调了一万次的那行一样重。`}
                      >
                        {`${Math.round(group.latencyWeighted / group.calls)} ms`}
                      </Note>
                    ) : (
                      '—'
                    )}
                  </td>
                </tr>
                {expanded &&
                  group.rows.map((row) => {
                    const rowTotal = row.tokens_input + row.tokens_output;
                    const up = splitUpstream(row.upstream);
                    return (
                      <tr key={`${group.actor}|${rowKeyOf(row)}`}>
                        <td className="pl-8">
                          <Note
                            className="mono block w-full truncate text-[10px] text-[var(--muted)]"
                            text={up.base}
                          >
                            <span style={{ color: up.missing ? 'var(--warn)' : undefined }}>
                              {up.model}
                            </span>
                            {` · ${row.origin} · ${row.api_style === '-' ? '未发起' : row.api_style}`}
                          </Note>
                        </td>
                        <td className={`${NUM} text-[10px]`} style={{ color: 'var(--muted)' }}>
                          <Note text={`${compact(rowTotal)} / ${compact(grand)}：与一级行同一个分母（全场合计），所以同一个执行者的几行加起来正好等于它的那一行`}>
                            {percent(rowTotal, grand)}
                          </Note>
                        </td>
                        <td className={`${NUM} text-[10px]`}>{row.calls.toLocaleString()}</td>
                        <td
                          className={`${NUM} text-[10px]`}
                          style={{ color: row.failed_calls > 0 ? 'var(--danger)' : 'var(--muted)' }}
                        >
                          {row.failed_calls.toLocaleString()}
                        </td>
                        <td className={`${NUM} text-[10px]`}>{row.tokens_input.toLocaleString()}</td>
                        <td className={`${NUM} text-[10px]`}>{row.tokens_output.toLocaleString()}</td>
                        <td className={`${NUM} text-[10px]`} style={{ color: 'var(--muted)' }}>
                          {row.tokens_cached.toLocaleString()}
                        </td>
                        <td className={`${NUM} text-[10px]`}>
                          <Note
                            className="text-[var(--muted)]"
                            text={cacheFormula(row.api_style, row.tokens_input, row.tokens_cached)}
                          >
                            {hitRate(row.api_style, row.tokens_input, row.tokens_cached)}
                          </Note>
                        </td>
                        <td className={`${NUM} text-[10px]`} style={{ color: 'var(--muted)' }}>
                          {row.calls > 0 ? (
                            <Note text={perCallNote('输入', row.tokens_input, row.calls)}>
                              {Math.round(row.tokens_input / row.calls).toLocaleString()}
                            </Note>
                          ) : (
                            '—'
                          )}
                        </td>
                        <td className={`${NUM} text-[10px]`} style={{ color: 'var(--muted)' }}>
                          {row.avg_latency_ms ? `${row.avg_latency_ms} ms` : '—'}
                        </td>
                      </tr>
                    );
                  })}
              </Fragment>
            );
          })}
          {absent.map((name) => (
            <tr key={`absent-${name}`} style={{ opacity: 0.45 }}>
              <td>
                <Note className="mono flex w-full items-baseline gap-1 text-[11px]" text={actorHint(name)}>
                  <span className="w-2 shrink-0" />
                  <span className="min-w-0 truncate">{name}</span>
                </Note>
              </td>
              <td colSpan={9} className="text-[10px] text-[var(--muted)]">
                本窗口没有调用（闭集里登记了，不是这段没花）
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <p className="text-[11px] text-[var(--muted)]">
        条上的每一段就是表里的一行，同一个顺序、同一个分母（全场合计的输入+输出）。
        一级行是执行者，点开是它的四格行——「一个执行者 × 一个环境 × 一个上游 ×
        一种协议」四格合起来才是账上唯一的一行，同一个执行者换过环境就是两行。
        上游那格写模型名，指针停上去看它挂在哪条线路上；写「未发起」的行没有任何
        上游被尝试——请求在池级熔断那一步就被拒了。
      </p>
      {spenders.length < groups.length && (
        <p className="text-[11px] text-[var(--muted)]">
          {groups.length - spenders.length} 行是零 token 的：它们有调用、没拿到回应，
          所以不进那条分摊条，但留在表里——把一条试过却失败的调用从「谁在花」里删掉，
          读起来就像那段时间根本没人调模型。
        </p>
      )}
    </div>
  );
}

/** 列宽只写在这里：两级行共用同一个 `<table>`，宽度写第二次就会有一天两边差几像素。 */
const W = {
  actor: '17rem',
  share: '6.5rem',
  calls: '5.5rem',
  failed: '5rem',
  input: '7rem',
  output: '7rem',
  cached: '7rem',
  rate: '7.5rem',
  per: '7rem',
  latency: '5.5rem',
} as const;

/** 数值一律右对齐、等宽、等宽数字：一列比大小靠的是位数对齐。 */
const NUM = 'mono text-right tabular-nums text-[11px]';

function rowKeyOf(row: ActorUsage): string {
  return `${row.origin}|${row.upstream}|${row.api_style}`;
}

/** 一级行的注释：这个执行者是谁、这段走过哪些环境与协议、以及展开能看到什么。 */
function groupNote(group: ActorGroup): string {
  const parts = [actorHint(group.actor)];
  const origins = [...new Set(group.rows.map((row) => row.origin))];
  parts.push(`这段 ${group.rows.length} 行：环境 ${origins.join('、')}；协议 ${group.styles.join('、')}`);
  if (group.calls > 0 && group.input + group.output === 0) {
    parts.push('有调用、一次也没拿到回应：token 为 0 是因为请求都在拿到回应前就结束了');
  }
  if (group.calls > 0) parts.push('点开看它的四格行');
  return parts.join('。');
}

/** 「每调用多少 token」的来路：分子、分母、以及分母里为什么含被挡回来的那些。 */
function perCallNote(what: string, tokens: number, calls: number): string {
  return `${tokens.toLocaleString()} ${what} token / ${calls.toLocaleString()} 次调用。分母是全部尝试，含被拒、失败的那些——它们的 token 是 0，所以这一格读的是「摊到每次尝试上有多少」，不是「成功那几次有多长」。`;
}

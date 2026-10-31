/**
 * 一面上的图：把一份读数的**形状**画出来。
 *
 * 表回答「是哪几条」，图回答「多不多、偏不偏、什么时候开始」。同一份快照两边都用，
 * 所以图上不会出现表里没有的那一条——图不是第二份数据，是同一份数据的另一种排法。
 *
 * 四条约定，四条都是为了让图上那句结论和读数对得上：
 *
 * - 幅度用同一个色（`--live`），状态才用保留色（`--ok` / `--warn` / `--danger`）。
 *   拿状态色去区分「第几个系列」，读者的第一反应会是「这几个有问题」。
 * - 标签按数据自己带出来的名字写，不另编一套漂亮的；图上叫 `gantt`，表里也叫
 *   `gantt`，读者才对得上。
 * - 每一格都带 `title`：鼠标停在上面能读出这一格的原值。密度是这个界面的功能，
 *   给每一格都标数字会把图压成表，所以标在悬停里。
 * - 画不出来的量不画成 0。「没法量」与「量出来是 0」在图上必须是两种样子，
 *   否则读者会把「没量过」当成「一条都没有」——这正是这些面最常犯的错。
 *
 * 没有第二套颜色：色值全部取自 `styles/tokens.css` 的角色令牌，换主题时它们跟着
 * 一起换。数值、名字、图例一律用文字色，不用系列色——颜色只标身份，不标内容。
 *
 * 这四个形状都只画**一条**量。两条不同尺度的量并排（比如「花销」与「失败次数」）
 * 要画两幅：一幅一个纵轴，读者才不用在脑子里换算哪个高度算「多」。
 */

import type { ReactNode } from 'react';
import { Note } from '@/ui/Note';

/** 图上能用的那几种语义。名字是语义不是色值：换主题时只换令牌。 */
export type Tone = 'magnitude' | 'ok' | 'warn' | 'danger' | 'muted';

const TONE_VAR: Record<Tone, string> = {
  magnitude: 'var(--live)',
  ok: 'var(--ok)',
  warn: 'var(--warn)',
  danger: 'var(--danger)',
  muted: 'var(--muted)',
};

/**
 * 一幅图的题头与注脚。
 *
 * 题头写这一幅回答的是哪个问题（不是「柱状图」这种形状名）；注脚写它的来路与
 * 边界。两行都用文字色：颜色在这套界面里只标身份与状态，不标内容。
 */
function Caption({ label, note }: { label: string; note?: string }) {
  return (
    <div className="flex flex-col gap-[2px] pb-1">
      <span className="text-[11px] text-[var(--paper-dim)]">{label}</span>
      {note && <span className="text-[11px] text-[var(--muted)]">{note}</span>}
    </div>
  );
}

/** 一格一段的横条：每个名字一段，长的按最大值归一。 */
export interface BarRow {
  label: string;
  /** 同名的行不止一条时用它区分（React 的 key 与悬停的身份）。不填就用 `label`。 */
  id?: string;
  value: number;
  /** 值的写法（带上单位）。不填就写数字。 */
  note?: string;
  tone?: Tone;
  /** 悬停时多一句：这条为什么是这个长度。 */
  detail?: string;
}

/**
 * 横条组：一条一行，长度按本组最大值归一。
 *
 * 不按总数归一：一屏上十行，最大那条占满整幅，读者要的是「谁最多、差多少」，
 * 不是「占全部的百分之几」——后者用 `Strip`。
 */
export function BarList({
  rows,
  label,
  note,
  empty,
  max: bound,
}: {
  rows: BarRow[];
  /** 这一幅图回答的问题。也是它的可读名。 */
  label: string;
  /** 来路与边界：这份数从哪读的、哪一段没算进来。 */
  note?: string;
  empty?: ReactNode;
  /**
   * 共同的量尺。不给就按本组最大值归一（「谁最多」）；给了就一律按它归一
   * （「离满还差多少」）——百分比那种行必须给，否则满屏都是 50% 时每一行都会
   * 画满，看上去像全都够了。
   */
  max?: number;
}) {
  const max = bound ?? rows.reduce((m, r) => Math.max(m, r.value), 0);
  return (
    <div className="px-4 py-2">
      <Caption label={label} note={note} />
      {rows.length === 0 ? (
        <p className="text-[11px] text-[var(--muted)]">{empty ?? '没有可画的读数'}</p>
      ) : (
        <div role="img" aria-label={`${label}：${rows.map((r) => `${r.label} ${r.value}`).join('，')}`}>
          {rows.map((row) => {
            const width = max > 0 ? Math.max(row.value > 0 ? 2 : 0, (row.value / max) * 100) : 0;
            return (
              <div key={row.id ?? row.label} className="flex items-center gap-2 py-[3px]">
                <Note className="w-40 shrink-0 truncate text-[11px] text-[var(--paper-dim)]" text={row.label}>
                  {row.label}
                </Note>
                <span className="relative h-2 flex-1 bg-[var(--ink-700)]">
                  {width > 0 && (
                    <Note
                      className="absolute inset-y-0 left-0 rounded-r-[4px]"
                      style={{ width: `${width}%`, background: TONE_VAR[row.tone ?? 'magnitude'] }}
                      text={`${row.label}：${row.note ?? row.value}${row.detail ? `（${row.detail}）` : ''}`}
                    />
                  )}
                </span>
                <span className="w-16 shrink-0 text-right text-[11px] tabular-nums text-[var(--paper-dim)]">
                  {row.note ?? row.value}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/** 一整条按比例分段：每段是这一类的占比，段与段之间留 2px 的缝。 */
export interface StripSegment {
  label: string;
  value: number;
  tone: Tone;
}

/**
 * 分布条：一条横条按比例切开，回答「这一堆里各有几个」。
 *
 * 段与段之间那 2 像素不是装饰：没有它，两个相邻的段在深色底上会连成一条，
 * 读者数不出究竟有几段。图例另起一行写成「名字 数字」，所以颜色不是唯一的
 * 身份线索——色觉不同的读者照样读得出来。
 */
export function Strip({
  segments,
  label,
  note,
  empty,
}: {
  segments: StripSegment[];
  label: string;
  note?: string;
  empty?: ReactNode;
}) {
  const shown = segments.filter((s) => s.value > 0);
  const total = shown.reduce((n, s) => n + s.value, 0);
  return (
    <div className="px-4 py-2">
      <Caption label={label} note={note} />
      {shown.length === 0 ? (
        <p className="text-[11px] text-[var(--muted)]">{empty ?? '没有可画的读数'}</p>
      ) : (
        <div role="img" aria-label={`${label}：${shown.map((s) => `${s.label} ${s.value}`).join('，')}`}>
          <div className="flex h-2 w-full gap-[2px]">
            {shown.map((s) => (
              <Note
                key={s.label}
                className="h-full first:rounded-l-[4px] last:rounded-r-[4px]"
                style={{ width: `${(s.value / total) * 100}%`, background: TONE_VAR[s.tone] }}
                text={`${s.label}：${s.value}`}
              />
            ))}
          </div>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 pt-1">
            {shown.map((s) => (
              <span key={s.label} className="flex items-center gap-1 text-[11px] text-[var(--paper-dim)]">
                <span
                  aria-hidden
                  className="inline-block h-[8px] w-[8px] rounded-[2px]"
                  style={{ background: TONE_VAR[s.tone] }}
                />
                {s.label}
                <span className="tabular-nums">{s.value}</span>
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * 一段折线：一组按时间先后排好的数。
 *
 * 不画坐标轴、不标刻度：这一幅要回答的是「什么时候起变的」，读确切数值去表里。
 * 标上刻度会让它看起来像一张能读数的图，而它手里的数只是快照里带的那几段。
 */
export function Sparkline({
  values,
  label,
  note,
  tone = 'magnitude',
  height = 28,
}: {
  values: number[];
  label: string;
  note?: string;
  tone?: Tone;
  height?: number;
}) {
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const span = max - min || 1;
  const step = values.length > 1 ? 100 / (values.length - 1) : 0;
  const points = values
    .map((v, i) => `${(i * step).toFixed(2)},${(100 - ((v - min) / span) * 100).toFixed(2)}`)
    .join(' ');
  return (
    <div className="px-4 py-2">
      <Caption label={label} note={note} />
      {values.length < 2 ? (
        <p className="text-[11px] text-[var(--muted)]">只有 {values.length} 个点，画不出走向</p>
      ) : (
        <div role="img" aria-label={`${label}：${values.length} 个点，最低 ${min}，最高 ${max}`}>
          <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ height }} className="w-full">
            <title>{`${label}：${values.length} 个点，最低 ${min}，最高 ${max}`}</title>
            <polyline
              points={points}
              fill="none"
              stroke={TONE_VAR[tone]}
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
              vectorEffect="non-scaling-stroke"
            />
          </svg>
        </div>
      )}
    </div>
  );
}

/**
 * 一格对着一个上限的量。
 *
 * `value` 为 `null` 时不画成空条：空条读作 0，而真相是这一格**没量出来**。那种
 * 情形画出斜纹并写明原因——「没量」与「量出来是 0」在这一面上是两件事。
 */
export function Meter({
  value,
  max,
  label,
  note,
  unmeasurable,
  tone = 'magnitude',
}: {
  value: number | null;
  max: number;
  label: string;
  note?: string;
  /** `value` 为 null 时写在图上的原因。 */
  unmeasurable?: string;
  tone?: Tone;
}) {
  const ratio = value === null || max <= 0 ? null : Math.min(1, Math.max(0, value / max));
  return (
    <div className="px-4 py-2">
      <div className="flex items-baseline justify-between gap-3">
        <span className="text-[11px] text-[var(--paper-dim)]">{label}</span>
        <span className="text-[11px] tabular-nums text-[var(--paper-dim)]">
          {value === null ? '没量出来' : (note ?? `${value} / ${max}`)}
        </span>
      </div>
      <Note
        as="div"
        className="mt-[3px] h-2 w-full bg-[var(--ink-700)]"
        role="img"
        aria-label={
          value === null
            ? `${label}：没量出来${unmeasurable ? `，${unmeasurable}` : ''}`
            : `${label}：${note ?? `${value} / ${max}`}`
        }
        text={value === null ? unmeasurable : undefined}
      >
        {ratio === null ? (
          <span
            className="block h-full w-full opacity-40"
            style={{
              background: 'repeating-linear-gradient(135deg, var(--muted) 0 3px, transparent 3px 6px)',
            }}
          />
        ) : (
          <span
            className="block h-full rounded-r-[4px]"
            style={{ width: `${ratio * 100}%`, background: TONE_VAR[tone] }}
          />
        )}
      </Note>
      {value === null && unmeasurable && (
        <p className="pt-1 text-[11px] text-[var(--warn)]">{unmeasurable}</p>
      )}
    </div>
  );
}

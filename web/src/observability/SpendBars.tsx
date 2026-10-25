import type { WindowSpend } from '@/api/client';

/**
 * 一条按窗口排开的花销柱：每个整点窗口花了多少 token。
 *
 * 这张图只回答一个问题——「什么时候烧的」。输入 / 输出各占多少不在这里分开画：
 * 两支相邻的颜色在深色底上差异很小，而这条图的价值是把尖峰挑出来；拆分放在
 * 悬停提示和下面的表里，那里有地方写清楚。一列一个窗口，没有第二根纵轴。
 *
 * 没有折叠行的窗口画成虚线的空格，不画成零高的柱：折叠表里缺一格要么是那一
 * 小时没人用，要么是还没折到这里，两者在图上都是空，而零高的柱会把后者说成
 * 前者。缺口是这段图里唯一用虚线的形状，图例就写在标题里。
 */
export function SpendBars({
  rows,
  since,
  until,
}: {
  rows: WindowSpend[];
  since: string | null;
  until: string | null;
}) {
  const base = buildSlots(rows);
  if (base.slots.length === 0) {
    return (
      <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
        这段区间没有被折叠行覆盖{since && until ? '（端点非空却没有行，是账本身不一致）' : ''}：
        不是「没花钱」，是折叠还没走到这里——最早要等一个整点窗口关掉。
      </p>
    );
  }

  const { slots: display, size } = bucket(base.slots, MAX_BARS);
  const peak = Math.max(...display.map((slot) => slot.input + slot.output), 0);

  return (
    <div className="flex flex-col gap-2 px-4 py-3">
      <div className="flex items-baseline justify-between gap-3 text-[11px]">
        <span className="text-[var(--paper-dim)]">
          每个窗口的 token（输入 + 输出）
          <span className="ml-2 text-[var(--muted)]">虚线格 = 这一格没有折叠行</span>
        </span>
        <span className="mono shrink-0 text-[var(--muted)]">
          {base.stepLabel}
          {size > 1 && ` · 每 ${size} 格并一柱`}
          {peak > 0 && ` · 峰值 ${compact(peak)}`}
        </span>
      </div>

      <div className="flex h-24 items-end gap-[2px] border-b border-[var(--line)]">
        {display.map((slot) => {
          const total = slot.input + slot.output;
          if (slot.missing === slot.windows) {
            return (
              <div
                key={slot.key}
                title={`${stamp(slot.at)}：这一格没有折叠行（没人用，或还没折到这里）`}
                className="flex-1 self-stretch border-x border-dashed border-[var(--line)]"
                style={{ minWidth: 1 }}
              />
            );
          }
          // 零花销但有折叠行：这一格真的量过，画一条贴着基线的实线，和虚线缺口分开。
          const height = peak > 0 ? Math.max((total / peak) * 100, 1.5) : 1.5;
          return (
            <div
              key={slot.key}
              title={describe(slot)}
              className="flex-1 transition-colors"
              style={{
                height: `${height}%`,
                minWidth: 1,
                background: slot.failed > 0 && total === 0 ? 'var(--danger)' : 'var(--brass-dim)',
                borderRadius: '4px 4px 0 0',
              }}
            />
          );
        })}
      </div>

      <div className="mono flex justify-between text-[10px] text-[var(--muted)]">
        <span>{stamp(display[0].at)}</span>
        <span>{stamp(display[display.length - 1].end)}</span>
      </div>
    </div>
  );
}

/** 一屏最多几根柱。再多就把相邻窗口并成一柱，并在标题里写明并了几格。 */
const MAX_BARS = 72;

interface Slot {
  key: string;
  at: Date;
  end: Date;
  input: number;
  output: number;
  cached: number;
  calls: number;
  failed: number;
  /** 这一格里一共有几个折叠窗口。 */
  windows: number;
  /** 其中没有折叠行的有几个。全缺才算缺口。 */
  missing: number;
  top: { actor: string; tokens: number }[];
}

interface Built {
  slots: Slot[];
  /** 一格代表多长（「每 1 小时」）。读数来自数据自己的窗口边界，不是写死的。 */
  stepLabel: string;
}

/**
 * 把折叠行排到时间轴上。
 *
 * 步长取自数据自己相邻两个窗口的间隔，不假定折叠一定是整点：写死一小时会让
 * 换过窗口长度的账在同一张图上错位。区间里缺的那些格补成缺口，让「没有行」
 * 在图上占一个位置而不是被跳过——跳过之后没人看得出中间断过。
 */
function buildSlots(rows: WindowSpend[]): Built {
  const starts = [...new Set(rows.map((row) => Date.parse(row.window_start)))].sort((a, b) => a - b);
  if (starts.length === 0) return { slots: [], stepLabel: '' };

  const step = commonStep(starts, rows);
  const byStart = new Map<number, WindowSpend[]>();
  for (const row of rows) {
    const at = Date.parse(row.window_start);
    const list = byStart.get(at);
    if (list) list.push(row);
    else byStart.set(at, [row]);
  }

  const slots: Slot[] = [];
  const last = starts[starts.length - 1];
  for (let at = starts[0]; at <= last; at += step) {
    const group = byStart.get(at);
    if (!group) {
      slots.push({
        key: `${at}`,
        at: new Date(at),
        end: new Date(at + step),
        input: 0,
        output: 0,
        cached: 0,
        calls: 0,
        failed: 0,
        windows: 1,
        missing: 1,
        top: [],
      });
      continue;
    }
    const sum = (pick: (row: WindowSpend) => number) => group.reduce((total, row) => total + pick(row), 0);
    const byActor = new Map<string, number>();
    for (const row of group) {
      byActor.set(row.actor, (byActor.get(row.actor) ?? 0) + row.tokens_input + row.tokens_output);
    }
    slots.push({
      key: `${at}`,
      at: new Date(at),
      end: new Date(Date.parse(group[0].window_end)),
      input: sum((row) => row.tokens_input),
      output: sum((row) => row.tokens_output),
      cached: sum((row) => row.tokens_cached),
      calls: sum((row) => row.calls),
      failed: sum((row) => row.failed_calls),
      windows: 1,
      missing: 0,
      top: [...byActor.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([actor, tokens]) => ({ actor, tokens })),
    });
  }
  return { slots, stepLabel: `每 ${describeStep(step)} 一格` };
}

/** 相邻窗口最常见的间隔。折叠表里窗口边界由网格决定，所以最常见的就是网格步长。 */
function commonStep(starts: number[], rows: WindowSpend[]): number {
  if (starts.length === 1) {
    const width = Date.parse(rows[0].window_end) - starts[0];
    return width > 0 ? width : 3_600_000;
  }
  const counts = new Map<number, number>();
  for (let i = 1; i < starts.length; i += 1) {
    const delta = starts[i] - starts[i - 1];
    if (delta > 0) counts.set(delta, (counts.get(delta) ?? 0) + 1);
  }
  let best = 3_600_000;
  let bestCount = 0;
  for (const [delta, count] of counts) {
    if (count > bestCount || (count === bestCount && delta < best)) {
      best = delta;
      bestCount = count;
    }
  }
  return best;
}

/** 柱太多时把相邻几格并成一柱：并了就在标题里说出来，别让读者以为一格是一小时。 */
function bucket(slots: Slot[], max: number): { slots: Slot[]; size: number } {
  if (slots.length <= max) return { slots, size: 1 };
  const size = Math.ceil(slots.length / max);
  const out: Slot[] = [];
  for (let i = 0; i < slots.length; i += size) {
    const group = slots.slice(i, i + size);
    const byActor = new Map<string, number>();
    for (const slot of group) {
      for (const entry of slot.top) byActor.set(entry.actor, (byActor.get(entry.actor) ?? 0) + entry.tokens);
    }
    out.push({
      key: group[0].key,
      at: group[0].at,
      end: group[group.length - 1].end,
      input: group.reduce((total, slot) => total + slot.input, 0),
      output: group.reduce((total, slot) => total + slot.output, 0),
      cached: group.reduce((total, slot) => total + slot.cached, 0),
      calls: group.reduce((total, slot) => total + slot.calls, 0),
      failed: group.reduce((total, slot) => total + slot.failed, 0),
      windows: group.reduce((total, slot) => total + slot.windows, 0),
      missing: group.reduce((total, slot) => total + slot.missing, 0),
      top: [...byActor.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([actor, tokens]) => ({ actor, tokens })),
    });
  }
  return { slots: out, size };
}

/** 悬停时说的那一段：这一格是谁花的、拆开是多少。 */
function describe(slot: Slot): string {
  const lines = [
    `${stamp(slot.at)} – ${stamp(slot.end)}`,
    `${compact(slot.input + slot.output)} token（入 ${compact(slot.input)} / 出 ${compact(slot.output)}）`,
    `${slot.calls} 次调用${slot.failed > 0 ? `，失败 ${slot.failed}` : ''}`,
  ];
  if (slot.missing > 0) lines.push(`${slot.missing}/${slot.windows} 格没有折叠行`);
  if (slot.top.length > 0) {
    lines.push('主要花在：' + slot.top.map((entry) => `${entry.actor} ${compact(entry.tokens)}`).join(' · '));
  }
  return lines.join('\n');
}

function describeStep(ms: number): string {
  const minutes = Math.round(ms / 60_000);
  if (minutes < 60) return `${minutes} 分钟`;
  const hours = minutes / 60;
  return `${Number.isInteger(hours) ? hours : hours.toFixed(1)} 小时`;
}

function stamp(at: Date): string {
  return at.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function compact(value: number): string {
  if (value >= 100_000_000) return `${(value / 100_000_000).toFixed(1)}亿`;
  if (value >= 10_000) return `${(value / 10_000).toFixed(1)}万`;
  return value.toLocaleString();
}

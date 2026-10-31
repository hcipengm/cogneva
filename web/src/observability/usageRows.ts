import type { ActorUsage } from '@/api/client';

/**
 * 折叠账两种粒度之间的换算：四格行 → 执行者。
 *
 * 折叠表的一行是「执行者 × 环境 × 上游 × 协议」四格合起来才唯一的；「谁在花」
 * 要先把它们按执行者合起来，再让每一行在二级里各自站着。合起来的那一份只能算
 * 一次：分摊条的每一段、一级行的每一格、占比的分母若各算一遍，它们迟早各说各的
 * ——屏幕上表现为「条上看着占七成、表里写着 68%」，而没有任何东西报错。
 */
export interface ActorGroup {
  actor: string;
  /** 这个执行者的四格行，按（输入+输出）从大到小。 */
  rows: ActorUsage[];
  calls: number;
  failed: number;
  input: number;
  output: number;
  cached: number;
  /**
   * 加权均耗的分子：Σ(行均耗 × 行调用)。
   *
   * 一行的 `avg_latency_ms` 是那一行自己的平均；把几行的平均再平均一次，等于让
   * 「调了一次的那一行」和「调了一万次的那一行」在结果里一样重。分母因此是调用数。
   */
  latencyWeighted: number;
  /** 这一组出现过的协议。只有一个时，一级行才印得出缓存占比。 */
  styles: string[];
}

/** 一行折叠账的唯一键：四格合起来才是唯一的一行。 */
export function rowKey(actor: ActorUsage): string {
  return `${actor.actor}|${actor.origin}|${actor.upstream}|${actor.api_style}`;
}

/** 把四格行合到执行者一级。顺序：合计大的在前，其次调用多的，最后按名字。 */
export function actorTotals(actors: ActorUsage[]): ActorGroup[] {
  const byActor = new Map<string, ActorGroup>();
  for (const actor of actors) {
    const group: ActorGroup = byActor.get(actor.actor) ?? {
      actor: actor.actor,
      rows: [],
      calls: 0,
      failed: 0,
      input: 0,
      output: 0,
      cached: 0,
      latencyWeighted: 0,
      styles: [],
    };
    group.rows.push(actor);
    group.calls += actor.calls;
    group.failed += actor.failed_calls;
    group.input += actor.tokens_input;
    group.output += actor.tokens_output;
    group.cached += actor.tokens_cached;
    group.latencyWeighted += actor.avg_latency_ms * actor.calls;
    if (!group.styles.includes(actor.api_style)) group.styles.push(actor.api_style);
    byActor.set(actor.actor, group);
  }
  const groups = [...byActor.values()];
  for (const group of groups) {
    group.rows.sort(
      (a, b) =>
        b.tokens_input + b.tokens_output - (a.tokens_input + a.tokens_output) ||
        b.calls - a.calls ||
        rowKey(a).localeCompare(rowKey(b))
    );
  }
  return groups.sort(
    (a, b) =>
      b.input + b.output - (a.input + a.output) ||
      b.calls - a.calls ||
      a.actor.localeCompare(b.actor)
  );
}

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
 * 也正因为分母逐协议不同，两个协议的缓存不能先加起来再除：那是一个协议的分子
 * 除以另一个协议的分母。
 */
export function cacheBase(style: string, input: number, cached: number): number | null {
  if (style.startsWith('openai')) return input;
  if (style === 'anthropic') return input + cached;
  return null;
}

/** 这一格命中率怎么写；分母为零或协议不认识时没有比例可言，写 `—`。 */
export function hitRate(style: string, input: number, cached: number): string {
  const base = cacheBase(style, input, cached);
  if (base === null || base <= 0) return '—';
  return percent(cached, base);
}

/** 这一格命中率的来路：分子、分母、以及这个分母为什么长这样。 */
export function cacheFormula(style: string, input: number, cached: number): string {
  const base = cacheBase(style, input, cached);
  if (base === null) return `${style}：这个协议没有已知的缓存记法，比例算不出来（不是 0%）`;
  if (base <= 0) return `${style}：分母为 0（这段没有输入 token），没有比例可言`;
  const how =
    style === 'anthropic'
      ? 'Anthropic 面上 input 与缓存不相交，分母是 input + cached'
      : 'OpenAI 兼容面的 input 已经含缓存命中，分母就是 input';
  return `缓存 ${cached.toLocaleString()} / 分母 ${base.toLocaleString()}（${how}）`;
}

/**
 * 逐协议的 `input` / `cached` 小计。
 *
 * 两种协议共享同一个词，但 `input` 的含义不同：OpenAI 兼容面它已经含了缓存命中，
 * Anthropic 面两者不相交。小计必须按协议分开，命中率的分母才可能落对——先跨协议
 * 求和再除，是一个协议的分子除以另一个协议的分母。
 */
export function styleCells(
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

/** 一份占一份的百分比；整份为零时没有比例可说。 */
export function percent(part: number, whole: number): string {
  if (whole <= 0) return '—';
  const value = (part / whole) * 100;
  return `${value >= 10 ? Math.round(value) : value.toFixed(1)}%`;
}

/** 账上的上游是「地址|模型」；`-` 是「一个都没试」。 */
export function splitUpstream(raw: string): { model: string; base: string; missing: boolean } {
  if (raw === '-') {
    return { model: '未发起', base: '池级熔断拒绝：请求没有送到任何上游', missing: true };
  }
  const cut = raw.lastIndexOf('|');
  if (cut < 0) return { model: raw, base: '网关没有记下模型名', missing: false };
  return { model: raw.slice(cut + 1), base: raw.slice(0, cut), missing: false };
}

/** 大数按万/亿缩写：读的人要的是量级，不是每一位都念一遍。 */
export function compact(value: number): string {
  if (value >= 100_000_000) return `${(value / 100_000_000).toFixed(1)}亿`;
  if (value >= 10_000) return `${(value / 10_000).toFixed(1)}万`;
  return value.toLocaleString();
}

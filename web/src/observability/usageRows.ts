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

/**
 * 缓存写入这一半：三种状态，不是三个数。
 *
 * 命中为 0 有两种成因，修法相反——压根没让上游缓存任何东西（标记没送出去），
 * 与标记送到了但前缀每发都在变。分开它们的只有这一列，所以这一列**缺读数时
 * 不许印 0**：0 是一个结论（「上游说它没写」），「没量过」是另一个答案
 * （OpenAI 兼容面与 Responses 面根本不报；旧镜像里连这一列都没有）。
 *
 * 聚合规则跟着网关走：组里只要有一行没报，整条线就是 `null`。把报了的那几行
 * 加起来会得到一个像模像样的数，而它只盖住了这一组的一半，屏上没有任何东西
 * 能把「残的」与「全的」分开。
 */
/** 组里只要有一行没报，整条线就是 `null`；全都报了才是那几行的和。 */
export function cacheWrite(
  rows: { api_style: string; tokens_cache_created?: number | null }[]
): number | null {
  if (rows.length === 0) return null;
  let total = 0;
  for (const row of rows) {
    const value = row.tokens_cache_created;
    // 这里是 `=== null || === undefined`，不是 `?? 0`：`?? 0` 会把「没量到」压成
    // 一个量到的零，而那正是这一列要分开的两件事。
    if (value === null || value === undefined) return null;
    total += value;
  }
  return total;
}

/**
 * 这一半为什么没有数。三种「没有」的下一步完全不同，所以不能合成一句话：
 * - `absent`：整键都不在（这一版网关还没有这一列）——去升级镜像；
 * - `foreign`：这家协议根本不报这门读数（OpenAI 兼容面与 Responses 面）——没有可修的；
 * - `unreported`：该报的行这一窗没报——这一窗的读数只能当缺。
 */
function writeState(
  rows: { api_style: string; tokens_cache_created?: number | null }[]
): 'absent' | 'foreign' | 'unreported' {
  if (rows.length > 0 && rows.every((row) => row.tokens_cache_created === undefined)) {
    return 'absent';
  }
  if (rows.length > 0 && rows.every((row) => row.api_style !== 'anthropic')) {
    return 'foreign';
  }
  return 'unreported';
}

/** 缓存写入那一格写什么。缺读数时印的是「为什么没有」，绝不是 0。 */
export function cacheWriteValue(
  rows: { api_style: string; tokens_cache_created?: number | null }[]
): string {
  const tokens = cacheWrite(rows);
  if (tokens !== null) return tokens.toLocaleString();
  const state = writeState(rows);
  if (state === 'absent') return '还没进账';
  if (state === 'foreign') return '这家不报';
  return '没报';
}

/** 缓存写入那一格的来路：三态各有一句话，只有量到的那一态才印数字。 */
export function cacheWriteNote(
  rows: { api_style: string; tokens_cache_created?: number | null }[]
): string {
  const tokens = cacheWrite(rows);
  if (tokens !== null) {
    return `上游报的缓存写入量（cache_creation_input_tokens）= ${tokens.toLocaleString()}；这一半与「缓存」那一列的读回量不相加、不互为分母。`;
  }
  const state = writeState(rows);
  if (state === 'absent') {
    return '这一半还没进账（现网镜像还没有这一列）：字段缺席是「没量过」，不是「写了 0」。';
  }
  if (state === 'foreign') {
    return '这家不报缓存写入这一半：OpenAI 兼容面与 Responses 面根本不发这门读数，这里的「没有」不是量到 0。';
  }
  return '这一组有一只上游这一窗没报缓存写入，整条线因此不印数——把报了的几行加起来会得到一个只盖住半张窗的数。';
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

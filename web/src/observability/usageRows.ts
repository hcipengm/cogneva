import type { ActorUsage, LlmPoolView } from '@/api/client';

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
  /** 这一组出现过的协议（请求走的哪个面）。命中率不再依赖它，见 [`cacheBase`]。 */
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
 * 缓存命中的分母：整个 prompt，也就是 `input + cached`。与协议无关。
 *
 * 记账面在落账前就把两个协议面归一了：OpenAI 兼容面的 `prompt_tokens` **含**缓存
 * 命中、Anthropic 面的 `input_tokens` 不含，前者先减去命中量再记进 `input`。于是
 * 两列装的是同一件事的两半——`input` 是没被缓存服务的那部分，`cached` 是被缓存
 * 服务的那部分——命中率一律 `cached / (input + cached)`。
 *
 * 这里曾经按 `api_style` 挑分母（OpenAI 面取 `input`，Anthropic 面取两者之和）。
 * 那个写法在两种记法并存时也是错的：`api_style` 记的是**请求走哪个面**，而上游回
 * 哪种形状是另一回事——实测过一行 `api_style=anthropic` 却带回 OpenAI 形状的尾帧，
 * 按 Anthropic 那一支算，88% 的命中被读成 1.1%。归一之后那一支更是把 OpenAI 面的
 * 命中率压到一成多。一个不是见证者的字段不许决定分母，所以分母只有一个。
 */
export function cacheBase(input: number, cached: number): number {
  return input + cached;
}

/** 这一格命中率怎么写；分母为零时没有比例可言，写 `—`。 */
export function hitRate(input: number, cached: number): string {
  return percent(cached, cacheBase(input, cached));
}

/** 这一格命中率的来路：分子、分母、以及这个分母为什么长这样。 */
export function cacheFormula(style: string, input: number, cached: number): string {
  const base = cacheBase(input, cached);
  if (base <= 0) return `${style}：分母为 0（这段没有输入 token），没有比例可言`;
  return `缓存 ${cached.toLocaleString()} / 分母 ${base.toLocaleString()}（分母是整个 prompt：input 只装没被缓存服务的那部分，两个协议面在记账时已经归一）`;
}

/**
 * 逐协议的 `input` / `cached` 小计。
 *
 * `input` 只装没被缓存服务的那部分、`cached` 是被缓存服务的那部分——这条不变式
 * 两个协议面一样（记账时已经归一），所以两者之和就是整段 prompt，跨协议相加也
 * 不再是把两件事加在一起。仍然分协议列出来，是因为「哪个面问的」本身是读数：
 * 两个面的命中率差得再远，也要看得见是谁拖的。
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
/**
 * 这一列的行；`calls`／`failed_calls` 只用来判「这段到底有没有成功过」，
 * `tokens_cached` 只用来判同一个窗口里**读回**了多少——见 [`cacheWriteNote`]。
 */
type WriteRow = {
  api_style: string;
  tokens_cache_created?: number | null;
  tokens_cached?: number;
  calls?: number;
  failed_calls?: number;
};

/** 组里只要有一行没报，整条线就是 `null`；全都报了才是那几行的和。 */
export function cacheWrite(rows: WriteRow[]): number | null {
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
function writeState(rows: WriteRow[]): 'absent' | 'foreign' | 'unreported' {
  if (rows.length > 0 && rows.every((row) => row.tokens_cache_created === undefined)) {
    return 'absent';
  }
  if (rows.length > 0 && rows.every((row) => row.api_style !== 'anthropic')) {
    return 'foreign';
  }
  return 'unreported';
}

/** 这一段里有没有一次调用真的拿到了回应——这一半只随成功的那几次落账。 */
function anySuccess(rows: WriteRow[]): boolean {
  return rows.some((row) => (row.calls ?? 0) > (row.failed_calls ?? 0));
}

/** 缓存写入那一格写什么。缺读数时印的是「为什么没有」，绝不是 0。 */
export function cacheWriteValue(rows: WriteRow[]): string {
  const tokens = cacheWrite(rows);
  if (tokens !== null) return tokens.toLocaleString();
  const state = writeState(rows);
  if (state === 'absent') return '还没进账';
  if (state === 'foreign') return '这家不报';
  return '没报';
}

/**
 * 缓存写入那一格的来路：三态各有一句话，只有量到的那一态才印数字。
 *
 * 「空着」这件事本身要能说清是**正常的**：这一半只在一次成功调用把用量带回来
 * 时才落账，所以一段窗口整片没有数，最可能的成因是那段窗口根本没有成功过——账上
 * 的事实，不是采集坏了。读者把「还没进账」读成「采集断了」，就会去查一个不存在的
 * 故障，而真正该看的是池子什么时候能再送出一件活。
 */
export function cacheWriteNote(rows: WriteRow[]): string {
  const tokens = cacheWrite(rows);
  if (tokens !== null) {
    // 报上来的 0 与同一窗的读回量一起读，才是这一格的意思。
    //
    // 「写了 0」单看是一个结论，而它有两个互不相容的成因：这段窗口的每一条
    // 都在读一段更早写下的前缀（5 分钟 TTL 之内，一次写可以被后面好几条读），
    // 或者这家面根本不逐次报写入。唯一能把两者分开的证据就在隔壁那一列——
    // 读回量大于 0 说明缓存确实存在，于是这个 0 不再是「没建过缓存」。
    // 2026-11-01 21 时实测：148 次调用、命中 45.8%，写入一栏 127 行全报 0，
    // 按旧的读法这一屏会告诉读者「前缀每发都在变」，而读回量正反驳它。
    const readBack = rows.reduce((sum, row) => sum + (row.tokens_cached ?? 0), 0);
    if (tokens === 0 && readBack > 0) {
      return (
        `上游报的缓存写入量 = 0，但同一批调用从缓存读回了 ${readBack.toLocaleString()} token。` +
        `这两半互相矛盾：没有写过的缓存读不回来。所以这个 0 不是「这段没建过缓存」，` +
        `要么这批前缀是更早的窗口写下的（每次写入可以被 TTL 内的后面几条读走），` +
        `要么这家面不逐次报写入。判前缀稳不稳看「缓存命中率」那一列，不要用这一格。`
      );
    }
    return `上游报的缓存写入量（cache_creation_input_tokens）= ${tokens.toLocaleString()}；这一半与「缓存」那一列的读回量不相加、不互为分母。`;
  }
  const state = writeState(rows);
  if (state === 'absent') {
    return '这一半还没进账：跑着的网关还没有这个字段，而且即便有了，也要等一次成功调用把用量带回来才有数。整片空着是正常的，不是采集坏了，也不是「上游说它写了 0」。';
  }
  if (state === 'foreign') {
    return '这家不报缓存写入这一半：OpenAI 兼容面与 Responses 面根本不发这门读数，这里的「没有」不是量到 0。';
  }
  if (!anySuccess(rows)) {
    const calls = rows.reduce((sum, row) => sum + (row.calls ?? 0), 0);
    return `这段 ${calls.toLocaleString()} 次调用一次也没成功，所以没有任何一次调用能带回这一半的读数——空着是账上的事实，不是采集坏了。`;
  }
  return '这一组有调用成功，却有上游这一窗没报缓存写入，整条线因此不印数——把报了的几行加起来会得到一个只盖住半张窗的数。';
}

/** 「下一次可能进账」那一格的判据：时刻是哪来的、以及它为什么不是承诺。 */
export interface CacheWriteLanding {
  /** 最早可能进账的时刻；池子的快照里两个时刻都没有时是 null。 */
  at: string | null;
  /** 这个时刻是谁说的（复位的承诺方不同，可信度也不同）。 */
  basis: string;
  note: string;
}

/**
 * 缓存写入那一半下一次可能进账的时刻，取自**上游池的当刻快照**。
 *
 * 这一格不能写死日期：配额窗的复位时刻是上游自己说的、随时会变，写进前端包里的
 * 那一天过了就成了假读数，而且没有任何东西会红。能从账上读到的只有池子此刻的
 * 三个时刻——上游自述复位（它说的最早时刻，不是承诺）、我方下次探测（我方重试的
 * 节拍）、最后送出（真正送出过一件活的时刻）。它们都不是「一定会在那时进账」，
 * 所以这一格印的是**界与依据**，不是倒计时。
 */
export function cacheWriteLanding(
  pool: LlmPoolView | null | undefined,
  now: number = Date.now()
): CacheWriteLanding {
  const recovery = pool?.recovery_at ?? null;
  const probe = pool?.next_attempt_at ?? null;
  const served = pool?.last_served_at ?? null;
  // 只认**还在未来**的时刻。这两个时刻是绝对时刻，而屏上的「现在」一直在往前走：
  // 一个已经到过的时刻照原样印成排期，读的人会一直等一件已经发生过、或者根本不会
  // 再发生的事。页面拿不到「它为什么没兑现」——那要两次读之间比大小，一次读看不见
  // ——所以这里只报「它已经过去了」，不猜成因。
  const future = [recovery, probe]
    .filter((at): at is string => at !== null && Date.parse(at) > now)
    .sort((a, b) => Date.parse(a) - Date.parse(b));
  const at = future[0] ?? null;
  const basis = at === null
    ? '快照里没有还在未来的时刻'
    : at === recovery
      ? '上游自述复位'
      : '我方下次探测';
  const parts: string[] = [];
  if (recovery) {
    parts.push(
      `上游自述复位 ${when(recovery)}${Date.parse(recovery) > now ? '（它说的最早时刻，不是承诺）' : '（已经过去了：这个时刻现在是一个已经到过的时刻，不是排期）'}`
    );
  }
  if (probe) {
    parts.push(
      `我方下次探测 ${when(probe)}${Date.parse(probe) > now ? '（重试的节拍，不是上游的承诺）' : '（已经过去了：不是排期）'}`
    );
  }
  parts.push(
    served
      ? `池子最后一次真的拿到上游回应 ${when(served)}（探测通不算）`
      : '写快照的那一代进程还没有成功过一件活'
  );
  return {
    at,
    basis,
    note: `${parts.join('；')}。这一半要等一次成功调用把用量带回来才有数：现在整片空着是账上真的没有这样的调用，不是采集坏了。`,
  };
}

/** 时刻说成人话，用浏览器本地时区——与页面别处的时间读数同一口径。 */
function when(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN');
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

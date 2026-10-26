/**
 * 项目一览里先给读者看哪几行。
 *
 * 任务图里一条 goal 常常只有一条任务（2026-10-26 实测 3831 条任务上有 3726 个
 * goal），于是这一栏会有几千行。把几千行平铺在四张屏上面，四张屏就在几万像素
 * 之外——读者看到的是「一墙 UUID」，结论是「这里没有范围、没有看板」。视图全在，
 * 只是排在了墙后面。
 *
 * 所以默认只铺前几行（服务端已按任务数从多到少排好，前几行就是有内容的那些），
 * 其余的用一个数字说清楚「还有多少没列出」，再由读者决定是过滤还是展开。三个
 * 数字分开记，因为它们回答三个不同的问题：`hidden` 是「还在墙后面」，
 * `filteredOut` 是「被你这句话筛掉的」，`shown` 是「此刻屏幕上真有这几行」。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-live-projects.sh` 就是这么验的）。
 */

/** 默认铺几行。够看出项目之间的差别，又不至于把下面四张屏推出去。 */
export const STRIP_CAP = 12;

export interface StripOptions {
  /** 读者输入的过滤词，空串＝不过滤。 */
  query?: string;
  /** 读者点过「显示全部」。 */
  expanded?: boolean;
  /** 当前选中的项目；它必须在场，否则一览与下面的面板说的不是同一个项目。 */
  selected?: string | null;
  cap?: number;
}

export interface StripView<R> {
  /** 屏幕上真的会画出来的那几行，顺序与传进来的一致（服务端按任务数排好）。 */
  shown: R[];
  /** 与过滤词相符、但因为折叠没画出来的行数。 */
  hidden: number;
  /** 被过滤词筛掉的行数。与 `hidden` 是两件事：前者是读者的选择，后者是这里的取舍。 */
  filteredOut: number;
}

/**
 * 挑出这一帧要画的项目行。
 *
 * 两条例外都写在返回值里而不是靠调用方记得：选中行永远在场（哪怕它在折叠线
 * 以下），读者一旦给了过滤词就按他的话铺满（他已经在缩小范围，再替他砍一刀
 * 只会让他以为没搜到）。
 */
export function visibleProjects<R extends { id: string }>(
  rows: R[],
  options: StripOptions = {}
): StripView<R> {
  const query = (options.query ?? '').trim().toLowerCase();
  const cap = options.cap ?? STRIP_CAP;
  const base = query ? rows.filter((r) => r.id.toLowerCase().includes(query)) : rows;
  const room = options.expanded || query ? base.length : Math.max(0, cap);
  const shown = base.slice(0, room);
  const selected = options.selected ?? null;
  if (selected && !shown.some((r) => r.id === selected)) {
    const hit = base.find((r) => r.id === selected);
    // 只补选中行本身，不把中间那几千行一起带出来。
    if (hit) shown.push(hit);
  }
  return {
    shown,
    hidden: base.length - shown.length,
    filteredOut: rows.length - base.length,
  };
}

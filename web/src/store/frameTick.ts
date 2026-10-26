/**
 * 「帧到了，这一面该重读了吗」——把轮询面接到总线上的那一段判断。
 *
 * 平台上的读数大多是这样来的：每 10/15/20 秒读一次 REST 快照，帧另走一路进活动
 * 流与时间轴。于是「屏幕在动」这件事只跟轮询间隔有关：把间隔调成 10 分钟，任务刚
 * 结束、流程刚往前走，屏幕一动不动——而总线上那一帧其实已经到了。这一面要做的
 * 不是把帧当数用（帧里只有「这条任务现在是什么状态」，没有基线、没有预算），而是
 * 拿帧当**触发器**：有东西变了就立刻重读，读回来的仍然是服务端现算的那一份。
 *
 * 三种做法都试过，只有第三种不坏：
 *
 *   - 每来一帧就重读：忙的时候一秒三帧，同一个接口被连取几十遍，读的还是同一份
 *     字节。构建与集群同机，这条路把门禁自己拖慢。
 *   - 纯尾随去抖（每来一帧把定时器往后推）：常忙时帧永不停，定时器永远到不了点，
 *     这一面反而比轮询还旧——把「帧驱动」做成了「帧饿死」。
 *   - 前沿 + 地板：能立刻读就立刻读，两读之间至少隔 `MIN_GAP_MS`；间隔内的帧合并
 *     成到期那一刻的一次读。忙时读的频次有上界，闲时第一帧就见效。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-stream-driven-refresh.sh` 就是这么验的）。
 */

/**
 * 两次重读之间至少隔这么久。
 *
 * 取两秒而不是零：读者看不出 100 毫秒与两秒的差别，接口却看得出——而且一屏上
 * 通常同时挂着三四个读数，一次触发是把它们一起重读。
 */
export const MIN_GAP_MS = 2_000;

/** 这一次该做什么：立刻读、等一会儿读、还是什么都不做。 */
export type RefreshAction = 'now' | 'wait' | 'skip';

export interface RefreshPlan {
  action: RefreshAction;
  /** `action` 为 `wait` 时，还要等多少毫秒。 */
  waitMs: number;
}

/**
 * 来了一帧（或一帧改动了折叠出来的态）之后，这一面该怎么重读。
 *
 * `lastRefreshMs` 是**上一次真的读出去**的时刻，不是上一次想读的时刻：拿想法当
 * 读数，间隔内的帧会把地板一次次推后，等于把前沿做回尾随。
 *
 * `hasPending` 是「已经有一发排在路上了」。这时什么都不做——不是省一次读，是防止
 * 每一帧都排一发，十秒里排出几十个定时器、随后一起响。
 *
 * 时钟往回拨（`nowMs < lastRefreshMs`）按「现在就读」处理：等一个负的间隔会等到
 * 永不到期的定时器，这一面就静默停住了。
 */
export function planRefresh(
  nowMs: number,
  lastRefreshMs: number | null,
  hasPending: boolean
): RefreshPlan {
  if (hasPending) return { action: 'skip', waitMs: 0 };
  if (lastRefreshMs === null || !Number.isFinite(lastRefreshMs)) {
    return { action: 'now', waitMs: 0 };
  }
  if (!Number.isFinite(nowMs) || nowMs < lastRefreshMs) {
    return { action: 'now', waitMs: 0 };
  }
  const elapsed = nowMs - lastRefreshMs;
  if (elapsed >= MIN_GAP_MS) return { action: 'now', waitMs: 0 };
  return { action: 'wait', waitMs: MIN_GAP_MS - elapsed };
}

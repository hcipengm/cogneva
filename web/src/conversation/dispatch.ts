import type { ProcessOverview } from '@/api/client';
import type { ToolCallRecord } from '@/store/streamStore';

/**
 * 一段对话交出去的那道目标，现在谁在接着做。
 *
 * 对话面能自己画出来的只有「说了什么」，说不出来的是「交给谁了、做到哪儿了」。
 * 那部分在流程总览里：目标是任务图上的一个父行，它下面每一行带着执行者。
 * 这里把总览折成按 `goal_id` 取的一张表，让每一段对话都能指着自己的目标问一句
 * 「现在到什么程度、谁在跑」。
 *
 * 折法只有一处：几段对话同时读同一份总览，各自再折一遍会把同一批行数成不同的数。
 */

export interface GoalLive {
  goalId: string;
  total: number;
  completed: number;
  failed: number;
  running: number;
  pending: number;
  /** 0–100。分母为 0 时是 0，不是 NaN——一条目标还没分解出子任务时它确实是零。 */
  progressPct: number;
  /** 这道目标的任务被派给过的执行者。空表示还没有人认领。 */
  agents: string[];
}

export function goalLiveIndex(overview: ProcessOverview | null): Record<string, GoalLive> {
  const index: Record<string, GoalLive> = {};
  for (const goal of overview?.goals ?? []) {
    const agents: string[] = [];
    for (const task of goal.tasks) {
      if (task.agent_id && !agents.includes(task.agent_id)) agents.push(task.agent_id);
    }
    index[goal.goal_id] = {
      goalId: goal.goal_id,
      total: goal.total,
      completed: goal.completed,
      failed: goal.failed,
      running: goal.running,
      pending: goal.pending,
      progressPct: goal.progress_pct,
      agents,
    };
  }
  return index;
}

/** 一段话在界面上怎么念：目标的头八位够认，长了会把标题行挤掉。 */
export function shortGoal(goalId: string): string {
  return goalId.slice(0, 8);
}

/**
 * 这道目标开始之后，它名下的执行者调过的工具。
 *
 * 工具的帧只带 `agent_id`，不带目标——所以归属只能按「这道目标派给过谁」加「这道
 * 目标什么时候交出去的」两件事一起判。两条都缺的时候宁可不显示，也不把别人的活
 * 画到这段对话下面：一段对话里出现一件它没让人做的事，比空着更坏。
 *
 * 说不准的地方写在界面上（「同一执行者可能同时在跑别的目标」），不在这里悄悄滤掉
 * ——滤掉之后读者看到的是一份完整但不真的账。
 */
export function callsForGoal(
  calls: ToolCallRecord[],
  live: GoalLive | null | undefined,
  since: string | null
): ToolCallRecord[] {
  if (!live || live.agents.length === 0) return [];
  const from = since ? Date.parse(since) : Number.NEGATIVE_INFINITY;
  const agents = new Set(live.agents);
  return calls
    .filter((call) => {
      if (!call.agent || !agents.has(call.agent)) return false;
      const at = Date.parse(call.startedAt);
      return Number.isFinite(at) ? at >= from : true;
    })
    .slice()
    .reverse();
}

import { create } from 'zustand';

/**
 * 「总线上又有一帧过去了」——按帧的种类各记一个数，不带内容。
 *
 * 内容是各面自己的事：这里只回答「刚刚有没有这一类帧」，由读快照的那几面拿它当
 * 重读的依赖。为什么不是一个布尔值：`true → false` 的复位要靠读的一方做，读的
 * 一方有两三个，谁先复位谁就把别人的信号吞了；涨上去的整数没有这个问题。
 *
 * 为什么按种类而不是只有一个总数：平台上 `task_status_change` 一刻不停，若它算
 * 「有东西变了」，那么配额、花销、设置这些与任务无关的面也会被它拖着每两秒重读
 * 一次——那不是事件驱动，是换了个说法的轮询，还把服务端拖进去。每一面只订自己
 * 认得的那几种，剩下的帧对它等于没发生。
 *
 * 心跳与正文增量不推这里：它们按构造高频（每个 token 一帧），让它们触发重读等于
 * 把轮询换成另一种更密的轮询。
 */
interface FrameTickState {
  /** 任何一帧都涨：给「全量帧」那一面（审计）用。 */
  tick: number;
  /**
   * 按种类各涨一个。键有两种：总线原生帧就是变体名（`task_status_change`、
   * `agent_end`……），REST 侧一次成功的写则是 `resource:<前缀>`（见
   * `resourceKey`）——两边共用一个表，键就是这一面订的那件事。
   */
  byKind: Record<string, number>;
  note: (kind: string) => void;
}

export const useFrameTickStore = create<FrameTickState>((set) => ({
  tick: 0,
  byKind: {},
  note: (kind) =>
    set((state) => ({
      tick: state.tick + 1,
      byKind: { ...state.byKind, [kind]: (state.byKind[kind] ?? 0) + 1 },
    })),
}));

import { create } from 'zustand';

/**
 * 「总线上又有一帧改动了折叠出来的态」——只计数，不带内容。
 *
 * 内容是各面自己的事：这一支只回答「刚刚有没有东西变过」，由读快照的那几面拿它
 * 当重读的依赖。为什么不是一个布尔值：`true → false` 的复位要靠读的一方做，读的
 * 一方有两三个，谁先复位谁就把别人的信号吞了；涨上去的整数没有这个问题，谁读过
 * 谁记着自己读到哪个数。
 *
 * 心跳与正文增量不推这里：它们按构造高频（每个 token 一帧），让它们触发重读等于
 * 把轮询换成另一种更密的轮询。它们改的是画布上正在流的那一条消息，那一面本来就
 * 是逐帧渲染的。
 */
interface FrameTickState {
  tick: number;
  note: () => void;
}

export const useFrameTickStore = create<FrameTickState>((set) => ({
  tick: 0,
  note: () => set((state) => ({ tick: state.tick + 1 })),
}));

import { create } from 'zustand';
import { EMPTY_PULSE, foldArrival, type Pulse } from '@/store/livePulse';

interface LivePulseState {
  /**
   * 帧到达的折叠结果：最近一条什么时候到、窗口里到了几帧、连上以后一共几帧。
   *
   * 与各面的活体状态分开存：那些折的是「帧说了什么」，这里折的是「帧有没有来」。
   * 平台停摆时前者一动不动、后者才是唯一会说「停了多久」的读数——两者合成一份，
   * 就看不出「没有新事情」与「没有新帧」的区别了。
   */
  pulse: Pulse;
  /** 一帧到了。`atMs` 是到达时刻（调用方给 `Date.now()`），不是帧里的 timestamp。 */
  noteArrival: (atMs: number) => void;
  clear: () => void;
}

export const useLivePulseStore = create<LivePulseState>((set) => ({
  pulse: EMPTY_PULSE,
  noteArrival: (atMs) => set((state) => ({ pulse: foldArrival(state.pulse, atMs) })),
  clear: () => set({ pulse: EMPTY_PULSE }),
}));

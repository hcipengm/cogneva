import { create } from 'zustand';
import {
  foldAgentFrame,
  pruneAgents,
  type LiveAgentFrame,
  type LiveAgentRow,
} from '@/store/liveAgents';

interface LiveAgentState {
  /**
   * 每个执行者一行，键是 agent_id。
   *
   * 与会话无关、跨面共用：名单面读它补状态，画布与流程面将来也要读同一份——
   * 两处各折一遍，同一个人在两张屏上会说出两个状态。
   */
  rows: Record<string, LiveAgentRow>;
  noteFrame: (frame: LiveAgentFrame) => void;
  clear: () => void;
}

export const useLiveAgentStore = create<LiveAgentState>((set) => ({
  rows: {},
  noteFrame: (frame) =>
    set((state) => {
      const next = {
        ...state.rows,
        [frame.agentId]: foldAgentFrame(state.rows[frame.agentId], frame),
      };
      return { rows: pruneAgents(next) };
    }),
  clear: () => set({ rows: {} }),
}));

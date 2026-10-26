import { create } from 'zustand';
import {
  foldAlertFrame,
  pruneAlerts,
  type LiveAlertFrame,
  type LiveAlertRow,
} from '@/store/liveAlerts';

interface LiveAlertState {
  /**
   * 每次越界一行，键是 `agent_id::metric`。
   *
   * 与会话无关、跨面共用：告警面读它说清「表之外刚发生了什么」，状态条将来也要
   * 读同一份——两处各折一遍，同一次越界会在两张屏上给出两个数值。
   *
   * 每行什么时候不再显示不在这里决定：那是「比上次读表晚」这个判据的事，由画面
   * 拿快照时刻去问 `breachesSinceSnapshot`。这里只留事实。
   */
  rows: Record<string, LiveAlertRow>;
  noteFrame: (frame: LiveAlertFrame) => void;
  clear: () => void;
}

export const useLiveAlertStore = create<LiveAlertState>((set) => ({
  rows: {},
  noteFrame: (frame) =>
    set((state) => {
      const next = {
        ...state.rows,
        [frame.key]: foldAlertFrame(state.rows[frame.key], frame),
      };
      return { rows: pruneAlerts(next) };
    }),
  clear: () => set({ rows: {} }),
}));

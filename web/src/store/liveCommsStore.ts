import { create } from 'zustand';
import {
  foldCommsFrame,
  pruneComms,
  type LiveCommsFrame,
  type LiveCommsRow,
} from '@/store/liveComms';

interface LiveCommsState {
  /**
   * 每个「任务 + 结局」一行，键是 `task_id::status`。
   *
   * 与会话无关、跨面共用：通信面读它说清「图之外刚交了哪些活」，任务面将来也要读
   * 同一份——两处各折一遍，同一次交接会在两张屏上说出两个时刻。
   *
   * 什么时候不再显示不在这里决定：那是「比上次读图晚」这个判据的事，由画面拿快照
   * 时刻去问 `outcomesSinceSnapshot`。这里只留事实。
   */
  rows: Record<string, LiveCommsRow>;
  noteFrame: (frame: LiveCommsFrame) => void;
  clear: () => void;
}

export const useLiveCommsStore = create<LiveCommsState>((set) => ({
  rows: {},
  noteFrame: (frame) =>
    set((state) => {
      const next = {
        ...state.rows,
        [frame.key]: foldCommsFrame(state.rows[frame.key], frame),
      };
      return { rows: pruneComms(next) };
    }),
  clear: () => set({ rows: {} }),
}));

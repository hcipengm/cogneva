import { create } from 'zustand';
import {
  foldTaskFrame,
  pruneTasks,
  type LiveTaskFrame,
  type LiveTaskRow,
} from '@/store/liveProjects';

interface LiveProjectState {
  /**
   * 每条任务一行，键是任务 id。
   *
   * 跨面共用：范围、看板、甘特、挣值四张屏读同一份折叠结果。各折一遍的话，同一次
   * 状态变化会在两张屏上给出两个时刻，而读者分不出哪个是这一次的。
   *
   * 什么时候不再显示不在这里决定：那是「比这次读表晚」这个判据的事，由画面拿快照
   * 时刻去问 `changedSinceSnapshot`。这里只留事实。
   */
  rows: Record<string, LiveTaskRow>;
  /**
   * 哪条任务归哪个项目。范围树是唯一知道这件事的地方，它读回来时登记，其余三面
   * 据此判断一帧要不要惊动自己。登记只增不删：项目之间的归属不随状态变化而变，
   * 删掉会让下一次帧到达时误判成「不属于任何项目」而少一次重读。
   */
  owned: Record<string, string>;
  noteFrame: (frame: LiveTaskFrame) => void;
  own: (projectId: string, taskIds: string[]) => void;
  clear: () => void;
}

export const useLiveProjectStore = create<LiveProjectState>((set) => ({
  rows: {},
  owned: {},
  noteFrame: (frame) =>
    set((state) => {
      const next = {
        ...state.rows,
        [frame.taskId]: foldTaskFrame(state.rows[frame.taskId], frame),
      };
      return { rows: pruneTasks(next) };
    }),
  own: (projectId, taskIds) =>
    set((state) => {
      let changed = false;
      const owned = { ...state.owned };
      for (const id of taskIds) {
        if (owned[id] === projectId) continue;
        owned[id] = projectId;
        changed = true;
      }
      // 每次读回来都写一份新对象，会让订阅者（四个面板）在每一拍轮询后都重渲染
      // 一次，而它们读的是同一个数。没有任何变化时原样返回。
      return changed ? { owned } : {};
    }),
  clear: () => set({ rows: {}, owned: {} }),
}));

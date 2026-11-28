import { create } from 'zustand';

/**
 * 这个人写下的约定：下一轮开头会自动进提示词的那几句。
 *
 * 与权限规则同一本账的写法：`null` 是「还没读到」，空数组是「读到了，一句也
 * 没写」。两者在界面上是两句不同的话——合起来写，一次没连上的读取会被读成
 * 一次干净的家底。
 */
export interface MemoryEntry {
  id: string;
  text: string;
}

interface MemoryState {
  entries: MemoryEntry[] | null;
  setEntries: (entries: MemoryEntry[]) => void;
  /** 断开连接时清回「还没读到」：上一条连接读到的名单不该留在屏幕上冒充现状。 */
  clearEntries: () => void;
}

export const useMemoryStore = create<MemoryState>((set) => ({
  entries: null,
  setEntries: (entries) => set({ entries }),
  clearEntries: () => set({ entries: null }),
}));

import { create } from 'zustand';

/**
 * 还没保存的改动，按路径存。
 *
 * 为什么放在页签之外：页签是可以关掉的，而一份没保存的草稿不该因为「关了个
 * 标签」就消失。关掉再打开同一个文件时草稿还在，保存或显式放弃才清掉它。
 * 也不落 localStorage——草稿里可能有密钥，而这份东西的寿命只该到这一屏为止。
 */
interface DraftState {
  drafts: Record<string, string>;
  setDraft: (path: string, text: string) => void;
  clearDraft: (path: string) => void;
}

export const useDrafts = create<DraftState>((set) => ({
  drafts: {},
  setDraft: (path, text) =>
    set((state) => ({ drafts: { ...state.drafts, [path]: text } })),
  clearDraft: (path) =>
    set((state) => {
      const drafts = { ...state.drafts };
      delete drafts[path];
      return { drafts };
    }),
}));

import { create } from 'zustand';

/**
 * 工作台的状态：哪一面活着、开了哪些页、底部面板停在哪一格。
 *
 * 全部放在一个 store 里，而不是每个视图各持一份：命令面板要能「跳到某一面并
 * 选中它」，状态条要读「现在这一面有没有未处理的问题」，这些都必须看到同一份
 * 当前状态。视图自己只持自己的数据。
 */

/** 左栏的活动项。每一项对应侧栏里的一个面板，与编辑器里的页不是一回事。 */
export type ActivityId =
  | 'workspace'
  | 'files'
  | 'inbox'
  | 'agents'
  | 'tasks'
  | 'alerts'
  | 'changes'
  | 'governance'
  | 'observability'
  | 'settings';

/** 每一面在页签上写什么。切面时要能顺手开出这一页，名字就得在这里认得出。 */
export const VIEW_TITLES: Record<ActivityId, string> = {
  workspace: '工作台',
  files: '文件',
  inbox: '收件箱',
  agents: '智能体',
  tasks: '任务',
  alerts: '告警',
  changes: '变更',
  governance: '治理',
  observability: '观测',
  settings: '设置',
};

/** 底部面板的四格：问题、输出、日志、终端。 */
export type PanelTab = 'problems' | 'output' | 'logs';

/** 编辑器里的一页。`view` 是视图注册表里的键，`id` 是这一页自己的身份。 */
export interface EditorTab {
  id: string;
  title: string;
  view: string;
  /** 页标题旁的细小说明，例如「只读」「按 30 秒轮询」。 */
  hint?: string;
}

interface ShellState {
  activity: ActivityId;
  sidebarVisible: boolean;
  tabs: EditorTab[];
  activeTabId: string;
  panelOpen: boolean;
  panelTab: PanelTab;
  /** 命令面板与快速打开共用一个浮层，`mode` 决定它问的是什么。 */
  palette: 'closed' | 'commands' | 'open';
  /** 底部输出格的累积行。命令与失败都写在这里，读者不必打开控制台。 */
  output: string[];
  /**
   * 「把这一页打开到这一行」的请求。搜索结果是路径 + 行号，而页的身份是
   * `file:<路径>`：点击时先把页打开、再把落点留在这里，由文件页在读到正文之后
   * 消费掉——它才是唯一知道这一行在正文里是什么位置的那一层。
   */
  reveal: { path: string; line: number; query: string } | null;

  setActivity: (activity: ActivityId) => void;
  toggleSidebar: () => void;
  openTab: (tab: EditorTab) => void;
  closeTab: (id: string) => void;
  activateTab: (id: string) => void;
  togglePanel: () => void;
  setPanelTab: (tab: PanelTab) => void;
  setPalette: (mode: ShellState['palette']) => void;
  appendOutput: (line: string) => void;
  clearOutput: () => void;
  setReveal: (reveal: ShellState['reveal']) => void;
}

const FIRST_TAB: EditorTab = {
  id: 'workspace',
  title: '工作台',
  view: 'workspace',
};

export const useShell = create<ShellState>((set, get) => ({
  activity: 'workspace',
  sidebarVisible: true,
  tabs: [FIRST_TAB],
  activeTabId: FIRST_TAB.id,
  panelOpen: false,
  panelTab: 'problems',
  palette: 'closed',
  output: [],
  reveal: null,

  setActivity: (activity) =>
    set((state) => {
      // 点左栏 = 「我要看这一面」：侧栏展开，编辑器同步开到这一面的页上。
      // 只切侧栏不切页，会让同一个动作在窄屏上看起来什么都没发生。
      // 先找「这一面自己的页」，再找「认同一个视图的页」：文件面会为每个打开
      // 的文件开一页（id 是 `file:<路径>`），点左栏时该落到最后打开的那个文件上，
      // 而不是再开一页空的「文件」。
      const existing =
        state.tabs.find((t) => t.id === activity) ??
        state.tabs.find((t) => t.view === activity);
      if (existing) {
        return { activity, sidebarVisible: true, activeTabId: existing.id };
      }
      const tab = tabFor(activity, VIEW_TITLES[activity]);
      return {
        activity,
        sidebarVisible: true,
        tabs: [...state.tabs, tab],
        activeTabId: tab.id,
      };
    }),

  toggleSidebar: () => set((state) => ({ sidebarVisible: !state.sidebarVisible })),

  openTab: (tab) =>
    set((state) => {
      if (state.tabs.some((t) => t.id === tab.id)) {
        return { activeTabId: tab.id, palette: 'closed' };
      }
      return {
        tabs: [...state.tabs, tab],
        activeTabId: tab.id,
        palette: 'closed',
      };
    }),

  closeTab: (id) =>
    set((state) => {
      // 最后一页不关：编辑器空着的时候读者没有落点，而「关掉工作台」不是
      // 任何人想要的结果。关闭只把焦点移到相邻页。
      if (state.tabs.length === 1) {
        return state;
      }
      const index = state.tabs.findIndex((t) => t.id === id);
      const tabs = state.tabs.filter((t) => t.id !== id);
      const activeTabId =
        state.activeTabId === id
          ? tabs[Math.max(0, index - 1)].id
          : state.activeTabId;
      return { tabs, activeTabId };
    }),

  activateTab: (id) => {
    const tab = get().tabs.find((t) => t.id === id);
    if (!tab) return;
    set({ activeTabId: id, activity: tab.view as ActivityId });
  },

  togglePanel: () => set((state) => ({ panelOpen: !state.panelOpen })),
  setPanelTab: (panelTab) => set({ panelTab, panelOpen: true }),
  setPalette: (palette) => set({ palette }),
  appendOutput: (line) =>
    set((state) => ({
      // 输出格是「刚刚发生了什么」的读面，不是历史档案：保留最后 500 行，
      // 足够看到一次失败与它的上下文，又不会让内存随一次长跑无限长。
      output: [...state.output.slice(-499), line],
    })),
  clearOutput: () => set({ output: [] }),
  setReveal: (reveal) => set({ reveal }),
}));

/** 一页页签的身份：视图键就是身份，同一面不会开两页。 */
export function tabFor(view: ActivityId, title: string, hint?: string): EditorTab {
  return { id: view, title, view, hint };
}

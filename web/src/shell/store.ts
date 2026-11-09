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
  | 'people'
  | 'venues'
  | 'files'
  | 'scm'
  | 'inbox'
  | 'agents'
  | 'tasks'
  | 'projects'
  | 'flow'
  | 'comms'
  | 'alerts'
  | 'progress'
  | 'changes'
  | 'governance'
  | 'capabilities'
  | 'output'
  | 'observability'
  | 'token'
  | 'resources'
  | 'storage'
  | 'eval'
  | 'timeline'
  | 'delivery'
  | 'audit'
  | 'settings';

/** 每一面在页签上写什么。切面时要能顺手开出这一页，名字就得在这里认得出。 */
export const VIEW_TITLES: Record<ActivityId, string> = {
  workspace: '工作台',
  people: '员工',
  venues: '场地',
  files: '文件',
  scm: '源代码管理',
  inbox: '收件箱',
  agents: '员工',
  tasks: '任务',
  projects: '项目',
  flow: '流程',
  comms: '通信',
  alerts: '告警',
  progress: '进度',
  changes: '变更',
  governance: '治理',
  capabilities: '能力',
  output: '产出',
  observability: '观测',
  token: 'Token',
  resources: '资源',
  storage: '存储',
  eval: '评测',
  timeline: '时间轴',
  delivery: '构建与部署',
  audit: '审计',
  settings: '设置',
};

/** 底部面板的四格：问题、输出、日志、终端。 */
export type PanelTab = 'problems' | 'output' | 'logs' | 'frames' | 'text';

/** 编辑器里的一页。`view` 是视图注册表里的键，`id` 是这一页自己的身份。 */
export interface EditorTab {
  id: string;
  title: string;
  view: string;
  /** 页标题旁的细小说明，例如「只读」「按 30 秒轮询」。 */
  hint?: string;
}

/** 左栏宽度的上下界：再窄名字就截成两个字，再宽主区就放不下一段对话。 */
export const SIDEBAR_MIN_WIDTH = 190;
export const SIDEBAR_MAX_WIDTH = 640;
export const SIDEBAR_DEFAULT_WIDTH = 262;
const SIDEBAR_STORAGE_KEY = 'cogneva.sidebarWidth';

/** 宽度只在这一台机器的这个浏览器里记着，所以刷新之后不用重新拖一次。 */
function bootSidebarWidth(): number {
  try {
    const raw = window.localStorage.getItem(SIDEBAR_STORAGE_KEY);
    const width = raw ? Number(raw) : NaN;
    if (Number.isFinite(width)) return clampSidebarWidth(width);
  } catch {
    // 隐私模式下面 localStorage 会抛：拖拽照样能用，只是不跨刷新记住。
  }
  return SIDEBAR_DEFAULT_WIDTH;
}

export function clampSidebarWidth(width: number): number {
  return Math.min(SIDEBAR_MAX_WIDTH, Math.max(SIDEBAR_MIN_WIDTH, Math.round(width)));
}

/**
 * 左栏活动项的顺序，由读者拖出来。
 *
 * 记的是「读过的那一份」，不是「全部活动项」：默认顺序与每一项的图标、名字住在
 * `ActivityBar` 里，这里再存一份就是两台真相——一面加了新项，另一份名单不知道。
 * 所以这里只存改过的先后，落不到的项由 [`mergeActivityOrder`] 补在末尾。
 */
const ACTIVITY_ORDER_STORAGE_KEY = 'cogneva.activityOrder';

function bootActivityOrder(): ActivityId[] | null {
  try {
    const raw = window.localStorage.getItem(ACTIVITY_ORDER_STORAGE_KEY);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return null;
    return parsed.filter((id): id is string => typeof id === 'string') as ActivityId[];
  } catch {
    // 隐私模式下面 localStorage 会抛：拖拽照样能用，只是不跨刷新记住。
  }
  return null;
}

/**
 * 把记下的先后叠到这一版声明的名单上。
 *
 * 声明的名单是全集——它决定谁存在；记下的先后只决定谁在前。记下的那些里已经不
 * 存在的项被丢掉（那一面改名或删了），没记过的项按声明顺序补在末尾（这一版新加
 * 的面不能因为读者三周前拖过一次就消失）。
 */
export function mergeActivityOrder(
  saved: readonly ActivityId[] | null,
  declared: readonly ActivityId[]
): ActivityId[] {
  const known = new Set(declared);
  const seen = new Set<ActivityId>();
  const out: ActivityId[] = [];
  for (const id of saved ?? []) {
    if (!known.has(id) || seen.has(id)) continue;
    seen.add(id);
    out.push(id);
  }
  for (const id of declared) {
    if (seen.has(id)) continue;
    out.push(id);
  }
  return out;
}

/** 把 `moving` 挪到 `before` 的位置上。两格相邻时与原顺序一致，不是「换位」。 */
export function moveActivity(
  order: readonly ActivityId[],
  moving: ActivityId,
  before: ActivityId
): ActivityId[] {
  const from = order.indexOf(moving);
  const to = order.indexOf(before);
  if (from < 0 || to < 0 || from === to) return [...order];
  const next = [...order];
  next.splice(from, 1);
  next.splice(to, 0, moving);
  return next;
}

/**
 * 「这个数的来路」那一条抽屉的宽度上下界。再窄一行字折成三截，再宽主区就被挤到
 * 只剩一条缝——它说的是某个数的来路，不该占掉读那个数的地方。
 */
export const DRAWER_MIN_WIDTH = 260;
export const DRAWER_MAX_WIDTH = 720;
export const DRAWER_DEFAULT_WIDTH = 340;
const DRAWER_STORAGE_KEY = 'cogneva.explainDrawerWidth';

export function clampDrawerWidth(width: number): number {
  return Math.min(DRAWER_MAX_WIDTH, Math.max(DRAWER_MIN_WIDTH, Math.round(width)));
}

function bootDrawerWidth(): number {
  try {
    const raw = window.localStorage.getItem(DRAWER_STORAGE_KEY);
    const width = raw ? Number(raw) : NaN;
    if (Number.isFinite(width)) return clampDrawerWidth(width);
  } catch {
    // 同上：记不住只影响下一次打开用默认宽度。
  }
  return DRAWER_DEFAULT_WIDTH;
}

interface ShellState {
  activity: ActivityId;
  sidebarVisible: boolean;
  /** 左栏现在的宽度（像素）。读者拖过它之后就是这个数。 */
  sidebarWidth: number;
  /** 读者拖出来的活动栏先后；`null` 是没拖过，按 `ActivityBar` 声明的顺序。 */
  activityOrder: ActivityId[] | null;
  /** 「来路」抽屉现在的宽度（像素）。 */
  drawerWidth: number;
  tabs: EditorTab[];
  activeTabId: string;
  panelOpen: boolean;
  panelTab: PanelTab;
  /**
   * 命令面板、快速打开、帮助共用一个浮层，`mode` 决定它问的是什么。
   * `help` 是「这个界面去哪找什么」：问号按钮打开它，答案是一条条能走到的去处，
   * 而不是一段说明文字——说明书和界面一旦分开，界面改版时说明书永远落后。
   */
  palette: 'closed' | 'commands' | 'open' | 'help';
  /**
   * 打开面板时预填的查询。问号按钮与「这一页有哪些动作」这类入口要落到一个
   * 已经缩小过的列表上——面板打开后一片空白，读者还得自己想搜什么。
   */
  paletteSeed: string;
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
  setSidebarWidth: (width: number) => void;
  setActivityOrder: (order: ActivityId[]) => void;
  setDrawerWidth: (width: number) => void;
  /** 把一页挪到另一页前面。相邻挪动等于不动。 */
  moveTab: (moving: string, before: string) => void;
  openTab: (tab: EditorTab) => void;
  closeTab: (id: string) => void;
  activateTab: (id: string) => void;
  togglePanel: () => void;
  setPanelTab: (tab: PanelTab) => void;
  setPalette: (mode: ShellState['palette'], seed?: string) => void;
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
  sidebarWidth: bootSidebarWidth(),
  activityOrder: bootActivityOrder(),
  drawerWidth: bootDrawerWidth(),
  tabs: [FIRST_TAB],
  activeTabId: FIRST_TAB.id,
  panelOpen: false,
  panelTab: 'problems',
  palette: 'closed',
  paletteSeed: '',
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

  setSidebarWidth: (width) => {
    const clamped = clampSidebarWidth(width);
    set({ sidebarWidth: clamped });
    try {
      window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(clamped));
    } catch {
      // 记不住不改变这一屏的宽度，只是下一次打开回到默认。
    }
  },

  setActivityOrder: (order) => {
    set({ activityOrder: order });
    try {
      window.localStorage.setItem(ACTIVITY_ORDER_STORAGE_KEY, JSON.stringify(order));
    } catch {
      // 记不住不改变这一屏的顺序，只是下一次打开回到声明顺序。
    }
  },

  setDrawerWidth: (width) => {
    const clamped = clampDrawerWidth(width);
    set({ drawerWidth: clamped });
    try {
      window.localStorage.setItem(DRAWER_STORAGE_KEY, String(clamped));
    } catch {
      // 同上：记不住只影响下一次打开用默认宽度。
    }
  },

  moveTab: (moving, before) =>
    set((state) => {
      const from = state.tabs.findIndex((t) => t.id === moving);
      const to = state.tabs.findIndex((t) => t.id === before);
      if (from < 0 || to < 0 || from === to) return state;
      const tabs = [...state.tabs];
      const [tab] = tabs.splice(from, 1);
      tabs.splice(to, 0, tab);
      return { tabs };
    }),

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
  setPalette: (palette, seed = '') => set({ palette, paletteSeed: seed }),
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

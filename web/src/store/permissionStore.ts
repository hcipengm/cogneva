import { create } from 'zustand';

/**
 * 等着人答的「要不要动手」。
 *
 * 一条询问按请求号寻址，不按会话：同一段对话里可以有两问同时在等（用户连发两
 * 句），按会话答会把答案交给错的那一个。所以这一页的账是「一行一个问题」，而
 * 每一段对话的窗口只显示属于自己那一段的那些。
 */
export interface PermissionAsk {
  /** 服务端生成的请求号，答案按它回去。 */
  id: string;
  sessionId: string;
  /** 它要动的是什么工具。 */
  tool: string;
  /** 它要拿什么动手——这一行是人真正要看的那一格。 */
  summary: string;
  /** 这一问发生在哪一格模式下。 */
  mode: string;
}

/**
 * 一条留下来的规则：人点过「总是」的那些。
 *
 * 五格都照服务端递来的原样存着：哪个工具、碰什么、放还是拦、管这一段还是管这个人、
 * 撤销时按哪个号。界面不解释「同类」是什么意思——那是评估规则的那一侧的事，界面
 * 只把规则本来的样子摆出来。
 */
export interface PermissionRule {
  id: string;
  tool: string;
  subject?: string;
  allow: boolean;
  /** `session` 或 `owner`。 */
  scope: string;
}

interface PermissionState {
  asks: PermissionAsk[];
  /** 每一段对话当前那一格模式；没声明过的不在这里，读作 default。 */
  modes: Record<string, string>;
  /**
   * 这个人手上留下来的规则。
   *
   * `null` 是「还没读到」，空数组是「读到了，一条也没有」。两者在界面上是两句不同
   * 的话：前者说「还在读」，后者说「你还没有留下过规则」。合成一个空数组，读者会
   * 把一次没连上的读取当成一次干净的家底。
   */
  rules: PermissionRule[] | null;
  ask: (ask: PermissionAsk) => void;
  /** 这一问已经有答案了，从账上摘掉。摘得早不早由服务端说了算：答案没送到时
      服务端那一轮仍然是拒绝，屏幕上也就不该继续挂着这一问。 */
  answered: (id: string) => void;
  setMode: (sessionId: string, mode: string) => void;
  modeOf: (sessionId: string) => string;
  setRules: (rules: PermissionRule[]) => void;
  /** 断开连接时把规则清回「还没读到」：上一条连接的名单不该留在屏幕上冒充现状。 */
  forgetRules: () => void;
}

export const DEFAULT_PERMISSION_MODE = 'default';

export const usePermissionStore = create<PermissionState>((set, get) => ({
  asks: [],
  modes: {},
  ask: (ask) =>
    set((state) => ({
      asks: state.asks.some((a) => a.id === ask.id) ? state.asks : [...state.asks, ask],
    })),
  answered: (id) => set((state) => ({ asks: state.asks.filter((a) => a.id !== id) })),
  setMode: (sessionId, mode) =>
    set((state) => ({ modes: { ...state.modes, [sessionId]: mode } })),
  modeOf: (sessionId) => get().modes[sessionId] ?? DEFAULT_PERMISSION_MODE,
  rules: null,
  setRules: (rules) => set({ rules }),
  forgetRules: () => set({ rules: null }),
}));

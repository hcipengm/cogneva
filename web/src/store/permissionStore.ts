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

interface PermissionState {
  asks: PermissionAsk[];
  /** 每一段对话当前那一格模式；没声明过的不在这里，读作 default。 */
  modes: Record<string, string>;
  ask: (ask: PermissionAsk) => void;
  /** 这一问已经有答案了，从账上摘掉。摘得早不早由服务端说了算：答案没送到时
      服务端那一轮仍然是拒绝，屏幕上也就不该继续挂着这一问。 */
  answered: (id: string) => void;
  setMode: (sessionId: string, mode: string) => void;
  modeOf: (sessionId: string) => string;
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
}));

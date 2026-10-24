import { create } from 'zustand';
import type { AgentEvent } from '@/types/events';

export type MessageRole = 'user' | 'assistant' | 'system';

/** 活动流保留的行数。连着看的是「刚才发生了什么」，不是审计——翻更早的用任务与 trace。 */
export const ACTIVITY_LIMIT = 300;

/**
 * 一条活动：谁，在什么时候，做了什么。
 *
 * `detail` 留着原始参数或结果的一小段，因为「调用了 write_file」与「写了哪个
 * 文件的哪一段」是同一个动作的两个层次，只留前者的面板看着像进度条。
 */
export interface ActivityEntry {
  id: string;
  at: string;
  type: string;
  agent: string | null;
  summary: string;
  detail: string | null;
  failed: boolean;
}

/** 一段 JSON 的紧凑预览，超长截断——活动流一行放不下整份工具输出。 */
function preview(value: unknown, max = 180): string | null {
  if (value === undefined || value === null) return null;
  let text: string;
  try {
    text = typeof value === 'string' ? value : JSON.stringify(value);
  } catch {
    text = String(value);
  }
  if (!text) return null;
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : preview(value, 80) ?? '';
}

/**
 * 事件 → 一行活动。
 *
 * 认识的变体各自写清在做什么；不认识的也留一行。丢掉不认识的帧是界面最容易
 * 犯的错：服务端加了一个新事件，界面看上去「什么都没发生」，而事实上发生了。
 */
export function describeEvent(event: AgentEvent): ActivityEntry {
  const p = event.payload as Record<string, unknown>;
  const agent = typeof p.agent_id === 'string' ? p.agent_id : null;
  const base = { id: event.id, at: event.timestamp, type: event.type, agent };
  switch (event.type) {
    case 'tool_execution_start':
      return {
        ...base,
        summary: `调用工具 ${text(p.tool_name)}`,
        detail: preview(p.args),
        failed: false,
      };
    case 'tool_execution_end':
      return {
        ...base,
        summary: `工具 ${text(p.tool_name) || text(p.tool_call_id)} ${p.is_error ? '失败' : '返回'}`,
        detail: preview(p.result),
        failed: Boolean(p.is_error),
      };
    case 'react_step_start':
      return { ...base, summary: `第 ${text(p.iteration)} 轮思考`, detail: null, failed: false };
    case 'react_step_end':
      return {
        ...base,
        summary: `第 ${text(p.iteration)} 轮：${preview(p.thought, 120) ?? '完成'}`,
        detail: Array.isArray(p.tool_calls) && p.tool_calls.length > 0
          ? preview(p.tool_calls)
          : null,
        failed: false,
      };
    case 'state_change':
      return { ...base, summary: `状态 ${text(p.from)} → ${text(p.to)}`, detail: null, failed: false };
    case 'task_status_change':
      // 钩子事件借同一个变体过总线（`status` 上写着 `hook_event`），它们不是
      // 任务在换状态，印成「任务 X 变为 hook_event」会读成一次虚假的状态迁移。
      if (p.status === 'hook_event') {
        return {
          ...base,
          summary: `钩子事件（任务 ${text(p.task_id)}）`,
          detail: null,
          failed: false,
        };
      }
      return {
        ...base,
        summary: `任务 ${text(p.task_id)} 变为 ${text(p.status)}`,
        detail: null,
        failed: p.status === 'failed',
      };
    case 'agent_start':
      return { ...base, summary: '开始一次运行', detail: null, failed: false };
    case 'agent_end':
      return {
        ...base,
        summary: '运行结束',
        detail: Array.isArray(p.messages) ? `${p.messages.length} 条消息` : null,
        failed: false,
      };
    case 'agent_error':
      return {
        ...base,
        summary: `错误 ${text(p.error_code)}`,
        detail: preview(p.details),
        failed: true,
      };
    case 'self_review':
      return {
        ...base,
        summary: `自评 ${text(p.status)}（${text(p.score)}）`,
        detail: preview(p.summary ?? p.critique),
        failed: p.status !== 'PASS',
      };
    case 'resource_alert':
      return {
        ...base,
        summary: `资源 ${text(p.metric)} 到达 ${text(p.current)}（阈值 ${text(p.threshold)}）`,
        detail: null,
        failed: false,
      };
    case 'message_start':
      return { ...base, summary: '开始回话', detail: null, failed: false };
    case 'message_end':
      return { ...base, summary: '回话结束', detail: preview(p.message), failed: false };
    case 'heartbeat':
    case 'checkpoint_saved':
    case 'message_update':
    case 'turn_start':
    case 'turn_end':
      return { ...base, summary: event.type, detail: null, failed: false };
    default:
      return { ...base, summary: event.type, detail: preview(p), failed: false };
  }
}

export interface Message {
  id: string;
  role: MessageRole;
  text: string;
  status: 'streaming' | 'done' | 'error';
}

export interface TaskProgress {
  taskId: string;
  progress: number;
  step: string;
}

export interface StreamState {
  messages: Message[];
  taskProgress: TaskProgress | null;
  connectionStatus: 'connecting' | 'open' | 'closed' | 'error';
  /**
   * 最近活动，新的在前。
   *
   * 它是「总线上真的来了什么」的唯一去处：每个认识的事件都进来，不认识的也进来
   * 一行。面板据此判断平台是不是在干活——不是看有没有任务在跑，而是看有没有
   * 工具在调。
   */
  activity: ActivityEntry[];
  recordActivity: (entry: ActivityEntry) => void;
  clearActivity: () => void;
  startMessage: (id: string, role: MessageRole) => void;
  appendDelta: (messageId: string, delta: string) => void;
  endMessage: (messageId: string) => void;
  /**
   * 收尾时用整条消息补一次正文。
   *
   * 只有助手的话是逐段流出来的；用户发的、工具回的都是整条到达。不收尾时
   * 补一次，画布上那条就是空的——读者会以为这句话没说出来。
   */
  finishMessage: (messageId: string, fullText: string) => void;
  setTaskProgress: (progress: TaskProgress) => void;
  setConnectionStatus: (status: StreamState['connectionStatus']) => void;
}

export const useStreamStore = create<StreamState>((set) => ({
  messages: [],
  taskProgress: null,
  connectionStatus: 'connecting',
  activity: [],

  recordActivity: (entry) =>
    set((state) => {
      // 同一 id 只留一行：服务端重连后会把窗口内的帧重放一遍，重复的
      // 「调用工具」看着像平台在重复干活。
      if (state.activity.some((a) => a.id === entry.id)) return state;
      const next = [entry, ...state.activity];
      return { activity: next.length > ACTIVITY_LIMIT ? next.slice(0, ACTIVITY_LIMIT) : next };
    }),

  clearActivity: () => set({ activity: [] }),

  startMessage: (id, role) =>
    set((state) => ({
      messages: [
        ...state.messages,
        { id, role, text: '', status: 'streaming' },
      ],
    })),

  appendDelta: (messageId, delta) =>
    set((state) => ({
      messages: state.messages.map((message) =>
        message.id === messageId
          ? { ...message, text: message.text + delta }
          : message
      ),
    })),

  endMessage: (messageId) =>
    set((state) => ({
      messages: state.messages.map((message) =>
        message.id === messageId
          ? { ...message, status: 'done' }
          : message
      ),
    })),

  finishMessage: (messageId, fullText) =>
    set((state) => ({
      messages: state.messages.map((message) =>
        message.id === messageId
          ? { ...message, text: message.text || fullText, status: 'done' }
          : message
      ),
    })),

  setTaskProgress: (progress) => set({ taskProgress: progress }),

  setConnectionStatus: (status) => set({ connectionStatus: status }),
}));

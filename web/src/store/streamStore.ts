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

/**
 * 一帧能落进去的三处。
 *
 * 三处答的不是同一个问题：「帧流」是总线上来了什么，「时间轴」是这些事按时间排开
 * 长什么样，「态」是到这一刻为止平台被折成了什么样子。一条帧只进其中一两处是常态，
 * 但哪几处被它改过必须看得见——读者才能分清「这帧没影响任何状态」与「这帧根本没被
 * 认出来」。
 */
export type FrameSink = 'stream' | 'timeline' | 'state';

export const SINK_LABELS: Record<FrameSink, string> = {
  stream: '帧流',
  timeline: '时间轴',
  state: '态',
};

/** 时间轴保留的点数。与活动流同一个量级：它记的也是「刚才」，不是审计。 */
export const TIMELINE_LIMIT = 300;

/** 时间轴上的一个点：一行活动，外加这一帧真的落进了哪几处。 */
export interface TimelinePoint extends ActivityEntry {
  fed: FrameSink[];
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
    case 'tool_execution_update':
      // 工具跑起来之后的中途输出：它不是「又调了一次」，印成一次新的调用会让
      // 一次长工具在活动流里叠出十几行一模一样的开头。
      return {
        ...base,
        summary: `工具 ${text(p.tool_name) || text(p.tool_call_id)} 输出中`,
        detail: preview(p.partial_result ?? p.chunk ?? p.delta ?? p.output),
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
    case 'resource_changed':
      // 一次成功的写落在哪个前缀下。名字是 REST 的前缀，不是界面上的面名：
      // 一帧只说事实，怎么叫是各面自己的事。
      return {
        ...base,
        summary: `${text(p.domain) || '某个面'}上有东西被改了`,
        detail: null,
        failed: false,
      };
    default:
      return { ...base, summary: event.type, detail: preview(p), failed: false };
  }
}

export interface Message {
  id: string;
  /** 谁在说。总线上的消息按 agent 分，少了这一列就没法把它归到某一次运行。 */
  agent: string | null;
  role: MessageRole;
  text: string;
  status: 'streaming' | 'done' | 'error';
}

/** 原文保留条数。面板是「刚才流过来了什么」，不是转录存档。 */
export const MESSAGE_LIMIT = 200;

/**
 * 一条任务在帧里的状态。
 *
 * 它是轮询读数的补丁，不是第二本账：轮询每十几秒给一次快照，帧在快照之间到，
 * 读者看的是「比快照新的那一个」。所以带着到达时刻，读的时候比新旧。
 */
export interface LiveTaskStatus {
  taskId: string;
  status: string;
  at: string;
}

/** 帧里记住的任务状态条数。超过就丢最老的，没人会翻几百条前的状态变迁。 */
export const LIVE_TASK_LIMIT = 200;

/**
 * 一次工具调用，按调用身份收成一格。
 *
 * 活动流里那是三行（开始 / 输出中 / 返回），一行 180 字——够回答「在干什么」，
 * 不够回答「它到底传了什么、回了什么」。而一次调用真正能被人复核的就是这两段
 * 原文：参数说它想做什么，结果说它做成了没有。所以这里按 `tool_call_id` 收成
 * 一格，参数与结果都留着，只在超过 `TOOL_TEXT_CAP` 时截断并写明截了。
 */
export interface ToolCallRecord {
  id: string;
  agent: string | null;
  name: string;
  /** 入参原文（紧凑 JSON）。 */
  args: string;
  /** 返回原文。跑到一半时是空串，不是「返回了空」。 */
  result: string;
  /** 中途输出，按到达顺序拼起来；只有这一个工具真的报过进度时才有。 */
  progress: string;
  status: 'running' | 'ok' | 'error';
  startedAt: string;
  endedAt: string | null;
}

/** 留多少格。与活动流同量级：读者翻的是「这一轮调了什么」，不是审计。 */
export const TOOL_CALL_LIMIT = 120;

/** 参数 / 结果各自留多少字符。超过就截断并在原文末尾写明，不假装是全份。 */
export const TOOL_TEXT_CAP = 8000;

function toolText(value: unknown): string {
  if (value === undefined || value === null) return '';
  let out: string;
  try {
    out = typeof value === 'string' ? value : JSON.stringify(value) ?? '';
  } catch {
    out = String(value);
  }
  return out.length > TOOL_TEXT_CAP
    ? `${out.slice(0, TOOL_TEXT_CAP)}\n…（共 ${out.length} 字符，只留了前 ${TOOL_TEXT_CAP}）`
    : out;
}

export interface StreamState {
  messages: Message[];
  /**
   * 帧里读到的任务状态，新的在前，一条任务只留最新那一次。
   *
   * 任务面与流程面拿它去补轮询快照：快照说 `pending`、帧刚说 `running`，画面上
   * 就该是 `running`。没有这一列的时候，`task_status_change` 每一条都到了、都进了
   * 活动流，而所有任务视图仍停在最多十几秒前的快照上。
   */
  liveTasks: LiveTaskStatus[];
  connectionStatus: 'connecting' | 'open' | 'closed' | 'error';
  /**
   * 最近活动，新的在前。
   *
   * 它是「总线上真的来了什么」的唯一去处：每个认识的事件都进来，不认识的也进来
   * 一行。面板据此判断平台是不是在干活——不是看有没有任务在跑，而是看有没有
   * 工具在调。
   */
  activity: ActivityEntry[];
  /**
   * 进了账的每一帧，新的在前，各带「它落进了哪几处」。
   *
   * 与活动流是同一条帧的两种读法：活动流按「做了什么」排，这一列按「这一帧改了什么」
   * 排。少了它，「这帧只记了一行、没改任何状态」与「这帧认出来了」在画面上是同一件事。
   */
  timeline: TimelinePoint[];
  /**
   * 按构造不入账的帧，按类型计数。
   *
   * 正文增量每 token 一帧、心跳每 agent 一次，逐条入账会把上面两列一起冲干净。
   * 不计的话，它们的缺席就变成「总线上从没有过这些帧」——那是在替数据说谎。
   */
  unrecorded: Record<string, number>;
  /**
   * 工具调用，新的在前。
   *
   * 与活动流分开：活动流回答「刚才发生了什么」，这里回答「那一次调用到底传了
   * 什么、回了什么」。合成一格的话，要么活动流里每行都拖着一份长原文，要么
   * 这里只剩一行摘要。
   */
  toolCalls: ToolCallRecord[];
  /** 记一次工具调用的进展；同一次调用（同 id）收在同一格里。 */
  noteToolCall: (record: {
    id: string;
    agent: string | null;
    name: string;
    args?: unknown;
    result?: unknown;
    progress?: string;
    status: ToolCallRecord['status'];
    at: string;
  }) => void;
  recordActivity: (entry: ActivityEntry) => void;
  clearActivity: () => void;
  noteFrame: (point: TimelinePoint) => void;
  noteUnrecorded: (type: string) => void;
  clearTimeline: () => void;
  startMessage: (id: string, agent: string | null, role: MessageRole) => void;
  appendDelta: (messageId: string, delta: string) => void;
  endMessage: (messageId: string) => void;
  /**
   * 收尾时用整条消息补一次正文。
   *
   * 只有助手的话是逐段流出来的；用户发的、工具回的都是整条到达。不收尾时
   * 补一次，原文格里那条就是空的——读者会以为这句话没说出来。
   */
  finishMessage: (messageId: string, fullText: string) => void;
  noteTaskStatus: (entry: LiveTaskStatus) => void;
  setConnectionStatus: (status: StreamState['connectionStatus']) => void;
}

/** 帧里那一列折成按任务 id 取的表。折叠只有一处，读者不必各自建一遍。 */
export function liveIndex(rows: LiveTaskStatus[]): Record<string, LiveTaskStatus> {
  const index: Record<string, LiveTaskStatus> = {};
  for (const row of rows) index[row.taskId] = row;
  return index;
}

/**
 * 快照与帧之间取新的那个。
 *
 * 只有帧确实晚于这一行快照的更新时间才算数：重连后服务端会把窗口内的帧重放一遍，
 * 一串旧帧若盖在刚读到的快照上，画面就会从 `running` 倒退回 `pending`——一次
 * 从未发生的回退，比慢十几秒更坏。
 */
export function frameIsNewer(frame: LiveTaskStatus | undefined, snapshotAt: string): boolean {
  if (!frame) return false;
  const frameAt = Date.parse(frame.at);
  if (!Number.isFinite(frameAt)) return false;
  const snapshot = Date.parse(snapshotAt);
  if (!Number.isFinite(snapshot)) return true;
  return frameAt > snapshot;
}

export function effectiveStatus(
  row: { id: string; status: string; updated_at: string },
  live: Record<string, LiveTaskStatus>
): { status: string; fromFrame: boolean } {
  const frame = live[row.id];
  if (!frame || !frameIsNewer(frame, row.updated_at)) {
    return { status: row.status, fromFrame: false };
  }
  return { status: frame.status, fromFrame: true };
}

export const useStreamStore = create<StreamState>((set) => ({
  messages: [],
  liveTasks: [],
  connectionStatus: 'connecting',
  activity: [],
  timeline: [],
  unrecorded: {},
  toolCalls: [],

  noteToolCall: (record) =>
    set((state) => {
      const index = state.toolCalls.findIndex((call) => call.id === record.id);
      const base =
        index >= 0
          ? state.toolCalls[index]
          : {
              id: record.id,
              agent: record.agent,
              name: record.name,
              args: '',
              result: '',
              progress: '',
              status: 'running' as const,
              startedAt: record.at,
              endedAt: null,
            };
      const merged: ToolCallRecord = {
        ...base,
        status: record.status,
        args: record.args === undefined ? base.args : toolText(record.args),
        result: record.result === undefined ? base.result : toolText(record.result),
        progress: record.progress ? `${base.progress}${record.progress}`.slice(-TOOL_TEXT_CAP) : base.progress,
        endedAt: record.status === 'running' ? base.endedAt : record.at,
      };
      const rest = state.toolCalls.filter((call) => call.id !== record.id);
      const next = [merged, ...rest];
      return { toolCalls: next.length > TOOL_CALL_LIMIT ? next.slice(0, TOOL_CALL_LIMIT) : next };
    }),

  recordActivity: (entry) =>
    set((state) => {
      // 同一 id 只留一行：服务端重连后会把窗口内的帧重放一遍，重复的
      // 「调用工具」看着像平台在重复干活。
      if (state.activity.some((a) => a.id === entry.id)) return state;
      const next = [entry, ...state.activity];
      return { activity: next.length > ACTIVITY_LIMIT ? next.slice(0, ACTIVITY_LIMIT) : next };
    }),

  clearActivity: () => set({ activity: [] }),

  noteFrame: (point) =>
    set((state) => {
      // 与活动流同一条去重：重连时服务端会把窗口内的帧重放一遍，同一个点上
      // 出现两次「调用工具」像是平台在重复干活。
      if (state.timeline.some((p) => p.id === point.id)) return state;
      const next = [point, ...state.timeline];
      return { timeline: next.length > TIMELINE_LIMIT ? next.slice(0, TIMELINE_LIMIT) : next };
    }),

  noteUnrecorded: (type) =>
    set((state) => ({ unrecorded: { ...state.unrecorded, [type]: (state.unrecorded[type] ?? 0) + 1 } })),

  clearTimeline: () => set({ timeline: [], unrecorded: {} }),

  startMessage: (id, agent, role) =>
    set((state) => {
      const next = [
        ...state.messages,
        { id, agent, role, text: '', status: 'streaming' as const },
      ];
      return { messages: next.length > MESSAGE_LIMIT ? next.slice(-MESSAGE_LIMIT) : next };
    }),

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

  noteTaskStatus: (entry) =>
    set((state) => {
      const rest = state.liveTasks.filter((row) => row.taskId !== entry.taskId);
      const next = [entry, ...rest];
      return { liveTasks: next.length > LIVE_TASK_LIMIT ? next.slice(0, LIVE_TASK_LIMIT) : next };
    }),

  setConnectionStatus: (status) => set({ connectionStatus: status }),
}));

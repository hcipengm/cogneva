/**
 * 事件的名字以服务端为准。
 *
 * 服务端把 `AgentEvent` 以变体自己的 snake_case 名字直接写在 `type` 上
 * （`tool_execution_start`、`state_change`……）。这里曾经记的是一套界面自己
 * 编的名字（`tool_call.start`），于是客户端只认三种名字、其余全部静默丢弃——
 * 事件一直在总线上流，界面却只看得见自己编的那几个。名字多写一个都对不上，
 * 所以这张表和 Rust 侧的 `event_type_name` 一一对应。
 */
export type EventType =
  | 'agent_start'
  | 'agent_end'
  | 'turn_start'
  | 'turn_end'
  | 'message_start'
  | 'message_update'
  | 'message_end'
  | 'tool_execution_start'
  | 'tool_execution_update'
  | 'tool_execution_end'
  | 'state_change'
  | 'task_status_change'
  | 'self_review'
  | 'react_step_start'
  | 'react_step_end'
  | 'agent_error'
  | 'resource_alert'
  | 'heartbeat'
  | 'checkpoint_saved'
  | 'resource_changed';

const KNOWN_TYPES: ReadonlySet<string> = new Set<EventType>([
  'agent_start',
  'agent_end',
  'turn_start',
  'turn_end',
  'message_start',
  'message_update',
  'message_end',
  'tool_execution_start',
  'tool_execution_update',
  'tool_execution_end',
  'state_change',
  'task_status_change',
  'self_review',
  'react_step_start',
  'react_step_end',
  'agent_error',
  'resource_alert',
  'heartbeat',
  'checkpoint_saved',
  'resource_changed',
]);

export function isEventType(type: unknown): type is EventType {
  return typeof type === 'string' && KNOWN_TYPES.has(type);
}

/**
 * 一条事件。`payload` 是事件本身：服务端把变体的字段平铺在 `type` 旁边，
 * 没有第二层信封。
 */
export interface AgentEvent {
  id: string;
  type: EventType;
  timestamp: string;
  payload: Record<string, unknown>;
}

export interface ToolExecutionStartFields {
  agent_id: string;
  tool_call_id: string;
  tool_name: string;
  args: unknown;
}

export interface ToolExecutionEndFields {
  agent_id: string;
  tool_call_id: string;
  result: unknown;
  is_error: boolean;
}

export interface StateChangeFields {
  agent_id: string;
  from: string;
  to: string;
}

export interface TaskStatusChangeFields {
  task_id: string;
  status: string;
  agent_id?: string | null;
}

export interface AgentErrorFields {
  agent_id: string;
  error_code: string;
  details: string;
  severity?: string;
}

export interface SelfReviewFields {
  agent_id: string;
  status: string;
  score: number;
  summary?: string | null;
  critique?: string | null;
}

export interface ReActStepEndFields {
  agent_id: string;
  iteration: number;
  thought: string;
  tool_calls: unknown[];
  observations: unknown[];
}

export interface ResourceAlertFields {
  agent_id: string;
  metric: string;
  threshold: number;
  current: number;
}

export interface MessageStartFields {
  agent_id: string;
  message?: { id?: string; role?: string };
}

export interface MessageEndFields {
  agent_id: string;
  message?: { id?: string; role?: string; content?: unknown };
}

/**
 * 聊天流自己的帧族。名字里带点，和上面那套 agent 事件不是一张表。
 *
 * 服务端按 `message.start` / `message.text_delta` / `message.end` 发一回合的
 * 每一段（`crates/cog-gateway/src/chat.rs` 的 `envelope`），外层仍然是
 * `agent_event` 信封，内层才是这一族。把它们塞进 `KNOWN_TYPES` 是不对的：
 * 那张表与 Rust 侧 `event_type_name` 一一对应，而聊天帧不是 `AgentEvent`
 * 的变体。它们曾经因此被整族丢掉——回复一直在总线上，屏幕上一条也没有。
 */
export type ChatFrameKind =
  | 'start'
  | 'text_delta'
  | 'end'
  | 'thinking_start'
  | 'thinking_delta'
  | 'thinking_end';

/** 帧名 → 这一帧在做什么。不在这张表里的名字不是聊天帧。 */
const CHAT_FRAME_KINDS: Record<string, ChatFrameKind> = {
  'message.start': 'start',
  'message.text_delta': 'text_delta',
  'message.end': 'end',
  'message.thinking_start': 'thinking_start',
  'message.thinking_delta': 'thinking_delta',
  'message.thinking_end': 'thinking_end',
};

export interface ChatFrame {
  id: string;
  /** 这段对话的身份。多段对话各有一条流，帧必须带着它自己的那一条。 */
  sessionId: string;
  /** 一条消息的身份。同一条消息的第一帧是 `start`，之后按它聚合。 */
  messageId: string;
  kind: ChatFrameKind;
  /** 只有 `start` 带着角色；其余帧从这条消息已有的角色继承。 */
  role: 'user' | 'assistant' | 'system' | null;
  /** 只有 `*_delta` 带着增量。 */
  delta: string | null;
  timestamp: string;
}

/**
 * 一段聊天帧 → 界面认得的形状。认不出返回 `null`。
 *
 * `sessionId` 从信封上取而不是从帧体里取：帧体里没有它，而一段没有会话身份
 * 的消息落不进任何一段对话——它会被并到默认会话里，读者看到自己的话出现在
 * 别人的窗口里。
 */
export function normalizeChatFrame(
  payload: Record<string, unknown>,
  sessionId: string | undefined,
  fallbackId: string
): ChatFrame | null {
  const rawType = payload.type;
  if (typeof rawType !== 'string') return null;
  const kind = CHAT_FRAME_KINDS[rawType];
  if (!kind) return null;

  const inner = (payload.payload as Record<string, unknown> | undefined) ?? payload;
  const messageId = typeof inner.message_id === 'string' ? inner.message_id : null;
  if (!messageId) return null;

  const role = inner.role;
  return {
    id: (payload.id as string | undefined) ?? fallbackId,
    sessionId: sessionId && sessionId.trim() ? sessionId : 'default',
    messageId,
    kind,
    role:
      role === 'user' || role === 'assistant' || role === 'system' ? role : null,
    delta: typeof inner.delta === 'string' ? inner.delta : null,
    timestamp:
      (payload.timestamp as string | undefined) ?? new Date().toISOString(),
  };
}

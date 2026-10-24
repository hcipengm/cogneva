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
  | 'checkpoint_saved';

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

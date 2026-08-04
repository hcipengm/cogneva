export type EventType =
  | 'message.start'
  | 'message.text_delta'
  | 'message.end'
  | 'tool_call.start'
  | 'tool_call.end'
  | 'task.progress'
  | 'agent.state_change'
  | 'agent.error'
  | 'system.heartbeat';

export interface BaseAgentEvent {
  id: string;
  type: EventType;
  timestamp: string;
  payload: Record<string, unknown>;
}

export interface MessageStartEvent extends BaseAgentEvent {
  type: 'message.start';
  payload: {
    message_id: string;
    role: 'user' | 'assistant' | 'system';
  };
}

export interface TextDeltaEvent extends BaseAgentEvent {
  type: 'message.text_delta';
  payload: {
    message_id: string;
    delta: string;
  };
}

export interface MessageEndEvent extends BaseAgentEvent {
  type: 'message.end';
  payload: {
    message_id: string;
  };
}

export interface TaskProgressEvent extends BaseAgentEvent {
  type: 'task.progress';
  payload: {
    task_id: string;
    progress: number;
    step: string;
  };
}

export interface ToolCallEvent extends BaseAgentEvent {
  type: 'tool_call.start' | 'tool_call.end';
  payload: {
    tool_name: string;
    params?: Record<string, unknown>;
    result?: unknown;
  };
}

export type AgentEvent =
  | MessageStartEvent
  | TextDeltaEvent
  | MessageEndEvent
  | TaskProgressEvent
  | ToolCallEvent
  | BaseAgentEvent;

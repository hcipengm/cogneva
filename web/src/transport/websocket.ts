import type { AgentEvent, EventType } from '@/types/events';

export type ConnectionStatus = 'connecting' | 'open' | 'closed' | 'error';

export interface WebSocketOptions {
  url: string;
  channels?: string[];
  reconnectIntervalMs?: number;
  maxReconnectAttempts?: number;
  onEvent: (event: AgentEvent) => void;
  onStatusChange?: (status: ConnectionStatus) => void;
  onError?: (error: Error) => void;
}

export interface ServerMessage {
  type: string;
  connection_id?: string;
  server_time?: string;
  missed_events?: string[];
  event_id?: string;
  session_id?: string;
  task_id?: string;
  payload?: Record<string, unknown>;
  code?: string;
  message?: string;
}

export class EventStreamClient {
  private ws: WebSocket | null = null;
  private options: WebSocketOptions;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;

  constructor(options: WebSocketOptions) {
    this.options = {
      reconnectIntervalMs: 3000,
      maxReconnectAttempts: 10,
      channels: [],
      ...options,
    };
  }

  connect(): void {
    this.setStatus('connecting');

    try {
      this.ws = new WebSocket(this.options.url);
    } catch (error) {
      this.handleError(error instanceof Error ? error : new Error(String(error)));
      return;
    }

    this.ws.onopen = () => {
      this.reconnectAttempts = 0;
      this.setStatus('open');
      this.subscribe();
      this.startHeartbeat();
    };

    this.ws.onmessage = (event) => {
      this.handleMessage(event.data);
    };

    this.ws.onclose = () => {
      this.stopHeartbeat();
      this.setStatus('closed');
      this.scheduleReconnect();
    };

    this.ws.onerror = () => {
      this.setStatus('error');
    };
  }

  disconnect(): void {
    this.stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.close();
      this.ws = null;
    }
  }

  send(message: unknown): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }

  ping(seq: number): void {
    this.send({ type: 'ping', seq, timestamp: new Date().toISOString() });
  }

  subscribe(): void {
    if (!this.options.channels || this.options.channels.length === 0) return;
    this.send({
      type: 'subscribe',
      channels: this.options.channels,
      timestamp: new Date().toISOString(),
    });
  }

  typing(sessionId: string, isTyping: boolean): void {
    this.send({
      type: 'typing',
      session_id: sessionId,
      is_typing: isTyping,
      timestamp: new Date().toISOString(),
    });
  }

  private setStatus(status: ConnectionStatus): void {
    this.options.onStatusChange?.(status);
  }

  private handleMessage(data: unknown): void {
    if (typeof data !== 'string') return;

    let message: ServerMessage;
    try {
      message = JSON.parse(data) as ServerMessage;
    } catch {
      return;
    }

    if (message.type === 'agent_event' && message.payload) {
      const event = normalizeAgentEvent(message.payload, message.event_id ?? message.payload.id as string);
      if (event) {
        this.options.onEvent(event);
      }
    } else if (message.type === 'task_update' && message.payload) {
      const event = normalizeAgentEvent(message.payload, message.event_id ?? message.payload.id as string);
      if (event) {
        this.options.onEvent(event);
      }
    } else if (message.type === 'notification' && message.payload) {
      const event = normalizeAgentEvent(message.payload, message.event_id ?? message.payload.id as string);
      if (event) {
        this.options.onEvent(event);
      }
    }
  }

  private handleError(error: Error): void {
    this.setStatus('error');
    this.options.onError?.(error);
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    const { reconnectIntervalMs, maxReconnectAttempts } = this.options;
    if (!reconnectIntervalMs || !maxReconnectAttempts) return;
    if (this.reconnectAttempts >= maxReconnectAttempts) return;

    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, reconnectIntervalMs * Math.min(this.reconnectAttempts, 5));
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    let seq = 0;
    this.pingTimer = setInterval(() => {
      this.ping(seq++);
    }, 30000);
  }

  private stopHeartbeat(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
}

function normalizeAgentEvent(
  payload: Record<string, unknown>,
  fallbackId: string
): AgentEvent | null {
  const type = payload.type as EventType | undefined;
  if (!type) return null;

  return {
    id: (payload.id as string) ?? fallbackId,
    type,
    timestamp: (payload.timestamp as string) ?? new Date().toISOString(),
    payload: (payload.payload as Record<string, unknown>) ?? payload,
  };
}

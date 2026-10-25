import {
  isEventType,
  normalizeChatFrame,
  type AgentEvent,
  type ChatFrame,
} from '@/types/events';
import type { NotificationItem } from '@/api/client';

export type ConnectionStatus = 'connecting' | 'open' | 'closed' | 'error';

export interface WebSocketOptions {
  url: string;
  channels?: string[];
  reconnectIntervalMs?: number;
  maxReconnectAttempts?: number;
  onEvent: (event: AgentEvent) => void;
  /**
   * 服务端推来的通知。它不走 `onEvent`：通知没有 `type` 字段，是另一种
   * 信封——塞进 `normalizeAgentEvent` 会被当成不认识的事件静默丢掉，
   * 而它恰恰是站内通信唯一的实时到达路径。
   */
  onNotification?: (notification: NotificationItem) => void;
  /**
   * 聊天帧。它不经过 `onEvent`：聊天流的名字自成一族，混进 agent 事件的
   * 分派里只会被当成不认识的事件丢掉。
   */
  onChatFrame?: (frame: ChatFrame) => void;
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
  private lastPongAt = 0;

  private static readonly HEARTBEAT_INTERVAL_MS = 30000;
  /** Half-open connections: no pong within this window → close + reconnect. */
  private static readonly PONG_TIMEOUT_MS =
    EventStreamClient.HEARTBEAT_INTERVAL_MS * 2 + 5000;
  /** Slow steady-state retry cadence once fast attempts are exhausted. */
  private static readonly SLOW_RECONNECT_MS = 30000;

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
      this.lastPongAt = Date.now();
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

    if (message.type === 'pong') {
      this.lastPongAt = Date.now();
      return;
    }

    // 服务端把 agent 事件按事件自己的名字直接写在 `type` 上，没有外层信封。
    // 只认 `agent_event` 那层信封的话，工具调用、状态迁移、错误这些帧全部在
    // 这里被静默丢掉——总线上有、界面上永远没有。
    if (isEventType(message.type)) {
      this.options.onEvent({
        id: message.event_id ?? `${message.type}-${Date.now()}`,
        type: message.type,
        timestamp:
          (message as unknown as { timestamp?: string }).timestamp ??
          new Date().toISOString(),
        payload: message as unknown as Record<string, unknown>,
      });
      return;
    }

    if (message.type === 'agent_event' && message.payload) {
      // 聊天帧与 agent 事件共用这一层信封，内层的名字不同。先问聊天那一族：
      // 它的名字（`message.start`）不在 agent 事件的已知表里，交给下面那个
      // 归一函数只会被当成不认识的事件丢掉——回复于是永远上不了屏。
      const chat = normalizeChatFrame(
        message.payload,
        message.session_id,
        message.event_id ?? (message.payload.id as string)
      );
      if (chat) {
        this.options.onChatFrame?.(chat);
        return;
      }
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
      const notification = normalizeNotification(
        message.payload,
        message.event_id ?? (message.payload.id as string | undefined)
      );
      if (notification) {
        this.options.onNotification?.(notification);
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

    this.reconnectAttempts += 1;
    // Fast exponential-ish backoff for the first maxReconnectAttempts tries,
    // then keep retrying forever at a slow fixed cadence — a long server
    // outage must not silently kill the live view.
    const delay =
      this.reconnectAttempts <= maxReconnectAttempts
        ? reconnectIntervalMs * Math.min(this.reconnectAttempts, 5)
        : EventStreamClient.SLOW_RECONNECT_MS;
    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    let seq = 0;
    this.pingTimer = setInterval(() => {
      // A half-open socket never fires onclose; detect it via missing pongs
      // and close so the reconnect path takes over.
      if (Date.now() - this.lastPongAt > EventStreamClient.PONG_TIMEOUT_MS) {
        this.ws?.close();
        return;
      }
      this.ping(seq++);
    }, EventStreamClient.HEARTBEAT_INTERVAL_MS);
  }

  private stopHeartbeat(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
}

/**
 * 服务端的通知信封 → 列表端点的行形状。
 *
 * 两种入口（列表端点、WebSocket 推送）必须拼成同一个 `NotificationItem`，
 * 否则同一条通知在面板里会显示两遍、或者刷新一下换了样子。服务端推的是
 * 完整的 `Notification`（多一个 `user_id`、少一层 `data`），这里把差异收在
 * 这一个函数里。
 */
function normalizeNotification(
  payload: Record<string, unknown>,
  fallbackId: string | undefined
): NotificationItem | null {
  const id = (payload.id as string | undefined) ?? fallbackId;
  if (!id) return null;
  return {
    id,
    title: (payload.title as string | undefined) ?? '',
    body: (payload.body as string | undefined) ?? '',
    is_read: (payload.is_read as boolean | undefined) ?? false,
    created_at:
      (payload.created_at as string | undefined) ?? new Date().toISOString(),
    read_at: (payload.read_at as string | undefined) ?? null,
  };
}

function normalizeAgentEvent(
  payload: Record<string, unknown>,
  fallbackId: string
): AgentEvent | null {
  const type = payload.type;
  if (!isEventType(type)) return null;

  return {
    id: (payload.id as string) ?? fallbackId,
    type,
    timestamp: (payload.timestamp as string) ?? new Date().toISOString(),
    payload: (payload.payload as Record<string, unknown>) ?? payload,
  };
}

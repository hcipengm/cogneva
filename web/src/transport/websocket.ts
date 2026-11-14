import {
  isEventType,
  normalizeChatFrame,
  type AgentEvent,
  type ChatFrame,
} from '@/types/events';
import type { NotificationItem } from '@/api/client';

export type ConnectionStatus = 'connecting' | 'open' | 'closed' | 'error';

/**
 * 服务端那一帧「要不要动手」按线路上的样子。
 *
 * 只声明会读的那几格：服务端还带着 `mode`，这一页用它显示「这一问是在哪一格下
 * 发生的」；其余字段不声明，免得在这里长出一份与 Rust 侧悄悄分家的字段表。
 */
/** 服务端递回来的一条规则，五格都照原样读，见 `permissionStore`。 */
export interface PermissionRuleWire {
  id?: string;
  tool?: string;
  subject?: string;
  allow?: boolean;
  scope?: string;
}

export interface PermissionRulesWire {
  session_id?: string;
  rules?: PermissionRuleWire[];
}

/** 服务端递回来的一句约定：号与原文，见 `memoryStore`。 */
export interface OwnerMemoryEntryWire {
  id?: string;
  text?: string;
}

export interface OwnerMemoryWire {
  entries?: OwnerMemoryEntryWire[];
}

export interface PermissionAskWire {
  request_id?: string;
  session_id?: string;
  tool?: string;
  summary?: string;
  mode?: string;
  /** 待批准的计划正文，只有 plan 模式那一问带它。 */
  plan?: string;
}

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
  /**
   * 一次「要不要动手」的询问。它不经过 `onEvent`：这不是循环吐出来的事件，而是
   * 一个正卡在那里等人回答的调用——丢进事件流里，屏幕上就只剩下一行日志，而
   * 那一轮会一直等到连接断掉才被拒绝。
   */
  onPermissionRequest?: (ask: PermissionAskWire) => void;
  /**
   * 留下来的规则名单。它不走 `onChatFrame`：规则不是这一轮吐出来的东西，而是
   * 一个人设过一次之后留在平台上的设置——混进聊天流会被当成一句助手说的话。
   */
  onPermissionRules?: (rules: PermissionRuleWire[]) => void;
  /**
   * 这个人写下的约定。与规则同一个理由：它是设置，不是这一轮的产出，混进聊天流
   * 会被当成一句助手说的话。
   */
  onOwnerMemory?: (entries: OwnerMemoryEntryWire[]) => void;
  onStatusChange?: (status: ConnectionStatus) => void;
  onError?: (error: Error) => void;
}

/**
 * 服务端信封里客户端真会读的那几格。
 *
 * 服务端的 `ServerMessage` 还带着 `server_time`、`connection_id`、`missed_events`
 * 等字段，这一页一格都不读——在这里声明它们，等于给下一个读者留一份会与 Rust 侧
 * 悄悄分家的第二份字段表。`type` 保持 `string`：判它是哪一个信封的是下面几个
 * 字面量分支，不是类型系统。
 */
export interface ServerMessage {
  type: string;
  event_id?: string;
  session_id?: string;
  payload?: Record<string, unknown>;
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
    } else if (message.type === 'permission_request') {
      // 认不出请求号的帧不能落地：答案是按它回去的，没有它就等于一条永远等在
      // 那里的问题。
      const wire = message as unknown as PermissionAskWire;
      const requestId = wire.request_id;
      if (!requestId) return;
      this.options.onPermissionRequest?.(wire);
      return;
    } else if (message.type === 'permission_rules') {
      // 名单为空是真的「一条也没有」：服务端只在读到的时候发这一帧，所以收到它
      // 就等于读到了。认不出形状的那一份按空名单处理，不拿上次的名单冒充这次。
      const wire = message as unknown as PermissionRulesWire;
      this.options.onPermissionRules?.(Array.isArray(wire.rules) ? wire.rules : []);
      return;
    } else if (message.type === 'owner_memory') {
      // 名单为空是真的「一句也没写」：服务端只在读到的时候发这一帧。认不出形状
      // 的那一份按空名单处理，不拿上次的名单冒充这一次。
      const wire = message as unknown as OwnerMemoryWire;
      this.options.onOwnerMemory?.(Array.isArray(wire.entries) ? wire.entries : []);
      return;
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

/**
 * 线上的一条帧 → 界面认得的形状。实时流与历史事件查询共用这一个函数：
 * 两处各写一份映射，同一帧在直播和回放里会被说成两件事（也曾经被丢过一次）。
 */
export function normalizeAgentEvent(
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

import { useEffect, useRef } from 'react';
import { EventStreamClient } from '@/transport/websocket';
import { MockEventStreamClient } from '@/transport/mockStream';
import { describeEvent, useStreamStore, type MessageRole } from '@/store/streamStore';
import type { AgentEvent } from '@/types/events';
import type { NotificationItem } from '@/api/client';

/**
 * 一条消息里的纯文本。
 *
 * 服务端的 `Message::content` 是内容块数组（文本、思考、工具调用、图片混在
 * 一起），画布只画说得出口的那部分——把工具调用的 JSON 当正文铺开，读的人
 * 会以为那是 agent 说的话。
 */
function messageText(message: unknown): string {
  if (typeof message === 'string') return message;
  const content = (message as { content?: unknown })?.content;
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .map((block) => {
      const b = block as { type?: string; text?: string };
      return b.type === 'text' ? b.text ?? '' : '';
    })
    .filter(Boolean)
    .join('\n');
}

export type StreamClient = EventStreamClient | MockEventStreamClient;

export interface UseEventStreamOptions {
  url?: string;
  channels?: string[];
  mock?: boolean;
  /** 服务端推来的通知；调用方每次渲染换一个新函数也不会重连（走 ref） */
  onNotification?: (notification: NotificationItem) => void;
}

/**
 * 一条增量事件里真正要追加的那段文字。
 *
 * 只有 `text_delta` 是正文。`thinking_delta` 是模型的内部草稿，把它铺进画布
 * 会让读的人分不清哪些是会落进回答的；工具调用的参数是 JSON 片段，拼起来才
 * 有意义，单看每一片是噪声。
 */
function extractDelta(assistantEvent: unknown): string | null {
  const e = assistantEvent as { type?: string; delta?: string } | null;
  if (!e) return null;
  return e.type === 'text_delta' && typeof e.delta === 'string' ? e.delta : null;
}

export function useEventStream(options: UseEventStreamOptions = {}): {
  clientRef: React.RefObject<StreamClient | null>;
} {
  const clientRef = useRef<StreamClient | null>(null);
  const notificationHandler = useRef(options.onNotification);
  notificationHandler.current = options.onNotification;
  const startMessage = useStreamStore((state) => state.startMessage);
  const appendDelta = useStreamStore((state) => state.appendDelta);
  const endMessage = useStreamStore((state) => state.endMessage);
  const finishMessage = useStreamStore((state) => state.finishMessage);
  const setTaskProgress = useStreamStore((state) => state.setTaskProgress);
  const setConnectionStatus = useStreamStore(
    (state) => state.setConnectionStatus
  );
  const recordActivity = useStreamStore((state) => state.recordActivity);
  /** 每个 agent 正在流的那一条消息（agent_id → 画布上的消息 id）。 */
  const streaming = useRef(new Map<string, string>());

  useEffect(() => {
    // 未登录时 url 为空，不要建连——new WebSocket('') 会解析到页面根路径
    if (!options.mock && !options.url) {
      return;
    }

    const handleEvent = (event: AgentEvent) => {
      // 先记一行活动，再按类型分派：活动流是「总线上来了什么」的完整账，
      // 任何一条被分派分支漏掉的事件也仍然在账上，界面上不会凭空消失。
      recordActivity(describeEvent(event));

      const p = event.payload as Record<string, unknown>;
      const agentId = typeof p.agent_id === 'string' ? p.agent_id : null;

      switch (event.type) {
        case 'message_start': {
          if (!agentId) break;
          const id = `${agentId}:${event.timestamp}`;
          const role = ((p.message as { role?: string } | undefined)?.role ??
            'assistant') as MessageRole;
          streaming.current.set(agentId, id);
          startMessage(id, role);
          break;
        }
        case 'message_update': {
          if (!agentId) break;
          const delta = extractDelta(p.assistant_event);
          const id = streaming.current.get(agentId);
          if (id && delta) appendDelta(id, delta);
          break;
        }
        case 'message_end': {
          if (!agentId) break;
          const id = streaming.current.get(agentId);
          // 用整条消息补一次正文：用户发的与工具回的不走增量，只收尾的话
          // 画布上那一条是空的。
          if (id) finishMessage(id, messageText(p.message));
          streaming.current.delete(agentId);
          break;
        }
        case 'agent_end': {
          // 运行结束时把还没收尾的流关掉：少了这一句，画布上最后一条会永远
          // 转着「生成中」，而平台其实早就停了。
          if (agentId) {
            const id = streaming.current.get(agentId);
            if (id) endMessage(id);
            streaming.current.delete(agentId);
          }
          break;
        }
        case 'task_status_change': {
          const taskId = typeof p.task_id === 'string' ? p.task_id : null;
          const status = typeof p.status === 'string' ? p.status : '';
          if (!taskId) break;
          setTaskProgress({
            taskId,
            progress: status === 'completed' ? 100 : 0,
            step: status,
          });
          break;
        }
        default:
          break;
      }
    };

    const client = options.mock
      ? new MockEventStreamClient({
          onEvent: handleEvent,
          onStatusChange: (status) => setConnectionStatus(status),
        })
      : new EventStreamClient({
          url: options.url ?? '',
          channels: options.channels,
          onEvent: handleEvent,
          onNotification: (notification) =>
            notificationHandler.current?.(notification),
          onStatusChange: (status) => setConnectionStatus(status),
          onError: (error) => {
            console.error('WebSocket error:', error);
          },
        });

    clientRef.current = client;
    client.connect();

    return () => {
      client.disconnect();
      clientRef.current = null;
    };
  }, [
    options.mock,
    options.url,
    options.channels,
    startMessage,
    appendDelta,
    endMessage,
    finishMessage,
    setTaskProgress,
    setConnectionStatus,
    recordActivity,
  ]);

  return { clientRef };
}

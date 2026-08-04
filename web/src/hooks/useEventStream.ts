import { useEffect, useRef } from 'react';
import { EventStreamClient } from '@/transport/websocket';
import { MockEventStreamClient } from '@/transport/mockStream';
import { useStreamStore } from '@/store/streamStore';
import type { AgentEvent } from '@/types/events';

export type StreamClient = EventStreamClient | MockEventStreamClient;

export interface UseEventStreamOptions {
  url?: string;
  channels?: string[];
  mock?: boolean;
}

export function useEventStream(options: UseEventStreamOptions = {}): {
  clientRef: React.RefObject<StreamClient | null>;
} {
  const clientRef = useRef<StreamClient | null>(null);
  const startMessage = useStreamStore((state) => state.startMessage);
  const appendDelta = useStreamStore((state) => state.appendDelta);
  const endMessage = useStreamStore((state) => state.endMessage);
  const setTaskProgress = useStreamStore((state) => state.setTaskProgress);
  const setConnectionStatus = useStreamStore(
    (state) => state.setConnectionStatus
  );

  useEffect(() => {
    // 未登录时 url 为空，不要建连——new WebSocket('') 会解析到页面根路径
    if (!options.mock && !options.url) {
      return;
    }

    const handleEvent = (event: AgentEvent) => {
      switch (event.type) {
        case 'message.start': {
          const { message_id, role } = event.payload as {
            message_id: string;
            role: 'user' | 'assistant' | 'system';
          };
          startMessage(message_id, role);
          break;
        }
        case 'message.text_delta': {
          const { message_id, delta } = event.payload as {
            message_id: string;
            delta: string;
          };
          appendDelta(message_id, delta);
          break;
        }
        case 'message.end': {
          const { message_id } = event.payload as { message_id: string };
          endMessage(message_id);
          break;
        }
        case 'task.progress': {
          const { task_id, progress, step } = event.payload as {
            task_id: string;
            progress: number;
            step: string;
          };
          setTaskProgress({ taskId: task_id, progress, step });
          break;
        }
        default:
          // Forward unknown events to store for future UI components.
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
    setTaskProgress,
    setConnectionStatus,
  ]);

  return { clientRef };
}

import type { AgentEvent } from '@/types/events';

export interface MockStreamOptions {
  onEvent: (event: AgentEvent) => void;
  onStatusChange?: (status: 'connecting' | 'open' | 'closed' | 'error') => void;
}

export class MockEventStreamClient {
  private options: MockStreamOptions;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private connected = false;

  constructor(options: MockStreamOptions) {
    this.options = options;
  }

  connect(): void {
    this.connected = true;
    this.options.onStatusChange?.('open');
  }

  disconnect(): void {
    this.connected = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.options.onStatusChange?.('closed');
  }

  send(_message: unknown): void {
    // Mock transport does not send to a real server.
  }

  simulateUserMessage(text: string): void {
    const messageId = `user-${Date.now()}`;
    this.options.onEvent({
      id: messageId,
      type: 'message.start',
      timestamp: new Date().toISOString(),
      payload: { message_id: messageId, role: 'user' },
    });
    this.options.onEvent({
      id: `${messageId}-delta`,
      type: 'message.text_delta',
      timestamp: new Date().toISOString(),
      payload: { message_id: messageId, delta: text },
    });
    this.options.onEvent({
      id: `${messageId}-end`,
      type: 'message.end',
      timestamp: new Date().toISOString(),
      payload: { message_id: messageId },
    });

    this.timer = setTimeout(() => {
      this.simulateAssistantReply(messageId);
    }, 400);
  }

  private simulateAssistantReply(_userMessageId: string): void {
    const messageId = `assistant-${Date.now()}`;
    const replyText =
      'Cogneva 已收到你的意图。正在通过 Agent Event Stream 协调多个子代理处理：\n\n1. 分析 /health 接口当前延迟瓶颈\n2. 生成性能优化建议\n3. 应用并验证补丁\n\n所有进展都会以事件流形式实时投影到当前 UI。';

    this.options.onEvent({
      id: messageId,
      type: 'message.start',
      timestamp: new Date().toISOString(),
      payload: { message_id: messageId, role: 'assistant' },
    });

    let index = 0;
    const stream = () => {
      if (!this.connected) return;
      if (index >= replyText.length) {
        this.options.onEvent({
          id: `${messageId}-end`,
          type: 'message.end',
          timestamp: new Date().toISOString(),
          payload: { message_id: messageId },
        });
        return;
      }

      const chunkSize = Math.floor(Math.random() * 4) + 1;
      const chunk = replyText.slice(index, index + chunkSize);
      this.options.onEvent({
        id: `${messageId}-${index}`,
        type: 'message.text_delta',
        timestamp: new Date().toISOString(),
        payload: { message_id: messageId, delta: chunk },
      });
      index += chunkSize;
      this.timer = setTimeout(stream, 30);
    };

    stream();
  }
}

import type { AgentEvent } from '@/types/events';

export interface MockStreamOptions {
  onEvent: (event: AgentEvent) => void;
  onStatusChange?: (status: 'connecting' | 'open' | 'closed' | 'error') => void;
}

/**
 * 没有后端时的演示流。
 *
 * 帧的形状与服务端一致（`agent_id` ＋ `message`／`assistant_event`），否则
 * 演示能跑通、接上真后端就哑了——模拟器编一套形状、服务端另一套，两边都
 * 只验证了自己那一半。
 */
export class MockEventStreamClient {
  private options: MockStreamOptions;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private connected = false;

  private static readonly AGENT = 'demo-agent';

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
    this.emit('message_start', { role: 'user', content: [{ type: 'text', text }] });
    this.emit('message_end', { role: 'user', content: [{ type: 'text', text }] });

    this.timer = setTimeout(() => {
      this.simulateAssistantReply();
    }, 400);
  }

  private emit(type: AgentEvent['type'], fields: Record<string, unknown>): void {
    if (!this.connected) return;
    this.options.onEvent({
      id: `${type}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      type,
      timestamp: new Date().toISOString(),
      payload: { agent_id: MockEventStreamClient.AGENT, ...fields },
    });
  }

  private simulateAssistantReply(): void {
    const replyText =
      'Cogneva 已收到你的意图。正在通过 Agent Event Stream 协调多个子代理处理：\n\n1. 分析 /health 接口当前延迟瓶颈\n2. 生成性能优化建议\n3. 应用并验证补丁\n\n所有进展都会以事件流形式实时投影到当前 UI。';

    this.emit('agent_start', {});
    this.emit('state_change', { from: 'idle', to: 'thinking' });
    this.emit('message_start', { message: { role: 'assistant', content: [] } });

    let index = 0;
    const stream = () => {
      if (!this.connected) return;
      if (index >= replyText.length) {
        this.emit('message_end', {
          message: {
            role: 'assistant',
            content: [{ type: 'text', text: replyText }],
          },
        });
        // 回话之后调一次工具：活动面板要能看见「读了哪个文件」，而不是只有
        // 一段文字流过去。
        this.emit('tool_execution_start', {
          tool_call_id: 'demo-call-1',
          tool_name: 'read_file',
          args: { path: 'crates/cog-gateway/src/lib.rs' },
        });
        this.timer = setTimeout(() => {
          this.emit('tool_execution_end', {
            tool_call_id: 'demo-call-1',
            result: { bytes: 18422 },
            is_error: false,
          });
          this.emit('state_change', { from: 'thinking', to: 'idle' });
          this.emit('agent_end', { messages: [] });
        }, 600);
        return;
      }

      const chunkSize = Math.floor(Math.random() * 4) + 1;
      const chunk = replyText.slice(index, index + chunkSize);
      this.emit('message_update', {
        assistant_event: { type: 'text_delta', content_index: 0, delta: chunk },
      });
      index += chunkSize;
      this.timer = setTimeout(stream, 30);
    };

    stream();
  }
}

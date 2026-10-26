import type { AgentEvent, ChatFrame } from '@/types/events';

export interface MockStreamOptions {
  onEvent: (event: AgentEvent) => void;
  /** 聊天帧走自己那一路，与真后端一致：它们不是 agent 事件的变体。 */
  onChatFrame?: (frame: ChatFrame) => void;
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
    // 与服务端同形：先回声用户那一句（三帧），再流助手的。
    const userMid = `user-${Math.random().toString(36).slice(2, 10)}`;
    this.chat('start', userMid, { role: 'user' });
    this.chat('text_delta', userMid, { delta: text });
    this.chat('end', userMid);

    this.timer = setTimeout(() => {
      this.simulateAssistantReply();
    }, 400);
  }

  /** 一帧聊天。名字带点，与 `crates/cog-gateway/src/chat.rs` 发的一致。 */
  private chat(
    kind: 'start' | 'text_delta' | 'end',
    messageId: string,
    fields: Record<string, unknown> = {}
  ): void {
    if (!this.connected) return;
    this.options.onChatFrame?.({
      id: `chat-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      sessionId: this.sessionId,
      messageId,
      kind,
      role: (fields.role as 'user' | 'assistant' | null) ?? null,
      delta: (fields.delta as string | null) ?? null,
      goalId: (fields.goalId as string | null) ?? null,
      timestamp: new Date().toISOString(),
    });
  }

  /** 演示流只服务一段对话；多段由界面自己开，连上真后端时才有各自的流。 */
  setSession(sessionId: string): void {
    this.sessionId = sessionId;
  }

  private sessionId = 'main';

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
    const replyMid = `assistant-${Math.random().toString(36).slice(2, 10)}`;
    this.chat('start', replyMid, { role: 'assistant' });

    let index = 0;
    const stream = () => {
      if (!this.connected) return;
      if (index >= replyText.length) {
        this.chat('end', replyMid);
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
      this.chat('text_delta', replyMid, { delta: chunk });
      index += chunkSize;
      this.timer = setTimeout(stream, 30);
    };

    stream();
  }
}

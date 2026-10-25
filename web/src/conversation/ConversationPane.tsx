import { useCallback, useEffect, useRef, useState } from 'react';
import type { StreamClient } from '@/hooks/useEventStream';
import { useChatStore, type ChatMessage } from '@/store/chatStore';

/**
 * 一段对话的窗口。
 *
 * 画成回合而不是气泡：气泡把每一句圈成一张卡片，一段会话于是变成一串互不相干
 * 的胶囊，读者要在里面找「上一句是谁说的、这一轮到哪儿了」。这里用左侧一条细线
 * 贯穿一轮、角色用等宽体标在行首，句子就是句子本身。正在流的那一轮线是亮的，
 * 流完就暗下去——一轮的边界因此是看得见的，而不是靠气泡的边缘去猜。
 */

function stamp(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}

function Turn({ message }: { message: ChatMessage }) {
  const [showThinking, setShowThinking] = useState(false);
  const streaming = message.status === 'streaming' || message.status === 'sending';
  const who = message.role === 'user' ? '你' : message.role === 'system' ? '系统' : '执行者';

  return (
    <article className="group relative pl-4">
      <span
        aria-hidden
        className="chat-turn-rule absolute left-0 top-1 bottom-1 w-px"
        data-streaming={streaming}
      />
      <header className="mb-1 flex items-baseline gap-2">
        <span
          className="mono text-[11px]"
          style={{ color: message.role === 'user' ? 'var(--brass)' : 'var(--live)' }}
        >
          {who}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">{stamp(message.at)}</span>
        {message.status === 'sending' && (
          <span className="text-[10px] text-[var(--muted)]">已发出，等待回声</span>
        )}
        {message.failure && (
          <span className="text-[10px]" style={{ color: 'var(--warn)' }}>
            {message.failure}
          </span>
        )}
      </header>

      {message.thinking && (
        <div className="mb-1">
          <button
            type="button"
            onClick={() => setShowThinking((v) => !v)}
            className="text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--paper-dim)]"
          >
            {showThinking ? '收起思考' : '思考过程'}
          </button>
          {showThinking && (
            <p className="mt-1 whitespace-pre-wrap border-l border-[var(--line)] pl-2 text-[11px] leading-[1.6] text-[var(--muted)]">
              {message.thinking}
            </p>
          )}
        </div>
      )}

      <p
        className="whitespace-pre-wrap break-words text-[13px] leading-[1.7]"
        style={{ color: message.role === 'user' ? 'var(--paper)' : 'var(--paper-dim)' }}
      >
        {message.text}
        {streaming && <span className="chat-caret" aria-hidden />}
      </p>
    </article>
  );
}

export interface ConversationPaneProps {
  sessionId: string;
  active: boolean;
  onFocus: () => void;
  onClose: () => void;
  clientRef: React.RefObject<StreamClient | null>;
  /** 只剩一段时不给关：关掉最后一段对话，面上就没有落点了。 */
  closable: boolean;
  /**
   * 去不去读回历史。演示模式没有后端，读一个不存在的接口只会让每一段对话的
   * 顶上挂一条「历史没读到」——那是把「没有这个功能」和「这个功能坏了」画成
   * 同一个样子。
   */
  historyEnabled?: boolean;
}

export function ConversationPane({
  sessionId,
  active,
  onFocus,
  onClose,
  clientRef,
  closable,
  historyEnabled = true,
}: ConversationPaneProps) {
  const session = useChatStore((s) => s.sessions[sessionId]);
  const appendUserTurn = useChatStore((s) => s.appendUserTurn);
  const hydrate = useChatStore((s) => s.hydrate);
  const [draft, setDraft] = useState('');
  const scrollerRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);

  // 打开就先把历史读回来。读过的不再读：重读会把刚流出来的这一段冲掉。
  useEffect(() => {
    if (historyEnabled) void hydrate(sessionId);
  }, [hydrate, historyEnabled, sessionId]);

  // 只在读者本来就贴着底的时候跟着滚。正在往回翻的时候被拽回底部，是对话界面
  // 最容易被骂的一个动作。
  useEffect(() => {
    const el = scrollerRef.current;
    if (!el || !stickRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [session?.messages]);

  const onScroll = useCallback(() => {
    const el = scrollerRef.current;
    if (!el) return;
    stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
  }, []);

  const send = useCallback(() => {
    const text = draft.trim();
    if (!text) return;
    setDraft('');
    // 先入账，再出门。顺序反过来时，网络慢一拍、或者这个部署的回声路径断了，
    // 用户看到的就是「我说话了，但这里什么都没发生」。
    appendUserTurn(sessionId, text);
    stickRef.current = true;

    const client = clientRef.current;
    if (!client) return;
    if ('simulateUserMessage' in client) {
      client.setSession?.(sessionId);
      client.simulateUserMessage(text);
    } else {
      client.send({ type: 'chat_message', session_id: sessionId, content: text });
    }
  }, [appendUserTurn, clientRef, draft, sessionId]);

  if (!session) return null;

  const streaming = Boolean(session.streaming);

  return (
    <section
      onMouseDown={onFocus}
      className="relative flex min-h-0 min-w-0 flex-1 flex-col"
      style={{
        borderLeft: active ? '1px solid var(--line-strong)' : '1px solid transparent',
      }}
      aria-label={`对话 ${session.title}`}
    >
      <header className="flex shrink-0 items-center gap-2 px-4 pt-3 pb-2">
        <span
          aria-hidden
          className="h-1.5 w-1.5 rounded-full"
          style={{
            background: streaming ? 'var(--brass)' : 'var(--muted)',
            boxShadow: streaming ? '0 0 8px var(--brass)' : 'none',
          }}
        />
        <h2 className="truncate text-[12px] font-medium text-[var(--paper)]">{session.title}</h2>
        <span className="mono text-[10px] text-[var(--muted)]">{sessionId}</span>
        <span className="ml-auto flex items-center gap-3">
          {session.hydrating && (
            <span className="text-[10px] text-[var(--muted)]">正在读回历史…</span>
          )}
          {session.hydrateError && (
            <span className="text-[10px]" style={{ color: 'var(--warn)' }}>
              历史没读到：{session.hydrateError}
            </span>
          )}
          {closable && (
            <button
              type="button"
              onClick={onClose}
              className="text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--danger)]"
              aria-label="关掉这一段对话"
            >
              关闭
            </button>
          )}
        </span>
      </header>

      <div
        ref={scrollerRef}
        onScroll={onScroll}
        className="min-h-0 flex-1 overflow-y-auto px-4 pb-3"
      >
        {session.messages.length === 0 && !session.hydrating && (
          <p className="max-w-[46ch] text-[12px] leading-[1.7] text-[var(--muted)]">
            还没有说过话。说一句你要它做的事——它会先回一句话，然后把这件事拆成任务，
            每一步都会回到这一屏。
          </p>
        )}
        <div className="flex flex-col gap-4">
          {session.messages.map((message) => (
            <Turn key={message.id} message={message} />
          ))}
        </div>
      </div>

      <div className="shrink-0 px-4 pb-4 pt-1">
        {/*
          一条发问的行，不是一个输入框。
          没有边框、没有底色、没有「请输入…」的空白盒：这几样合起来是表单的样子，
          读者会以为在填一格东西。这里是一句话的起头——一个提示符，一句还没说出口
          的话，右边是「怎么送出去」。分隔靠上方那一条发丝线，焦点在它上面时才亮。
        */}
        <div
          className="flex items-stretch gap-2 border-t pt-2 transition-colors"
          style={{ borderColor: draft ? 'var(--line-strong)' : 'var(--line)' }}
        >
          <span className="mono flex items-center pl-0.5 text-[12px] text-[var(--brass)]">›</span>
          <input
            type="text"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onFocus={onFocus}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                send();
              }
            }}
            placeholder="要它做什么…"
            aria-label="说一句话"
            className="min-w-0 flex-1 bg-transparent px-0.5 py-1 text-[13px] text-[var(--paper)] outline-none placeholder:text-[var(--muted)]"
          />
          <button
            type="button"
            onClick={send}
            disabled={!draft.trim()}
            className="mono px-2 text-[10px] transition-opacity disabled:cursor-not-allowed disabled:opacity-0"
            style={{ color: 'var(--brass)' }}
          >
            ↵ 发送
          </button>
        </div>
      </div>
    </section>
  );
}

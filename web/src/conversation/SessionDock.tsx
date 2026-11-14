import { useState } from 'react';
import { conversationNames } from '@/conversation/callsign';
import { SessionHistory } from '@/conversation/SessionHistory';
import type { StreamClient } from '@/hooks/useEventStream';
import { useChatStore } from '@/store/chatStore';
import { Note } from '@/ui/Note';

/**
 * 开着的几段对话，一条一条排在一行上。
 *
 * 并排几段是这里的主要用法，不是例外：一段在等模型答，另一段可以接着问；一段
 * 交给了守护在跑，另一段照常说话。所以每一段要有自己的名字（点得到、念得出来）、
 * 自己的状态（正在说的那一条有没有在动），也要能单独关掉。
 *
 * 名字不按顺序编号，从会话 id 折出来（见 `callsign`）：关掉中间一段，别的名字
 * 不会跟着串位；刷新之后还是同一个人。
 */
export function SessionDock({
  clientRef,
}: {
  clientRef: React.RefObject<StreamClient | null>;
}) {
  const order = useChatStore((s) => s.order);
  const sessions = useChatStore((s) => s.sessions);
  const active = useChatStore((s) => s.active);
  const setActive = useChatStore((s) => s.setActive);
  const createSession = useChatStore((s) => s.createSession);
  const closeSession = useChatStore((s) => s.closeSession);
  const listSessions = useChatStore((s) => s.listSessions);
  // 过去的会话不是一个面板的开关，是一条找回的路：打开它就去服务端读一次名单，
  // 浏览器里那几段丢了之后，路只有这一条。
  const [historyOpen, setHistoryOpen] = useState(false);
  const names = conversationNames(order);

  /**
   * 关掉一段之前，先把这一段上还在跑的那一轮停掉。
   *
   * 不先停就关：那一格从屏幕上消失，而服务端那一边还在生成、还在记账——用户以为
   * 关掉就是不要了，账上却多花了一笔。停止是尽力而为的：连接断了就停不到，那时
   * 服务端那一轮会自己跑完，记录照样落在库里。
   */
  const close = (id: string) => {
    const client = clientRef.current;
    if (client && sessions[id]?.streaming && !('simulateUserMessage' in client)) {
      client.send({ type: 'chat_cancel', session_id: id });
    }
    closeSession(id);
  };

  return (
    <>
    <div
      className="flex shrink-0 items-stretch gap-1 overflow-x-auto border-b border-[var(--line)] px-2 py-1"
      aria-label="开着的对话"
    >
      {order.map((id) => {
        const session = sessions[id];
        const current = id === active;
        const streaming = Boolean(session?.streaming);
        return (
          <div
            key={id}
            className={`flex min-w-0 items-center gap-1 border px-1.5 py-0.5 text-[11px] ${
              current ? 'bg-[var(--ink-700)]' : 'hover:bg-[var(--ink-800)]'
            }`}
            style={{ borderColor: current ? 'var(--line-strong)' : 'var(--line)' }}
          >
            <Note
              as="button"
              type="button"
              onClick={() => setActive(id)}
              aria-current={current ? 'true' : undefined}
              text={`${session?.title ?? '新的对话'}（会话 ${id}）`}
              className="flex min-w-0 items-baseline gap-1.5 text-left"
            >
              <span
                aria-hidden
                className="h-1 w-1 shrink-0 rounded-full"
                style={{
                  background: streaming ? 'var(--brass)' : 'var(--muted)',
                  boxShadow: streaming ? '0 0 6px var(--brass)' : 'none',
                }}
              />
              <span
                className="mono shrink-0"
                style={{ color: current ? 'var(--brass)' : 'var(--paper-dim)' }}
              >
                {names.get(id)}
              </span>
              <span className="max-w-[12ch] truncate text-[var(--muted)]">
                {session?.title ?? '新的对话'}
              </span>
            </Note>
            {order.length > 1 && (
              <button
                type="button"
                onClick={() => close(id)}
                aria-label={`关掉 ${names.get(id)} 这一段对话`}
                className="shrink-0 text-[var(--muted)] transition-colors hover:text-[var(--danger)]"
              >
                ×
              </button>
            )}
          </div>
        );
      })}
      <Note
        as="button"
        type="button"
        onClick={() => createSession()}
        className="mono shrink-0 px-1.5 text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--brass)]"
        text="再开一段对话，原来那几段继续开着"
      >
        ＋ 新对话
      </Note>
      <Note
        as="button"
        type="button"
        onClick={() => {
          const next = !historyOpen;
          setHistoryOpen(next);
          if (next) void listSessions();
        }}
        aria-expanded={historyOpen}
        className="mono shrink-0 px-1.5 text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--brass)]"
        text="在这个人的过去会话里找，找到就接上那一段"
      >
        历史
      </Note>
    </div>
    {historyOpen && <SessionHistory onClose={() => setHistoryOpen(false)} />}
    </>
  );
}

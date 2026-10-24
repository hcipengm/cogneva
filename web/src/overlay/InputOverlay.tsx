import { useState, useCallback } from 'react';
import type { StreamClient } from '@/hooks/useEventStream';

interface InputOverlayProps {
  clientRef: React.RefObject<StreamClient | null>;
}

export function InputOverlay({ clientRef }: InputOverlayProps) {
  const [text, setText] = useState('');

  const send = useCallback(() => {
    const trimmed = text.trim();
    if (!trimmed) return;

    const client = clientRef.current;
    if (client && 'simulateUserMessage' in client) {
      client.simulateUserMessage(trimmed);
    } else if (client) {
      // Real WebSocket path: send user message through REST API or WS.
      client.send({
        type: 'chat_message',
        session_id: 'default',
        content: trimmed,
      });
    }

    setText('');
  }, [text, clientRef]);

  return (
    <div className="pointer-events-none absolute inset-x-0 bottom-0 p-4">
      <div className="pointer-events-auto mx-auto flex max-w-3xl items-stretch gap-2 border border-[var(--line-strong)] bg-[color-mix(in_srgb,var(--ink-800)_88%,transparent)] p-1.5 backdrop-blur-sm">
        <span className="mono flex items-center pl-2 text-[11px] text-[var(--muted)]">意图</span>
        <input
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') send();
          }}
          placeholder="说一句话，交给在岗的执行者…"
          className="min-w-0 flex-1 bg-transparent px-2 py-2 text-[13px] text-[var(--paper)] outline-none placeholder:text-[var(--muted)]"
          aria-label="发一句话"
        />
        <button
          type="button"
          onClick={send}
          disabled={!text.trim()}
          className="px-4 py-2 text-[12px] transition-colors disabled:cursor-not-allowed disabled:opacity-40"
          style={{ background: 'var(--brass)', color: 'var(--ink-1000)' }}
        >
          发送
        </button>
      </div>
    </div>
  );
}

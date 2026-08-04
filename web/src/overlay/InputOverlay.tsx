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
    <div className="absolute bottom-0 left-0 right-0 p-4 pointer-events-none">
      <div className="mx-auto flex max-w-3xl gap-3 pointer-events-auto rounded-2xl border border-slate-700/60 bg-slate-900/80 p-2 shadow-2xl backdrop-blur-md">
        <input
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') send();
          }}
          placeholder="输入意图..."
          className="flex-1 rounded-xl bg-transparent px-4 py-3 text-white placeholder-slate-500 outline-none"
          aria-label="Message input"
        />
        <button
          onClick={send}
          disabled={!text.trim()}
          className="rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 px-6 py-3 font-medium text-white shadow-lg transition hover:from-blue-500 hover:to-indigo-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          发送
        </button>
      </div>
    </div>
  );
}

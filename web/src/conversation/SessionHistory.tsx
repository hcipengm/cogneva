import { useState } from 'react';
import { useChatStore } from '@/store/chatStore';
import { Note } from '@/ui/Note';

/**
 * 过去的会话：这个人在服务端有过的全部对话，最近说话的在前。
 *
 * 拨号条上是这一页开着的那几段，记在浏览器里。换一台机器、清掉浏览器存储，那几段
 * 就没了——而记录还在服务端，所以这个名单是唯一找回来的路。点一段就把它放回
 * 工作台，历史按同一个会话 id 读回来：接上的是原来那一段，不是新开一段同名的。
 *
 * 搜索是字面匹配、只在自己的会话里找。空名单与读失败分开画：前者是「你确实没有
 * 别的会话」，后者是「这一页不知道你有没有」，把后者画成前者，人就会以为自己
 * 丢了东西。
 */
export function SessionHistory({ onClose }: { onClose: () => void }) {
  const history = useChatStore((s) => s.history);
  const loading = useChatStore((s) => s.historyLoading);
  const error = useChatStore((s) => s.historyError);
  const listSessions = useChatStore((s) => s.listSessions);
  const openSession = useChatStore((s) => s.openSession);
  const renameSession = useChatStore((s) => s.renameSession);
  const hydrate = useChatStore((s) => s.hydrate);
  const [query, setQuery] = useState('');

  const open = (id: string, title: string) => {
    openSession(id);
    // 拨号条上这一格的名字：名单知道它开头说了什么，新开的那一格不知道。
    if (title) renameSession(id, title);
    void hydrate(id);
    onClose();
  };

  const when = (iso: string) => {
    const at = new Date(iso);
    return Number.isNaN(at.getTime()) ? iso : at.toLocaleString();
  };

  return (
    <div
      className="shrink-0 border-b border-[var(--line)] px-2 py-2"
      aria-label="过去的会话"
    >
      <div className="flex items-center gap-2">
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') void listSessions(query);
          }}
          placeholder="在自己的会话里找…"
          aria-label="在自己的会话里找"
          className="mono min-w-0 flex-1 border border-[var(--line)] bg-transparent px-2 py-1 text-[11px] text-[var(--paper)] outline-none focus:border-[var(--brass)]"
        />
        <button
          type="button"
          onClick={() => void listSessions(query)}
          className="mono shrink-0 px-1.5 text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--brass)]"
        >
          找
        </button>
        <button
          type="button"
          onClick={onClose}
          aria-label="收起过去的会话"
          className="shrink-0 text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
        >
          ×
        </button>
      </div>

      {loading && (
        <p className="mono mt-2 text-[11px] text-[var(--muted)]">正在读名单…</p>
      )}
      {!loading && error && (
        <p className="mono mt-2 text-[11px] text-[var(--danger)]">{error}</p>
      )}
      {!loading && !error && history.length === 0 && (
        <p className="mono mt-2 text-[11px] text-[var(--muted)]">
          在这个搜索下没有别的会话
        </p>
      )}
      {!loading && !error && history.length > 0 && (
        <ul className="mt-2 max-h-48 overflow-y-auto">
          {history.map((session) => (
            <li key={session.session_id}>
              <Note
                as="button"
                type="button"
                onClick={() => open(session.session_id, session.title)}
                className="flex w-full min-w-0 items-baseline gap-2 px-1 py-0.5 text-left text-[11px] hover:bg-[var(--ink-800)]"
                text={`接着聊「${session.title}」`}
              >
                <span className="min-w-0 flex-1 truncate text-[var(--paper-dim)]">
                  {session.title || '（这一段的开头没有话）'}
                </span>
                <span className="mono shrink-0 text-[var(--muted)]">
                  {when(session.last_activity_at)}
                </span>
              </Note>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

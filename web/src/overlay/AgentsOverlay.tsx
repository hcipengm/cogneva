import { useState } from 'react';
import { useAgentConversation } from '@/hooks/useAgentConversation';
import type { AgentConversationLine } from '@/api/client';

/**
 * 和每个 agent 的会话窗口。
 *
 * 左边是在岗名单，右边是与选中的那个 agent 说过的每一句和它的结局。名单取的是
 * 注册表（活着的 unit），不是任务图里的 owner 名字——后者可以是一个已经退场的
 * 会话名，对着它说话永远不会有人应答。
 */

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

const OUTCOME_LABEL: Record<string, { text: string; className: string }> = {
  queued: { text: '已送达，等它自己回', className: 'text-slate-400' },
  replied: { text: '已回', className: 'text-emerald-400' },
  timed_out: { text: '没在等待窗口内回答', className: 'text-amber-400' },
  refused: { text: '没送到', className: 'text-red-400' },
  failed: { text: '发送或回收答的通道断了', className: 'text-red-400' },
};

function Line({ line }: { line: AgentConversationLine }) {
  const outcome = OUTCOME_LABEL[line.outcome] ?? {
    text: line.outcome,
    className: 'text-slate-400',
  };
  return (
    <div className="border-t border-slate-800/60 py-2">
      <div className="flex items-baseline gap-2 text-xs">
        <span className="font-medium text-slate-200">{line.from_unit}</span>
        <span className="text-slate-500">→ {line.to_unit}</span>
        <span className="text-slate-500">{line.kind}</span>
        <span className="text-slate-500">{stamp(line.created_at)}</span>
        <span className={outcome.className}>{outcome.text}</span>
      </div>
      <div className="mt-1 whitespace-pre-wrap text-sm text-slate-200">{line.body}</div>
      {line.reply && (
        <div className="mt-1 whitespace-pre-wrap border-l-2 border-emerald-500/40 pl-2 text-sm text-slate-300">
          {line.reply}
        </div>
      )}
      {line.outcome_detail && (
        <div className="mt-1 text-xs text-amber-400/90">{line.outcome_detail}</div>
      )}
    </div>
  );
}

export function AgentsOverlay({ onClose }: { onClose: () => void }) {
  const {
    agents,
    agentsError,
    selected,
    select,
    lines,
    loading,
    error,
    sending,
    sendError,
    send,
    refreshAgents,
    refreshConversation,
  } = useAgentConversation(true);
  const [draft, setDraft] = useState('');

  const submit = async () => {
    const text = draft;
    setDraft('');
    await send(text);
  };

  return (
    <div className="absolute inset-0 z-50 flex items-center justify-center bg-slate-950/90 p-6 backdrop-blur-sm">
      <div className="flex h-[90vh] w-full max-w-6xl flex-col rounded-2xl border border-slate-700/60 bg-slate-900/90 p-6 shadow-2xl">
        <div className="mb-3 flex items-start justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-white">数字员工会话</h1>
            <p className="mt-1 text-xs text-slate-400">
              名单来自在岗注册表；说过的每一句和它的结局都落库，关掉窗口再打开还在。
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={refreshAgents}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              刷新名单
            </button>
            <button
              onClick={onClose}
              className="rounded-lg px-2 py-1 text-xs text-slate-400 transition hover:bg-slate-800 hover:text-slate-200"
            >
              关闭
            </button>
          </div>
        </div>

        <div className="flex min-h-0 flex-1 gap-4">
          <aside className="w-64 shrink-0 overflow-y-auto rounded-xl border border-slate-800/60 bg-slate-950/40 p-2">
            {agentsError && (
              <div className="mb-2 rounded-lg border border-amber-500/40 bg-amber-500/10 p-2 text-xs text-amber-300">
                名单没读到：{agentsError}
              </div>
            )}
            {!agentsError && agents.length === 0 && (
              <div className="p-2 text-xs text-slate-500">
                注册表里没有在岗的 agent。这里只列活着的 unit，不拿任务图里的 owner
                名字顶替。
              </div>
            )}
            {agents.map((agent) => (
              <button
                key={agent.agent_id}
                onClick={() => select(agent.agent_id)}
                className={`mb-1 block w-full rounded-lg px-2 py-1.5 text-left text-xs transition ${
                  selected === agent.agent_id
                    ? 'bg-sky-500/15 text-sky-200'
                    : 'text-slate-300 hover:bg-slate-800/70'
                }`}
              >
                <div className="font-medium">{agent.agent_id}</div>
                <div className="text-slate-500">
                  {agent.role} · {agent.workspace_id}
                </div>
              </button>
            ))}
          </aside>

          <section className="flex min-h-0 flex-1 flex-col">
            <div className="mb-2 flex items-center justify-between text-xs text-slate-400">
              <span>
                {selected ? `与 ${selected} 的会话` : '先选一个 agent'}
                {loading && ' · 读取中…'}
              </span>
              <button
                onClick={refreshConversation}
                className="rounded-lg px-2 py-1 transition hover:bg-slate-800 hover:text-slate-200"
              >
                刷新会话
              </button>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto rounded-xl border border-slate-800/60 bg-slate-950/40 p-3">
              {error && (
                <div className="mb-2 rounded-lg border border-amber-500/40 bg-amber-500/10 p-2 text-xs text-amber-300">
                  会话没读到：{error}
                </div>
              )}
              {!error && selected && lines.length === 0 && !loading && (
                <div className="text-xs text-slate-500">还没有说过话。</div>
              )}
              {lines.map((line) => (
                <Line key={line.id} line={line} />
              ))}
            </div>

            {sendError && (
              <div className="mt-2 rounded-lg border border-red-500/40 bg-red-500/10 p-2 text-xs text-red-300">
                {sendError}
              </div>
            )}
            <div className="mt-2 flex gap-2">
              <textarea
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    submit();
                  }
                }}
                rows={2}
                disabled={!selected || sending}
                placeholder={selected ? '对它说一句，Enter 发送，Shift+Enter 换行' : '先选一个 agent'}
                className="flex-1 resize-none rounded-lg border border-slate-700 bg-slate-950/60 px-3 py-2 text-sm text-slate-200 outline-none focus:border-sky-500/60 disabled:opacity-50"
              />
              <button
                onClick={submit}
                disabled={!selected || sending || !draft.trim()}
                className="rounded-lg border border-sky-500/50 bg-sky-500/10 px-4 py-2 text-sm text-sky-200 transition hover:bg-sky-500/20 disabled:opacity-40"
              >
                {sending ? '等待回答…' : '发送'}
              </button>
            </div>
            <p className="mt-1 text-[11px] text-slate-500">
              发送会等它回话（最多一分钟）；超过窗口它可能仍在跑，那一行会写成“没在等待窗口内回答”，
              不是失败。
            </p>
          </section>
        </div>
      </div>
    </div>
  );
}

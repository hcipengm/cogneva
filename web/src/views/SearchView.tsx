import { useState } from 'react';
import {
  searchKnowledge,
  searchPlatform,
  type KnowledgeSearchResponse,
  type PlatformSearchResponse,
} from '@/api/client';
import { Button, Empty, Loading, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { Note } from '@/ui/Note';

/**
 * 搜索面：一次查询分别问「知识库」与「平台」。
 *
 * 两条检索接口此前只有 API：能力在、能查，人却找不到入口。两个来源分开列，因为
 * 它们的召回与失败方式不同——混成一个列表会把「知识库里没有」和「平台没搜到」说成同一件事。
 *
 * 结果按服务端**实际**答的形状画：
 * - 知识库的每一条带 `source`，说明这一条由哪一层答的（memory:summary /
 *   memory:schema / wiki）。这一列不能省：空结果和「某一层从没开口」在列表里
 *   长得一样，而它们要去看的地方不同。
 * - 平台检索走搜索后端的索引，只答 doc_id / score / highlights，正文不在响应里；
 *   把高亮当正文读会让读者以为这里能读全文。
 *
 * 画面：结果是检索命中，一次查询一份；画成图会掩盖召回本身的样子（顺序、相关度、
 * 来源），而那正是读者要判断的东西。
 */

type Scope = 'knowledge' | 'platform';

export function SearchView() {
  const [text, setText] = useState('');
  const [scope, setScope] = useState<Scope>('knowledge');
  const [asked, setAsked] = useState<string | null>(null);
  const [knowledge, setKnowledge] = useState<KnowledgeSearchResponse | null>(null);
  const [platform, setPlatform] = useState<PlatformSearchResponse | null>(null);
  const [error, setError] = useState<{ scope: Scope; status?: number; message?: string } | null>(null);
  const [busy, setBusy] = useState(false);

  const run = async (next: Scope) => {
    const query = text.trim();
    if (!query) return;
    setScope(next);
    setAsked(query);
    setBusy(true);
    setError(null);
    try {
      if (next === 'knowledge') {
        setKnowledge(await searchKnowledge(query));
        setPlatform(null);
      } else {
        setPlatform(await searchPlatform(query));
        setKnowledge(null);
      }
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setError({ scope: next, status: err.status, message: err.message });
      setKnowledge(null);
      setPlatform(null);
    } finally {
      setBusy(false);
    }
  };

  const retry = () => void run(scope);

  return (
    <ViewFrame
      title="搜索"
      meta={
        asked
          ? `${scope === 'knowledge' ? '知识库' : '平台'} · 「${asked}」`
          : '知识库检索与平台检索，两条接口的入口'
      }
    >
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--line)] px-3 py-2">
        <input
          className="mono w-96 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] text-[var(--paper)] outline-none focus:border-[var(--brass)]"
          placeholder="查什么"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && text.trim()) void run(scope);
          }}
        />
        <Button tone="accent" disabled={busy || !text.trim()} onClick={() => void run('knowledge')}>
          {busy && scope === 'knowledge' ? '查中…' : '查知识库'}
        </Button>
        <Button disabled={busy || !text.trim()} onClick={() => void run('platform')}>
          {busy && scope === 'platform' ? '查中…' : '查平台'}
        </Button>
        <span className="text-[11px] text-[var(--muted)]">
          知识库 POST /knowledge/search · 平台 GET /search?q=&amp;limit=&amp;index=
        </span>
      </div>

      {asked === null ? (
        <Empty>
          还没有查询。选一个来源再查——两个来源的召回不一样，合起来列会把差别藏掉。
        </Empty>
      ) : error ? (
        <ReadError
          what={error.scope === 'knowledge' ? '知识库检索' : '平台检索'}
          error={`HTTP ${error.status ?? '?'}：${error.message ?? '未知原因'}`}
          onRetry={retry}
        />
      ) : busy ? (
        <Loading what="检索结果" />
      ) : knowledge ? (
        <KnowledgeResults page={knowledge} />
      ) : platform ? (
        <PlatformResults page={platform} />
      ) : null}
    </ViewFrame>
  );
}

function KnowledgeResults({ page }: { page: KnowledgeSearchResponse }) {
  if (page.results.length === 0) {
    return (
      <Empty>
        <span className="mono">{page.count}</span> 条命中：知识库里没有与这次查询相关的东西。
        空结果不等于每一层都说没有——某一层没开口时，这里同样是空。
      </Empty>
    );
  }
  return (
    <section>
      <div className="px-4 py-2 text-[11px] text-[var(--muted)]">
        {page.count} 条命中 · 按相关度排 · 每条的来源是答这一条的那一层
      </div>
      <Table
        columns={[
          {
            key: 'source',
            title: '来源',
            width: '190px',
            render: (h) => <span className="mono text-[10px] text-[var(--paper-dim)]">{h.source || '—'}</span>,
          },
          { key: 'title', title: '标题', width: '220px', render: (h) => h.title || '（无标题）' },
          {
            key: 'score',
            title: '相关度',
            width: '80px',
            render: (h) => <span className="mono text-[11px]">{h.relevance_score.toFixed(3)}</span>,
          },
          {
            key: 'content',
            title: '内容',
            render: (h) => (
              <Note text={h.content} className="text-[11px] text-[var(--paper-dim)]">
                <span className="line-clamp-2">{h.content}</span>
              </Note>
            ),
          },
        ]}
        rows={page.results}
        rowKey={(h) => h.id}
      />
    </section>
  );
}

function PlatformResults({ page }: { page: PlatformSearchResponse }) {
  if (page.results.length === 0) {
    return (
      <Empty>
        索引 <span className="mono">{page.index}</span> 里没有与这次查询相关的东西。
        这一条答的是搜索后端；它没配时路由答 500，会走「读不到」，所以空列表就是真的没搜到。
      </Empty>
    );
  }
  return (
    <section>
      <div className="px-4 py-2 text-[11px] text-[var(--muted)]">
        {page.results.length} 条命中 · 索引 <span className="mono">{page.index}</span> ·
        查询串 <span className="mono">{page.query}</span> · 响应里没有正文，只有高亮片段
      </div>
      <Table
        columns={[
          {
            key: 'doc',
            title: '文档',
            width: '280px',
            render: (r) => <span className="mono truncate text-[11px]">{r.doc_id}</span>,
          },
          { key: 'index', title: '索引', width: '140px', render: (r) => <span className="mono text-[11px]">{r.index}</span> },
          {
            key: 'score',
            title: '得分',
            width: '80px',
            render: (r) => <span className="mono text-[11px]">{r.score.toFixed(3)}</span>,
          },
          {
            key: 'highlights',
            title: '高亮',
            render: (r) =>
              r.highlights.length === 0 ? (
                '—'
              ) : (
                <ul className="space-y-0.5 text-[11px] text-[var(--paper-dim)]">
                  {r.highlights.map((line, i) => (
                    <li key={i} className="line-clamp-2">
                      {line}
                    </li>
                  ))}
                </ul>
              ),
          },
        ]}
        rows={page.results}
        rowKey={(r) => `${r.index}/${r.doc_id}`}
      />
    </section>
  );
}

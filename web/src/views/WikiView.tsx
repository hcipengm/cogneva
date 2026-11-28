import { useState } from 'react';
import {
  getWikiDocument,
  getWikiInfo,
  listWikiDocuments,
  searchWiki,
  type WikiDocument,
  type WikiSearchResponse,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { Note } from '@/ui/Note';

/**
 * 知识面：库里有哪几份文档、检索命中哪几份，以及其中一份的正文。
 *
 * 三条读面都在（列表 / 检索 / 正文），此前界面只接了前两条的半截：正文按 `id`
 * 去取，而 `/wiki/document` 认的是 `path`——类型上写着 `path`，调用却传了另一列，
 * 打开任何一份都是空的。现在按路径取，列表里也把路径画出来。
 *
 * 「是否健康」与「库里有什么」是两件事，分开读：适配器没配时 doc 路由答 503，
 * 列表同样 503；而检索答的是适配器自己的召回，空结果不等于库是空的。
 *
 * 画面：这一面的内容就是文档本身——标题、路径、正文。读者要读的是字，把正文排成
 * 条形图没有意义；列表的排序已经按更新时间给出。
 */
export function WikiView() {
  const page = useResource('知识文档', () => listWikiDocuments(), 60_000);
  const info = useResource('知识库适配器', getWikiInfo, 60_000);
  // 文档由 `/api/v1/wiki` 下的写进来（草拟、收录），一次成功写就报一次这个前缀。
  useKindTickedRefresh([resourceKey('wiki')], [page.refresh, info.refresh]);
  const appendOutput = useShell((s) => s.appendOutput);

  const [open, setOpen] = useState<string | null>(null);
  const doc = useResource(
    open === null ? '未选择文档' : `文档 ${open}`,
    async () => (open === null ? null : getWikiDocument(open)),
    0
  );

  const [query, setQuery] = useState('');
  const [hit, setHit] = useState<WikiSearchResponse | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);

  const runSearch = async () => {
    const q = query.trim();
    if (!q) return;
    setSearching(true);
    setSearchError(null);
    try {
      setHit(await searchWiki(q));
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setHit(null);
      setSearchError(`检索失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setSearching(false);
    }
  };

  const openDoc = (path: string) => {
    setOpen(path);
    appendOutput(`打开知识文档 ${path}`);
  };

  if (page.error) return <ReadError what="知识文档" error={page.error} onRetry={page.refresh} />;
  if (!page.data) return <Loading what="知识文档" />;
  const rows = page.data.documents ?? [];

  return (
    <ViewFrame
      title="知识"
      meta={
        info.error
          ? '适配器读不到（列表与检索会各自报自己的错）'
          : info.data
            ? `${info.data.provider} · ${info.data.healthy ? '报健康' : '自报不健康'} · ${rows.length} 份文档`
            : '读取中'
      }
    >
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--line)] px-3 py-2">
        <input
          className="mono w-80 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] text-[var(--paper)] outline-none focus:border-[var(--brass)]"
          placeholder="在知识库里检索"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') void runSearch();
          }}
        />
        <Button tone="accent" disabled={searching || !query.trim()} onClick={() => void runSearch()}>
          {searching ? '检索中…' : '检索'}
        </Button>
        {hit && (
          <Button
            onClick={() => {
              setHit(null);
              setQuery('');
            }}
          >
            收起检索结果
          </Button>
        )}
        <span className="text-[11px] text-[var(--muted)]">
          检索 POST /wiki/search · 正文 GET /wiki/document?path=
        </span>
      </div>

      {searchError && (
        <div className="px-4 py-2 text-[11px]" style={{ color: 'var(--danger)' }}>
          {searchError}
        </div>
      )}
      {hit && (
        <section className="border-b border-[var(--line)]">
          <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
            检索命中 · {hit.count} 条
          </h2>
          {hit.results.length === 0 ? (
            <Empty>
              适配器这次一条都没召回。空结果不等于库里没有——检索答的是召回，列表答的是库存。
            </Empty>
          ) : (
            <Table
              columns={[
                { key: 'title', title: '标题', width: '240px', render: (r) => r.document.title || '（无标题）' },
                {
                  key: 'path',
                  title: '路径',
                  render: (r) => <span className="mono text-[11px] text-[var(--paper-dim)]">{r.document.path}</span>,
                },
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
                    (r.highlights ?? []).length === 0 ? (
                      '—'
                    ) : (
                      <ul className="space-y-0.5 text-[11px] text-[var(--paper-dim)]">
                        {(r.highlights ?? []).map((line, i) => (
                          <li key={i} className="line-clamp-2">
                            {line}
                          </li>
                        ))}
                      </ul>
                    ),
                },
                {
                  key: 'open',
                  title: '',
                  width: '64px',
                  render: (r) => <Button onClick={() => openDoc(r.document.path)}>打开</Button>,
                },
              ]}
              rows={hit.results}
              rowKey={(r) => r.document.path}
            />
          )}
        </section>
      )}

      {rows.length === 0 ? (
        <Empty>
          知识库里没有任何文档。这不是「没有知识」——这一面只读索引，草拟的那一侧还没写过，
          或者索引读到了空集，两种情形在这里长得一样。
        </Empty>
      ) : (
        <Table
          columns={[
            { key: 'title', title: '标题', width: '280px', render: (r: WikiDocument) => r.title || '（无标题）' },
            {
              key: 'path',
              title: '路径',
              render: (r: WikiDocument) => (
                <span className="mono text-[11px] text-[var(--paper-dim)]">{r.path}</span>
              ),
            },
            {
              key: 'tags',
              title: '标签',
              width: '200px',
              render: (r: WikiDocument) =>
                r.tags && r.tags.length > 0 ? (
                  r.tags.join('、')
                ) : (
                  <span className="text-[var(--muted)]">—</span>
                ),
            },
            {
              key: 'updated',
              title: '更新于',
              width: '110px',
              render: (r: WikiDocument) =>
                r.updated_at ? (
                  <Moment iso={r.updated_at} />
                ) : (
                  <Note text="适配器没有给这一份的更新时间">—</Note>
                ),
            },
            {
              key: 'open',
              title: '',
              width: '64px',
              render: (r: WikiDocument) => <Button onClick={() => openDoc(r.path)}>打开</Button>,
            },
          ]}
          rows={rows}
          rowKey={(r) => r.path}
        />
      )}

      {open !== null && (
        <section className="mt-4 rounded border p-3" style={{ borderColor: 'var(--line)' }}>
          <header className="mb-2 flex items-center gap-3 text-[11px]" style={{ color: 'var(--muted)' }}>
            <span className="mono">{open}</span>
            <button className="underline decoration-dotted" onClick={() => setOpen(null)}>
              收起
            </button>
          </header>
          {doc.error ? (
            <ReadError what="文档正文" error={doc.error} onRetry={doc.refresh} />
          ) : !doc.data ? (
            <Loading what="文档正文" />
          ) : (
            <>
              <h2 className="mb-1 text-[13px] text-[var(--paper)]">{doc.data.title}</h2>
              <pre className="whitespace-pre-wrap text-[12px]">{doc.data.content}</pre>
            </>
          )}
        </section>
      )}
    </ViewFrame>
  );
}

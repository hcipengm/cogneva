import { useState } from 'react';
import { getWikiDocument, listWikiDocuments } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, ReadError, Table, ViewFrame } from '@/ui/primitives';

/**
 * 知识面：库里有哪几份文档，以及其中一份的正文。
 *
 * 这一面在此之前只有接口、没有入口——能力在 API 上存在，人却看不到，等于不存在。
 * 它读的两条路径（列表与正文）由网关的 wiki 路由提供，与草拟、检索共用同一份存储。
 */
export function WikiView() {
  const page = useResource('知识文档', () => listWikiDocuments({ limit: 100 }), 60_000);
  const [open, setOpen] = useState<string | null>(null);
  const r0 = (id: string) => id;
  const doc = useResource(
    open === null ? '未选择文档' : `文档 ${open}`,
    async () => (open === null ? null : getWikiDocument(r0(open))),
    0,
  );

  if (page.error) return <ReadError what="知识文档" error={page.error} onRetry={page.refresh} />;
  if (!page.data) return <Loading what="知识文档" />;
  const rows = page.data.documents ?? [];

  return (
    <ViewFrame title="知识" what="库里的文档与正文；草拟与检索走同一份存储">
      {rows.length === 0 ? (
        <Empty>
          知识库里没有任何文档。这不是「没有知识」——这一面只读索引，草拟的那一侧还没写过，
          或者索引读到了空集，两种情形在这里长得一样。
        </Empty>
      ) : (
        <Table
          columns={[
            { key: 'title', title: '标题', render: (r) => r.title },
            { key: 'kind', title: '类型', render: (r) => r.kind ?? '—' },
            {
              key: 'open',
              title: '正文',
              width: '120px',
              render: (r) => (
                <button
                  className="text-[11px] underline decoration-dotted"
                  style={{ color: 'var(--brass)' }}
                  onClick={() => setOpen(r.id)}
                >
                  打开
                </button>
              ),
            },
          ]}
          rows={rows}
        />
      )}
      {open !== null && (
        <section className="mt-4 rounded border p-3" style={{ borderColor: 'var(--line)' }}>
          <header className="mb-2 flex items-center gap-3 text-[11px]" style={{ color: 'var(--muted)' }}>
            <span>{open}</span>
            <button className="underline decoration-dotted" onClick={() => setOpen(null)}>
              收起
            </button>
          </header>
          {doc.error ? (
            <ReadError what="文档正文" error={doc.error} onRetry={doc.refresh} />
          ) : !doc.data ? (
            <Loading what="文档正文" />
          ) : (
            <pre className="whitespace-pre-wrap text-[12px]">{doc.data.content}</pre>
          )}
        </section>
      )}
    </ViewFrame>
  );
}

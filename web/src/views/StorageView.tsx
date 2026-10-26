import { useState } from 'react';
import {
  getMemoryRawContent,
  getMemoryStats,
  listMemoryRaw,
  listMemorySchema,
  listMemorySummary,
  listRawLogs,
  type RawContent,
  type RawLogEntry,
  type RawMetadata,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { BarList } from '@/ui/plots';

const PAGE = 50;
const PREVIEW_CHARS = 4000;

function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

/**
 * 一份原始来源的字节，点开才取。
 *
 * 列表只带长度不带字节，正是为了这个决定：先看见有多大，再决定拉不拉。这里也只
 * 画前一段——把几十兆的工具输出整段塞进 DOM，页面会先卡死，读者也就读不到它。
 */
function RawPreview({ item, onClose }: { item: RawMetadata; onClose: () => void }) {
  const content = useResource<RawContent>(
    `原始来源 ${item.id}`,
    () => getMemoryRawContent(item.id),
    300_000
  );
  const data = content.data;

  return (
    <section className="border-b border-[var(--line)]">
      <div className="flex items-baseline gap-3 px-4 py-2">
        <span className="mono truncate text-[11px] text-[var(--paper-dim)]" title={item.id}>
          {item.id}
        </span>
        <span className="text-[11px] text-[var(--muted)]">
          {item.content_type} · {bytes(item.payload_length)}
        </span>
        <button
          onClick={onClose}
          className="ml-auto text-[11px] text-[var(--muted)] hover:text-[var(--paper)]"
        >
          收起
        </button>
      </div>
      {content.error ? (
        <ReadError what="原始来源" error={content.error} onRetry={content.refresh} />
      ) : !data && content.loading ? (
        <Loading what="原始来源" />
      ) : data ? (
        <>
          <div className="px-4 pb-2 text-[11px] text-[var(--muted)]">
            命名空间 <span className="mono text-[var(--paper-dim)]">{data.namespace}</span> · 编码{' '}
            <span className="mono text-[var(--paper-dim)]">{data.encoding}</span> · 存于{' '}
            <Moment iso={data.created_at} />
          </div>
          <pre className="mono mx-4 mb-3 max-h-64 overflow-auto border border-[var(--line)] p-2 text-[11px] text-[var(--paper-dim)]">
            {data.payload.length > PREVIEW_CHARS
              ? `${data.payload.slice(0, PREVIEW_CHARS)}\n… 只显示前 ${PREVIEW_CHARS} 个字符`
              : data.payload}
          </pre>
        </>
      ) : null}
    </section>
  );
}

/**
 * 存储面：平台自己的记忆后端里到底存了什么。
 *
 * 与资源面分开：资源面读的是盘上的容量与配额（还有多少余量），这一面读的是内容
 * （存了哪些东西、多大、什么时候进来的）。两者不能互相推出来——盘还有空不等于有东西
 * 存进去过，存了很多也不等于盘快满了。
 */
export function StorageView() {
  const [prefix, setPrefix] = useState('');
  const [picked, setPicked] = useState<RawMetadata | null>(null);

  // 筛选条件进标签：这个读面按名字重开，闭包里换参数不会触发重读。
  const raw = useResource(
    prefix ? `原始来源（前缀 ${prefix}）` : '原始来源',
    () => listMemoryRaw({ prefix: prefix || undefined, limit: PAGE }),
    60_000
  );
  const stats = useResource('归档计数', getMemoryStats, 60_000);
  const logs = useResource('日志索引', () => listRawLogs({ limit: PAGE }), 60_000);
  const schema = useResource('结构化记忆', listMemorySchema, 120_000);
  const summary = useResource('摘要记忆', listMemorySummary, 120_000);
  // 存进去、清出去都落在 memory 前缀下；原始日志进的是 raw_logs 那一族。五份一起
  // 重读——上半页的条目与下半页的计数是同一批东西的两个视角，分两次读会自相矛盾。
  useKindTickedRefresh(
    [resourceKey('memory'), resourceKey('raw_logs')],
    [raw.refresh, stats.refresh, logs.refresh, schema.refresh, summary.refresh]
  );

  const listing = raw.data;
  const items = listing?.items ?? [];
  const logEntries = logs.data?.entries ?? [];
  const logBytes = logEntries.reduce((sum, e) => sum + e.size_bytes, 0);
  const logEvents = logEntries.reduce((sum, e) => sum + e.event_count, 0);

  return (
    <ViewFrame
      title="存储"
      meta={
        listing
          ? `列到 ${items.length} 条${listing.total !== undefined ? ` / 共 ${listing.total} 条` : '（这次列全了）'}`
          : '记忆后端里存了什么 · 按 60 秒重读'
      }
    >
      {stats.data && (
        <Readings
          from={stats}
          source="GET /api/v1/memory/stats"
          caveat="这六个数是网关进程自己的计数器：从进程启动算起，重启归零；它们数的是动作，不是存量。"
        >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="归档过的来源" value={stats.data.raw_archived} />
          <Reading label="被读回来的次数" value={stats.data.raw_retrieved} />
          <Reading label="写入的 schema" value={stats.data.schema_stored} />
          <Reading label="schema 查询" value={stats.data.schema_searched} />
          <Reading label="写入的摘要" value={stats.data.summary_stored} />
          <Reading label="摘要查询" value={stats.data.summary_searched} />
        </div>
        </Readings>
      )}
      {stats.data && (
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          这六个数是从网关这个进程启动算起的，重启归零——它们说的是「发生过多少次」，
          不是「现在存着多少」。现在存着多少看下面两张表，那是盘上的东西。
        </p>
      )}
      {/* 六个计数器并排成六格时，读不出「写入比读回多出多少」这种失衡。按长短排开，
          哪一类动作在重复发生就一眼看见——它回答的是动作，不是存量。 */}
      {stats.data && (
        <div className="border-b border-[var(--line)]">
          <BarList
            label="这六类动作各发生过几次"
            note="进程启动算起的累计次数（GET /api/v1/memory/stats），重启归零"
            rows={[
              { label: '归档来源', value: stats.data.raw_archived },
              { label: '读回原始来源', value: stats.data.raw_retrieved },
              { label: '写 schema', value: stats.data.schema_stored },
              { label: '查 schema', value: stats.data.schema_searched },
              { label: '写摘要', value: stats.data.summary_stored },
              { label: '查摘要', value: stats.data.summary_searched },
            ].sort((a, b) => b.value - a.value)}
          />
        </div>
      )}
      <section className="border-b border-[var(--line)]">
        <div className="flex items-baseline gap-2 px-4 py-2">
          <h2 className="text-[11px] uppercase tracking-wider text-[var(--muted)]">原始来源</h2>
          <input
            value={prefix}
            onChange={(e) => setPrefix(e.target.value)}
            placeholder="按内容类型前缀筛（留空看全部）"
            className="mono ml-auto w-64 border border-[var(--line)] bg-transparent px-2 py-1 text-[11px] text-[var(--paper-dim)] outline-none focus:border-[var(--line-strong)]"
          />
        </div>
        {raw.error ? (
          <ReadError what="原始来源" error={raw.error} onRetry={raw.refresh} />
        ) : !listing && raw.loading ? (
          <Loading what="原始来源" />
        ) : items.length === 0 ? (
          <Empty>
            {prefix ? `没有内容类型以「${prefix}」开头的来源。` : '这个命名空间里还没有归档过任何来源。'}
          </Empty>
        ) : (
          <Table
            columns={[
              { key: 'id', title: '来源', width: '320px', render: (r) => (
                <button
                  onClick={() => setPicked(picked?.id === r.id ? null : r)}
                  className="mono block w-full truncate text-left text-[11px] text-[var(--paper-dim)] underline-offset-2 hover:underline"
                  title={r.id}
                >
                  {r.id}
                </button>
              ) },
              { key: 'type', title: '内容类型', width: '160px', render: (r) => (
                <span className="mono text-[11px]">{r.content_type}</span>
              ) },
              { key: 'size', title: '大小', width: '90px', render: (r) => (
                <span className="mono text-[11px]">{bytes(r.payload_length)}</span>
              ) },
              { key: 'at', title: '存于', width: '110px', render: (r) => <Moment iso={r.created_at} /> },
            ]}
            rows={items}
            rowKey={(r) => r.id}
          />
        )}
        {listing?.total !== undefined && (
          <p className="px-4 pb-3 pt-2 text-[11px] text-[var(--warn)]">
            这一页是前缀：后端共 {listing.total} 条，这里只列了最近 {items.length} 条。
          </p>
        )}
        {listing && listing.total === undefined && items.length > 0 && (
          <p className="px-4 pb-3 pt-2 text-[11px] text-[var(--muted)]">
            这一次列全了：没有截断，{items.length} 条就是全部。
          </p>
        )}
      </section>
      {picked && <RawPreview item={picked} onClose={() => setPicked(null)} />}
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          日志归档索引
        </h2>
        {logs.error ? (
          <ReadError what="日志索引" error={logs.error} onRetry={logs.refresh} />
        ) : !logs.data && logs.loading ? (
          <Loading what="日志索引" />
        ) : logEntries.length === 0 ? (
          <Empty>
            索引里没有文件。读不到这张索引与它为空是两件事：这一屏能画出表头，说明读是通的；
            空着只有一种解释——还没人往归档里写过。
          </Empty>
        ) : (
          <>
            <Readings
              from={logs}
              source="GET /api/v1/memory/raw_logs 返回的这一页"
              caveat="三个数只覆盖这一页：翻页、换过滤之后它们各自重算，不是全仓的总量。"
            >
            <div className="flex flex-wrap py-2">
              <Reading label="这一页的文件" value={logEntries.length} />
              <Reading label="这一页的字节" value={bytes(logBytes)} />
              <Reading label="这一页的事件" value={logEvents} />
            </div>
            </Readings>
            <Table
              columns={[
                { key: 'stream', title: '流', width: '150px', render: (e: RawLogEntry) => (
                  <span className="mono text-[11px]">{e.stream_name}</span>
                ) },
                { key: 'date', title: '日期', width: '110px', render: (e) => (
                  <span className="mono text-[11px]">{e.log_date}</span>
                ) },
                { key: 'hour', title: '小时', width: '60px', render: (e) => (
                  <span className="mono text-[11px]">{String(e.hour).padStart(2, '0')}</span>
                ) },
                { key: 'tier', title: '档', width: '80px', render: (e) => (
                  <span className="mono text-[11px]">{e.tier}</span>
                ) },
                { key: 'events', title: '事件', width: '90px', render: (e) => (
                  <span className="mono text-[11px]">{e.event_count}</span>
                ) },
                { key: 'size', title: '字节', width: '90px', render: (e) => (
                  <span className="mono text-[11px]">{bytes(e.size_bytes)}</span>
                ) },
                { key: 'path', title: '文件', render: (e) => (
                  <span className="mono truncate text-[11px] text-[var(--paper-dim)]" title={e.file_path}>
                    {e.file_path}
                  </span>
                ) },
              ]}
              rows={logEntries}
              rowKey={(e) => `${e.stream_name}:${e.log_date}:${e.hour}:${e.format}:${e.file_path}`}
            />
            <BarList
              label="这一页里每个流各占多少字节"
              note="按这一页的 size_bytes 汇总（GET /api/v1/memory/raw_logs），不是整个归档的合计"
              rows={[...logEntries
                .reduce((byStream, entry) => {
                  byStream.set(entry.stream_name, (byStream.get(entry.stream_name) ?? 0) + entry.size_bytes);
                  return byStream;
                }, new Map<string, number>())
                .entries()]
                .sort((a, b) => b[1] - a[1])
                .map(([stream, size]) => ({ id: stream, label: stream, value: size, note: bytes(size) }))}
            />
            <p className="px-4 pb-3 pt-2 text-[11px] text-[var(--muted)]">
              上面三个数只算这一页：它不是整个归档的合计，别拿它当总量。
            </p>
          </>
        )}
      </section>
      <section>
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          结构化记忆的层数
        </h2>
        <div className="flex flex-wrap py-2">
          <Reading
            label="schema 条数"
            value={schema.data ? schema.data.length : schema.error ? '读不到' : '…'}
            note="全量列举，后端不分页"
          />
          <Reading
            label="summary 条数"
            value={summary.data ? summary.data.length : summary.error ? '读不到' : '…'}
            note="全量列举，后端不分页"
          />
        </div>
        {(schema.error || summary.error) && (
          <ReadError
            what="结构化记忆"
            error={schema.error ?? summary.error ?? ''}
            onRetry={() => {
              schema.refresh();
              summary.refresh();
            }}
          />
        )}
        <p className="px-4 pb-4 text-[11px] text-[var(--muted)]">
          这两层各自整份读回来再数：后端没有「只给条数」的读面，所以条目多的时候这一屏会慢。
          慢不是坏，它只是这条读面的成本。
        </p>
      </section>
    </ViewFrame>
  );
}

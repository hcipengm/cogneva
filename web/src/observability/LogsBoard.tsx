import { listRawLogs } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, Table } from '@/ui/primitives';

/**
 * 日志板：归档索引，不是日志正文。
 *
 * 这里列的是盘上真的有的那些归档文件——一条一份，带大小与事件条数。正文在存储面
 * （那一面知道怎么按内容类型拆开、怎么截断、怎么取字节），这一面只回答「有什么可读」。
 *
 * `count` 是这一页返回的行数，不是归档总量：读回来 50 行时，第 51 份归档是否存在
 * 这一面并不知道，所以标题写「这一页」，不写「共」。
 */
export function LogsBoard() {
  const setActivity = useShell((s) => s.setActivity);
  const page = useResource('日志归档', () => listRawLogs({ limit: 50 }), 60_000);

  if (page.error) {
    return <ReadError what="日志归档" error={page.error} onRetry={page.refresh} />;
  }
  if (!page.data) return <Loading what="日志归档" />;

  const entries = page.data.entries ?? [];
  if (entries.length === 0) {
    return (
      <Empty>
        归档索引里没有任何文件。这不是「没有日志」——这一面只读索引表，写归档的那一侧
        还没写过，或者索引表读到了空集，两种情形在这里长得一样。
      </Empty>
    );
  }

  return (
    <>
      <div className="flex items-center gap-3 px-4 py-2 text-[11px] text-[var(--muted)]">
        <span>这一页 {page.data.count} 份归档</span>
        <Button onClick={page.refresh}>重读</Button>
        <div className="ml-auto">
          <Button tone="accent" onClick={() => setActivity('storage')}>
            去存储面看正文
          </Button>
        </div>
      </div>
      <Table
        columns={[
          { key: 'stream', title: '流', width: '200px', render: (e) => (
            <span className="mono text-[11px]">{e.stream_name}</span>
          ) },
          { key: 'when', title: '日期 · 时', width: '130px', render: (e) => (
            <span className="mono text-[11px] text-[var(--paper-dim)]">
              {e.log_date} · {String(e.hour).padStart(2, '0')} 时
            </span>
          ) },
          { key: 'tier', title: '档', width: '80px', render: (e) => (
            <span className="text-[11px] text-[var(--muted)]">{e.tier}</span>
          ) },
          { key: 'format', title: '格式', width: '90px', render: (e) => (
            <span className="mono text-[11px] text-[var(--muted)]">{e.format}</span>
          ) },
          { key: 'events', title: '事件', width: '90px', render: (e) => (
            <span className="mono text-[11px]">{e.event_count.toLocaleString()}</span>
          ) },
          { key: 'size', title: '大小', width: '90px', render: (e) => (
            <span className="mono text-[11px] text-[var(--muted)]">
              {e.size_bytes >= 1024 * 1024
                ? `${(e.size_bytes / 1024 / 1024).toFixed(1)} MB`
                : `${Math.round(e.size_bytes / 1024)} KB`}
            </span>
          ) },
          { key: 'path', title: '文件', render: (e) => (
            <span className="mono text-[11px] text-[var(--muted)]">{e.file_path}</span>
          ) },
        ]}
        rows={entries}
        rowKey={(e) => `${e.tier}|${e.file_path}`}
      />
    </>
  );
}

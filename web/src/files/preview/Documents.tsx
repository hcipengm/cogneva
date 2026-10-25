import { useEffect, useState } from 'react';
import { getWorkspaceFileBytes } from '@/api/client';
import { readDocx, type DocxDocument } from '@/files/preview/docx';
import { columnName, readXlsx, MAX_COLUMNS, MAX_ROWS, type Sheet } from '@/files/preview/xlsx';
import { Empty, Loading, ReadError } from '@/ui/primitives';

/**
 * Word 与 Excel 的阅读面。
 *
 * 两种文档都在浏览器里拆（ZIP + XML），不经服务端转换：转换这件事一旦放到服务端，
 * 就得为每一种办公格式的每一个版本维护一份解析器，而读者要的只是这一份里写了什么。
 * 拆不动的时候把原因原样说出来，并留一个下载——「打不开」和「没有内容」必须
 * 分得开。
 */

function useBytes(path: string) {
  const [buffer, setBuffer] = useState<ArrayBuffer | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setBuffer(null);
    getWorkspaceFileBytes(path)
      .then((bytes) => {
        if (!cancelled) setBuffer(bytes);
      })
      .catch((e: { message?: string }) => {
        if (!cancelled) setError(e.message ?? '读取失败');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [path, attempt]);

  return { buffer, error, loading, retry: () => setAttempt((n) => n + 1) };
}

export function DocxView({ path }: { path: string }) {
  const { buffer, error, loading, retry } = useBytes(path);
  const [document, setDocument] = useState<DocxDocument | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);

  useEffect(() => {
    if (!buffer) return;
    let cancelled = false;
    setParseError(null);
    readDocx(buffer)
      .then((parsed) => {
        if (!cancelled) setDocument(parsed);
      })
      .catch((e: { message?: string }) => {
        if (!cancelled) setParseError(e.message ?? '拆不开这份文档');
      });
    return () => {
      cancelled = true;
    };
  }, [buffer]);

  if (loading) return <Loading what={path} />;
  if (error) return <div className="px-4 py-3"><ReadError what={path} error={error} onRetry={retry} /></div>;
  if (parseError) {
    return (
      <div className="px-4 py-3">
        <Empty>{parseError}。可以下载后用本机的 Word 打开。</Empty>
      </div>
    );
  }
  if (!document) return <Loading what={path} />;

  if (document.blocks.length === 0) {
    return (
      <div className="px-4 py-3">
        <Empty>这份文档里没有可读的正文段落（可能全是图片或空白）。</Empty>
      </div>
    );
  }

  return (
    <div className="min-h-0 flex-1 overflow-y-auto px-8 py-5">
      <article className="mx-auto flex max-w-[72ch] flex-col gap-2.5">
        {document.blocks.map((block, index) => {
          const runs = block.runs.map((run, runIndex) => (
            <span
              key={runIndex}
              className={run.code ? 'mono text-[12px]' : undefined}
              style={{
                fontWeight: run.bold ? 500 : undefined,
                fontStyle: run.italic ? 'italic' : undefined,
                color: block.kind === 'heading' ? 'var(--paper)' : 'var(--paper-dim)',
              }}
            >
              {run.text}
            </span>
          ));

          if (block.kind === 'heading') {
            return (
              <h3
                key={index}
                className="mt-3 font-medium tracking-tight text-[var(--paper)]"
                style={{ fontSize: block.level <= 2 ? '14px' : '13px' }}
              >
                {runs}
              </h3>
            );
          }
          if (block.kind === 'listItem') {
            return (
              <p key={index} className="ml-4 flex gap-2 text-[12.5px] leading-[1.8]">
                <span className="shrink-0 text-[var(--muted)]">·</span>
                <span className="whitespace-pre-wrap">{runs}</span>
              </p>
            );
          }
          return (
            <p key={index} className="whitespace-pre-wrap text-[12.5px] leading-[1.8]">
              {runs}
            </p>
          );
        })}
      </article>
      {document.unsupported.length > 0 && (
        <p className="mono mx-auto mt-6 max-w-[72ch] text-[11px] text-[var(--muted)]">
          这一面读不出来的部分：{document.unsupported.join('、')}。要原样看，下载后用 Word 打开。
        </p>
      )}
    </div>
  );
}

export function XlsxView({ path }: { path: string }) {
  const { buffer, error, loading, retry } = useBytes(path);
  const [sheets, setSheets] = useState<Sheet[] | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [active, setActive] = useState(0);

  useEffect(() => {
    if (!buffer) return;
    let cancelled = false;
    setParseError(null);
    readXlsx(buffer)
      .then((parsed) => {
        if (!cancelled) {
          setSheets(parsed);
          setActive(0);
        }
      })
      .catch((e: { message?: string }) => {
        if (!cancelled) setParseError(e.message ?? '拆不开这份表格');
      });
    return () => {
      cancelled = true;
    };
  }, [buffer]);

  if (loading) return <Loading what={path} />;
  if (error) return <div className="px-4 py-3"><ReadError what={path} error={error} onRetry={retry} /></div>;
  if (parseError) {
    return (
      <div className="px-4 py-3">
        <Empty>{parseError}。可以下载后用本机的 Excel 打开。</Empty>
      </div>
    );
  }
  if (!sheets) return <Loading what={path} />;

  const sheet = sheets[active];
  const columns = Math.min(sheet.columnTotal || 1, MAX_COLUMNS);
  const rows = sheet.rows;
  const truncatedRows = sheet.rowTotal > MAX_ROWS;
  const truncatedColumns = sheet.columnTotal > MAX_COLUMNS;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {sheets.length > 1 && (
        <div className="flex shrink-0 items-center gap-1 border-b border-[var(--line)] px-4 py-1">
          {sheets.map((entry, index) => (
            <button
              key={entry.name}
              type="button"
              onClick={() => setActive(index)}
              className="px-2 py-0.5 text-[11px] transition-colors"
              style={{
                color: index === active ? 'var(--paper)' : 'var(--muted)',
                background: index === active ? 'var(--ink-700)' : 'transparent',
              }}
            >
              {entry.name}
            </button>
          ))}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-auto">
        <table className="border-collapse text-[11.5px]">
          <thead>
            <tr>
              <th className="sticky left-0 top-0 z-20 w-10 border border-[var(--line)] bg-[var(--ink-800)] px-1 py-0.5 text-right font-normal text-[var(--muted)]" />
              {Array.from({ length: columns }, (_, index) => (
                <th
                  key={index}
                  className="sticky top-0 z-10 min-w-[80px] border border-[var(--line)] bg-[var(--ink-800)] px-1.5 py-0.5 text-left font-normal text-[var(--muted)]"
                >
                  {columnName(index)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                <th className="sticky left-0 z-10 border border-[var(--line)] bg-[var(--ink-800)] px-1 py-0.5 text-right font-normal text-[var(--muted)]">
                  {rowIndex + 1}
                </th>
                {Array.from({ length: columns }, (_, columnIndex) => {
                  const cell = row[columnIndex];
                  const text = cell?.text ?? '';
                  return (
                    <td
                      key={columnIndex}
                      className={`max-w-[280px] truncate border border-[var(--line)] px-1.5 py-0.5 ${
                        cell?.kind === 'n' ? 'mono text-right' : ''
                      }`}
                      style={{
                        color:
                          cell?.kind === 'e'
                            ? 'var(--danger)'
                            : text
                              ? 'var(--paper-dim)'
                              : 'transparent',
                      }}
                      title={text}
                    >
                      {text || '·'}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="mono shrink-0 border-t border-[var(--line)] px-4 py-1 text-[10px] text-[var(--muted)]">
        这张表共 {sheet.rowTotal} 行 × {sheet.columnTotal} 列
        {truncatedRows || truncatedColumns
          ? `；这里只画了前 ${Math.min(sheet.rowTotal, MAX_ROWS)} 行、前 ${Math.min(sheet.columnTotal, MAX_COLUMNS)} 列，其余下载原文件看`
          : ''}
      </p>
    </div>
  );
}

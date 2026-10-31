import { useEffect, useRef, useState } from 'react';
import { searchWorkspace, type WorkspaceSearchResult } from '@/api/client';
import { useShell } from '@/shell/store';
import { Note } from '@/ui/Note';

/**
 * 工作区搜索：一个输入框，一份「哪个文件里有」的清单。
 *
 * 与编辑器里的查找分工不同：那条只在一个已打开的文件里走，回答「这一份里还有
 * 没有下一处」；这一条跨文件，回答「在哪一个文件里」，所以它的结果是路径加行号，
 * 点一下要做的不是「选中下一处」而是「打开那个文件并落到那一行」。
 *
 * 输入停顿之后才发请求：每敲一个字母发一次，回来的顺序不保证，屏上显示的就会
 * 是上一次查询的结果配这一次的字。
 */
const DEBOUNCE_MS = 220;

export function SearchPanel() {
  const [query, setQuery] = useState('');
  const [result, setResult] = useState<WorkspaceSearchResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const openTab = useShell((s) => s.openTab);
  const setReveal = useShell((s) => s.setReveal);
  // 只让最后一次请求写状态：慢的那一次回来得晚，不能盖掉新的结果。
  const seq = useRef(0);

  useEffect(() => {
    const trimmed = query.trim();
    if (!trimmed) {
      setResult(null);
      setError(null);
      setBusy(false);
      return;
    }
    const mine = ++seq.current;
    setBusy(true);
    const timer = window.setTimeout(() => {
      void (async () => {
        try {
          const page = await searchWorkspace(trimmed);
          if (seq.current !== mine) return;
          setResult(page);
          setError(null);
        } catch (e) {
          if (seq.current !== mine) return;
          setError((e as { message?: string }).message ?? '搜索失败');
          setResult(null);
        } finally {
          if (seq.current === mine) setBusy(false);
        }
      })();
    }, DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [query]);

  const open = (path: string, line: number, needle: string) => {
    openTab({
      id: `file:${path}`,
      title: path.split('/').pop() ?? path,
      view: 'files',
      hint: path,
    });
    setReveal({ path, line, query: needle });
  };

  const shown = result?.matches ?? [];

  return (
    <section className="border-b border-[var(--line)] pb-2">
      <div className="flex items-center px-3 py-1.5">
        <h2 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">搜索</h2>
        {busy && <span className="ml-2 text-[10px] text-[var(--muted)]">搜索中…</span>}
      </div>
      <div className="px-2 pb-1">
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="在整个工作区里找（不区分英文大小写）"
          className="mono w-full border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none focus:border-[var(--brass)]"
          style={{ color: 'var(--paper)' }}
        />
      </div>
      {error && (
        <p className="px-3 py-1 text-[11px]" style={{ color: 'var(--danger)' }}>
          {error}
        </p>
      )}
      {result && (
        <p className="px-3 py-1 text-[10px] text-[var(--muted)]">
          走了 {result.scanned_files} 个文件 · {shown.length} 处
          {result.truncated ? ' · 到上限了，只列了这些' : ''}
        </p>
      )}
      {result && shown.length === 0 && !busy && (
        <p className="px-3 py-1 text-[11px] text-[var(--muted)]">没有匹配</p>
      )}
      <div className="max-h-[40vh] overflow-y-auto">
        {shown.map((match) => (
          <Note
            as="div"
            key={`${match.path}:${match.line}:${match.column}`}
            role="button"
            tabIndex={0}
            onClick={() => open(match.path, match.line, result?.query ?? '')}
            onKeyDown={(event) => {
              if (event.key !== 'Enter' && event.key !== ' ') return;
              event.preventDefault();
              open(match.path, match.line, result?.query ?? '');
            }}
            text={`${match.path}:${match.line}:${match.column}`}
            className="cursor-pointer px-3 py-[2px] text-[11px] transition-colors hover:bg-[var(--ink-700)]"
          >
            <div className="mono truncate" style={{ color: 'var(--paper-dim)' }}>
              {match.path}
              <span className="text-[var(--muted)]">:{match.line}</span>
            </div>
            <div
              className="mono truncate pl-2"
              style={{ color: 'var(--muted)' }}
            >
              {match.text.trim()}
            </div>
          </Note>
        ))}
      </div>
    </section>
  );
}

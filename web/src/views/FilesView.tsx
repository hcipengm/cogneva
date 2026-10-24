import { useCallback, useEffect, useState } from 'react';
import {
  downloadWorkspaceFile,
  getWorkspaceFile,
  saveWorkspaceFile,
  type WorkspaceFile,
} from '@/api/client';
import { CodeEditor } from '@/files/CodeEditor';
import { findMatches } from '@/files/highlight';
import { useDrafts } from '@/files/store';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError } from '@/ui/primitives';

/**
 * 文件面：一页一个文件。
 *
 * 页的身份是 `file:<路径>`，所以同一个文件不会开出两页、不同文件各占一页。
 * 编辑中的内容是草稿（`files/store`），保存才写回服务端：每次按键都发一次
 * PUT 会让「盘上的内容」取决于最后一次网络请求有没有成功，而这在编辑器里
 * 是最不该出现的语义。
 */
export function FilesView() {
  const tabs = useShell((s) => s.tabs);
  const activeTabId = useShell((s) => s.activeTabId);
  const appendOutput = useShell((s) => s.appendOutput);
  const openTab = useShell((s) => s.openTab);

  const active = tabs.find((tab) => tab.id === activeTabId);
  const path = active && active.view === 'files' && active.id.startsWith('file:')
    ? active.id.slice('file:'.length)
    : null;

  const reveal = useShell((s) => s.reveal);
  const setReveal = useShell((s) => s.setReveal);

  const draft = useDrafts((s) => (path ? s.drafts[path] : undefined));
  const setDraft = useDrafts((s) => s.setDraft);
  const clearDraft = useDrafts((s) => s.clearDraft);

  const [file, setFile] = useState<WorkspaceFile | null>(null);
  const [error, setError] = useState<{ message: string } | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  /** 保存被 409 挡下时的提示；空表示没有冲突。 */
  const [conflict, setConflict] = useState<string | null>(null);
  const [newPath, setNewPath] = useState('');
  const [findOpen, setFindOpen] = useState(false);
  const [findQuery, setFindQuery] = useState('');
  const [findIndex, setFindIndex] = useState(0);

  const load = useCallback(async (target: string) => {
    setLoading(true);
    setError(null);
    try {
      const page = await getWorkspaceFile(target);
      setFile(page);
      // 重读之后手里的底稿就是盘上这一份：冲突已经处理过，条子该收了。
      setConflict(null);
    } catch (e) {
      setError({ message: (e as { message?: string }).message ?? '读取失败' });
      setFile(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!path) return;
    setFile(null);
    void load(path);
  }, [path, load]);

  const text = draft ?? file?.text ?? '';
  const dirty = draft !== undefined && draft !== file?.text;
  const lines = text ? text.split('\n').length : 0;
  const matches = findOpen ? findMatches(text, findQuery) : [];

  // 搜索结果是「哪个文件、第几行」。落到哪儿由这一层算：只有它手里有正文。
  useEffect(() => {
    if (!path || !file || !reveal || reveal.path !== path) return;
    const hits = findMatches(text, reveal.query);
    const index = hits.findIndex(
      ([start]) => text.slice(0, start).split('\n').length - 1 === reveal.line - 1
    );
    setFindOpen(true);
    setFindQuery(reveal.query);
    setFindIndex(index >= 0 ? index : 0);
    setReveal(null);
  }, [path, file, text, reveal, setReveal]);

  /**
   * 保存。默认拿着读进来时的指纹去写：盘上那一份在这期间被别的写者换过，服务端
   * 回 409，这里把它摆成一条要人选的岔路，而不是把对方的改动盖掉。`force` 是
   * 读者自己选的那条「我就是要覆盖」。
   */
  const save = useCallback(
    async (force = false) => {
      if (!path || !dirty || saving) return;
      setSaving(true);
      try {
        const saved = await saveWorkspaceFile(path, text, force ? null : file?.hash);
        setFile(saved);
        clearDraft(path);
        setConflict(null);
        appendOutput(`已保存 ${path}（${saved.size} 字节）`);
      } catch (e) {
        const status = (e as { status?: number }).status;
        if (status === 409 && !force) {
          // 服务端说的是「你手里那份底稿已经不在了」。原文是给机器看的一行 JSON，
          // 这里换成读者能照着做的两个动作。
          setConflict(`盘上的 ${path} 在你打开之后被改过（另一个会话或智能体写过）。`);
        } else {
          const message = (e as { message?: string }).message ?? '保存失败';
          appendOutput(`保存 ${path} 失败：${message}`);
          setError({ message });
        }
      } finally {
        setSaving(false);
      }
    },
    [path, dirty, saving, text, file, clearDraft, appendOutput]
  );

  const createFile = async () => {
    const target = newPath.trim();
    if (!target) return;
    try {
      const created = await saveWorkspaceFile(target, '');
      setNewPath('');
      appendOutput(`已新建 ${target}`);
      openTab({ id: `file:${created.path}`, title: target.split('/').pop() ?? target, view: 'files', hint: created.path });
    } catch (e) {
      setError({ message: (e as { message?: string }).message ?? '新建失败' });
    }
  };

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const meta = event.metaKey || event.ctrlKey;
      if (meta && event.key.toLowerCase() === 's') {
        event.preventDefault();
        void save();
      } else if (meta && event.key.toLowerCase() === 'f') {
        event.preventDefault();
        setFindOpen(true);
      } else if (event.key === 'Escape' && findOpen) {
        setFindOpen(false);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [save, findOpen]);

  if (!path) {
    return (
      <div className="flex h-full flex-col">
        <header className="flex shrink-0 items-center gap-3 border-b border-[var(--line)] px-4 py-2">
          <h1 className="text-[15px] font-medium tracking-tight">文件</h1>
          <span className="text-[11px] text-[var(--muted)]">
            左边是工作区；点一个文件就在这里打开，一页一个文件
          </span>
        </header>
        <div className="flex min-h-0 flex-1 flex-col items-center justify-center gap-3">
          <Empty>还没有打开文件。从左侧工作区里点一个，或在这里新建：</Empty>
          <div className="flex items-center gap-2">
            <input
              value={newPath}
              onChange={(e) => setNewPath(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') void createFile();
              }}
              placeholder="例如 notes/todo.md"
              className="mono w-64 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[12px] outline-none focus:border-[var(--brass)]"
              style={{ color: 'var(--paper)' }}
            />
            <Button onClick={() => void createFile()} tone="accent">
              新建
            </Button>
          </div>
          {error && (
            <p className="mono text-[11px]" style={{ color: 'var(--danger)' }}>
              {error.message}
            </p>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <header className="flex shrink-0 items-center gap-3 border-b border-[var(--line)] px-4 py-2">
        <h1 className="mono text-[13px] font-medium tracking-tight" title={path}>
          {path}
        </h1>
        {dirty && (
          <span className="text-[10px] uppercase tracking-[0.14em]" style={{ color: 'var(--warn)' }}>
            未保存
          </span>
        )}
        <span className="text-[11px] text-[var(--muted)]">
          {file ? `${file.size} 字节 · ${lines} 行` : '读取中'}
          {file?.truncated ? ' · 超过上限，只显示前一段' : ''}
          {file?.binary ? ' · 二进制文件不显示正文' : ''}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <Button onClick={() => setFindOpen((open) => !open)}>查找 ⌘F</Button>
          <Button onClick={() => void downloadWorkspaceFile(path)}>下载</Button>
          <Button onClick={() => void load(path)}>重读</Button>
          <Button onClick={() => void save()} tone={dirty ? 'accent' : undefined}>
            {saving ? '保存中…' : '保存 ⌘S'}
          </Button>
        </div>
      </header>

      {conflict && (
        <div
          className="flex shrink-0 items-center gap-3 border-b border-[var(--line)] px-4 py-1.5"
          style={{ background: 'var(--ink-800)' }}
        >
          <span className="text-[11px]" style={{ color: 'var(--warn)' }}>
            {conflict}
          </span>
          <div className="ml-auto flex items-center gap-2">
            <Button onClick={() => void save(true)} tone="accent">
              仍要覆盖
            </Button>
            <Button
              onClick={() => {
                if (path) void load(path);
                setConflict(null);
              }}
            >
              重读（留下我的版本）
            </Button>
          </div>
        </div>
      )}

      {error && (
        <div className="px-4 py-2">
          <ReadError
            what={path}
            error={error.message}
            onRetry={() => void load(path)}
          />
        </div>
      )}

      {findOpen && (
        <div className="flex shrink-0 items-center gap-2 border-b border-[var(--line)] bg-[var(--ink-800)] px-4 py-1">
          <input
            autoFocus
            value={findQuery}
            onChange={(event) => {
              setFindQuery(event.target.value);
              setFindIndex(0);
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                const step = event.shiftKey ? -1 : 1;
                setFindIndex((current) => current + step);
              }
            }}
            placeholder="查找（回车下一处，Shift+回车上一处）"
            className="mono w-72 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[12px] outline-none focus:border-[var(--brass)]"
            style={{ color: 'var(--paper)' }}
          />
          <span className="mono text-[11px] text-[var(--muted)]">
            {findQuery
              ? matches.length === 0
                ? '没有匹配'
                : `${(((findIndex % matches.length) + matches.length) % matches.length) + 1} / ${matches.length}`
              : '输入要查找的文字'}
          </span>
          <button
            type="button"
            onClick={() => setFindOpen(false)}
            className="ml-auto text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
          >
            关闭
          </button>
        </div>
      )}

      {loading && !file ? (
        <Loading what={path} />
      ) : file?.binary ? (
        <Empty>这是一个二进制文件（{file.size} 字节），不在编辑器里打开。</Empty>
      ) : (
        <CodeEditor
          path={path}
          text={text}
          findQuery={findOpen ? findQuery : undefined}
          findIndex={findIndex}
          onChange={(next) => {
            setDraft(path, next);
          }}
        />
      )}
    </div>
  );
}

import { useCallback, useEffect, useState } from 'react';
import {
  downloadWorkspaceFile,
  getWorkspaceFile,
  saveWorkspaceFile,
  type FileSource,
  type WorkspaceFile,
} from '@/api/client';
import { CodeEditor } from '@/files/CodeEditor';
import { findMatches, replaceMatches } from '@/files/highlight';
import { DocxView, XlsxView } from '@/files/preview/Documents';
import { Html } from '@/files/preview/Html';
import { KIND_LABEL, needsBytes, previewKindOf } from '@/files/preview/kind';
import { Markdown } from '@/files/preview/Markdown';
import { Tex } from '@/files/preview/Tex';
import { SOURCE_HINT, SOURCE_LABEL, parseFileTab } from '@/files/source';
import { useDrafts } from '@/files/store';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError } from '@/ui/primitives';

/**
 * 文件面：一页一个文件。
 *
 * 页的身份是 `file:<路径>`，所以同一个文件不会开出两页、不同文件各占一页。
 * 编辑中的内容是草稿（`files/store`），保存才写回服务端：每次按键都发一次
 * PUT 会让「盘上的内容」取决于最后一次网络请求有没有成功，而这在编辑器里
 * 是最不该出现的语义。
 *
 * 画面：这一面要显示的就是文件本身——源码、排版后的成品、替换命中的那几行。文件
 * 内容就是画面，任何统计图都只是把读者真正在找的那几行往下挤。
 */
export function FilesView() {
  const tabs = useShell((s) => s.tabs);
  const activeTabId = useShell((s) => s.activeTabId);
  const appendOutput = useShell((s) => s.appendOutput);
  const openTab = useShell((s) => s.openTab);

  const active = tabs.find((tab) => tab.id === activeTabId);
  const opened = active && active.view === 'files' ? parseFileTab(active.id) : null;
  const path = opened?.path ?? null;
  /** 项目面只读：它的字节由部署器建镜像、由智能体改，一条网页里的保存会绕过这两条路。 */
  const source = opened?.source ?? 'workspace';
  const readOnly = source === 'project';

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
  const [replaceText, setReplaceText] = useState('');
  const [gotoOpen, setGotoOpen] = useState(false);
  const [gotoInput, setGotoInput] = useState('');
  /** 跳转请求。`serial` 让「同一行再跳一次」也真的发生。 */
  const [goto, setGoto] = useState<{ line: number; serial: number } | null>(null);
  /**
   * 打开有排版可言的文档时先给成品，源码在第二栏。
   *
   * 一份 md 打开就是一堆井号，读者得自己在脑子里排版——那不是在读文档，是在读
   * 源码。反过来，只有源码可看时又把排版关起来，想改一个字都做不到。两条都给，
   * 默认落在成品上。
   */
  const [preview, setPreview] = useState(true);

  const load = useCallback(async (target: string, from: FileSource) => {
    setLoading(true);
    setError(null);
    try {
      const page = await getWorkspaceFile(target, from);
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
    setPreview(true);
    void load(path, source);
  }, [path, source, load]);

  // 盘上这个文件被别处改过——另一个面保存、一次提交、一次外部写——就落在 ide 前缀
  // 下。不订这一族的话，别人改完之后这一页要关掉重开才看得见新内容，而「刚才那笔
  // 提交改了什么」正是这一页要答的问题。
  useKindTickedRefresh([resourceKey('ide')], () => {
    if (path) void load(path, source);
  });

  const kind = path ? previewKindOf(path) : 'text';
  /** 这一种文件有没有「成品」可看：源码文件没有，它本身就是成品。 */
  const hasPreview = kind !== 'text';
  const text = draft ?? file?.text ?? '';
  const dirty = draft !== undefined && draft !== file?.text;
  const lines = text ? text.split('\n').length : 0;
  const matches = findOpen ? findMatches(text, findQuery) : [];
  /** 查找框里显示的「第几处」，按命中数为 0 时回到第 1 处处理。 */
  const resolveFindIndex = (): number => {
    if (matches.length === 0) return 0;
    return ((findIndex % matches.length) + matches.length) % matches.length;
  };

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
      if (!path || !dirty || saving || readOnly) return;
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
    [path, dirty, saving, readOnly, text, file, clearDraft, appendOutput]
  );

  /**
   * 替换。落在草稿上、不落盘：改完还要按一次保存才算写进工作区。
   *
   * 这一条与「保存」分开是有意的——替换全部一次能改几十处，直接写盘的话读者
   * 只能靠撤销找回原样；改草稿则一次关掉页签就能整份放弃。
   */
  const replace = (all: boolean) => {
    if (!path || !findQuery) return;
    const outcome = replaceMatches(text, findQuery, replaceText, all ? undefined : resolveFindIndex());
    if (outcome.replaced === 0) {
      appendOutput(`没有可替换的「${findQuery}」`);
      return;
    }
    setDraft(path, outcome.text);
    appendOutput(`替换 ${outcome.replaced} 处「${findQuery}」（还没保存）`);
    setFindIndex(0);
  };

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
      } else if (meta && event.key.toLowerCase() === 'g') {
        event.preventDefault();
        setGotoOpen(true);
      } else if (event.key === 'Escape' && findOpen) {
        setFindOpen(false);
      } else if (event.key === 'Escape' && gotoOpen) {
        setGotoOpen(false);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [save, findOpen, gotoOpen]);

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
        <span
          title={SOURCE_HINT[source]}
          className="shrink-0 border px-1.5 py-0.5 text-[10px]"
          style={{
            borderColor: readOnly ? 'var(--warn)' : 'var(--line)',
            color: readOnly ? 'var(--warn)' : 'var(--muted)',
          }}
        >
          {SOURCE_LABEL[source]}
          {readOnly ? ' · 只读' : ''}
        </span>
        <h1 className="mono text-[13px] font-medium tracking-tight" title={path}>
          {path}
        </h1>
        {dirty && !readOnly && (
          <span className="text-[10px] uppercase tracking-[0.14em]" style={{ color: 'var(--warn)' }}>
            未保存
          </span>
        )}
        <span className="text-[11px] text-[var(--muted)]">
          {file ? `${file.size} 字节 · ${lines} 行` : '读取中'}
          {file?.truncated ? ' · 超过上限，只显示前一段' : ''}
          {file?.binary && !needsBytes(kind) ? ' · 二进制文件不显示正文' : ''}
        </span>
        <div className="ml-auto flex items-center gap-2">
          {hasPreview && !file?.binary && (
            <span className="flex items-center border border-[var(--line)]">
              {(['preview', 'source'] as const).map((mode) => {
                const active = mode === 'preview' ? preview : !preview;
                return (
                  <button
                    key={mode}
                    type="button"
                    onClick={() => setPreview(mode === 'preview')}
                    className="px-2 py-0.5 text-[11px] transition-colors"
                    style={{
                      color: active ? 'var(--paper)' : 'var(--muted)',
                      background: active ? 'var(--ink-700)' : 'transparent',
                    }}
                  >
                    {mode === 'preview' ? KIND_LABEL[kind] : '源码'}
                  </button>
                );
              })}
            </span>
          )}
          <Button onClick={() => setFindOpen((open) => !open)}>查找 ⌘F</Button>
          <Button onClick={() => setGotoOpen((open) => !open)}>跳到行 ⌘G</Button>
          <Button onClick={() => void downloadWorkspaceFile(path, source)}>下载</Button>
          <Button onClick={() => void load(path, source)}>重读</Button>
          {!readOnly && (
            <Button onClick={() => void save()} tone={dirty ? 'accent' : undefined}>
              {saving ? '保存中…' : '保存 ⌘S'}
            </Button>
          )}
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
                if (path) void load(path, source);
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
            onRetry={() => void load(path, source)}
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
          <input
            value={replaceText}
            onChange={(event) => setReplaceText(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                replace(event.shiftKey ? true : false);
              }
            }}
            placeholder="替换为（回车替换这一处，Shift+回车全部）"
            className="mono w-72 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[12px] outline-none focus:border-[var(--brass)]"
            style={{ color: 'var(--paper)' }}
          />
          <Button onClick={() => replace(false)} disabled={!findQuery}>
            替换
          </Button>
          <Button onClick={() => replace(true)} disabled={!findQuery} tone="accent">
            全部替换
          </Button>
          <button
            type="button"
            onClick={() => setFindOpen(false)}
            className="ml-auto text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
          >
            关闭
          </button>
        </div>
      )}

      {gotoOpen && (
        <div className="flex shrink-0 items-center gap-2 border-b border-[var(--line)] bg-[var(--ink-800)] px-4 py-1">
          <input
            autoFocus
            value={gotoInput}
            onChange={(event) => setGotoInput(event.target.value.replace(/[^0-9]/g, ''))}
            onKeyDown={(event) => {
              if (event.key !== 'Enter') return;
              event.preventDefault();
              const line = Number(gotoInput);
              if (!Number.isFinite(line) || line < 1) return;
              setGoto({ line, serial: Date.now() });
              setGotoOpen(false);
            }}
            placeholder="第几行"
            className="mono w-24 border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[12px] outline-none focus:border-[var(--brass)]"
            style={{ color: 'var(--paper)' }}
          />
          <span className="mono text-[11px] text-[var(--muted)]">
            这一份共 {lines} 行；超出时停在最后一行
          </span>
          <button
            type="button"
            onClick={() => setGotoOpen(false)}
            className="ml-auto text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
          >
            关闭
          </button>
        </div>
      )}

      {loading && !file ? (
        <Loading what={path} />
      ) : file && needsBytes(kind) ? (
        kind === 'docx' ? (
          <DocxView path={path} />
        ) : (
          <XlsxView path={path} />
        )
      ) : file?.binary ? (
        <Empty>
          这是一个二进制文件（{file.size} 字节）。这里只能读文档与源码，其余的下载后在本机打开。
        </Empty>
      ) : preview && kind === 'markdown' ? (
        <div className="min-h-0 flex-1 overflow-y-auto px-8">
          <div className="mx-auto max-w-[74ch]">
            <Markdown source={text} />
          </div>
        </div>
      ) : preview && kind === 'html' ? (
        <Html source={text} title={path} />
      ) : preview && kind === 'tex' ? (
        <div className="min-h-0 flex-1 overflow-y-auto px-8">
          <div className="mx-auto max-w-[74ch]">
            <Tex source={text} />
          </div>
        </div>
      ) : (
        <CodeEditor
          path={path}
          text={text}
          readOnly={readOnly}
          findQuery={findOpen ? findQuery : undefined}
          findIndex={findIndex}
          goto={goto}
          onChange={(next) => {
            if (readOnly) return;
            setDraft(path, next);
          }}
        />
      )}
    </div>
  );
}

import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type MouseEvent as ReactMouseEvent,
  type ReactElement,
} from 'react';
import {
  createWorkspaceDir,
  deleteWorkspaceEntry,
  getWorkspaceTree,
  renameWorkspaceEntry,
  uploadWorkspaceFile,
  type WorkspaceEntry,
} from '@/api/client';
import { Icon } from '@/shell/icons';
import { useShell } from '@/shell/store';

/**
 * 工作区文件树。
 *
 * 一次只列一层：真实工作区里几千个文件，整棵树一次拉完既慢又没人看得完，
 * 而且任何一层新增文件都要靠整树重拉才出现。展开才请求，读到的子目录缓存在
 * 这一份状态里，刷新按钮把它整个丢掉。
 *
 * 读失败画在自己这一行上：目录读不到（权限、被删、服务端不可达）与「这个目录
 * 就是空的」在界面上必须分得开，否则空目录会被当成「我的文件没了」。
 *
 * 改名 / 删除 / 新建 / 上传都在这里：这些动作改的是磁盘，所以每一次都要么成功
 * 刷新出新的样子，要么把失败原文贴出来——静默失败在这里等于「我的文件去哪了」。
 */
export function FileTree() {
  const [children, setChildren] = useState<Record<string, WorkspaceEntry[]>>({});
  const [expanded, setExpanded] = useState<Record<string, boolean>>({ '': true });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [truncated, setTruncated] = useState<Record<string, boolean>>({});
  const [busy, setBusy] = useState(false);
  const uploadRef = useRef<HTMLInputElement>(null);

  const openTab = useShell((s) => s.openTab);
  const activeTabId = useShell((s) => s.activeTabId);
  const appendOutput = useShell((s) => s.appendOutput);

  const report = useCallback(
    (message: string) => {
      appendOutput(message);
      setErrors((prev) => ({ ...prev, '': message }));
    },
    [appendOutput]
  );

  const load = useCallback(async (path: string) => {
    setLoading((prev) => ({ ...prev, [path]: true }));
    try {
      const page = await getWorkspaceTree(path);
      setChildren((prev) => ({ ...prev, [path]: page.entries }));
      setTruncated((prev) => ({ ...prev, [path]: page.truncated }));
      setErrors((prev) => {
        if (!(path in prev)) return prev;
        const next = { ...prev };
        delete next[path];
        return next;
      });
    } catch (e) {
      const message = (e as { message?: string }).message ?? '读取失败';
      setErrors((prev) => ({ ...prev, [path]: message }));
    } finally {
      setLoading((prev) => ({ ...prev, [path]: false }));
    }
  }, []);

  useEffect(() => {
    void load('');
  }, [load]);

  const reload = useCallback(
    (path: string) => {
      setChildren((prev) => {
        const next = { ...prev };
        delete next[path];
        return next;
      });
      void load(path);
    },
    [load]
  );

  const refreshAll = () => {
    setChildren({});
    setExpanded({ '': true });
    setErrors({});
    setTruncated({});
    void load('');
  };

  const toggle = (entry: WorkspaceEntry) => {
    const path = entry.path;
    const open = !expanded[path];
    setExpanded((prev) => ({ ...prev, [path]: open }));
    if (open && children[path] === undefined && !loading[path]) {
      void load(path);
    }
  };

  const openFile = (entry: WorkspaceEntry) => {
    openTab({
      id: `file:${entry.path}`,
      title: entry.name,
      view: 'files',
      hint: entry.path,
    });
  };

  const parentOf = (path: string) =>
    path.includes('/') ? path.slice(0, path.lastIndexOf('/')) : '';

  const newFolder = async () => {
    const base = parentOf(activePathOf(activeTabId));
    const name = window.prompt('新建目录（相对路径）', base ? `${base}/` : '');
    if (!name) return;
    setBusy(true);
    try {
      const created = await createWorkspaceDir(name.trim());
      appendOutput(`已新建目录 ${created.path}`);
      setExpanded((prev) => ({ ...prev, [parentOf(created.path)]: true }));
      reload(parentOf(created.path));
    } catch (e) {
      report(`新建目录失败：${(e as { message?: string }).message ?? '未知错误'}`);
    } finally {
      setBusy(false);
    }
  };

  const rename = async (entry: WorkspaceEntry) => {
    const next = window.prompt('改名 / 移动（相对路径）', entry.path);
    if (!next || next.trim() === entry.path) return;
    setBusy(true);
    try {
      await renameWorkspaceEntry(entry.path, next.trim());
      appendOutput(`已改名 ${entry.path} → ${next.trim()}`);
      reload(parentOf(entry.path));
      reload(parentOf(next.trim()));
    } catch (e) {
      report(`改名失败：${(e as { message?: string }).message ?? '未知错误'}`);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (entry: WorkspaceEntry) => {
    if (!window.confirm(`删除 ${entry.path}？${entry.dir ? '（目录必须为空）' : ''}`)) return;
    setBusy(true);
    try {
      await deleteWorkspaceEntry(entry.path);
      appendOutput(`已删除 ${entry.path}`);
      reload(parentOf(entry.path));
    } catch (e) {
      report(`删除失败：${(e as { message?: string }).message ?? '未知错误'}`);
    } finally {
      setBusy(false);
    }
  };

  const upload = async (file: File) => {
    setBusy(true);
    try {
      const created = await uploadWorkspaceFile(file.name, file);
      appendOutput(`已上传 ${created.path}（${created.size} 字节）`);
      reload('');
    } catch (e) {
      report(`上传失败：${(e as { message?: string }).message ?? '未知错误'}`);
    } finally {
      setBusy(false);
      if (uploadRef.current) uploadRef.current.value = '';
    }
  };

  const renderLevel = (path: string, depth: number) => {
    const entries = children[path];
    const error = errors[path];
    const items: ReactElement[] = [];

    if (error) {
      items.push(
        <div
          key={`${path}:error`}
          className="px-3 py-1 text-[11px] leading-relaxed"
          style={{ paddingLeft: 12 + depth * 12, color: 'var(--danger)' }}
        >
          读不到这一层：{error}
        </div>
      );
    } else if (entries === undefined) {
      items.push(
        <div
          key={`${path}:loading`}
          className="px-3 py-1 text-[11px] text-[var(--muted)]"
          style={{ paddingLeft: 12 + depth * 12 }}
        >
          {loading[path] ? '读取中…' : ''}
        </div>
      );
    } else if (entries.length === 0) {
      items.push(
        <div
          key={`${path}:empty`}
          className="px-3 py-1 text-[11px] text-[var(--muted)]"
          style={{ paddingLeft: 12 + depth * 12 }}
        >
          空目录
        </div>
      );
    } else {
      for (const entry of entries) {
        const active = activeTabId === `file:${entry.path}`;
        const isOpen = !!expanded[entry.path];
        items.push(
          <div
            key={entry.path}
            role="button"
            tabIndex={0}
            onClick={() => (entry.dir ? toggle(entry) : openFile(entry))}
            onKeyDown={(event) => {
              if (event.key !== 'Enter' && event.key !== ' ') return;
              event.preventDefault();
              if (entry.dir) toggle(entry);
              else openFile(entry);
            }}
            title={entry.path}
            className="group flex w-full cursor-pointer items-center gap-1 py-[3px] pr-2 text-left text-[12px] transition-colors hover:bg-[var(--ink-700)]"
            style={{
              paddingLeft: 8 + depth * 12,
              background: active ? 'var(--ink-700)' : undefined,
              color: active ? 'var(--paper)' : 'var(--paper-dim)',
            }}
          >
            {entry.dir ? (
              <Icon name={isOpen ? 'chevronDown' : 'chevronRight'} size={11} className="shrink-0 opacity-60" />
            ) : (
              <span className="w-[11px] shrink-0" />
            )}
            <Icon
              name={entry.dir ? 'folder' : 'file'}
              size={13}
              className="shrink-0"
              style={{ color: entry.dir ? 'var(--brass)' : 'var(--muted)' }}
            />
            <span className="truncate">{entry.name}</span>
            <span className="ml-auto hidden shrink-0 items-center gap-1 group-hover:flex">
              <InlineAction
                title="改名 / 移动"
                onClick={(event) => {
                  event.stopPropagation();
                  void rename(entry);
                }}
              >
                <span className="mono text-[10px]">改</span>
              </InlineAction>
              <InlineAction
                title="删除"
                onClick={(event) => {
                  event.stopPropagation();
                  void remove(entry);
                }}
              >
                <Icon name="close" size={10} />
              </InlineAction>
            </span>
          </div>
        );
        if (entry.dir && isOpen) {
          items.push(...renderLevel(entry.path, depth + 1));
        }
      }
      if (truncated[path]) {
        items.push(
          <div
            key={`${path}:truncated`}
            className="px-3 py-1 text-[11px] text-[var(--warn)]"
            style={{ paddingLeft: 12 + depth * 12 }}
          >
            这一层条数超过上限，只列了前一部分
          </div>
        );
      }
    }

    return items;
  };

  return (
    <section className="pb-2">
      <div className="flex items-center px-3 py-1.5">
        <h2 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">工作区</h2>
        <span className="ml-auto flex items-center gap-1">
          <IconButton title="新建目录" onClick={() => void newFolder()} disabled={busy}>
            <Icon name="plus" size={12} />
          </IconButton>
          <IconButton title="上传文件到根目录" onClick={() => uploadRef.current?.click()} disabled={busy}>
            <Icon name="save" size={12} />
          </IconButton>
          <IconButton title="重新读取" onClick={refreshAll} disabled={busy}>
            <Icon name="refresh" size={12} />
          </IconButton>
        </span>
      </div>
      <input
        ref={uploadRef}
        type="file"
        className="hidden"
        onChange={(event) => {
          const file = event.target.files?.[0];
          if (file) void upload(file);
        }}
      />
      <div>{renderLevel('', 0)}</div>
    </section>
  );
}

function IconButton({
  title,
  onClick,
  disabled,
  children,
}: {
  title: string;
  onClick: () => void;
  disabled?: boolean;
  children: ReactElement;
}) {
  return (
    <button
      type="button"
      title={title}
      onClick={onClick}
      disabled={disabled}
      className="flex h-4 w-4 items-center justify-center text-[var(--muted)] transition-colors hover:text-[var(--paper)] disabled:opacity-40"
    >
      {children}
    </button>
  );
}

function InlineAction({
  title,
  onClick,
  children,
}: {
  title: string;
  onClick: (event: ReactMouseEvent) => void;
  children: ReactElement;
}) {
  return (
    <button
      type="button"
      title={title}
      onClick={onClick}
      className="flex h-4 w-4 items-center justify-center text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
    >
      {children}
    </button>
  );
}

/** 页签 id 形如 `file:<路径>`；不是文件页时返回空串。 */
function activePathOf(tabId: string): string {
  return tabId.startsWith('file:') ? tabId.slice('file:'.length) : '';
}

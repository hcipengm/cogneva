import { useCallback, useEffect, useState, type ReactElement } from 'react';
import { getWorkspaceTree, type WorkspaceEntry } from '@/api/client';
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
 */
export function FileTree() {
  const [children, setChildren] = useState<Record<string, WorkspaceEntry[]>>({});
  const [expanded, setExpanded] = useState<Record<string, boolean>>({ '': true });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [truncated, setTruncated] = useState<Record<string, boolean>>({});

  const openTab = useShell((s) => s.openTab);
  const activeTabId = useShell((s) => s.activeTabId);

  const load = useCallback(async (path: string) => {
    setLoading((prev) => ({ ...prev, [path]: true }));
    try {
      const page = await getWorkspaceTree(path);
      setChildren((prev) => ({ ...prev, [path]: page.entries }));
      setTruncated((prev) => ({ ...prev, [path]: page.truncated }));
      setErrors((prev) => {
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

  const refresh = () => {
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
          <button
            key={entry.path}
            type="button"
            onClick={() => (entry.dir ? toggle(entry) : openFile(entry))}
            title={entry.path}
            className="flex w-full items-center gap-1 py-[3px] pr-2 text-left text-[12px] transition-colors hover:bg-[var(--ink-700)]"
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
          </button>
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
        <button
          type="button"
          onClick={refresh}
          title="重新读取"
          className="ml-auto text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
        >
          <Icon name="refresh" size={13} />
        </button>
      </div>
      <div>{renderLevel('', 0)}</div>
    </section>
  );
}

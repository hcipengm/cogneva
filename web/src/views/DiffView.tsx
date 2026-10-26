import { useEffect, useState } from 'react';
import {
  getScmDiff,
  getWorkspaceFile,
  saveWorkspaceFile,
  type DiffLine,
  type FileDiff,
} from '@/api/client';
import { pairHunk } from '@/files/diff';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, ViewFrame } from '@/ui/primitives';

/**
 * 一个文件的差异，左右两栏并排。
 *
 * 左栏是旧的一侧、右栏是新的一侧，行号由服务端给：前后端各算一份行号，
 * 迟早会在某一刻错位，而错位的 diff 会把改动指到错误的行上。
 *
 * 一行之内只画一边的配对（删除行对新增行），多余的删除或新增各自占一行、
 * 另一边留空——这是并排 diff 唯一不会让人读错的对齐方式：按行数硬凑会把
 * 一处改动画成两处无关的改动。
 *
 * 画面：并排的两栏 diff 本身就是这一面的画面——它画的是那两个文件的差，一行
 * 一行对得很齐。再叠一层统计图上去只会挤掉真正要看的那几行。
 */
export function DiffView() {
  const tabs = useShell((s) => s.tabs);
  const activeTabId = useShell((s) => s.activeTabId);
  const appendOutput = useShell((s) => s.appendOutput);
  const closeTab = useShell((s) => s.closeTab);

  const active = tabs.find((tab) => tab.id === activeTabId);
  const parsed = parseDiffId(active?.id ?? '');

  const [diff, setDiff] = useState<FileDiff | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  /** 重读的号码：差异由服务端现算，盘上变了就得重新取一份。 */
  const [reload, setReload] = useState(0);

  // 另一处暂存、提交、保存都会改动「这一份差异」的前提，而它们都落在 ide 前缀下。
  // 这里换的是一个号码，取数那一支照旧——差异怎么算仍然只有服务端一处。
  useKindTickedRefresh([resourceKey('ide')], () => setReload((n) => n + 1));

  useEffect(() => {
    if (!parsed) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    getScmDiff(parsed.path, parsed.staged)
      .then((value) => {
        if (!cancelled) setDiff(value);
      })
      .catch((e: { message?: string }) => {
        if (!cancelled) {
          setError(e.message ?? '读取差异失败');
          setDiff(null);
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [parsed?.path, parsed?.staged, reload]);

  if (!parsed) {
    return (
      <ViewFrame title="差异">
        <Empty>这一页需要一个文件路径。</Empty>
      </ViewFrame>
    );
  }

  /** 把左边那一栏画到工作区里——差异页也可以直接采用新的一侧。 */
  const takeRight = async (line: DiffLine) => {
    try {
      const current = await getWorkspaceFile(parsed.path);
      if (current.text === null) {
        appendOutput(`${parsed.path} 不是文本文件，没法按行取用`);
        return;
      }
      const rows = current.text.split('\n');
      const at = (line.new_no ?? 0) - 1;
      if (at < 0 || at >= rows.length) {
        appendOutput(`第 ${line.new_no} 行已经不在这一份里了（重读一次）`);
        return;
      }
      rows[at] = line.text;
      const saved = await saveWorkspaceFile(parsed.path, rows.join('\n'), current.hash);
      appendOutput(`已把 ${parsed.path}:${line.new_no} 换回上一版这一行（${saved.size} 字节）`);
      closeTab(`diff:${parsed.staged ? 'staged' : 'worktree'}:${parsed.path}`);
    } catch (e) {
      appendOutput(`取用失败：${(e as { message?: string }).message ?? '未知错误'}`);
    }
  };

  const added = diff?.added ?? 0;
  const removed = diff?.removed ?? 0;

  return (
    <ViewFrame
      title={parsed.path}
      meta={
        <span className="mono">
          {parsed.staged ? '暂存区 vs 最近一次提交' : '工作区 vs 暂存区'}
          {diff ? ` · +${added} −${removed}` : ''}
        </span>
      }
      actions={
        <>
          <Button
            onClick={() => {
              setDiff(null);
              getScmDiff(parsed.path, parsed.staged)
                .then(setDiff)
                .catch((e: { message?: string }) => setError(e.message ?? '读取差异失败'));
            }}
          >
            重读
          </Button>
          <Button onClick={() => closeTab(active?.id ?? '')}>关闭</Button>
        </>
      }
    >
      {loading && !diff ? (
        <Loading what={`${parsed.path} 的差异`} />
      ) : error ? (
        <div className="p-4">
          <ReadError what={`${parsed.path} 的差异`} error={error} />
        </div>
      ) : !diff ? (
        <Empty>没有内容。</Empty>
      ) : diff.binary ? (
        <Empty>这是一个二进制文件：差异只能看大小，没法逐行比。</Empty>
      ) : diff.hunks.length === 0 ? (
        <Empty>
          两边内容一致——这个文件在
          {parsed.staged ? '暂存区与最近一次提交' : '工作区与暂存区'}之间没有文本差异。
        </Empty>
      ) : (
        <div className="mono text-[12px]" style={{ lineHeight: '1.6' }}>
          {diff.hunks.map((hunk, hunkIndex) => (
            <div key={`${hunk.header}:${hunkIndex}`}>
              <div
                className="sticky top-0 z-10 border-y border-[var(--line)] px-3 py-0.5 text-[11px]"
                style={{ background: 'var(--ink-800)', color: 'var(--live)' }}
              >
                {hunk.header}
              </div>
              {pairHunk(hunk).map((row, rowIndex) => (
                <div key={rowIndex} className="flex">
                  <Cell line={row.left} side="left" onTake={takeRight} />
                  <Cell line={row.right} side="right" onTake={takeRight} />
                </div>
              ))}
            </div>
          ))}
          {diff.truncated && (
            <p className="px-3 py-2 text-[11px]" style={{ color: 'var(--warn)' }}>
              差异太长，这里只画了前一部分。
            </p>
          )}
        </div>
      )}
    </ViewFrame>
  );
}

function Cell({
  line,
  side,
  onTake,
}: {
  line: DiffLine | null;
  side: 'left' | 'right';
  onTake: (line: DiffLine) => void;
}) {
  if (!line) {
    return <div className="w-1/2 shrink-0 border-r border-[var(--line)]" />;
  }
  const number = side === 'left' ? line.old_no : line.new_no;
  const background =
    line.kind === 'del'
      ? 'color-mix(in srgb, var(--danger) 14%, transparent)'
      : line.kind === 'add'
        ? 'color-mix(in srgb, var(--ok) 14%, transparent)'
        : undefined;
  const marker = line.kind === 'del' ? '−' : line.kind === 'add' ? '+' : ' ';
  return (
    <div
      className="group flex w-1/2 min-w-0 shrink-0 border-r border-[var(--line)]"
      style={{ background }}
    >
      <span
        className="w-10 shrink-0 select-none pr-2 text-right text-[11px]"
        style={{ color: 'var(--muted)' }}
      >
        {number ?? ''}
      </span>
      <span className="w-3 shrink-0 select-none" style={{ color: 'var(--muted)' }}>
        {marker}
      </span>
      <span className="whitespace-pre-wrap break-all pr-2" style={{ color: 'var(--paper)' }}>
        {line.text || ' '}
      </span>
      {line.kind === 'del' && (
        <button
          type="button"
          onClick={() => onTake(line)}
          title="把工作区里这一行换回左边这一版"
          className="ml-auto shrink-0 px-1 text-[10px] text-[var(--muted)] opacity-0 transition-opacity group-hover:opacity-100 hover:text-[var(--paper)]"
        >
          取用
        </button>
      )}
    </div>
  );
}

/** 页签身份：`diff:<staged|worktree>:<路径>`。 */
function parseDiffId(id: string): { staged: boolean; path: string } | null {
  if (!id.startsWith('diff:')) return null;
  const rest = id.slice('diff:'.length);
  const at = rest.indexOf(':');
  if (at < 0) return null;
  const scope = rest.slice(0, at);
  const path = rest.slice(at + 1);
  if (!path) return null;
  return { staged: scope === 'staged', path };
}

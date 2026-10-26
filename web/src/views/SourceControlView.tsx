import { useEffect, useState, type ReactNode } from 'react';
import {
  commitStaged,
  getScmLog,
  getScmStatus,
  initWorkspaceRepo,
  stagePaths,
  unstagePaths,
  type ScmChange,
  type ScmStatus,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, ViewFrame } from '@/ui/primitives';

/**
 * 源代码管理：工作区里那本本地 git 历史。
 *
 * 这一页只做四件事——看改了哪些、暂存、提交、翻历史。没有远端、没有分支切换：
 * 工作区的历史是这个用户自己的，一条把它推到别处的通道在这里没有位置。
 *
 * 状态由服务端现算（`git status`），提交之后拿服务端回的那一份覆盖本地，
 * 而不是自己在内存里推测「刚才那一笔应该让哪一行消失」——两边各算一遍，
 * 迟早会在某一刻显示两个不同的工作区。
 */
export function SourceControlView() {
  const openTab = useShell((s) => s.openTab);
  const appendOutput = useShell((s) => s.appendOutput);

  const statusRes = useResource('工作区仓库', getScmStatus, 20_000);
  const logRes = useResource('提交历史', () => getScmLog(20), 60_000);
  // 暂存、提交、初始化都是 ide 前缀下的写。别处（比如文件面保存）也会落在同一个
  // 前缀下——同一个工作区的改动就都在这一族帧里，两个面一起重读。
  useKindTickedRefresh(
    [resourceKey('ide')],
    [statusRes.refresh, logRes.refresh]
  );

  const [status, setStatus] = useState<ScmStatus | null>(null);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  useEffect(() => {
    if (statusRes.data) setStatus(statusRes.data);
  }, [statusRes.data]);

  const run = async (label: string, work: () => Promise<ScmStatus>) => {
    setBusy(label);
    setActionError(null);
    try {
      setStatus(await work());
      logRes.refresh();
    } catch (e) {
      const text = (e as { message?: string }).message ?? `${label}失败`;
      setActionError(text);
      appendOutput(`${label}失败：${text}`);
    } finally {
      setBusy(null);
    }
  };

  const commit = async () => {
    const text = message.trim();
    if (!text) return;
    setBusy('提交');
    setActionError(null);
    try {
      const outcome = await commitStaged(text);
      setStatus(outcome.status);
      setMessage('');
      appendOutput(`已提交 ${outcome.commit.short}：${outcome.commit.subject}`);
      logRes.refresh();
    } catch (e) {
      const msg = (e as { message?: string }).message ?? '提交失败';
      setActionError(msg);
      appendOutput(`提交失败：${msg}`);
    } finally {
      setBusy(null);
    }
  };

  const openDiff = (path: string, staged: boolean) => {
    openTab({
      id: `diff:${staged ? 'staged' : 'worktree'}:${path}`,
      title: path.split('/').pop() ?? path,
      view: 'diff',
      hint: staged ? '暂存区' : '工作区',
    });
  };

  const rows = (changes: ScmChange[], staged: boolean) =>
    changes.map((change) => (
      <div
        key={`${staged ? 's' : 'w'}:${change.path}`}
        className="group flex items-center gap-2 px-3 py-0.5 text-[12px] hover:bg-[var(--ink-700)]"
      >
        <span
          className="mono w-4 shrink-0 text-center text-[11px]"
          style={{ color: codeColor(change.code) }}
          title={
            change.code === '?' ? '未跟踪（还没被 git 看见）' : CODE_NAMES[change.code] ?? change.code
          }
        >
          {change.code === '?' ? 'U' : change.code}
        </span>
        <button
          type="button"
          onClick={() => openDiff(change.path, staged)}
          className="mono min-w-0 flex-1 truncate text-left text-[var(--paper-dim)] hover:text-[var(--paper)]"
          title={`${change.path}（点开看差异）`}
        >
          {change.path}
        </button>
        <button
          type="button"
          onClick={() =>
            void run(staged ? '取消暂存' : '暂存', () =>
              staged ? unstagePaths([change.path]) : stagePaths([change.path])
            )
          }
          disabled={busy !== null}
          className="shrink-0 text-[10px] text-[var(--muted)] opacity-0 transition-opacity group-hover:opacity-100 hover:text-[var(--paper)]"
          title={staged ? `把 ${change.path} 从暂存区拿回来` : `把 ${change.path} 放进暂存区`}
        >
          {staged ? '−' : '+'}
        </button>
      </div>
    ));

  return (
    <ViewFrame
      title="源代码管理"
      meta={
        status
          ? status.repo
            ? `${status.branch ?? '（分离头指针）'}${status.head ? ` · ${status.head}` : ' · 还没有提交'}`
            : '这个工作区还不是仓库'
          : statusRes.error
            ? '读不到'
            : '读取中'
      }
      actions={
        <>
          <Button onClick={statusRes.refresh} disabled={statusRes.loading}>
            重读
          </Button>
          {status && !status.repo && (
            <Button tone="accent" onClick={() => void run('初始化仓库', initWorkspaceRepo)}>
              初始化仓库
            </Button>
          )}
        </>
      }
    >
      {statusRes.error && !status ? (
        <div className="p-4">
          <ReadError what="工作区的 git 状态" error={statusRes.error} onRetry={statusRes.refresh} />
        </div>
      ) : !status ? (
        <Loading what="工作区的 git 状态" />
      ) : !status.repo ? (
        <div className="flex flex-col items-center gap-3 p-10">
          <Empty>
            这个工作区还没有 git 仓库。初始化之后，改动会先落在暂存区，提交留下一条可以回头看的历史。
          </Empty>
          <p className="max-w-lg text-center text-[11px] leading-relaxed text-[var(--muted)]">
            历史只留在你的工作区里：这一面不连任何远端，也不推送。
          </p>
        </div>
      ) : (
        <div className="flex min-h-0 flex-col">
          <div className="border-b border-[var(--line)] px-3 py-2">
            <textarea
              value={message}
              onChange={(event) => setMessage(event.target.value)}
              onKeyDown={(event) => {
                if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
                  event.preventDefault();
                  void commit();
                }
              }}
              rows={2}
              placeholder="提交说明（⌘↵ 提交）"
              className="mono w-full resize-none border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[12px] outline-none focus:border-[var(--brass)]"
              style={{ color: 'var(--paper)' }}
            />
            <div className="mt-1 flex items-center gap-2">
              <Button
                tone="accent"
                onClick={() => void commit()}
                disabled={busy !== null || status.staged.length === 0 || !message.trim()}
              >
                {busy === '提交' ? '提交中…' : `提交（${status.staged.length} 个已暂存）`}
              </Button>
              {status.staged.length === 0 && (
                <span className="text-[11px] text-[var(--muted)]">
                  暂存区是空的：先点下面那些改动，把它们放进来。
                </span>
              )}
              {busy && busy !== '提交' && (
                <span className="text-[11px] text-[var(--muted)]">{busy}中…</span>
              )}
            </div>
            {actionError && (
              <p className="mono mt-1 text-[11px]" style={{ color: 'var(--danger)' }}>
                {actionError}
              </p>
            )}
          </div>

          <Group
            title="已暂存"
            count={status.staged.length}
            action={
              status.staged.length > 0 ? (
                <Button
                  onClick={() =>
                    void run('全部取消暂存', () =>
                      unstagePaths(status.staged.map((c) => c.path))
                    )
                  }
                  disabled={busy !== null}
                >
                  全部取消
                </Button>
              ) : undefined
            }
          >
            {status.staged.length === 0 ? (
              <p className="px-3 py-1 text-[11px] text-[var(--muted)]">没有已暂存的改动。</p>
            ) : (
              rows(status.staged, true)
            )}
          </Group>

          <Group
            title="未暂存"
            count={status.unstaged.length}
            action={
              status.unstaged.length > 0 ? (
                <Button
                  onClick={() =>
                    void run('全部暂存', () =>
                      stagePaths(status.unstaged.map((c) => c.path))
                    )
                  }
                  disabled={busy !== null}
                >
                  全部暂存
                </Button>
              ) : undefined
            }
          >
            {status.unstaged.length === 0 ? (
              <p className="px-3 py-1 text-[11px] text-[var(--muted)]">
                工作区与最近一次提交一致。
              </p>
            ) : (
              rows(status.unstaged, false)
            )}
          </Group>

          {status.truncated && (
            <p className="px-3 py-1 text-[11px]" style={{ color: 'var(--warn)' }}>
              改动太多，这里只列了前一部分；完整清单在 `git status` 里。
            </p>
          )}

          <Group title="提交历史" count={logRes.data?.commits.length ?? 0}>
            {logRes.error ? (
              <p className="px-3 py-1 text-[11px]" style={{ color: 'var(--danger)' }}>
                提交历史没读到：{logRes.error}
              </p>
            ) : (logRes.data?.commits.length ?? 0) === 0 ? (
              <p className="px-3 py-1 text-[11px] text-[var(--muted)]">还没有提交。</p>
            ) : (
              logRes.data?.commits.map((entry) => (
                <div key={entry.id} className="flex items-center gap-2 px-3 py-0.5 text-[12px]">
                  <span className="mono shrink-0 text-[11px]" style={{ color: 'var(--brass)' }}>
                    {entry.short}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-[var(--paper-dim)]" title={entry.subject}>
                    {entry.subject}
                  </span>
                  <span className="shrink-0 text-[11px] text-[var(--muted)]">{entry.author}</span>
                  <Moment iso={new Date(entry.at * 1000).toISOString()} />
                </div>
              ))
            )}
          </Group>
        </div>
      )}
    </ViewFrame>
  );
}

function Group({
  title,
  count,
  action,
  children,
}: {
  title: string;
  count: number;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="border-b border-[var(--line)] py-1">
      <div className="flex items-center gap-2 px-3 py-1">
        <h2 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
          {title} · {count}
        </h2>
        <div className="ml-auto flex items-center gap-1">{action}</div>
      </div>
      {children}
    </section>
  );
}

/** 状态字到颜色：新增绿、删除红、改与改名取中性强调色。 */
function codeColor(code: string): string {
  if (code === 'A' || code === '?') return 'var(--ok)';
  if (code === 'D') return 'var(--danger)';
  if (code === 'U') return 'var(--warn)';
  return 'var(--brass)';
}

const CODE_NAMES: Record<string, string> = {
  M: '改过',
  A: '新增',
  D: '删除',
  R: '改名',
  C: '复制',
  U: '冲突',
  '?': '未跟踪',
};

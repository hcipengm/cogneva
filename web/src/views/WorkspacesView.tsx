import { useEffect, useState } from 'react';
import {
  getWorkspace,
  listWorkspaces,
  type WorkspaceMemberView,
  type WorkspaceSummary,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Empty, Loading, Moment, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { Note } from '@/ui/Note';

/**
 * 工作区面：多租户的容器，以及每个容器里的人。
 *
 * 这一面此前是「按接口原样列出」的临时页。接口现在只有读——没有创建、没有邀请、
 * 没有改成员；界面照这个事实来做，不画一组按下去只会回 404 的按钮。
 *
 * 空列表要当心：存储没配时处理函数答的也是 `{items: []}`，与「配了但没有工作区」
 * 完全同形。所以空态把那两种情形都写出来，让读者知道这个空不是结论。
 *
 * 画面：这一面是容器与人的对照——左边列工作区，右边列这个工作区里的人及其角色。
 * 一个工作区有几个成员是随容器变的，画柱状只是把一列数字重排一遍；读者要问的是
 * 「谁在这个容器里、什么角色」，那是在表里找行。
 */

interface WorkspaceDetail extends WorkspaceSummary {
  members: WorkspaceMemberView[];
}

export function WorkspacesView() {
  const list = useResource('工作区', listWorkspaces, 30_000);
  const [selected, setSelected] = useState<string | null>(null);

  // 选中的那一格变了才重读详情；闭包每次渲染都是新的，`useResource` 把它放在
  // ref 里，所以这里写内联箭头函数不会让定时器每帧重开。
  useEffect(() => {
    if (!selected && list.data && list.data.length > 0) {
      setSelected(list.data[0].id);
    }
  }, [list.data, selected]);

  const detail = useResource<WorkspaceDetail | null>(
    `工作区 ${selected ?? ''}`,
    () => (selected ? getWorkspace(selected) : Promise.resolve(null)),
    30_000,
    Boolean(selected)
  );
  // 工作区面现在只有读，但写它的不止这一面：一次落在 `/api/v1/workspaces` 下的写
  // （别的入口建的、改的）同样要让列表与选中那份重读。
  useKindTickedRefresh([resourceKey('workspaces')], [list.refresh, detail.refresh]);

  const items = list.data ?? [];

  return (
    <ViewFrame
      title="工作区"
      meta={
        list.data
          ? `${items.length} 个工作区 · 接口只读（没有创建 / 邀请 / 改成员的路由）`
          : list.error
            ? '读不到'
            : '读取中'
      }
    >
      {list.error ? (
        <ReadError what="工作区" error={list.error} onRetry={list.refresh} />
      ) : !list.data ? (
        <Loading what="工作区" />
      ) : items.length === 0 ? (
        <Empty>
          接口答了一个空列表。它有两种来源，形状一样：这个部署没有配工作区存储，或者配了存储但
          还没有任何工作区——哪一种，这一次读分不出来。
        </Empty>
      ) : (
        <div className="flex h-full min-h-0">
          <aside className="w-64 shrink-0 overflow-y-auto border-r border-[var(--line)] p-2">
            {items.map((workspace) => {
              const active = workspace.id === selected;
              return (
                <button
                  key={workspace.id}
                  type="button"
                  onClick={() => setSelected(workspace.id)}
                  className={`mb-1 block w-full px-2 py-1.5 text-left transition-colors ${
                    active
                      ? 'bg-[var(--ink-700)] text-[var(--paper)]'
                      : 'text-[var(--paper-dim)] hover:bg-[var(--ink-800)]'
                  }`}
                >
                  <div className="truncate text-[12px]">{workspace.name}</div>
                  <div className="mono truncate text-[10px] text-[var(--muted)]">{workspace.id}</div>
                </button>
              );
            })}
          </aside>

          <section className="flex min-h-0 flex-1 flex-col">
            {detail.error ? (
              <ReadError what="这个工作区" error={detail.error} onRetry={detail.refresh} />
            ) : !detail.data ? (
              <Loading what="工作区详情" />
            ) : (
              <>
                <header className="shrink-0 border-b border-[var(--line)] px-4 py-2">
                  <div className="flex items-baseline gap-3">
                    <h2 className="text-[14px]">{detail.data.name}</h2>
                    <span className="mono text-[11px] text-[var(--muted)]">{detail.data.id}</span>
                  </div>
                  <div className="mt-1 flex flex-wrap gap-4 text-[11px] text-[var(--muted)]">
                    <span>
                      所有者 <span className="mono">{detail.data.owner_id}</span>
                    </span>
                    <span>
                      建于 <Moment iso={detail.data.created_at} />
                    </span>
                    <span>{detail.data.members.length} 个成员</span>
                  </div>
                </header>
                <div className="min-h-0 flex-1 overflow-auto">
                  {detail.data.members.length === 0 ? (
                    <Empty>这个工作区没有成员行。所有者那一格不属于成员表——它不是被邀请进来的。</Empty>
                  ) : (
                    <Table
                      columns={[
                        {
                          key: 'user',
                          title: '成员',
                          render: (m: WorkspaceMemberView) => <span className="mono text-[11px]">{m.user_id}</span>,
                        },
                        {
                          key: 'role',
                          title: '在这一格里是什么',
                          width: '160px',
                          render: (m: WorkspaceMemberView) => (
                            <Note text="这是成员在工作区里的身份（owner / admin / member），不是这个人的职务——职务挂在每一次派发上">
                              {m.role}
                            </Note>
                          ),
                        },
                      ]}
                      rows={detail.data.members}
                      rowKey={(m) => m.user_id}
                    />
                  )}
                </div>
              </>
            )}
          </section>
        </div>
      )}
    </ViewFrame>
  );
}

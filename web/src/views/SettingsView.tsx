import { listEvalDatasets } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';

/**
 * 设置：模型、评估集、界面。
 *
 * 模型那一格只读不写——写入走向导，因为保存时服务端要拿这把钥匙做一次真实
 * 探测；在这里做一个「改 base_url 的输入框」会绕开那次探测，存下一条从没
 * 连通过的配置。
 */
export function SettingsView() {
  const { llm, overlays, signOut, username } = useServices();
  const { openLlmSetup } = overlays;
  const datasets = useResource('评估集', listEvalDatasets, 120_000);
  const sidebarVisible = useShell((s) => s.sidebarVisible);
  const toggleSidebar = useShell((s) => s.toggleSidebar);
  const panelOpen = useShell((s) => s.panelOpen);
  const togglePanel = useShell((s) => s.togglePanel);

  const status = llm.status;
  const backend = status?.backends?.[0];

  return (
    <ViewFrame
      title="设置"
      meta={status ? (status.configured ? '模型已配置' : '模型未配置') : '正在读模型状态'}
      actions={<Button onClick={llm.refresh}>重读</Button>}
    >
      <section className="border-b border-[var(--line)]">
        <header className="flex items-center gap-3 px-4 py-2">
          <h2 className="text-[12px]">模型上游</h2>
          <span className="text-[11px] text-[var(--muted)]">
            保存时服务端会按双协议各探测一次，探测失败会如实写回来
          </span>
          <div className="ml-auto">
            <Button tone="accent" onClick={openLlmSetup}>
              {status?.configured ? '修改配置' : '配置模型'}
            </Button>
          </div>
        </header>
        {llm.loading && !status ? (
          <Loading what="模型状态" />
        ) : !status ? (
          <Empty>读不到模型状态。这一格空着，不代表没有配置。</Empty>
        ) : (
          <>
            <div className="flex flex-wrap py-2">
              <Reading
                label="已配置上游"
                value={status.backends.filter((b) => b.enabled).length}
                tone={status.configured ? 'ok' : 'danger'}
              />
              <Reading label="共登记" value={status.backends.length} />
            </div>
            {backend ? (
              <Table
                columns={[
                  { key: 'provider', title: '线路', width: '160px', render: (b) => (
                    <span className="mono text-[11px]">{b.provider}</span>
                  ) },
                  { key: 'base', title: '上游', render: (b) => (
                    <span className="mono text-[11px] text-[var(--paper-dim)]">{b.base_url}</span>
                  ) },
                  { key: 'model', title: '模型', width: '200px', render: (b) => (
                    <span className="mono text-[11px]">{b.model}</span>
                  ) },
                  { key: 'style', title: '协议', width: '110px', render: (b) => b.api_style },
                  { key: 'enabled', title: '启用', width: '80px', render: (b) => (
                    <span style={{ color: b.enabled ? 'var(--ok)' : 'var(--muted)' }}>
                      {b.enabled ? '是' : '否'}
                    </span>
                  ) },
                  { key: 'key', title: '钥匙', width: '90px', render: (b) => (
                    <span style={{ color: b.has_key ? 'var(--ok)' : 'var(--warn)' }}>
                      {b.has_key ? '有' : '缺'}
                    </span>
                  ) },
                ]}
                rows={status.backends}
                rowKey={(b) => `${b.provider}|${b.base_url}|${b.model}`}
              />
            ) : (
              <Empty>还没登记任何上游。点右上角「配置模型」开始。</Empty>
            )}
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              这里的名字与地址只来自网关自己的登记表；真钥匙在网关的库里，这一页读不到它。
            </p>
          </>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <header className="flex items-center gap-3 px-4 py-2">
          <h2 className="text-[12px]">评估集</h2>
          <span className="text-[11px] text-[var(--muted)]">
            列的是磁盘上真的有的用例集；空列表是「一个都没有」，不是读失败
          </span>
        </header>
        {datasets.error ? (
          <ReadError what="评估集" error={datasets.error} onRetry={datasets.refresh} />
        ) : !datasets.data && datasets.loading ? (
          <Loading what="评估集" />
        ) : (datasets.data ?? []).length === 0 ? (
          <Empty>评估目录里没有数据集。</Empty>
        ) : (
          <Table
            columns={[
              { key: 'name', title: '名称', width: '220px', render: (d) => d.name },
              { key: 'cases', title: '用例', width: '90px', render: (d) => (
                <span className="mono text-[11px]">{d.case_count}</span>
              ) },
              { key: 'tags', title: '标签', width: '220px', render: (d) => (
                <span className="text-[11px] text-[var(--muted)]">{d.tags.join(', ') || '—'}</span>
              ) },
              { key: 'path', title: '路径', render: (d) => (
                <span className="mono text-[11px] text-[var(--muted)]">{d.path}</span>
              ) },
            ]}
            rows={datasets.data ?? []}
            rowKey={(d) => d.path}
          />
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <header className="px-4 py-2">
          <h2 className="text-[12px]">界面</h2>
        </header>
        <div className="flex flex-wrap gap-x-8 gap-y-2 px-4 pb-3 text-[12px]">
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={sidebarVisible} onChange={toggleSidebar} />
            侧栏
          </label>
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={panelOpen} onChange={togglePanel} />
            底部面板
          </label>
        </div>
        <table className="grid-table">
          <tbody>
            {[
              ['⌘K', '命令面板'],
              ['⌘P', '打开一页'],
              ['⌘B', '收起 / 展开侧栏'],
              ['⌘J', '收起 / 展开底部面板'],
              ['esc', '关闭浮层'],
            ].map(([keys, what]) => (
              <tr key={keys}>
                <td className="mono w-24 text-[11px] text-[var(--paper-dim)]">{keys}</td>
                <td className="text-[12px] text-[var(--paper-dim)]">{what}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="flex items-center gap-3 px-4 py-3 text-[12px]">
        <span className="text-[var(--paper-dim)]">当前用户 {username ?? '—'}</span>
        <Button onClick={signOut}>退出登录</Button>
      </section>
    </ViewFrame>
  );
}

import { getQuotaUsage, listEvalDatasets } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { useTheme } from '@/shell/theme';
import { THEMES } from '@/styles/themes';
import { ThemePicker } from '@/ui/ThemePicker';
import { Button, Empty, Loading, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

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
  const quota = useResource('额度', getQuotaUsage, 60_000);
  // 额度跟着花销走：一轮话、一次运行结束就是又花了一笔。评估集只在有人增删时才变
  // ——那是 eval 前缀下的一次写。两件事都不是任务状态，不该被任务帧拖着重读。
  useKindTickedRefresh(
    ['usage', resourceKey('quota'), resourceKey('eval'), resourceKey('admin')],
    [datasets.refresh, quota.refresh]
  );
  const sidebarVisible = useShell((s) => s.sidebarVisible);
  const toggleSidebar = useShell((s) => s.toggleSidebar);
  const panelOpen = useShell((s) => s.panelOpen);
  const togglePanel = useShell((s) => s.togglePanel);
  const theme = useTheme((s) => s.theme);
  const setTheme = useTheme((s) => s.setTheme);

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
            <Readings
              from={{ what: llm.what, at: llm.at }}
              source="GET /api/v1/llm/status"
              caveat="这一格是网关问自己登记表得到的那一份：它说「配了没有、钥匙在不在」，不说「上游通不通」——通不通要真发一次请求才知道。"
            >
            <div className="flex flex-wrap py-2">
              <Reading
                label="已配置上游"
                value={status.backends.filter((b) => b.enabled).length}
                tone={status.configured ? 'ok' : 'danger'}
              />
              <Reading label="共登记" value={status.backends.length} />
            </div>
            </Readings>
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
        <header className="flex items-center gap-3 px-4 py-2">
          <h2 className="text-[12px]">额度与用量</h2>
          <span className="text-[11px] text-[var(--muted)]">
            按账号算；按执行者拆开的 token 花销在观测页
          </span>
          <div className="ml-auto">
            <Button onClick={quota.refresh}>重读</Button>
          </div>
        </header>
        {quota.error ? (
          <ReadError what="额度" error={quota.error} onRetry={quota.refresh} />
        ) : !quota.data ? (
          <Loading what="额度" />
        ) : (
          <Readings
            from={quota}
            source="GET /api/v1/quota/usage（配额管理这一侧的数）"
            caveat="这一侧拦调用，观测页那一侧记花销；两套计数对不上时以拦得住的这一侧为准。"
          >
          <div className="flex flex-wrap py-2">
            <Reading label="今天已用" value={quota.data.used_today.toLocaleString()} />
            <Reading label="剩余额度" value={quota.data.remaining.toLocaleString()} />
            <Reading label="额度总量" value={quota.data.total_quota.toLocaleString()} />
            <Reading label="账号" value={<span className="mono text-[13px]">{quota.data.user_id}</span>} />
          </div>
          </Readings>
        )}
        <p className="px-4 pb-2 text-[11px] text-[var(--muted)]">
          剩余额度是配额管理这一侧的数，与观测页按执行者统计的 token 花销是两套计数：前者拦调用，
          后者记账，对不上时以拦得住的这一侧为准。
        </p>
      </section>

      <section className="border-b border-[var(--line)]">
        <header className="flex items-center gap-3 px-4 py-2">
          <h2 className="text-[12px]">配色主题</h2>
          <span className="text-[11px] text-[var(--muted)]">
            只影响这台浏览器，不写回服务端；命令面板里搜主题名也能换
          </span>
        </header>
        <ThemePicker themes={THEMES} active={theme} onPick={setTheme} />
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

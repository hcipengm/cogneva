import {
  getContributionStatus,
  getQuotaUsage,
  getSearchFallback,
  listEvalDatasets,
} from '@/api/client';
import { BarList } from '@/ui/plots';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useServices } from '@/shell/context';
import { useShell } from '@/shell/store';
import { useTheme } from '@/shell/theme';
import { THEMES } from '@/styles/themes';
import { ThemePicker } from '@/ui/ThemePicker';
import { Button, Empty, Loading, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

/** 这一条能用来做什么：说中文，别把服务端的枚举值原样摆在人眼前。 */
function capabilityLabel(capability: string): string {
  if (capability === 'both') return '检索与抓取';
  if (capability === 'fetch') return '只抓取';
  return '只检索';
}

/**
 * 设置：模型、上网后备、评估集、界面。
 *
 * 模型那一格只读不写——写入走向导，因为保存时服务端要拿这把钥匙做一次真实
 * 探测；在这里做一个「改 base_url 的输入框」会绕开那次探测，存下一条从没
 * 连通过的配置。
 *
 * 上网后备那一格同理：这里只读回「有哪几条、哪条缺钥匙」，写走向导。多一层
 * 理由——钥匙在网关保存时写进带外 Secret，读面永远不回明文；在这一页塞输入框
 * 会让人以为填了就等于生效，而真正的生效判据是新 Pod 读回来的那份池变了。
 */
export function SettingsView() {
  const { llm, overlays, signOut, username } = useServices();
  const { openLlmSetup, openWebSearch, openCredentials } = overlays;
  const datasets = useResource('评估集', listEvalDatasets, 120_000);
  const quota = useResource('额度', getQuotaUsage, 60_000);
  const searchFallback = useResource('上网后备', getSearchFallback, 60_000);
  const contribution = useResource('代码平台', getContributionStatus, 60_000);
  // 额度跟着花销走：一轮话、一次运行结束就是又花了一笔。评估集只在有人增删时才变
  // ——那是 eval 前缀下的一次写。后备池同理：保存是 admin 前缀下的一次写，之后
  // 网关自己滚动，读回来才是新的那一份。三件事都不是任务状态，不该被任务帧拖着重读。
  useKindTickedRefresh(
    ['usage', resourceKey('quota'), resourceKey('eval'), resourceKey('admin')],
    [datasets.refresh, quota.refresh, searchFallback.refresh, contribution.refresh]
  );
  const sidebarVisible = useShell((s) => s.sidebarVisible);
  const toggleSidebar = useShell((s) => s.toggleSidebar);
  const panelOpen = useShell((s) => s.panelOpen);
  const togglePanel = useShell((s) => s.togglePanel);
  const theme = useTheme((s) => s.theme);
  const setTheme = useTheme((s) => s.setTheme);

  const status = llm.status;
  const backend = status?.backends?.[0];

  const pool = searchFallback.data?.upstreams ?? [];
  const fromLlm = searchFallback.data?.from_llm ?? [];
  const poolWithKey = pool.filter((u) => u.has_key).length;
  // 顺序不是服务端的一条字段，它就是数组下标——数组的顺序就是被问的顺序。
  const poolRows = pool.map((u, i) => ({ ...u, order: i + 1 }));

  const contrib = contribution.data;
  const identity = contrib?.git_identity;
  // 身份的三个状态各有各的下一步：ready 是终点，pending 是「密钥在、登记没确认」，
  // absent 是还没连过。合成一句「已配置/未配置」会把中间那个状态读成终点。
  const identityLabel =
    identity === undefined
      ? '…'
      : identity.state === 'ready'
        ? '已就绪'
        : identity.state === 'pending'
          ? '未确认'
          : identity.state === 'unknown'
            ? '读不到'
            : '没有密钥';

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
          <h2 className="text-[12px]">上网检索与抓取</h2>
          <span className="text-[11px] text-[var(--muted)]">
            本机两层先答，答不上才按下面这个顺序问第三方，全答不上才用现网 LLM 自带的工具
          </span>
          <div className="ml-auto">
            <Button tone="accent" onClick={openWebSearch}>
              {pool.length > 0 ? '修改后备' : '配置后备'}
            </Button>
          </div>
        </header>
        {searchFallback.error ? (
          <ReadError what="上网后备" error={searchFallback.error} onRetry={searchFallback.refresh} />
        ) : !searchFallback.data ? (
          <Loading what="上网后备" />
        ) : (
          <>
            <Readings
              from={{ what: searchFallback.what, at: searchFallback.at }}
              source="GET /api/v1/admin/search-fallback"
              caveat="这一格说「配了哪几条、哪条有钥匙」，不说「上游通不通」——通不通要真发一次检索才知道，读它的是 web_search 自己。"
            >
              <div className="flex flex-wrap py-2">
                <Reading label="第三方后备" value={pool.length} />
                <Reading
                  label="已配钥匙"
                  value={poolWithKey}
                  tone={pool.length === 0 ? 'plain' : poolWithKey === pool.length ? 'ok' : 'warn'}
                />
                <Reading
                  label="现网 LLM 自带工具"
                  value={fromLlm.length}
                  tone={fromLlm.length > 0 ? 'ok' : 'plain'}
                  note="跟着现网 LLM 一起轮换，不在这里配"
                />
              </div>
            </Readings>
            {pool.length > 0 ? (
              <Table
                columns={[
                  { key: 'order', title: '顺序', width: '56px', render: (u) => (
                    <span className="mono text-[11px]">{u.order}</span>
                  ) },
                  { key: 'id', title: '名字', width: '140px', render: (u) => (
                    <span className="mono text-[11px]">{u.id}</span>
                  ) },
                  { key: 'base', title: '地址', render: (u) => (
                    <span className="mono text-[11px] text-[var(--paper-dim)]">{u.base_url}</span>
                  ) },
                  { key: 'capability', title: '用途', width: '110px', render: (u) => capabilityLabel(u.capability) },
                  { key: 'key', title: '钥匙', width: '90px', render: (u) => (
                    <span style={{ color: u.has_key ? 'var(--ok)' : 'var(--warn)' }}>
                      {u.has_key ? '有' : '缺'}
                    </span>
                  ) },
                ]}
                rows={poolRows}
                rowKey={(u) => `${u.order}|${u.id}|${u.base_url}`}
              />
            ) : (
              <Empty>
                还没有第三方后备：本机 SearXNG / crawl4ai 答不上时，这条路上没有别的东西可问。
                点右上角「配置后备」开始。
              </Empty>
            )}
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              顺序就是优先级，而且是一问一答的串行：上一条答出结果就不问下一条，所以后面的
              不会因为前面的存在而被白花额度。钥匙只写不读——保存时网关把它写进带外 Secret，
              这一页永远读不回明文；缺钥匙的那一条会被跳过，不会被当成「这家挂了」。
              {fromLlm.length === 0 && (
                <> 现网在用的 LLM 没有自带的上网工具面，所以最后一层现在也是空的。</>
              )}
            </p>
          </>
        )}
      </section>

      <section className="border-b border-[var(--line)]">
        <header className="flex items-center gap-3 px-4 py-2">
          <h2 className="text-[12px]">代码平台与凭据</h2>
          <span className="text-[11px] text-[var(--muted)]">
            GitHub / Gitee 的令牌与 SSH 身份；令牌只进安全网关，这一页读不回它
          </span>
          <div className="ml-auto">
            <Button tone="accent" onClick={openCredentials}>
              {contrib?.configured ? '管理连接' : '连接代码平台'}
            </Button>
          </div>
        </header>
        {contribution.error ? (
          <ReadError what="代码平台" error={contribution.error} onRetry={contribution.refresh} />
        ) : !contrib ? (
          <Loading what="代码平台" />
        ) : (
          <>
            <Readings
              from={{ what: contribution.what, at: contribution.at }}
              source="GET /api/v1/admin/contribution-status"
              caveat="这一格说「连上了没有、钥匙停在哪一步」，不说「平台通不通」——通不通要真发一次请求才知道。"
            >
              <div className="flex flex-wrap py-2">
                <Reading
                  label="已连接平台"
                  value={contrib.configured ? contrib.provider : '未连接'}
                  tone={contrib.configured ? 'ok' : 'warn'}
                />
                <Reading
                  label="SSH 身份"
                  value={identityLabel}
                  tone={
                    identity?.state === 'ready'
                      ? 'ok'
                      : identity?.state === 'pending'
                        ? 'warn'
                        : 'plain'
                  }
                />
                <Reading label="待提交变更" value={contrib.pending_count} />
                <Reading label="回流策略" value={contrib.policy} />
              </div>
            </Readings>
            {identity?.fingerprint && (
              <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
                公钥指纹 <span className="mono">{identity.fingerprint}</span>
              </p>
            )}
            {identity?.note && (
              <p className="px-4 py-2 text-[11px] text-[var(--warn)]">{identity.note}</p>
            )}
            <p className="px-4 py-2 text-[11px] text-[var(--muted)]">
              GitHub 设备码 {contrib.device_flow_available ? '可用' : '不可用（缺 OAuth App client_id）'}；
              Gitee 授权码 {contrib.gitee_oauth_available ? '可用' : '不可用（缺 client_secret）'}；
              两条都不可用时用粘贴令牌。凭据的写入都在浮层里完成，写完之后网关自己滚动，
              读回来才是新状态。
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
        {(datasets.data ?? []).length > 0 && (
          <BarList
            label="每个评估集有几个用例"
            note="按磁盘上实际存在的用例数（GET /api/v1/eval/datasets）；用例多不等于覆盖广，覆盖看标签"
            rows={(datasets.data ?? [])
              .map((d) => ({ id: d.path, label: d.name, value: d.case_count }))
              .sort((a, b) => b.value - a.value)}
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

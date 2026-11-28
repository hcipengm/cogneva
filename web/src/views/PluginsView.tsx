import { useState } from 'react';
import { listPlugins, loadPlugin, unloadPlugin, type PluginSummary } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { FormRow, TextArea } from '@/ui/forms';

/**
 * 插件面：本地插件目录里有哪些清单，以及装载 / 卸载。
 *
 * 两条路都在，此前界面上一条入口都没有。要看清它们各自做到了什么：
 *
 * - 列表读的是插件目录里的**清单**，不是已装载的句柄。所以「在列表里」等于
 *   「目录里有它的 manifest」，不等于「它正在跑」——两者在这一面上必须分开说，
 *   否则一次成功的装载会看起来像目录里多了一行。
 * - 装载的请求体就是清单本身。字节由网关按清单里的地址自己去取，界面不传
 *   二进制；注册表没配、地址取不到时这一步答 400 或 503，原文落在输出格里。
 * - 卸载按名字寻址，而服务端现在的卸载是一个记录动作（MVP：不真正丢句柄）。
 *   界面照实说「已请求卸载」，不说「已停掉」。
 *
 * 画面：这一面是插件目录的清单——一行一个 manifest，以及它声明了哪些能力。一份
 * 清单读出多长就是多长，没有连续的量可画；读者要问的是「这个能力由谁提供」，
 * 那是在表里按能力找行。
 */

function capabilityText(plugin: PluginSummary): string {
  const caps = plugin.capabilities ?? [];
  if (caps.length === 0) return '—';
  return caps
    .map((c) => {
      switch (c.type) {
        case 'tool':
          return `工具 ${c.name ?? ''}`.trim();
        case 'skill':
          return `技能 ${c.skill_id ?? ''}`.trim();
        case 'backend':
          return `后端 ${c.backend_type ?? ''}`.trim();
        default:
          return c.type;
      }
    })
    .join('、');
}

export function PluginsView() {
  const page = useResource('插件', listPlugins, 30_000);
  // 装载、卸载都是 `/api/v1/plugins` 下的写：目录里多一份或少一份清单，这一面
  // 要跟着帧重读，而不是等下一拍。
  useKindTickedRefresh([resourceKey('plugins')], page.refresh);
  const appendOutput = useShell((s) => s.appendOutput);
  const [open, setOpen] = useState(false);
  const [manifest, setManifest] = useState('');
  const [busy, setBusy] = useState(false);

  const upload = async () => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(manifest);
    } catch (e) {
      appendOutput(`插件没有装载：清单不是合法 JSON（${(e as Error).message}）`);
      return;
    }
    setBusy(true);
    try {
      const res = await loadPlugin(parsed);
      appendOutput(`已装载插件 ${res.name}（${res.plugin_id}，${res.status}）`);
      setManifest('');
      setOpen(false);
      page.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`装载插件失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setBusy(false);
    }
  };

  const unload = async (plugin: PluginSummary) => {
    if (!window.confirm(`请求卸载插件 ${plugin.name}？服务端的卸载目前只记账，不会停掉已经实例化的模块。`))
      return;
    try {
      await unloadPlugin(plugin.name);
      appendOutput(`已请求卸载插件 ${plugin.name}`);
      page.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`卸载插件 ${plugin.name} 失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    }
  };

  const plugins = page.data?.plugins ?? [];

  return (
    <ViewFrame
      title="插件"
      meta={
        page.data
          ? `${plugins.length} 份清单在插件目录里（清单在目录 ≠ 已装载）`
          : page.error
            ? '读不到'
            : '读取中'
      }
      actions={<Button tone="accent" onClick={() => setOpen((v) => !v)}>{open ? '收起' : '装载插件'}</Button>}
    >
      {open && (
        <section className="border-b border-[var(--line)] px-4 py-2">
          <FormRow
            label="清单"
            hint={
              <>
                一个 JSON 对象，字段与服务端 `PluginManifest` 一致：name / version / author /
                capabilities / entry / signature。字节由网关按清单取，界面不传文件。
              </>
            }
          >
            <TextArea
              value={manifest}
              onChange={setManifest}
              rows={8}
              placeholder={'{\n  "name": "example",\n  "version": "0.1.0",\n  "author": "…",\n  "capabilities": [],\n  "entry": "plugin.wasm",\n  "signature": ""\n}'}
            />
          </FormRow>
          <div className="flex items-center gap-2 pt-1">
            <Button tone="accent" onClick={upload} disabled={busy || !manifest.trim()}>
              {busy ? '装载中…' : '装载'}
            </Button>
          </div>
        </section>
      )}

      {page.error ? (
        <ReadError what="插件" error={page.error} onRetry={page.refresh} />
      ) : !page.data ? (
        <Loading what="插件" />
      ) : plugins.length === 0 ? (
        <Empty>
          插件目录里没有任何清单。插件注册表没配时接口答 503，会走「读不到」——所以这里空着，
          说明目录真的在、里面真的没有插件。
        </Empty>
      ) : (
        <Table
          columns={[
            { key: 'name', title: '名字', render: (p) => <span className="mono text-[11px]">{p.name}</span> },
            { key: 'version', title: '版本', width: '90px', render: (p) => <span className="mono">{p.version}</span> },
            { key: 'author', title: '作者', width: '120px', render: (p) => p.author || '—' },
            { key: 'caps', title: '声明提供', render: (p) => capabilityText(p) },
            { key: 'entry', title: '入口', width: '150px', render: (p) => <span className="mono text-[11px]">{p.entry}</span> },
            {
              key: 'signature',
              title: '签名',
              width: '90px',
              render: (p) => (p.signature ? '有' : <span className="text-[var(--warn)]">没有</span>),
            },
            {
              key: 'op',
              title: '',
              width: '64px',
              render: (p) => <Button onClick={() => void unload(p)}>卸载</Button>,
            },
          ]}
          rows={plugins}
          rowKey={(p) => `${p.name}@${p.version}`}
        />
      )}
    </ViewFrame>
  );
}

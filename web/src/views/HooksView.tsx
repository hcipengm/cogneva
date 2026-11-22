import { useState } from 'react';
import {
  createHook,
  deleteHook,
  getHooksHealth,
  HOOK_ACTIONS,
  HOOK_TRIGGERS,
  listHooks,
  type HookActionName,
  type HookDefView,
  type HookTriggerName,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { Check, FormRow, NumberInput, Select, TextArea, TextInput } from '@/ui/forms';

/**
 * 钩子面：登记在网关上的十几种触发点，以及触发之后做什么。
 *
 * 这一面此前是一个「按接口原样列出」的临时页，界面上没有地方登记或删除——钩子的
 * 增删查都在，入口不在。这里把三件事都接上，并且把两条服务端不会替你检查的
 * **语义**写在提交之前：
 *
 * 1. `block` / `rewrite` 只在 `on_tool_before` 上有意义。它们要在调用发生前决定
 *    这次调用做不做、换什么参数；挂在别的触发点上，服务端会收下这条钩子，然后
 *    它永远不会被问到——界面上「登记成功」，行为上什么都没发生。
 * 2. 这条决定还得写清管哪个工具。不带 `tool` 的 `block` 对照的是「所有工具」，
 *    而工具守卫对没有模式的决定型钩子不咨询，所以它同样是一条安静的钩子。
 *
 * 这两条不是界面发明的规则，是 `HookDef` 的文档说的；界面替读者挡在提交之前。
 *
 * 画面：这一面是触发点与动作的对照表——一行一条钩子。两边都是闭集（十几种触发点、
 * 几种动作），画分布只会在十几个可能里数人；读者要问的是「哪个工具在调用前被挡住」，
 * 那是一条一条读表的题。
 */

const TRIGGER_LABEL: Record<HookTriggerName, string> = {
  on_agent_start: '智能体启动',
  on_agent_end: '智能体结束',
  on_task_complete: '任务完成',
  on_task_fail: '任务失败',
  on_crew_complete: '小队完成',
  on_ralph_pass: 'Ralph 一轮通过',
  on_ralph_unrecoverable: 'Ralph 无法恢复',
  on_squad_retry: '小队重试',
  on_tool_before: '工具调用前',
  on_tool_after: '工具调用后',
};

const ACTION_LABEL: Record<HookActionName, string> = {
  webhook: '回调 URL',
  redis_stream: '写 Redis 流',
  log: '写日志',
  notify: '通知某人',
  block: '拒绝这次调用',
  rewrite: '换掉调用参数',
};

const DECIDING: readonly HookActionName[] = ['block', 'rewrite'];

function actionSummary(action: HookDefView['action']): string {
  switch (action.type) {
    case 'webhook':
      return `回调 ${String(action.url ?? '')}`;
    case 'redis_stream':
      return `流 ${String(action.channel ?? '')}`;
    case 'log':
      return `日志 ${String(action.level ?? 'info')}`;
    case 'notify':
      return `通知 ${String(action.user_id ?? '')}`;
    case 'block':
      return action.reason ? `拒绝：${String(action.reason)}` : '拒绝（无理由）';
    case 'rewrite':
      return '替换参数';
    default:
      return String(action.type);
  }
}

export function HooksView() {
  const page = useResource('钩子', listHooks, 30_000);
  const health = useResource('钩子引擎', getHooksHealth, 60_000);
  // 钩子的增删都落在 `/api/v1/hooks` 前缀下；引擎健康也跟着这条面走——登记成功
  // 之后读者要立刻看见它是不是真的被引擎收下了。
  useKindTickedRefresh([resourceKey('hooks')], [page.refresh, health.refresh]);
  const appendOutput = useShell((s) => s.appendOutput);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);

  const [id, setId] = useState('');
  const [trigger, setTrigger] = useState<HookTriggerName>('on_task_complete');
  const [scope, setScope] = useState<'global' | 'crew' | 'squad'>('global');
  const [tool, setTool] = useState('');
  const [actionType, setActionType] = useState<HookActionName>('log');
  const [url, setUrl] = useState('');
  const [headers, setHeaders] = useState('{}');
  const [channel, setChannel] = useState('');
  const [level, setLevel] = useState<'trace' | 'debug' | 'info' | 'warn' | 'error'>('info');
  const [userId, setUserId] = useState('');
  const [reason, setReason] = useState('');
  const [argumentsJson, setArgumentsJson] = useState('{}');
  const [limited, setLimited] = useState(false);
  const [burst, setBurst] = useState('');
  const [perSecond, setPerSecond] = useState('');
  const [timeoutMs, setTimeoutMs] = useState('');

  const reset = () => {
    setId('');
    setTrigger('on_task_complete');
    setScope('global');
    setTool('');
    setActionType('log');
    setUrl('');
    setHeaders('{}');
    setChannel('');
    setLevel('info');
    setUserId('');
    setReason('');
    setArgumentsJson('{}');
    setLimited(false);
    setBurst('');
    setPerSecond('');
    setTimeoutMs('');
  };

  /** 把表单拼成一条 `HookDef`。拼不出来的那几种情形在这里就地拒绝，不发请求。 */
  const compose = (): { def?: HookDefView; problem?: string } => {
    if (!id.trim()) return { problem: 'id 不能空：它是这条钩子被删除、被读回时的身份。' };
    if (DECIDING.includes(actionType) && trigger !== 'on_tool_before') {
      return {
        problem: `${ACTION_LABEL[actionType]}只在「工具调用前」上有意义：它会收下这条钩子，但永远不会问它。`,
      };
    }
    if (DECIDING.includes(actionType) && !tool.trim()) {
      return { problem: '决定型钩子要写清管哪个工具（名字或 `*` 通配），否则工具守卫不咨询它。' };
    }

    let action: HookDefView['action'];
    try {
      switch (actionType) {
        case 'webhook': {
          if (!url.trim()) return { problem: 'webhook 要一个 URL。' };
          const parsed = JSON.parse(headers || '{}') as Record<string, string>;
          if (typeof parsed !== 'object' || Array.isArray(parsed)) {
            return { problem: 'headers 要一个 JSON 对象，形如 {"Authorization":"Bearer …"}。' };
          }
          action = { type: 'webhook', url: url.trim(), headers: parsed };
          break;
        }
        case 'redis_stream': {
          if (!channel.trim()) return { problem: 'redis_stream 要一个频道名。' };
          action = { type: 'redis_stream', channel: channel.trim() };
          break;
        }
        case 'log':
          action = { type: 'log', level };
          break;
        case 'notify': {
          if (!userId.trim()) return { problem: 'notify 要一个人（user_id）。' };
          action = { type: 'notify', user_id: userId.trim() };
          break;
        }
        case 'block':
          action = reason.trim() ? { type: 'block', reason: reason.trim() } : { type: 'block' };
          break;
        case 'rewrite': {
          const parsed = JSON.parse(argumentsJson || '{}') as unknown;
          action = { type: 'rewrite', arguments: parsed };
          break;
        }
        default:
          return { problem: `没有认得的动作类型：${actionType}` };
      }
    } catch (e) {
      return { problem: `JSON 解析失败：${(e as Error).message}` };
    }

    const def: HookDefView = { id: id.trim(), trigger, scope, action };
    if (tool.trim()) def.tool = tool.trim();
    if (limited) {
      const b = Number(burst);
      const p = Number(perSecond);
      if (!Number.isFinite(b) || !Number.isFinite(p) || b <= 0 || p <= 0) {
        return { problem: '限流要两个正整数：桶深与每秒补充。' };
      }
      def.rate_limit = { burst: b, per_second: p };
    }
    if (timeoutMs.trim()) {
      const ms = Number(timeoutMs);
      if (!Number.isFinite(ms) || ms <= 0) return { problem: '超时要一个正毫秒数。' };
      def.timeout_ms = ms;
    }
    return { def };
  };

  const submit = async () => {
    const { def, problem } = compose();
    if (!def) {
      appendOutput(`钩子没有登记：${problem}`);
      return;
    }
    setBusy(true);
    try {
      await createHook(def);
      appendOutput(`已登记钩子 ${def.id}（${TRIGGER_LABEL[def.trigger]} → ${ACTION_LABEL[def.action.type]}）`);
      reset();
      setOpen(false);
      page.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`登记钩子 ${def.id} 失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (hook: HookDefView) => {
    if (!window.confirm(`删除钩子 ${hook.id}？触发时不再执行 ${ACTION_LABEL[hook.action.type]}。`)) return;
    try {
      await deleteHook(hook.id);
      appendOutput(`已删除钩子 ${hook.id}`);
      page.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`删除钩子 ${hook.id} 失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    }
  };

  const hooks = page.data?.hooks ?? [];

  return (
    <ViewFrame
      title="钩子"
      meta={
        page.data
          ? `${hooks.length} 条登记在网关的内存里——进程重启就没了，不是持久化配置`
          : page.error
            ? '读不到'
            : '读取中'
      }
      actions={<Button tone="accent" onClick={() => setOpen((v) => !v)}>{open ? '收起' : '登记一条钩子'}</Button>}
    >
      {open && (
        <section className="border-b border-[var(--line)] px-4 py-2">
          <FormRow label="id" hint="这条钩子的身份；删除按它寻址。">
            <TextInput value={id} onChange={setId} placeholder="如 guard-destructive-bash" />
          </FormRow>
          <FormRow label="何时触发" hint="十一个触发点是闭集，服务端按这些词分发。">
            <Select
              value={trigger}
              onChange={setTrigger}
              options={HOOK_TRIGGERS.map((value) => ({ value, label: `${TRIGGER_LABEL[value]}（${value}）` }))}
            />
          </FormRow>
          <FormRow label="范围" hint="crew / squad 只有在事件带上对应 id 时才匹配。">
            <Select
              value={scope}
              onChange={setScope}
              options={[
                { value: 'global', label: '全局' },
                { value: 'crew', label: '小队群（crew）' },
                { value: 'squad', label: '小队（squad）' },
              ]}
            />
          </FormRow>
          <FormRow label="工具模式" hint="名字或 `*` 通配（bash、mcp__*、*_write）。不填＝所有工具；决定型钩子必须填。">
            <TextInput value={tool} onChange={setTool} placeholder="如 bash 或 mcp__*" />
          </FormRow>
          <FormRow label="做什么">
            <Select
              value={actionType}
              onChange={setActionType}
              options={HOOK_ACTIONS.map((value) => ({ value, label: `${ACTION_LABEL[value]}（${value}）` }))}
            />
          </FormRow>

          {actionType === 'webhook' && (
            <>
              <FormRow label="URL">
                <TextInput value={url} onChange={setUrl} placeholder="https://…" />
              </FormRow>
              <FormRow label="headers" hint="JSON 对象；留 {} 表示不带额外头。">
                <TextArea value={headers} onChange={setHeaders} rows={2} />
              </FormRow>
            </>
          )}
          {actionType === 'redis_stream' && (
            <FormRow label="频道">
              <TextInput value={channel} onChange={setChannel} placeholder="如 hooks:events" />
            </FormRow>
          )}
          {actionType === 'log' && (
            <FormRow label="级别">
              <Select
                value={level}
                onChange={setLevel}
                options={([
                  'trace',
                  'debug',
                  'info',
                  'warn',
                  'error',
                ] as const).map((value) => ({ value, label: value }))}
              />
            </FormRow>
          )}
          {actionType === 'notify' && (
            <FormRow label="通知谁" hint="user_id。">
              <TextInput value={userId} onChange={setUserId} />
            </FormRow>
          )}
          {actionType === 'block' && (
            <FormRow label="理由" hint="模型会在工具结果里读到这句话，可以绕开；不写也有理由（空）。">
              <TextInput value={reason} onChange={setReason} width="100%" />
            </FormRow>
          )}
          {actionType === 'rewrite' && (
            <FormRow label="新参数" hint="一个 JSON 值：它将替换模型给这次调用的参数。">
              <TextArea value={argumentsJson} onChange={setArgumentsJson} rows={4} />
            </FormRow>
          )}

          <FormRow label="限流" hint="同一触发在窗口内过多时丢弃；服务端默认每秒一百次。">
            <span className="flex items-center gap-2">
              <Check checked={limited} onChange={setLimited} label="启用" />
              {limited && (
                <>
                  <NumberInput value={burst} onChange={setBurst} placeholder="桶深" min={1} />
                  <NumberInput value={perSecond} onChange={setPerSecond} placeholder="每秒" min={1} />
                </>
              )}
            </span>
          </FormRow>
          <FormRow label="超时" hint="毫秒。不填＝30 秒；钩子卡住会把触发它的那一步一起拖住。">
            <NumberInput value={timeoutMs} onChange={setTimeoutMs} placeholder="30000" min={1} />
          </FormRow>

          <div className="flex items-center gap-2 pt-2">
            <Button tone="accent" onClick={submit} disabled={busy}>
              {busy ? '提交中…' : '登记'}
            </Button>
            <Button onClick={reset}>清空</Button>
          </div>
        </section>
      )}

      {/* 这一格只说明「引擎装没装」——服务端从 `hook_engine.is_some()` 算出 overall，
          redis 那一档的 status 也固定是 unknown。把它读成一次活的探测，就会在一次
          Redis 掉线之后仍然看到「up」。措辞按事实写。 */}
      {health.data && (
        <Readings
          from={health}
          source="GET /api/v1/hooks/health"
          caveat="overall 反映的是引擎有没有装配，不是一次活的探测；redis 那一档的 status 恒为 unknown。"
        >
          <div className="flex flex-wrap border-b border-[var(--line)] py-2">
            <Reading
              label="引擎"
              value={health.data.overall === 'up' ? '已装配' : '未装配'}
              tone={health.data.overall === 'up' ? 'ok' : 'danger'}
            />
            <Reading
              label="redis 档"
              value={health.data.tiers['redis'] ? '在名单里' : '不在'}
              note={`status=${health.data.tiers['redis']?.status ?? '未知'}（恒为 unknown，不是探测结果）`}
            />
            <Reading
              label="归档档"
              value={
                health.data.tiers['archive']?.configured
                  ? health.data.tiers['archive']?.status ?? '已配'
                  : '未配'
              }
              tone={health.data.tiers['archive']?.configured ? 'ok' : 'plain'}
            />
            <Reading label="读于" value={new Date(health.data.timestamp).toLocaleTimeString('zh-CN')} />
          </div>
        </Readings>
      )}
      {page.error ? (
        <ReadError what="钩子" error={page.error} onRetry={page.refresh} />
      ) : !page.data ? (
        <Loading what="钩子" />
      ) : hooks.length === 0 ? (
        <Empty>
          网关上一把钩子都没有登记。这也是「钩子引擎没配」时的同一个形状——接口在那种情况下答 503，
          会走「读不到」，所以空列表就是真的空。
        </Empty>
      ) : (
        <Table
          columns={[
            { key: 'id', title: 'id', render: (h) => <span className="mono text-[11px]">{h.id}</span> },
            { key: 'trigger', title: '何时', width: '140px', render: (h) => TRIGGER_LABEL[h.trigger] ?? h.trigger },
            { key: 'scope', title: '范围', width: '80px', render: (h) => h.scope ?? 'global' },
            {
              key: 'tool',
              title: '管哪个工具',
              width: '130px',
              render: (h) => (h.tool ? <span className="mono text-[11px]">{h.tool}</span> : '所有工具'),
            },
            { key: 'action', title: '做什么', render: (h) => actionSummary(h.action) },
            {
              key: 'rate',
              title: '限流 / 超时',
              width: '130px',
              render: (h) =>
                `${h.rate_limit ? `${h.rate_limit.per_second}/s` : '不限'} · ${
                  h.timeout_ms ? `${h.timeout_ms}ms` : '30s'
                }`,
            },
            {
              key: 'op',
              title: '',
              width: '64px',
              render: (h) => (
                <Button onClick={() => void remove(h)}>删除</Button>
              ),
            },
          ]}
          rows={hooks}
          rowKey={(h) => h.id}
        />
      )}
    </ViewFrame>
  );
}

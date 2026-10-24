import type { ReactNode } from 'react';
import { useServices } from '@/shell/context';
import { useShell, type ActivityId } from '@/shell/store';
import { useStreamStore } from '@/store/streamStore';

/**
 * 底部状态条：一眼能看到「现在有几件事不对」。
 *
 * 每一格都是一个读数，不是装饰：读不到时写「读不到」，不写 0——0 是「问了，
 * 答案是零」，两者在运维眼里要做的事完全相反。
 */
export function StatusBar() {
  const connectionStatus = useStreamStore((s) => s.connectionStatus);
  const { overview, alerts, notifications, llm, username } = useServices();
  const setActivity = useShell((s) => s.setActivity);
  const toggleSidebar = useShell((s) => s.toggleSidebar);
  const togglePanel = useShell((s) => s.togglePanel);
  const setPalette = useShell((s) => s.setPalette);

  const o = overview.data;
  const firing = alerts.alerts?.count ?? 0;

  return (
    <footer
      className="flex h-6 shrink-0 items-center border-t border-[var(--line)] bg-[var(--ink-1000)] text-[11px]"
      aria-label="状态条"
    >
      <Cell
        onClick={() => setActivity('workspace')}
        tone={connectionStatus === 'open' ? 'var(--ok)' : 'var(--warn)'}
        title="实时事件流。断开时画布与日志格都不会更新，但读数仍在轮询。"
      >
        实时流 {connectionStatus === 'open' ? '已连接' : connectionStatus}
      </Cell>

      <Cell
        onClick={() => setActivity('observability')}
        tone={
          overview.error
            ? 'var(--warn)'
            : o?.cluster_health === 'healthy'
              ? 'var(--ok)'
              : 'var(--warn)'
        }
        title={overview.error ?? '集群判词只覆盖告警面，不是每个组件都验过。'}
      >
        集群 {overview.error ? '读不到' : (o?.cluster_health ?? '—')}
      </Cell>

      <Cell onClick={() => setActivity('agents')} title="在岗 / 全部智能体">
        智能体 {o ? `${o.active_agents ?? '—'}/${o.total_agents ?? '—'}` : '—'}
      </Cell>

      <Cell onClick={() => setActivity('tasks')} title="运行中 / 就绪 / 失败">
        任务{' '}
        {o
          ? `${o.active_tasks ?? '—'} 跑 · ${o.queued_tasks ?? '—'} 等 · ${o.failed_tasks ?? '—'} 败`
          : '—'}
      </Cell>

      <Cell
        onClick={() => setActivity('alerts')}
        tone={alerts.error ? 'var(--warn)' : firing > 0 ? 'var(--danger)' : 'var(--ok)'}
        title={alerts.error ?? '正在 firing 的告警'}
      >
        告警 {alerts.error ? '读不到' : firing}
      </Cell>

      <Cell
        onClick={() => setActivity('settings')}
        tone={llm.status?.configured ? 'var(--ok)' : llm.loading ? 'var(--muted)' : 'var(--danger)'}
        title="模型上游是否已配置"
      >
        模型 {llm.loading ? '读取中' : llm.status?.configured ? '已配置' : '未配置'}
      </Cell>

      <Cell
        onClick={() => setActivity('inbox')}
        tone={notifications.error ? 'var(--warn)' : 'var(--paper-dim)'}
        title={notifications.error ?? '未读通知'}
      >
        收件 {notifications.error ? '读不到' : notifications.unreadCount}
      </Cell>

      <div className="ml-auto flex items-center">
        <Hint onClick={() => setPalette('commands')}>⌘K 命令</Hint>
        <Hint onClick={toggleSidebar}>⌘B 侧栏</Hint>
        <Hint onClick={togglePanel}>⌘J 面板</Hint>
        <span className="px-3 text-[var(--muted)]">{username ?? ''}</span>
      </div>
    </footer>
  );
}

function Cell({
  children,
  onClick,
  tone = 'var(--paper-dim)',
  title,
}: {
  children: ReactNode;
  onClick: () => void;
  tone?: string;
  title?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      className="mono h-full whitespace-nowrap px-2.5 transition-colors hover:bg-[var(--ink-800)]"
      style={{ color: tone }}
    >
      {children}
    </button>
  );
}

function Hint({ children, onClick }: { children: ReactNode; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="hidden h-full whitespace-nowrap px-2 text-[var(--muted)] transition-colors hover:bg-[var(--ink-800)] hover:text-[var(--paper-dim)] lg:block"
    >
      {children}
    </button>
  );
}

/** 供命令面板复用：跳到某一面。 */
export function gotoActivity(id: ActivityId) {
  useShell.getState().setActivity(id);
}

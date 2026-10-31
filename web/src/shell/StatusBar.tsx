import type { ReactNode } from 'react';
import { useServices } from '@/shell/context';
import { useShell, type ActivityId } from '@/shell/store';
import { useTheme } from '@/shell/theme';
import { Note } from '@/ui/Note';
import { themeById } from '@/styles/themes';
import { useStreamStore } from '@/store/streamStore';
import { pulseVerdict } from '@/store/livePulse';
import { useLivePulseStore } from '@/store/livePulseStore';
import { useNow } from '@/hooks/useNow';

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
  const theme = useTheme((s) => s.theme);

  const o = overview.data;
  const firing = alerts.alerts?.count ?? 0;

  // 这一格要能说出「已经多久没有帧」，就不能只靠帧来重渲染——帧不来的时候它
  // 一次都不会重渲染，读数会停在「刚刚」。所以它有自己的钟。
  //
  // 「没有帧」还要跟「有没有活」一起才成句：有活而全静是断流，没活而静是空闲。
  // 这一份读数来自轮询而不是帧，正是为了在帧全无的时候还有话可说——两样都取自
  // 总线，总线一停就什么都判不了。
  const pulse = useLivePulseStore((s) => s.pulse);
  const now = useNow(5_000);
  const inflight =
    o && (o.active_tasks !== null || o.queued_tasks !== null)
      ? (o.active_tasks ?? 0) + (o.queued_tasks ?? 0)
      : null;
  const stream = pulseVerdict(pulse, now, connectionStatus, inflight);

  return (
    <footer
      className="flex h-6 shrink-0 items-center border-t border-[var(--line)] bg-[var(--ink-1000)] text-[11px]"
      aria-label="状态条"
    >
      <Cell
        onClick={() => setActivity('workspace')}
        tone={stream.color}
        title={stream.title}
      >
        实时流 {stream.text}
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

      <Cell
        onClick={() => setActivity('settings')}
        tone="var(--muted)"
        title="配色主题：在这里点开设置换，或在命令面板里搜主题名"
      >
        主题 {themeById(theme).name}
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
    <Note
      as="button"
      type="button"
      onClick={onClick}
      text={title}
      className="mono h-full whitespace-nowrap px-2.5 transition-colors hover:bg-[var(--ink-800)]"
      style={{ color: tone }}
    >
      {children}
    </Note>
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

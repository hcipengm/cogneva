import { useServices } from '@/shell/context';
import { Icon } from '@/shell/icons';
import { useShell } from '@/shell/store';

/**
 * 顶栏：这是哪套系统、去哪找东西、有没有人找你。
 *
 * 中间那个搜索框不是输入框而是按钮——按下去开命令面板。做成真的输入框会
 * 让人在顶栏里打字，然后发现结果出现在别处。
 */
export function TitleBar() {
  const setPalette = useShell((s) => s.setPalette);
  const setActivity = useShell((s) => s.setActivity);
  const { alerts, notifications, username } = useServices();

  const firing = alerts.alerts?.count ?? 0;
  const unread = notifications.unreadCount;
  const initial = (username ?? '?').slice(0, 1).toUpperCase();

  return (
    <header className="flex h-9 shrink-0 items-center gap-3 border-b border-[var(--line)] bg-[var(--ink-800)] px-3">
      <div className="flex items-center gap-2">
        <span
          className="flex h-4 w-4 items-center justify-center"
          style={{ background: 'var(--brass)', color: 'var(--ink-1000)' }}
          aria-hidden="true"
        >
          <span className="mono text-[10px] font-semibold leading-none">C</span>
        </span>
        <span className="text-[12px] tracking-[0.06em] text-[var(--paper)]">Cogneva</span>
        <span className="text-[11px] text-[var(--muted)]">控制台</span>
      </div>

      <button
        type="button"
        onClick={() => setPalette('commands')}
        className="mx-auto flex h-6 w-[min(420px,40vw)] items-center gap-2 border border-[var(--line)] bg-[var(--ink-900)] px-2 text-[11px] text-[var(--muted)] transition-colors hover:border-[var(--line-strong)] hover:text-[var(--paper-dim)]"
        title="命令面板（⌘K）"
      >
        <Icon name="search" size={12} />
        <span>搜索命令、页面与动作</span>
        <span className="mono ml-auto text-[10px]">⌘K</span>
      </button>

      <div className="flex items-center gap-1">
        <button
          type="button"
          onClick={() => setActivity('alerts')}
          title={alerts.error ? '告警读数没读到' : `${firing} 条告警在 firing`}
          className="flex h-6 items-center gap-1.5 px-2 text-[11px] transition-colors hover:bg-[var(--ink-700)]"
          style={{ color: alerts.error ? 'var(--warn)' : firing > 0 ? 'var(--danger)' : 'var(--muted)' }}
        >
          <Icon name="alerts" size={14} />
          {alerts.error ? '?' : firing}
        </button>
        <button
          type="button"
          onClick={() => setActivity('inbox')}
          title={notifications.error ? '通知读数没读到' : `${unread} 条未读`}
          className="flex h-6 items-center gap-1.5 px-2 text-[11px] transition-colors hover:bg-[var(--ink-700)]"
          style={{ color: notifications.error ? 'var(--warn)' : unread > 0 ? 'var(--live)' : 'var(--muted)' }}
        >
          <Icon name="inbox" size={14} />
          {notifications.error ? '?' : unread}
        </button>
        <button
          type="button"
          onClick={() => setActivity('settings')}
          title="设置"
          className="flex h-6 w-6 items-center justify-center text-[var(--muted)] transition-colors hover:bg-[var(--ink-700)] hover:text-[var(--paper)]"
        >
          <Icon name="settings" size={14} />
        </button>
        <span
          className="mono ml-1 flex h-5 w-5 items-center justify-center border border-[var(--line-strong)] text-[10px] text-[var(--paper-dim)]"
          title={username ?? '未登录'}
        >
          {initial}
        </span>
      </div>
    </header>
  );
}

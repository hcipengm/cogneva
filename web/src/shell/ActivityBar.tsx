import { useServices } from '@/shell/context';
import { Icon, type IconName } from '@/shell/icons';
import { useShell, type ActivityId } from '@/shell/store';
import { Note } from '@/ui/Note';

/**
 * 最左侧的一条：每一格是一面。
 *
 * 顺序按「出事时先看谁」排，不按字母也不按重要度：工作台是常态，收件箱与
 * 告警是有人找你，任务与变更是流水线在跑，治理与观测是回头看。
 */
const ITEMS: { id: ActivityId; label: string; icon: IconName }[] = [
  { id: 'workspace', label: '工作台', icon: 'workspace' },
  { id: 'people', label: '员工', icon: 'agents' },
  { id: 'venues', label: '场地', icon: 'workspace' },
  { id: 'files', label: '文件', icon: 'folder' },
  { id: 'scm', label: '源代码管理', icon: 'scm' },
  { id: 'inbox', label: '收件箱', icon: 'inbox' },
  { id: 'alerts', label: '告警', icon: 'alerts' },
  { id: 'tasks', label: '任务', icon: 'tasks' },
  { id: 'projects', label: '项目', icon: 'projects' },
  { id: 'flow', label: '流程', icon: 'flow' },
  { id: 'comms', label: '通信', icon: 'comms' },
  { id: 'agents', label: '智能体', icon: 'agents' },
  { id: 'progress', label: '进度', icon: 'platform' },
  { id: 'changes', label: '变更', icon: 'changes' },
  { id: 'governance', label: '治理', icon: 'governance' },
  { id: 'observability', label: '观测', icon: 'observability' },
  { id: 'token', label: 'Token', icon: 'token' },
  { id: 'resources', label: '资源', icon: 'resources' },
  { id: 'storage', label: '存储', icon: 'storage' },
  { id: 'eval', label: '评测', icon: 'eval' },
  { id: 'timeline', label: '时间轴', icon: 'timeline' },
  { id: 'delivery', label: '构建与部署', icon: 'delivery' },
  { id: 'audit', label: '审计', icon: 'audit' },
];

export function ActivityBar() {
  const activity = useShell((s) => s.activity);
  const setActivity = useShell((s) => s.setActivity);
  const { alerts, notifications, signOut, username } = useServices();

  const firing = alerts.alerts?.count ?? 0;
  const unread = notifications.unreadCount;

  const badge = (id: ActivityId): string | null => {
    if (id === 'alerts') {
      if (alerts.error) return '?';
      return firing > 0 ? (firing > 99 ? '99+' : String(firing)) : null;
    }
    if (id === 'inbox') {
      if (notifications.error) return '?';
      return unread > 0 ? (unread > 99 ? '99+' : String(unread)) : null;
    }
    return null;
  };

  return (
    <nav
      aria-label="活动栏"
      className="flex h-full w-[104px] shrink-0 flex-col overflow-y-auto border-r border-[var(--line)] bg-[var(--ink-800)] py-1"
    >
      {ITEMS.map((item) => {
        const active = activity === item.id;
        const mark = badge(item.id);
        return (
          <Note
            as="button"
            key={item.id}
            type="button"
            onClick={() => setActivity(item.id)}
            text={item.label}
            aria-current={active ? 'page' : undefined}
            className="relative flex h-9 w-full shrink-0 items-center gap-2 pl-3 pr-2 text-left transition-colors"
            style={{ color: active ? 'var(--paper)' : 'var(--muted)' }}
          >
            {active && (
              <span
                className="absolute inset-y-1.5 left-0 w-[2px]"
                style={{ background: 'var(--brass)' }}
              />
            )}
            <Icon name={item.icon} size={16} />
            {/* 名字写出来，不藏在悬停提示里：图标只有形状，新读者认不出哪一格是
                「员工」、哪一格是「场地」，认不出就等于这一面不存在。 */}
            <span className="min-w-0 flex-1 truncate text-[11px]">{item.label}</span>
            {mark && (
              <span
                className="mono min-w-[15px] shrink-0 px-[3px] text-center text-[9px] leading-[14px]"
                style={{
                  background: mark === '?' ? 'var(--warn)' : 'var(--danger)',
                  color: 'var(--ink-1000)',
                }}
              >
                {mark}
              </span>
            )}
          </Note>
        );
      })}

      <div className="mt-auto flex flex-col">
        <Note
          as="button"
          type="button"
          onClick={() => setActivity('settings')}
          text="设置"
          aria-current={activity === 'settings' ? 'page' : undefined}
          className="relative flex h-9 w-full items-center gap-2 pl-3 pr-2 text-left transition-colors"
          style={{ color: activity === 'settings' ? 'var(--paper)' : 'var(--muted)' }}
        >
          {activity === 'settings' && (
            <span className="absolute inset-y-1.5 left-0 w-[2px]" style={{ background: 'var(--brass)' }} />
          )}
          <Icon name="settings" size={16} />
          <span className="min-w-0 flex-1 truncate text-[11px]">设置</span>
        </Note>
        <Note
          as="button"
          type="button"
          onClick={signOut}
          text={username ? `退出（${username}）` : '退出登录'}
          className="flex h-9 w-full items-center gap-2 pl-3 pr-2 text-left text-[var(--muted)] transition-colors hover:text-[var(--danger)]"
        >
          <Icon name="signout" size={16} />
          <span className="min-w-0 flex-1 truncate text-[11px]">退出</span>
        </Note>
      </div>
    </nav>
  );
}

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
  const { alerts, notifications, llm, overlays, username } = useServices();

  const firing = alerts.alerts?.count ?? 0;
  const unread = notifications.unreadCount;
  const initial = (username ?? '?').slice(0, 1).toUpperCase();

  // 模型状态在顶栏只用一个点说三种事，每种都写出来源：配了、没配、还没读到。
  // 「还没读到」不能画成「没配」——那会让人去重配一条已经配好的上游。
  const configured = llm.status?.configured;
  const modelTone =
    configured == null ? 'var(--muted)' : configured ? 'var(--ok)' : 'var(--warn)';
  const modelLabel =
    configured == null ? '模型状态还没读到' : configured ? '模型已配置' : '模型未配置';
  const readAt = llm.at ? new Date(llm.at).toLocaleTimeString('zh-CN') : '从没读到过';

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
        {/* 模型入口：顶栏直达配置，不必先进设置再找那一格。状态点与设置页读的是
            同一份读数（Shell 里那一份 useLlmStatus），所以两处不会各说一套。 */}
        <button
          type="button"
          onClick={overlays.openLlmSetup}
          title={`${modelLabel} · GET /api/v1/llm/status · 读于 ${readAt} · 点开配置（保存时服务端会按双协议各探测一次）`}
          aria-label={`${modelLabel}，点开配置模型`}
          className="flex h-6 items-center gap-1.5 px-2 text-[11px] transition-colors hover:bg-[var(--ink-700)] hover:text-[var(--paper)]"
          style={{ color: modelTone }}
        >
          <span
            className="inline-block h-1.5 w-1.5 rounded-full"
            style={{ background: modelTone }}
            aria-hidden="true"
          />
          模型
        </button>
        <button
          type="button"
          onClick={() => setActivity('settings')}
          title="设置"
          className="flex h-6 w-6 items-center justify-center text-[var(--muted)] transition-colors hover:bg-[var(--ink-700)] hover:text-[var(--paper)]"
        >
          <Icon name="settings" size={14} />
        </button>
        <button
          type="button"
          onClick={() => setPalette('help')}
          title="去哪找什么"
          aria-label="帮助：去哪找什么"
          className="flex h-6 w-6 items-center justify-center text-[var(--muted)] transition-colors hover:bg-[var(--ink-700)] hover:text-[var(--paper)]"
        >
          <Icon name="help" size={14} />
        </button>
        {/* 账号位以前是一个不可点的方块：读者点它，什么都问不出来。它现在是
            通往账号与凭据那一页的入口——上面写着谁登录了，点开就是那一页。 */}
        <button
          type="button"
          onClick={() => setActivity('settings')}
          title={username ? `${username} · 账号与凭据` : '还没登录 · 去设置'}
          aria-label="账号与凭据"
          className="mono ml-1 flex h-5 w-5 items-center justify-center border border-[var(--line-strong)] text-[10px] text-[var(--paper-dim)] transition-colors hover:border-[var(--brass)] hover:text-[var(--paper)]"
        >
          {initial}
        </button>
      </div>
    </header>
  );
}

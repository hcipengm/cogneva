import type { ReactNode } from 'react';
import { FileTree } from '@/files/FileTree';
import { SearchPanel } from '@/files/SearchPanel';
import { WorkspaceUsage } from '@/files/WorkspaceUsage';
import { useServices } from '@/shell/context';
import { Icon, type IconName } from '@/shell/icons';
import { useShell, type ActivityId } from '@/shell/store';
import { useStreamStore } from '@/store/streamStore';
import { Moment } from '@/ui/primitives';

/**
 * 左栏：每一面的目录与现在正在发生的事。
 *
 * 它不重复编辑器里那些端点。同一份读数在两个地方各轮询一次，除了双倍流量，
 * 还会在某一刻给出两个不同的数——读者没法判断哪个是真的。这里只放已经由
 * 工作台共享的那几份（告警、收件箱、集群概况）与跳转。
 */
export function SideBar() {
  const activity = useShell((s) => s.activity);
  const visible = useShell((s) => s.sidebarVisible);
  const toggleSidebar = useShell((s) => s.toggleSidebar);

  if (!visible) return null;

  return (
    <aside className="flex h-full w-[262px] shrink-0 flex-col border-r border-[var(--line)] bg-[var(--ink-800)]">
      <header className="flex h-9 shrink-0 items-center border-b border-[var(--line)] px-3">
        <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">
          {TITLES[activity]}
        </span>
        <button
          type="button"
          onClick={toggleSidebar}
          title="收起侧栏（⌘B）"
          className="ml-auto text-[var(--muted)] transition-colors hover:text-[var(--paper)]"
        >
          <Icon name="sidebar" size={15} />
        </button>
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto py-1">
        <ActivityPanel activity={activity} />
      </div>
    </aside>
  );
}

const TITLES: Record<ActivityId, string> = {
  workspace: '工作台',
  files: '资源管理器',
  inbox: '收件箱',
  agents: '智能体',
  tasks: '任务',
  alerts: '告警',
  changes: '变更',
  governance: '治理',
  observability: '观测',
  settings: '设置',
};

function Section({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="pb-2">
      <div className="flex items-center px-3 py-1.5">
        <h2 className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">{title}</h2>
        <div className="ml-auto flex items-center gap-1">{action}</div>
      </div>
      {children}
    </section>
  );
}

function Row({
  label,
  note,
  icon,
  onClick,
  tone,
}: {
  label: string;
  note?: ReactNode;
  icon?: IconName;
  onClick?: () => void;
  tone?: string;
}) {
  const body = (
    <>
      {icon && <Icon name={icon} size={13} className="mr-2 shrink-0 opacity-70" />}
      <span className="min-w-0 flex-1 truncate" style={tone ? { color: tone } : undefined}>
        {label}
      </span>
      {note && <span className="ml-2 shrink-0 text-[10px] text-[var(--muted)]">{note}</span>}
    </>
  );
  if (!onClick) {
    return <div className="flex items-center px-3 py-1 text-[12px] text-[var(--paper-dim)]">{body}</div>;
  }
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex w-full items-center px-3 py-1 text-left text-[12px] text-[var(--paper-dim)] transition-colors hover:bg-[var(--ink-700)] hover:text-[var(--paper)]"
    >
      {body}
    </button>
  );
}

function Note({ children }: { children: ReactNode }) {
  return <p className="px-3 py-1 text-[11px] leading-relaxed text-[var(--muted)]">{children}</p>;
}

/** 到别的一面的跳转。每一面都有几条，「出事时下一步该看哪」。 */
function CrossLinks({ links }: { links: { id: ActivityId; label: string; note?: string }[] }) {
  const setActivity = useShell((s) => s.setActivity);
  return (
    <Section title="相关">
      {links.map((link) => (
        <Row
          key={link.id}
          label={link.label}
          note={link.note}
          icon="chevronRight"
          onClick={() => setActivity(link.id)}
        />
      ))}
    </Section>
  );
}

function ActivityPanel({ activity }: { activity: ActivityId }) {
  if (activity === 'files')
    return (
      <>
        <SearchPanel />
        <FileTree />
        <WorkspaceUsage />
      </>
    );
  const setActivity = useShell((s) => s.setActivity);
  const { alerts, notifications, overview } = useServices();
  const connectionStatus = useStreamStore((s) => s.connectionStatus);
  const activityRows = useStreamStore((s) => s.activity);

  switch (activity) {
    case 'workspace':
      return (
        <>
          <Section title="实时流">
            <Row
              label={connectionStatus === 'open' ? '已连接' : connectionStatus}
              note={`最近 ${activityRows.length} 条事件`}
              tone={connectionStatus === 'open' ? 'var(--ok)' : 'var(--warn)'}
            />
            <Note>
              画布上画的是总线上真的来了什么。没连上时它本来就是空的——那不是「平台没干活」。
            </Note>
          </Section>
          <Section title="快捷入口">
            <Row
              label="收件箱"
              note={notifications.error ? '?' : String(notifications.unreadCount)}
              icon="inbox"
              onClick={() => setActivity('inbox')}
            />
            <Row
              label="告警"
              note={alerts.error ? '?' : String(alerts.alerts?.count ?? 0)}
              icon="alerts"
              onClick={() => setActivity('alerts')}
            />
            <Row label="任务队列" icon="tasks" onClick={() => setActivity('tasks')} />
            <Row label="在岗智能体" icon="agents" onClick={() => setActivity('agents')} />
          </Section>
          <Section title="键盘">
            <Row label="命令面板" note="⌘K" />
            <Row label="收起/展开侧栏" note="⌘B" />
            <Row label="收起/展开面板" note="⌘J" />
          </Section>
        </>
      );

    case 'inbox': {
      const unread = notifications.items.filter((n) => !n.is_read);
      return (
        <>
          <Section title="读数">
            <Row label="未读" note={notifications.error ? '读不到' : String(notifications.unreadCount)} />
            <Row label="已收" note={String(notifications.items.length)} />
            {notifications.error && (
              <Note>
                这次没读到通知列表。空列表在这里不等于「没人发过」——它只说明清单没回来。
              </Note>
            )}
          </Section>
          <Section title="未读在前">
            {unread.length === 0 ? (
              <Note>没有未读通知。</Note>
            ) : (
              unread.slice(0, 8).map((n) => (
                <Row
                  key={n.id}
                  label={n.title}
                  note={<Moment iso={n.created_at} />}
                  onClick={() => setActivity('inbox')}
                />
              ))
            )}
          </Section>
        </>
      );
    }

    case 'alerts': {
      const list = alerts.alerts?.alerts ?? [];
      return (
        <>
          <Section title="读数">
            <Row
              label="正在 firing"
              note={alerts.error ? '读不到' : String(list.length)}
              tone={list.length > 0 ? 'var(--danger)' : undefined}
            />
            <Row
              label="未确认"
              note={String(list.filter((a) => !a.acknowledged_at).length)}
            />
            <Row
              label="耐久源"
              note={
                alerts.alerts
                  ? alerts.alerts.durable_alerts_read === null
                    ? '未接'
                    : alerts.alerts.durable_alerts_read
                      ? '已读到'
                      : '读失败'
                  : '—'
              }
              tone={
                alerts.alerts?.durable_alerts_read === false ? 'var(--warn)' : undefined
              }
            />
            {alerts.error && (
              <Note>告警读数没读到。列表短了不代表清掉了——它可能根本没读上来。</Note>
            )}
          </Section>
          <Section title="在 firing">
            {list.length === 0 ? (
              <Note>当前没有 firing 的告警。</Note>
            ) : (
              list.slice(0, 6).map((a) => (
                <Row
                  key={a.id}
                  label={a.message}
                  note={a.severity === 'critical' ? 'critical' : undefined}
                  tone={a.severity === 'critical' ? 'var(--danger)' : undefined}
                  onClick={() => setActivity('alerts')}
                />
              ))
            )}
          </Section>
        </>
      );
    }

    case 'tasks':
      return (
        <>
          <Section title="状态的读法">
            <Row label="ready" note="等执行者认领" />
            <Row label="running" note="正在执行" />
            <Row label="completed" note="有结局" />
            <Row label="failed" note="带错误收场" />
            <Note>
              停在上面的四个数之外的状态也会出现在列表里——过滤只筛服务端认的那几个词。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'agents', label: '在岗智能体', note: '谁在领任务' },
              { id: 'changes', label: '变更队列', note: '任务产出了什么' },
            ]}
          />
        </>
      );

    case 'agents':
      return (
        <>
          <Section title="这一面怎么读">
            <Note>
              名单是「谁能被叫到」：每个条目带角色、能力、主机与最后一次心跳。心跳停在多久以前，
              比它现在写着什么更说明问题。
            </Note>
            <Row label="对话面板" icon="workspace" onClick={() => setActivity('workspace')} />
          </Section>
          <CrossLinks
            links={[
              { id: 'tasks', label: '任务', note: '谁在干什么' },
              { id: 'governance', label: '治理', note: '这些角色谁在守' },
            ]}
          />
        </>
      );

    case 'changes':
      return (
        <>
          <Section title="两半">
            <Row label="台账" note="谁还欠一次判读" />
            <Row label="流水线队列" note="谁在等一次构建" />
            <Note>
              两半刻意不相加：一个数的是欠账，另一个数的是排队。加起来只会得到一个解释不了的总数。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'governance', label: '治理', note: '面的判词' },
              { id: 'observability', label: '观测', note: '部署有没有跟上' },
            ]}
          />
        </>
      );

    case 'governance':
      return (
        <>
          <Section title="判词">
            <Row label="on_target" note="达标" tone="var(--ok)" />
            <Row label="红档" note="拦变更" tone="var(--danger)" />
            <Row label="黄档" note="报告不拦" tone="var(--warn)" />
            <Row label="没有实测" note="没读过" />
            <Note>
              所有者的「声明」与「实测」不一致时，表格里写的是实测那一侧，并标出这一格取自哪一侧。
            </Note>
          </Section>
          <CrossLinks links={[{ id: 'changes', label: '变更', note: '判词要求的落地' }]} />
        </>
      );

    case 'observability':
      return (
        <>
          <Section title="集群">
            <Row
              label={overview.data ? `判词 ${overview.data.cluster_health}` : overview.error ? '读不到' : '读取中'}
              tone={
                overview.data?.cluster_health === 'healthy'
                  ? 'var(--ok)'
                  : overview.data
                    ? 'var(--warn)'
                    : undefined
              }
            />
            <Row label="在岗智能体" note={String(overview.data?.active_agents ?? '—')} />
            <Row label="活动任务" note={String(overview.data?.active_tasks ?? '—')} />
            {overview.error && <Note>集群概况读不到。下面各格可能仍有读数，它们各自独立。</Note>}
          </Section>
          <CrossLinks
            links={[
              { id: 'alerts', label: '告警', note: '健康判词的来源' },
              { id: 'tasks', label: '任务', note: '任务面的全量' },
            ]}
          />
        </>
      );

    case 'settings':
      return (
        <>
          <Section title="这一页有什么">
            <Row label="模型" note="上游配置与连通探测" />
            <Row label="用量" note="按执行者的令牌花销" />
            <Row label="评估集" note="可跑的评估用例" />
            <Row label="界面" note="侧栏与面板的开关" />
          </Section>
          <Note>
            模型配置保存时会在服务端做双协议探测；失败会如实写回来，不会假装存好了。
          </Note>
        </>
      );
  }
}

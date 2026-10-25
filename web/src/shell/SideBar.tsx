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
  scm: '源代码管理',
  inbox: '收件箱',
  agents: '智能体',
  tasks: '任务',
  flow: '流程',
  comms: '通信',
  alerts: '告警',
  changes: '变更',
  governance: '治理',
  observability: '观测',
  resources: '资源',
  storage: '存储',
  eval: '评测',
  delivery: '构建与部署',
  audit: '审计',
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

    case 'scm':
      return (
        <>
          <Section title="这一面怎么读">
            <Row label="已暂存" note="下一次提交会带走" />
            <Row label="未暂存" note="还没被选中" />
            <Row label="提交历史" note="只在这个工作区里" />
            <Note>
              历史只留在自己的工作区：没有远端，也没有推送。差异页的行号由服务端算，
              取用的那一行会先跟盘上的指纹对一次。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'files', label: '资源管理器', note: '改动落在哪个文件' },
              { id: 'changes', label: '变更', note: '平台自己的变更队列' },
            ]}
          />
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

    case 'flow':
      return (
        <>
          <Section title="这一面怎么读">
            <Row label="意图" note="一个 goal 一行" />
            <Row label="被挡住" note="等前面的任务完成" />
            <Row label="就绪" note="没有依赖，等人认领" />
            <Row label="执行中" note="已经有人在做" />
            <Note>
              任务面是对象图（谁等谁），这一面是时间轴（停在哪一段）。评测与落地在各自的
              面上，不在这条线上。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'tasks', label: '任务', note: '依赖与归属的细节' },
              { id: 'agents', label: '智能体', note: '执行者自己的读数' },
            ]}
          />
        </>
      );

    case 'comms':
      return (
        <>
          <Section title="边的四种">
            <Row label="交接" note="跑完了，下游被放出来" tone="var(--paper-dim)" />
            <Row label="重试" note="失败，还会再跑一次" tone="var(--warn)" />
            <Row label="死信" note="失败，不再重试" tone="var(--danger)" />
            <Row label="重启" note="这一格没有写入者" />
            <Note>
              图在网关进程的内存里，重启即空。空图是「这个进程还没见过一次完成」，
              不是「没有过交接」。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'tasks', label: '任务', note: '依赖与结局的细节' },
              { id: 'flow', label: '流程', note: '停在哪一段' },
            ]}
          />
        </>
      );

    case 'resources':
      return (
        <>
          <Section title="这一面怎么读">
            <Row label="配额" note="准入前：申请会不会被拒" />
            <Row label="余量" note="调度时：还能不能再塞一个 Pod" />
            <Row label="账本" note="事后：盘上真的占了多少" />
            <Note>
              三段读的是同一个命名空间在不同时刻的账：配额在准入时决定，余量在调度时决定，
              账本事后才量得出来。混成一个「资源使用率」会让三种处置看起来一样。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'observability', label: '观测', note: '节点 / Pod 的逐层读数' },
              { id: 'delivery', label: '构建与部署', note: '谁在吃这些资源' },
            ]}
          />
        </>
      );

    case 'storage':
      return (
        <>
          <Section title="三层记忆">
            <Row label="原始来源" note="工具输出原样归档，不带字节列举" />
            <Row label="schema" note="从原始来源抽出来的结构" />
            <Row label="summary" note="压过一遍的摘要" />
            <Note>
              上面的计数从网关进程启动算起，重启归零；存着多少看下面的列表，那是盘上的。
              资源面看的是盘还剩多少空，这一面看的是盘里有什么。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'resources', label: '资源', note: '盘还有多少空' },
              { id: 'observability', label: '观测', note: '归档循环在不在跑' },
            ]}
          />
        </>
      );

    case 'eval':
      return (
        <>
          <Section title="两列">
            <Row label="数据集" note="数据目录里的输入文件" />
            <Row label="报告" note="跑完落盘的结论" />
            <Note>
              空列表说的是这个目录里没有，不是平台没跑过。只有「跑一次」是写，其余都是读；
              报告存下来才会有下一次可比的基线。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'changes', label: '变更', note: '这次改动凭什么进集群' },
              { id: 'storage', label: '存储', note: '数据目录里还有什么' },
            ]}
          />
        </>
      );

    case 'delivery':
      return (
        <>
          <Section title="这一面怎么读">
            <Row label="版本" note="Deployment 的 revision 注解" />
            <Row label="就绪 / 目标" note="滚动还没走完时会差一截" />
            <Row label="条件" note="Progressing=False 是卡住" />
            <Note>
              构建运行与门禁读数还没有服务端来源，这一面只画部署版本。没有数据的那一半写
              「没有来源」，不画一张空图。
            </Note>
          </Section>
          <CrossLinks
            links={[
              { id: 'changes', label: '变更', note: '这些版本由哪些改动推上来' },
              { id: 'observability', label: '观测', note: '跑起来的副本健康' },
            ]}
          />
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

    case 'audit':
      return (
        <>
          <Section title="这一页有什么">
            <Row label="留档事件" note="只追加，读不写" />
            <Row label="过滤" note="触发类型 / 智能体 / 时间窗" />
            <Row label="展开" note="点内容那一格看整段" />
          </Section>
          <Note>
            审计只覆盖 hook 事件：没有事件不等于没有动作，只是那一种动作不进这张表。
          </Note>
          <CrossLinks
            links={[
              { id: 'observability', label: '观测', note: '现在的读数' },
              { id: 'governance', label: '治理', note: '判据与放行' },
            ]}
          />
        </>
      );

    case 'settings':
      return (
        <>
          <Section title="这一页有什么">
            <Row label="模型" note="上游配置与连通探测" />
            <Row label="额度与用量" note="按账号的剩余额度；按执行者的花销在观测页" />
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

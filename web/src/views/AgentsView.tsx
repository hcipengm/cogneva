import { listAgents, type AgentSummary } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Button, Empty, Loading, Moment, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';

/**
 * 在岗名单。
 *
 * 读的是注册表（活着的 unit），不是任务图里的 owner 名字：后者可能是一个已经退
 * 场的会话名，对着它说话永远没人应答。所以这一面的每一行都是「现在能叫到的人」，
 * 行数就是当下能同时干活的个数。
 */
export function AgentsView() {
  const { data, error, loading, refresh } = useResource('在岗名单', listAgents, 15_000);
  const agents = data ?? [];
  const roles = new Map<string, number>();
  for (const agent of agents) {
    roles.set(agent.role, (roles.get(agent.role) ?? 0) + 1);
  }

  return (
    <ViewFrame
      title="智能体"
      meta={`${agents.length} 个在岗 · ${roles.size} 种角色 · 按 15 秒重读`}
      actions={<Button onClick={refresh}>重读</Button>}
    >
      {error ? (
        <ReadError what="在岗名单" error={error} onRetry={refresh} />
      ) : !data && loading ? (
        <Loading what="在岗名单" />
      ) : agents.length === 0 ? (
        <Empty>当前没有在岗的智能体。心跳过期的注册不会出现在这里。</Empty>
      ) : (
        <>
          <div className="flex flex-wrap border-b border-[var(--line)] py-2">
            {[...roles.entries()].map(([role, count]) => (
              <Reading key={role} label={role} value={count} />
            ))}
          </div>
          <Table<AgentSummary>
            columns={[
              { key: 'id', title: 'agent_id', render: (a) => <span className="mono text-[11px]">{a.agent_id}</span> },
              { key: 'role', title: '角色', width: '110px', render: (a) => a.role },
              { key: 'cap', title: '能力', render: (a) => (
                <span className="text-[11px] text-[var(--paper-dim)]">
                  {a.capabilities.length ? a.capabilities.join(' · ') : '—'}
                </span>
              ) },
              { key: 'host', title: '主机', width: '160px', render: (a) => (
                <span className="mono text-[11px] text-[var(--muted)]">{a.hostname}</span>
              ) },
              { key: 'ip', title: 'Pod IP', width: '130px', render: (a) => (
                <span className="mono text-[11px] text-[var(--muted)]">{a.pod_ip}</span>
              ) },
              { key: 'beat', title: '最后一次心跳', width: '110px', render: (a) => <Moment iso={a.last_heartbeat} /> },
            ]}
            rows={agents}
            rowKey={(a) => a.agent_id}
          />
        </>
      )}
    </ViewFrame>
  );
}

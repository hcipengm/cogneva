import { useMemo } from 'react';
import { listAgents, type AgentSummary } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import {
  rosterWithFrames,
  seenAfterSnapshot,
  type LiveAgentRow,
} from '@/store/liveAgents';
import { useLiveAgentStore } from '@/store/liveAgentStore';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';

/**
 * 在岗名单。
 *
 * 读的是注册表（活着的 unit），不是任务图里的 owner 名字：后者可能是一个已经退
 * 场的会话名，对着它说话永远没人应答。所以这一面的每一行都是「现在能叫到的人」，
 * 行数就是当下能同时干活的个数。
 *
 * 名单是每 15 秒读一次的快照，**状态那一列不是**：它由总线上的帧折出来（`agent_start`
 * / `state_change` / `agent_end` / `agent_error`），心跳则推「最后一次心跳」那一格。
 * 少了这一折，一个人刚起来要等下一拍轮询才出现、换了状态要等下一拍才显示——这一面
 * 就成了每 15 秒拍一张照片。
 */

const STATE_LABEL: Record<string, string> = {
  started: '已启动',
  ended: '已结束',
  error: '出错',
};

/** 状态格：帧说它现在是什么。没有过状态帧时明写「—」，不拿「空闲」去冒充。 */
function LiveState({ live }: { live: LiveAgentRow | null }) {
  if (!live || !live.state) {
    return <span className="text-[11px] text-[var(--muted)]">—</span>;
  }
  const label = STATE_LABEL[live.state] ?? live.state;
  const tone =
    live.state === 'error'
      ? 'var(--danger)'
      : live.state === 'ended'
        ? 'var(--paper-dim)'
        : live.state === 'started'
          ? 'var(--ok)'
          : 'var(--brass)';
  return (
    <span className="inline-flex items-baseline gap-1.5">
      <span className="text-[11px]" style={{ color: tone }} title={live.detail ?? undefined}>
        {label}
      </span>
      <span
        className="mono text-[10px] text-[var(--brass)]"
        title={`这一格来自 ${live.stateFrame} 帧（${live.stateAt}），不是这次轮询`}
      >
        帧
      </span>
    </span>
  );
}

export function AgentsView() {
  const resources = useResource('在岗名单', listAgents, 15_000);
  const { data, error, loading, refresh } = resources;
  const agents = data ?? [];
  const liveRows = useLiveAgentStore((s) => s.rows);
  const { rows, unseen } = useMemo(
    () => rosterWithFrames(agents, liveRows),
    [agents, liveRows]
  );
  const roles = new Map<string, number>();
  for (const agent of agents) {
    roles.set(agent.role, (roles.get(agent.role) ?? 0) + 1);
  }
  const fromFrame = rows.filter(({ row, live }) =>
    seenAfterSnapshot(row.last_heartbeat, live)
  ).length + unseen.length;

  return (
    <ViewFrame
      title="智能体"
      meta={`${agents.length} 个在岗 · ${roles.size} 种角色 · ${fromFrame} 条有比这次轮询更新的帧 · 快照按 15 秒重读`}
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
          <Readings from={resources} source="GET /api/v1/agents 按角色数这一页" caveat="数的是这次读回来的名单，心跳过期的注册不在里面。状态一列不是这份快照给的，是总线上的帧折出来的。">
          <div className="flex flex-wrap border-b border-[var(--line)] py-2">
            {[...roles.entries()].map(([role, count]) => (
              <Reading key={role} label={role} value={count} />
            ))}
          </div>
          </Readings>
          {unseen.length > 0 && (
            <div className="border-b border-[var(--line)] py-2">
              <div className="mb-1 text-[11px] text-[var(--paper-dim)]">
                帧里有、名单里还没有（{unseen.length}）：刚起来的执行者要等下一拍轮询才进名单，现在已经在干活了。
              </div>
              {unseen.map((entry) => (
                <div key={entry.agentId} className="mono text-[11px] text-[var(--muted)]">
                  <span className="mr-2 text-[var(--paper)]">{entry.agentId}</span>
                  <LiveState live={entry} />
                  <span className="ml-2">
                    最后一帧 <Moment iso={entry.seenAt} />
                  </span>
                </div>
              ))}
            </div>
          )}
          <Table<{ row: AgentSummary; live: LiveAgentRow | null }>
            columns={[
              { key: 'id', title: 'agent_id', render: ({ row: a }) => <span className="mono text-[11px]">{a.agent_id}</span> },
              { key: 'state', title: '状态', width: '120px', render: ({ live }) => <LiveState live={live} /> },
              { key: 'role', title: '角色', width: '110px', render: ({ row: a }) => a.role },
              { key: 'cap', title: '能力', render: ({ row: a }) => (
                <span className="text-[11px] text-[var(--paper-dim)]">
                  {a.capabilities.length ? a.capabilities.join(' · ') : '—'}
                </span>
              ) },
              { key: 'host', title: '主机', width: '160px', render: ({ row: a }) => (
                <span className="mono text-[11px] text-[var(--muted)]">{a.hostname}</span>
              ) },
              { key: 'ip', title: 'Pod IP', width: '130px', render: ({ row: a }) => (
                <span className="mono text-[11px] text-[var(--muted)]">{a.pod_ip}</span>
              ) },
              { key: 'beat', title: '最后一次心跳', width: '130px', render: ({ row: a, live }) => {
                const fromBeat = seenAfterSnapshot(a.last_heartbeat, live);
                return (
                  <span className="inline-flex items-baseline gap-1.5">
                    <Moment iso={fromBeat && live ? live.seenAt : a.last_heartbeat} />
                    {fromBeat && (
                      <span className="mono text-[10px] text-[var(--brass)]" title="这条心跳比这次轮询读到的快照更晚">
                        帧
                      </span>
                    )}
                  </span>
                );
              } },
            ]}
            rows={rows}
            rowKey={({ row }) => row.agent_id}
          />
        </>
      )}
    </ViewFrame>
  );
}

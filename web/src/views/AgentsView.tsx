import { useMemo } from 'react';
import { listAgents, type AgentSummary } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import {
  rosterWithFrames,
  seenAfterSnapshot,
  type LiveAgentRow,
} from '@/store/liveAgents';
import { useLiveAgentStore } from '@/store/liveAgentStore';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { BarList, Strip, type Tone } from '@/ui/plots';

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

/**
 * 帧里那串名字怎么念。
 *
 * 前三个不是执行者的生命周期状态，是两种帧自己的说法：`agent_start` 说「起来了」、
 * `agent_end` 说「结束了」、`agent_error` 说「出错了」——它们没有对应的
 * `AgentState`，所以不能并进下面那张表里假装是同一套词。
 *
 * 其余八个是 `AgentState` 的线上写法（serde 的 snake_case），由生命周期的状态转换
 * 帧直接带过来。这一格照抄而不翻译成别的词：屏幕上写的和总线上流的是同一串，
 * 排查时才对得上。没有映射的名字（新加的状态、或线上真出现了别的写法）照原样显示，
 * 不退回一个看着体面的默认值——那会把「不认识」画成「没事」。
 */
const STATE_LABEL: Record<string, string> = {
  // 帧自己的说法
  started: '已启动',
  ended: '已结束',
  error: '出错',
  // AgentState
  init: '初始化',
  registered: '已注册',
  active: '在跑',
  idle: '空闲',
  completing: '收尾中',
  inactive: '已停用',
  suspect: '失联',
  dead: '已判死',
};

/** 状态的色：同一种处境用同一种色，不靠位次猜。 */
const STATE_TONE: Record<string, string> = {
  error: 'var(--danger)',
  dead: 'var(--danger)',
  suspect: 'var(--warn)',
  ended: 'var(--paper-dim)',
  inactive: 'var(--paper-dim)',
  started: 'var(--ok)',
  active: 'var(--ok)',
};

/** 状态在分布条上的色：与 `STATE_TONE` 同一套词，只是换成图能用的那几种。 */
const STATE_PLOT_TONE: Record<string, Tone> = {
  error: 'danger',
  dead: 'danger',
  suspect: 'warn',
  started: 'ok',
  active: 'ok',
  ended: 'muted',
  inactive: 'muted',
};

/** 状态格：帧说它现在是什么。没有过状态帧时明写「—」，不拿「空闲」去冒充。 */
function LiveState({ live }: { live: LiveAgentRow | null }) {
  if (!live || !live.state) {
    return <span className="text-[11px] text-[var(--muted)]">—</span>;
  }
  const label = STATE_LABEL[live.state] ?? live.state;
  const tone = STATE_TONE[live.state] ?? 'var(--brass)';
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
  // 名单是每 15 秒一拍读回来的快照；谁起来了、谁换态了、谁走了，都在总线上。
  // 没有这一行，把间隔一调长，这张表就停在上一次读回来的那一刻——而平台早就
  // 换了一拨人。
  useKindTickedRefresh(['agents', resourceKey('agents')], resources.refresh);
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
  // 状态分布只数帧里说过的那几个人：这一列本来就不是快照给的，没有状态帧的行如实
  // 单独成一段，不并进「空闲」——那是把「不知道」画成「没事」。
  const states = new Map<string, { label: string; value: number; tone: Tone }>();
  for (const { live } of rows) {
    const key = live?.state ?? '';
    const seen = states.get(key);
    if (seen) {
      seen.value += 1;
    } else {
      states.set(key, {
        label: key ? (STATE_LABEL[key] ?? key) : '没有状态帧',
        value: 1,
        tone: STATE_PLOT_TONE[key] ?? 'magnitude',
      });
    }
  }

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
            <Reading label="在岗" value={agents.length} />
            <Reading label="角色" value={roles.size} />
            <Reading
              label="比快照新"
              value={fromFrame}
              note="帧里见过、这次快照还没轮到"
            />
          </div>
          </Readings>
          {/* 名单这一屏要回答的第二问是「这一拨人是怎么构成的」：哪个角色人多、有几个人
              在跑。角色在表里是一列名字，读不出谁多谁少；状态分散在几十行里，读不出
              整体的分布——所以这两问画出来，确切的名字仍去表里查。 */}
          <div className="border-b border-[var(--line)]">
            <BarList
              label="每个角色在岗几人"
              note="按 GET /api/v1/agents 这次读回来的名单数，同一行一个人"
              rows={[...roles.entries()]
                .sort((a, b) => b[1] - a[1])
                .map(([role, count]) => ({ label: role, value: count }))}
            />
            <Strip
              label="这一拨人现在处在哪个状态"
              note="状态由总线上的帧折出来；「没有状态帧」是这一轮没听到过它，不是它空闲"
              segments={[...states.values()].sort((a, b) => b.value - a.value)}
            />
          </div>
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

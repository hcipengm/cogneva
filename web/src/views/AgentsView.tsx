import { useMemo } from 'react';
import { getAgentRoster, type AgentSummary } from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import {
  rosterWithFrames,
  seenAfterSnapshot,
  type LiveAgentRow,
} from '@/store/liveAgents';
import { useLiveAgentStore } from '@/store/liveAgentStore';
import { Button, Empty, Loading, Moment, ReadError, Reading, Readings, Table } from '@/ui/primitives';
import { BarList, Strip, type Tone } from '@/ui/plots';

import { Note } from '@/ui/Note';
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
 *
 * 这是员工页的「名册」标签，不是独立的一面：员工与智能体读的是同一份名单、同一批
 * 人，分成两页只会让同一件事有两处读数。这里只画「这一拨人现在怎么构成的」，说话
 * 那一半在同一个页面的「对话」标签里，点一行就切过去。
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
      <Note className="text-[11px]" style={{ color: tone }} text={live.detail ?? undefined}>
        {label}
      </Note>
      <Note
        className="mono text-[10px] text-[var(--brass)]"
        text={`这一格来自 ${live.stateFrame} 帧（${live.stateAt}），不是这次轮询`}
      >
        帧
      </Note>
    </span>
  );
}

/** 名册表上的一行：一个人，或者「还没派给人的座位」那一组。 */
type RosterGroup = {
  personId: string | null;
  displayName: string;
  roles: string[];
  agentIds: string[];
  caps: number;
  live: boolean;
  retired: boolean;
  /** 画状态、心跳、主机用的代表行：多顶帽子时取第一顶；没派人的那组取第一个座位。 */
  lead: AgentSummary | null;
  leadLive: LiveAgentRow | null;
};

export function EmployeeRoster({ onPick }: { onPick?: (agentId: string) => void }) {
  // 名册按人收好后再交给界面：座位是地址，人是身份。同一个人戴两顶帽子时，
  // 按座位展开的表会出现两次同一个人，读的人以为撞了名；而任务一多，座位能长
  // 到几百行，人却没变。收在服务端做一次（`people` / `seats_without_a_person`），
  // 这里不再自己数一遍——两处各数一次，两个数迟早会对不上。
  const resources = useResource('在岗名单', getAgentRoster, 15_000);
  // 名单是每 15 秒一拍读回来的快照；谁起来了、谁换态了、谁走了，都在总线上。
  // 没有这一行，把间隔一调长，这张表就停在上一次读回来的那一刻——而平台早就
  // 换了一拨人。
  useKindTickedRefresh(['agents', resourceKey('agents')], resources.refresh);
  const { data, error, loading, refresh } = resources;
  const agents = data?.agents ?? [];
  const people = data?.people ?? [];
  const seatsWithoutAPerson = data?.seats_without_a_person ?? null;
  const liveRows = useLiveAgentStore((s) => s.rows);
  const { rows, unseen } = useMemo(
    () => rosterWithFrames(agents, liveRows),
    [agents, liveRows]
  );
  // 手上有活的人此刻在岗没有，由注册表（`agents[].live`）回答；`people` 那一行
  // 已经替我们收好了，这里只按 agent id 找回代表行去画状态与心跳。
  const byId = useMemo(() => new Map(agents.map((a) => [a.agent_id, a])), [agents]);
  const groups = useMemo<RosterGroup[]>(
    () =>
      people.map((p) => {
        const lead = p.agent_ids.map((id) => byId.get(id)).find((a) => a !== undefined) ?? null;
        return {
          personId: p.person_id,
          displayName: p.display_name,
          roles: p.roles,
          agentIds: p.agent_ids,
          caps: p.caps,
          live: p.live,
          retired: p.retired,
          lead,
          leadLive: lead ? liveRows[lead.agent_id] ?? null : null,
        };
      }),
    [people, byId, liveRows]
  );
  // 名单现在装两种人：心跳上活着的，和说过话、名字在名册上但此刻不在线的。
  // 合成一个「在岗」数，读者分不出「没人干活」和「人都下线了」。
  const liveCount = groups.filter((group) => group.live).length;
  const restCount = groups.length - liveCount;
  const roles = new Map<string, number>();
  for (const group of groups) {
    for (const role of group.roles) {
      roles.set(role, (roles.get(role) ?? 0) + 1);
    }
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
    <div className="flex h-full min-h-0 flex-col overflow-auto">
      <div className="flex flex-wrap items-baseline gap-2 border-b border-[var(--line)] px-4 py-1.5 text-[11px] text-[var(--muted)]">
        <span>
          {`${liveCount} 人在岗${restCount > 0 ? ` · ${restCount} 人休息中` : ''} · ${roles.size} 种角色 · ${fromFrame} 条有比这次轮询更新的帧 · 快照按 15 秒重读`}
        </span>
        <span className="ml-auto">
          <Button onClick={refresh}>重读</Button>
        </span>
      </div>
      {error ? (
        <ReadError what="在岗名单" error={error} onRetry={refresh} />
      ) : !data && loading ? (
        <Loading what="在岗名单" />
      ) : groups.length === 0 ? (
        <Empty>
          {agents.length > 0
            ? '这一版网关还没返回按人收好的名册（people）；名册接口与员工页要用同一版。'
            : '当前没有在岗的智能体，也没有说过话的人。可对话的员工在说完话几十秒后会离开心跳，但仍留在名册上。'}
        </Empty>
      ) : (
        <>
          <Readings from={resources} source="GET /api/v1/agents 按人收好的名册" caveat="按人这一列由服务端收好：同一个人戴几顶帽子也只占一行；座位背后的人只记在名册（cogneva:employees）那一份上，名册读不到时「没派人的座位」这一格是空的，不是 0。">
          <div className="flex flex-wrap border-b border-[var(--line)] py-2">
            <Reading label="人" value={groups.length} />
            <Reading
              label="帽子"
              value={agents.length}
              note="可寻址的地址数：一个人可以同时戴几顶"
            />
            <Reading label="角色" value={roles.size} />
            <Reading
              label="比快照新"
              value={fromFrame}
              note="帧里见过、这次快照还没轮到"
            />
            <Reading
              label="没派人的座位"
              value={seatsWithoutAPerson === null ? '—' : seatsWithoutAPerson}
              note={
                seatsWithoutAPerson === null
                  ? '名册没读到：这个数与「量到 0」不是一回事'
                  : '座位背后还没有人；阶段收口时这个数要归零'
              }
            />
          </div>
          </Readings>
          {/* 名单这一屏要回答的第二问是「这一拨人是怎么构成的」：哪个角色人多、有几个人
              在跑。角色在表里是一列名字，读不出谁多谁少；状态分散在几十行里，读不出
              整体的分布——所以这两问画出来，确切的名字仍去表里查。 */}
          <div className="border-b border-[var(--line)]">
            <BarList
              label="每个角色几人"
              note="按人算：一个人同时戴几顶不同角色的帽子，就在几个角色里各算一次"
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
          <Table<RosterGroup>
            columns={[
              // 第一列是这个人叫什么，不是它内部的 id：名册给每个上过册的人发过
              // 「工号 姓名」，读的人要认的是那个；id 放进悬停里，排障时才需要。
              // 没派人的那一行不是一个人，名字由服务端直接写成「座位，尚未分配给人」。
              {
                key: 'person',
                title: '员工',
                render: (g) => (
                  <span className="inline-flex items-baseline gap-1.5">
                    <Note
                      className="text-[12px]"
                      text={g.personId ?? `${g.caps} 个座位，都还没有人`}
                    >
                      {g.displayName}
                    </Note>
                    {g.retired && (
                      <span className="text-[10px] text-[var(--muted)]">离职</span>
                    )}
                  </span>
                ),
              },
              { key: 'state', title: '状态', width: '120px', render: (g) => (
                g.live ? <LiveState live={g.leadLive} /> : (
                  <span className="text-[11px] text-[var(--muted)]">休息中</span>
                )
              ) },
              { key: 'role', title: '角色', width: '160px', render: (g) => (
                <span className="text-[11px] text-[var(--paper-dim)]">
                  {g.roles.length ? g.roles.join(' · ') : '—'}
                </span>
              ) },
              { key: 'caps', title: '帽子', width: '64px', render: (g) => (
                <span className="mono text-[11px] text-[var(--muted)]">{g.caps}</span>
              ) },
              // 主机、Pod IP、能力是「进程」那一层的，一个人的几顶帽子可以落在不
              // 同进程里；只有一顶时它就是那个人的地址，多顶时这一格不属于某一个人。
              { key: 'cap', title: '能力', render: (g) => (
                <span className="text-[11px] text-[var(--paper-dim)]">
                  {g.caps === 1 && g.lead?.capabilities.length
                    ? g.lead.capabilities.join(' · ')
                    : '—'}
                </span>
              ) },
              { key: 'host', title: '主机', width: '160px', render: (g) => (
                <span className="mono text-[11px] text-[var(--muted)]">
                  {g.caps === 1 ? g.lead?.hostname ?? '' : '—'}
                </span>
              ) },
              { key: 'ip', title: 'Pod IP', width: '130px', render: (g) => (
                <span className="mono text-[11px] text-[var(--muted)]">
                  {g.caps === 1 ? g.lead?.pod_ip ?? '' : '—'}
                </span>
              ) },
              { key: 'beat', title: '最后一次心跳', width: '130px', render: (g) => {
                if (!g.lead) {
                  return <span className="text-[11px] text-[var(--muted)]">—</span>;
                }
                const fromBeat = seenAfterSnapshot(g.lead.last_heartbeat, g.leadLive);
                return (
                  <span className="inline-flex items-baseline gap-1.5">
                    <Moment iso={fromBeat && g.leadLive ? g.leadLive.seenAt : g.lead.last_heartbeat} />
                    {fromBeat && (
                      <Note className="mono text-[10px] text-[var(--brass)]" text="这条心跳比这次轮询读到的快照更晚">
                        帧
                      </Note>
                    )}
                  </span>
                );
              } },
              // 名册与对话在同一个页面里，点一行就切到那个人的对话框：两页之间
              // 跳转曾要重新找人，而两边读的是同一份名单。帽子多于一时，这一行
              // 不指向某一个地址，按钮不出现——点哪一顶都由读的人自己说。
              ...(onPick
                ? [
                    {
                      key: 'talk',
                      title: '',
                      width: '64px',
                      render: (g: RosterGroup) =>
                        g.caps === 1 ? (
                          <Button onClick={() => onPick(g.agentIds[0])}>对话</Button>
                        ) : null,
                    },
                  ]
                : []),
            ]}
            rows={groups}
            rowKey={(g) => g.personId ?? '__unassigned__'}
          />
        </>
      )}
    </div>
  );
}

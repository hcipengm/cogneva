import { useState } from 'react';
import {
  getCollaborationGraph,
  getTaskCollaboration,
  listTasks,
  type CollaborationLink,
  type CollaborationLinkType,
  type TaskRow,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { outcomesSinceSnapshot, type LiveCommsRow } from '@/store/liveComms';
import { useLiveCommsStore } from '@/store/liveCommsStore';
import { Note } from '@/ui/Note';
import { openExplain } from '@/ui/explain';
import { Empty, Loading, Moment, ReadError, Reading, Readings, Table, ViewFrame } from '@/ui/primitives';
import { Strip, type Tone } from '@/ui/plots';

const LINK_LABELS: Record<CollaborationLinkType, string> = {
  hand_off: '交接',
  retry: '重试',
  dead_letter: '死信',
  respawn: '重启',
};

const LINK_TONES: Record<CollaborationLinkType, string> = {
  hand_off: 'var(--paper-dim)',
  retry: 'var(--warn)',
  dead_letter: 'var(--danger)',
  respawn: 'var(--live)',
};

function LinkType({ value }: { value: CollaborationLinkType }) {
  return (
    <span className="mono text-[11px]" style={{ color: LINK_TONES[value] }}>
      {LINK_LABELS[value] ?? value}
    </span>
  );
}

/**
 * 一条边的两端。
 *
 * 重试与死信是任务指向自己的自环——「它又跑了一次」没有第二个任务可指。把它们画成
 * 「A → A」会读成数据错了，所以写成「同一任务」。
 */
function TwoEnds({ link, onPick }: { link: CollaborationLink; onPick: (id: string) => void }) {
  const self = link.source_task_id === link.target_task_id;
  return (
    <span className="mono flex items-baseline gap-1 text-[11px]">
      <Note
        as="button"
        type="button"
        onClick={() => onPick(link.source_task_id)}
        className="max-w-[150px] truncate underline-offset-2 hover:underline"
        text={link.source_task_id}
      >
        {link.source_task_id}
      </Note>
      <span className="text-[var(--muted)]">{self ? '（同一任务）' : '→'}</span>
      {!self && (
        <Note
          as="button"
          type="button"
          onClick={() => onPick(link.target_task_id)}
          className="max-w-[150px] truncate underline-offset-2 hover:underline"
          text={link.target_task_id}
        >
          {link.target_task_id}
        </Note>
      )}
    </span>
  );
}

/**
 * 一条边的「智能体」：谁干的。
 *
 * 三种来路，各说各的，因为它们是三件事：
 * - 边上记着（重启边，以及新的交接/重试/死信边）：加边那一刻谁干的。之后任务换了
 *   人，这一格不变。
 * - 边没记（这条边是写入者补上之前加的）：退回任务行现在的归属——交过结果的人优先，
 *   其次是被指定的人。它答的是「这条任务现在归谁」，不是「当时谁干的」。
 * - 都查不到：任务不在这一页的列表里，或者从没被指派。写「不知道」，不做成按钮——
 *   点开只能说「不知道」的格子比不点更坏。
 */
function AgentCell({
  link,
  owners,
  tasksAt,
}: {
  link: CollaborationLink;
  owners: Map<string, TaskRow>;
  tasksAt: string | null;
}) {
  const buttonClass =
    'mono text-[11px] text-[var(--paper-dim)] underline decoration-dotted decoration-transparent underline-offset-4 transition-colors hover:decoration-[var(--muted)]';

  if (link.agent_id) {
    return (
      <button
        type="button"
        className={buttonClass}
        onClick={() =>
          openExplain({
            label: '智能体',
            value: link.agent_id as string,
            source: 'GET /api/v1/collaboration/graph',
            computedAt: link.timestamp,
            caveat:
              '这个执行者是加边那一刻记在边上的：之后任务换了人，这一格不变。重启边记的是「谁被重启」，交接与失败边记的是「结果是谁交的／谁指定来做」。',
          })
        }
      >
        {link.agent_id}
      </button>
    );
  }

  const row = owners.get(link.source_task_id);
  const current = row?.agent_id ?? row?.owner_unit ?? null;
  if (current) {
    return (
      <button
        type="button"
        className={buttonClass}
        onClick={() =>
          openExplain({
            label: '智能体（任务现在的归属）',
            value: current,
            source: 'GET /api/v1/tasks/list',
            computedAt: tasksAt,
            caveat:
              '这条边加的时候没有带执行者（那时还没有写入者）。这里显示的是任务行现在的归属：交过结果的人优先，其次是被指定的人。任务换过人，这一格会跟着变。',
          })
        }
      >
        <span className="mr-1 text-[10px] text-[var(--muted)]">现</span>
        {current}
      </button>
    );
  }

  return (
    <Note
      className="mono text-[11px] text-[var(--muted)]"
      text="边和它的任务都不知道执行者：任务不在这一页的列表里，或者从没被指派"
    >
      —
    </Note>
  );
}

/**
 * 一个任务的两侧：谁把它交出来的，它在完成后又把活交给了谁。
 *
 * 这里说的是「交接」，不是「依赖」：依赖关系在任务图里，是排程读的那份；这里的边
 * 只记下真的发生过的完成与失败，所以一个还没跑完的任务在这里两侧都是空的。
 */
function TaskLinks({ taskId, onClose }: { taskId: string; onClose: () => void }) {
  const links = useResource(
    `任务交接 ${taskId}`,
    () => getTaskCollaboration(taskId),
    30_000
  );
  const data = links.data;

  return (
    <section className="border-b border-[var(--line)]">
      <div className="flex items-center px-4 py-2">
        <span className="mono text-[11px] text-[var(--paper-dim)]">{taskId}</span>
        <button
          onClick={onClose}
          className="ml-auto text-[11px] text-[var(--muted)] hover:text-[var(--paper)]"
        >
          收起
        </button>
      </div>
      {links.error ? (
        <ReadError what="任务交接" error={links.error} onRetry={links.refresh} />
      ) : !data && links.loading ? (
        <Loading what="任务交接" />
      ) : (
        <div className="grid grid-cols-2 gap-4 px-4 pb-3">
          <div>
            <div className="mb-1 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              谁交过来的（{data?.upstream.length ?? 0}）
            </div>
            {data && data.upstream.length > 0 ? (
              <ul className="space-y-1">
                {data.upstream.map((link, index) => (
                  <li key={`${link.source_task_id}-${index}`} className="flex items-baseline gap-2">
                    <LinkType value={link.link_type} />
                    <Note className="mono truncate text-[11px]" text={link.source_task_id}>
                      {link.source_task_id}
                    </Note>
                    <Moment iso={link.timestamp} />
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-[11px] text-[var(--muted)]">这一侧没有边。</p>
            )}
          </div>
          <div>
            <div className="mb-1 text-[11px] uppercase tracking-wider text-[var(--muted)]">
              交给了谁（{data?.downstream.length ?? 0}）
            </div>
            {data && data.downstream.length > 0 ? (
              <ul className="space-y-1">
                {data.downstream.map((link, index) => (
                  <li key={`${link.target_task_id}-${index}`} className="flex items-baseline gap-2">
                    <LinkType value={link.link_type} />
                    <Note className="mono truncate text-[11px]" text={link.target_task_id}>
                      {link.target_task_id}
                    </Note>
                    <Moment iso={link.timestamp} />
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-[11px] text-[var(--muted)]">这一侧没有边。</p>
            )}
          </div>
        </div>
      )}
      <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
        两侧都空的时候，先分清是哪种空：任务还没跑完、没有下游，还是网关这个进程重启过。
        只有第一种能从任务自己身上读掉。
      </p>
    </section>
  );
}

/**
 * 总线刚说有了结局、而交接图还没画出来的那几个任务。
 *
 * 这一块不是边的一份更快拷贝：帧能说「这个任务完成了，图要给它的每个下游各加一条
 * 交接边」，却说不出它当时有几个下游；失败时也分不清是重试边还是死信边——那位
 * `retried` 标志在线上被抹掉了。所以这里只写自己知道的：谁、什么结局、会带出哪一类
 * 边，并且明写「表里还没有」，免得读者去数一个帧报不出的条数。
 */
function FreshOutcomes({ rows }: { rows: LiveCommsRow[] }) {
  return (
    <section className="border-b border-[var(--line)] bg-[color-mix(in_srgb,var(--brass)_8%,transparent)]">
      <div className="px-4 py-2 text-[11px] text-[var(--paper-dim)]">
        总线刚说 {rows.length} 个任务有了结局（比这张图更晚，图还没画）：
      </div>
      <ul className="px-4 pb-2">
        {rows.map((row) => (
          <li key={row.key} className="flex flex-wrap items-baseline gap-2 py-0.5 text-[11px]">
            <span className="mono text-[var(--paper)]">{row.taskId}</span>
            <span
              className={row.status === 'failed' ? 'text-[var(--danger)]' : 'text-[var(--ok)]'}
            >
              {row.status === 'failed' ? '失败' : '完成'}
            </span>
            <span className="text-[var(--muted)]">
              {row.edge === 'handoff'
                ? '图上要给它每个下游各加一条交接边（几条这一帧没说）'
                : '图上要给它自己加一条重试边或死信边（哪一种这一帧没说）'}
            </span>
            {row.agentId && <span className="mono text-[var(--muted)]">agent {row.agentId}</span>}
            <span className="text-[var(--muted)]">
              最近 <Moment iso={row.lastAt} />
            </span>
            <Note
              className="mono text-[10px] text-[var(--brass)]"
              text={`这一格来自 task_status_change 帧（${row.frames} 帧折成一行），不是这次轮询`}
            >
              帧
            </Note>
          </li>
        ))}
      </ul>
    </section>
  );
}

/**
 * 通信面：任务之间的交接是怎么发生的，以及这条链断在哪里。
 *
 * 这张图不是依赖图的副本。任务图记的是「谁该在谁之后跑」——排程靠它；这里记的是
 * 「谁跑完了、于是谁被放出来」「谁失败了、于是重试还是进死信」，由网关里那条任务
 * 事件监听器一条一条加上去。
 */
export function CommsView() {
  const graph = useResource('交接图', getCollaborationGraph, 15_000);
  const liveRows = useLiveCommsStore((s) => s.rows);
  // 帧比这份图晚，就说明它的边不可能是这份图里的：只列那几条，重读之后它们自己
  // 消失，所以同一条交接不会在表和这一块各出现一次。
  const fresh = outcomesSinceSnapshot(graph.at, liveRows);
  // 任务行是「谁干的」的正本：边上有执行者就用边上的，边上没有就退回这里。
  const tasks = useResource('任务归属', () => listTasks({ limit: 500 }), 60_000);
  // 「重读之后它们自己消失」靠的是一直有重读在发生。以前只有那一拍 15 秒：轮询
  // 一调长，那条边就会以「图上还没有」的样子留在这一块里，比这一面自己的读数还久。
  // 交接帧一到就重读，图当场把边收进去，这一块随之清空——这一块本来就是为两拍之间
  // 的那段时间准备的，不该在停顿里变成常驻。任务归属一并重读：边上要挂的人名，
  // 常常是和这条边同一个时刻才有的。
  useKindTickedRefresh(
    ['comms', 'tasks', resourceKey('collaboration'), resourceKey('tasks')],
    [graph.refresh, tasks.refresh]
  );
  const owners = new Map((tasks.data ?? []).map((task) => [task.id, task]));
  const [picked, setPicked] = useState<string | null>(null);

  const stats = graph.data?.stats;
  const links = graph.data?.links ?? [];
  // 新的在前：边是按发生顺序追加的，读者要看的是最近发生了什么。
  const ordered = [...links].reverse();

  return (
    <ViewFrame
      title="通信"
      meta={
        stats
          ? `${stats.total_links} 条边${
              fresh.length > 0 ? ` · ${fresh.length} 个结局刚推到、图还没画` : ''
            } · 按 15 秒重读`
          : '任务之间的交接 · 按 15 秒重读'
      }
    >
      {stats && (
        <Readings
          from={graph}
          source="GET /api/v1/collaboration/graph 的 stats 一栏"
          caveat="这张图活在网关进程的内存里：重启归零，归零不等于没有过交接。"
        >
        <div className="flex flex-wrap border-b border-[var(--line)] py-2">
          <Reading label="边总数" value={stats.total_links} />
          <Reading label="交接" value={stats.handoffs} tone={stats.handoffs > 0 ? 'ok' : 'plain'} />
          <Reading label="重试" value={stats.retries} tone={stats.retries > 0 ? 'warn' : 'plain'} />
          <Reading
            label="死信"
            value={stats.dead_letters}
            tone={stats.dead_letters > 0 ? 'danger' : 'plain'}
          />
          <Reading
            label="重启"
            value={stats.respawns}
            note="执行者被重启时手上有任务"
            caveat="只数「手上仍有在跑的任务的执行者被重启」：空转的执行者被重启不产生边，所以 0 说的是「没有在跑的任务被打断」，不是「没有重启过执行者」。"
          />
        </div>
        </Readings>
      )}
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">边的来路</h2>
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          一次任务完成，会给它的每个下游各加一条交接边；一次任务失败，会给它自己加一条
          重试边或死信边；一个执行者被重启且手上还有在跑的任务，会给那个任务加一条指向自己的
          重启边——被打断的是这个任务的执行，重启后接着做，所以两端是同一行。空转的执行者
          被重启不产生边。
        </p>
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          这张图存在网关进程的内存里：重启之后从空开始。空图不读作「没有过交接」，读作
          「这个进程还没见过一次完成或失败」。要跨重启看交接，得看任务自己的结局与任务图。
          「智能体」那一列记的是加边那一刻谁干的：重启边记「谁被重启」，交接边记「结果
          是谁交的」，失败边记「谁把它跑挂的」。写入者补上之前加的老边没有这一格，退回任务
          行看到的是「这条任务现在归谁」，那一格标着「现」。
        </p>
      </section>
      {/* 五种计数并排时看不出「这些边里几成是正常交接、几成是出事」。分成几段排开，
          一眼看见这条进程见过的通信里有多少是坏的。名字照抄线的写法，与表里那一列同一套。 */}
      {stats && stats.total_links > 0 && (
        <div className="border-b border-[var(--line)]">
          <Strip
            label="这些边各是哪一种"
            note="按 stats 那一栏数（GET /api/v1/collaboration/graph）；重启边只数「手上还有在跑的任务」的那些"
            segments={[
              { label: LINK_LABELS.hand_off, value: stats.handoffs, tone: 'magnitude' as Tone },
              { label: LINK_LABELS.retry, value: stats.retries, tone: 'warn' as Tone },
              { label: LINK_LABELS.dead_letter, value: stats.dead_letters, tone: 'danger' as Tone },
              { label: LINK_LABELS.respawn, value: stats.respawns, tone: 'magnitude' as Tone },
            ]}
          />
        </div>
      )}
      {fresh.length > 0 && <FreshOutcomes rows={fresh} />}
      {graph.error ? (
        <ReadError what="交接图" error={graph.error} onRetry={graph.refresh} />
      ) : !graph.data && graph.loading ? (
        <Loading what="交接图" />
      ) : ordered.length === 0 ? (
        <Empty>
          这个进程还没记到任何一条边。任务完成、失败，或执行者带着在跑的任务被重启，都会在这里
          留下一条；如果这三件事明明发生过，那要去问网关是不是重启过。
        </Empty>
      ) : (
        <Table
          columns={[
            { key: 'ends', title: '从谁到谁', width: '360px', render: (l) => <TwoEnds link={l} onPick={setPicked} /> },
            { key: 'type', title: '类型', width: '90px', render: (l) => <LinkType value={l.link_type} /> },
            {
              key: 'agent',
              title: '智能体',
              width: '150px',
              render: (l) => <AgentCell link={l} owners={owners} tasksAt={tasks.at} />,
            },
            { key: 'at', title: '时刻', width: '110px', render: (l) => <Moment iso={l.timestamp} /> },
            {
              key: 'open',
              title: '',
              width: '70px',
              render: (l) => (
                <button
                  type="button"
                  onClick={() => setPicked(l.target_task_id)}
                  className="text-[11px] text-[var(--muted)] hover:text-[var(--paper)]"
                >
                  看两侧
                </button>
              ),
            },
          ]}
          rows={ordered}
          rowKey={(l) => `${l.source_task_id}->${l.target_task_id}@${l.timestamp}`}
        />
      )}
      {picked && <TaskLinks taskId={picked} onClose={() => setPicked(null)} />}
      <p className="px-4 py-3 text-[11px] text-[var(--muted)]">
        智能体那一列：边上有值的，是加边那一刻的「谁干的」；标着「现」的，是这条边没记执行者
        时退回任务行看到的归属——交过结果的人优先，其次是被指定的人。两件事不一样，所以写法
        不一样：前者不会因为任务后来换人而变，后者会。两者都查不到的那一格写「—」，它说的是
        「这条边和它的任务都不知道」，不是「没人干」。
      </p>
      {stats && stats.total_links > 0 && (
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          这份读面不分页：{stats.total_links} 条边全在这里，新的在前。
        </p>
      )}
    </ViewFrame>
  );
}

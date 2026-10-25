import { useState } from 'react';
import {
  getCollaborationGraph,
  getTaskCollaboration,
  type CollaborationLink,
  type CollaborationLinkType,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, Loading, Moment, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';

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
      <button
        type="button"
        onClick={() => onPick(link.source_task_id)}
        className="max-w-[150px] truncate underline-offset-2 hover:underline"
        title={link.source_task_id}
      >
        {link.source_task_id}
      </button>
      <span className="text-[var(--muted)]">{self ? '（同一任务）' : '→'}</span>
      {!self && (
        <button
          type="button"
          onClick={() => onPick(link.target_task_id)}
          className="max-w-[150px] truncate underline-offset-2 hover:underline"
          title={link.target_task_id}
        >
          {link.target_task_id}
        </button>
      )}
    </span>
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
                    <span className="mono truncate text-[11px]" title={link.source_task_id}>
                      {link.source_task_id}
                    </span>
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
                    <span className="mono truncate text-[11px]" title={link.target_task_id}>
                      {link.target_task_id}
                    </span>
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
 * 通信面：任务之间的交接是怎么发生的，以及这条链断在哪里。
 *
 * 这张图不是依赖图的副本。任务图记的是「谁该在谁之后跑」——排程靠它；这里记的是
 * 「谁跑完了、于是谁被放出来」「谁失败了、于是重试还是进死信」，由网关里那条任务
 * 事件监听器一条一条加上去。
 */
export function CommsView() {
  const graph = useResource('交接图', getCollaborationGraph, 15_000);
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
          ? `${stats.total_links} 条边 · 按 15 秒重读`
          : '任务之间的交接 · 按 15 秒重读'
      }
    >
      {stats && (
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
            note="没有任何一处在写这个计数"
          />
        </div>
      )}
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">边的来路</h2>
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          一次任务完成，会给它的每个下游各加一条交接边；一次任务失败，会给它自己加一条
          重试边或死信边。除此之外没有别的写入者——所以「重启」那一格永远是 0，那是没人在
          写，不是没有重启。
        </p>
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          这张图存在网关进程的内存里：重启之后从空开始。空图不读作「没有过交接」，读作
          「这个进程还没见过一次完成或失败」。要跨重启看交接，得看任务自己的结局与任务图。
        </p>
      </section>
      {graph.error ? (
        <ReadError what="交接图" error={graph.error} onRetry={graph.refresh} />
      ) : !graph.data && graph.loading ? (
        <Loading what="交接图" />
      ) : ordered.length === 0 ? (
        <Empty>
          这个进程还没记到任何一条边。等一次任务完成或失败，它就会出现在这里；如果任务明明跑完过，
          那要去问网关是不是重启过。
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
              render: (l) => (
                <span className="mono text-[11px] text-[var(--paper-dim)]">{l.agent_id ?? '—'}</span>
              ),
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
        智能体那一列永远是空的：网关加边的时候没有把「谁干的」带上，这一格现在没有来路。
        要知道某个任务归谁，去任务面按任务 id 看它自己的行。
      </p>
      {stats && stats.total_links > 0 && (
        <p className="px-4 pb-3 text-[11px] text-[var(--muted)]">
          这份读面不分页：{stats.total_links} 条边全在这里，新的在前。
        </p>
      )}
    </ViewFrame>
  );
}

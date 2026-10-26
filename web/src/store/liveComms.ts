/**
 * 「谁刚刚交出了结果」——通信面里由帧折出来的那一半。
 *
 * 交接图是每 15 秒重读一次的快照，而让图长出边的那些事一直在总线上：一个任务
 * `completed`，网关的协作监听器就给它的每个下游各加一条交接边；一个任务 `failed`，
 * 就给自己加一条重试边或死信边。以前这些帧只进了活动流与时间轴，这一面上一条边
 * 都不动：交接要等下一拍轮询才出现，而这一面问的正是「现在这条链走到哪了」。
 *
 * 帧能说的**只有种类，说不出几条**：`completed` 之后会加几条交接边，取决于那个
 * 任务当时有几个下游——那是图知道、帧不知道的事；`failed` 之后是重试边还是死信边，
 * 取决于失败方有没有重试额度，这个判断在服务端，线上的帧把 `retried` 那一位抹掉了。
 * 所以这里折出来的不是边，是「图上马上要多出边」的那几个任务。硬把它画成一条边，
 * 就是替图编一条它还没算出来的线。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-live-comms.sh` 就是这么验的）。
 */

/** 线上的一帧，只取折叠用得到的那几个字段。 */
export interface CommsFrameInput {
  type: string;
  timestamp: string;
  payload: Record<string, unknown>;
}

/** 一帧能折成什么：一个任务的结局，以及它会在图上带出哪一类边。 */
export interface LiveCommsFrame {
  /** 同一个任务的同一种结局算同一次：这是它在总线上的身份。 */
  key: string;
  taskId: string;
  status: 'completed' | 'failed';
  at: string;
  agentId: string | null;
  /** 这一帧之后图上会出现的那一类边。说不清条数，就不说。 */
  edge: 'handoff' | 'retry_or_dead_letter';
}

/** 折叠后的一行。 */
export interface LiveCommsRow {
  key: string;
  taskId: string;
  status: 'completed' | 'failed';
  edge: 'handoff' | 'retry_or_dead_letter';
  agentId: string | null;
  /** 第一次见到这个结局的时刻。 */
  firstAt: string;
  /** 最近一帧的时刻。 */
  lastAt: string;
  /** 折进这一行的帧数。重放不算。 */
  frames: number;
}

const str = (value: unknown): string => (typeof value === 'string' ? value : '');

/**
 * 一帧 → 一条可折的结局；不是结局的帧返回 `null`。
 *
 * 只有 `completed` 与 `failed` 会往图上加边：`running` / `scheduled` / `retried`
 * 这些状态在图上什么都不产生，折进来只是让这一块在没人交接的时候也在动——那正好
 * 是「这一面是事件流的回音」最容易被做假的地方。`hook_event` 借同一个变体过总线，
 * 它不是任务在换状态，白名单按构造把它挡在外面。
 */
export function commsFrame(event: CommsFrameInput): LiveCommsFrame | null {
  if (event.type !== 'task_status_change') return null;
  const p = event.payload ?? {};
  const taskId = str(p.task_id);
  if (!taskId) return null;
  const status = str(p.status);
  if (status !== 'completed' && status !== 'failed') return null;
  const agentId = str(p.agent_id);
  return {
    key: `${taskId}::${status}`,
    taskId,
    status,
    at: event.timestamp,
    agentId: agentId || null,
    edge: status === 'completed' ? 'handoff' : 'retry_or_dead_letter',
  };
}

/**
 * 把一帧折进已有那一行。
 *
 * 重连之后服务端会把窗口内的帧重放一遍。一串旧帧若盖在刚折出来的行上，画面就会
 * 把「半分钟前刚交出来的那个」翻回「一小时前的那一次」——一次从未发生的回退。
 * 所以更旧的帧一律不折，`lastAt` 也不许被拉回去。
 */
export function foldCommsFrame(
  existing: LiveCommsRow | undefined,
  frame: LiveCommsFrame
): LiveCommsRow {
  if (!existing) {
    return {
      key: frame.key,
      taskId: frame.taskId,
      status: frame.status,
      edge: frame.edge,
      agentId: frame.agentId,
      firstAt: frame.at,
      lastAt: frame.at,
      frames: 1,
    };
  }
  const prev = Date.parse(existing.lastAt);
  const next = Date.parse(frame.at);
  if (Number.isFinite(prev) && Number.isFinite(next) && next < prev) {
    return existing;
  }
  return {
    ...existing,
    agentId: frame.agentId ?? existing.agentId,
    lastAt: frame.at,
    frames: existing.frames + 1,
  };
}

export const LIVE_COMMS_LIMIT = 100;

/** 折叠结果按 `lastAt` 留住最近 `LIVE_COMMS_LIMIT` 个。 */
export function pruneComms(
  rows: Record<string, LiveCommsRow>,
  limit: number = LIVE_COMMS_LIMIT
): Record<string, LiveCommsRow> {
  const keys = Object.keys(rows);
  if (keys.length <= limit) return rows;
  const keep = keys
    .sort((a, b) => {
      const ta = Date.parse(rows[a].lastAt);
      const tb = Date.parse(rows[b].lastAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    })
    .slice(0, limit);
  const next: Record<string, LiveCommsRow> = {};
  for (const key of keep) next[key] = rows[key];
  return next;
}

/**
 * 上次读交接图之后才有的结局，新的在前。
 *
 * 图上此刻还没有它们的边，所以这一块说的和表说的不是同一件事：表是「已经画出来
 * 的边」，这一块是「马上要画的」。一次重读之后快照越过这些帧，它们自己就消失了，
 * 所以同一条交接不会在这两处各出现一次。
 *
 * `snapshotAt` 读不出来时全部返回：这时无法证明那份快照覆盖过这些帧，藏起来就是
 * 替它说它没说过的话。
 */
export function outcomesSinceSnapshot(
  snapshotAt: string | null,
  rows: Record<string, LiveCommsRow>
): LiveCommsRow[] {
  const snap = snapshotAt ? Date.parse(snapshotAt) : NaN;
  return Object.values(rows)
    .filter((row) => {
      if (!Number.isFinite(snap)) return true;
      const at = Date.parse(row.lastAt);
      // 帧的时刻读不出来时按「比快照晚」算：宁可多显示一条，不可藏一条。
      return !Number.isFinite(at) || at > snap;
    })
    .sort((a, b) => {
      const ta = Date.parse(a.lastAt);
      const tb = Date.parse(b.lastAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    });
}

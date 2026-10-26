/**
 * 「这个执行者此刻怎么样」——由帧折出来的那一半。
 *
 * 在岗名单是每 15 秒重读一次 REST 快照，而总线上 `agent_start` / `state_change` /
 * `agent_end` / `agent_error` / `heartbeat` 一直在走。以前这几种帧只进了活动流与
 * 时间轴，一个人面上一件事都不改：刚起来的执行者要等下一拍轮询才出现，换的那次
 * 状态要等下一拍才显示。于是「UI 是事件流的回音」在这一面根本不成立——它是「每
 * 15 秒拍一张照片」。
 *
 * 状态与「还在不在」分开记：`state_change` 说它换到了什么状态，`heartbeat` 只说明
 * 它还在。合成一格的话，一条心跳会把刚结束的状态翻回「在跑」，那是替数据说谎。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-live-frames.sh` 就是这么验的）。
 */

/** 线上的一帧，只取折叠用得到的那几个字段。 */
export interface AgentFrameInput {
  type: string;
  timestamp: string;
  payload: Record<string, unknown>;
}

/** 一帧能折成什么：状态帧给 `state`，心跳只给「还在」的时刻。 */
export interface LiveAgentFrame {
  agentId: string;
  at: string;
  /** 这一帧说它处在什么状态。心跳不给状态——它只说明还活着。 */
  state: string | null;
  frame: string;
  /** 只有错误帧带：严重度与原文。 */
  detail: string | null;
}

/** 折叠后的一行。 */
export interface LiveAgentRow {
  agentId: string;
  /** 最后一次状态帧说的值；没有过状态帧时是空串。 */
  state: string;
  /** 写下 `state` 的那一类帧。 */
  stateFrame: string;
  /** 写下 `state` 的那一帧的时刻。 */
  stateAt: string;
  /** 任何一帧（含心跳）的最新时刻：它说明这个人还在。 */
  seenAt: string;
  detail: string | null;
}

const str = (value: unknown): string => (typeof value === 'string' ? value : '');

/**
 * 一帧 → 一条可折的记录；不是执行者相关的帧返回 `null`。
 *
 * `heartbeat` 也算：它不改状态，但它是在岗名单上「最后一次心跳」那一格的唯一实时
 * 来源。按构造高频，所以调用方仍按原样只按类型计数、不为它落时间轴点。
 */
export function agentFrame(event: AgentFrameInput): LiveAgentFrame | null {
  const p = event.payload ?? {};
  const agentId = str(p.agent_id);
  if (!agentId) return null;
  const at = event.timestamp;
  switch (event.type) {
    case 'agent_start':
      return { agentId, at, state: 'started', frame: event.type, detail: null };
    case 'agent_end':
      return { agentId, at, state: 'ended', frame: event.type, detail: null };
    case 'state_change': {
      const to = str(p.to);
      // 没有 `to` 的 payload 不能折：把空串写进状态格，画面上就是「这个人没有状态」，
      // 而真相是这一帧没带状态。宁可不折。
      if (!to) return null;
      return { agentId, at, state: to, frame: event.type, detail: null };
    }
    case 'agent_error': {
      const severity = str(p.severity);
      const what = str(p.details) || str(p.error_code);
      const detail = [severity, what].filter(Boolean).join('：') || null;
      return { agentId, at, state: 'error', frame: event.type, detail };
    }
    case 'heartbeat':
      return { agentId, at, state: null, frame: event.type, detail: null };
    default:
      return null;
  }
}

const later = (a: string, b: string): string => {
  const ta = Date.parse(a);
  const tb = Date.parse(b);
  if (!Number.isFinite(ta)) return b;
  if (!Number.isFinite(tb)) return a;
  return tb > ta ? b : a;
};

/**
 * 把一帧折进已有那一行。
 *
 * 两个时刻各管各的前进方向：`seenAt` 任何帧都能推，`state` 只由状态帧推。重连之后
 * 服务端会把窗口内的帧重放一遍，一串旧帧若盖在刚折出来的状态上，画面就会从
 * `running` 倒退回 `started`——一次从未发生的回退，比晚十几秒更坏。
 */
export function foldAgentFrame(
  existing: LiveAgentRow | undefined,
  frame: LiveAgentFrame
): LiveAgentRow {
  if (!existing) {
    return {
      agentId: frame.agentId,
      state: frame.state ?? '',
      stateFrame: frame.state === null ? '' : frame.frame,
      stateAt: frame.state === null ? '' : frame.at,
      seenAt: frame.at,
      detail: frame.state === null ? null : frame.detail,
    };
  }
  const seenAt = later(existing.seenAt, frame.at);
  if (frame.state === null) {
    return { ...existing, seenAt };
  }
  const newer =
    !existing.stateAt || later(existing.stateAt, frame.at) === frame.at;
  if (!newer) {
    // 旧状态帧只推「还在」的那一刻，不改状态格。
    return { ...existing, seenAt };
  }
  return {
    agentId: existing.agentId,
    state: frame.state,
    stateFrame: frame.frame,
    stateAt: frame.at,
    seenAt,
    detail: frame.detail,
  };
}

export const LIVE_AGENT_LIMIT = 200;

/** 折叠结果按 `seenAt` 留住最近 `LIVE_AGENT_LIMIT` 个，免得一张长跑的表无限长。 */
export function pruneAgents(
  rows: Record<string, LiveAgentRow>,
  limit: number = LIVE_AGENT_LIMIT
): Record<string, LiveAgentRow> {
  const ids = Object.keys(rows);
  if (ids.length <= limit) return rows;
  const keep = ids
    .sort((a, b) => {
      const ta = Date.parse(rows[a].seenAt);
      const tb = Date.parse(rows[b].seenAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    })
    .slice(0, limit);
  const next: Record<string, LiveAgentRow> = {};
  for (const id of keep) next[id] = rows[id];
  return next;
}

/**
 * 名单与帧合起来读。
 *
 * `unseen` 是名单里还没有、帧里已经有的那些人：轮询最多晚 15 秒，而一个刚起来的
 * 执行者在这一刻就是「能叫到的人」。把它折进名单里会让它带上一个假的角色与主机，
 * 所以单列出来，由画面自己说清「这条只有帧」。
 */
export function rosterWithFrames<T extends { agent_id: string }>(
  roster: T[],
  live: Record<string, LiveAgentRow>
): { rows: Array<{ row: T; live: LiveAgentRow | null }>; unseen: LiveAgentRow[] } {
  const seen = new Set(roster.map((row) => row.agent_id));
  const rows = roster.map((row) => ({ row, live: live[row.agent_id] ?? null }));
  const unseen = Object.values(live)
    .filter((entry) => !seen.has(entry.agentId) && entry.state !== 'ended')
    .sort((a, b) => {
      const ta = Date.parse(a.seenAt);
      const tb = Date.parse(b.seenAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    });
  return { rows, unseen };
}

/** 帧里的「还在」比这次轮询读到的最后一次心跳更晚吗——「帧」那个徽记的判据。 */
export function seenAfterSnapshot(
  snapshotAt: string,
  live: LiveAgentRow | null
): boolean {
  if (!live || !live.seenAt) return false;
  const frameAt = Date.parse(live.seenAt);
  if (!Number.isFinite(frameAt)) return false;
  const snap = Date.parse(snapshotAt);
  if (!Number.isFinite(snap)) return true;
  return frameAt > snap;
}

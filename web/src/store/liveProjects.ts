/**
 * 「总线上刚改了哪条任务的状态」——项目面里由帧折出来的那一半。
 *
 * 项目面的四个视图（范围、看板、甘特、挣值）读的都是 REST 快照，每 15 秒重读
 * 一次；而 `task_status_change` 一直在总线上走。以前这些帧只进了活动流、时间轴
 * 与任务表，项目面上却一件事都不改：一条刚结束的任务要等下一拍轮询才会从「执行
 * 中」列里挪走，挣值的完成量、甘特的实到时刻同样晚一拍。这一面回答的正是「这活
 * 现在到哪一步了」，十五秒的旧读数在这面就是错误读数。
 *
 * 这里折出来的**不是**那四份快照的一份更快的拷贝。快照里的每一格都由服务端按
 * 整张任务图现算，而帧里只有「这条任务现在是什么状态」——没有计划基线，没有预算，
 * 也没有这帧之前的状态。硬拿帧去改服务端的数，就是把两份账写成一本，而读面上
 * 分不出哪一格是哪一份算的。所以这里只回答一个问题：**上次读这一面之后，总线上
 * 又改过哪些节点**——那几条快照里不可能有。
 *
 * 拿到这份名单之后做什么由画面决定：重读这一面，让服务端按新状态把四个视图一起
 * 重算。帧是**触发**，不是真值。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-live-projects.sh` 就是这么验的）。
 */

/** 线上的一帧，只取折叠用得到的那几个字段。 */
export interface TaskFrameInput {
  type: string;
  timestamp: string;
  payload: Record<string, unknown>;
}

/** 一帧能折成什么：哪条任务、变成了什么状态、什么时候。 */
export interface LiveTaskFrame {
  taskId: string;
  status: string;
  at: string;
}

/** 折叠后的一行。一条任务一行，后到的帧盖住先到的。 */
export interface LiveTaskRow {
  taskId: string;
  /** 最近一帧说的状态。 */
  status: string;
  /** 这条任务第一次被总线提到改状态的时刻。 */
  firstAt: string;
  /** 最近一帧的时刻：这一行有多新，就看它。 */
  lastAt: string;
  /** 折进这一行的帧数。重放不算。 */
  frames: number;
}

const str = (value: unknown): string => (typeof value === 'string' ? value : '');

/**
 * 一帧 → 一次可折的状态变化；不是任务状态帧的返回 `null`。
 *
 * `hook_event` 借这个变体过总线（`status` 里放的是钩子的名字），它说的是钩子跑
 * 完了，不是任务换了状态。把它当状态折进去，看板上会凭空出现一次从没发生的迁移。
 */
export function taskStatusFrame(event: TaskFrameInput): LiveTaskFrame | null {
  if (event.type !== 'task_status_change') return null;
  const p = event.payload ?? {};
  const taskId = str(p.task_id);
  const status = str(p.status);
  if (!taskId || !status) return null;
  if (status === 'hook_event') return null;
  return { taskId, status, at: event.timestamp };
}

/**
 * 把一帧折进已有那一行。
 *
 * 重连之后服务端会把窗口内的帧重放一遍。一串旧帧若盖在刚折出来的状态上，画面
 * 就会把「刚跑完」翻回「还在跑」，读者据此重读一次，读回来的又是对的——一次
 * 凭空出现又凭空消失的回退。所以更旧的帧一律不折，`lastAt` 也不许被拉回去。
 */
export function foldTaskFrame(
  existing: LiveTaskRow | undefined,
  frame: LiveTaskFrame
): LiveTaskRow {
  if (!existing) {
    return {
      taskId: frame.taskId,
      status: frame.status,
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
    status: frame.status,
    lastAt: frame.at,
    frames: existing.frames + 1,
  };
}

export const LIVE_TASK_LIMIT = 500;

/** 按 `lastAt` 留住最近 `limit` 条，免得一张长跑的表无限长。 */
export function pruneTasks(
  rows: Record<string, LiveTaskRow>,
  limit: number = LIVE_TASK_LIMIT
): Record<string, LiveTaskRow> {
  const keys = Object.keys(rows);
  if (keys.length <= limit) return rows;
  const keep = keys
    .sort((a, b) => {
      const ta = Date.parse(rows[a].lastAt);
      const tb = Date.parse(rows[b].lastAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    })
    .slice(0, limit);
  const next: Record<string, LiveTaskRow> = {};
  for (const key of keep) next[key] = rows[key];
  return next;
}

/**
 * 这个项目的任务上折出来的行，新的在前。
 *
 * `owned` 是「哪条任务归哪个项目」，由范围树读回来时登记。没有它，任何一条任务
 * 换状态都要惊动每一个打开着的项目面——而那些帧多半不属于眼下这一面，重读一次
 * 只是把同一份字节再取一遍。
 */
export function rowsForProject(
  projectId: string,
  owned: Record<string, string>,
  rows: Record<string, LiveTaskRow>
): LiveTaskRow[] {
  return Object.values(rows)
    .filter((row) => owned[row.taskId] === projectId)
    .sort((a, b) => {
      const ta = Date.parse(a.lastAt);
      const tb = Date.parse(b.lastAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    });
}

/**
 * 这一面的版本号：本项目任务上最近一帧的时刻，毫秒；一帧都没有时为 0。
 *
 * 画面拿它当重读的依赖。取「最近一帧的时刻」而不是帧数，是因为重放不会动它：
 * 同一批帧被推第二遍，版本号不变，也就不会再白读一次。
 */
export function projectRevision(
  projectId: string,
  owned: Record<string, string>,
  rows: Record<string, LiveTaskRow>
): number {
  let max = 0;
  for (const row of Object.values(rows)) {
    if (owned[row.taskId] !== projectId) continue;
    const at = Date.parse(row.lastAt);
    if (Number.isFinite(at) && at > max) max = at;
  }
  return max;
}

/**
 * 比这份快照更晚的那几条——也就是快照里不可能有的那几条。
 *
 * `snapshotAt` 是这一面读成功的本地时刻。一旦重读，它越过这些帧，名单自己就空
 * 了：这一块显示的永远是「表里还没有的」，而不是「表里也有、这里再抄一遍」。
 *
 * `snapshotAt` 读不出来（还没成功读过、或者格式坏了）时全部返回：这时无法证明
 * 那份快照覆盖过这些帧，藏起来就是替它说它没说过的话。
 */
export function changedSinceSnapshot(
  snapshotAt: string | null,
  rows: LiveTaskRow[]
): LiveTaskRow[] {
  const snap = snapshotAt ? Date.parse(snapshotAt) : NaN;
  if (!Number.isFinite(snap)) return rows;
  return rows.filter((row) => {
    const at = Date.parse(row.lastAt);
    // 帧的时刻读不出来时按「比快照晚」算：宁可多显示一条，不可藏一条。
    return !Number.isFinite(at) || at > snap;
  });
}

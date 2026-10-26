/**
 * 「总线上刚刚越了什么界」——告警面里由帧折出来的那一半。
 *
 * 活跃告警那张表是每 30 秒重读一次的快照，而总线上 `resource_alert` 一直在走。
 * 以前这种帧只进了活动流与时间轴，告警面上一件事都不改：一次越界要等下一拍轮询
 * 才出现，而告警恰恰是「现在就要知道」的那种读数——晚 30 秒的告警，等于把这段
 * 时间还给了出事的那一方。
 *
 * 这里折出来的**不是**那张表的一份更快的拷贝。表里的行有 `dedup_key`、有「最后
 * 一次确认还在 firing」的时刻、有谁点过确认；帧里只有这一次的实测值。两者身份
 * 对不上，硬合并会把不同来源的账写成同一笔，所以这里只回答一个问题：**上次读这
 * 张表之后，总线上又推来了哪几次越界**——那几次表里不可能有。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-live-alerts.sh` 就是这么验的）。
 */

/** 线上的一帧，只取折叠用得到的那几个字段。 */
export interface AlertFrameInput {
  type: string;
  timestamp: string;
  payload: Record<string, unknown>;
}

/** 一帧能折成什么：一次越界的三样东西——谁、哪项指标、越到哪儿。 */
export interface LiveAlertFrame {
  /** 同一个人同一项指标算同一次越界：这是它在总线上的身份。 */
  key: string;
  agentId: string;
  metric: string;
  threshold: number;
  current: number;
  crewId: string | null;
  at: string;
}

/** 折叠后的一行。 */
export interface LiveAlertRow {
  key: string;
  agentId: string;
  metric: string;
  /** 最近一帧说的阈值与实测值。 */
  threshold: number;
  current: number;
  crewId: string | null;
  /** 这一次越界第一次被推上总线的时刻。 */
  firstAt: string;
  /** 最近一帧的时刻：这一行有多新，就看它。 */
  lastAt: string;
  /** 折进这一行的帧数。重放不算。 */
  frames: number;
}

const str = (value: unknown): string => (typeof value === 'string' ? value : '');

const num = (value: unknown): number | null =>
  typeof value === 'number' && Number.isFinite(value) ? value : null;

/**
 * 一帧 → 一条可折的越界；不是 `resource_alert` 的帧返回 `null`。
 *
 * 阈值或实测值缺一个就整帧不折：只报「某项指标出事了」而说不出越到哪儿，读者
 * 没法判断该不该现在动手，那一行只是噪声。宁可不折。
 */
export function alertFrame(event: AlertFrameInput): LiveAlertFrame | null {
  if (event.type !== 'resource_alert') return null;
  const p = event.payload ?? {};
  const agentId = str(p.agent_id);
  const metric = str(p.metric);
  if (!agentId || !metric) return null;
  const threshold = num(p.threshold);
  const current = num(p.current);
  if (threshold === null || current === null) return null;
  const crewId = str(p.crew_id);
  return {
    key: `${agentId}::${metric}`,
    agentId,
    metric,
    threshold,
    current,
    crewId: crewId || null,
    at: event.timestamp,
  };
}

/**
 * 把一帧折进已有那一行。
 *
 * 重连之后服务端会把窗口内的帧重放一遍。一串旧帧若盖在刚折出来的数值上，画面
 * 就会把「半分钟前越到 0.93」翻回「一小时前越到 0.91」——一次从未发生的变化。
 * 所以更旧的帧一律不折，`lastAt` 也不许被拉回去。
 */
export function foldAlertFrame(
  existing: LiveAlertRow | undefined,
  frame: LiveAlertFrame
): LiveAlertRow {
  if (!existing) {
    return {
      key: frame.key,
      agentId: frame.agentId,
      metric: frame.metric,
      threshold: frame.threshold,
      current: frame.current,
      crewId: frame.crewId,
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
    threshold: frame.threshold,
    current: frame.current,
    crewId: frame.crewId ?? existing.crewId,
    lastAt: frame.at,
    frames: existing.frames + 1,
  };
}

export const LIVE_ALERT_LIMIT = 100;

/** 折叠结果按 `lastAt` 留住最近 `LIVE_ALERT_LIMIT` 个，免得一张长跑的表无限长。 */
export function pruneAlerts(
  rows: Record<string, LiveAlertRow>,
  limit: number = LIVE_ALERT_LIMIT
): Record<string, LiveAlertRow> {
  const keys = Object.keys(rows);
  if (keys.length <= limit) return rows;
  const keep = keys
    .sort((a, b) => {
      const ta = Date.parse(rows[a].lastAt);
      const tb = Date.parse(rows[b].lastAt);
      return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
    })
    .slice(0, limit);
  const next: Record<string, LiveAlertRow> = {};
  for (const key of keep) next[key] = rows[key];
  return next;
}

/**
 * 上次读那张表之后又推来的越界，新的在前。
 *
 * `snapshotAt` 是那份快照读成功的时刻，`rows` 是帧折出来的。帧比它晚，就说明
 * 这一行不可能是那份快照里的——这正是这一条只在帧里出现、而不会在表里重复一次
 * 的理由：一旦重读，`snapshotAt` 越过这一帧，它自己就从这里消失。
 *
 * `snapshotAt` 读不出来（还没成功读过、或者格式坏了）时全部返回：这时无法证明
 * 那份快照覆盖过这些帧，藏起来就是替它说它没说过的话。
 */
export function breachesSinceSnapshot(
  snapshotAt: string | null,
  rows: Record<string, LiveAlertRow>
): LiveAlertRow[] {
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

/**
 * 「这一路帧还活着吗」——状态条那一格由帧折出来的读数。
 *
 * 状态条过去只写「实时流 已连接」。连接开着而一帧都不来时，那一格和平台真的没事
 * 时长得一模一样：断线看得见，**活着但不说话**看不见。等平台停摆的时候，屏幕上
 * 一切都是静止的，没有一格说「总线已经很久没推来东西了」——读者的结论会是
 * 「平台很闲」，而不是「发帧的那一端停了」。
 *
 * 所以这里数的是**到达**：每来一帧记一次钟。数的不是帧里那个 `timestamp`——重连
 * 之后服务端会把窗口内的帧重放一遍，一串半小时前的旧帧会让「刚刚在动」读成「半小时
 * 没动」，正好把要判断的事说反。到达时刻只有调用方知道，所以折叠函数收的是一个
 * 毫秒数，而不是一帧事件。
 *
 * 这个文件没有 import：它必须能单文件编译、在 node 里按判据跑
 * （`deploy/scripts/check-web-live-pulse.sh` 就是这么验的）。
 */

/** 算「最近一分钟几帧」的窗口。 */
export const PULSE_WINDOW_MS = 60_000;

/**
 * 多久没有一帧就值得说一声。
 *
 * 这个数是量出来的，不是拍的。取 24 小时的落库事件流（`cog_observability_events`，
 * 742808 帧）算相邻到达间隔：最长一次静默 700 秒，超过两分钟的有 21 次，超过五分钟
 * 的有 6 次——也就是说按两分钟判，这套平台每天会亮二十来次「流断了」，把真断的那
 * 一次淹掉。取十五分钟：比实测最长的那次正常静默还高出三分多钟。
 *
 * 这里没有拿心跳当界：落库面 24 小时里一条 heartbeat 都没有（心跳帧不入账），
 * 拿不到的周期不能当判据。而这个平台「没事可干」并不等于「总线上没有帧」——上游
 * 熔断、任务全部押住的那半小时里，agent 照样在起来、在发增量（近 30 分钟 426 次
 * `agent_start`）。所以静默到十五分钟确实是异常，不是「平台很闲」。
 */
export const PULSE_STALE_MS = 900_000;

/** 窗口内最多留这么多个到达时刻；窗口本身会把更早的裁掉，这只是个上界。 */
export const PULSE_SAMPLES = 600;

export interface Pulse {
  /** 第一条帧到达的时刻（毫秒）；一帧都没来过时是 null。 */
  firstAtMs: number | null;
  /** 最近一帧到达的时刻。 */
  lastAtMs: number | null;
  /** 连上以后一共到了多少帧，含 `message_update`、`heartbeat` 这类不入账的高频帧。 */
  total: number;
  /** 窗口内的到达时刻，用来数「最近一分钟几帧」。 */
  recent: number[];
}

export const EMPTY_PULSE: Pulse = {
  firstAtMs: null,
  lastAtMs: null,
  total: 0,
  recent: [],
};

/**
 * 一帧到了。
 *
 * `atMs` 是**到达**的时刻（调用方传 `Date.now()`），不是帧里的 `timestamp`。
 * 时钟被往回拨也不会让这一格倒着走：`lastAtMs` 只增不减。
 */
export function foldArrival(pulse: Pulse, atMs: number): Pulse {
  if (!Number.isFinite(atMs)) return pulse;
  const recent = pulse.recent.filter((t) => atMs - t < PULSE_WINDOW_MS);
  recent.push(atMs);
  if (recent.length > PULSE_SAMPLES) recent.splice(0, recent.length - PULSE_SAMPLES);
  const last = pulse.lastAtMs;
  return {
    firstAtMs: pulse.firstAtMs ?? atMs,
    lastAtMs: last === null || atMs > last ? atMs : last,
    total: pulse.total + 1,
    recent,
  };
}

/** 最近一分钟到了几帧。窗口里有多少就是多少，不外推。 */
export function framesPerMinute(pulse: Pulse, nowMs: number): number {
  if (!Number.isFinite(nowMs)) return 0;
  return pulse.recent.filter((t) => nowMs - t < PULSE_WINDOW_MS).length;
}

export type PulseTone = 'ok' | 'warn' | 'mute';

/** 判词的颜色由这里定，画面不再自己映射：两处各判一遍，同一件事会有两种颜色。 */
const TONE_COLOR: Record<PulseTone, string> = {
  ok: 'var(--ok)',
  warn: 'var(--warn)',
  mute: 'var(--muted)',
};

/** 状态条那一格要说的三样东西：颜色、正文、悬停解释。 */
export interface PulseVerdict {
  tone: PulseTone;
  /** 那一格直接用的颜色。 */
  color: string;
  text: string;
  title: string;
}

const STATUS_TEXT: Record<string, string> = {
  connecting: '连接中',
  open: '已连接',
  closed: '已断开',
  error: '连不上',
};

/** 一段时长的说法。一秒以内说「刚刚」，一分钟以内按秒，再往上按分。 */
export function humanAge(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '—';
  if (ms < 1_000) return '刚刚';
  if (ms < PULSE_WINDOW_MS) return `${Math.floor(ms / 1_000)} 秒前`;
  return `${Math.floor(ms / PULSE_WINDOW_MS)} 分前`;
}

/**
 * 这一格该说什么。
 *
 * 没有帧时的两种「静」不是一回事：平台上有活没干完却一帧都没有，那是发帧的那一端
 * 停了；平台上本来就没有活，那静得对。只看帧分不开这两件——断流与空闲在画面上完全
 * 一样——所以判词还要一个来源：`inflight` 是还没做完的任务数，由状态条从它自己的
 * 那一份读数里传进来（`null`＝那一份没读到，这时不替它猜，按「可能停了」说）。
 *
 * 五种情形分开说，因为读者要做的事各不一样：没连上（去查连接）、连上了但一帧都
 * 没到（有活就先去查订阅与发帧的那一端）、连上了但已经很久没有帧（有活＝断流，
 * 没活＝空闲）、有帧（这一格是活的，读数可以信它「此刻」）。
 */
export function pulseVerdict(
  pulse: Pulse,
  nowMs: number,
  connected: string,
  inflight: number | null = null
): PulseVerdict {
  if (connected !== 'open') {
    return {
      tone: 'warn',
      color: TONE_COLOR.warn,
      text: STATUS_TEXT[connected] ?? connected,
      title:
        '实时事件流没连着。断开时画布与日志格都不会更新，各面的读数仍在按自己的间隔轮询——' +
        '屏幕上静止不等于平台静止，反过来也一样。',
    };
  }
  if (pulse.total === 0 || pulse.lastAtMs === null) {
    if (inflight !== null && inflight > 0) {
      // 一帧都没到，而平台上有活：这不可能是「平台没事」，只能是没送到。
      return {
        tone: 'warn',
        color: TONE_COLOR.warn,
        text: `已连接 · ${inflight} 条活没干完，一帧都没到`,
        title:
          `连接开着，平台上也还有 ${inflight} 条活没干完，可总线一帧都没推来。` +
          '有活而全静，先查订阅的那几族与发帧的那一端——它不该是静的。',
      };
    }
    return {
      tone: 'mute',
      color: TONE_COLOR.mute,
      text: '已连接 · 还没有一帧',
      title:
        '连接开着，但总线一帧都没推来。可能是平台此刻真的没事，也可能是订阅的那几族没送到——' +
        (inflight === 0
          ? '平台上此刻一条没做完的活都没有，所以这更像真的没事。'
          : '两种在画面上完全一样，所以这一格只报「一帧都没到」，不替它猜是哪一种。'),
    };
  }
  const idle = nowMs - pulse.lastAtMs;
  if (idle >= PULSE_STALE_MS) {
    const age = humanAge(idle);
    if (inflight === 0) {
      // 没有活，也没有帧：两件「静」是同一件，不是故障。这一格不该报警色——
      // 报久了读者就不再信它，真断的那一次会被淹掉。
      return {
        tone: 'mute',
        color: TONE_COLOR.mute,
        text: `已连接 · 平台空闲 · 最近一帧在 ${age}`,
        title:
          `路连着，最近一帧在 ${age}（连上以后一共 ${pulse.total} 帧）。` +
          '平台此刻没有没做完的活，所以这一段静得对。要判的是反过来的那种：有活没干完却没有帧。',
      };
    }
    if (inflight !== null) {
      return {
        tone: 'warn',
        color: TONE_COLOR.warn,
        text: `已连接 · ${inflight} 条活没干完 · 最近一帧在 ${age}`,
        title:
          `平台上有 ${inflight} 条活没干完，总线却已经 ${age}没有一帧（连上以后一共 ${pulse.total} 帧）。` +
          '有活而没有帧是断流的判据，不是平台空闲。各面的读数仍按自己的间隔轮询，屏幕不会自己变。',
      };
    }
    return {
      tone: 'warn',
      color: TONE_COLOR.warn,
      text: `已连接 · 最近一帧在 ${age}`,
      title:
        `路还连着，但已经 ${age}没有一帧（连上以后一共 ${pulse.total} 帧）。` +
        '平台可能停了，也可能发帧的那一端停了——没读到在飞的活，所以这一格不替它分是哪一种。' +
        '各面的读数仍按自己的间隔轮询，屏幕不会自己变。',
    };
  }
  const perMinute = framesPerMinute(pulse, nowMs);
  const first = pulse.firstAtMs;
  // 窗口还没走满时，「帧/分」会把十秒里来的帧说成一分钟的量；这时只报实数。
  if (first !== null && nowMs - first < PULSE_WINDOW_MS) {
    return {
      tone: 'ok',
      color: TONE_COLOR.ok,
      text: `已连接 · 刚 ${Math.max(1, Math.floor((nowMs - first) / 1_000))} 秒来了 ${pulse.total} 帧`,
      title:
        '帧数是按**到达**时刻算的，与帧里的 timestamp 无关：重连重放一串旧帧，这一格仍旧是「刚刚在动」。',
    };
  }
  return {
    tone: 'ok',
    color: TONE_COLOR.ok,
    text: `已连接 · ${perMinute} 帧/分 · 最近 ${humanAge(idle)}`,
    title:
      `最近一帧在 ${humanAge(idle)}。帧数是按**到达**时刻算的，与帧里的 timestamp 无关：` +
      '重连重放一串旧帧，这一格仍旧是「刚刚在动」。心跳与正文增量也算在内，它们不入活动流但确实是帧。',
  };
}

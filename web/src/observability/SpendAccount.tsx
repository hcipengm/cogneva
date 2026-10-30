import type { SkipLine, TokenAccount, VerdictLine } from '@/api/client';

/**
 * 平台自己那本账：它决定不花的那一侧。
 *
 * 结局那一块说的是「哪些调用被挡回来了」。还有第三种零花销它答不了：**根本没
 * 发起过**。一个因为上游全挂而拒绝规划的 planner 一次调用都不发，于是在结局面
 * 是空、在花销面也是空，两个空凑在一起，看起来就和「今天没人用模型」一样。
 *
 * 这里读的两本是唯一知道区别的账：
 * - 省下账：哪个阶段决定不调、记下了什么理由，多少次；
 * - 判定账：每个调用点的每个小时被判成了什么。`unexpected_absent` 是唯一一句
 *   「它本来在花、现在无声地停了」——那句话以前写进表里之后就没人读过。
 *
 * 两本账都一直在写。这一块存在的意义就是把它们读回来；一栏空着只说明这本账这段
 * 没说什么，不说明另一栏也空，所以两栏各说各的，不合并成一句「没有」。
 */
export function SpendAccount({ data }: { data: TokenAccount }) {
  const skips = [...data.skips].sort((a, b) => b.skips - a.skips);
  const verdicts = [...data.verdicts].sort(
    (a, b) => rank(a.verdict) - rank(b.verdict) || b.windows - a.windows,
  );
  const unexplained = verdicts.filter((v) => v.verdict === 'unexpected_absent');
  const totalSkips = skips.reduce((sum, line) => sum + line.skips, 0);

  return (
    <div className="flex flex-col gap-2 px-4 py-3" style={{ userSelect: 'text' }}>
      <div className="flex items-baseline gap-3 text-[11px]">
        <span className="text-[var(--paper-dim)]">
          这次读的区间是 {stamp(data.since)} – {stamp(data.until)}
        </span>
        <span className="mono text-[var(--muted)]">
          省下的调用 {totalSkips.toLocaleString()} 次，判定过的窗口 {verdicts.reduce((s, v) => s + v.windows, 0).toLocaleString()} 个
        </span>
      </div>

      {unexplained.length > 0 && (
        <p className="text-[11px] text-[var(--danger)]">
          有 {unexplained.length} 个调用点本来在花、这段却无声地停了，账上也没有省下的记录：
          {unexplained.map((v) => v.actor).join('、')}。这是唯一一句「不是决定、是坏了」的判定。
        </p>
      )}

      <div className="flex flex-col gap-1">
        <span className="text-[11px] text-[var(--paper-dim)]">决定不花（省下账）</span>
        {skips.length === 0 ? (
          <p className="text-[11px] text-[var(--muted)]">
            这段区间里没有任何阶段记下「我不调」。这不等于「没有人省」——一本没人写的省下账
            也是这个空，区别只在写者是否还活着。
          </p>
        ) : (
          skips.map((line) => (
            <div key={`${line.stage}|${line.reason}`} className="flex items-baseline gap-3 text-[11px]">
              <span className="mono w-40 shrink-0 text-[var(--paper-dim)]">{line.stage}</span>
              <span className="min-w-0 flex-1 truncate text-[var(--muted)]">{skipReason(line)}</span>
              <span className="mono w-24 shrink-0 text-right text-[var(--paper-dim)]">
                {line.skips.toLocaleString()} 次
              </span>
            </div>
          ))
        )}
      </div>

      <div className="flex flex-col gap-1">
        <span className="text-[11px] text-[var(--paper-dim)]">每个调用点的窗口判定（判定账）</span>
        {verdicts.length === 0 ? (
          <p className="text-[11px] text-[var(--muted)]">
            这段区间里没有判定过的窗口。判定循环每十分钟判一次已经关上的小时，一个窗口也没判过，
            说明它没在跑，而不是「每个窗口都花了钱」。
          </p>
        ) : (
          verdicts.map((line) => {
            const read = verdictReading(line);
            return (
              <div
                key={`${line.actor}|${line.verdict}|${line.skip_reason}`}
                className="flex items-baseline gap-3 text-[11px]"
              >
                <span className="mono w-40 shrink-0 text-[var(--paper-dim)]">{line.actor}</span>
                <span className="mono w-28 shrink-0" style={{ color: read.tone }}>
                  {read.label}
                </span>
                <span className="min-w-0 flex-1 truncate text-[var(--muted)]">{read.hint}</span>
                <span className="mono w-20 shrink-0 text-right text-[var(--muted)]">
                  {line.windows.toLocaleString()} 窗
                </span>
                <span className="mono w-24 shrink-0 text-right text-[var(--paper-dim)]">
                  {(line.tokens_input + line.tokens_output).toLocaleString()} token
                </span>
              </div>
            );
          })
        )}
      </div>

      <p className="text-[11px] text-[var(--muted)]">
        花销为零有三种成因，三块读数各答一种：结局说「试过但被挡回来」，省下账说「决定不调」，
        判定账说「本来该在花、现在停了」。少任何一块，剩下的两种就会读成同一件事。
      </p>
    </div>
  );
}

/** 判定值怎么读给人听。闭集之外的词不猜，原样显示并说明界面没见过它。 */
function verdictReading(line: VerdictLine): { label: string; hint: string; tone: string } {
  const saved = line.skip_reason ? `（省下的理由是「${line.skip_reason}」）` : '';
  switch (line.verdict) {
    case 'necessary':
      return { label: '花了', hint: '这一窗它真的花了钱，别的面也看得出来', tone: 'var(--ok)' };
    case 'documented_skip':
      return { label: '有记录地省下', hint: `没花，但省得有账${saved}`, tone: 'var(--brass)' };
    case 'unexpected_absent':
      return {
        label: '无声停下',
        hint: '它本来在花、这一窗停了，而账上没有任何省下的记录——这是要修的那一种',
        tone: 'var(--danger)',
      };
    case 'unsettled':
      return {
        label: '判不了',
        hint: '省下账这一窗读不出来，所以既不判它停了、也不判它省了',
        tone: 'var(--warn)',
      };
    default:
      return {
        label: line.verdict,
        hint: '判定表里出现了界面不认识的词，原样显示',
        tone: 'var(--muted)',
      };
  }
}

function skipReason(line: SkipLine): string {
  switch (line.reason) {
    case 'upstream_unavailable':
      return '上游池里没有健康成员，发出去也只会失败';
    case 'self_evolution':
      return '自进化路径判过这一段，不值得再花一次模型';
    default:
      return `调用点记下的理由是「${line.reason}」`;
  }
}

/** 需要修的排最前，然后是判不了的：这一栏越往下越不需要人看。 */
function rank(verdict: string): number {
  switch (verdict) {
    case 'unexpected_absent':
      return 0;
    case 'unsettled':
      return 1;
    case 'documented_skip':
      return 2;
    default:
      return 3;
  }
}

function stamp(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

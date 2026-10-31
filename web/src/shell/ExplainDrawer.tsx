import { useEffect } from 'react';
import { closeExplain, useExplainSubject } from '@/ui/explain';
import { Icon } from '@/shell/icons';
import { Note } from '@/ui/Note';

function moment(iso: string | null): string {
  if (!iso) return '没有记下来';
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return iso;
  const now = Date.now();
  const seconds = Math.max(0, Math.round((now - at.getTime()) / 1000));
  const ago =
    seconds < 60
      ? `${seconds} 秒前`
      : seconds < 3600
        ? `${Math.round(seconds / 60)} 分钟前`
        : `${Math.round(seconds / 3600)} 小时前`;
  return `${at.toLocaleTimeString('zh-CN', { hour12: false })}（${ago}）`;
}

/**
 * 一个数字的来路：读的是什么、什么时候算的、由哪几帧折出来。
 *
 * 它答的是「我该不该信这个数」，不是「这个数是多少」——后者屏幕上已经写着。
 * 所以三件事按顺序摆：来路、时刻、帧；缺哪一件就写缺哪一件，不拿前一件顶替。
 */
export function ExplainDrawer() {
  const subject = useExplainSubject();

  useEffect(() => {
    if (!subject) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeExplain();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [subject]);

  if (!subject) return null;

  return (
    <aside
      className="absolute right-0 top-0 z-40 flex h-full w-[340px] flex-col border-l border-[var(--line-strong)] bg-[var(--ink-900)] shadow-xl"
      aria-label="这个数的来路"
    >
      <header className="flex h-9 shrink-0 items-center gap-2 border-b border-[var(--line)] px-3">
        <span className="text-[11px] uppercase tracking-[0.12em] text-[var(--muted)]">来路</span>
        <span className="min-w-0 flex-1 truncate text-[12px] text-[var(--paper)]">{subject.label}</span>
        <Note
          as="button"
          type="button"
          onClick={closeExplain}
          text="关掉（Esc）"
          className="text-[var(--muted)] hover:text-[var(--paper)]"
        >
          <Icon name="close" size={12} />
        </Note>
      </header>
      <div className="min-h-0 flex-1 overflow-auto px-3 py-2 text-[11px] leading-relaxed">
        <div className="mono text-[20px] text-[var(--paper)]">{subject.value}</div>

        <div className="mt-3 text-[var(--muted)]">来源</div>
        <div className="text-[var(--paper-dim)]">{subject.source}</div>

        <div className="mt-3 text-[var(--muted)]">什么时候算的</div>
        <div className="mono text-[var(--paper-dim)]">{moment(subject.computedAt)}</div>

        <div className="mt-3 text-[var(--muted)]">由哪几帧算出来</div>
        {subject.frames === undefined ? (
          <div className="text-[var(--paper-dim)]">
            这个数不是帧折出来的：它是服务端在那一时刻给的聚合，没有逐帧的中间账。
          </div>
        ) : subject.frames.length === 0 ? (
          <div className="text-[var(--paper-dim)]">
            标着「由帧算」却是空的——这要么是刚连上还没收到帧，要么是这一格的折算没写全。
          </div>
        ) : (
          <ul className="mono mt-1 space-y-0.5">
            {subject.frames.map((frame, index) => (
              <li key={`${frame.at}-${index}`} className="text-[var(--paper-dim)]">
                <span className="text-[var(--muted)]">
                  {new Date(frame.at).toLocaleTimeString('zh-CN', { hour12: false })}
                </span>{' '}
                <span className="text-[var(--live)]">{frame.type}</span> {frame.summary}
              </li>
            ))}
          </ul>
        )}

        {subject.caveat && (
          <>
            <div className="mt-3 text-[var(--muted)]">这一格的边界</div>
            <div className="text-[var(--paper-dim)]">{subject.caveat}</div>
          </>
        )}
      </div>
      <p className="border-t border-[var(--line)] px-3 py-2 text-[11px] text-[var(--muted)]">
        抽屉只说它知道的：没有来源就写没有来源，没有时刻就写没有时刻。屏幕上每一个带
        虚线的数都能这样点开，没有虚线的那些格子没有可说的来路，也就不做成按钮。
      </p>
    </aside>
  );
}

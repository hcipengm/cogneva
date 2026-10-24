import type { Theme, TokenKey } from '@/styles/themes';

/** 一张卡片上露出的五格颜色：底色、表面、强调、实时、文字。 */
const SWATCH: TokenKey[] = ['ink-900', 'ink-700', 'brass', 'live', 'paper'];

/**
 * 配色主题的选择器。
 *
 * 每张卡片按这个主题**自己的令牌**上色，所以选之前就看得见成品，不用先切过去
 * 再判断。卡片同时印一句性格说明——只给名字的话，十个名字读起来是十种差不多的
 * 感觉，选的人只能靠试。
 */
export function ThemePicker({
  themes,
  active,
  onPick,
}: {
  themes: Theme[];
  active: string;
  onPick: (id: string) => void;
}) {
  return (
    <div className="grid grid-cols-[repeat(auto-fill,minmax(196px,1fr))] gap-2 px-4 py-3">
      {themes.map((theme) => {
        const selected = theme.id === active;
        return (
          <button
            key={theme.id}
            type="button"
            onClick={() => onPick(theme.id)}
            aria-pressed={selected}
            title={theme.note}
            className="group flex flex-col gap-1.5 border p-2 text-left transition-colors"
            style={{
              borderColor: selected ? 'var(--brass)' : 'var(--line)',
              background: selected ? 'var(--ink-700)' : 'var(--ink-800)',
            }}
          >
            <span className="flex h-8 w-full overflow-hidden" aria-hidden="true">
              {SWATCH.map((key) => (
                <span key={key} className="h-full flex-1" style={{ background: theme.tokens[key] }} />
              ))}
            </span>
            <span className="flex items-baseline gap-2">
              <span className="text-[12px]" style={{ color: selected ? 'var(--brass)' : 'var(--paper-dim)' }}>
                {theme.name}
              </span>
              {theme.light && (
                <span className="text-[10px] text-[var(--muted)]">亮色</span>
              )}
              <span className="ml-auto text-[10px] text-[var(--muted)] opacity-0 transition-opacity group-hover:opacity-100">
                {selected ? '当前' : '切换'}
              </span>
            </span>
            <span className="text-[10px] leading-snug text-[var(--muted)]">{theme.note}</span>
          </button>
        );
      })}
    </div>
  );
}

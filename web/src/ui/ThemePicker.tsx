import type { CSSProperties } from 'react';
import {
  FAMILY_LABEL,
  FAMILY_ORDER,
  type Theme,
  type ThemeFamily,
  type ThemeTokens,
  themeFamily,
} from '@/styles/themes';

import { Note } from '@/ui/Note';
/**
 * 配色主题的选择器。
 *
 * 每张卡片不是一排色块，而是一张**用这个主题自己的令牌画出来的迷你工作台**：
 * 侧栏、编辑区、强调按钮、状态条各就各位。色块只能说明「有这几种颜色」，
 * 迷你工作台说明的是「它落在这张界面上是什么样」——选之前就看得见成品。
 *
 * 卡片分组摆：十七张平铺起来，后两行没人看得到。
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
  const grouped = FAMILY_ORDER.map((family) => ({
    family,
    items: themes.filter((theme) => themeFamily(theme) === family),
  })).filter((group) => group.items.length > 0);

  return (
    <div className="flex flex-col gap-3 px-4 py-3">
      {grouped.map((group) => (
        <div key={group.family} className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <span className="text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
              {FAMILY_LABEL[group.family as ThemeFamily]}
            </span>
            <span className="text-[10px] text-[var(--muted)]">{group.items.length}</span>
            <span className="h-px flex-1 bg-[var(--line)]" />
          </div>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(184px,1fr))] gap-2">
            {group.items.map((theme) => (
              <ThemeCard
                key={theme.id}
                theme={theme}
                selected={theme.id === active}
                onPick={onPick}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ThemeCard({
  theme,
  selected,
  onPick,
}: {
  theme: Theme;
  selected: boolean;
  onPick: (id: string) => void;
}) {
  return (
    <Note
      as="button"
      type="button"
      onClick={() => onPick(theme.id)}
      aria-pressed={selected}
      text={theme.note}
      className="group flex flex-col gap-1.5 border p-2 text-left transition-colors"
      style={{
        borderColor: selected ? 'var(--brass)' : 'var(--line)',
        background: selected ? 'var(--ink-700)' : 'var(--ink-800)',
      }}
    >
      <Preview tokens={theme.tokens} />
      <span className="flex items-baseline gap-2">
        <span className="text-[12px]" style={{ color: selected ? 'var(--brass)' : 'var(--paper-dim)' }}>
          {theme.name}
        </span>
        {theme.light && <span className="text-[10px] text-[var(--muted)]">亮色</span>}
        <span className="ml-auto text-[10px] text-[var(--muted)] opacity-0 transition-opacity group-hover:opacity-100">
          {selected ? '当前' : '切换'}
        </span>
      </span>
      <span className="mono text-[10px] leading-snug text-[var(--paper-dim)]">{theme.origin}</span>
      <span className="text-[10px] leading-snug text-[var(--muted)]">{theme.note}</span>
    </Note>
  );
}

/**
 * 迷你工作台：标题条 / 侧栏 / 编辑区 / 状态条。
 *
 * 所有颜色都读传进来的那组令牌，不读文档根上的变量——否则十七张卡片会全部
 * 长成当前主题的样子。
 */
function Preview({ tokens }: { tokens: ThemeTokens }) {
  const surface = (key: keyof ThemeTokens): CSSProperties => ({ background: tokens[key] });
  return (
    <span className="flex h-[62px] w-full flex-col overflow-hidden" aria-hidden="true" style={surface('ink-1000')}>
      {/* 标题条 */}
      <span className="flex h-[9px] w-full items-center gap-[3px] px-1.5" style={surface('ink-1000')}>
        <span className="h-[3px] w-[3px] rounded-full" style={surface('danger')} />
        <span className="h-[3px] w-[3px] rounded-full" style={surface('warn')} />
        <span className="h-[3px] w-[3px] rounded-full" style={surface('ok')} />
      </span>
      <span className="flex min-h-0 flex-1">
        {/* 侧栏：三条目录，第一条是选中项 */}
        <span className="flex w-[30px] flex-col gap-[3px] border-r py-1 pl-1" style={{ ...surface('ink-800'), borderColor: tokens.line }}>
          <span className="h-[3px] w-[16px]" style={surface('brass')} />
          <span className="h-[3px] w-[19px] opacity-70" style={surface('paper-dim')} />
          <span className="h-[3px] w-[14px] opacity-45" style={surface('paper-dim')} />
          <span className="h-[3px] w-[17px] opacity-45" style={surface('paper-dim')} />
          <span className="mt-auto h-[3px] w-[9px] rounded-sm" style={surface('live')} />
        </span>
        {/* 编辑区：正文 + 一行强调按钮 */}
        <span className="flex min-w-0 flex-1 flex-col gap-[3px] px-1.5 py-1" style={surface('ink-900')}>
          <span className="h-[3px] w-[62%]" style={surface('paper')} />
          <span className="h-[3px] w-[78%] opacity-70" style={surface('paper-dim')} />
          <span className="h-[3px] w-[52%] opacity-55" style={surface('paper-dim')} />
          <span className="h-[3px] w-[70%] opacity-40" style={surface('muted')} />
          <span className="mt-auto flex items-center gap-[3px]">
            <span className="h-[9px] w-[26px] rounded-[2px]" style={surface('brass')} />
            <span
              className="h-[9px] w-[18px] rounded-[2px] border"
              style={{ borderColor: tokens['line-strong'] }}
            />
          </span>
        </span>
      </span>
      {/* 状态条 */}
      <span className="flex h-[9px] items-center gap-[4px] px-1.5" style={surface('ink-1000')}>
        <span className="h-[2px] w-[8px]" style={surface('brass')} />
        <span className="ml-auto h-[2px] w-[6px]" style={surface('ok')} />
        <span className="h-[2px] w-[6px]" style={surface('warn')} />
      </span>
    </span>
  );
}

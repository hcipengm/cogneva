import type { ReactNode } from 'react';

/**
 * 表单里共用的几件控件。
 *
 * 这些页面在加进来之前，每一面都是只读的列表：没有输入，也就没有第二条样式。
 * 有了可写的面之后，输入框、下拉、多行文本如果各写各的 `className`，同一种控件
 * 在五个页面里会长成四种样子，而「这是什么控件」在那种差别里就看不出来了。
 *
 * 控件只负责形状，不负责校验：每一次写要满足什么，是发请求的那一面知道的
 * （比如「block 只在 on_tool_before 上有意义」），把校验塞进控件会让它猜业务。
 */

const FIELD =
  'border border-[var(--line)] bg-[var(--ink-900)] px-2 py-1 text-[11px] outline-none focus:border-[var(--brass)]';

/** 一行：标签、控件、说明。说明常驻，不用悬停才出现——它写的是「这一格填错会怎样」。 */
export function FormRow({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: ReactNode;
  children: ReactNode;
}) {
  return (
    <label className="grid grid-cols-[96px_1fr] items-start gap-2 py-1">
      <span className="pt-1 text-[11px] text-[var(--paper-dim)]">{label}</span>
      <span className="flex flex-col gap-1">
        {children}
        {hint && <span className="text-[10px] leading-relaxed text-[var(--muted)]">{hint}</span>}
      </span>
    </label>
  );
}

export function TextInput({
  value,
  onChange,
  placeholder,
  mono = true,
  width,
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  mono?: boolean;
  width?: string;
}) {
  return (
    <input
      className={`${FIELD} ${mono ? 'mono' : ''}`}
      style={{ color: 'var(--paper)', width }}
      value={value}
      placeholder={placeholder}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

export function NumberInput({
  value,
  onChange,
  placeholder,
  min,
  width = '96px',
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  min?: number;
  width?: string;
}) {
  return (
    <input
      type="number"
      className={`${FIELD} mono`}
      style={{ color: 'var(--paper)', width }}
      value={value}
      placeholder={placeholder}
      min={min}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

export function TextArea({
  value,
  onChange,
  placeholder,
  rows = 4,
  mono = true,
  width = '100%',
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  rows?: number;
  mono?: boolean;
  width?: string;
}) {
  return (
    <textarea
      className={`${FIELD} ${mono ? 'mono' : ''} resize-y leading-relaxed`}
      style={{ color: 'var(--paper)', width }}
      value={value}
      rows={rows}
      placeholder={placeholder}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

export function Select<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T;
  onChange: (value: T) => void;
  options: readonly { value: T; label: string }[];
}) {
  return (
    <select
      className={FIELD}
      style={{ color: 'var(--paper)', width: 'fit-content' }}
      value={value}
      onChange={(event) => onChange(event.target.value as T)}
    >
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  );
}

/** 勾选一行：用状态开关表达「这一项整条要不要带上」，比让人把字段留空更明确。 */
export function Check({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
}) {
  return (
    <label className="inline-flex items-center gap-1.5 text-[11px] text-[var(--paper-dim)]">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        style={{ accentColor: 'var(--brass)' }}
      />
      {label}
    </label>
  );
}

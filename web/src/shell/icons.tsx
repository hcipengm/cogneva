import type { SVGProps } from 'react';

/**
 * 一套 16px 的线描图标。
 *
 * 自己画而不是引一个图标包：这一面只用得到十来个形状，而一个图标包会把几
 * 千个用不到的路径打进包里；线描也比实心图标更贴细线分区的排版。
 */
const PATHS: Record<string, string[]> = {
  workspace: ['M4 5.5h16v9.5H9.5L5 18.5V15H4z'],
  inbox: ['M3.5 12.5h4l1.5 2.5h6l1.5-2.5h4', 'M3.5 12.5 6 4.5h12l2.5 8v5.5h-17z'],
  agents: [
    'M6.5 6.5h11v11h-11z',
    'M9.5 9.5h5v5h-5z',
    'M9 3v3.5M15 3v3.5M9 17.5V21M15 17.5V21M3 9h3.5M3 15h3.5M17.5 9H21M17.5 15H21',
  ],
  tasks: ['M11 7h9M11 12h9M11 17h9', 'M4 7l1.6 1.6L8.7 5.5', 'M4 17l1.6 1.6L8.7 15.5'],
  alerts: ['M12 4a5 5 0 0 0-5 5v4l-1.5 3h13L17 13V9a5 5 0 0 0-5-5z', 'M10 19a2 2 0 0 0 4 0'],
  changes: [
    'M6.5 8v8',
    'M15.5 11v2.5a3.5 3.5 0 0 1-3.5 3.5H6.5',
    'M8.5 6a2 2 0 1 1-4 0 2 2 0 0 1 4 0z',
    'M8.5 18a2 2 0 1 1-4 0 2 2 0 0 1 4 0z',
    'M19.5 9a2 2 0 1 1-4 0 2 2 0 0 1 4 0z',
  ],
  governance: ['M12 3.5 19 6v6c0 4-3 6.5-7 8.5-4-2-7-4.5-7-8.5V6z', 'M9 12l2 2 4-4'],
  observability: ['M3 12h4l2.5-6 4 12 2.5-6h5'],
  audit: ['M6 3.5h9l3.5 3.5v13H6z', 'M15 3.5V7h3.5', 'M9 12h6M9 15.5h6M9 8.5h2.5'],
  // 一个分叉：两条线汇到一点，再各自出去——这正是「改动从哪儿来」。
  resources: ['M4 5.5h16v5H4z', 'M4 13h16v5H4z', 'M7.5 8h.5', 'M7.5 15.5h.5'],
  delivery: ['M12 3.5v9', 'M8 8.5l4 4 4-4', 'M4.5 17.5h15'],
  scm: ['M6 4.5a2 2 0 1 0 0 4 2 2 0 0 0 0-4z', 'M18 4.5a2 2 0 1 0 0 4 2 2 0 0 0 0-4z', 'M6 8.5v4a4 4 0 0 0 4 4h4', 'M18 8.5v1a4 4 0 0 1-4 4h-4', 'M10 14.5a2 2 0 1 0 0 4 2 2 0 0 0 0-4z'],
  settings: ['M4 7h9M18 7h2M4 12h3M12 12h8M4 17h11M20 17h0', 'M15.5 5.2v3.6M9.5 10.2v3.6M17.5 15.2v3.6'],
  search: ['M11 17a6 6 0 1 0 0-12 6 6 0 0 0 0 12z', 'M15.5 15.5 20 20'],
  close: ['M6 6l12 12M18 6 6 18'],
  panel: ['M3.5 4.5h17v15h-17z', 'M3.5 14.5h17'],
  sidebar: ['M3.5 4.5h17v15h-17z', 'M9.5 4.5v15'],
  plus: ['M12 5v14M5 12h14'],
  refresh: ['M20 12a8 8 0 1 1-2.3-5.6', 'M20 4v4h-4'],
  chevronDown: ['m6 9 6 6 6-6'],
  chevronRight: ['m9 6 6 6-6 6'],
  signout: ['M14 4.5H6.5v15H14', 'M11 12h9M17 8.5l3.5 3.5L17 15.5'],
  dot: ['M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8z'],
  help: [
    'M12 20.5a8.5 8.5 0 1 0 0-17 8.5 8.5 0 0 0 0 17z',
    'M9.6 9.3a2.5 2.5 0 1 1 3.3 2.4c-.6.2-.9.8-.9 1.4v.6',
    'M12 17.1h.01',
  ],
  folder: ['M3.5 6.5h6l2 2.5h9v9.5h-17z'],
  file: ['M6.5 3.5h7l4 4v13h-11z', 'M13.5 3.5v4h4'],
  save: ['M5 5.5h11l3 3v10H5z', 'M8 5.5v4h7v-4', 'M8 13.5h8v5H8z'],
};

export type IconName = keyof typeof PATHS;

export function Icon({
  name,
  size = 16,
  ...rest
}: { name: IconName; size?: number } & SVGProps<SVGSVGElement>) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...rest}
    >
      {PATHS[name].map((d) => (
        <path key={d} d={d} />
      ))}
    </svg>
  );
}

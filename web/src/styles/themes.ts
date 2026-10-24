/*
 * 配色主题：同一张工作台，换的是它认的那组颜色令牌。
 *
 * 令牌是唯一的接口——各面里没有一处直接写颜色，全部读 `--x`。所以一个主题
 * 只要把这一组令牌填满，整个界面（包括画布上的自绘部分）就一起换了；反过来，
 * 少填一个令牌不会报错，它会悄悄继承上一个主题的颜色，在一屏里留下两种风格。
 * 因此每个主题都必须填满 `TOKEN_KEYS`（`deploy/scripts/check-web-theme-picker.sh`
 * 会因为它缺键而红）。
 *
 * 亮色主题不是把颜色调淡：墨阶（`ink-*`）与纸色（`paper*`）的角色是「底色 → 表面
 * → 文字」，亮色里 `ink-*` 变浅、`paper` 变深，对比度才站得住。字体不随主题变。
 */

export const TOKEN_KEYS = [
  'ink-1000',
  'ink-900',
  'ink-800',
  'ink-700',
  'ink-600',
  'line',
  'line-strong',
  'paper',
  'paper-dim',
  'muted',
  'brass',
  'brass-dim',
  'live',
  'danger',
  'warn',
  'ok',
] as const;

export type TokenKey = (typeof TOKEN_KEYS)[number];
export type ThemeTokens = Record<TokenKey, string>;

export interface Theme {
  id: string;
  name: string;
  /** 一行说明这个主题的性格，选的时候看得见。 */
  note: string;
  /** 亮色主题要把浏览器的原生控件（滚动条、表单）也切到亮色。 */
  light: boolean;
  tokens: ThemeTokens;
}

export const THEMES: Theme[] = [
  {
    id: 'lagoon',
    name: '霓虹泻湖',
    note: '深青底 · 亮青强调 · 珊瑚告警',
    light: false,
    tokens: {
      'ink-1000': '#02171a',
      'ink-900': '#05232a',
      'ink-800': '#0a2f38',
      'ink-700': '#0f3d46',
      'ink-600': '#154c56',
      line: '#1c5a63',
      'line-strong': '#2b7f88',
      paper: '#dcf7f5',
      'paper-dim': '#a5d4d2',
      muted: '#6fa6a8',
      brass: '#22d3ee',
      'brass-dim': '#0e7490',
      live: '#34e5c4',
      danger: '#fb7185',
      warn: '#fbbf24',
      ok: '#4ade80',
    },
  },
  {
    id: 'sugarstorm',
    name: '电糖风暴',
    note: '洋红配霓虹绿 · 高饱和赛博糖果',
    light: false,
    tokens: {
      'ink-1000': '#120616',
      'ink-900': '#1a0a20',
      'ink-800': '#241030',
      'ink-700': '#2f1540',
      'ink-600': '#3d1b52',
      line: '#4b2465',
      'line-strong': '#6d3a8c',
      paper: '#ffe9ff',
      'paper-dim': '#e3b8ee',
      muted: '#a878b8',
      brass: '#ff3df2',
      'brass-dim': '#a21c9a',
      live: '#39ff14',
      danger: '#ff4d6d',
      warn: '#ffd166',
      ok: '#7cffb2',
    },
  },
  {
    id: 'peachy',
    name: '蜜桃',
    note: '热粉强调 · 薄荷与奶油色点缀',
    light: false,
    tokens: {
      'ink-1000': '#1c0a12',
      'ink-900': '#241018',
      'ink-800': '#301820',
      'ink-700': '#3d1f2a',
      'ink-600': '#4d2836',
      line: '#5e3344',
      'line-strong': '#8a4a63',
      paper: '#ffeef4',
      'paper-dim': '#f0c3d3',
      muted: '#bd8a9c',
      brass: '#ff4791',
      'brass-dim': '#b02a63',
      live: '#7fffd4',
      danger: '#ff5470',
      warn: '#ffe08a',
      ok: '#9be15d',
    },
  },
  {
    id: 'violet',
    name: '紫罗兰',
    note: '深浅紫打底 · 薰衣草强调色',
    light: false,
    tokens: {
      'ink-1000': '#170f24',
      'ink-900': '#211334',
      'ink-800': '#2a1b47',
      'ink-700': '#352258',
      'ink-600': '#422c6b',
      line: '#523a80',
      'line-strong': '#7a5bb0',
      paper: '#efe8ff',
      'paper-dim': '#c7b8e8',
      muted: '#9b86c4',
      brass: '#b388ff',
      'brass-dim': '#7b52c7',
      live: '#82aaff',
      danger: '#ff6e9c',
      warn: '#ffcb6b',
      ok: '#c3e88d',
    },
  },
  {
    id: 'orangefox',
    name: '橙狐',
    note: '火焰橙强调 · 暖棕底',
    light: false,
    tokens: {
      'ink-1000': '#170e0a',
      'ink-900': '#1f130d',
      'ink-800': '#2a1a12',
      'ink-700': '#362218',
      'ink-600': '#442c1f',
      line: '#553827',
      'line-strong': '#80543a',
      paper: '#ffeedd',
      'paper-dim': '#e2c4a8',
      muted: '#b08c6e',
      brass: '#ff6b35',
      'brass-dim': '#b8420f',
      live: '#ffa000',
      danger: '#ff5252',
      warn: '#ffca28',
      ok: '#9ccc65',
    },
  },
  {
    id: 'barva',
    name: '刺绣',
    note: '红蓝黄绿四色并置 · 民俗撞色',
    light: false,
    tokens: {
      'ink-1000': '#100c0b',
      'ink-900': '#171110',
      'ink-800': '#211917',
      'ink-700': '#2c2220',
      'ink-600': '#3a2d2a',
      line: '#4a3a36',
      'line-strong': '#6d5751',
      paper: '#f4efe6',
      'paper-dim': '#cfc4b4',
      muted: '#9c8f80',
      brass: '#ffd400',
      'brass-dim': '#b89500',
      live: '#00b3ff',
      danger: '#ff3b30',
      warn: '#ff9f0a',
      ok: '#34c759',
    },
  },
  {
    id: 'matrix',
    name: '矩阵',
    note: '近黑底 · 荧光绿读数',
    light: false,
    tokens: {
      'ink-1000': '#030d07',
      'ink-900': '#06150c',
      'ink-800': '#0a1f13',
      'ink-700': '#0e2919',
      'ink-600': '#13341f',
      line: '#1a4429',
      'line-strong': '#256b40',
      paper: '#ccffdd',
      'paper-dim': '#8fe0ac',
      muted: '#5aa677',
      brass: '#00ff88',
      'brass-dim': '#0a8f52',
      live: '#00ffcc',
      danger: '#ff3b5c',
      warn: '#d4ff3f',
      ok: '#38ff9c',
    },
  },
  {
    id: 'spicy',
    name: '辛辣红',
    note: '酒红底 · 朱红强调 · 琥珀点缀',
    light: false,
    tokens: {
      'ink-1000': '#150607',
      'ink-900': '#1d0a0c',
      'ink-800': '#260f12',
      'ink-700': '#331519',
      'ink-600': '#421b20',
      line: '#55242a',
      'line-strong': '#8a3a44',
      paper: '#ffe8e6',
      'paper-dim': '#e3b3ae',
      muted: '#b07f79',
      brass: '#ff5c47',
      'brass-dim': '#b32d1c',
      live: '#ff9e57',
      danger: '#ff3355',
      warn: '#ffc24b',
      ok: '#6be08c',
    },
  },
  {
    id: 'sunny',
    name: '晴天',
    note: '米黄纸底 · 橙强调（亮色）',
    light: true,
    tokens: {
      'ink-1000': '#ffffff',
      'ink-900': '#f7f6ef',
      'ink-800': '#efeee3',
      'ink-700': '#e5e3d5',
      'ink-600': '#d7d4c4',
      line: '#cfcbb8',
      'line-strong': '#a9a591',
      paper: '#23281f',
      'paper-dim': '#4a5142',
      muted: '#757c6a',
      brass: '#e06c00',
      'brass-dim': '#a35000',
      live: '#007e8a',
      danger: '#c9252d',
      warn: '#9a6200',
      ok: '#1a7f43',
    },
  },
  {
    id: 'bubblegum',
    name: '泡泡糖',
    note: '粉纸底 · 洋红强调（亮色）',
    light: true,
    tokens: {
      'ink-1000': '#ffffff',
      'ink-900': '#fdf1f7',
      'ink-800': '#f9e4ef',
      'ink-700': '#f3d5e5',
      'ink-600': '#e9c2d6',
      line: '#e2b5cc',
      'line-strong': '#b988a4',
      paper: '#2d1226',
      'paper-dim': '#5e3350',
      muted: '#8c6279',
      brass: '#d81b8c',
      'brass-dim': '#9c1263',
      live: '#0b8f80',
      danger: '#c62828',
      warn: '#9a6200',
      ok: '#17794a',
    },
  },
];

export const DEFAULT_THEME = 'lagoon';
export const THEME_STORAGE_KEY = 'cogneva.theme';

export function themeById(id: string | null | undefined): Theme {
  return THEMES.find((theme) => theme.id === id) ?? THEMES.find((theme) => theme.id === DEFAULT_THEME) ?? THEMES[0];
}

/** 把一组令牌写到文档根上：内联变量盖过 `:root`，所以换主题不需要重载样式表。 */
export function applyTheme(id: string): Theme {
  const theme = themeById(id);
  const root = document.documentElement;
  for (const key of TOKEN_KEYS) {
    root.style.setProperty(`--${key}`, theme.tokens[key]);
  }
  root.dataset.theme = theme.id;
  root.style.colorScheme = theme.light ? 'light' : 'dark';
  return theme;
}

/** 进页面时先套用上次选的主题，再让 React 渲染——否则首屏会用回默认色闪一下。 */
export function bootTheme(): Theme {
  let stored: string | null = null;
  try {
    stored = window.localStorage.getItem(THEME_STORAGE_KEY);
  } catch {
    // 隐私模式里 localStorage 会抛；没有存过的记录就当默认主题。
  }
  return applyTheme(stored ?? DEFAULT_THEME);
}

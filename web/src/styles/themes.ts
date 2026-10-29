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
 *
 * 几套糖果霓虹主题的取值直接来自 Pink Pixel 那组 VS Code 主题的官方配色
 * （Electric Sugarstorm / Neon Lagoon / Candy Nebula / Aurora Burst）：编辑器背景、
 * 侧栏、强调色、终端 ANSI 色各自落在同一个角色上，所以换过去就是同一套观感，
 * 而不是「照着名字另调一版」。
 */

/**
 * `tag-*` 是顶栏那五格的**身份色**，不是状态色：代码平台、上网、模型、告警、
 * 收件箱各占一个，同一个主题里五个色相撑开四十度以上，所以一眼能把它们分开。
 * 它们以前共用 `brass` 与 `live`——两格同色，读者只能靠字认，认字就等于没有色。
 *
 * 每套主题按自己的强调色对齐转一个角度取这五个色相，所以换主题之后它们跟着换，
 * 又不至于压到这套主题的强调色和四个状态色上；每个色对的对比度都对过顶栏底色，
 * 最浅的一套也有 5:1。亮色主题的取法不均匀（跳过黄绿的橄榄段），因为同样的角度
 * 网格在浅底上会让其中一格变成脏橄榄色。
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
  'tag-platform',
  'tag-web',
  'tag-model',
  'tag-alerts',
  'tag-inbox',
] as const;

export type TokenKey = (typeof TOKEN_KEYS)[number];
export type ThemeTokens = Record<TokenKey, string>;

export interface Theme {
  id: string;
  name: string;
  /**
   * 这套配色在上游叫什么（主题包 / 主题名）。
   *
   * 单独占一格而不是并进 `note`：中文名是我们起的，认这套主题的人记的是
   * 上游那个名字（Pink Pixel 的 Neon Lagoon、Absolutely 的 Matrix…）。并进
   * 说明里就变成了「一句描述」，没人能从里面认出自己找的那一套。
   */
  origin: string;
  /** 一行说明这个主题的性格，选的时候看得见。 */
  note: string;
  /** 亮色主题要把浏览器的原生控件（滚动条、表单）也切到亮色。 */
  light: boolean;
  tokens: ThemeTokens;
}

/** 选择器按这三族分栏：一串十七张卡片平铺，选的人只会看前两行。 */
export type ThemeFamily = 'candy' | 'classic' | 'light';

export const FAMILY_ORDER: ThemeFamily[] = ['candy', 'classic', 'light'];

export const FAMILY_LABEL: Record<ThemeFamily, string> = {
  candy: '糖果霓虹',
  classic: '暗色经典',
  light: '亮色护眼',
};

/** 从 VS Code 糖果霓虹主题搬过来的那几套，单独成族。 */
const CANDY_IDS = new Set(['lagoon', 'sugarstorm', 'nebula', 'aurora', 'peachy', 'elhassan']);

export function themeFamily(theme: Theme): ThemeFamily {
  if (theme.light) return 'light';
  return CANDY_IDS.has(theme.id) ? 'candy' : 'classic';
}

export const THEMES: Theme[] = [
  {
    id: 'lagoon',
    name: '霓虹泻湖',
    origin: 'Pink Pixel · Neon Lagoon',
    note: '深青底 / 亮青强调 / 珊瑚粉蓝',
    light: false,
    tokens: {
      'ink-1000': '#031014',
      'ink-900': '#04121a',
      'ink-800': '#03161d',
      'ink-700': '#082430',
      'ink-600': '#0c3140',
      line: '#123c4b',
      'line-strong': '#1c5b6e',
      paper: '#dcf9ff',
      'paper-dim': '#93c8d6',
      muted: '#4e6e7c',
      brass: '#28ffe0',
      'brass-dim': '#0f9c8a',
      live: '#46c5ff',
      danger: '#ff6b7a',
      warn: '#f7ff5a',
      ok: '#a4ff5f',
      'tag-platform': '#5785f4',
      'tag-web': '#e13af2',
      'tag-model': '#f3523f',
      'tag-alerts': '#90c90d',
      'tag-inbox': '#0dc96c',
    },
  },
  {
    id: 'sugarstorm',
    name: '电糖风暴',
    origin: 'Pink Pixel · Electric Sugarstorm',
    note: '洋红 + 霓虹青，这一组里最张扬的一套',
    light: false,
    tokens: {
      'ink-1000': '#050607',
      'ink-900': '#0a0d10',
      'ink-800': '#0d1116',
      'ink-700': '#141a21',
      'ink-600': '#1c242e',
      line: '#26313d',
      'line-strong': '#3a4a5c',
      paper: '#fdf8ff',
      'paper-dim': '#b9b1c4',
      muted: '#5b616c',
      brass: '#ff21a3',
      'brass-dim': '#9c1263',
      live: '#0cfff7',
      danger: '#ff0066',
      warn: '#fff952',
      ok: '#4bff9e',
      'tag-platform': '#f2532c',
      'tag-web': '#7ec90d',
      'tag-model': '#0dc97e',
      'tag-alerts': '#617ff5',
      'tag-inbox': '#ef0ff0',
    },
  },
  {
    id: 'nebula',
    name: '糖果星云',
    origin: 'Pink Pixel · Candy Nebula',
    note: '深紫底 / 品红 / 薄荷绿',
    light: false,
    tokens: {
      'ink-1000': '#08010e',
      'ink-900': '#0b0213',
      'ink-800': '#140118',
      'ink-700': '#1d0626',
      'ink-600': '#270a33',
      line: '#351244',
      'line-strong': '#5a2270',
      paper: '#ffecf7',
      'paper-dim': '#d0a9d6',
      muted: '#8c6f97',
      brass: '#ff61b0',
      'brass-dim': '#a3246c',
      live: '#7efff5',
      danger: '#ff4fa3',
      warn: '#ffc26d',
      ok: '#7effc3',
      'tag-platform': '#f33f48',
      'tag-web': '#adc90d',
      'tag-model': '#0dc94f',
      'tag-alerts': '#2782f1',
      'tag-inbox': '#c949f3',
    },
  },
  {
    id: 'aurora',
    name: '极光爆发',
    origin: 'Pink Pixel · Aurora Burst',
    note: '石墨底 / 珊瑚红 / 荧光绿',
    light: false,
    tokens: {
      'ink-1000': '#111319',
      'ink-900': '#16181e',
      'ink-800': '#1a1c24',
      'ink-700': '#22252f',
      'ink-600': '#2b2f3a',
      line: '#353a47',
      'line-strong': '#4b5265',
      paper: '#d5d8da',
      'paper-dim': '#a7abb2',
      muted: '#767b86',
      brass: '#ff617c',
      'brass-dim': '#b53b52',
      live: '#b5ff8a',
      danger: '#d68492',
      warn: '#e0caa3',
      ok: '#9ec08a',
      'tag-platform': '#f36549',
      'tag-web': '#85c90d',
      'tag-model': '#0dc977',
      'tag-alerts': '#6a8bf6',
      'tag-inbox': '#ec44f3',
    },
  },
  {
    id: 'peachy',
    name: '蜜桃',
    origin: 'Peachy Themes',
    note: '热粉 #FF4791 / 薄荷 / 奶油黄',
    light: false,
    tokens: {
      'ink-1000': '#1a0a10',
      'ink-900': '#220d15',
      'ink-800': '#2c121b',
      'ink-700': '#3a1824',
      'ink-600': '#4a2030',
      line: '#5c2a3d',
      'line-strong': '#8a4159',
      paper: '#ffeef4',
      'paper-dim': '#f0c3d3',
      muted: '#bd8a9c',
      brass: '#ff4791',
      'brass-dim': '#b02a63',
      live: '#7fffd4',
      danger: '#ff5470',
      warn: '#ffe08a',
      ok: '#9be15d',
      'tag-platform': '#f45c57',
      'tag-web': '#9ec90d',
      'tag-model': '#0dc95e',
      'tag-alerts': '#528ef4',
      'tag-inbox': '#d952f4',
    },
  },
  {
    id: 'elhassan',
    name: '荧光紫',
    origin: 'Elhassan Neon',
    note: '深紫底 / 品红 / RGB 青色强调',
    light: false,
    tokens: {
      'ink-1000': '#080312',
      'ink-900': '#0d0620',
      'ink-800': '#130a2b',
      'ink-700': '#1b0f3a',
      'ink-600': '#25154d',
      line: '#331e63',
      'line-strong': '#4d2f92',
      paper: '#f0e9ff',
      'paper-dim': '#c3b0ee',
      muted: '#8d78c2',
      brass: '#ff2e88',
      'brass-dim': '#a3135a',
      live: '#00f0ff',
      danger: '#ff3860',
      warn: '#ffd23f',
      ok: '#39ff88',
      'tag-platform': '#f05319',
      'tag-web': '#71c90d',
      'tag-model': '#0dc98b',
      'tag-alerts': '#6a7df6',
      'tag-inbox': '#f11ee2',
    },
  },
  {
    id: 'violet',
    name: '紫罗兰',
    origin: 'Shades of Purple',
    note: '深浅紫打底 / 薰衣草强调',
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
      'tag-platform': '#e76ad0',
      'tag-web': '#e07e42',
      'tag-model': '#5fb81e',
      'tag-alerts': '#1eb896',
      'tag-inbox': '#8d8fec',
    },
  },
  {
    id: 'orangefox',
    name: '橙狐',
    origin: 'OrangeFox',
    note: '火焰橙强调 / 暖棕底',
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
      'tag-platform': '#b8b61e',
      'tag-web': '#1eb83a',
      'tag-model': '#2c99dd',
      'tag-alerts': '#ba77e9',
      'tag-inbox': '#e76a85',
    },
  },
  {
    id: 'oscurange',
    name: '暗橙',
    origin: 'Absolutely Theme · Oscurange',
    note: '近黑底 / 琥珀橙 / 暖纸色',
    light: false,
    tokens: {
      'ink-1000': '#0e0a06',
      'ink-900': '#141008',
      'ink-800': '#1c160c',
      'ink-700': '#261e11',
      'ink-600': '#322717',
      line: '#42331f',
      'line-strong': '#6b5232',
      paper: '#ffeccc',
      'paper-dim': '#dcc19a',
      muted: '#a08a6a',
      brass: '#ff9f1c',
      'brass-dim': '#b86a00',
      live: '#ffd166',
      danger: '#ff5a3c',
      warn: '#ffcc33',
      ok: '#9ad46b',
      'tag-platform': '#a7b81e',
      'tag-web': '#1eb84e',
      'tag-model': '#4290e0',
      'tag-alerts': '#c165e6',
      'tag-inbox': '#e5616d',
    },
  },
  {
    id: 'lobster',
    name: '龙虾',
    origin: 'Absolutely Theme · Lobster',
    note: '酒红底 / 朱红强调 / 珊瑚橙',
    light: false,
    tokens: {
      'ink-1000': '#120507',
      'ink-900': '#190a0d',
      'ink-800': '#220f13',
      'ink-700': '#2d151a',
      'ink-600': '#3b1c23',
      line: '#4d262f',
      'line-strong': '#7a3b48',
      paper: '#ffe9e4',
      'paper-dim': '#e5b3a6',
      muted: '#b07f74',
      brass: '#ff5e4d',
      'brass-dim': '#b32e22',
      live: '#ffb347',
      danger: '#ff2d55',
      warn: '#ffd166',
      ok: '#6ee7a8',
      'tag-platform': '#d76d23',
      'tag-web': '#5ab81e',
      'tag-model': '#1eb89b',
      'tag-alerts': '#8180ea',
      'tag-inbox': '#e250c3',
    },
  },
  {
    id: 'barva',
    name: '刺绣',
    origin: 'Barva',
    note: '红蓝黄绿四色并置 / 民俗撞色',
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
      'tag-platform': '#66b81e',
      'tag-web': '#1eb88f',
      'tag-model': '#8087ea',
      'tag-alerts': '#e458d2',
      'tag-inbox': '#df703a',
    },
  },
  {
    id: 'matrix',
    name: '矩阵',
    origin: 'Absolutely Theme · Matrix',
    note: '近黑底 / 荧光绿读数',
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
      'tag-platform': '#1ea6b8',
      'tag-web': '#9e7bea',
      'tag-model': '#e561a1',
      'tag-alerts': '#b88d1e',
      'tag-inbox': '#2bb81e',
    },
  },
  {
    id: 'spicy',
    name: '辛辣红',
    origin: 'Spicy Red',
    note: '酒红底 / 朱红强调 / 琥珀点缀',
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
      'tag-platform': '#b8991e',
      'tag-web': '#1eb81e',
      'tag-model': '#1e9ab8',
      'tag-alerts': '#a173e8',
      'tag-inbox': '#e55d93',
    },
  },
  {
    id: 'softgreen',
    name: '护眼绿',
    origin: 'IDEA 自定义背景 · #E9F5DB',
    note: '长时间读日志用的浅绿纸（亮色）',
    light: true,
    tokens: {
      'ink-1000': '#ffffff',
      'ink-900': '#e9f5db',
      'ink-800': '#f3faeb',
      'ink-700': '#dcecce',
      'ink-600': '#c9dfb6',
      line: '#bcd3a6',
      'line-strong': '#93b47c',
      paper: '#1f2a1b',
      'paper-dim': '#46543f',
      muted: '#6b7d61',
      brass: '#2f7d32',
      'brass-dim': '#1d5a20',
      live: '#00796b',
      danger: '#c62828',
      warn: '#8a6100',
      ok: '#2e7d32',
      'tag-platform': '#c51b3f',
      'tag-web': '#945914',
      'tag-model': '#107535',
      'tag-alerts': '#156b9d',
      'tag-inbox': '#871fe0',
    },
  },
  {
    id: 'cream',
    name: '米黄纸',
    origin: 'IDEA 自定义背景 · #FFFFE0',
    note: '米黄纸底 / 低蓝光（亮色）',
    light: true,
    tokens: {
      'ink-1000': '#ffffff',
      'ink-900': '#ffffe0',
      'ink-800': '#fffff0',
      'ink-700': '#f6f3c9',
      'ink-600': '#e8e3b0',
      line: '#ddd7a0',
      'line-strong': '#b9b076',
      paper: '#2b2410',
      'paper-dim': '#574c26',
      muted: '#7d7245',
      brass: '#b35900',
      'brass-dim': '#7a3c00',
      live: '#00695c',
      danger: '#c62828',
      warn: '#8a6100',
      ok: '#2e7d32',
      'tag-platform': '#bc1a9c',
      'tag-web': '#ce1c26',
      'tag-model': '#277911',
      'tag-alerts': '#117962',
      'tag-inbox': '#1f29e0',
    },
  },
  {
    id: 'sunny',
    name: '晴天',
    origin: 'Mango / Sunny',
    note: '米黄纸底 / 橙强调（亮色）',
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
      'tag-platform': '#af1887',
      'tag-web': '#bc1c1a',
      'tag-model': '#1e700f',
      'tag-alerts': '#0f6c5e',
      'tag-inbox': '#221fe0',
    },
  },
  {
    id: 'bubblegum',
    name: '泡泡糖',
    origin: 'Bubblegum',
    note: '粉纸底 / 洋红强调（亮色）',
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
      'tag-platform': '#116879',
      'tag-web': '#1f2ee0',
      'tag-model': '#b81940',
      'tag-alerts': '#755c10',
      'tag-inbox': '#166c0f',
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

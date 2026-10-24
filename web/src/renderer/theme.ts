/*
 * 画布上的颜色也只从主题令牌里取。
 *
 * `<canvas>` 不会继承 CSS 变量——写死的十六进制在这里会停在作者当时那一套配色
 * 上，而周围的面已经换了主题。所以每次落笔前从文档根上读一次当前值：换主题时
 * 下一帧就是新颜色，不需要重载或重建画布。
 */

function readToken(name: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(`--${name}`).trim();
  return value || '#888888';
}

/** 令牌的实色。 */
export function token(name: string): string {
  return readToken(name);
}

/** 令牌的半透明版本：`#rrggbb` 或 `#rgb` 转成 `rgba()`，其它写法原样返回。 */
export function tokenAlpha(name: string, alpha: number): string {
  const raw = readToken(name);
  const match = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(raw);
  if (!match) return raw;
  const hex = match[1];
  const full =
    hex.length === 3
      ? hex
          .split('')
          .map((c) => c + c)
          .join('')
      : hex;
  const red = parseInt(full.slice(0, 2), 16);
  const green = parseInt(full.slice(2, 4), 16);
  const blue = parseInt(full.slice(4, 6), 16);
  return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
}

/** 与界面同一族的字体：界面用 --font-ui，读数与标识用 --font-mono。 */
export function font(kind: 'ui' | 'mono' = 'ui'): string {
  return getComputedStyle(document.documentElement).getPropertyValue(kind === 'mono' ? '--font-mono' : '--font-ui').trim();
}

/**
 * 访问令牌的寿命算术。三个纯函数，没有 import——所以它能单文件编译、在 node
 * 里按判据跑（`deploy/scripts/check-web-token-refresh.sh` 就是这么验的）。
 *
 * 为什么要这一层：网关签的访问令牌只活 15 分钟
 * （`DEFAULT_ACCESS_TOKEN_TTL_MINUTES`）。界面以前只把 `access_token` 存进
 * localStorage，同一份登录响应里的 `refresh_token` 直接丢掉，也不看令牌什么
 * 时候到期。于是登录一刻钟之后：事件流每 30 秒按旧令牌重连一次、每次都被网关
 * 判 `Token expired`，页脚状态条写着 closed，而所有 REST 读数 401——界面看着
 * 还在，其实一格都没有在动。到期时刻必须从令牌自己身上读出来，因为它是唯一
 * 一份在刷新、重开页面、OAuth 回调三条路上都存在的凭据。
 */

/** 提前多久续期：留一分钟，够一次往返，又不至于把 15 分钟的寿命续成 14 分钟。 */
export const REFRESH_LEAD_MS = 60_000;

/** 读不出到期时刻时的续期间隔：不猜，但也别等到 401 才发现。 */
export const FALLBACK_REFRESH_MS = 10 * 60_000;

const B64URL = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

/**
 * base64url → 字节。手写而不用 `atob`：这个模块要同时在浏览器与 node 里跑，
 * 而 `atob` 只在前者有、`Buffer` 只在后者有；两者都不认的写法只剩手写。
 * jwt 的载荷是 JSON，`"exp":` 与数字都是 ASCII，所以下面按 latin1 读字符足够。
 */
function base64urlBytes(input: string): Uint8Array | null {
  const out: number[] = [];
  let buffer = 0;
  let bits = 0;
  for (const ch of input) {
    if (ch === '=') break;
    const value = B64URL.indexOf(ch);
    if (value < 0) return null;
    buffer = (buffer << 6) | value;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push((buffer >> bits) & 0xff);
    }
  }
  return new Uint8Array(out);
}

/**
 * 令牌自己的到期时刻（毫秒），读不出来就是 `null`。
 *
 * 读不出来不等于「马上到期」：调用方拿 `null` 走兜底间隔，而不是当场登出——
 * 一个不是本平台签的形状（比如平台 OAuth 透传下来的 access token）照样要能
 * 用满它自己的寿命。
 */
export function tokenExpiryMs(token: string | null | undefined): number | null {
  if (!token) return null;
  const parts = token.split('.');
  if (parts.length < 2) return null;
  const bytes = base64urlBytes(parts[1]);
  if (!bytes) return null;
  let text = '';
  for (let i = 0; i < bytes.length; i += 1) text += String.fromCharCode(bytes[i]);
  const match = /"exp"\s*:\s*(-?\d+)/.exec(text);
  if (!match) return null;
  const seconds = Number(match[1]);
  if (!Number.isFinite(seconds) || seconds <= 0) return null;
  return seconds * 1000;
}

/**
 * 距下一次续期还有多久（毫秒）。到期时刻已在提前量之内或已经过去 ⇒ 0，
 * 也就是「现在就续」；`null` ⇒ 兜底间隔。
 *
 * 返回 0 而不是负数：`setTimeout` 收到负数按 0 处理，但把它交出去会让读日志的
 * 人以为这里有个「过去的时间」，而它其实是一个「立刻」。
 */
export function nextRefreshDelayMs(
  expiresAtMs: number | null,
  nowMs: number,
  leadMs: number = REFRESH_LEAD_MS,
  fallbackMs: number = FALLBACK_REFRESH_MS
): number {
  if (expiresAtMs === null || !Number.isFinite(expiresAtMs)) return fallbackMs;
  const delay = expiresAtMs - nowMs - leadMs;
  return delay > 0 ? delay : 0;
}

import type { LoginResponse } from '@/api/client';

/**
 * 浏览器存储里的键只在这里声明一次。
 *
 * 同一个字符串写在两处时，没有任何东西会把它们对上：改一处的名字，另一处仍
 * 在原键上读写，于是「登录成功」与「登录页又出现了」之间没有一个字解释出了
 * 什么事。网关渲染的落地页（`crates/cog-gateway/src/auth/platform.rs`）写的是
 * 同样的键，它没法 import 这个文件，只能在那边再声明一次——两边的名字与取值
 * 由 `web_storage_keys_are_declared_once_test` 对上。
 */
export const TOKEN_KEY = 'cogneva_token';
export const REFRESH_KEY = 'cogneva_refresh_token';
export const USER_KEY = 'cogneva_user';

export interface StoredSession {
  token: string | null;
  refreshToken: string | null;
  username: string | null;
}

/** 存下来的名字，读坏了一律当作没存过。 */
function storedUsername(): string | null {
  try {
    const raw = localStorage.getItem(USER_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { username?: unknown };
    return typeof parsed.username === 'string' ? parsed.username : null;
  } catch {
    // 一份存坏的 JSON 不该让整个登录一起失败：名字是显示用的，凭据才是命门。
    return null;
  }
}

/** 两份凭据与用户名一起读：只读回其中一份，到期那一刻的行为就说不清了。 */
export function readSession(): StoredSession {
  return {
    token: localStorage.getItem(TOKEN_KEY),
    refreshToken: localStorage.getItem(REFRESH_KEY),
    username: storedUsername(),
  };
}

/**
 * 一次登录拿到的东西，一次写全。
 *
 * 入口有好几个（口令、平台令牌、OAuth 换码、回调落地页的 postMessage），每个
 * 自己写一遍存储就必然有一个少写一个字段——少了续期凭据，那一支会话 15 分钟
 * 后只能登出；少了名字，刷新页面后状态条上就只剩一个空位。
 */
export function storeSession(
  session: Pick<LoginResponse, 'token' | 'refreshToken' | 'user'>
): void {
  storeTokens(session.token, session.refreshToken);
  localStorage.setItem(
    USER_KEY,
    JSON.stringify({ id: session.user.id, username: session.user.username })
  );
}

/**
 * 只换令牌，不动用户名：续期拿到的是同一枚会话的新令牌，把上一次登录存下的
 * 名字抹掉，状态条上就会在续期那一刻变成空白。
 */
export function storeTokens(token: string, refreshToken: string | null): void {
  localStorage.setItem(TOKEN_KEY, token);
  if (refreshToken) localStorage.setItem(REFRESH_KEY, refreshToken);
  else localStorage.removeItem(REFRESH_KEY);
}

export function clearSession(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(REFRESH_KEY);
  localStorage.removeItem(USER_KEY);
}

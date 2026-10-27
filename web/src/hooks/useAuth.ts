import { useState, useCallback, useEffect } from 'react';
import {
  api,
  login,
  platformLogin,
  refreshSession,
  type LoginResponse,
  type PlatformProvider,
} from '@/api/client';
import { nextRefreshDelayMs, tokenExpiryMs } from '@/auth/tokenLifecycle';
import {
  clearSession,
  readSession,
  storeSession,
  storeTokens,
} from '@/auth/session';

export interface AuthState {
  token: string | null;
  refreshToken: string | null;
  username: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
}

export function useAuth() {
  const [state, setState] = useState<AuthState>(() => {
    const { token, refreshToken, username } = readSession();
    if (token) {
      api.setToken(token);
    }
    return {
      token,
      refreshToken,
      username,
      isAuthenticated: !!token,
      isLoading: false,
      error: null,
    };
  });

  useEffect(() => {
    const { token, refreshToken, username } = readSession();
    if (token) {
      api.setToken(token);
      setState((prev) => ({
        ...prev,
        token,
        refreshToken,
        username,
        isAuthenticated: true,
      }));
    }
  }, []);

  /**
   * 换一枚新的访问令牌。**令牌变了 `App` 才会重新渲染，WebSocket 的地址才会
   * 跟着变**（`/ws?token=…` 就是从它拼的），所以这一步同时是「续期」与「重连」。
   */
  const applyToken = useCallback((token: string, refreshToken: string | null) => {
    storeTokens(token, refreshToken);
    api.setToken(token);
    setState((prev) => ({
      ...prev,
      token,
      refreshToken,
      isAuthenticated: true,
      isLoading: false,
      error: null,
    }));
  }, []);

  const applyLogin = useCallback(
    (response: LoginResponse) => {
      storeSession(response);
      api.setToken(response.token);
      setState({
        token: response.token,
        refreshToken: response.refreshToken,
        username: response.user.username,
        isAuthenticated: true,
        isLoading: false,
        error: null,
      });
    },
    []
  );

  const signIn = useCallback(
    async (username: string, password: string) => {
      setState((prev) => ({ ...prev, isLoading: true, error: null }));
      try {
        applyLogin(await login(username, password));
      } catch (error) {
        const message =
          (error as { message?: string }).message ?? 'Login failed';
        setState((prev) => ({ ...prev, isLoading: false, error: message }));
      }
    },
    [applyLogin]
  );

  const signInPlatformToken = useCallback(
    async (provider: PlatformProvider, accessToken: string) => {
      setState((prev) => ({ ...prev, isLoading: true, error: null }));
      try {
        applyLogin(await platformLogin(provider, accessToken));
      } catch (error) {
        const message =
          (error as { message?: string }).message ?? 'Login failed';
        setState((prev) => ({ ...prev, isLoading: false, error: message }));
        throw error;
      }
    },
    [applyLogin]
  );

  // The OAuth callback landing page posts the finished login back to the tab
  // that opened it; adopt the token so the app unlocks without a reload.
  useEffect(() => {
    const onMessage = (e: MessageEvent) => {
      if (e.origin !== window.location.origin) return;
      const data = (e.data ?? {}) as {
        type?: string;
        data?: {
          auth?: {
            access_token?: string;
            refresh_token?: string;
            user?: { id: string; username: string };
          };
        };
      };
      if (data.type !== 'cogneva-login') return;
      const token = data.data?.auth?.access_token;
      const user = data.data?.auth?.user;
      if (!token || !user) return;
      applyLogin({ token, refreshToken: data.data?.auth?.refresh_token ?? null, user });
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [applyLogin]);

  const signOut = useCallback(() => {
    clearSession();
    api.clearToken();
    setState({
      token: null,
      refreshToken: null,
      username: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,
    });
  }, []);

  const { token, refreshToken } = state;

  /**
   * 到期前换一枚新的访问令牌；换不到就登出。
   *
   * 登出是刻意的：没有这一句，界面会停在一个「状态条写着 closed、每一格 REST
   * 都 401」的壳上——它看起来完全像「实时流没实现」，而不是「凭据过期了」。
   */
  const renew = useCallback(async () => {
    if (!refreshToken) {
      signOut();
      return;
    }
    try {
      applyToken(await refreshSession(refreshToken), refreshToken);
    } catch {
      signOut();
    }
  }, [refreshToken, applyToken, signOut]);

  useEffect(() => {
    if (!token) return;
    const delay = nextRefreshDelayMs(tokenExpiryMs(token), Date.now());
    const timer = window.setTimeout(() => {
      void renew();
    }, delay);
    // 后台标签页的定时器会被浏览器节流到一分钟一拍甚至更久，机器睡眠期间干脆
    // 不走。回到前台时按同一个判据再问一次：已经进入提前量就当场续。
    const onVisible = () => {
      if (document.visibilityState !== 'visible') return;
      if (nextRefreshDelayMs(tokenExpiryMs(token), Date.now()) === 0) {
        void renew();
      }
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [token, renew]);

  return { ...state, signIn, signInPlatformToken, signOut };
}

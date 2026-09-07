import { useState, useCallback, useEffect } from 'react';
import {
  api,
  login,
  platformLogin,
  type LoginResponse,
  type PlatformProvider,
} from '@/api/client';

const TOKEN_KEY = 'cogneva_token';

export interface AuthState {
  token: string | null;
  username: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
}

export function useAuth() {
  const [state, setState] = useState<AuthState>(() => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      api.setToken(token);
    }
    return {
      token,
      username: null,
      isAuthenticated: !!token,
      isLoading: false,
      error: null,
    };
  });

  useEffect(() => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      api.setToken(token);
      setState((prev) => ({ ...prev, token, isAuthenticated: true }));
    }
  }, []);

  const applyLogin = useCallback((response: LoginResponse) => {
    localStorage.setItem(TOKEN_KEY, response.token);
    api.setToken(response.token);
    setState({
      token: response.token,
      username: response.user.username,
      isAuthenticated: true,
      isLoading: false,
      error: null,
    });
  }, []);

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
        data?: { auth?: { access_token?: string; user?: { id: string; username: string } } };
      };
      if (data.type !== 'cogneva-login') return;
      const token = data.data?.auth?.access_token;
      const user = data.data?.auth?.user;
      if (!token || !user) return;
      applyLogin({ token, user });
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [applyLogin]);

  const signOut = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY);
    api.clearToken();
    setState({
      token: null,
      username: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,
    });
  }, []);

  return { ...state, signIn, signInPlatformToken, signOut };
}

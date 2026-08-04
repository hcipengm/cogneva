import { useState, useCallback, useEffect } from 'react';
import { api, login } from '@/api/client';

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

  const signIn = useCallback(async (username: string, password: string) => {
    setState((prev) => ({ ...prev, isLoading: true, error: null }));
    try {
      const response = await login(username, password);
      localStorage.setItem(TOKEN_KEY, response.token);
      api.setToken(response.token);
      setState({
        token: response.token,
        username: response.user.username,
        isAuthenticated: true,
        isLoading: false,
        error: null,
      });
    } catch (error) {
      const message =
        (error as { message?: string }).message ?? 'Login failed';
      setState((prev) => ({
        ...prev,
        isLoading: false,
        error: message,
      }));
    }
  }, []);

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

  return { ...state, signIn, signOut };
}

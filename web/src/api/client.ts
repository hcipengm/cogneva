const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';

export interface ApiError {
  status: number;
  message: string;
}

export class ApiClient {
  private token: string | null = null;

  setToken(token: string): void {
    this.token = token;
  }

  clearToken(): void {
    this.token = null;
  }

  async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<T> {
    const headers = new Headers(options.headers);
    headers.set('Accept', 'application/json');
    headers.set('Content-Type', 'application/json');

    if (this.token) {
      headers.set('Authorization', `Bearer ${this.token}`);
    }

    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...options,
      headers,
    });

    if (!response.ok) {
      const text = await response.text();
      throw {
        status: response.status,
        message: text || response.statusText,
      } as ApiError;
    }

    if (response.status === 204) {
      return undefined as T;
    }

    return (await response.json()) as T;
  }

  get<T>(path: string): Promise<T> {
    return this.request<T>(path, { method: 'GET' });
  }

  post<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>(path, {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }
}

export const api = new ApiClient();

export interface LoginResponse {
  token: string;
  user: {
    id: string;
    username: string;
  };
}

export interface ClusterOverview {
  nodes: ClusterNode[];
  agents: ClusterAgent[];
  tasks: ClusterTask[];
}

export interface ClusterNode {
  id: string;
  name: string;
  status: 'healthy' | 'degraded' | 'unhealthy' | 'unknown';
  cpu_percent: number;
  memory_percent: number;
}

export interface ClusterAgent {
  id: string;
  name: string;
  role: string;
  status: 'idle' | 'running' | 'error' | 'recovering';
}

export interface ClusterTask {
  id: string;
  name: string;
  status: string;
  progress: number;
}

export interface Session {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
}

export interface Message {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  created_at: string;
}

export async function login(
  username: string,
  password: string
): Promise<LoginResponse> {
  const res = await api.post<{
    access_token: string;
    user: { id: string; username: string };
  }>('/api/v1/auth/login', { username, password });
  return { token: res.access_token, user: res.user };
}

// ── Platform-account login (connect GitHub/Gitee = sign in) ──

export type PlatformProvider = 'github' | 'gitee';

export interface PlatformAuthPayload {
  auth: {
    access_token: string;
    user: { id: string; username: string };
  };
  account_created: boolean;
}

function toLoginResponse(p: PlatformAuthPayload): LoginResponse {
  return { token: p.auth.access_token, user: p.auth.user };
}

/** PAT / manual token login — also the offline escape hatch. */
export async function platformLogin(
  provider: PlatformProvider,
  accessToken: string
): Promise<LoginResponse> {
  const p = await api.post<PlatformAuthPayload>('/api/v1/auth/platform/login', {
    provider,
    access_token: accessToken,
  });
  return toLoginResponse(p);
}

/** Begin the OAuth authorization-code flow. */
export async function platformOAuthStart(
  provider: PlatformProvider
): Promise<{ authorize_url: string; state: string }> {
  return api.post('/api/v1/auth/platform/start', {
    provider,
    redirect_origin: window.location.origin,
  });
}

/** Finish OAuth with the pasted code or full redirect URL. */
export async function platformOAuthExchange(
  provider: PlatformProvider,
  state: string,
  code?: string,
  redirectUrl?: string
): Promise<LoginResponse> {
  const p = await api.post<PlatformAuthPayload>('/api/v1/auth/platform/exchange', {
    provider,
    state,
    code,
    redirect_url: redirectUrl,
  });
  return toLoginResponse(p);
}

/** Region probe: mainland-China timezones/locales get Gitee recommended. */
export function preferredPlatform(): PlatformProvider {
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone ?? '';
  const lang = navigator.language ?? '';
  const cnTz = ['Asia/Shanghai', 'Asia/Chongqing', 'Asia/Harbin', 'Asia/Urumqi'];
  if (cnTz.includes(tz)) return 'gitee';
  if (lang.toLowerCase().startsWith('zh') && !tz.startsWith('America')) return 'gitee';
  return 'github';
}

export async function getClusterOverview(): Promise<ClusterOverview> {
  return api.get<ClusterOverview>('/api/v1/cluster/overview');
}

export async function listSessions(): Promise<Session[]> {
  return api.get<Session[]>('/api/v1/sessions');
}

export async function createSession(title: string): Promise<Session> {
  return api.post<Session>('/api/v1/sessions', { title });
}

export async function sendMessage(
  sessionId: string,
  content: string
): Promise<Message> {
  return api.post<Message>(`/api/v1/sessions/${sessionId}/messages`, {
    content,
  });
}

export interface LlmBackend {
  provider: string;
  base_url: string;
  model: string;
  api_style: string;
  enabled: boolean;
  has_key: boolean;
}

export interface LlmStatus {
  configured: boolean;
  backends: LlmBackend[];
}

export interface LlmConfigPayload {
  base_url: string;
  model: string;
  api_key: string;
  /** 协议面首猜，后端保存时做双协议实证探测，猜错会被纠正 */
  api_style?: string;
  /** 探测失败后的"仍然保存"：跳过连通验证直接落库 */
  skip_verify?: boolean;
}

export interface LlmConfigResult {
  ok: boolean;
  restarted: string[];
  failed_restarts: string[];
  message: string;
}

export async function getLlmStatus(): Promise<LlmStatus> {
  return api.get<LlmStatus>('/api/v1/admin/llm-status');
}

export async function saveLlmConfig(
  payload: LlmConfigPayload
): Promise<LlmConfigResult> {
  return api.post<LlmConfigResult>('/api/v1/admin/llm-config', payload);
}

// ── 令牌花销（按 actor，读耐久 rollup） ──

/** 一条 `(actor, origin, upstream, 协议)` 的花销汇总行。 */
export interface ActorUsage {
  /** 网关归一化后的调用方（`self_review`、`agent:<role>`、`unknown`） */
  actor: string;
  /** 调用来自哪种环境（`gateway_pool`／`resident_worker`／`interactive`／`sandbox`／`unknown`） */
  origin: string;
  /** 服务这次调用的上游池身份 */
  upstream: string;
  /**
   * 计数所用的线路协议族。`tokens_input` 的含义**逐协议不同**——
   * OpenAI 兼容上游里它已含 `tokens_cached`，Anthropic 上两者不相交——
   * 所以缓存命中率只在一个协议内可比，不能跨协议对消。
   */
  api_style: string;
  /** 区间内所有计量调用，含失败的 */
  calls: number;
  /** `calls` 里失败的那部分。它单独留着，因为失败行按构造不产 token：
   *  「全失败」与「完全没流量」的 token 数一样，靠这个数分开 */
  failed_calls: number;
  tokens_input: number;
  tokens_output: number;
  tokens_cached: number;
  /** 覆盖到的调用按次数加权的平均墙钟耗时 */
  avg_latency_ms: number;
}

/**
 * 一次汇总读：花销行，以及它们所汇总的窗口范围。
 *
 * 范围必须跟着行一起读：**没有折叠的区间与空闲的区间给出同一个答案（空列表）**。
 * 折叠每整点落一行，所以调用方最先伸手要的那个尾窗恰好就是还没折的那个；
 * 没有端点，读者分不出「没折」与「没花」。
 */
export interface TokenUsage {
  /** 折叠层答出的窗口起点；请求范围内没有折叠窗口时与 `covered_until` 同为 null */
  covered_since: string | null;
  covered_until: string | null;
  actors: ActorUsage[];
}

/**
 * 过去 `hours` 小时各 actor 的令牌花销。
 *
 * 不传 `hours` 用服务端默认；服务端对窗口上限有截断，且回的是**它答出的**
 * 窗口而不是请求的窗口，所以展示以响应的 `covered_*` 为准。
 */
export async function getTokenUsage(hours?: number): Promise<TokenUsage> {
  const query = hours === undefined ? '' : `?hours=${hours}`;
  return api.get<TokenUsage>(`/api/v1/observability/token-usage${query}`);
}

// ── 活跃告警（内存事件 + 耐久表合并） ──

/** 一条正在 firing 的告警。 */
export interface ActiveAlert {
  /** 内存事件与耐久行各自的身份，只在各自的来源里唯一 */
  id: string;
  /** `warning`／`critical`／`info` */
  severity: string;
  /** 规则名（耐久行是 `rule`，内存事件是 `event_type`） */
  event_type: string;
  message: string;
  agent_id: string | null;
  task_id: string | null;
  crew_id: string | null;
  /** 这一条开始 firing 的时间 */
  timestamp: string;
  /**
   * 生产者最后一次确认这条还在 firing 的时间。耐久行才有；内存事件没有，
   * 因为它们是被推送进来的事件、不是被反复求值的条件。
   */
  last_seen_at: string | null;
  resolved: boolean;
  /** `supervisor`（进程内事件）或 `durable`（PostgreSQL 表） */
  source: string;
}

/**
 * 当前活跃告警。
 *
 * `durable_alerts_read` 是这次读取的结局，不能省：`false` 表示耐久那一半
 * 读失败，此时列表只剩进程内事件——**看起来更短，不代表那些告警已经清了**。
 * `null` 表示这个部署根本没有耐久源。
 */
export interface ActiveAlerts {
  alerts: ActiveAlert[];
  count: number;
  durable_alerts_read: boolean | null;
}

export async function getActiveAlerts(): Promise<ActiveAlerts> {
  return api.get<ActiveAlerts>('/api/v1/alerts/active');
}

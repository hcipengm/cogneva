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

  put<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>(path, {
      method: 'PUT',
      body: JSON.stringify(body),
    });
  }

  delete<T>(path: string): Promise<T> {
    return this.request<T>(path, { method: 'DELETE' });
  }

  /**
   * 原样收发字节。
   *
   * 不能走 `request`：那条路会自己写上 `Content-Type: application/json`，而搬
   * 文件要的是字节本身——给一个 PNG 贴上 JSON 的类型，服务端再原样存下来，
   * 盘上的文件头就还是对的，但中间任何一层都可能按声明去解释它。
   */
  async sendBytes<T>(path: string, method: 'POST' | 'PUT', body: Blob): Promise<T> {
    const headers = new Headers();
    headers.set('Content-Type', 'application/octet-stream');
    if (this.token) headers.set('Authorization', `Bearer ${this.token}`);
    const response = await fetch(`${API_BASE_URL}${path}`, { method, body, headers });
    if (!response.ok) {
      const text = await response.text();
      throw { status: response.status, message: text || response.statusText } as ApiError;
    }
    return (await response.json()) as T;
  }

  async getBytes(path: string): Promise<Blob> {
    const headers = new Headers();
    if (this.token) headers.set('Authorization', `Bearer ${this.token}`);
    const response = await fetch(`${API_BASE_URL}${path}`, { headers });
    if (!response.ok) {
      const text = await response.text();
      throw { status: response.status, message: text || response.statusText } as ApiError;
    }
    return response.blob();
  }
}

export const api = new ApiClient();

export interface LoginResponse {
  token: string;
  /**
   * 续期凭据。服务端在登录响应里一直给了它，界面以前直接丢掉——丢掉的代价
   * 是访问令牌 15 分钟一到，事件流每拍重连都被判 `Token expired`，而界面
   * 只把状态条写成 closed，什么都不说。`null` 表示这次登录没有拿到续期凭据
   * （形状不同的入口），那时到期只能登出，不能假装还连着。
   */
  refreshToken: string | null;
  user: {
    id: string;
    username: string;
  };
}

/**
 * 集群概况：**计数，不是节点清单**。
 *
 * 服务端回的是各口径的数与一个健康判词，没有逐节点或逐 agent 的数组。
 * 按节点清单去读，界面上每个字段都是 `undefined`——读的人只会看到一片空白，
 * 分不出「集群里没有节点」与「类型对不上」。
 */
export interface ClusterOverview {
  total_agents: number | null;
  active_agents: number | null;
  total_tasks: number | null;
  active_tasks: number | null;
  queued_tasks: number | null;
  failed_tasks: number | null;
  avg_task_duration_ms: number | null;
  /**
   * 告警面的判词：有东西在 firing 就是 degraded，没有来源答话就是 unknown。
   *
   * 闭集而不是 `string`：界面按这几个词选颜色、决定要不要报一条问题，多写的
   * 那个值不会报错，只会让行画成默认色——读起来正好是「一切正常」。
   */
  cluster_health: 'healthy' | 'degraded' | 'unknown';
  timestamp: string;
  total_squads: number | null;
  active_squads: number | null;
  /**
   * 模型面上游池的判词，由持有凭证的网关按 TTL 发布、这里只读。
   *
   * 为 null 是「没有快照可读」，不是「上游都好着」：写者消失与池子健康在
   * 界面上必须长得不一样。
   */
  llm_pool: LlmPoolView | null;
}

/** 上游池能做什么。只有计数与时刻，没有厂商名：那部分留在持有凭证的网关上。 */
export interface LlmPoolView {
  available: boolean;
  /** 池子里配了几条；0 是「写者没说」，不是「一条都没有」。 */
  pool_size: number;
  /** 运维声明的下限；没有声明就是 null。 */
  min_healthy: number | null;
  unavailable_count: number;
  /** 嫌疑上游声明的最长配额窗（秒）。说了窗长却说不出时刻时，这是唯一的界。 */
  quota_window_secs: number;
  /** 上游自己说的最早恢复时刻。 */
  recovery_at: string | null;
  /** 我方下一次探测上游的时刻。 */
  next_attempt_at: string | null;
  /** 写它的那个进程最后一次看见上游真的答话的时刻。 */
  last_served_at: string | null;
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
    refresh_token?: string;
    user: { id: string; username: string };
  }>('/api/v1/auth/login', { username, password });
  return {
    token: res.access_token,
    refreshToken: res.refresh_token ?? null,
    user: res.user,
  };
}

/**
 * 用续期凭据换一枚新访问令牌。
 *
 * 走请求体而不是登录时那枚 HttpOnly cookie：cookie 写着 `Secure`，而现网入口
 * 只有 80 口（明文 http），浏览器根本不存它；就算存了，服务端也不读它——续期
 * 处理只从 JSON 体里取 `refresh_token`。所以唯一通的这条路是把 body 里那枚
 * 凭据留下来。
 */
export async function refreshSession(refreshToken: string): Promise<string> {
  const res = await api.post<{ access_token: string }>(
    '/api/v1/auth/refresh',
    { refresh_token: refreshToken }
  );
  return res.access_token;
}

// ── Platform-account login (connect GitHub/Gitee = sign in) ──

export type PlatformProvider = 'github' | 'gitee';

export interface PlatformAuthPayload {
  auth: {
    access_token: string;
    refresh_token?: string;
    user: { id: string; username: string };
  };
  account_created: boolean;
}

function toLoginResponse(p: PlatformAuthPayload): LoginResponse {
  return {
    token: p.auth.access_token,
    refreshToken: p.auth.refresh_token ?? null,
    user: p.auth.user,
  };
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

/** 一段对话里的一行，读回来的形状。 */
export interface ChatTranscriptLine {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  /** 这一轮没有答案的原因。有值就是「问过，但没答上来」。 */
  failure: string | null;
  /**
   * 这一轮交出去的那道目标。有值＝这段对话是常驻的（活交给编排器了，关掉页面
   * 它还在跑）；空＝问答，回复只走当前这条连接。
   */
  goal_id?: string | null;
  created_at: string;
}

/**
 * 读回一段对话说过的话。
 *
 * 这是「关掉页面再打开，那段对话还在」的那条腿。不带 `failure` 的那个选择
 * 会让没答上来的那一轮凭空消失——读者会以为那句话从没说过。
 */
export async function getSessionMessages(
  sessionId: string
): Promise<ChatTranscriptLine[]> {
  const res = await api.get<{
    code: string;
    data?: { items?: ChatTranscriptLine[] };
    message?: string;
  }>(`/api/v1/sessions/${encodeURIComponent(sessionId)}/messages`);
  return res.data?.items ?? [];
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

// ── 上网检索与抓取的后备池 ──

/**
 * 池里的一家。`has_key` 是服务器愿意说的全部——凭证从不回显，所以页面上
 * 「已配置」与「未配置」只能从这一格读。
 */
export interface SearchUpstream {
  id: string;
  /** tavily / exa / qianfan */
  shape: string;
  base_url: string;
  /** search / fetch / both */
  capability: string;
  has_key: boolean;
}

export interface SearchFallbackStatus {
  configured: boolean;
  upstreams: SearchUpstream[];
  /** 跟着现网在用的 LLM 走的那两层：只展示，不可编辑（同一把 key，同一次轮换） */
  from_llm: SearchUpstream[];
  read_error: string | null;
  shapes: string[];
  capabilities: string[];
}

export interface SearchFallbackEntryPayload {
  id?: string;
  shape: string;
  base_url: string;
  /**
   * 省略 = 保留这一条原来那把；空串 = 明确改成不带凭证。
   * 这条区分就是「页面不回声凭证」能成立的原因。
   */
  api_key?: string;
  capability?: string;
}

export interface SearchFallbackResult {
  ok: boolean;
  restarted: string[];
  upstreams: number;
  from_llm: number;
  message: string;
}

export async function getSearchFallback(): Promise<SearchFallbackStatus> {
  return api.get<SearchFallbackStatus>('/api/v1/admin/search-fallback');
}

export async function saveSearchFallback(
  upstreams: SearchFallbackEntryPayload[]
): Promise<SearchFallbackResult> {
  return api.post<SearchFallbackResult>('/api/v1/admin/search-fallback', {
    upstreams,
  });
}

// ── 代码平台与凭据（GitHub / Gitee 令牌、SSH 身份） ──

/**
 * 网关的 git 身份。
 *
 * `state` 只认三个值加一个 unknown：`ready` 是「密钥在、状态位也是 ready、两半
 * 对得上」；有私钥却没有状态位是 `pending`——**未确认**，不是已生效；读不到
 * Secret 的进程（集群外面板）只能报 `unknown`，不能替网关说「没配」。
 *
 * 公钥与指纹是公开材料，回读没有风险；私钥与令牌从不回传，所以这里没有它们的
 * 字段——缺字段就是这条规则，不是漏写。
 */
export interface GitIdentityStatus {
  state: 'ready' | 'pending' | 'absent' | 'unknown';
  public_key: string | null;
  fingerprint: string | null;
  token_present?: boolean;
  repo_configured?: boolean;
  needs_token?: boolean;
  note: string | null;
}

export interface ContributionStatus {
  configured: boolean;
  provider: 'github' | 'gitee' | 'none';
  github?: { configured: boolean };
  gitee?: { configured: boolean };
  ssh?: { key_present: boolean };
  git_identity: GitIdentityStatus;
  github_app_configured: boolean;
  policy: string;
  pending_count: number;
  /** GitHub 设备码通道可用（有 OAuth App client_id） */
  device_flow_available: boolean;
  /** Gitee 授权码通道可用（有应用 client_id 与 client_secret） */
  gitee_oauth_available: boolean;
  gitee_oauth_secret_configured: boolean;
  github_oauth_secret_configured: boolean;
  note?: string;
}

export interface ContributionConnectResult {
  ok: boolean;
  status: string;
  provider?: string;
  account?: string;
  ssh?: string;
  message: string;
}

/** GitHub 设备码流程的第一步：把 user_code 给人，让他在验证页上填。 */
export interface DeviceStart {
  client_id: string;
  device_code: string;
  user_code: string;
  verification_uri: string;
  expires_in: number;
  interval: number;
}

/**
 * 轮询的四种结局。`pending`／`slow_down` 是「人还没在浏览器里点完」，`error` 是
 * 平台自己给的判词（过期、被拒绝），`connected` 是唯一可以停下的那一种。
 */
export type DevicePollResult =
  | { status: 'pending'; interval?: number }
  | { status: 'slow_down'; interval: number }
  | { status: 'error'; message: string }
  | { status: 'connected'; ok?: boolean; account?: string; ssh?: string; message?: string };

export interface GiteeOAuthStart {
  authorize_url: string;
  state: string;
  redirect_uri: string;
}

export async function getContributionStatus(): Promise<ContributionStatus> {
  return api.get<ContributionStatus>('/api/v1/admin/contribution-status');
}

export async function startGithubDevice(): Promise<DeviceStart> {
  return api.post<DeviceStart>('/api/v1/admin/contribution/device/start', {});
}

export async function pollGithubDevice(
  client_id: string,
  device_code: string
): Promise<DevicePollResult> {
  return api.post<DevicePollResult>('/api/v1/admin/contribution/device/poll', {
    client_id,
    device_code,
  });
}

export async function startGiteeOAuth(redirect_origin: string): Promise<GiteeOAuthStart> {
  return api.post<GiteeOAuthStart>('/api/v1/admin/contribution/gitee/oauth/start', {
    redirect_origin,
  });
}

/**
 * 收尾授权码通道。`codeOrUrl` 既可以是裸码，也可以是授权后浏览器地址栏里那一整条
 * 网址——用户复制哪一种都该能用，判断交服务端。
 */
export async function exchangeGiteeOAuth(
  state: string,
  codeOrUrl: string
): Promise<ContributionConnectResult> {
  const looksLikeUrl = /^https?:\/\//i.test(codeOrUrl.trim());
  return api.post<ContributionConnectResult>(
    '/api/v1/admin/contribution/gitee/oauth/exchange',
    { state, ...(looksLikeUrl ? { redirect_url: codeOrUrl.trim() } : { code: codeOrUrl.trim() }) }
  );
}

/** 手动令牌通道：GitHub 没配 OAuth App、或用户手里已经有 PAT 时用。 */
export async function connectContribution(
  provider: 'github' | 'gitee',
  token: string
): Promise<ContributionConnectResult> {
  return api.post<ContributionConnectResult>('/api/v1/admin/contribution-config', {
    provider,
    token,
  });
}

export async function disconnectContribution(): Promise<ContributionConnectResult> {
  return api.post<ContributionConnectResult>('/api/v1/admin/contribution/disconnect', {});
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

/**
 * 折叠账的一个窗口里的一行：某个 `(执行者, 来源, 上游, 协议)` 在那个整点
 * 窗口上花了多少。
 *
 * 与 `ActorUsage` 同轴不同粒度：那一行是整个区间加起来的，这一行只算一个
 * 窗口。把窗口加起来得到前者，而前者抹掉的正是尖峰。
 */
export interface WindowSpend {
  window_start: string;
  window_end: string;
  actor: string;
  origin: string;
  upstream: string;
  api_style: string;
  calls: number;
  failed_calls: number;
  tokens_input: number;
  tokens_output: number;
  tokens_cached: number;
}

/** 按窗口分行的折叠账，连同它覆盖到的区间（同 `TokenUsage` 的端点语义）。 */
export interface UsageSeries {
  covered_since: string | null;
  covered_until: string | null;
  /** 一个窗口 × 一个执行者组合一行，不在这里相加。 */
  rows: WindowSpend[];
}

/** 过去 `hours` 小时按窗口分行的令牌花销。端点语义与 `getTokenUsage` 相同。 */
export async function getTokenUsageSeries(hours?: number): Promise<UsageSeries> {
  const query = hours === undefined ? '' : `?hours=${hours}`;
  return api.get<UsageSeries>(`/api/v1/observability/token-usage/series${query}`);
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
  /**
   * 有人点过「确认」的时间，以及是谁。只有耐久行有；进程内事件背后没有行，
   * 没有可以写确认的地方。告警重新 firing 时会被清掉——确认只覆盖一次发作。
   */
  acknowledged_at: string | null;
  acknowledged_by: string | null;
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

/**
 * 确认一条正在 firing 的告警已被人看见。
 *
 * `id` 是列表里给的 `dedup_key`。网关对已经清掉的那一条回 404——它不写一个
 * 下次 firing 会复用的旧标记，所以 404 是「这条已经不用确认了」，不是重试。
 */
export async function acknowledgeAlert(
  id: string,
): Promise<{ acknowledged_at: string; acknowledged_by: string }> {
  const res = await api.post<{
    acknowledged_at: string;
    acknowledged_by: string;
  }>(`/api/v1/alerts/${encodeURIComponent(id)}/ack`, {});
  return res;
}

// ── 通知中心（站内通信的收端） ──

/** 一条通知。字段名与网关的列表端点一致，`ntype` 的值是 snake_case 字符串。 */
export interface NotificationItem {
  id: string;
  title: string;
  body: string;
  is_read: boolean;
  created_at: string;
  read_at: string | null;
}

export interface NotificationPage {
  items: NotificationItem[];
  unread_count: number;
  next_cursor: string | null;
  has_more: boolean;
}

/**
 * 当前用户的通知。
 *
 * 列表端点在未配置通知存储时也回 200（空列表），所以「空」在这里分不出
 * 「没人发过」与「这个部署没接存储」——要分就看得失配的 `unread_count` 与
 * WebSocket 推送，而不是靠列表长度猜。
 */
export async function listNotifications(
  unreadOnly = false
): Promise<NotificationPage> {
  const query = unreadOnly ? '?unread_only=true' : '';
  const res = await api.get<{ code: string; data: NotificationPage }>(
    `/api/v1/notifications${query}`
  );
  return res.data;
}

export async function markNotificationRead(id: string): Promise<void> {
  await api.post(
    `/api/v1/notifications/${encodeURIComponent(id)}/read`,
    {}
  );
}

export async function markAllNotificationsRead(): Promise<void> {
  await api.post('/api/v1/notifications/read-all', {});
}

// ---- 和每个 agent 的会话 -------------------------------------------------
//
// 名单、对话、发送三件事都走 `/api/v1/agents`：名单是「谁能被叫到」，对话是
// 「之前跟它说过什么、它回了什么」，发送是「现在说一句」。对话在服务端按发送
// 与接收两侧都算进同一个会话，所以这里不需要分开拉两份。

export interface AgentSummary {
  agent_id: string;
  role: string;
  workspace_id: string;
  hostname: string;
  pod_ip: string;
  capabilities: string[];
  last_heartbeat: string;
}

export async function listAgents(): Promise<AgentSummary[]> {
  const page = await api.get<{ agents: AgentSummary[] }>('/api/v1/agents');
  return page.agents ?? [];
}

export interface AgentConversationLine {
  id: number;
  from_unit: string;
  to_unit: string;
  kind: string;
  body: string;
  /** 结局。闭集：存储层只写这几个词，面板按它选那一行的话与颜色。 */
  outcome: 'queued' | 'replied' | 'timed_out' | 'refused' | 'failed';
  reply: string | null;
  outcome_detail: string | null;
  created_at: string;
}

export async function getAgentConversation(
  agentId: string,
  limit = 200
): Promise<AgentConversationLine[]> {
  const page = await api.get<{ agent_id: string; messages: AgentConversationLine[] }>(
    `/api/v1/agents/${encodeURIComponent(agentId)}/messages?limit=${limit}`
  );
  return page.messages ?? [];
}

/**
 * 一句发出去的话，以及它落定的形状。
 *
 * 不以抛错表示「没答上来」：等待超时、流断开、名字没人应答，都是这次发送的
 * 结局，而不是这次调用失败——面板要把结局写在那一行上，而不是弹一个错误框
 * 让人以为话没发出去。
 */
export interface AgentSendOutcome {
  /** 话送出去了、没有等回答（202）。 */
  queued: boolean;
  replied: boolean;
  reply: string | null;
  error: string | null;
}

export async function sendAgentPrompt(
  agentId: string,
  prompt: string,
  waitSecs?: number
): Promise<AgentSendOutcome> {
  const query = waitSecs ? `?wait_secs=${waitSecs}` : '';
  try {
    const body = await api.post<{ replied?: boolean; reply?: unknown; queued?: boolean }>(
      `/api/v1/agents/${encodeURIComponent(agentId)}/messages${query}`,
      { type: 'prompt', goal: prompt }
    );
    return {
      queued: !!body.queued,
      replied: !!body.replied,
      reply: body.reply === undefined ? null : String(body.reply),
      error: null,
    };
  } catch (e) {
    const err = e as { status?: number; message?: string };
    let message = err?.message?.trim() || `发送失败（HTTP ${err?.status ?? '?'}）`;
    try {
      const parsed = JSON.parse(message) as { error?: string };
      if (parsed.error) message = parsed.error;
    } catch {
      // 不是 JSON 就把原文留着：原文本身也是读数。
    }
    return {
      queued: false,
      replied: false,
      reply: null,
      error: message,
    };
  }
}

// ─── 系统面：面与读数、任务、后端健康、评估集 ───
//
// 每个面都读网关现有的路由，不另造一份数据：读数在别处被门禁与常驻 loop 用着，
// 界面若从第二条路重算一次，就会在某一刻与门禁看到的不是同一个数。

export interface GovernanceFaceSpec {
  display_name: string;
  /** 面还是横切线。闭集：普查把这两类分开数，混成一个总数会掩盖「丢了一个面、多了一条线」。 */
  kind: 'face' | 'cross';
  deputy_unit: string | null;
  witness_unit: string | null;
  time_share: string | null;
  vacancy_until: string | null;
  vacancy_assigner: string | null;
  gap_task_id: string | null;
  first_reading: string | null;
  entry_path: string | null;
  revision: number;
}

export interface GovernanceFace {
  face_id: string;
  owner_unit: string | null;
  bound_by_handover_id: string | null;
  spec: GovernanceFaceSpec;
  updated_at: string;
}

export interface GovernanceCensus {
  faces: number;
  cross: number;
  unstaffed: number;
  expired: number;
}

export interface GovernanceFacesPage {
  generated_at: string;
  census: GovernanceCensus;
  faces: GovernanceFace[];
}

export interface FaceDriftReading {
  face_id: string;
  /**
   * 这一轮的判词。闭集：面板按它选颜色，红／黄／灰／通过四档由 `DriftState`
   * 自己判定，界面不另立一套说法。
   */
  state:
    | 'on_target'
    | 'owner_idle'
    | 'drifted_binding'
    | 'no_owner'
    | 'vacancy_expired'
    | 'unsigned_handover'
    | 'witness_is_owner'
    | 'deputy_overlap'
    | 'unread';
  owner: string | null;
  /** 这一格的所有者是怎么来的。闭集：面板只给这两种各一句话。 */
  owner_source: 'declared' | 'measured';
  red_rules: string[];
  yellow_rules: string[];
  cause: string | null;
  detail: string | null;
  observed_at: string;
  source_revision: number;
}

export interface GovernanceDriftPage {
  generated_at: string;
  readings: FaceDriftReading[];
}

export async function getGovernanceFaces(): Promise<GovernanceFacesPage> {
  return api.get<GovernanceFacesPage>('/api/v1/governance/faces');
}

export async function getGovernanceDrift(): Promise<GovernanceDriftPage> {
  return api.get<GovernanceDriftPage>('/api/v1/governance/drift');
}

export interface TaskSummaryCounts {
  total: number;
  ready: number;
  running: number;
  completed: number;
  failed: number;
  all_completed: boolean;
}

export interface ProcessTask {
  id: string;
  task_type: string;
  status: string;
  agent_id: string | null;
  blocked_by: string[];
  retry_count: number;
  max_retries: number;
  error: string | null;
  created_at: string;
  started_at: string | null;
}

export interface GoalGroup {
  goal_id: string;
  total: number;
  completed: number;
  failed: number;
  running: number;
  pending: number;
  progress_pct: number;
  tasks: ProcessTask[];
}

export interface CrewProgress {
  crew_id: string;
  agent_ids: string[];
  total_tasks: number;
  completed_tasks: number;
  failed_tasks: number;
  progress_pct: number;
  crew_retry_count: number;
  updated_at: string;
}

export interface AgentLoad {
  agent_id: string;
  role: string;
  capabilities: string[];
  /** healthy / suspect / stuck / dead / unknown（没有监管者判词）。 */
  health: string;
  running_tasks: number;
  load_score: number | null;
  last_heartbeat: string;
}

export interface ProcessOverview {
  timestamp: string;
  goals: GoalGroup[];
  crews: CrewProgress[];
  agents: AgentLoad[];
  health: { healthy: number; suspect: number; stuck: number; dead: number };
  dlq_len: number;
}

export async function getProcessOverview(): Promise<ProcessOverview> {
  return api.get<ProcessOverview>('/api/v1/processes');
}

export async function getTaskSummary(): Promise<TaskSummaryCounts> {
  return api.get<TaskSummaryCounts>('/api/v1/tasks/status');
}

export interface TaskRow {
  id: string;
  task_type: string;
  status: string;
  owner_unit: string | null;
  agent_id: string | null;
  goal_id: string | null;
  error: string | null;
  created_at: string;
  updated_at: string;
  input: Record<string, unknown>;
  /** 依赖：这条任务要等谁。 */
  blocked_by?: string[];
  /** 解锁：这条任务完成后放开谁。与 `blocked_by` 是同一条边的两个方向。 */
  blocks?: string[];
  /** 失败原因的类型（传输层信号），与给人看的 `error` 文本分开。 */
  error_cause?: string | null;
  lease_owner?: string | null;
  lease_expires_at?: string | null;
  retry_not_before?: string | null;
  started_at?: string | null;
  timeout_seconds?: number;
  retry_count?: number;
  max_retries?: number;
  priority?: number;
  parent_task_id?: string | null;
  sink_task_id?: string | null;
  face_id?: string | null;
  bound_by_handover_id?: string | null;
  is_executable?: boolean;
  result?: Record<string, unknown> | null;
}

export interface TaskGraph {
  nodes: TaskRow[];
  /** 依赖边 `[先, 后]`：两端都在这张图里，悬边已被服务端收掉。 */
  edges: [string, string][];
}

/** 任务图。过滤条件与列表相同；边只连两端都留下来的节点。 */
export async function getTaskGraph(
  params: { status?: string; goal_id?: string; agent_id?: string } = {}
): Promise<TaskGraph> {
  const query = new URLSearchParams();
  if (params.status) query.set('status', params.status);
  if (params.goal_id) query.set('goal_id', params.goal_id);
  if (params.agent_id) query.set('agent_id', params.agent_id);
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return api.get<TaskGraph>(`/api/v1/tasks/graph${suffix}`);
}

/** 一份读数从哪来：读的哪张表、哪个读者、哪一刻读的。 */
export interface ReadingSource {
  table: string;
  reader: string;
  read_at: string;
}

export interface ProjectStatusCount {
  status: string;
  count: number;
}

export interface ProjectRow {
  id: string;
  /** `goal` 或 `ungrouped`。后者是没有 goal 的任务的具名桶，不是一个真项目。 */
  kind: 'goal' | 'ungrouped';
  tasks: number;
  /** 可被调度的任务数。与 `tasks` 的差是只做层级与查询的占位行。 */
  executables: number;
  by_status: ProjectStatusCount[];
  blocked: number;
  first_created_at: string | null;
  last_updated_at: string | null;
  /** 有没有计划基线。基线表还没落地，今天是恒 false——读作「排期这一件还读不到」。 */
  planned: boolean;
}

export interface ProjectsReading {
  projects: ProjectRow[];
  /** 本页各行 `tasks` 之和。与 `projects` 逐行对得上，是这一页自己的账。 */
  counted_tasks: number;
  /** 读者可见的任务总数，**不随分页变化**。 */
  total_tasks: number;
  /** 读者可见的项目总数，不随分页变化。 */
  total_projects: number;
  /** 下一页的位置；`null` 表示这一页就是最后一页。 */
  next_cursor: string | null;
  source: ReadingSource;
}

/**
 * 项目清单：任务图按 goal 根折出来的行。
 *
 * 这是一个分页的读：一个 goal 一行，项目数没有上界。界面必须同时读 `total_projects`
 * 与本页长度——只印本页长度，读者会把「读了 200 个」读成「一共有 200 个」。
 */
export async function getProjects(params: {
  limit?: number;
  cursor?: string | null;
} = {}): Promise<ProjectsReading> {
  const query = new URLSearchParams();
  if (params.limit) query.set('limit', String(params.limit));
  if (params.cursor) query.set('cursor', params.cursor);
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return api.get<ProjectsReading>(`/api/v1/projects${suffix}`);
}

export interface BoardCard {
  id: string;
  title: string;
  /** 它现在在哪一列。 */
  status: string;
  parent_task_id: string | null;
  /** 依赖还没全部完成。依赖不在可见的这一批里时也算卡住。 */
  blocked: boolean;
  /** 有没有验收标准。没有的工作包写不了计划基线。 */
  has_acceptance: boolean;
}

export interface BoardColumn {
  status: string;
  count: number;
  /**
   * 这一列的通行上限。`null` 是「没配」，不是「不限」也不是「没超」——界面必须
   * 把这三件事分开写，否则一个不拦派发的列会看起来像在拦。
   */
  wip_limit: number | null;
  over_limit: boolean | null;
  /** 这一列带回来的卡片，最近动过的在前。长度与本列总数未必相等。 */
  cards: BoardCard[];
  /** 因为超过每列上限而没带回来的张数。0 表示这一列的张数就是全部。 */
  cards_hidden: number;
}

/**
 * 这一台进程的列上限是从哪来的。
 *
 * 六列的 `wip_limit` 全是 `null` 时，看板分不出「这台按设计不限流」与「这台上限
 * 没接上」——两者对读者的下一步正相反。所以服务端把变量的原文带回来，让「没声明」
 * 自己说出来，而不是留一格空白让人猜。
 */
export interface BoardWipLimitsReading {
  /** 变量名。读者要拿它去查自己的部署。 */
  variable: string;
  /** 这一台读到的原文；没设这个变量时是 `null`。 */
  declared: string | null;
  /** 声明了却用不上的条目。它们不出现在任何一列上，不点名就等于「配了没人知道」。 */
  refused: string[];
  /** 一句话讲清现状。 */
  basis: string;
}

export interface ProjectBoardReading {
  project_id: string;
  columns: BoardColumn[];
  total: number;
  wip_limits: BoardWipLimitsReading;
  source: ReadingSource;
}

export async function getProjectBoard(id: string): Promise<ProjectBoardReading> {
  return api.get<ProjectBoardReading>(`/api/v1/projects/${encodeURIComponent(id)}/board`);
}

export interface GanttRow {
  id: string;
  title: string;
  status: string;
  created_at: string;
  started_at: string | null;
  updated_at: string;
  /** 计划基线。两头都是 `null` 时 `unplanned` 为真，不许画成零长度条。 */
  planned_start: string | null;
  planned_end: string | null;
  unplanned: boolean;
}

export interface ProjectGanttReading {
  project_id: string;
  has_plan: boolean;
  /**
   * 读不到计划基线时的原因；读到了是 `null`。
   *
   * 它必须与行一起读：读不到时每行的 `unplanned` 也是真，与「确实没排期」同形。
   * 先看这里，再说行。
   */
  plan_error: string | null;
  rows: GanttRow[];
  source: ReadingSource;
}

export async function getProjectGantt(id: string): Promise<ProjectGanttReading> {
  return api.get<ProjectGanttReading>(`/api/v1/projects/${encodeURIComponent(id)}/gantt`);
}

/**
 * 挣值的一个量。`value` 与 `unmeasurable` 互斥：量不出来时 `value` 是 `null`
 * 而 `unmeasurable` 说明为什么。**不要**把 `null` 当 0 显示——计划价值为 0 是
 * 还没开工、成本为 0 是一分没花，两者都与「算不出来」差得很远。
 */
export interface EvmReading {
  value: number | null;
  unmeasurable: string | null;
  basis: string;
}

/**
 * 每一格都是 `number | null`：`null` 是「这一格没量过」，不是零。
 */
export interface EvmAttribution {
  tasks_with_cost: number | null;
  baselined_tasks_without_cost: number | null;
  /** 项目里没有基线的任务条数：它们的预算不在挣值里。 */
  tasks_outside_baseline: number | null;
}

/** 一条超支的节点。预算与花销一起给：只给差额，读者还得自己去找那两行。 */
export interface OverrunRow {
  task_id: string;
  title: string;
  budget_tokens: number;
  spent_tokens: number;
}

/** 一条过了计划止期还没跑完的节点。 */
export interface BehindRow {
  task_id: string;
  title: string;
  status: string;
  planned_end: string;
  /** 过了止期多久：差三分钟与差三天是两件事。 */
  overdue_secs: number;
}

export interface ProjectEvmReading {
  project_id: string;
  unit: string;
  planned_value: EvmReading;
  earned_value: EvmReading;
  /** 整个项目的花销。它不是成本绩效的分母。 */
  actual_cost: EvmReading;
  /** 有基线任务的花销，与挣值覆盖同一批任务——成本绩效的分母。 */
  baselined_actual_cost: EvmReading;
  cost_performance_index: number | null;
  schedule_performance_index: number | null;
  /**
   * 同一窗口里没有任务身份的用量。它不属于任何项目，所以不进实际成本；
   * 也不折成 0，那样会把归因缺口读成省钱。只有数、没有比例——这条账没有
   * 项目维度，拿它比本项目的花销会得到反向比例。
   */
  unattributed_tokens: number | null;
  /** 上面那个数的窗口起点；终点是 `source.read_at`。 */
  unattributed_window_start: string | null;
  attribution: EvmAttribution;
  /**
   * 超支的节点。`null` 是「量不了」（基线或逐笔账读不到），空数组才是
   * 「一条也没超」——两者印成同一句，读者会以为系统查过了。
   */
  overruns: OverrunRow[] | null;
  /** 过了计划止期还没跑完的节点。`null` 同上。 */
  behind_schedule: BehindRow[] | null;
  /** 算这三个量用的假设，明写在响应里。 */
  assumption: string;
  source: ReadingSource;
}

export async function getProjectEvm(id: string): Promise<ProjectEvmReading> {
  return api.get<ProjectEvmReading>(`/api/v1/projects/${encodeURIComponent(id)}/evm`);
}

// ─── 项目面的写 ───────────────────────────────────────────────────────────
//
// 读面能把这些数画出来，写的入口也得在同一个界面上：一个只能看的项目管理面，
// 客户看到的第一件事就是「这棵树拆错了，而我没有地方改」。四个写端点各管一件事，
// 六个列名、四种拒绝码都在下面写清楚——调用方按 `code` 分支，不按句子。

/** 新建一个项目：它就是一整棵 goal 树的根，不触发分解。 */
export interface CreateProjectInput {
  name: string;
  /** 不填时只在调用方恰好属于一个 workspace 时才继承。 */
  workspace_id?: string;
}

export interface CreateProjectResult {
  id: string;
  name: string;
  workspace_id: string;
  source: ReadingSource;
}

export async function createProject(input: CreateProjectInput): Promise<CreateProjectResult> {
  return api.post<CreateProjectResult>('/api/v1/projects', input);
}

export interface CreateProjectTaskInput {
  title: string;
  /** 不给就是项目的顶层。 */
  parent_task_id?: string | null;
  acceptance?: string[];
  blocked_by?: string[];
  workspace_id?: string;
}

export interface CreateProjectTaskResult {
  id: string;
  project_id: string;
  parent_task_id: string | null;
  status: string;
  source: ReadingSource;
}

export async function createProjectTask(
  projectId: string,
  input: CreateProjectTaskInput
): Promise<CreateProjectTaskResult> {
  return api.post<CreateProjectTaskResult>(
    `/api/v1/projects/${encodeURIComponent(projectId)}/tasks`,
    input
  );
}

/**
 * 改一条任务的范围结构字段。
 *
 * `parent_task_id` 是三态，不是两态：字段缺席＝这次不改，`null`＝提到顶层，
 * 字符串＝挂到那个父节点下。压成两态就没有「提到顶层」这个动作。
 */
export interface UpdateTaskInput {
  parent_task_id?: string | null;
  title?: string;
  acceptance?: string[];
}

export interface UpdateTaskResult {
  id: string;
  parent_task_id: string | null;
  title: string;
  acceptance: string[];
}

export async function updateTask(id: string, input: UpdateTaskInput): Promise<UpdateTaskResult> {
  return api.request<UpdateTaskResult>(`/api/v1/tasks/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    body: JSON.stringify(input),
  });
}

/** 写一条计划基线。没有验收标准的工作包会被拒——它的「做完」没有判据。 */
export interface SaveTaskPlanInput {
  planned_start?: string | null;
  planned_end?: string | null;
  /** 这一版打算花多少 token。缺席时沿用上一版；连上一版都没有就被拒。 */
  budget_tokens?: number | null;
}

export async function saveTaskPlan(
  taskId: string,
  input: SaveTaskPlanInput
): Promise<Record<string, unknown>> {
  return api.put<Record<string, unknown>>(
    `/api/v1/tasks/${encodeURIComponent(taskId)}/plan`,
    input
  );
}

/** 看板上的六个列名，与任务状态的六个拼写是同一套词。 */
export const BOARD_COLUMNS = [
  'pending',
  'scheduled',
  'running',
  'completed',
  'failed',
  'cancelled',
] as const;

export type BoardColumnId = (typeof BOARD_COLUMNS)[number];

/** 挪一张卡。目标列不相邻、或那一列满员时被拒，拒因带稳定码。 */
export async function moveTask(
  taskId: string,
  status: BoardColumnId
): Promise<Record<string, unknown>> {
  return api.post<Record<string, unknown>>(`/api/v1/tasks/${encodeURIComponent(taskId)}/move`, {
    status,
  });
}

/** 项目时间线上的一行：谁、什么时候、把哪个资源改成了什么。 */
export interface ProjectAuditRow {
  id: number;
  actor: string;
  action: string;
  resource_type: string;
  resource_id: string;
  before: unknown;
  after: unknown;
  created_at: string;
}

export interface ProjectAuditReading {
  project_id: string;
  count: number;
  rows: ProjectAuditRow[];
  source: ReadingSource;
}

export async function getProjectAudit(
  projectId: string,
  limit = 50
): Promise<ProjectAuditReading> {
  return api.get<ProjectAuditReading>(
    `/api/v1/projects/${encodeURIComponent(projectId)}/audit?limit=${limit}`
  );
}

/**
 * 四个视图的 CSV 导出。
 *
 * 先取字节再交给浏览器另存：`<a href>` 那条路不带 Authorization，导出的会是
 * 一次 401，而 401 被浏览器当成下载存成一个文件，读者打开才发现里面是一段错误。
 * 文件名由这里给，与服务端 `Content-Disposition` 里那个同一套拼法。
 */
export async function downloadProjectCsv(
  projectId: string,
  view: 'wbs' | 'board' | 'gantt' | 'evm'
): Promise<void> {
  const blob = await api.getBytes(
    `/api/v1/projects/${encodeURIComponent(projectId)}/export.csv?view=${view}`
  );
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `${projectId}-${view}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  // 立刻 revoke 会让一部分浏览器的另存对话框拿到失效的 URL，表现是 0 字节。
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

/**
 * 任务行。过滤条件只有服务端认的三个：`goal_id`、`agent_id`、`status`。
 *
 * `limit` 在本地截：端点不认它，发过去也会被忽略——带着一个看着生效、实际
 * 没生效的参数，读者会以为看到的是「最近 50 条」，其实是全部。
 */
export async function listTasks(params: {
  status?: string;
  goal_id?: string;
  agent_id?: string;
  limit?: number;
} = {}): Promise<TaskRow[]> {
  const query = new URLSearchParams();
  if (params.status) query.set('status', params.status);
  if (params.goal_id) query.set('goal_id', params.goal_id);
  if (params.agent_id) query.set('agent_id', params.agent_id);
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const rows = await api.get<TaskRow[]>(`/api/v1/tasks/list${suffix}`);
  return params.limit ? rows.slice(0, params.limit) : rows;
}

/** 一条交接边：`source` 的结局引出了 `target`。 */
export type CollaborationLinkType = 'hand_off' | 'retry' | 'dead_letter' | 'respawn';

export interface CollaborationLink {
  source_task_id: string;
  target_task_id: string;
  link_type: CollaborationLinkType;
  agent_id?: string;
  timestamp: string;
}

export interface CollaborationStats {
  total_links: number;
  handoffs: number;
  retries: number;
  dead_letters: number;
  respawns: number;
}

/**
 * 智能体之间的交接图。
 *
 * 它由网关里那条任务事件监听器喂出来，只存在那个进程的内存里：重启之后从空开始。
 * 空的图不读作「没有过交接」，读作「这个进程还没见过一次完成或失败」。
 */
export interface CollaborationGraph {
  links: CollaborationLink[];
  stats: CollaborationStats;
}

export interface TaskCollaboration {
  task_id: string;
  upstream: CollaborationLink[];
  downstream: CollaborationLink[];
}

export async function getCollaborationGraph(): Promise<CollaborationGraph> {
  return api.get<CollaborationGraph>('/api/v1/collaboration/graph');
}

/** 一个任务的上游与下游。读的是同一张内存图，所以它的空与整图的空是同一件事。 */
export async function getTaskCollaboration(taskId: string): Promise<TaskCollaboration> {
  return api.get<TaskCollaboration>(
    `/api/v1/collaboration/links/${encodeURIComponent(taskId)}`
  );
}

/** 一份被采集下来的执行轨迹的元数据。 */
export interface TraceMeta {
  trace_id: string;
  agent_id: string;
  task_id: string;
  event_count: number;
  created_at: string;
  tier: string;
}

export async function listTraces(limit = 50): Promise<TraceMeta[]> {
  const page = await api.get<{ traces: TraceMeta[] }>(`/api/v1/traces?limit=${limit}`);
  return page.traces ?? [];
}

/**
 * 一份轨迹里的一帧。
 *
 * 服务端把变体的字段平铺在 `type` 旁边，没有第二层信封——所以这里除 `type` 与
 * `timestamp` 之外的一切都属于这一帧自己。
 */
export interface WireFrame {
  type: string;
  timestamp: string;
  [field: string]: unknown;
}

export interface AgentTrace {
  trace_id: string;
  session_id?: string | null;
  task_id: string;
  agent_id: string;
  created_at: string;
  event_count: number;
  byte_size: number;
  version: string;
  tier: string;
  compression: number;
  checksum: string;
  events: WireFrame[];
  llm_requests?: unknown[];
  llm_responses?: unknown[];
  tool_calls?: unknown[];
}

export async function getTrace(traceId: string): Promise<AgentTrace> {
  return api.get<AgentTrace>(`/api/v1/traces/${encodeURIComponent(traceId)}`);
}

/**
 * 每一行是一次探测的结论。`status` 是网关 `BackendState` 声明的那几个词，
 * 面板按它选颜色，所以这里是闭集：多写一个 `| string` 就等于说「随便什么词
 * 我都收」，而收下之后落进的那一支正是「没探出结论」那一支。
 */
export interface BackendHealth {
  name: string;
  status: 'up' | 'down' | 'degraded' | 'unknown';
  latency_ms: number | null;
  last_checked: string | null;
  error: string | null;
}

export interface BackendHealthMatrix {
  overall: 'up' | 'down' | 'degraded' | 'unknown';
  backends: BackendHealth[];
}

export async function getBackendHealth(): Promise<BackendHealthMatrix> {
  return api.get<BackendHealthMatrix>('/api/v1/health/backends');
}

/**
 * 六条观测读数。`value` 为空是「问不到 / 没有样本」，与读到 0 不是一回事；
 * `dashboard_uid` 为空是「这条读数没有面板」，界面据此说明为什么点不过去。
 */
export interface ObservabilityReading {
  id: string;
  title: string;
  query: string;
  unit: string;
  /** 阈值为空＝这条读数没有阈值，只能报值、不能报警。 */
  warn: number | null;
  crit: number | null;
  higher_is_worse: boolean;
  series: string;
  produced_by: string;
  dashboard_uid: string | null;
  value: number | null;
  /**
   * 判词。闭集：`| string` 会把联合类型变成一句装饰——加了它，任何词都合法，
   * 拼错的比较不会被类型检查拦下，落进色表与中文名的兜底分支，画成中性色。
   */
  verdict: 'ok' | 'warning' | 'critical' | 'unknown';
  unavailable_because?: string;
}

export interface ObservabilityReadings {
  prometheus: string;
  readings: ObservabilityReading[];
}

export async function getObservabilityReadings(): Promise<ObservabilityReadings> {
  return api.get<ObservabilityReadings>('/api/v1/observability/readings');
}

/**
 * 监管者当前一轮健康巡检的结论。
 *
 * 四个数分开给而不是一个「健康度」：健康、失联、可疑、卡住是四种不同的
 * 处置，合成一个数之后，读的人分不出该重启谁。
 */
export interface SupervisorStatus {
  cycle: number;
  healthy_agents: number;
  dead_agents: number;
  suspect_agents: number;
  stuck_agents: number;
  pending_handoffs: number;
  scheduler_paused: boolean;
  /** 上一次真的有 rebalance 计划发出去的时间；从没发过是 `null`。 */
  last_plan_emitted_at: string | null;
  timestamp: string;
}

export async function getSupervisorStatus(): Promise<SupervisorStatus> {
  return api.get<SupervisorStatus>('/api/v1/supervisor/status');
}

/** 演化与变更的累计计数，来自平台自己登记的那几个指标。 */
export interface EvolutionMetrics {
  events_total: number;
  events_failed: number;
  changes_applied: number;
  changes_failed: number;
}

export async function getEvolutionMetrics(): Promise<EvolutionMetrics> {
  return api.get<EvolutionMetrics>('/api/v1/evolution/metrics');
}

/**
 * 一次构建尝试。同一条变更重试三次就是三行——按变更去重会把重试藏起来，
 * 而重试次数正是决定下一次把构建花在哪里的那个数。
 */
export interface BuildRun {
  change_id: string;
  /** 变更的入口；生成它的那一侧没有携带时是 `unattributed`。 */
  intent: string;
  /** `built` / `failed` / `timed_out` / `unstarted`。 */
  outcome: string;
  /** 这次构建占用主机的秒数；`unstarted` 没跑，所以没有这一格而不是 0。 */
  seconds?: number;
  at: string;
}

/**
 * 构建跑次这次读的出处。窗口是演进进程内存里的，重启清零。
 *
 * `attached` 为 false 时 `runs` 恒为空，而那不是「一次都没构建过」：那种情形
 * 要去看接线，空列表要做的只是什么都不做。
 */
export interface BuildRunsView {
  attached: boolean;
  limit: number;
  runs: BuildRun[];
}

export async function getBuildRuns(): Promise<BuildRunsView> {
  return api.get<BuildRunsView>('/api/v1/evolution/builds');
}

export interface EvalDatasetInfo {
  name: string;
  path: string;
  case_count: number;
  tags: string[];
}

export async function listEvalDatasets(): Promise<EvalDatasetInfo[]> {
  const page = await api.get<{ datasets: EvalDatasetInfo[] }>(
    '/api/v1/eval/datasets'
  );
  return page.datasets ?? [];
}

/** 跑一次评测。报告会落到数据目录里，跑完就能在报告列表里读到。 */
export async function runEval(datasetPath: string): Promise<void> {
  await api.post('/api/v1/eval/run', { dataset_path: datasetPath });
}

/**
 * 一份已经落盘的报告。
 *
 * `total` 是应用上限**之前**目录里有几份：它比 `reports.length` 大，说明这次只拿到
 * 了前缀；`dir` 是这次读的目录，空列表要能说清是哪个目录空。
 */
export interface EvalReportList {
  dir: string;
  reports: EvalReportSummary[];
  total: number;
}

export interface EvalReportSummary {
  id: string;
  dataset_name: string;
  created_at: string;
  size_bytes: number;
}

export async function listEvalReports(limit?: number): Promise<EvalReportList> {
  const suffix = limit !== undefined ? `?limit=${limit}` : '';
  return api.get<EvalReportList>(`/api/v1/eval/reports${suffix}`);
}

/** 一份报告的两个面：渲染过的 markdown，与原样的 JSON。 */
export interface EvalReportDetail {
  run_id: string;
  dataset_name: string;
  report_markdown: string;
  report_json: string;
}

export async function getEvalReport(reportId: string): Promise<EvalReportDetail> {
  return api.get<EvalReportDetail>(
    `/api/v1/eval/report/${encodeURIComponent(reportId)}`
  );
}

/**
 * 记忆后端自己数的六个计数。
 *
 * 它们是这个进程启动以来发生过多少次，不是现在存着多少条——重启归零。要问「现在存着
 * 多少」，读原始来源的列表，那才是盘上的东西。
 */
export interface MemoryStats {
  raw_archived: number;
  raw_retrieved: number;
  schema_stored: number;
  schema_searched: number;
  summary_stored: number;
  summary_searched: number;
}

export async function getMemoryStats(): Promise<MemoryStats> {
  return api.get<MemoryStats>('/api/v1/memory/stats');
}

export interface RawMetadata {
  id: string;
  content_type: string;
  payload_length: number;
  created_at: string;
}

/**
 * 一次原始来源的列举。
 *
 * `total` 只在列表被 `limit` 截短时出现，且是截短**之前**的条数。它缺席就是
 * 「这一次列全了」；它出现就是「你看到的是前缀」——两种情形在界面上必须分开说。
 */
export interface RawListing {
  items: RawMetadata[];
  total?: number;
}

export async function listMemoryRaw(
  params: { prefix?: string; limit?: number } = {}
): Promise<RawListing> {
  const query = new URLSearchParams();
  if (params.prefix) query.set('prefix', params.prefix);
  if (params.limit !== undefined) query.set('limit', String(params.limit));
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return api.get<RawListing>(`/api/v1/memory/raw${suffix}`);
}

/** 一份原始来源的字节。`encoding` 说这是 UTF-8 文本还是 base64。 */
export interface RawContent {
  id: string;
  namespace: string;
  content_type: string;
  payload_length: number;
  encoding: 'utf8' | 'base64';
  payload: string;
  created_at: string;
}

export async function getMemoryRawContent(id: string): Promise<RawContent> {
  return api.get<RawContent>(`/api/v1/memory/raw/${encodeURIComponent(id)}/content`);
}

/**
 * 历史事件查询。
 *
 * 两件事是这一面的窗口，读的人必须知道：服务端把 `since` 钉在「最近 24 小时」，
 * 并且一次最多读 1000 条（读满就停）。于是它回答的是「这一天的头一千条里有什么」，
 * 不是「这一天一共发生过多少件事」——把上限读成总数，是这一面最容易说的假话。
 *
 * 返回的是变体字段平铺的原始事件（没有 `payload` 这一层，也没有 `id`），交给
 * `normalizeAgentEvent` 归一——与实时流走同一个函数，两条路不会把同一帧说成两件事。
 */
export async function listEvents(
  params: { agentId?: string; taskId?: string; squadId?: string; eventTypes?: string[] } = {}
): Promise<Record<string, unknown>[]> {
  const query = new URLSearchParams();
  if (params.agentId) query.set('agent_id', params.agentId);
  if (params.taskId) query.set('task_id', params.taskId);
  if (params.squadId) query.set('squad_id', params.squadId);
  if (params.eventTypes?.length) query.set('event_types', params.eventTypes.join(','));
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return api.get<Record<string, unknown>[]>(`/api/v1/events${suffix}`);
}

/** 日志归档的一条索引：一天一个流可能有多份文件，`format` 也是键的一部分。 */
export interface RawLogEntry {
  hour: number;
  stream_name: string;
  log_date: string;
  format: string;
  file_path: string;
  tier: string;
  size_bytes: number;
  event_count: number;
  checksum: string;
  start_time: string;
  end_time: string;
  created_at: string;
}

export async function listRawLogs(
  params: { stream?: string; tier?: string; limit?: number } = {}
): Promise<{ count: number; entries: RawLogEntry[] }> {
  const query = new URLSearchParams();
  if (params.stream) query.set('stream', params.stream);
  if (params.tier) query.set('tier', params.tier);
  if (params.limit !== undefined) query.set('limit', String(params.limit));
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return api.get<{ count: number; entries: RawLogEntry[] }>(`/api/v1/raw_logs${suffix}`);
}

/** 结构化记忆：schema 与 summary 两层各自的全量列举。 */
export interface SchemaEntry {
  id: string;
  kind: string;
  name: string;
  key: string;
  confidence: number;
}

export interface SummaryEntry {
  id: string;
  text: string;
  score: number;
  confidence: number;
}

export async function listMemorySchema(): Promise<SchemaEntry[]> {
  const page = await api.get<{ results: SchemaEntry[] }>('/api/v1/memory/schema/list');
  return page.results ?? [];
}

export async function listMemorySummary(): Promise<SummaryEntry[]> {
  const page = await api.get<{ results: SummaryEntry[] }>('/api/v1/memory/summary/list');
  return page.results ?? [];
}


/** 集群里三个固定工作负载各自的存活读数。 */
export interface ClusterComponent {
  id: string;
  /** up / down / unknown。unknown 是读不到，不是挂了。 */
  status: string;
  ready_replicas: number | null;
  desired_replicas: number | null;
}

export interface ClusterComponentsPage {
  components: ClusterComponent[];
}

export async function getClusterComponents(): Promise<ClusterComponentsPage> {
  return api.get<ClusterComponentsPage>('/api/v1/cluster/components');
}

// ── 原则台账 ──

/** 语料按判定结果分四格。`unmeasured` 是「没有一次判读settle过它」。 */
export interface PrincipleCensus {
  enforced: number;
  unenforced: number;
  unmeasured: number;
  retired: number;
}

/** 语料按「该怎么查」分格；`undeclared` 是没人声明过档位的那部分。 */
export interface PrincipleCheckKindCensus {
  shape: number;
  tree: number;
  live: number;
  judgement: number;
  undeclared: number;
}

export interface RefusedPrinciple {
  slug: string;
  lead: string | null;
  refusals: number;
  last_refused_at: string | null;
  /** 这条原则会在哪儿红。缺席是「还没人说过」，不是「没有门」。 */
  where_red?: string | null;
  /** 载体（记忆文件）。 */
  carrier_path?: string | null;
  host_gate?: string | null;
  platform_gate?: string | null;
  /** shape / tree / live / judgement；缺席是还没人声明怎么查。 */
  check_kind?: string | null;
  /** enforced / unenforced / retired；缺席是还没有一次判读落地。 */
  state?: string | null;
  priority?: string;
}

/**
 * 原则台账的三份读数。
 *
 * `refused` 是**节选**，两个总数却是全集的：拿节选求和当总数，会在被拦过的
 * 原则多于本页条数时低报，而低报的那个数看起来完全正常。
 */
export interface PrinciplesPage {
  generated_at: string;
  census: PrincipleCensus;
  check_kinds: PrincipleCheckKindCensus;
  refused: RefusedPrinciple[];
  refused_principles: number;
  refusals_total: number;
}

export async function getGovernancePrinciples(): Promise<PrinciplesPage> {
  return api.get<PrinciplesPage>('/api/v1/governance/principles');
}

// ── 变更账 ──

/**
 * 一格状态计数。
 *
 * `outstanding` 由服务端给，不在前端按名字推：六个状态里恰好两个终态不算欠账，
 * 而这两个正是最容易数错的两个。
 */
export interface ChangeStateCell {
  state: string;
  outstanding: boolean;
  rows: number;
}

/** 一个优先级带上的欠账条数；每个带都在，空的给 0。 */
export interface ChangeBandCell {
  band: string;
  rows: number;
}

/**
 * 一条还欠着的变更。
 *
 * `measured_at` 为空是「还没有人看过」，不是「很久以前看过」：两者要做的活相反，
 * 所以服务端不发一个填好的时间戳。
 */
export interface ChangeLedgerRow {
  /** 渲染用的 `kind:scope:literal`，不是可以反解的键——字面量里常有冒号。 */
  identity: string;
  /** 身份的三段。要一条行的历史就发这三段，不要发上面那个键。 */
  kind: string;
  scope: string;
  literal: string;
  state: string;
  title: string | null;
  assertion: string | null;
  sources: string[];
  first_seen_at: string;
  measured_at: string | null;
  measured_rev: string | null;
}

/**
 * 一条变更走过的一步。
 *
 * `from_state` 为空是入账那一步（此前没有状态），不是「状态未知」：把两者印成
 * 一样，读者就看不出这条行是刚进来的还是状态读丢了。
 */
export interface ChangeTimelineEvent {
  observed_at: string;
  from_state: string | null;
  to_state: string;
  measured_rev: string | null;
  retire_reason: string | null;
  unsettled_reason: string | null;
}

export interface ChangeTimeline {
  identity: string;
  kind: string;
  scope: string;
  literal: string;
  /** 新的在前。 */
  events: ChangeTimelineEvent[];
}

export async function getChangeTimeline(
  identity: { kind: string; scope: string; literal: string },
  limit = 50
): Promise<ChangeTimeline> {
  const query = new URLSearchParams({
    kind: identity.kind,
    scope: identity.scope,
    literal: identity.literal,
    limit: String(limit),
  });
  return api.get<ChangeTimeline>(`/api/v1/governance/changes/events?${query.toString()}`);
}

/**
 * 变更账的一份读数。
 *
 * `outstanding` 是**节选**，`outstanding_total` 才是全集：拿本页条数当欠账总条数，
 * 会让积压看起来和这一页一样小。
 */
export interface ChangesPage {
  generated_at: string;
  total: number;
  census: ChangeStateCell[];
  owed_by_band: ChangeBandCell[];
  pending_declarations: number;
  pending_retirements: number;
  outstanding_total: number;
  outstanding: ChangeLedgerRow[];
}

export async function getGovernanceChanges(): Promise<ChangesPage> {
  return api.get<ChangesPage>('/api/v1/governance/changes');
}

// ── 变更队列 ──

/** 一条变更候选。`status` 是它在流水线上的位置，服务端给的是原字符串。 */
export interface EvolutionChange {
  id: string;
  kind: string;
  description: string;
  status: string;
  created_at: string;
  diff_summary?: string | null;
  eval_summary?: string | null;
}

/**
 * 列举一条变更实际读的队列。
 *
 * 空列表与「读的不是同一个队列」在没有这条读数时长得一样：两个部署把不同的
 * 卷挂在同一个相对路径上，生成变更的进程和列举变更的进程读的不是同一批文件。
 */
export interface EvolutionQueueView {
  dir: string;
  owner: boolean;
}

export interface EvolutionChangesPage {
  changes: EvolutionChange[];
  queue: EvolutionQueueView | null;
}

export async function getEvolutionChanges(): Promise<EvolutionChangesPage> {
  return api.get<EvolutionChangesPage>('/api/v1/evolution/changes');
}

// ── 工作区文件 ──

/**
 * 文件树里的一条。`path` 是相对用户根的路径，既当 key 也当下一页的入参：
 * 界面上显示的是 `name`，请求里带的是 `path`，两者不能混用——同名文件在不同
 * 目录下很常见。
 */
export interface WorkspaceEntry {
  path: string;
  name: string;
  dir: boolean;
  size: number;
  modified_ms: number | null;
}

export interface WorkspaceTree {
  path: string;
  entries: WorkspaceEntry[];
  /** 目录条数超过服务端上限，这里只列了前一部分。 */
  truncated: boolean;
}

export interface WorkspaceFile {
  path: string;
  size: number;
  /** 二进制文件没有正文；`text` 那时是 null。 */
  binary: boolean;
  truncated: boolean;
  text: string | null;
  modified_ms: number | null;
  /**
   * 这一份内容的指纹；写回时带上，服务端据此判断「我读到的这一份还在不在」。
   * 二进制与超过上限的文件没有指纹——那两种情况下手里的不是完整的一份。
   */
  hash: string | null;
}

/**
 * 集群拓扑：集群 → 节点 → Pod → 容器。
 *
 * 到头是容器：容器里还跑着进程，这一份读数不读它们。卷写在 Pod 上、由容器挂进来，
 * 事件是独立对象按 `involvedObject` 挂在 Pod 或节点上——两者都不是容器的下级。
 *
 * 三个数量级始终并排：申报量决定排不排得进来，上限决定会不会被杀，用量决定这台
 * 机器累不累。混成一个数，读者会拿其中一个去回答另一个问题。
 */
export interface TopologyContainer {
  name: string;
  image: string;
  ready: boolean;
  restarts: number;
  /** running / waiting:<原因> / terminated:<原因> / unknown */
  state: string;
  ports: { name: string | null; container_port: number; protocol: string }[];
  cpu_request_millicores: number | null;
  memory_request_bytes: number | null;
  cpu_limit_millicores: number | null;
  memory_limit_bytes: number | null;
  /** 上一次是怎么结束的。重启次数只说「几次」，这里才说「为什么」。 */
  last_termination: TopologyTermination | null;
  mounts: { name: string; mount_path: string; read_only: boolean }[];
  /** liveness / readiness / startup 里配了的那些。 */
  probes: string[];
  /** 实际用量；null 表示这个集群没有 metrics-server，不是「用了 0」。 */
  cpu_used_millicores: number | null;
  memory_used_bytes: number | null;
  /**
   * 这个容器里的进程表。由节点上的采集器按宿主 cgroup 树读出来——镜像是什么都读得到，
   * 所以第三方镜像（postgres、nats、registry）也有这一格；跑本平台镜像的容器还会
   * 自己再报一份，两个来源落在同一张表上。
   *
   * `null` 不是「里面没有进程」：要么这台节点上没有采集器（看 Pod 的
   * `process_census_at`），要么这一拍没读到（刚起、已经退出）。两种在现场的修法相反。
   */
  processes: TopologyProcess[] | null;
}

/** 容器里的一行进程。列表按 PID 排，只读得到我们自己那一份 `/proc`。 */
export interface TopologyProcess {
  pid: number;
  parent_pid: number;
  command: string;
  rss_bytes: number;
}

export interface TopologyTermination {
  /** Completed / Error / OOMKilled / ContainerCannotRun … */
  reason: string;
  exit_code: number;
  message: string | null;
  finished_at: string | null;
}

/** Scheduled / Initialized / Ready / ContainersReady … 卡在哪一步就看这里。 */
export interface TopologyCondition {
  kind: string;
  ok: boolean;
  reason: string | null;
  message: string | null;
}

export interface TopologyClaim {
  name: string;
  phase: string;
  storage_class: string | null;
  capacity_bytes: number | null;
  access_modes: string[];
  volume_name: string | null;
  /** 盘上实际占用，来自平台自己的走查端；null 是这个卷还没被量到，不是空的。 */
  measured_bytes: number | null;
}

export interface TopologyVolume {
  name: string;
  /** configMap / secret / persistentVolumeClaim / emptyDir / hostPath … */
  kind: string;
  claim: TopologyClaim | null;
}

export interface TopologyEvent {
  /** Normal / Warning */
  kind: string;
  reason: string;
  message: string;
  count: number;
  last_seen: string | null;
}

export interface TopologyPod {
  namespace: string;
  name: string;
  phase: string;
  ready: boolean;
  restarts: number;
  node: string | null;
  cpu_requested_millicores: number;
  memory_requested_bytes: number;
  cpu_used_millicores: number | null;
  memory_used_bytes: number | null;
  /**
   * 这个 Pod 里跑着几个进程（kubelet 的 `process_stats`）。这是 Pod 级的数，
   * 名字在它下面每个容器的 `processes` 里——进程属于容器，不属于 Pod。
   * `null` 是「这个来源量不到」，不是「零个进程」。
   */
  processes: number | null;
  /**
   * 这个 Pod 所在的节点上，进程采集器最后一次报到的时刻（ISO 字符串）。
   * `null` ＝ 这台节点上没有活着的采集器，于是它上面每个容器的进程表都没有人能填。
   */
  process_census_at: string | null;
  containers: TopologyContainer[];
  conditions: TopologyCondition[];
  volumes: TopologyVolume[];
  events: TopologyEvent[];
}

export interface TopologyNode {
  name: string;
  ready: boolean;
  roles: string[];
  cpu_allocatable_millicores: number | null;
  memory_allocatable_bytes: number | null;
  pods_allocatable: number | null;
  cpu_requested_millicores: number;
  memory_requested_bytes: number;
  cpu_used_millicores: number | null;
  memory_used_bytes: number | null;
  /** Ready / MemoryPressure / DiskPressure …：压力条件为 true 才是坏消息。 */
  conditions: TopologyCondition[];
  /** 这台节点上进程采集器最后一次报到的时刻；`null` ＝ 这台节点上没有采集器。 */
  process_census_at: string | null;
  pods: TopologyPod[];
}

export interface TopologyCluster {
  /** 接口给这个连接的编号（`in-cluster`），不是集群的名字——Kubernetes 里没有这一项。 */
  id: string;
  /** 这一面读的是这一个命名空间里的 Pod；节点是整台机器的。 */
  namespace: string;
  reachable: boolean;
  error: string | null;
  version: string | null;
  /**
   * 实际用量这一次从哪儿读到的：metrics-server / kubelet-summary / null（都读不到）。
   * 为 null 时所有用量字段都是 null——「不知道」不是「用了 0」。
   */
  usage_source: 'metrics-server' | 'kubelet-summary' | null;
  nodes: TopologyNode[];
  /** Pod 自己说没被调度到任何节点上。 */
  unscheduled: TopologyPod[];
  /** Pod 说在某台节点上，但这次没读到那台节点——不是「没被调度」。 */
  unplaced: TopologyPod[];
}

export interface ClusterTopology {
  clusters: TopologyCluster[];
  collected_at: string;
}

export async function getClusterTopology(): Promise<ClusterTopology> {
  return api.get<ClusterTopology>('/api/v1/cluster/topology');
}

export interface QuotaEntry {
  resource: string;
  /** apiserver 的原始写法（`4`、`500m`、`4Gi`），换算由界面做。 */
  hard: string;
  used: string;
  /** 两边同维度才有的百分比；null 是「算不出」，不是零。 */
  percent: number | null;
}

export interface QuotaView {
  name: string;
  entries: QuotaEntry[];
}

export interface LimitEntry {
  kind: string;
  resource: string;
  max: string | null;
  min: string | null;
  default: string | null;
  default_request: string | null;
}

export interface LimitView {
  name: string;
  entries: LimitEntry[];
}

export interface ResourceReport {
  namespace: string;
  error: string | null;
  quotas: QuotaView[];
  limits: LimitView[];
  collected_at: string;
}

export async function getResourceReport(): Promise<ResourceReport> {
  return api.get<ResourceReport>('/api/v1/cluster/quota');
}

/**
 * 命名空间里的一个工作负载，不分工种。
 *
 * `kind` 是工种（Deployment / StatefulSet / DaemonSet / CronJob / Job），以及没有
 * 控制器拥有的裸 Pod（评测环境一道题一台 Pod）。一部分字段只对某些工种有意义，
 * 别的工种给 `null` 而不是 0——屏幕上「不适用」和「零个就绪」是两件事，写成 0 就
 * 把没这个概念读成了坏状态。
 */
export interface WorkloadView {
  kind: string;
  name: string;
  /** 裸 Pod 的相位；别的工种为 `null`。跑完的 Pod 是 `0 / 1` 但有相位，不是坏了。 */
  phase: string | null;
  ready: number | null;
  desired: number | null;
  updated: number | null;
  available: number | null;
  revision: string | null;
  images: string[];
  conditions: TopologyCondition[];
}

export interface WorkloadsReport {
  namespace: string;
  error: string | null;
  workloads: WorkloadView[];
  collected_at: string;
}

export async function getWorkloads(): Promise<WorkloadsReport> {
  return api.get<WorkloadsReport>('/api/v1/cluster/deployments');
}

/**
 * 文件面的两个根。
 *
 * `workspace` 是读者自己的可写目录；`project` 是这个部署正在演化的那份源码，
 * 只读。分开是因为它们答的是两个不同的问题：「我的东西放哪」与「这棵树现在长
 * 什么样、某个实现怎么写的」。一个只有新建的编辑器答不了第二个，而第二棵树的
 * 字节由部署器与智能体改，一条网页里的保存会绕过这两条路。
 */
export type FileSource = 'workspace' | 'project';

function sourceQuery(source: FileSource): string {
  return source === 'workspace' ? '' : `&source=${source}`;
}

export async function getWorkspaceTree(
  path = '',
  source: FileSource = 'workspace'
): Promise<WorkspaceTree> {
  return api.get<WorkspaceTree>(
    `/api/v1/ide/tree?path=${encodeURIComponent(path)}${sourceQuery(source)}`
  );
}

export async function getWorkspaceFile(
  path: string,
  source: FileSource = 'workspace'
): Promise<WorkspaceFile> {
  return api.get<WorkspaceFile>(
    `/api/v1/ide/file?path=${encodeURIComponent(path)}${sourceQuery(source)}`
  );
}

/**
 * 写回一份文件。
 *
 * 带上 `expectHash` 就是一次比较交换：盘上那一份在这期间被别人换过，服务端回
 * 409 而不是把对方盖掉。不带就是无条件覆盖——新建文件、以及「知道自己在覆盖」
 * 的那一下。
 */
export async function saveWorkspaceFile(
  path: string,
  text: string,
  expectHash?: string | null
): Promise<WorkspaceFile> {
  return api.put<WorkspaceFile>('/api/v1/ide/file', {
    path,
    text,
    ...(expectHash ? { expect_hash: expectHash } : {}),
  });
}

export async function createWorkspaceDir(path: string): Promise<WorkspaceEntry> {
  return api.post<WorkspaceEntry>('/api/v1/ide/mkdir', { path });
}

export async function renameWorkspaceEntry(
  from: string,
  to: string
): Promise<WorkspaceFile> {
  return api.post<WorkspaceFile>('/api/v1/ide/rename', { from, to });
}

export async function deleteWorkspaceEntry(path: string): Promise<void> {
  await api.delete<void>(`/api/v1/ide/file?path=${encodeURIComponent(path)}`);
}

export async function uploadWorkspaceFile(
  path: string,
  blob: Blob
): Promise<WorkspaceFile> {
  return api.sendBytes<WorkspaceFile>(
    `/api/v1/ide/upload?path=${encodeURIComponent(path)}`,
    'POST',
    blob
  );
}

/**
 * 下载：先取字节再交给浏览器另存。
 *
 * 直接写一个 `<a href>` 不行——那条请求不带 Authorization 头，会被鉴权挡下，
 * 而把一个长期有效的令牌塞进 URL 里，等于把它留在浏览历史里。
 */
/**
 * 读一份文件的原始字节。
 *
 * docx / xlsx 这类容器没有「正文」可读，`/ide/file` 只能回一个「这是二进制」。
 * 要显示里面的内容就必须拿到字节，在浏览器里自己拆容器——这一条就是那条路。
 */
export async function getWorkspaceFileBytes(path: string): Promise<ArrayBuffer> {
  const blob = await api.getBytes(`/api/v1/ide/raw?path=${encodeURIComponent(path)}`);
  return blob.arrayBuffer();
}

export async function downloadWorkspaceFile(
  path: string,
  source: FileSource = 'workspace'
): Promise<void> {
  const blob = await api.getBytes(
    `/api/v1/ide/raw?path=${encodeURIComponent(path)}${sourceQuery(source)}`
  );
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = path.split('/').pop() ?? 'download';
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  // 立刻撤销会让部分浏览器拿到空文件，下一轮事件循环再放掉。
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

export interface WorkspaceSearchMatch {
  path: string;
  /** 从 1 开始；列按字符算，不按字节。 */
  line: number;
  column: number;
  text: string;
}

export interface WorkspaceSearchResult {
  query: string;
  matches: WorkspaceSearchMatch[];
  scanned_files: number;
  scanned_bytes: number;
  /** 命中条数或扫描面到了上限，结果被截断。 */
  truncated: boolean;
}

/**
 * 在整个工作区里按字面量找。
 *
 * 与文件内查找分开：那条只在已打开的这一份里走，回答的是「这一份里还有没有」；
 * 这一条回答「哪个文件里有」，所以它回的是路径 + 行号，不是编辑器里的下标。
 */
export async function searchWorkspace(
  query: string,
  limit = 100
): Promise<WorkspaceSearchResult> {
  return api.get<WorkspaceSearchResult>(
    `/api/v1/ide/search?q=${encodeURIComponent(query)}&limit=${limit}`
  );
}

export interface WorkspaceUsage {
  files: number;
  bytes: number;
  /** 0 表示不限量。 */
  limit_bytes: number;
}

/** 我的工作区占了多少、上限多少——写不进去之前，读者该先看见还剩多少。 */
export async function getWorkspaceUsage(): Promise<WorkspaceUsage> {
  return api.get<WorkspaceUsage>('/api/v1/ide/usage');
}

export interface AuditEvent {
  id: number;
  trigger_type: string;
  dedup_key: string | null;
  agent_id: string | null;
  task_id: string | null;
  crew_id: string | null;
  squad_id: string | null;
  payload: unknown;
  timestamp: string;
}

export interface AuditQuery {
  agent_id?: string;
  task_id?: string;
  trigger_type?: string;
  since?: string;
  limit?: number;
}

/**
 * 归档表里的一段事件。
 *
 * 这张表只追加：读得少、读错了都不影响它，所以「某一刻总线上去过什么」这件事
 * 事后还能问。不是全部动作都进来——只有 hook 事件在这里留档。
 */
export async function listAuditEvents(query: AuditQuery = {}): Promise<AuditEvent[]> {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === '') continue;
    params.set(key, String(value));
  }
  const suffix = params.toString();
  return api.get<AuditEvent[]>(`/api/v1/audit/events${suffix ? `?${suffix}` : ''}`);
}

export interface QuotaUsage {
  user_id: string;
  remaining: number;
  used_today: number;
  total_quota: number;
}

/** 我这一份 token 额度今天用了多少、还剩多少。 */
export async function getQuotaUsage(): Promise<QuotaUsage> {
  return api.get<QuotaUsage>('/api/v1/quota/usage');
}

export interface ScmChange {
  path: string;
  /** `M` 改、`A` 加、`D` 删、`R` 改名、`C` 复制、`U` 冲突、`?` 未跟踪。 */
  code: string;
}

export interface ScmStatus {
  /** 这个工作区还不是仓库时，下面各格都是空的。 */
  repo: boolean;
  branch: string | null;
  head: string | null;
  staged: ScmChange[];
  unstaged: ScmChange[];
  truncated: boolean;
}

export interface ScmCommit {
  id: string;
  short: string;
  author: string;
  /** 提交时刻（Unix 秒）。 */
  at: number;
  subject: string;
}

export interface ScmLog {
  repo: boolean;
  commits: ScmCommit[];
}

export interface DiffLine {
  kind: 'context' | 'add' | 'del';
  old_no: number | null;
  new_no: number | null;
  text: string;
}

export interface DiffHunk {
  header: string;
  old_start: number;
  old_lines: number;
  new_start: number;
  new_lines: number;
  lines: DiffLine[];
}

export interface FileDiff {
  path: string;
  staged: boolean;
  /** 二进制文件没有可比的正文。 */
  binary: boolean;
  hunks: DiffHunk[];
  added: number;
  removed: number;
  truncated: boolean;
}

export interface CommitOutcome {
  commit: ScmCommit;
  status: ScmStatus;
}

/** 工作区的 git 状态。轮询它，源码管理面板与页签上的角标读的是同一份。 */
export async function getScmStatus(): Promise<ScmStatus> {
  return api.get<ScmStatus>('/api/v1/ide/git/status');
}

export async function getScmLog(limit = 20): Promise<ScmLog> {
  return api.get<ScmLog>(`/api/v1/ide/git/log?limit=${limit}`);
}

/**
 * 一个文件的差异。行号由服务端算：前后端各算一份必然在某一刻错位，
 * 而错位的 diff 会把改动指到错误的行上。
 */
export async function getScmDiff(path: string, staged: boolean): Promise<FileDiff> {
  const params = new URLSearchParams({ path, staged: staged ? 'true' : 'false' });
  return api.get<FileDiff>(`/api/v1/ide/git/diff?${params.toString()}`);
}

export async function initWorkspaceRepo(): Promise<ScmStatus> {
  return api.post<ScmStatus>('/api/v1/ide/git/init', {});
}

export async function stagePaths(paths: string[]): Promise<ScmStatus> {
  return api.post<ScmStatus>('/api/v1/ide/git/stage', { paths });
}

export async function unstagePaths(paths: string[]): Promise<ScmStatus> {
  return api.post<ScmStatus>('/api/v1/ide/git/unstage', { paths });
}

/** 提交暂存区。说明由服务端去空白并拒绝空串——一条没有理由的提交没有信息。 */
export async function commitStaged(message: string): Promise<CommitOutcome> {
  return api.post<CommitOutcome>('/api/v1/ide/git/commit', { message });
}

/** 一份已经导出到发布目录的备份包。 */
export interface BackupPackage {
  name: string;
  bytes: number;
  /** 秒级 Unix 时间；0 表示读不出修改时间。 */
  created_at: number;
}

export interface BackupPackages {
  /** 现在有没有一趟导出在跑。页面靠它决定要不要继续轮询。 */
  running: boolean;
  packages: BackupPackage[];
}

export interface BackupExportStarted {
  job: string;
  encrypted: boolean;
  publish_dir: string;
}

export async function listBackupPackages(): Promise<BackupPackages> {
  return api.get<BackupPackages>('/api/v1/admin/backup/packages');
}

/**
 * 让服务端起一趟导出。
 *
 * `passphrase` 为空串是**不加密**：包里不带凭证，剩下的是整库数据本身。这个选择
 * 交给调用方明说，不给「留空就自动加密」这种默认——默认加密会让没有口令的人拿到
 * 一份打不开的包，而默认不加密至少是打开着的。
 */
export async function startBackupExport(passphrase: string): Promise<BackupExportStarted> {
  const trimmed = passphrase.trim();
  return api.post<BackupExportStarted>('/api/v1/admin/backup/export', {
    passphrase: trimmed ? trimmed : null,
  });
}

export async function deleteBackupPackage(name: string): Promise<void> {
  await api.delete(`/api/v1/admin/backup/packages/${encodeURIComponent(name)}`);
}

/** 下载：先取字节再交给浏览器另存——`<a href>` 那条路不带 Authorization。 */
export async function downloadBackupPackage(name: string): Promise<void> {
  const blob = await api.getBytes(`/api/v1/admin/backup/packages/${encodeURIComponent(name)}`);
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  // 交给浏览器一段时间再收回：立刻 revoke 会让一部分浏览器的另存对话框
  // 拿到一个已经失效的 URL，表现是「下载了 0 字节」。
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

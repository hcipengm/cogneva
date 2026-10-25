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
  /** 告警面的判词：有东西在 firing 就是 degraded，没有来源答话就是 unknown。 */
  cluster_health: string;
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

/** 一段对话里的一行，读回来的形状。 */
export interface ChatTranscriptLine {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  /** 这一轮没有答案的原因。有值就是「问过，但没答上来」。 */
  failure: string | null;
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
  outcome: 'queued' | 'replied' | 'timed_out' | 'refused' | 'failed' | string;
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
  kind: 'face' | 'cross' | string;
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
  state: string;
  owner: string | null;
  owner_source: 'declared' | 'measured' | string;
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

export interface BackendHealth {
  name: string;
  status: 'up' | 'down' | 'degraded' | 'unknown' | string;
  latency_ms: number | null;
  last_checked: string | null;
  error: string | null;
}

export interface BackendHealthMatrix {
  overall: string;
  backends: BackendHealth[];
}

export async function getBackendHealth(): Promise<BackendHealthMatrix> {
  return api.get<BackendHealthMatrix>('/api/v1/health/backends');
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
  identity: string;
  state: string;
  title: string | null;
  assertion: string | null;
  sources: string[];
  first_seen_at: string;
  measured_at: string | null;
  measured_rev: string | null;
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

export async function getWorkspaceTree(path = ''): Promise<WorkspaceTree> {
  return api.get<WorkspaceTree>(`/api/v1/ide/tree?path=${encodeURIComponent(path)}`);
}

export async function getWorkspaceFile(path: string): Promise<WorkspaceFile> {
  return api.get<WorkspaceFile>(`/api/v1/ide/file?path=${encodeURIComponent(path)}`);
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
export async function downloadWorkspaceFile(path: string): Promise<void> {
  const blob = await api.getBytes(`/api/v1/ide/raw?path=${encodeURIComponent(path)}`);
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

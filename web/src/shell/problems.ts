import type { ActiveAlerts, ClusterOverview, LlmStatus } from '@/api/client';
import type { ActivityId } from '@/shell/store';

/**
 * 一条问题：哪一面、什么级别、说的是什么。
 *
 * 它不是日志的另一个视图，而是「现在有哪几件事需要人处理」的汇总。级别只有
 * 两档有意义：报了但没人管会变成事故的，和不一定会变的。没有 info 档——一条
 * 只是说说的行会把真正要处理的行挤下去。
 */
export interface Problem {
  id: string;
  severity: 'error' | 'warning';
  source: string;
  message: string;
  at: string | null;
  /** 点这一行跳到能处置它的那一面。 */
  goto: ActivityId;
}

export function deriveProblems(input: {
  overview: { data: ClusterOverview | null; error: string | null };
  alerts: { data: ActiveAlerts | null; error: string | null };
  llm: { status: LlmStatus | null };
}): Problem[] {
  const problems: Problem[] = [];
  const { overview, alerts, llm } = input;
  const o = overview.data;
  const a = alerts.data;

  if (overview.error) {
    problems.push({
      id: 'overview-error',
      severity: 'error',
      source: '集群概况',
      message: overview.error,
      at: null,
      goto: 'observability',
    });
  }

  if (alerts.error) {
    problems.push({
      id: 'alerts-error',
      severity: 'error',
      source: '告警',
      message: alerts.error,
      at: null,
      goto: 'alerts',
    });
  }

  if (a?.durable_alerts_read === false) {
    problems.push({
      id: 'alerts-durable',
      severity: 'warning',
      source: '告警',
      message: '这次没读到耐久告警表：列表只剩进程内事件，缺少的不等于已经清掉。',
      at: null,
      goto: 'alerts',
    });
  }

  for (const alert of a?.alerts ?? []) {
    problems.push({
      id: `alert:${alert.source}:${alert.id}`,
      severity: alert.severity === 'critical' ? 'error' : 'warning',
      source: `告警 ${alert.event_type}`,
      message: alert.message,
      at: alert.timestamp,
      goto: 'alerts',
    });
  }

  if (o) {
    if (o.cluster_health !== 'healthy') {
      problems.push({
        id: 'cluster-health',
        severity: o.cluster_health === 'degraded' ? 'warning' : 'error',
        source: '集群判词',
        message: `集群判词是 ${o.cluster_health}：它只看告警面，别的面没验过。`,
        at: o.timestamp,
        goto: 'observability',
      });
    }
    if (o.failed_tasks != null && o.failed_tasks > 0) {
      problems.push({
        id: 'failed-tasks',
        severity: 'warning',
        source: '任务',
        message: `${o.failed_tasks} 个任务以失败收场。`,
        at: o.timestamp,
        goto: 'tasks',
      });
    }
    if (o.llm_pool === null) {
      problems.push({
        id: 'llm-pool-null',
        severity: 'warning',
        source: '模型池',
        message: '没有上游池快照可读：写它的网关不在，或快照已过期。',
        at: null,
        goto: 'observability',
      });
    } else if (!o.llm_pool.available) {
      problems.push({
        id: 'llm-pool-down',
        severity: 'error',
        source: '模型池',
        message: `上游池不可用，${o.llm_pool.unavailable_count} / ${o.llm_pool.pool_size} 条不可用。`,
        at: o.timestamp,
        goto: 'observability',
      });
    }
  }

  if (llm.status && !llm.status.configured) {
    problems.push({
      id: 'llm-unconfigured',
      severity: 'error',
      source: '模型',
      message: '还没有配置任何模型上游，平台跑不了任何一次调用。',
      at: null,
      goto: 'settings',
    });
  }

  const rank = { error: 0, warning: 1 } as const;
  return problems.sort((x, y) => rank[x.severity] - rank[y.severity]);
}

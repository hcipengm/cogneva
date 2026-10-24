import { useCallback, useEffect, useState } from 'react';
import {
  getBackendHealth,
  getEvolutionMetrics,
  getGovernanceDrift,
  getGovernanceFaces,
  getSupervisorStatus,
  getTaskSummary,
  listEvalDatasets,
  listTasks,
  type BackendHealthMatrix,
  type EvalDatasetInfo,
  type EvolutionMetrics,
  type FaceDriftReading,
  type GovernanceFace,
  type SupervisorStatus,
  type TaskRow,
  type TaskSummaryCounts,
} from '@/api/client';

/**
 * 系统面的读数：面、任务、后端、评估。
 *
 * 每一格各自持有一份 `error`。合成一个总的「加载失败」会在一个面板坏掉时
 * 把另外三个的读数一起藏起来，而**面板空着与读失败在界面上长得一样**——
 * 读的人会以为那里真的什么都没有。
 */
export interface SurfaceBucket<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  loadedAt: string | null;
}

function emptyBucket<T>(): SurfaceBucket<T> {
  return { data: null, loading: false, error: null, loadedAt: null };
}

function describeError(e: unknown, what: string): string {
  const err = e as { status?: number; message?: string };
  const raw = err?.message?.trim();
  if (!raw) return `读不到${what}（HTTP ${err?.status ?? '?'}）`;
  try {
    const parsed = JSON.parse(raw) as { message?: string; error?: string };
    const inner = parsed.message ?? parsed.error;
    if (inner) return `读不到${what}：${inner}`;
  } catch {
    // 不是 JSON 就把原文留着：原文本身也是读数。
  }
  return `读不到${what}：${raw}`;
}

export interface UseSystemSurfaceOptions {
  /** 面板开着才轮询；关掉之后留着最后一次读数，不接着发请求。 */
  enabled: boolean;
  /** 刷新间隔。读数本身就是按请求重算的，所以刷新等于重新问一次。 */
  intervalMs?: number;
}

export function useSystemSurface({ enabled, intervalMs = 15000 }: UseSystemSurfaceOptions) {
  const [faces, setFaces] = useState<SurfaceBucket<GovernanceFace[]>>(emptyBucket);
  const [drift, setDrift] = useState<SurfaceBucket<FaceDriftReading[]>>(emptyBucket);
  const [tasks, setTasks] = useState<SurfaceBucket<TaskSummaryCounts>>(emptyBucket);
  const [taskRows, setTaskRows] = useState<SurfaceBucket<TaskRow[]>>(emptyBucket);
  const [backends, setBackends] = useState<SurfaceBucket<BackendHealthMatrix>>(emptyBucket);
  const [datasets, setDatasets] = useState<SurfaceBucket<EvalDatasetInfo[]>>(emptyBucket);
  const [supervisor, setSupervisor] = useState<SurfaceBucket<SupervisorStatus>>(emptyBucket);
  const [evolution, setEvolution] = useState<SurfaceBucket<EvolutionMetrics>>(emptyBucket);

  const loadFaces = useCallback(async () => {
    setFaces((b) => ({ ...b, loading: true }));
    try {
      const page = await getGovernanceFaces();
      setFaces({ data: page.faces, loading: false, error: null, loadedAt: page.generated_at });
    } catch (e) {
      setFaces((b) => ({ ...b, loading: false, error: describeError(e, '面名册') }));
    }
  }, []);

  const loadDrift = useCallback(async () => {
    setDrift((b) => ({ ...b, loading: true }));
    try {
      const page = await getGovernanceDrift();
      setDrift({ data: page.readings, loading: false, error: null, loadedAt: page.generated_at });
    } catch (e) {
      setDrift((b) => ({ ...b, loading: false, error: describeError(e, '面的读数') }));
    }
  }, []);

  const loadTasks = useCallback(async () => {
    setTasks((b) => ({ ...b, loading: true }));
    setTaskRows((b) => ({ ...b, loading: true }));
    try {
      const summary = await getTaskSummary();
      setTasks({
        data: summary,
        loading: false,
        error: null,
        loadedAt: new Date().toISOString(),
      });
    } catch (e) {
      setTasks((b) => ({ ...b, loading: false, error: describeError(e, '任务计数') }));
    }
    try {
      const rows = await listTasks({ limit: 100 });
      setTaskRows({
        data: rows,
        loading: false,
        error: null,
        loadedAt: new Date().toISOString(),
      });
    } catch (e) {
      setTaskRows((b) => ({ ...b, loading: false, error: describeError(e, '任务列表') }));
    }
  }, []);

  const loadBackends = useCallback(async () => {
    setBackends((b) => ({ ...b, loading: true }));
    try {
      const matrix = await getBackendHealth();
      setBackends({
        data: matrix,
        loading: false,
        error: null,
        loadedAt: new Date().toISOString(),
      });
    } catch (e) {
      setBackends((b) => ({ ...b, loading: false, error: describeError(e, '后端健康') }));
    }
  }, []);

  const loadDatasets = useCallback(async () => {
    setDatasets((b) => ({ ...b, loading: true }));
    try {
      const list = await listEvalDatasets();
      setDatasets({
        data: list,
        loading: false,
        error: null,
        loadedAt: new Date().toISOString(),
      });
    } catch (e) {
      setDatasets((b) => ({ ...b, loading: false, error: describeError(e, '评估集') }));
    }
  }, []);

  const loadSupervisor = useCallback(async () => {
    setSupervisor((b) => ({ ...b, loading: true }));
    try {
      const status = await getSupervisorStatus();
      setSupervisor({
        data: status,
        loading: false,
        error: null,
        loadedAt: status.timestamp,
      });
    } catch (e) {
      setSupervisor((b) => ({ ...b, loading: false, error: describeError(e, '监管者状态') }));
    }
  }, []);

  const loadEvolution = useCallback(async () => {
    setEvolution((b) => ({ ...b, loading: true }));
    try {
      const metrics = await getEvolutionMetrics();
      setEvolution({
        data: metrics,
        loading: false,
        error: null,
        loadedAt: new Date().toISOString(),
      });
    } catch (e) {
      setEvolution((b) => ({ ...b, loading: false, error: describeError(e, '演化计数') }));
    }
  }, []);

  const refreshAll = useCallback(() => {
    void loadFaces();
    void loadDrift();
    void loadTasks();
    void loadBackends();
    void loadDatasets();
    void loadSupervisor();
    void loadEvolution();
  }, [loadFaces, loadDrift, loadTasks, loadBackends, loadDatasets, loadSupervisor, loadEvolution]);

  useEffect(() => {
    if (!enabled) return;
    refreshAll();
    const timer = setInterval(refreshAll, intervalMs);
    return () => clearInterval(timer);
  }, [enabled, intervalMs, refreshAll]);

  return {
    faces,
    drift,
    tasks,
    taskRows,
    backends,
    datasets,
    supervisor,
    evolution,
    refreshAll,
  };
}

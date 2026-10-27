import { useState } from 'react';
import {
  getEvalReport,
  listEvalDatasets,
  listEvalReports,
  runEval,
  type EvalReportDetail,
  type EvalReportSummary,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { Button, Empty, Loading, Moment, ReadError, Reading, Table, ViewFrame } from '@/ui/primitives';
import { BarList } from '@/ui/plots';

const PAGE = 100;

function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

/**
 * 一份报告。
 *
 * 画的是 markdown 原文而不是渲染后的样子：这一面没有 markdown 渲染器，硬渲染出一个
 * 半吊子的版本，读者会以为表格错乱是报告写的，其实是这里画的。原文至少是准的。
 */
function ReportBody({ report, onClose }: { report: EvalReportSummary; onClose: () => void }) {
  const detail = useResource<EvalReportDetail>(
    `评测报告 ${report.id}`,
    () => getEvalReport(report.id),
    300_000
  );
  const [showJson, setShowJson] = useState(false);
  const data = detail.data;

  return (
    <section className="border-b border-[var(--line)]">
      <div className="flex items-baseline gap-3 px-4 py-2">
        <span className="mono truncate text-[11px] text-[var(--paper-dim)]" title={report.id}>
          {report.id}
        </span>
        <span className="text-[11px] text-[var(--muted)]">
          {report.dataset_name || '（报告里没有数据集名）'} · {bytes(report.size_bytes)}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <Button onClick={() => setShowJson(!showJson)}>{showJson ? '看渲染稿' : '看 JSON'}</Button>
          <button onClick={onClose} className="text-[11px] text-[var(--muted)] hover:text-[var(--paper)]">
            收起
          </button>
        </div>
      </div>
      {detail.error ? (
        <ReadError what="评测报告" error={detail.error} onRetry={detail.refresh} />
      ) : !data && detail.loading ? (
        <Loading what="评测报告" />
      ) : data ? (
        <pre className="mono mx-4 mb-3 max-h-96 overflow-auto border border-[var(--line)] p-2 text-[11px] whitespace-pre-wrap text-[var(--paper-dim)]">
          {showJson ? data.report_json : data.report_markdown}
        </pre>
      ) : null}
    </section>
  );
}

/**
 * 评测面：手里有哪些数据集、跑过哪几次、每次的结论是什么。
 *
 * 「跑一次」是真的跑：它调模型、花时间，跑完的结论会落进数据目录。写在按钮旁边的
 * 这句不是为了客气，是因为这一面上别的都是读，只有这一下是写。
 */
export function EvalView() {
  const datasets = useResource('评测数据集', listEvalDatasets, 60_000);
  const reports = useResource('评测报告', () => listEvalReports(PAGE), 30_000);
  // 一次评测跑起来就是一条任务，跑完就是一次 `task_status_change`——报告列表在总
  // 线上有对应的事。以前只有「在这个页面点过跑评测」的那一条路会刷，别处跑完的要
  // 等下一拍 30 秒；而这一面答的正是「刚才那一轮做得怎么样」。
  useKindTickedRefresh(['tasks', resourceKey('eval')], [reports.refresh, datasets.refresh]);
  const [picked, setPicked] = useState<EvalReportSummary | null>(null);
  const [running, setRunning] = useState<string | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [runDone, setRunDone] = useState<string | null>(null);

  const start = async (path: string, name: string) => {
    setRunning(name);
    setRunError(null);
    setRunDone(null);
    try {
      await runEval(path);
      setRunDone(`「${name}」跑完了，报告已落盘。`);
      reports.refresh();
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setRunError(err?.message?.trim() || `跑不动（HTTP ${err?.status ?? '?'}）`);
    } finally {
      setRunning(null);
    }
  };

  const setList = datasets.data?.datasets ?? [];
  const reportList = reports.data?.reports ?? [];
  const reportTotal = reports.data?.total ?? 0;

  return (
    <ViewFrame
      title="评测"
      meta={
        reports.data
          ? `${setList.length} 个数据集 · ${reportTotal} 份报告 · 按 30 秒重读`
          : '数据集与跑过的报告 · 按 30 秒重读'
      }
    >
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">数据集</h2>
        {datasets.error ? (
          <ReadError what="评测数据集" error={datasets.error} onRetry={datasets.refresh} />
        ) : !datasets.data && datasets.loading ? (
          <Loading what="评测数据集" />
        ) : datasets.data && !datasets.data.present ? (
          <Empty>
            这个部署没有供给数据集目录：<span className="mono">{datasets.data.dir}</span>{' '}
            不存在。它不是「还没有数据集」——那个目录从来没人建过，所以这里永远是空。
          </Empty>
        ) : setList.length === 0 ? (
          <Empty>
            数据目录里还没有数据集。评测的输入是{' '}
            <span className="mono">{datasets.data?.dir}</span> 下的 jsonl / yaml 文件，放进去才会出现在这里。
          </Empty>
        ) : (
          <Table
            columns={[
              { key: 'name', title: '名称', width: '200px', render: (d) => (
                <span className="mono text-[11px]">{d.name}</span>
              ) },
              { key: 'cases', title: '用例', width: '80px', render: (d) => (
                <span className="mono text-[11px]">{d.case_count}</span>
              ) },
              { key: 'tags', title: '标签', width: '120px', render: (d) => (
                <span className="text-[11px] text-[var(--muted)]">{d.tags.join(' · ') || '—'}</span>
              ) },
              { key: 'path', title: '文件', render: (d) => (
                <span className="mono truncate text-[11px] text-[var(--paper-dim)]" title={d.path}>
                  {d.path}
                </span>
              ) },
              { key: 'run', title: '', width: '90px', render: (d) => (
                <button
                  type="button"
                  disabled={running !== null}
                  onClick={() => void start(d.path, d.name)}
                  className="text-[11px] text-[var(--brass)] hover:underline disabled:opacity-40"
                >
                  {running === d.name ? '跑着…' : '跑一次'}
                </button>
              ) },
            ]}
            rows={setList}
            rowKey={(d) => d.path}
          />
        )}
        <p className="px-4 pb-3 pt-2 text-[11px] text-[var(--muted)]">
          「跑一次」是真的跑：它会调模型、花时间，跑完的结论落进数据目录。这一面上别的都是读，只有这一下是写。
        </p>
        {runError && (
          <p className="px-4 pb-3 text-[11px] text-[var(--danger)]">跑不动：{runError}</p>
        )}
        {runDone && <p className="px-4 pb-3 text-[11px] text-[var(--ok)]">{runDone}</p>}
      </section>
      <section className="border-b border-[var(--line)]">
        <h2 className="px-4 py-2 text-[11px] uppercase tracking-wider text-[var(--muted)]">
          跑过的报告{reports.data ? ` · ${reports.data.dir}` : ''}
        </h2>
        {reports.error ? (
          <ReadError what="评测报告" error={reports.error} onRetry={reports.refresh} />
        ) : !reports.data && reports.loading ? (
          <Loading what="评测报告" />
        ) : reportList.length === 0 ? (
          <Empty>
            这个目录里还没有报告——不是读不通，是还没跑过。上面那一列里的「跑一次」就是往这里写。
          </Empty>
        ) : (
          <Table
            columns={[
              { key: 'dataset', title: '数据集', width: '180px', render: (r) => (
                <span className="text-[12px]">{r.dataset_name || '—'}</span>
              ) },
              { key: 'id', title: 'id', width: '280px', render: (r) => (
                <button
                  onClick={() => setPicked(picked?.id === r.id ? null : r)}
                  className="mono block w-full truncate text-left text-[11px] text-[var(--paper-dim)] underline-offset-2 hover:underline"
                  title={r.id}
                >
                  {r.id}
                </button>
              ) },
              { key: 'at', title: '跑于', width: '110px', render: (r) => <Moment iso={r.created_at} /> },
              { key: 'size', title: '大小', width: '90px', render: (r) => (
                <span className="mono text-[11px]">{bytes(r.size_bytes)}</span>
              ) },
            ]}
            rows={reportList}
            rowKey={(r) => r.id}
          />
        )}
        {reportList.length > 0 && (
          <BarList
            label="每个数据集跑过几份报告"
            note="只数这一页列出来的报告（GET /api/v1/eval/reports）；报告里没有分数，这里也就画不出分数"
            rows={[...reportList
              .reduce((byDataset, report) => {
                const name = report.dataset_name || '（报告里没有数据集名）';
                byDataset.set(name, (byDataset.get(name) ?? 0) + 1);
                return byDataset;
              }, new Map<string, number>())
              .entries()]
              .sort((a, b) => b[1] - a[1])
              .map(([name, count]) => ({ id: name, label: name, value: count }))}
          />
        )}
        {reportTotal > reportList.length && (
          <p className="px-4 pb-3 pt-2 text-[11px] text-[var(--warn)]">
            目录里有 {reportTotal} 份，这一页只列了新的 {reportList.length} 份。
          </p>
        )}
      </section>
      {picked && <ReportBody report={picked} onClose={() => setPicked(null)} />}
      <div className="flex flex-wrap border-t border-[var(--line)] py-2">
        <Reading
          label="数据集"
          from={datasets}
          source="GET /api/v1/eval/datasets"
          value={datasets.data && !datasets.data.present ? '未供给' : setList.length}
          note={datasets.data?.dir}
        />
        <Reading
          label="报告（全目录）"
          from={reports}
          source="GET /api/v1/eval/reports 的 total"
          caveat="服务端在截断前数出来的：目录里还有更老的报告，这一页不一定列全。"
          value={reportTotal}
        />
        <Reading
          label="报告（这一页）"
          from={reports}
          source="GET /api/v1/eval/reports 返回的这一页"
          value={reportList.length}
        />
      </div>
      <p className="px-4 pb-4 text-[11px] text-[var(--muted)]">
        两处都从盘上读：数据集是数据目录里的文件，报告是跑完落下来的 JSON。目录换过、挂载丢过，
        这一面就会空——空说的是「这个目录里没有」，不是「平台没跑过」。
      </p>
    </ViewFrame>
  );
}

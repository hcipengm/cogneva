import { useRef, useState } from 'react';
import {
  listDatasetVersions,
  listEvalDatasets,
  uploadFile,
  verifyDatasets,
  type DatasetCheckView,
  type DatasetVerifyReport,
  type DatasetVersionView,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { useKindTickedRefresh } from '@/hooks/useStreamDrivenRefresh';
import { resourceKey } from '@/store/frameTick';
import { useShell } from '@/shell/store';
import { Button, Empty, Loading, Moment, ReadError, Table, ViewFrame } from '@/ui/primitives';
import { FormRow, TextInput } from '@/ui/forms';
import { Note } from '@/ui/Note';

/**
 * 数据集面：两种数据集，两条不同的路。
 *
 * 这个仓库里「数据集」指两样东西，存储与读面都不同，所以这一页分两块画，不合在一起：
 *
 * 1. **评测数据集**：数据目录下 `eval/datasets` 里的 jsonl / yaml 文件，评测按文件
 *    路径跑。读面还会告诉人那个目录在不在——目录不在时的空列表不是「没有数据集」，
 *    是这个部署根本没有供给它。
 * 2. **对象目录**：`DatasetStore` 里登记过的每一个版本（名字、版本、摘要、行数、
 *    覆盖时间）。这里可以按名字查版本，也可以把整份目录对着对象存储验一遍：
 *    迁移之后「搬运完成」与「搬了个空」在进程外面同形，验一遍才知道字节对不对。
 *
 * 校验不通过时服务端答 409，但正文里那条逐项的检查表仍然有效——`verifyDatasets`
 * 把它捞出来，这里照常画，包括对不上的摘要。
 *
 * 画面：这一面是两个清单——评测文件、登记版本，再加一张逐项校验结论。版本按登记
 * 时间排，校验一行一份；把「对得上 / 对不上」画成色块会丢掉是哪一份、对不上差在
 * 哪几字节，而那正是迁移之后唯一要读的东西。
 */

export function DatasetsView() {
  const evalDatasets = useResource('评测数据集', listEvalDatasets, 60_000);
  // 这一面读的三个消费者各有一个前缀：评测目录在 `eval` 下，登记版本在 `datasets`
  // 下，上传写在 `files` 下。任何一条成功的写都让这一面重读——上传完的那一份要
  // 不重读，目录里已经多了字节而列表还是旧的那一版。
  useKindTickedRefresh(
    [resourceKey('eval'), resourceKey('datasets'), resourceKey('files')],
    evalDatasets.refresh
  );
  const appendOutput = useShell((s) => s.appendOutput);

  const [name, setName] = useState('');
  const [versions, setVersions] = useState<DatasetVersionView[] | null>(null);
  const [versionsError, setVersionsError] = useState<string | null>(null);
  const [looking, setLooking] = useState(false);

  const [verify, setVerify] = useState<DatasetVerifyReport | null>(null);
  const [verifyError, setVerifyError] = useState<string | null>(null);
  const [verifying, setVerifying] = useState(false);

  const [uploading, setUploading] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  const lookup = async (wanted?: string) => {
    const trimmed = (wanted ?? name).trim();
    if (!trimmed) return;
    setLooking(true);
    setVersionsError(null);
    try {
      const page = await listDatasetVersions(trimmed);
      setVersions(page.versions);
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setVersions(null);
      setVersionsError(
        err.status === 404
          ? `对象目录里没有叫「${trimmed}」的数据集。`
          : `读 ${trimmed} 的版本失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`
      );
    } finally {
      setLooking(false);
    }
  };

  /**
   * 上传一份文件。服务端把字节写进对象存储，并同时写一份目录行——不写目录行的
   * 上传会被它自己拒掉，因为那样之后没人点得到这份字节的名字。所以上传成功的
   * 结果里带着它登记成了哪个数据集的哪个版本，这里顺手把那一列版本查出来。
   */
  const upload = async (file: File) => {
    setUploading(true);
    try {
      const res = await uploadFile(file);
      appendOutput(
        `已上传 ${res.file_name}（${res.size.toLocaleString('zh-CN')} 字节，摘要 ${res.content_sha256.slice(0, 12)}…），` +
          `登记为数据集 ${res.dataset.name}@${res.dataset.version}`
      );
      setName(res.dataset.name);
      await lookup(res.dataset.name);
    } catch (e) {
      const err = e as { status?: number; message?: string };
      appendOutput(`上传失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setUploading(false);
      if (fileInput.current) fileInput.current.value = '';
    }
  };

  const runVerify = async () => {
    setVerifying(true);
    setVerifyError(null);
    try {
      const report = await verifyDatasets();
      setVerify(report);
      appendOutput(
        report.all_verified
          ? `对象目录校验通过：${report.checked} 份全部对得上`
          : `对象目录校验没通过：${report.checked} 份里 ${report.failed} 份不对`
      );
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setVerify(null);
      setVerifyError(`校验失败（HTTP ${err.status ?? '?'}）：${err.message ?? '未知原因'}`);
    } finally {
      setVerifying(false);
    }
  };

  return (
    <ViewFrame
      title="数据集"
      meta="评测数据集（文件）与对象目录（登记版本）两处，各有各的读面"
      actions={
        <Button tone="accent" onClick={runVerify} disabled={verifying}>
          {verifying ? '校验中…' : '校验对象目录'}
        </Button>
      }
    >
      <div className="p-3">
        {/* ---- 评测数据集 ---- */}
        <section className="mb-4">
          <h2 className="mb-1 text-[13px] text-[var(--paper)]">评测数据集</h2>
          {evalDatasets.error ? (
            <ReadError what="评测数据集" error={evalDatasets.error} onRetry={evalDatasets.refresh} />
          ) : !evalDatasets.data ? (
            <Loading what="评测数据集" />
          ) : !evalDatasets.data.present ? (
            <Empty>
              目录不存在：<span className="mono">{evalDatasets.data.dir}</span>。这不是「还没有数据集」，
              是这个部署没有供给评测数据集目录——往那里放文件不会生效，先要让部署把它挂进来。
            </Empty>
          ) : evalDatasets.data.datasets.length === 0 ? (
            <Empty>
              目录在（<span className="mono">{evalDatasets.data.dir}</span>），里面没有 jsonl / yaml 文件。
              放一份进去就会出现在这里。
            </Empty>
          ) : (
            <>
              <div className="mono mb-1 text-[10px] text-[var(--muted)]">{evalDatasets.data.dir}</div>
              <Table
                columns={[
                  { key: 'name', title: '名字', render: (d) => <span className="mono text-[11px]">{d.name}</span> },
                  { key: 'path', title: '文件', render: (d) => <span className="mono text-[11px] text-[var(--paper-dim)]">{d.path}</span> },
                  {
                    key: 'cases',
                    title: '用例数',
                    width: '80px',
                    render: (d) => (d.case_count > 0 ? String(d.case_count) : <Note text="不是 jsonl 的行数能数的那一种">—</Note>),
                  },
                  { key: 'tags', title: '标签', width: '180px', render: (d) => (d.tags.length ? d.tags.join('、') : '—') },
                ]}
                rows={evalDatasets.data.datasets}
                rowKey={(d) => d.path}
              />
            </>
          )}
        </section>

        {/* ---- 对象目录 ---- */}
        <section>
          <h2 className="mb-1 text-[13px] text-[var(--paper)]">对象目录</h2>
          <FormRow label="按名字查" hint="输入数据集名，列出它登记过的版本（最多 20 条，新的在前）。">
            <span className="flex items-center gap-2">
              <TextInput value={name} onChange={setName} placeholder="如 events-2026-11" width="260px" />
              <Button onClick={() => void lookup()} disabled={looking || !name.trim()}>
                {looking ? '读中…' : '查版本'}
              </Button>
            </span>
          </FormRow>
          <FormRow
            label="上传文件"
            hint="字段名 file。服务端把字节写进对象存储，并登记一行目录（名字取文件名）；对象存储或目录没配时答 503。"
          >
            <span className="flex items-center gap-2">
              <input
                ref={fileInput}
                type="file"
                disabled={uploading}
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) void upload(file);
                }}
                className="text-[11px] text-[var(--paper-dim)] file:mr-2 file:border file:border-[var(--line)] file:bg-[var(--ink-900)] file:px-2 file:py-1 file:text-[11px] file:text-[var(--paper-dim)]"
              />
              {uploading && <span className="text-[11px] text-[var(--muted)]">上传中…</span>}
            </span>
          </FormRow>

          {versionsError && <div className="py-1 text-[11px]" style={{ color: 'var(--danger)' }}>{versionsError}</div>}
          {versions && versions.length === 0 && (
            <Empty>这个数据集没有登记任何版本。名字对，但还没有人往目录里写过。</Empty>
          )}
          {versions && versions.length > 0 && (
            <Table
              columns={[
                { key: 'version', title: '版本', width: '110px', render: (v) => <span className="mono text-[11px]">{v.version}</span> },
                { key: 'bytes', title: '字节', width: '90px', render: (v) => v.bytes.toLocaleString('zh-CN') },
                {
                  key: 'rows',
                  title: '行数',
                  width: '80px',
                  render: (v) => (v.row_count === null ? <Note text="没有这一格就是没有，不是一个 0">—</Note> : String(v.row_count)),
                },
                {
                  key: 'coverage',
                  title: '覆盖时段',
                  render: (v) =>
                    v.coverage_from || v.coverage_to ? (
                      <span className="mono text-[10px]">
                        {v.coverage_from ?? '?'} → {v.coverage_to ?? '?'}
                      </span>
                    ) : (
                      '—'
                    ),
                },
                { key: 'source', title: '来源', width: '120px', render: (v) => v.source || '—' },
                { key: 'media', title: '类型', width: '110px', render: (v) => <span className="mono text-[11px]">{v.media_type}</span> },
                {
                  key: 'sha',
                  title: '摘要',
                  width: '130px',
                  render: (v) => <span className="mono text-[10px] text-[var(--muted)]">{v.content_sha256.slice(0, 12)}…</span>,
                },
                { key: 'created', title: '登记于', width: '120px', render: (v) => <Moment iso={v.created_at} /> },
              ]}
              rows={versions}
              rowKey={(v) => `${v.name}@${v.version}`}
            />
          )}

          <div className="mt-3">
            {verifyError && <div className="py-1 text-[11px]" style={{ color: 'var(--danger)' }}>{verifyError}</div>}
            {verify && <VerifyReport report={verify} />}
          </div>
        </section>
      </div>
    </ViewFrame>
  );
}

function VerifyReport({ report }: { report: DatasetVerifyReport }) {
  return (
    <section>
      <div className="mb-1 flex flex-wrap items-center gap-3 text-[11px]">
        <span style={{ color: report.all_verified ? 'var(--ok)' : 'var(--danger)' }}>
          {report.all_verified
            ? `${report.checked} 份全部对得上`
            : `${report.checked} 份里 ${report.failed} 份不对`}
        </span>
        <span className="text-[var(--muted)]">
          {report.checked === 0
            ? '一份都没有校验：对象目录是空的，一次「什么都没验」的校验不算通过'
            : `通过 ${report.verified} · 没通过 ${report.failed}`}
        </span>
      </div>
      {report.checks.length > 0 && (
        <Table
          columns={[
            { key: 'name', title: '数据集', render: (c: DatasetCheckView) => <span className="mono text-[11px]">{c.name}</span> },
            { key: 'version', title: '版本', width: '110px', render: (c) => <span className="mono text-[11px]">{c.version}</span> },
            {
              key: 'outcome',
              title: '结果',
              width: '110px',
              render: (c) =>
                c.outcome === 'verified' ? (
                  <span style={{ color: 'var(--ok)' }}>对得上</span>
                ) : c.outcome === 'mismatched' ? (
                  <span style={{ color: 'var(--danger)' }}>摘要不符</span>
                ) : c.outcome === 'missing' ? (
                  <span style={{ color: 'var(--danger)' }}>字节不在</span>
                ) : (
                  <span style={{ color: 'var(--warn)' }}>读不了</span>
                ),
            },
            {
              key: 'detail',
              title: '细节',
              render: (c) => {
                if (c.outcome === 'mismatched') {
                  return (
                    <span className="mono text-[10px]">
                      登记 {c.recorded_sha256?.slice(0, 12)}…（{c.recorded_bytes?.toLocaleString('zh-CN')} 字节）· 实际{' '}
                      {c.actual_sha256?.slice(0, 12)}…（{c.actual_bytes?.toLocaleString('zh-CN')} 字节）
                    </span>
                  );
                }
                if (c.outcome === 'unreadable') return <span className="text-[11px]">{c.error ?? '未知原因'}</span>;
                return '—';
              },
            },
          ]}
          rows={report.checks}
          rowKey={(c) => `${c.name}@${c.version}`}
        />
      )}
    </section>
  );
}

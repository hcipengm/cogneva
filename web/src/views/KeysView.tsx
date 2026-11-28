import { useState } from 'react';
import {
  getSecurityKeys,
  issueSecurityKey,
  revokeSecurityKey,
  rotateSecurityKey,
  type SecurityKey,
} from '@/api/client';
import { useResource } from '@/hooks/useResource';
import { Empty, ReadError, ViewFrame } from '@/ui/primitives';

/**
 * 密钥面：签名钥匙的签发、轮换、吊销，以及此刻有哪几把。
 *
 * 私钥不经过这一页：页面交出的是公钥与指纹（私钥在 KMS/密钥库里生成）。三个动作
 * 各写一条事实；「谁、何时、哪把钥、为什么」由事实回答，页面只是它的读数。
 */
export function KeysView() {
  const [refresh, setRefresh] = useState(0);
  const keys = useResource(`签名钥匙（${refresh}）`, getSecurityKeys, 60_000);
  const [actor, setActor] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const rows: SecurityKey[] = keys.data ?? [];

  const run = async (action: () => Promise<void>, done: string) => {
    try {
      await action();
      setMessage(done);
      setRefresh((n) => n + 1);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <ViewFrame
      title="密钥"
      meta="签发 / 轮换 / 吊销各写一条事实；私钥不进这一页，也不进事件"
    >
      {keys.error ? <ReadError what="签名钥匙" error={keys.error} /> : null}
      {rows.length === 0 && !keys.error ? <Empty>还没有密钥</Empty> : null}
      {rows.length > 0 ? (
        <table className="table">
          <thead>
            <tr>
              <th>key id</th>
              <th>状态</th>
              <th>用途</th>
              <th>指纹</th>
              <th>重叠到</th>
              <th>吊销原因</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((key) => (
              <tr key={key.key_id}>
                <td>{key.key_id}</td>
                <td>{key.state}</td>
                <td>{key.purpose || '—'}</td>
                <td>{key.fingerprint || '—'}</td>
                <td>{key.overlap_until || '—'}</td>
                <td>{key.revoked_reason || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}

      <label>
        操作者
        <input value={actor} onChange={(e) => setActor(e.target.value)} />
      </label>

      <IssueForm actor={actor} run={run} />
      <RotateForm actor={actor} run={run} />
      <RevokeForm actor={actor} run={run} />

      {message ? <p>{message}</p> : null}
    </ViewFrame>
  );
}

type Run = (action: () => Promise<void>, done: string) => Promise<void>;

function IssueForm({ actor, run }: { actor: string; run: Run }) {
  const [keyId, setKeyId] = useState('');
  const [purpose, setPurpose] = useState('signing');
  const [publicKey, setPublicKey] = useState('');
  const [fingerprint, setFingerprint] = useState('');
  return (
    <fieldset>
      <legend>签发</legend>
      <input placeholder="key id" value={keyId} onChange={(e) => setKeyId(e.target.value)} />
      <input placeholder="用途" value={purpose} onChange={(e) => setPurpose(e.target.value)} />
      <input
        placeholder="公钥（base64）"
        value={publicKey}
        onChange={(e) => setPublicKey(e.target.value)}
      />
      <input
        placeholder="指纹"
        value={fingerprint}
        onChange={(e) => setFingerprint(e.target.value)}
      />
      <button
        disabled={!keyId || !publicKey || !fingerprint || !actor}
        onClick={() =>
          run(
            () =>
              issueSecurityKey({
                key_id: keyId,
                purpose,
                public_key: publicKey,
                fingerprint,
                actor,
              }),
            `已签发 ${keyId}`
          )
        }
      >
        签发
      </button>
    </fieldset>
  );
}

function RotateForm({ actor, run }: { actor: string; run: Run }) {
  const [keyId, setKeyId] = useState('');
  const [previous, setPrevious] = useState('');
  const [publicKey, setPublicKey] = useState('');
  const [fingerprint, setFingerprint] = useState('');
  const [overlap, setOverlap] = useState('');
  return (
    <fieldset>
      <legend>轮换</legend>
      <input placeholder="新 key id" value={keyId} onChange={(e) => setKeyId(e.target.value)} />
      <input
        placeholder="旧 key id"
        value={previous}
        onChange={(e) => setPrevious(e.target.value)}
      />
      <input
        placeholder="新公钥（base64）"
        value={publicKey}
        onChange={(e) => setPublicKey(e.target.value)}
      />
      <input
        placeholder="新指纹"
        value={fingerprint}
        onChange={(e) => setFingerprint(e.target.value)}
      />
      <input
        placeholder="重叠到（RFC 3339，可空）"
        value={overlap}
        onChange={(e) => setOverlap(e.target.value)}
      />
      <button
        disabled={!keyId || !previous || !publicKey || !fingerprint || !actor}
        onClick={() =>
          run(
            () =>
              rotateSecurityKey({
                key_id: keyId,
                previous_key_id: previous,
                public_key: publicKey,
                fingerprint,
                overlap_until: overlap || undefined,
                actor,
              }),
            `已轮换 ${keyId}`
          )
        }
      >
        轮换
      </button>
    </fieldset>
  );
}

function RevokeForm({ actor, run }: { actor: string; run: Run }) {
  const [keyId, setKeyId] = useState('');
  const [reason, setReason] = useState('');
  return (
    <fieldset>
      <legend>吊销</legend>
      <input placeholder="key id" value={keyId} onChange={(e) => setKeyId(e.target.value)} />
      <input placeholder="原因" value={reason} onChange={(e) => setReason(e.target.value)} />
      <button
        disabled={!keyId || !reason || !actor}
        onClick={() =>
          run(
            () => revokeSecurityKey({ key_id: keyId, reason, actor }),
            `已吊销 ${keyId}`
          )
        }
      >
        吊销
      </button>
    </fieldset>
  );
}

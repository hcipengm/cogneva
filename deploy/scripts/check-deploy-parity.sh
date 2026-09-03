#!/usr/bin/env bash
# 部署拓扑 parity 校验：Helm chart 是应用拓扑的唯一权威源，deploy/k3s/ 静态清单
# （bootstrap 消费）必须与 chart 的 k3s profile 渲染结果能力对齐——工作负载一个
# 不少、env/卷/挂载/端口/SA 字段不弱。任何一侧改动后跑本脚本，差异即失败。
#
# 用法：bash deploy/scripts/check-deploy-parity.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

for bin in helm kubectl python3; do
  command -v "$bin" >/dev/null || { echo "缺少依赖：$bin" >&2; exit 2; }
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

kubectl kustomize deploy/k3s > "$TMP/k3s.yaml"

# k3s profile：单节点 K3s 形态（与 deploy/k3s 静态清单语义一一对应）。
# 渲染 apply 路径不带 Secret（内部密钥由 init-secrets.sh 安装时生成）。
helm template cogneva deploy/helm/cogneva \
  --set image.tag=local \
  --set evolution.gitRemote.mode=hostPath \
  --set evolution.gitRemote.hostPath=/var/lib/cogneva-data/git-remote \
  --set gitops.kubectlBin.enabled=true \
  --set gitops.kubectlBin.hostPath=/usr/local/bin/k3s \
  --set buildah.containerdSocket=/run/k3s/containerd \
  --set secrets.create=false \
  > "$TMP/helm.yaml"

python3 - "$TMP/k3s.yaml" "$TMP/helm.yaml" <<'PYEOF'
import sys, yaml

def load(path):
    return {(d['kind'], d['metadata']['name']): d
            for d in yaml.safe_load_all(open(path)) if d and d.get('kind')}

def pod_spec(doc):
    s = doc.get('spec', {})
    return s['template']['spec'] if 'template' in s else s

def vol_key(v):
    if 'configMap' in v: return 'configMap:' + v['configMap'].get('name', '?')
    if 'hostPath' in v: return 'hostPath:' + v['hostPath'].get('path', '?')
    if 'persistentVolumeClaim' in v:
        return 'pvc:' + v['persistentVolumeClaim'].get('claimName', '?')
    if 'emptyDir' in v: return 'emptyDir'
    if 'secret' in v: return 'secret:' + v['secret'].get('secretName', '?')
    return str([k for k in v if k != 'name'])

def workload(doc):
    ps = pod_spec(doc)
    out = {'sa': ps.get('serviceAccountName', '(default)'),
           'automount': ps.get('automountServiceAccountToken', '(default)'),
           'volumes': sorted(f"{v['name']}={vol_key(v)}" for v in ps.get('volumes', []))}
    conts = {}
    for c in ps.get('containers', []) + ps.get('initContainers', []):
        conts[c['name']] = {
            'args': ' '.join(c.get('args', [])),
            'ports': sorted(f"{p.get('name','')}:{p['containerPort']}" for p in c.get('ports', [])),
            'envFrom': sorted((e.get('configMapRef') or {}).get('name')
                              or (e.get('secretRef') or {}).get('name', '?')
                              for e in c.get('envFrom', [])),
            'env': sorted(e['name'] for e in c.get('env', [])),
            'mounts': sorted(f"{m['name']}->{m['mountPath']}" for m in c.get('volumeMounts', [])),
        }
    out['containers'] = conts
    return out

def svc(doc):
    s = doc['spec']
    return {'ports': sorted(f"{p.get('name','')}:{p['port']}->{p.get('targetPort','')}"
                            for p in s.get('ports', []))}

def cm(doc):
    return {'keys': sorted(doc.get('data', {}).keys())}

k, h = load(sys.argv[1]), load(sys.argv[2])
errors = []

only_k = sorted(set(k) - set(h))
only_h = sorted(set(h) - set(k))
for r in only_k: errors.append(f"helm 缺失资源 {r[0]}/{r[1]}")
for r in only_h: errors.append(f"helm 多出资源 {r[0]}/{r[1]}（k3s profile 不应有）")

for name in sorted(set(k) & set(h)):
    kind = name[0]
    if kind in ('Deployment', 'StatefulSet', 'DaemonSet'):
        ka, ha = workload(k[name]), workload(h[name])
        for key in ('sa', 'automount'):
            if ka[key] != ha[key]:
                errors.append(f"{kind}/{name[1]} {key}: k3s={ka[key]} helm={ha[key]}")
        for v in sorted(set(ka['volumes']) - set(ha['volumes'])):
            errors.append(f"{kind}/{name[1]} volume k3s-only: {v}")
        for v in sorted(set(ha['volumes']) - set(ka['volumes'])):
            errors.append(f"{kind}/{name[1]} volume helm-only: {v}")
        for cn in sorted(set(ka['containers']) | set(ha['containers'])):
            kc, hc = ka['containers'].get(cn), ha['containers'].get(cn)
            if not kc: errors.append(f"{kind}/{name[1]} container helm-only: {cn}"); continue
            if not hc: errors.append(f"{kind}/{name[1]} container k3s-only: {cn}"); continue
            if kc['args'] != hc['args']:
                errors.append(f"{kind}/{name[1]} [{cn}] args: k3s={kc['args']!r} helm={hc['args']!r}")
            for f in ('ports', 'envFrom', 'mounts', 'env'):
                for x in sorted(set(kc[f]) - set(hc[f])):
                    errors.append(f"{kind}/{name[1]} [{cn}] {f} k3s-only: {x}")
                for x in sorted(set(hc[f]) - set(kc[f])):
                    errors.append(f"{kind}/{name[1]} [{cn}] {f} helm-only: {x}")
    elif kind == 'Service':
        ka, ha = svc(k[name]), svc(h[name])
        for x in sorted(set(ka['ports']) - set(ha['ports'])):
            errors.append(f"Service/{name[1]} port k3s-only: {x}")
        for x in sorted(set(ha['ports']) - set(ka['ports'])):
            errors.append(f"Service/{name[1]} port helm-only: {x}")
    elif kind == 'ConfigMap':
        ka, ha = cm(k[name]), cm(h[name])
        for x in sorted(set(ka['keys']) - set(ha['keys'])):
            errors.append(f"ConfigMap/{name[1]} key k3s-only: {x}")
        for x in sorted(set(ha['keys']) - set(ka['keys'])):
            errors.append(f"ConfigMap/{name[1]} key helm-only: {x}")

if errors:
    print("PARITY 校验失败：")
    for e in errors: print("  - " + e)
    sys.exit(1)
print(f"PARITY OK：{len(k)} 个资源，工作负载字段（env/卷/挂载/端口/SA）全对齐")
PYEOF

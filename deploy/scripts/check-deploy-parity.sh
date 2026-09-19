#!/usr/bin/env bash
# 部署拓扑 parity 校验：Helm chart 是应用拓扑的唯一权威源，deploy/k3s/ 静态清单
# （bootstrap 消费）必须与 chart 的 k3s profile 渲染结果能力对齐——工作负载一个
# 不少、env/卷/挂载/端口/SA 字段不弱、配置文档一段不缺。任何一侧改动后跑本
# 脚本，差异即失败。
#
# ConfigMap 里真正的配置面常常不是 ConfigMap 的键，而是被嵌进某个值里的整份
# 文档（cogneva.json 挂进来时键只有一个，少掉的字段藏在值里），所以值是 JSON
# 的按字段逐层比，不能只比键名。
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
import sys, json, yaml

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

UNIT = {'n': 1e-9, 'u': 1e-6, 'm': 1e-3, '': 1.0, 'k': 1e3, 'M': 1e6, 'G': 1e9,
        'Ki': 2 ** 10, 'Mi': 2 ** 20, 'Gi': 2 ** 30, 'Ti': 2 ** 40}

def norm_qty(v):
    """Kubernetes resources: `1` and `1000m` are the same CPU amount. Comparing
    the spelling instead of the amount would make the gate fail on a synonym and
    pass on a real difference hidden behind a different unit."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    for suf in ('Ki', 'Mi', 'Gi', 'Ti', 'k', 'M', 'G', 'm', 'n', 'u', ''):
        if suf and not s.endswith(suf):
            continue
        try:
            return float(s[: len(s) - len(suf)] or 0) * UNIT[suf]
        except ValueError:
            break
    return s

def norm_resources(r):
    if not r:
        return None
    out = {}
    for sec in ('requests', 'limits'):
        out[sec] = sorted((k, norm_qty(v)) for k, v in (r.get(sec) or {}).items())
    return json.dumps(out, default=str, sort_keys=True)

def norm_probes(c):
    return json.dumps({k: c[k] for k in ('startupProbe', 'livenessProbe', 'readinessProbe') if k in c},
                      sort_keys=True)

def norm_command(c):
    """Shell bodies (init container scripts) exist twice: once in the chart
    template, once in the static manifest the GitOps consumer applies verbatim.
    A real divergence means the two install paths run different code, so compare
    them — but only full-line `#` comments are dropped, because the two sides
    word their comments differently on purpose. Inline comments are kept: a
    false positive here costs one line of noise, a missed body difference costs
    two divergent provisioning paths."""
    parts = []
    for x in c.get('command') or []:
        body = '\n'.join(l for l in x.splitlines() if not l.lstrip().startswith('#'))
        parts.append(body.strip())
    return '\n'.join(parts)

def workload(doc):
    ps = pod_spec(doc)
    out = {'sa': ps.get('serviceAccountName', '(default)'),
           'automount': ps.get('automountServiceAccountToken', '(default)'),
           # 探针 / 资源声明 / securityContext 也是能力面：少了 liveness 的进程
           # 卡死不会被重启，少了 request 的 Pod 是 BestEffort（节点有压力先被
           # 驱逐），少了 securityContext 的进程以镜像默认用户跑。它们不在
           # env/卷/挂载里，只比那三类会让"渲染路径少一整套探针"无声通过。
           'securityContext': json.dumps(ps.get('securityContext'), sort_keys=True),
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
            'resources': norm_resources(c.get('resources')),
            'probes': norm_probes(c),
            'command': norm_command(c),
            'securityContext': json.dumps(c.get('securityContext'), sort_keys=True),
            'workingDir': c.get('workingDir', ''),
        }
    out['containers'] = conts
    return out

def svc(doc):
    s = doc['spec']
    return {'ports': sorted(f"{p.get('name','')}:{p['port']}->{p.get('targetPort','')}"
                            for p in s.get('ports', []))}

def cm(doc):
    return doc.get('data', {})

def shortened(v, limit=200):
    """A value can be a whole document; printing it raw buries every other
    finding, so cap the excerpt but keep the size visible."""
    s = repr(v)
    return s if len(s) <= limit else f"{s[:limit]}…(+{len(s) - limit} chars)"

def cfg_value(raw):
    """The config document a ConfigMap value carries. JSON first (cogneva.json),
    then YAML (prompt and manifest fragments), else plain text with only
    trailing whitespace normalized — a block scalar's final newline differs
    between `|` and `|-` styles without the content differing."""
    try:
        return json.loads(raw)
    except ValueError:
        pass
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError:
        pass
    return '\n'.join(line.rstrip() for line in raw.rstrip().splitlines())

def json_diff(a, b, path=''):
    """Yield every place two config trees disagree, by path."""
    if type(a) is not type(b):
        yield f"{path}: k3s={shortened(a)} helm={shortened(b)}"
    elif isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                yield f"{path}.{k}: helm-only ({shortened(b[k])})"
            elif k not in b:
                yield f"{path}.{k}: k3s-only ({shortened(a[k])})"
            else:
                yield from json_diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list):
        if len(a) != len(b):
            yield f"{path}: k3s has {len(a)} entries, helm {len(b)}"
        else:
            for i, (x, y) in enumerate(zip(a, b)):
                yield from json_diff(x, y, f"{path}[{i}]")
    elif a != b:
        yield f"{path}: k3s={shortened(a)} helm={shortened(b)}"

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
        for key in ('sa', 'automount', 'securityContext'):
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
            for f in ('resources', 'probes', 'securityContext', 'workingDir', 'command'):
                if kc[f] != hc[f]:
                    errors.append(f"{kind}/{name[1]} [{cn}] {f}: k3s={kc[f]} helm={hc[f]}")
    elif kind == 'Service':
        ka, ha = svc(k[name]), svc(h[name])
        for x in sorted(set(ka['ports']) - set(ha['ports'])):
            errors.append(f"Service/{name[1]} port k3s-only: {x}")
        for x in sorted(set(ha['ports']) - set(ka['ports'])):
            errors.append(f"Service/{name[1]} port helm-only: {x}")
    elif kind in ('ResourceQuota', 'LimitRange', 'PersistentVolumeClaim'):
        # 治理对象与卷声明的数值是能力面的一部分（上限定小了扩容时新 Pod 会被
        # 直接拒绝，卷声明写小了应用会写爆也无人报错），所以整份 spec 逐字段
        # 比，不接受"名字对上就算对齐"。
        for d in json_diff(k[name].get('spec'), h[name].get('spec'), 'spec'):
            errors.append(f"{kind}/{name[1]}/{d}")
    elif kind == 'ConfigMap':
        ka, ha = cm(k[name]), cm(h[name])
        for x in sorted(set(ka) - set(ha)):
            errors.append(f"ConfigMap/{name[1]} key k3s-only: {x}")
        for x in sorted(set(ha) - set(ka)):
            errors.append(f"ConfigMap/{name[1]} key helm-only: {x}")
        # 值里的配置文档逐字段比。挂进来的 cogneva.json 键只有一个，真正的配置
        # 面全在值里，只比键名会让「少一段配置」无声通过。
        for x in sorted(set(ka) & set(ha)):
            for d in json_diff(cfg_value(ka[x]), cfg_value(ha[x]), x):
                errors.append(f"ConfigMap/{name[1]}/{d}")

if errors:
    print("PARITY 校验失败：")
    for e in errors: print("  - " + e)
    sys.exit(1)
print(f"PARITY OK：{len(k)} 个资源，工作负载字段与卷声明全对齐")
PYEOF

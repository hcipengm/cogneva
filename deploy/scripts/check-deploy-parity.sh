#!/usr/bin/env bash
# 部署拓扑 parity 校验：Helm chart 是应用拓扑的唯一权威源，deploy/k3s/ 静态清单
# （bootstrap 消费）必须与 chart 的 k3s profile 渲染结果能力对齐。任何一侧改动后
# 跑本脚本，差异即失败。
#
# 比什么：资源集合一个不少不多，且两侧都不是空的；每工作负载的 sa/automount/pod
# securityContext/整份卷声明/Pod 模板的 labels 与 annotations（Pod 标签是
# NetworkPolicy 的 podSelector 与 Service 的 selector 的匹配面，改名即换受众）；
# 每容器的 args/端口(含协议)/挂载(含只读)/env(含取值来源)/envFrom(含 optional 与
# prefix)/命令正文/探针/资源声明/securityContext/workingDir；Service 的
# type/selector/clusterIP 与其上的其余 spec 字段；NetworkPolicy、Ingress、
# StorageClass、Namespace、ServiceAccount 的整份对象（NetworkPolicy 那四条就是
# 零凭证红线落地的地方）；CronJob 的调度、Job 级字段与它的 Pod 模板；治理对象与
# PVC 的整份 spec；ConfigMap 的键与键值内的配置文档；Role/ClusterRole 的规则集合
# 与 RoleBinding/ClusterRoleBinding 的 subjects+roleRef（少一条授权就是生产里一条
# 路径 Forbidden，多一条是扩权，两侧都可能不同）。
# 判据面按「凡两侧都可能不同、且不同就改变运行时行为」取，不按「历史上错过什么」取
# —— 只比错过的那几类，下一类漂移照样无声通过。
#
# 每一类的比对方式都是一次决定，不是默认值：两侧都出现的资源类型必须在
# COVERED_KINDS 里有答案，否则失败。此前有整类资源只比了名字（名字对上就算对齐），
# 而漂移恰好落在没比的那几个字段上时，门禁一个字都不会说。
#
# 安装来源标签两侧按构造不同：静态清单写 app.kubernetes.io/managed-by=k3s、对象级
# name 写对象自己的名字，chart 渲染写 Helm 并多出 instance/version、name 写 chart
# 名。它们说的是「这份清单是谁装的」，不是对象做什么；留着它们比，会让每一条已经
# 对齐的资源都报一次假差异，把真差异埋进噪声里（真读标签的是选择器，而选择器在
# spec 里、按字段比）。
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

def load(path, side):
    """Documents by (kind, name).

    Two documents with the same key are not "the same resource listed twice" --
    the later one silently wins, so the side that lost is never compared and the
    gate reports on a tree nobody installs. Keying a comprehension on the pair
    makes that collapse invisible; counting it makes it a failure.
    """
    out, dupes = {}, []
    for d in yaml.safe_load_all(open(path)):
        if not d or not d.get('kind'):
            continue
        key = (d['kind'], d['metadata']['name'])
        if key in out:
            dupes.append(key)
        out[key] = d
    if dupes:
        raise SystemExit(f"{side}里有 {len(dupes)} 个同名资源："
                         + ', '.join(f"{k}/{n}" for k, n in sorted(dupes))
                         + "（后一份会顶掉前一份，被顶掉的那份没人比）")
    return out

def pod_spec(doc):
    s = doc.get('spec', {})
    return s['template']['spec'] if 'template' in s else s

def pod_meta(doc):
    s = doc.get('spec', {})
    return (s['template'].get('metadata') or {}) if 'template' in s else {}

def pod_doc(doc, kind):
    """A CronJob's pods sit one level deeper (spec.jobTemplate.spec.template).
    Rewrapping them into the shape a Deployment has lets the same comparison run
    on them, instead of a second, weaker one being written for the same object."""
    if kind != 'CronJob':
        return doc
    return {'spec': {'template': doc['spec']['jobTemplate']['spec']['template']}}

# 对象级不比的安装来源标签（见文件头）。`name` 在这里也在内：同一个对象在静态清单里
# 叫 `cogneva-evolution`、在 chart 渲染里叫 `cogneva`，指的是同一件事。
OBJECT_INSTALL_LABELS = {
    'app.kubernetes.io/managed-by', 'app.kubernetes.io/instance',
    'app.kubernetes.io/version', 'app.kubernetes.io/name',
}
# Pod 模板上只剔前三者：Pod 的 name/component 是选择器真正匹配的那两个键，两侧值
# 一致，留着比才看得见「一边改名、另一边没改」——改名不报错，只换掉受众。
POD_INSTALL_LABELS = OBJECT_INSTALL_LABELS - {'app.kubernetes.io/name'}

def strip_labels(labels, drop):
    return {k: v for k, v in (labels or {}).items() if k not in drop}

def obj_sig(doc, drop=OBJECT_INSTALL_LABELS):
    """对象里「它是干什么的」那一份：整份对象减 apiVersion/kind 与安装来源标签。

    只比过名字的那几类，漂移从来不落在名字上——NetworkPolicy 的 egress 白名单、
    StorageClass 的 reclaimPolicy、Ingress 的超时与体积注解、Namespace 的标签，
    改掉哪一笔名字都不动。所以按对象比，再把两侧按构造不同的那几笔剔掉。
    """
    out = {k: v for k, v in doc.items() if k not in ('apiVersion', 'kind')}
    meta = dict(out.get('metadata') or {})
    labels = strip_labels(meta.get('labels'), drop)
    if labels:
        meta['labels'] = labels
    else:
        meta.pop('labels', None)
    # helm 模板把未设置的 creationTimestamp 显式渲染成 null，静态清单里没有这个键：
    # 同一个事实的两种写法。
    if meta.get('creationTimestamp') is None:
        meta.pop('creationTimestamp', None)
    out['metadata'] = meta
    return out

def vol_key(v):
    if 'configMap' in v: return 'configMap:' + v['configMap'].get('name', '?')
    if 'hostPath' in v: return 'hostPath:' + v['hostPath'].get('path', '?')
    if 'persistentVolumeClaim' in v:
        return 'pvc:' + v['persistentVolumeClaim'].get('claimName', '?')
    if 'emptyDir' in v: return 'emptyDir'
    if 'secret' in v: return 'secret:' + v['secret'].get('secretName', '?')
    return str([k for k in v if k != 'name'])

def vol_sig(v):
    """A volume's runtime behaviour is the whole block, not its type and target.
    `hostPath.type` decides whether a missing directory is a mount error or gets
    created (the difference between a working rollout and a pod stuck in
    ContainerCreating); `configMap.items`/`defaultMode` decide which files appear
    and with which permissions; `emptyDir.medium` decides whether it is disk or
    memory. Keying only on path/name lets all of those drift unnoticed, so the
    readable key stays as a prefix and the exact spec is compared after it."""
    return f"{v['name']}={vol_key(v)} " + json.dumps(
        {k: x for k, x in v.items() if k != 'name'}, sort_keys=True, default=str)

def port_sig(p):
    """`protocol` defaults to TCP, so an absent key and an explicit `TCP` are the
    same port and must not read as a difference. A UDP flip on the same name and
    number is a different wire protocol, and nothing else in the spec reveals it."""
    return f"{p.get('name','')}:{p['containerPort']}/{p.get('protocol') or 'TCP'}"

def mount_sig(m):
    """`readOnly` absent and `false` are the same mount (Kubernetes defaults it to
    false), so normalize to presence. `true` is not the same thing: a mount a
    process needs to write turned read-only is a runtime failure the API server
    accepts, and mountPath alone cannot see it."""
    return f"{m['name']}->{m['mountPath']}" + (':ro' if m.get('readOnly') else '')

def env_sig(e):
    """The variable's name does not decide behaviour — its source does. The same
    name pointed at a different ConfigMap key, a different literal, or a different
    Secret field is a different configuration, and the two install paths (chart
    render vs the static manifest the in-cluster consumer applies verbatim) feed
    different processes. Comparing names only would call a swapped value aligned."""
    return f"{e['name']}=" + json.dumps({k: x for k, x in e.items() if k != 'name'},
                                        sort_keys=True, default=str)

def envfrom_sig(e):
    """Same reasoning one level up: `prefix` renames every variable that source
    contributes, and `optional` decides whether a missing Secret blocks startup
    or degrades. Both are contract, not decoration."""
    kind = 'configMapRef' if 'configMapRef' in e else 'secretRef'
    ref = e.get(kind) or {}
    sig = f"{kind}:{ref.get('name','?')} optional={bool(ref.get('optional'))}"
    return sig + (f" prefix={e['prefix']}" if e.get('prefix') else '')

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
    pm = pod_meta(doc)
    out = {'sa': ps.get('serviceAccountName', '(default)'),
           'automount': ps.get('automountServiceAccountToken', '(default)'),
           # 探针 / 资源声明 / securityContext 也是能力面：少了 liveness 的进程
           # 卡死不会被重启，少了 request 的 Pod 是 BestEffort（节点有压力先被
           # 驱逐），少了 securityContext 的进程以镜像默认用户跑。它们不在
           # env/卷/挂载里，只比那三类会让"渲染路径少一整套探针"无声通过。
           'securityContext': json.dumps(ps.get('securityContext'), sort_keys=True),
           # Pod 标签不是装饰：NetworkPolicy 的 podSelector 与 Service 的 selector
           # 都按它匹配，两侧一个键不同就是另一批 Pod 被放行/被寻址，而 Pod 本身
           # 照样 Running、探针照样绿。注解同理（prometheus.io/* 决定谁被抓取）。
           # 部署来源、镜像版本这类标签两侧按构造不同，剔除（见文件头）。
           'templateLabels': json.dumps(strip_labels(pm.get('labels'), POD_INSTALL_LABELS),
                                        sort_keys=True),
           'templateAnnotations': json.dumps(pm.get('annotations') or {}, sort_keys=True),
           'volumes': sorted(vol_sig(v) for v in ps.get('volumes', []))}
    conts = {}
    for c in ps.get('containers', []) + ps.get('initContainers', []):
        conts[c['name']] = {
            'args': ' '.join(c.get('args', [])),
            'ports': sorted(port_sig(p) for p in c.get('ports', [])),
            'envFrom': sorted(envfrom_sig(e) for e in c.get('envFrom', [])),
            'env': sorted(env_sig(e) for e in c.get('env', [])),
            'mounts': sorted(mount_sig(m) for m in c.get('volumeMounts', [])),
            'resources': norm_resources(c.get('resources')),
            'probes': norm_probes(c),
            'command': norm_command(c),
            'securityContext': json.dumps(c.get('securityContext'), sort_keys=True),
            'workingDir': c.get('workingDir', ''),
        }
    out['containers'] = conts
    return out

def role_rules(doc):
    """A Role as a comparable set of rules.

    Authorization is a set, not a sequence: the order of rules, and the order of
    apiGroups/resources/verbs inside one, carry no meaning -- a rule that grants
    the same three verbs in another order is the same permission. What does
    change behaviour is which grants exist: one missing verb turns a path
    Forbidden in production while every other gate stays green (the governance
    drift reading lost its cluster side exactly that way), and one extra verb is
    a widened capability. Neither side's spelling is the judge here, so both are
    normalized before they are compared."""
    return sorted(
        json.dumps(
            {k: (sorted(v) if isinstance(v, list) else v) for k, v in r.items()},
            sort_keys=True,
            default=str,
        )
        for r in (doc.get('rules') or [])
    )

def binding_sig(doc):
    """A binding's whole decision: who, and to what.

    Subjects are a set (one subject listed twice is one subject), and roleRef is
    the pair that actually selects the permissions. Comparing the subject list
    alone would let a binding keep its subject while pointing at a different
    Role."""
    subjects = sorted(
        json.dumps(s, sort_keys=True, default=str) for s in (doc.get('subjects') or [])
    )
    return json.dumps({'subjects': subjects, 'roleRef': doc.get('roleRef')},
                      sort_keys=True, default=str)

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

# 两侧都出现的资源类型都必须在这里有答案：比对方式是一次决定，没决定就不算比过。
COVERED_KINDS = {
    'Deployment', 'StatefulSet', 'DaemonSet', 'CronJob', 'Service',
    'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding',
    'ResourceQuota', 'LimitRange', 'PersistentVolumeClaim', 'ConfigMap',
    'NetworkPolicy', 'Ingress', 'StorageClass', 'Namespace', 'ServiceAccount',
}

k = load(sys.argv[1], 'deploy/k3s 渲染结果')
h = load(sys.argv[2], 'chart k3s profile 渲染结果')
if not k or not h:
    raise SystemExit(f"两侧都渲染出东西才谈得上对齐：k3s={len(k)} 个资源，"
                     f"helm={len(h)} 个资源（两侧都空时没有一条比对有题目，全绿是句空话）")
errors = []

only_k = sorted(set(k) - set(h))
only_h = sorted(set(h) - set(k))
for r in only_k: errors.append(f"helm 缺失资源 {r[0]}/{r[1]}")
for r in only_h: errors.append(f"helm 多出资源 {r[0]}/{r[1]}（k3s profile 不应有）")

both = sorted(set(k) & set(h))
for kind in sorted({n[0] for n in both}):
    if kind not in COVERED_KINDS:
        named = ', '.join(n[1] for n in both if n[0] == kind)
        errors.append(f"资源类型 {kind} 两侧都有（{named}），但没人决定怎么比："
                      "在脚本里写出它的比对方式，或显式记下为什么这一类比不了")

for name in sorted(both):
    kind = name[0]
    if kind in ('Deployment', 'StatefulSet', 'DaemonSet', 'CronJob'):
        ka = workload(pod_doc(k[name], kind))
        ha = workload(pod_doc(h[name], kind))
        for key in ('sa', 'automount', 'securityContext', 'templateLabels',
                    'templateAnnotations'):
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
        if kind == 'CronJob':
            # Pod 模板之外还有两处决定它做什么：调度与 Job 级策略。schedule 是它什么
            # 时候跑，concurrencyPolicy 是上一轮没跑完时这一轮怎么办，suspend 是关掉
            # 它，backoffLimit 与 ttlSecondsAfterFinished 是失败重试几次、留下的 Job
            # 多久清掉。只比 Pod 模板，「备份改成每小时一次但只改了一侧」会全绿。
            for level, kd, hd in (
                ('spec', k[name]['spec'], h[name]['spec']),
                ('jobTemplate.spec', k[name]['spec'].get('jobTemplate', {}).get('spec', {}),
                 h[name]['spec'].get('jobTemplate', {}).get('spec', {})),
            ):
                skip = 'jobTemplate' if level == 'spec' else 'template'
                for d in json_diff({x: y for x, y in kd.items() if x != skip},
                                   {x: y for x, y in hd.items() if x != skip}, level):
                    errors.append(f"CronJob/{name[1]}/{d}")
    elif kind == 'Service':
        ka, ha = svc(k[name]), svc(h[name])
        for x in sorted(set(ka['ports']) - set(ha['ports'])):
            errors.append(f"Service/{name[1]} port k3s-only: {x}")
        for x in sorted(set(ha['ports']) - set(ka['ports'])):
            errors.append(f"Service/{name[1]} port helm-only: {x}")
        # 端口之外的那几个字段同样是能力面：type 从 ClusterIP 换成 NodePort 是把服务
        # 摊到每个节点上，selector 换一个值是换掉它指向的 Pod（Pod 照样 Running，
        # 只是没人再指向它），clusterIP=None 是 headless（客户端直连 Pod）。
        for d in json_diff({x: y for x, y in k[name]['spec'].items() if x != 'ports'},
                           {x: y for x, y in h[name]['spec'].items() if x != 'ports'},
                           'spec'):
            errors.append(f"Service/{name[1]}/{d}")
    elif kind in ('Role', 'ClusterRole'):
        # 权限面也在这里比：少一条授权就是生产里某条路径 Forbidden（治理面漂移
        # 读数就这么瞎了），多一条是扩权。规则按集合比，不比字面顺序。
        ka, ha = role_rules(k[name]), role_rules(h[name])
        for x in sorted(set(ha) - set(ka)):
            errors.append(f"{kind}/{name[1]} rule helm-only: {x}")
        for x in sorted(set(ka) - set(ha)):
            errors.append(f"{kind}/{name[1]} rule k3s-only: {x}")
    elif kind in ('RoleBinding', 'ClusterRoleBinding'):
        ka, ha = binding_sig(k[name]), binding_sig(h[name])
        if ka != ha:
            errors.append(f"{kind}/{name[1]}: k3s={ka} helm={ha}")
    elif kind in ('NetworkPolicy', 'Ingress', 'StorageClass', 'Namespace',
                  'ServiceAccount'):
        # 这五类原先只比名字。其中四条 NetworkPolicy 正是零凭证红线落地的地方
        # （进化 Pod 的 egress 白名单、沙盒执行器的放行端口），StorageClass 的
        # reclaimPolicy 决定卷删了数据还在不在，Ingress 的注解决定一个请求能不能
        # 过得去。名字对上从来不代表它们做同一件事。
        for d in json_diff(obj_sig(k[name]), obj_sig(h[name]), 'object'):
            errors.append(f"{kind}/{name[1]}/{d}")
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
print(f"PARITY OK：{len(k)} 个资源 / {len({n[0] for n in both})} 类（每类都有决定好的比对方式，"
      "安装来源标签除外），全部逐字段对齐")
PYEOF

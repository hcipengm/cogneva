#!/usr/bin/env bash
# 治理数值的结构判据：ResourceQuota 的额度与 LimitRange 的上下限是**命名空间
# 准入**面，而声明它们的是同一套清单里的工作负载与卷。两者必须自洽，否则这套
# 清单装不进自己的命名空间——而失败发生在安装/落地时，读到的错误是"声明越界"，
# 跟"新版本好不好"毫无关系，却足以让整条落地通道停下（实机事故：卷声明和
# 188Gi vs 配额 110Gi，三个 PVC 被拒，落地通道断了整天）。数值本身是策略，
# 由 values 定；这个脚本不管数值该是多少，只管它们彼此不矛盾，并把头寸打出来，
# 让"剩多少"永远可见而不是靠人记。
#
# 校核的四条（都从渲染产物推导，不重复 values 里的任何常量）：
#   1. 每条卷声明都在 LimitRange 的 PVC 上下限内
#   2. 卷声明之和 ≤ ResourceQuota 的 requests.storage，且头寸 > 0（打印头寸）
#   3. 每个容器显式声明的 requests/limits 都在 LimitRange 的容器上下限内；
#      LimitRange 自己的 default/defaultRequest 也要在上下限内
#   4. 工作负载的单副本下限和（副本数 × 每 Pod 请求，DaemonSet 按 1 计）≤
#      quota 的 cpu/memory 额度，头寸 > 0。渲染期拿不到节点数，DaemonSet 与
#      滚动期的并发 Job 都按 1 计，所以这是一个**下限**：下限越界必定越界，
#      下限不越界不等于多节点下也不越界（那是 values 注释里运维 --set 的事）。
#
# 用法：bash deploy/scripts/check-governance-consistency.sh <渲染产物目录>
#       bash deploy/scripts/check-governance-consistency.sh deploy/rendered/k3s-single
set -euo pipefail

DIR="${1:?用法: check-governance-consistency.sh <渲染产物目录>}"
[ -d "$DIR" ] || { echo "目录不存在：$DIR" >&2; exit 2; }
command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$DIR" <<'PYEOF'
import glob, os, re, sys

import yaml

DIR = sys.argv[1]
PROFILE = os.path.basename(DIR.rstrip('/'))

_SUFFIX = {
    '': 1, 'm': 1e-3,
    'k': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'E': 1e18,
    'Ki': 2 ** 10, 'Mi': 2 ** 20, 'Gi': 2 ** 30, 'Ti': 2 ** 40, 'Pi': 2 ** 50, 'Ei': 2 ** 60,
}


def qty(text):
    """K8s 数量串 → 浮点数。只认十进制与二进制后缀，不认 '1.5Gi' 之外的写法。"""
    m = re.fullmatch(r'(\d+(?:\.\d+)?)([a-zA-Z]*)', str(text).strip())
    if not m or m.group(2) not in _SUFFIX:
        raise ValueError(f"无法解析数量 {text!r}")
    return float(m.group(1)) * _SUFFIX[m.group(2)]


def size(value):
    for unit, div in (('Gi', 2 ** 30), ('Mi', 2 ** 20), ('Ki', 2 ** 10)):
        if abs(value) >= div:
            return f"{value / div:.1f}{unit}"
    return f"{value:.0f}B"


def milli(cpu):
    return f"{cpu * 1000:.0f}m"


def pct(part, whole):
    return f"{100.0 * part / whole:.1f}%" if whole else "n/a"


docs = []
for path in sorted(glob.glob(os.path.join(DIR, '*.yaml'))):
    with open(path) as fh:
        docs.extend(d for d in yaml.safe_load_all(fh) if d and d.get('kind'))

by_kind = {}
for d in docs:
    by_kind.setdefault(d['kind'], []).append(d)

quotas = by_kind.get('ResourceQuota', [])
limits = by_kind.get('LimitRange', [])
claims = by_kind.get('PersistentVolumeClaim', [])
workloads = [d for k in ('Deployment', 'StatefulSet', 'DaemonSet', 'CronJob')
             for d in by_kind.get(k, [])]

if not quotas and not limits:
    print(f"GOVERNANCE SKIP [{PROFILE}]：未渲染 ResourceQuota/LimitRange（resourceGovernance.enabled=false），无数值可核")
    sys.exit(0)
if len(quotas) > 1 or len(limits) > 1:
    sys.exit(f"GOVERNANCE 校验失败 [{PROFILE}]：命名空间治理对象应各至多一个，实得 quota={len(quotas)} limitrange={len(limits)}")

hard = (quotas[0].get('spec', {}).get('hard') or {}) if quotas else {}
lr_container, lr_pvc = None, None
for entry in (limits[0].get('spec', {}).get('limits') or []) if limits else []:
    if entry.get('type') == 'Container':
        lr_container = entry
    elif entry.get('type') == 'PersistentVolumeClaim':
        lr_pvc = entry

errors = []
notes = []


def workload_claim_storage(doc):
    """StatefulSet 的 volumeClaimTemplates 也是卷声明——控制器按它建 PVC，
    照样进命名空间的 requests.storage 账、照样受 LimitRange 的 PVC 上限约束。
    只数 PersistentVolumeClaim 对象会把这一份漏掉（漏掉的是配额账上真实存在
    的 15Gi）。"""
    out = []
    for tpl in doc.get('spec', {}).get('volumeClaimTemplates') or []:
        raw = tpl.get('spec', {}).get('resources', {}).get('requests', {}).get('storage')
        if raw is not None:
            out.append((f"{doc['metadata']['name']}/{tpl.get('metadata', {}).get('name')}", raw))
    return out


# 卷声明：逐条核单卷上限，再核总和与配额。
declarations = [(d['metadata']['name'],
                 d.get('spec', {}).get('resources', {}).get('requests', {}).get('storage'))
                for d in claims]
declarations = [(n, raw) for n, raw in declarations if raw is not None]
for doc in workloads:
    declarations.extend(workload_claim_storage(doc))

total_storage, largest = 0.0, None
if declarations:
    for name, raw in declarations:
        value = qty(raw)
        total_storage += value
        if largest is None or value > largest[1]:
            largest = (name, value)
        if lr_pvc:
            upper = lr_pvc.get('max', {}).get('storage')
            lower = lr_pvc.get('min', {}).get('storage')
            if upper is not None and value > qty(upper):
                errors.append(
                    f"卷声明 {name} = {raw} 超过 LimitRange 单卷上限 {upper}"
                    f"（声明越界在安装面直接被拒，不是更安全而是装不进来）")
            if lower is not None and value < qty(lower):
                errors.append(f"卷声明 {name} = {raw} 低于 LimitRange 单卷下限 {lower}")

if 'requests.storage' in hard:
    ceiling = qty(hard['requests.storage'])
    headroom = ceiling - total_storage
    if headroom <= 0:
        relation = '超过' if total_storage > ceiling else '等于'
        errors.append(
            f"卷声明之和 {size(total_storage)} {relation}配额 requests.storage "
            f"{hard['requests.storage']}：没有头寸，再添一条卷声明或调大任何一条"
            f"都会让这套清单装不进自己的命名空间")
    else:
        notes.append(
            f"卷声明之和 {size(total_storage)} ≤ 配额 {hard['requests.storage']}，"
            f"头寸 {size(headroom)} ({pct(headroom, ceiling)})")
elif declarations:
    notes.append(f"卷声明之和 {size(total_storage)}（配额未设 requests.storage，不校核上限）")

if lr_pvc and largest:
    upper = lr_pvc.get('max', {}).get('storage')
    notes.append(
        f"最大单卷声明 {size(largest[1])} ({largest[0]})"
        + (f" ≤ 单卷上限 {upper}" if upper else "（LimitRange 未设单卷上限）"))

# 容器：显式声明逐条核上下限；LimitRange 自己的默认值也要自洽（默认值是给
# 未声明资源的容器用的，默认值越界等于默认就违规）。
if lr_container:
    lo, hi = lr_container.get('min') or {}, lr_container.get('max') or {}

    def bound_error(where, face, key, raw):
        value = qty(raw)
        if key in lo and value < qty(lo[key]):
            return f"{where} {face}.{key} = {raw} 低于 LimitRange min.{key} = {lo[key]}"
        if key in hi and value > qty(hi[key]):
            return f"{where} {face}.{key} = {raw} 超过 LimitRange max.{key} = {hi[key]}"
        return None

    checked = 0
    for doc in workloads:
        pod = doc.get('spec', {}).get('template', {}).get('spec', {})
        for c in pod.get('containers', []) + pod.get('initContainers', []):
            res = c.get('resources') or {}
            where = f"{doc['kind']}/{doc['metadata']['name']} [{c.get('name')}]"
            for face in ('requests', 'limits'):
                for key, raw in (res.get(face) or {}).items():
                    if key not in lo and key not in hi:
                        continue
                    checked += 1
                    if msg := bound_error(where, face, key, raw):
                        errors.append(msg)
    for face in ('default', 'defaultRequest'):
        for key, raw in (lr_container.get(face) or {}).items():
            if msg := bound_error("LimitRange 自身", face, key, raw):
                errors.append(msg + "（未声明资源的容器会拿到这个默认值）")
    for key, raw in (lr_container.get('defaultRequest') or {}).items():
        cap = (lr_container.get('default') or {}).get(key)
        if cap is not None and qty(raw) > qty(cap):
            errors.append(
                f"LimitRange 自身 defaultRequest.{key} = {raw} 超过 default.{key} = {cap}"
                f"（请求大于上限，容器一旦用默认值就被拒）")
    notes.append(f"容器显式声明 {checked} 项，均在 LimitRange 边界内")

# 工作负载的单副本下限和 vs quota（cpu/memory）。
for face, quota_key, unit in (('requests', 'requests.cpu', 'cpu'),
                             ('requests', 'requests.memory', 'memory'),
                             ('limits', 'limits.cpu', 'cpu'),
                             ('limits', 'limits.memory', 'memory')):
    if quota_key not in hard or not lr_container:
        continue
    default_face = 'defaultRequest' if face == 'requests' else 'default'
    totals = 0.0
    for doc in workloads:
        spec = doc.get('spec', {})
        if doc['kind'] in ('Deployment', 'StatefulSet', 'CronJob'):
            replicas = spec.get('replicas', 1) or 1
        else:
            replicas = 1
        pod = spec.get('template', {}).get('spec', {})
        per_pod = 0.0
        for c in pod.get('containers', []) + pod.get('initContainers', []):
            declared = ((c.get('resources') or {}).get(face) or {}).get(unit)
            if declared is not None:
                per_pod += qty(declared)
            else:
                fallback = (lr_container.get(default_face) or {}).get(unit)
                if fallback is not None:
                    per_pod += qty(fallback)
        totals += replicas * per_pod
    ceiling = qty(hard[quota_key])
    headroom = ceiling - totals
    label = 'CPU' if unit == 'cpu' else '内存'

    def shown(value):
        return milli(value) if unit == 'cpu' else size(value)

    if headroom < 0:
        errors.append(
            f"工作负载单副本{face}.{unit} 下限和 {shown(totals)} 已超过配额 {quota_key} = "
            f"{hard[quota_key]}：按 1 副本就已经越界")
    else:
        notes.append(
            f"{face}.{label} 单副本下限和 {shown(totals)} ≤ 配额 {hard[quota_key]}，"
            f"头寸 {shown(headroom)}（DaemonSet 与并发 Job 按 1 计）")

if errors:
    print(f"GOVERNANCE 校验失败 [{PROFILE}]：")
    for e in errors:
        print("  - " + e)
    sys.exit(1)
print(f"GOVERNANCE OK [{PROFILE}]：")
for n in notes:
    print("  - " + n)
PYEOF

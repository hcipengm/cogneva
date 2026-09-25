#!/usr/bin/env bash
# 探针失败预算的确定性判据：每个探针都必须显式给出 timeoutSeconds 与
# failureThreshold，且数值只能来自 values.yaml 的 healthProbes 一处。
#
# 为什么需要它：Kubernetes 对 timeoutSeconds 默认 1 秒、failureThreshold 默认
# 3 次，**省略就等于按空闲节点取值**。共址构建把节点 load 拉到 20+、可用内存
# 掉到几百 Mi 时，kubelet 连自己的 Pod IP 都 dial 不进去（实测 09-23 redis
# 被 liveness 杀掉，后端跟着再断一次）。判据只问"有没有取值面、是不是同一个"，
# 不问"值合不合理"——后者取决于负载事实，不是结构问题。
#
# 被查的文件由**内容**发现（`deploy/` 下谁写了探针就查谁），不列在名单里：名单
# 必然会漏掉新长出来的目录——`k3s/observability/manifests/` 与 `k8s/` 就是这么
# 漏掉的，两处正在犯这条判据要治的病（漏 timeoutSeconds）。为了让"漏掉"这件事
# 在结构上不可能，判据同时核对**覆盖面 census**：今天带探针的目录集合等于一个
# 显式声明的集合，多出一个目录即失败，逼下一次加树的人把它纳入判据。
#
# 判据自带对照组：同一份检查函数喂一个故意漏写 timeoutSeconds 的样例必须报错，
# 否则这条门禁是空转的。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"

python3 - "${repo}" <<'PY'
import pathlib, re, sys

repo = pathlib.Path(sys.argv[1])
probe_keys = ("startupProbe", "livenessProbe", "readinessProbe")
required = ("timeoutSeconds", "failureThreshold")


def containers(doc):
    spec = doc.get("spec", {})
    pod = spec.get("template", {}).get("spec", spec)
    return pod.get("containers", []) + pod.get("initContainers", [])


def check_docs(docs, origin):
    """每个探针都要显式声明完整预算。缺任何一个键 = 依赖 kubelet 默认值。"""
    bad = []
    for d in docs:
        if not d or d.get("kind") not in ("Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"):
            continue
        name = d.get("metadata", {}).get("name", "?")
        for c in containers(d):
            for key in probe_keys:
                probe = c.get(key)
                if probe is None:
                    continue
                for field in required:
                    if field not in probe:
                        bad.append(f"{origin} {name}[{c['name']}] {key} 缺少 {field}")
    return bad


yaml = None
try:
    import yaml as _yaml
    yaml = _yaml
except ImportError:
    pass

if yaml is None:
    print("需要 python3 的 yaml 模块（pip install pyyaml）", file=sys.stderr)
    sys.exit(2)

problems = []

templates = repo / "deploy" / "helm" / "cogneva" / "templates"

# 1) 每一份会被 apply 的清单（静态树与预渲染产物）都要写全预算。chart 模板除外：
#    那里的键由 `toYaml` 渲染，文本里看不到，由第 2 段按另一条规则查。
checked = {}
for path in sorted((repo / "deploy").rglob("*.yaml")):
    if templates in path.parents:
        continue
    text = path.read_text()
    if not any(key in text for key in probe_keys):
        continue
    rel = str(path.relative_to(repo))
    checked[rel] = str(path.parent.relative_to(repo / "deploy"))
    problems += check_docs(list(yaml.safe_load_all(text)), rel)

# 覆盖面 census：带探针的目录集合必须与声明一致。两个方向都会失败——新目录出现
# （有人加了一份带探针的清单而没进判据）或目录消失（有人搬了树而声明没跟上）。
declared_trees = {
    "k3s",
    "k3s/observability/manifests",
    "k8s",
    "rendered/k3s-single",
    "rendered/k3s-multi",
    "rendered/k8s-standard",
}
found_trees = set(checked.values())
if found_trees != declared_trees:
    for missing in sorted(declared_trees - found_trees):
        problems.append(
            f"声明的探针目录 {missing} 下一份带探针的清单都没有：判据的覆盖面 census 过期了"
        )
    for extra in sorted(found_trees - declared_trees):
        problems.append(
            f"{extra} 下有带探针的清单但不在声明的目录集合里："
            "把它纳入判据（并确认这些探针写全了预算）后再更新 census"
        )

# 2) chart 模板：探针的数值必须来自 .Values.healthProbes，不允许字面量。
#    字面量会让"改配置面"与"改部署"变成两件事，两侧必然漂移。
literal = re.compile(r"^\s*(initialDelaySeconds|periodSeconds|timeoutSeconds|failureThreshold):")
for path in sorted(templates.glob("*.yaml")):
    lines = path.read_text().splitlines()
    for i, line in enumerate(lines):
        key = line.strip().rstrip(":")
        if key not in probe_keys or not line.startswith(" " * 10):
            continue
        indent = len(line) - len(line.lstrip())
        block = []
        j = i + 1
        while j < len(lines):
            nxt = lines[j]
            # 探针块一直延伸到下一行同缩进或更浅的键；模板指令行本就在第 0 列，
            # 属于这个块，不能当成界定符。
            if nxt.strip() and (len(nxt) - len(nxt.lstrip()) <= indent) and not nxt.lstrip().startswith("{{"):
                break
            block.append(nxt)
            j += 1
        for offset, nxt in enumerate(block):
            if literal.match(nxt):
                problems.append(
                    f"{path.name}:{i + 2 + offset} {key} 的数值是字面量，应取自 .Values.healthProbes"
                )
        if not any("toYaml .Values.healthProbes" in nxt for nxt in block):
            problems.append(f"{path.name}:{i + 1} {key} 没有引用 .Values.healthProbes")

# 3) 取值面本身：三档预算都要在 values 里定义，且每档都有那两个键。
values = yaml.safe_load((repo / "deploy" / "helm" / "cogneva" / "values.yaml").read_text())
budgets = (values or {}).get("healthProbes") or {}
for kind in ("startup", "liveness", "readiness"):
    block = budgets.get(kind) or {}
    for field in required:
        if field not in block:
            problems.append(f"values.yaml healthProbes.{kind} 缺少 {field}")

# 对照组：同一份判据喂一个漏写 timeoutSeconds 的样例必须报错。
control = list(
    yaml.safe_load_all(
        """
kind: Deployment
metadata: {name: control}
spec:
  template:
    spec:
      containers:
        - name: c
          livenessProbe:
            httpGet: {path: /health, port: http}
            periodSeconds: 20
            failureThreshold: 6
"""
    )
)
if not check_docs(control, "control"):
    problems.append("对照组未被判出：判据对漏写 timeoutSeconds 的样例失明（门禁空转）")

if problems:
    print("探针预算判据失败：")
    for p in problems:
        print("  - " + p)
    sys.exit(1)
print("PASS: 每个探针的预算都显式给出，且数值只来自 values.yaml 的 healthProbes 一处")
PY

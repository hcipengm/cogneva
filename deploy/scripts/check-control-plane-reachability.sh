#!/usr/bin/env bash
# 自伤检测：这份渲染产物描述出来的控制面，还到得了吗。
#
# 渲染新鲜度与 parity 判的是「三份清单说的是不是同一件事」，判不了「这件事本身还能不能
# 被访问」：一次改动可以三面全绿，却把 Service 的 selector 改到一个没人带的标签、
# 把 targetPort 改成一个没声明的端口名、把主负载的 replicas 写成 0、把探针指向一个不存在
# 的端口、把一个卷指向不存在的 PVC、或者把 Ingress 指到一个不存在的 Service。
# 这些形状在 `kubectl apply` 之后**不报错**：对象建成了，Pod 也起来了，只是没有人能到它。
# 门禁读的从来只有退出码，所以「起不来」必须在退出码上有一个判词。
#
# 判据只读会被 apply 的那份字节，不读 values、不读 chart 模板：在这里再猜一次
# 「它应该长什么样」等于写第二份定义。会被 apply 的字节有两条投递路径，各自读一次：
#
#   - `deploy/rendered/<profile>/`：`helm install` 之后 apply 的预渲染产物；
#   - `deploy/k3s`：元启动自建集群时 `kubectl apply -k` 的对象集，**按
#     `kustomization.yaml` 的 `resources` 列读**，不按目录里有什么猜（目录里另有
#     values 文件与 examples，它们不是对象）。集群内收敛循环投递的也是这一份。
#
# 判的六族，每一族都是「对象之间必须互相对得上」的关系，不是内容偏好：
#
#   1. Service 的 selector 至少选中一条工作负载（选不中 ⇒ 没有 endpoints）；
#   2. Service 的 targetPort（以及具名时不写时取 port）在选中的工作负载里真的被声明
#      （对不上 ⇒ 流量打到没人听的端口）；
#   3. Deployment/StatefulSet 的 replicas 不是 0（写了 0 ⇒ 这条工作负载没有副本）；
#   4. 探针若按名字指端口，那个名字必须在**同一个容器**里声明过（指错 ⇒ 永远不 ready，
#      ready 副本为 0 的效果与第 3 条相同）；exec 探针与不带端口的探针不判；
#   5. 卷引用的 PVC、ConfigMap 必须在同一份渲染里存在（Secret 不在此列：本项目的
#      Secret 由安装期生成、不经清单，它的缺席是设计而不是缺陷）；
#   6. Ingress 的 backend 必须指向存在的 Service 与存在的 service port。
#
# 不判的东西也写在这里，免得下一次有人以为它判过：镜像 tag 在仓库里存不存在、探针的
# 路径对不对、NetworkPolicy 放不放行、ResourceQuota 的合计够不够、ConfigMap 里的键
# 是不是应用要的那几个——这些要么要连集群、要么要懂应用语义，不是结构关系。
#
# `deploy/k3s/observability/` 整棵不在本门内：那 21 份清单由安装脚本与收敛循环交付，
# 它们指向的 Service（grafana / prometheus）由 kube-prometheus-stack 提供、Secret 由
# 安装期随机生成，第 5、6 族在那棵子树里**没有可比对象**；要判只能按对象写豁免表，
# 而豁免表正是「判据看起来覆盖了、实际靠名单」的那种形状。代价写在明处：那棵子树
# 自己的选择器与端口此刻不由任何门判。
#
# 用法：bash deploy/scripts/check-control-plane-reachability.sh [仓库根目录]
# 退出码：0 全过；1 有不可达形状（逐条点名 profile 与对象）；2 判不了（缺依赖、没有产物）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3，无法判断控制面可达性" >&2; exit 2; }
python3 - deploy/rendered deploy/k3s <<'PYEOF'
import glob
import os
import sys

import yaml

RENDERED = sys.argv[1]
APPLY = sys.argv[2]
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet")


def fail(msg):
    global problems
    problems.append(msg)


def pod_template(doc):
    kind = doc.get("kind")
    if kind in ("Deployment", "StatefulSet", "DaemonSet"):
        return doc["spec"]["template"]
    return None


def replicas(doc):
    if doc.get("kind") in ("Deployment", "StatefulSet"):
        return doc["spec"].get("replicas", 1)
    return None


def containers(tmpl):
    return [c for c in tmpl["spec"].get("containers", []) if isinstance(c, dict)]


def port_names(tmpl):
    names = set()
    for c in containers(tmpl):
        for p in c.get("ports") or []:
            if isinstance(p, dict) and p.get("name"):
                names.add(p["name"])
    return names


def port_numbers(tmpl):
    nums = set()
    for c in containers(tmpl):
        for p in c.get("ports") or []:
            if isinstance(p, dict) and p.get("containerPort") is not None:
                nums.add(int(p["containerPort"]))
    return nums


def selector_matches(selector, labels):
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def probe_port_problems(name, tmpl):
    out = []
    for c in containers(tmpl):
        for probe_kind in ("livenessProbe", "readinessProbe", "startupProbe"):
            probe = c.get(probe_kind)
            if not isinstance(probe, dict):
                continue
            for handler in ("httpGet", "tcpSocket", "grpc"):
                h = probe.get(handler)
                if not isinstance(h, dict) or h.get("port") is None:
                    continue
                port = h["port"]
                if isinstance(port, str):
                    declared = {p.get("name") for p in (c.get("ports") or []) if isinstance(p, dict)}
                    if port not in declared:
                        out.append(
                            "%s 的 %s.%s 按名字指向端口 %r，但容器 %s 没有声明这个端口名"
                            % (name, c.get("name", "?"), probe_kind, port, c.get("name", "?"))
                        )
    return out


def apply_tree_sources(apply_dir):
    """交付树的清单：`kustomization.yaml` 的 `resources` 列，也就是 `apply -k` 的对象集。"""
    kustomization = os.path.join(apply_dir, "kustomization.yaml")
    if not os.path.isfile(kustomization):
        print("判不了：%s 下没有 kustomization.yaml，读不出交付对象集" % apply_dir,
              file=sys.stderr)
        sys.exit(2)
    with open(kustomization, encoding="utf-8") as fh:
        docs = [d for d in yaml.safe_load_all(fh) if d]
    if len(docs) != 1 or not isinstance(docs[0], dict):
        print("判不了：%s 不是单份 kustomization 文档" % kustomization, file=sys.stderr)
        sys.exit(2)
    resources = docs[0].get("resources") or []
    if not resources:
        print("判不了：%s 没有 resources 列表，读不出交付对象集" % kustomization,
              file=sys.stderr)
        sys.exit(2)
    sources = []
    for resource in resources:
        path = os.path.join(apply_dir, resource)
        if not os.path.isfile(path):
            print("判不了：%s 列的 %r 不存在" % (kustomization, resource), file=sys.stderr)
            sys.exit(2)
        sources.append(path)
    return sources


problems = []
trees = []  # (名字, 清单文件)
profiles = sorted(d for d in glob.glob(os.path.join(RENDERED, "*")) if os.path.isdir(d))
if not profiles:
    print("判不了：%s 下没有任何 profile 目录" % RENDERED, file=sys.stderr)
    sys.exit(2)
for profile in profiles:
    sources = sorted(glob.glob(os.path.join(profile, "*.yaml")))
    if not sources:
        print("判不了：%s 下没有清单" % profile, file=sys.stderr)
        sys.exit(2)
    trees.append(("deploy/rendered/" + os.path.basename(profile), sources))
trees.append((APPLY, apply_tree_sources(APPLY)))

judged = 0
for pname, sources in trees:
    docs = []
    for path in sources:
        try:
            with open(path, encoding="utf-8") as fh:
                for doc in yaml.safe_load_all(fh):
                    if doc:
                        docs.append(doc)
        except yaml.YAMLError as exc:
            print("判不了：%s 解析失败：%s" % (path, exc), file=sys.stderr)
            sys.exit(2)

    workloads = [(d, pod_template(d)) for d in docs if pod_template(d)]
    services = [d for d in docs if d.get("kind") == "Service"]
    pvcs = {d["metadata"]["name"] for d in docs if d.get("kind") == "PersistentVolumeClaim"}
    configmaps = {d["metadata"]["name"] for d in docs if d.get("kind") == "ConfigMap"}
    secrets_present = {d["metadata"]["name"] for d in docs if d.get("kind") == "Secret"}
    service_ports = {}
    for svc in services:
        spec = svc.get("spec") or {}
        service_ports[svc["metadata"]["name"]] = {
            p.get("port") for p in (spec.get("ports") or []) if isinstance(p, dict)
        }

    judged += len(services) + len(workloads)

    for svc in services:
        sname = svc["metadata"]["name"]
        spec = svc.get("spec") or {}
        selector = spec.get("selector") or {}
        if not selector:
            # ExternalName / 无 selector 的 Service 不按标签找 Pod，这一族不适用于它。
            continue
        selected = [
            (d, t) for (d, t) in workloads
            if selector_matches(selector, t["metadata"].get("labels") or {})
        ]
        if not selected:
            fail("%s：Service %s 的 selector %r 没有选中任何工作负载（它的 endpoints 会为空）"
                 % (pname, sname, selector))
            continue
        target_names = set()
        target_nums = set()
        for _, t in selected:
            target_names |= port_names(t)
            target_nums |= port_numbers(t)
        for port in spec.get("ports") or []:
            if not isinstance(port, dict):
                continue
            target = port.get("targetPort", port.get("port"))
            if isinstance(target, str):
                if target not in target_names:
                    fail("%s：Service %s 的 targetPort 名 %r 在选中的工作负载里没有声明"
                         "（声明过的端口名：%s）"
                         % (pname, sname, target, ", ".join(sorted(target_names)) or "无"))
            elif isinstance(target, int):
                if target not in target_nums:
                    fail("%s：Service %s 的 targetPort %d 在选中的工作负载里没有声明"
                         "（声明过的端口号：%s）"
                         % (pname, sname, target,
                            ", ".join(str(n) for n in sorted(target_nums)) or "无"))

    for doc, tmpl in workloads:
        wname = doc["metadata"]["name"]
        kind = doc.get("kind")
        r = replicas(doc)
        if r == 0:
            fail("%s：%s %s 的 replicas 是 0（这条工作负载没有副本）" % (pname, kind, wname))
        for name in probe_port_problems(wname, tmpl):
            fail("%s：%s" % (pname, name))
        pod_vols = tmpl["spec"].get("volumes") or []
        for vol in pod_vols:
            if not isinstance(vol, dict):
                continue
            claim = (vol.get("persistentVolumeClaim") or {}).get("claimName")
            if claim and claim not in pvcs:
                fail("%s：%s %s 的卷 %s 引用了不存在的 PVC %r"
                     % (pname, kind, wname, vol.get("name"), claim))
            cm = (vol.get("configMap") or {}).get("name")
            if cm and cm not in configmaps:
                fail("%s：%s %s 的卷 %s 引用了不存在的 ConfigMap %r"
                     % (pname, kind, wname, vol.get("name"), cm))
        for c in containers(tmpl):
            for key in ("envFrom",):
                for src in c.get(key) or []:
                    cm = (src.get("configMapRef") or {}).get("name") if isinstance(src, dict) else None
                    if cm and cm not in configmaps:
                        fail("%s：%s %s 的容器 %s 从 configMapRef 引用了不存在的 ConfigMap %r"
                             % (pname, kind, wname, c.get("name"), cm))
            for env in c.get("env") or []:
                if not isinstance(env, dict):
                    continue
                ref = (env.get("valueFrom") or {}).get("configMapKeyRef") or {}
                cm = ref.get("name")
                if cm and cm not in configmaps:
                    fail("%s：%s %s 的容器 %s 的 env %s 引用了不存在的 ConfigMap %r"
                         % (pname, kind, wname, c.get("name"), env.get("name"), cm))
                sref = (env.get("valueFrom") or {}).get("secretKeyRef") or {}
                sn = sref.get("name")
                if sn and sn not in secrets_present:
                    # Secret 由安装期生成、不经清单：这里只在该棵树声明过任何 Secret
                    # 时才判（声明过就有比对的基线），否则不判——缺省不是缺陷。
                    if secrets_present:
                        fail("%s：%s %s 的容器 %s 的 env %s 引用了不存在的 Secret %r"
                             % (pname, kind, wname, c.get("name"), env.get("name"), sn))

    for ing in (d for d in docs if d.get("kind") == "Ingress"):
        iname = ing["metadata"]["name"]
        spec = ing.get("spec") or {}
        backends = []
        default = spec.get("defaultBackend")
        if isinstance(default, dict):
            backends.append(default)
        for rule in spec.get("rules") or []:
            for path in ((rule or {}).get("http") or {}).get("paths") or []:
                if isinstance(path, dict) and isinstance(path.get("backend"), dict):
                    backends.append(path["backend"])
        for b in backends:
            svc = (b.get("service") or {})
            sname = svc.get("name")
            if not sname:
                continue
            if sname not in service_ports:
                fail("%s：Ingress %s 的后端指向不存在的 Service %r" % (pname, iname, sname))
                continue
            ports = svc.get("port") or {}
            num = ports.get("number")
            if num is not None and num not in service_ports[sname]:
                fail("%s：Ingress %s 的后端指向 Service %s 上不存在的端口 %d"
                     % (pname, iname, sname, num))

if problems:
    for p in problems:
        print(p, file=sys.stderr)
    print("控制面可达性：%d 处不可达形状（读了 %d 个 profile 与交付树 %s）"
          % (len(problems), len(profiles), APPLY), file=sys.stderr)
    sys.exit(1)

print("控制面可达性：%d 个 profile 与交付树 %s 全过（Service 选择与端口、副本数、探针端口、"
      "卷引用、Ingress 后端）" % (len(profiles), APPLY))
PYEOF

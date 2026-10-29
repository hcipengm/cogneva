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
# 判的九族，每一族都是「对象之间必须互相对得上」的关系，不是内容偏好：
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
#   7. NetworkPolicy 的选择器与端口落在树里真的存在的 Pod 与端口上：
#      - 同命名空间的对端 podSelector 至少选中一条工作负载（选不中 ⇒ 这条放行通不到
#        任何 Pod；反向也一样，一条规则被写窄成没人匹配时，被它挡住的那条通道**静默
#        丢掉**，日志里只有连接超时，与「对端本来不响应」同形）；
#      - 跨命名空间的对端按 `kubernetes.io/metadata.name` 指名时，那个名字得是 deploy/
#        里声明过的 namespace；
#      - 放行端口得是目标工作负载声明过的 containerPort（或配置里 spelling 过的
#        endpoint 端口）——注意这一条只抓「拼错」，抓不了「应用实际没监听」：容器不声明
#        containerPort 也能监听，所以端口在树里有一个说法就算过；
#      - 写了某个方向的规则，`policyTypes` 里却没有那个方向 ⇒ 这些规则一条都不生效
#        （`policyTypes` 是权威声明，不写时 K8s 才按规则自动推）；
#      - 整条策略（subject 与所有同命名空间对端）一个人都选不中 ⇒ 它是一份死清单。
#   8. 身份引用：Pod 模板写的 serviceAccountName、RoleBinding/ClusterRoleBinding 的
#      roleRef 与主体 ServiceAccount，都必须是同一棵树里声明过的对象（指错 ⇒ Pod 拿到
#      default 身份、按它判的调用一律 403；绑定授予一个没人拥有的身份 ⇒ 建得成、什么都不给）；
#   9. Ingress 注解里按名字引用的 traefik Middleware 存在（注解是字符串、对象是另一份
#      清单；指不到时 traefik 只是不套那层中间件，响应头悄悄消失）。
#
# 不判的东西也写在这里，免得下一次有人以为它判过：镜像 tag 在仓库里存不存在、探针的
# 路径对不对、ResourceQuota 的合计够不够、ConfigMap 里的键是不是应用要的那几个——
# 这些要么要连集群、要么要懂应用语义，不是结构关系。NetworkPolicy 只判到「选择器与
# 端口对得上树里的对象」为止：CNI 是否真的按它执行、放行集合与设计意图是否一致，
# 不是读清单能读出来的。subject 选择器选不中动态 Pod（运行时 Job）是合法的，那一面
# 的漂移由 `crates/cogneva/tests/network_policy_subjects_name_a_component_the_code_writes_test.rs`
# 判——它读得到代码里的组件常量，这里读不到。对端也有同样的一面：今天没有一条放行
# 指向运行时创建的 Pod，真出现了这里会红——那是在要求把那份 Pod 的标签变成可读的，
# 不是判据错了。
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
import json
import os
import sys

import yaml

RENDERED = sys.argv[1]
APPLY = sys.argv[2]
NAMESPACE_NAME_LABEL = "kubernetes.io/metadata.name"
# 集群自带、任何清单之前就存在的 namespace：DNS 住在这里，每一条 Pod 都需要它。
CLUSTER_NAMESPACES = {"kube-system"}


def fail(msg):
    global problems
    problems.append(msg)


def pod_template(doc):
    """Pod 模板：工作负载与定时任务都算。Job/CronJob 的 Pod 同样是「会被 apply 的字节
    描述出来的 Pod」，漏掉它们的探针端口与卷引用，等于让这两条工作负载不吃第 3～5 族。"""
    spec = doc.get("spec") or {}
    kind = doc.get("kind")
    if kind in ("Deployment", "StatefulSet", "DaemonSet", "Job"):
        return spec.get("template")
    if kind == "CronJob":
        return ((spec.get("jobTemplate") or {}).get("spec") or {}).get("template")
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


def label_selector_matches(selector, labels):
    """NetworkPolicy 的 LabelSelector：`matchLabels` 全等、`matchExpressions` 四个算子。
    空选择器匹配全部（`podSelector: {}` 是「本命名空间的任何 Pod」，不是「没有」）。"""
    for key, value in (selector.get("matchLabels") or {}).items():
        if labels.get(key) != value:
            return False
    for expr in selector.get("matchExpressions") or []:
        key = expr.get("key")
        op = expr.get("operator")
        values = expr.get("values") or []
        present = labels.get(key)
        if op == "In" and present not in values:
            return False
        if op == "NotIn" and present in values:
            return False
        if op == "Exists" and key not in labels:
            return False
        if op == "DoesNotExist" and key in labels:
            return False
    return True


def namespace_names(selector):
    """`namespaceSelector` 按名字指名的那几个 namespace；按别的标签选的返回 None
    （树里读不出「哪些 namespace 带这个标签」，那一条判不了）。"""
    names = []
    match_labels = selector.get("matchLabels") or {}
    if NAMESPACE_NAME_LABEL in match_labels:
        names.append(match_labels[NAMESPACE_NAME_LABEL])
    for expr in selector.get("matchExpressions") or []:
        if expr.get("key") == NAMESPACE_NAME_LABEL and expr.get("operator") == "In":
            names.extend(expr.get("values") or [])
    return names or None


def declared_namespaces():
    """deploy/ 下声明过的 namespace。跨命名空间的放行得指到一个真的会存在的 namespace
    上：名字拼错时策略照样建成，只是那条放行谁都不匹配——与「这条通道不需要放行」
    同形。`kube-system` 不在这份名单里也不该在：集群在任何清单跑起来之前就有它
    （DNS 的 Pod 住在那儿），我们的清单永远不会声明它。"""
    found = set()
    for path in glob.glob(os.path.join("deploy", "**", "*.y*ml"), recursive=True):
        try:
            with open(path, encoding="utf-8") as fh:
                docs = list(yaml.safe_load_all(fh))
        except (OSError, yaml.YAMLError):
            continue
        for doc in docs:
            if isinstance(doc, dict) and doc.get("kind") == "Namespace":
                name = (doc.get("metadata") or {}).get("name")
                if name:
                    found.add(name)
    return found


def config_endpoint_ports(docs):
    """配置里 spelling 过的 endpoint 端口：`{"port": 3306}` 这种。端口是同一套词汇
    在清单与配置里各拼一遍——放行清单里的 3306 与 `providers.db.options.port` 说的是
    同一件事，没有任何东西比过它们。这里只把配置里出现的端口收成一个集合，判词在
    端口对不上时点名。"""
    ports = {}

    def walk(node, where):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "port" and isinstance(value, int) and not isinstance(value, bool):
                    ports.setdefault(value, where)
                walk(value, "%s.%s" % (where, key))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, "%s[%d]" % (where, index))

    for doc in docs:
        if doc.get("kind") != "ConfigMap":
            continue
        for key, text in (doc.get("data") or {}).items():
            if not isinstance(text, str):
                continue
            try:
                parsed = json.loads(text)
            except ValueError:
                continue
            walk(parsed, "%s.%s" % (doc["metadata"]["name"], key))
    return ports


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
namespaces = declared_namespaces()
for pname, sources in trees:
    docs = []
    for path in sources:
        try:
            with open(path, encoding="utf-8") as fh:
                for doc in yaml.safe_load_all(fh):
                    if doc:
                        docs.append(doc)
        except OSError as exc:
            # 盘点之后读不出来：文件在这一次运行里被改掉或删掉了（同一棵树常常有别的
            # 会话在重新渲染）。这不是「不可达形状」，也不是全过——是判不了。
            print("判不了：%s 读不出来：%s（清单在盘点之后被改掉了？）" % (path, exc),
                  file=sys.stderr)
            sys.exit(2)
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

    # NetworkPolicy：策略里写的「谁能连谁、连哪个端口」必须落在树里真的存在的 Pod
    # 与端口上。选不中不会报错——策略照样建成，只是那条规则谁都不匹配；被它挡住或
    # 放开的通道于是只剩连接超时，与「对端本来就不响应」同形。
    endpoints = config_endpoint_ports(docs)
    policies = [d for d in docs if d.get("kind") == "NetworkPolicy"]
    judged += len(policies)

    for np in policies:
        nname = np["metadata"]["name"]
        nspace = np["metadata"].get("namespace")
        nspec = np.get("spec") or {}
        local = [(d, t) for (d, t) in workloads if d["metadata"].get("namespace") == nspace]

        # policyTypes 是权威声明：写了它，某个方向不在名单里就等于这份策略不管那个方向，
        # 该方向下写的规则一条都不生效（不写时 K8s 才按规则自动推出名单）。
        types = {str(t) for t in (nspec.get("policyTypes") or [])}
        for direction, listed in (("ingress", "Ingress"), ("egress", "Egress")):
            if (nspec.get(direction) or []) and types and listed not in types:
                fail("%s：NetworkPolicy %s 写了 %s 规则，policyTypes 里却没有 %s——"
                     "策略只按 policyTypes 生效，这些规则不作用于任何 Pod"
                     % (pname, nname, direction, listed))

        def selected(selector, local=local):
            return [
                (d, t) for (d, t) in local
                if label_selector_matches(selector, t["metadata"].get("labels") or {})
            ]

        subject = selected(nspec.get("podSelector") or {})
        living = bool(subject)
        for direction, key, side in (("ingress", "from", "来源"), ("egress", "to", "目的")):
            for rule in nspec.get(direction) or []:
                peers = [p for p in (rule.get(key) or []) if isinstance(p, dict)]
                targets = []
                for peer in peers:
                    namespace_selector = peer.get("namespaceSelector")
                    if namespace_selector is not None:
                        for nsname in namespace_names(namespace_selector) or []:
                            if nsname in namespaces or nsname in CLUSTER_NAMESPACES:
                                continue
                            fail("%s：NetworkPolicy %s 的 %s %s对端按名字指 namespace %r，"
                                 "但 deploy/ 里没有声明它（这条放行谁都不匹配）"
                                 % (pname, nname, direction, side, nsname))
                        continue
                    if peer.get("ipBlock") is not None:
                        continue
                    hit = selected(peer.get("podSelector") or {})
                    living = living or bool(hit)
                    if hit:
                        targets.extend(hit)
                    else:
                        fail("%s：NetworkPolicy %s 的 %s %s对端 podSelector %r 在 namespace %r 里"
                             "选不中任何工作负载（这条放行通不到任何 Pod）"
                             % (pname, nname, direction, side, peer.get("podSelector"), nspace))

                # 入站规则的端口落在被这份策略选中的 Pod 上，与来源无关，照样判；
                # 出站规则的端口落在对端 Pod 上，只有对端全是本命名空间的 podSelector
                # （或没写对端＝任何目的）时才知道对端是谁，混了跨命名空间或 ipBlock 就不判。
                cross = any(
                    p.get("namespaceSelector") is not None or p.get("ipBlock") is not None
                    for p in peers
                )
                judge_ports = direction == "ingress" or (not cross)
                ports = [p for p in (rule.get("ports") or []) if isinstance(p, dict)]
                if not judge_ports or not ports:
                    continue
                scope = subject if direction == "ingress" else targets
                if not scope:
                    continue
                port_name_set = set()
                port_num_set = set()
                for _, tmpl in scope:
                    port_name_set |= port_names(tmpl)
                    port_num_set |= port_numbers(tmpl)
                for port in ports:
                    value = port.get("port")
                    if isinstance(value, bool):
                        continue
                    if isinstance(value, int):
                        if value in port_num_set or value in endpoints:
                            continue
                        fail("%s：NetworkPolicy %s 的 %s 放行端口 %d，但目标工作负载没有一个声明这个 "
                             "containerPort，配置里也没有 endpoint 用它（声明过的端口号：%s；"
                             "配置里 spelling 过的：%s）"
                             % (pname, nname, direction, value,
                                ", ".join(str(n) for n in sorted(port_num_set)) or "无",
                                ", ".join(str(n) for n in sorted(endpoints)) or "无"))
                    elif isinstance(value, str) and value not in port_name_set:
                        fail("%s：NetworkPolicy %s 的 %s 放行具名端口 %r，但目标工作负载没有一个声明"
                             "这个端口名（声明过的端口名：%s）"
                             % (pname, nname, direction, value,
                                ", ".join(sorted(port_name_set)) or "无"))

        if not living:
            fail("%s：NetworkPolicy %s 的 subject 与所有同命名空间对端都选不中任何工作负载"
                 "（这份策略谁都不管）" % (pname, nname))

    # 引用面：Pod 指名的工作负载身份、RoleBinding 指的角色、绑定的主体，都得是这棵树里
    # 真的声明过的对象。指错的形态与前几族同形：对象照建，授予的东西落在空集上——Pod
    # 拿到的是 default 身份、按这份身份判的 API 调用一律 403；绑定授予了一个没人拥有的
    # 身份，于是「没权限」与「本来就不该有权限」从外面看一样。
    service_accounts = {
        (d["metadata"].get("namespace"), d["metadata"]["name"])
        for d in docs if d.get("kind") == "ServiceAccount"
    }
    roles = {
        (d.get("kind"), d["metadata"].get("namespace"), d["metadata"]["name"])
        for d in docs if d.get("kind") in ("Role", "ClusterRole")
    }
    bindings = [d for d in docs if d.get("kind") in ("RoleBinding", "ClusterRoleBinding")]
    judged += len(service_accounts) + len(bindings)

    for doc, tmpl in workloads:
        sa = tmpl["spec"].get("serviceAccountName") or tmpl["spec"].get("serviceAccount")
        if not sa:
            continue
        ns = doc["metadata"].get("namespace")
        if (ns, sa) in service_accounts:
            continue
        fail("%s：%s %s 的 serviceAccountName %r 在这棵树里没有被声明（该命名空间声明过的：%s）"
             "——Pod 会拿到 default 身份，按这份身份判的权限一律落空"
             % (pname, doc.get("kind"), doc["metadata"]["name"], sa,
                ", ".join(sorted(n for (a, n) in service_accounts if a == ns)) or "无"))

    for binding in bindings:
        bkind = binding.get("kind")
        bname = binding["metadata"]["name"]
        bns = binding["metadata"].get("namespace")
        ref = binding.get("roleRef") or {}
        if ref.get("kind") not in ("Role", "ClusterRole") or not ref.get("name"):
            fail("%s：%s %s 的 roleRef %r 不是可解析的角色引用" % (pname, bkind, bname, ref))
        else:
            where = bns if ref["kind"] == "Role" else None
            if (ref["kind"], where, ref["name"]) not in roles:
                fail("%s：%s %s 的 roleRef 指向 %s %r，这棵树里没有它声明的这个对象"
                     "（声明过的：%s）——绑定照建，只是它授予的东西落在空集上"
                     % (pname, bkind, bname, ref["kind"], ref["name"],
                        ", ".join(sorted("%s/%s" % (k, n) for (k, _, n) in roles)) or "无"))
        for subject in binding.get("subjects") or []:
            if not isinstance(subject, dict) or subject.get("kind") != "ServiceAccount":
                continue
            sname = subject.get("name")
            sns = subject.get("namespace") or bns
            if (sns, sname) not in service_accounts:
                fail("%s：%s %s 的主体 ServiceAccount %r 在命名空间 %r 里没有被声明"
                     "（声明过的：%s）——这条绑定授予的是一个没人拥有的身份"
                     % (pname, bkind, bname, sname, sns,
                        ", ".join(sorted("%s/%s" % (a, n) for (a, n) in service_accounts)) or "无"))

    # Ingress 注解里按名字引用的 Middleware：注解是字符串，Middleware 是另一份清单，
    # 两者之间没有任何东西比较。名字指不到时 traefik 不报错、Ingress 也不失败——它丢掉
    # 整条路由，站点整体 404（2026-10-29 实测），而 404 与「这个路由本来就没有」同形。
    #
    # 名字指的是**登记名**，不是对象的 metadata.name：CRD provider 按「命名空间-名字」
    # 登记，所以同一命名空间里的中间件在注解里也要写成 `<ns>-<name>@kubernetescrd`。
    # 只认后一半（曾经就是这么判的）会把一条 404 的注解判成过——判据与 traefik 的口径
    # 不一致时，绿的不是树，是判据自己。
    middlewares = {
        (d["metadata"].get("namespace"), d["metadata"]["name"])
        for d in docs if d.get("kind") == "Middleware"
    }
    judged += len(middlewares)
    for ing in (d for d in docs if d.get("kind") == "Ingress"):
        iname = ing["metadata"]["name"]
        ins = ing["metadata"].get("namespace")
        raw = (ing["metadata"].get("annotations") or {}).get(
            "traefik.ingress.kubernetes.io/router.middlewares")
        if not raw:
            continue
        judged += 1
        for entry in str(raw).split(","):
            entry = entry.strip()
            if not entry:
                continue
            if "@" not in entry:
                fail("%s：Ingress %s 的中间件注解里 %r 没有 provider 后缀（形如 name@kubernetescrd）"
                     % (pname, iname, entry))
                continue
            name, provider = entry.rsplit("@", 1)
            if provider != "kubernetescrd":
                # 别的 provider（@file / @internal）的中间件不在这棵树里声明。
                continue
            prefix = "%s-" % ins
            if name.startswith(prefix):
                obj_name = name[len(prefix):]
            else:
                obj_name = name
            if (ins, obj_name) not in middlewares:
                fail("%s：Ingress %s 的中间件注解指向 %r，但 namespace %r 里没有这个 Middleware"
                     "（声明过的：%s）——traefik 不会因此让 Ingress 失败，它丢弃整条路由，"
                     "站点整体 404"
                     % (pname, iname, entry, ins,
                        ", ".join(sorted("%s-%s@kubernetescrd" % (a, n)
                                         for (a, n) in middlewares if a == ins)) or "无"))
            elif not name.startswith(prefix):
                fail("%s：Ingress %s 的中间件注解指向 %r，而 traefik 的 CRD provider 按"
                     "「命名空间-名字」登记中间件——同一命名空间里也必须写成 %s，只写名字的"
                     "引用解析不到，整条路由会被丢弃"
                     % (pname, iname, entry, "%s%s@kubernetescrd" % (prefix, obj_name)))

if judged == 0:
    print("判不了：一个 Service / 工作负载 / NetworkPolicy / ServiceAccount 都没读到，"
          "这条判据没有题目",
          file=sys.stderr)
    sys.exit(2)

if problems:
    for p in problems:
        print(p, file=sys.stderr)
    print("控制面可达性：%d 处不可达形状（读了 %d 个 profile 与交付树 %s）"
          % (len(problems), len(profiles), APPLY), file=sys.stderr)
    sys.exit(1)

print("控制面可达性：%d 个 profile 与交付树 %s 全过（Service 选择与端口、副本数、探针端口、"
      "卷引用、Ingress 后端与中间件引用、NetworkPolicy 选择与端口、ServiceAccount 与 RBAC 引用，"
      "共判 %d 个对象）"
      % (len(profiles), APPLY, judged))
PYEOF

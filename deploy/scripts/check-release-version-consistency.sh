#!/usr/bin/env bash
# 发布版本一致性门禁：交付物里写着版本号的每一处，都要等于 Cargo.toml 声明的那一个。
#
# 这条判据原先住在 build-release-image.sh 里，判的是一份写死的四文件名单
# （deployment / gateway-deployment / sandbox-executor-deployment / evolution-deployment）。
# 树里跑本平台镜像的工作负载早就不止这四个：process-census、claim-store、host-ledger、
# cluster-registry、buildah 都在里面，其中一个还自己写上了版本标签。名单没点到的那些，
# 标签停在上一版也没有任何人说话——release 发出去的镜像里，那几个 Pod 挂的版本号与镜像
# 不是同一个，而门读作绿。名单与树是同一个事实的两份写法，中间没有东西比较。
#
# 判据（集合全部从树里读出来，本脚本不写第二份）：
#   1. chart：Chart.yaml 的 version 与 appVersion、values.yaml 的 image.tag；
#   2. 静态清单：deploy/k3s/*.yaml 里**本平台自己的**工作负载——kind 是
#      Deployment / DaemonSet / StatefulSet，且 Pod 模板的 app.kubernetes.io/name 以
#      `cogneva` 开头。第三方工作负载不归这条判：它们的版本标签写的是它们自己的版本
#      （postgres 16、qdrant 1.13、redis 7-alpine），拿发布版本去比是错的；
#   3. 这些工作负载的 Pod 模板上必须有 app.kubernetes.io/version，值等于声明的版本；
#      对象级也写了的话同样要比，但不强求写；
#   4. deploy/k3s/configmap.yaml 的 COGNEVA_APP_VERSION。
#
# 为什么要求「必须有」而不只是「写了就得对」：这个标签是
# `kubectl get pods -L app.kubernetes.io/version` 那一列的唯一来源，缺了就是空格。
# 空格读作「这个 Pod 不归发布管」，与「它跑的是上一版」在屏上没有区别。
#
# 一个本平台工作负载都没读到（或树被指到别处）时判「判不了」（退出码 2），不是全过：
# 读取器坏掉与树上本来就没有工作负载，在问题清单上都表现为空。
#
# 用法：bash deploy/scripts/check-release-version-consistency.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"

if ! VERSION="$(bash "${ROOT}/deploy/scripts/declared-version.sh")"; then
    echo "读不到声明版本——判不了，不代表一致" >&2
    exit 2
fi
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" "$VERSION" <<'PYEOF'
import glob, os, re, sys

try:
    import yaml
except ImportError:
    sys.exit("缺少依赖：PyYAML（python3 -c 'import yaml' 要能过）")

ROOT, WANT = sys.argv[1], sys.argv[2]
problems = []


def read(rel):
    path = os.path.join(ROOT, rel)
    if not os.path.isfile(path):
        problems.append(f"读不到 {rel}")
        return ""
    return open(path, encoding="utf-8").read()


# ── 1. chart 的版本声明 ──
chart = read("deploy/helm/cogneva/Chart.yaml")
for key in ("version", "appVersion"):
    m = re.search(rf'^{key}:\s*"?([^"\n]+?)"?\s*$', chart, re.M)
    if not m:
        problems.append(f"deploy/helm/cogneva/Chart.yaml: 没有 {key}")
    elif m.group(1) != WANT:
        problems.append(
            f"deploy/helm/cogneva/Chart.yaml: {key} 是 {m.group(1)}，声明版本是 {WANT}"
        )

values = read("deploy/helm/cogneva/values.yaml")
m = re.search(r'^image:\s*\n(?:\s+[^\n]*\n)*?\s+tag:\s*"([^"]+)"', values, re.M)
if not m:
    problems.append("deploy/helm/cogneva/values.yaml: 读不到 image.tag")
elif m.group(1) != WANT:
    problems.append(
        f"deploy/helm/cogneva/values.yaml: image.tag 是 {m.group(1)}，声明版本是 {WANT}"
    )

# ── 2-3. 静态清单里本平台自己的工作负载 ──
OUR_KINDS = ("Deployment", "DaemonSet", "StatefulSet")
judged = []
for path in sorted(glob.glob(os.path.join(ROOT, "deploy/k3s/*.yaml"))):
    rel = os.path.relpath(path, ROOT)
    try:
        with open(path, encoding="utf-8") as fh:
            docs = list(yaml.safe_load_all(fh))
    except yaml.YAMLError as exc:
        # 读不成也要点名：一份读不懂的清单没被这条判据看过，静默跳过就成了假绿。
        problems.append(f"{rel}: 读不成 YAML（{exc}）——这一份没有判")
        continue
    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") not in OUR_KINDS:
            continue
        meta = doc.get("metadata") or {}
        template = (doc.get("spec") or {}).get("template") or {}
        labels = (template.get("metadata") or {}).get("labels") or {}
        if not str(labels.get("app.kubernetes.io/name", "")).startswith("cogneva"):
            continue
        ident = f"{rel}: {doc['kind']}/{meta.get('name')}"
        judged.append(ident)

        pod_version = labels.get("app.kubernetes.io/version")
        if pod_version is None:
            problems.append(
                f"{ident}: Pod 模板没有 app.kubernetes.io/version——"
                "`kubectl get pods -L app.kubernetes.io/version` 上这一格是空的，"
                "它跑的是哪一版在屏上看不出来"
            )
        elif pod_version != WANT:
            problems.append(
                f"{ident}: Pod 模板的版本标签是 {pod_version}，声明版本是 {WANT}"
            )

        object_version = (meta.get("labels") or {}).get("app.kubernetes.io/version")
        if object_version is not None and object_version != WANT:
            problems.append(
                f"{ident}: 对象上的版本标签是 {object_version}，声明版本是 {WANT}"
            )

# ── 4. 主应用读的那一份版本号 ──
cfg = read("deploy/k3s/configmap.yaml")
m = re.search(r'COGNEVA_APP_VERSION:\s*"?([^"\n]+?)"?\s*$', cfg, re.M)
if not m:
    problems.append("deploy/k3s/configmap.yaml: 没有 COGNEVA_APP_VERSION")
elif m.group(1) != WANT:
    problems.append(
        f"deploy/k3s/configmap.yaml: COGNEVA_APP_VERSION 是 {m.group(1)}，声明版本是 {WANT}"
    )

if problems:
    sys.exit(
        f"版本漂移（声明版本 {WANT}）：\n  " + "\n  ".join(problems)
    )

if not judged:
    sys.stderr.write(
        "deploy/k3s/ 下读不到任何一个本平台工作负载——判不了，这不等于全过。\n"
    )
    sys.exit(2)

print(
    f"ok：chart 的 version 与 appVersion、values 的 image.tag、"
    f"configmap 的 COGNEVA_APP_VERSION，以及 deploy/k3s 里 {len(judged)} 个本平台"
    f"工作负载的 Pod 模板版本标签，都是 {WANT}"
)
PYEOF

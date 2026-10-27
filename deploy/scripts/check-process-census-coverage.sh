#!/usr/bin/env bash
# 进程读数全覆盖的门禁：屏上每一个容器都要有「里面跑着什么」，不能只有跑本平台镜像的那些。
#
# 这条门来自一次现场（2026-10-27）：容器展开面板里，第三方镜像那一格写的是
#
#   没有自报的进程表：只有跑本平台镜像的容器会报（第三方镜像不会）。
#
# 而屏上 53 个容器里只有 1 个有这张表——连主应用自己的 Pod 都没有（它的
# `/proc/self/cgroup` 在私有 cgroup 命名空间里读出来是 `0::/`，取不到容器 id，于是
# 安静地不报）。「容器不报」被写成了正常态，而那其实是采集面缺了一半。
#
# 判据：要让**所有**容器都有进程表，就得有一个不依赖容器配合的读法，而它必须是：
#   1. 二进制里真有一条 `process-census` 子命令（cli 映射 + main 分派 + 模块）；
#   2. chart 里真有一份 `cogneva-process-census` DaemonSet，且带 `hostPID: true`
#      ——没有它 `/proc` 里只有本容器自己，第三方容器一个都读不到；
#   3. 它把宿主 cgroup 根**只读**挂进来——容器实例的边界只能从那里数出来；
#   4. 它不给自己发 SA token（它不跟 apiserver 说话）；
#   5. **每个** profile 的渲染产物里都有这份 DaemonSet，静态清单 `deploy/k3s/` 也有
#      ——profile 集合从 `deploy/rendered/` 的目录读，不在本脚本里抄一份名单：抄一份就
#      会在新增 profile 时只漏那一个，而「漏掉的那一个」与「本来就没有这个 profile」
#      从判词上看不出区别；
#   6. 上报的落点仍是那条零凭证的内部面（网关的 `/api/v1/cluster/process-report`）。
#
# 用法：bash deploy/scripts/check-process-census-coverage.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

try:
    import yaml
except ImportError:
    sys.exit("缺少依赖：PyYAML（python3 -c 'import yaml' 要能过）")

ROOT = sys.argv[1]
problems = []


def read(rel):
    path = os.path.join(ROOT, rel)
    if not os.path.isfile(path):
        problems.append(f"读不到 {rel}")
        return ""
    return open(path, encoding="utf-8").read()


# ── 1. 二进制里有一条 process-census 子命令 ──
cli = read("crates/cogneva/src/cli.rs")
main = read("crates/cogneva/src/main.rs")
module = read("crates/cogneva/src/process_census.rs")
if not re.search(r'\("process-census",\s*Command::ProcessCensus\)', cli):
    problems.append("cli.rs: 没有 `process-census` 子命令——DaemonSet 里写的那条命令会落到完整应用启动上")
if "Command::ProcessCensus" not in main:
    problems.append("main.rs: `Command::ProcessCensus` 没有分派——子命令解析得出来却没人接")
if "pub mod process_census;" not in read("crates/cogneva/src/lib.rs"):
    problems.append("lib.rs: 没有 `pub mod process_census;`")
if not module:
    problems.append("process_census.rs 不存在——采集器本体没有落地")
# 上报落点：与容器自报端点同一条零凭证内部面。
REPORT_PATH = "/api/v1/cluster/process-report"
if REPORT_PATH not in module and REPORT_PATH not in read("crates/cogneva/src/process_report.rs"):
    problems.append(f"采集器没有报向 {REPORT_PATH}——读数进不了网关那一格")

# ── 2-4. chart 里那份 DaemonSet 的形状 ──
def daemonset_docs(path):
    docs = []
    for doc in yaml.safe_load_all(open(path, encoding="utf-8")):
        if isinstance(doc, dict) and doc.get("kind") == "DaemonSet":
            docs.append(doc)
    return docs


def check_daemonset(label, docs):
    if not docs:
        problems.append(f"{label}: 没有 cogneva-process-census DaemonSet——所有容器的进程表都没有人能填")
        return
    spec = ((docs[0] or {}).get("spec") or {}).get("template", {}).get("spec", {}) or {}
    if spec.get("hostPID") is not True:
        problems.append(
            f"{label}: 没有 hostPID=true——`/proc` 里只有本容器自己，第三方镜像的容器一个都读不到"
        )
    if spec.get("automountServiceAccountToken") is not False:
        problems.append(
            f"{label}: 没有 automountServiceAccountToken=false——它不跟 apiserver 说话，"
            "发一份令牌只是多一把能用的钥匙"
        )
    mounts = []
    for container in spec.get("containers") or []:
        mounts.extend(container.get("volumeMounts") or [])
    cgroup_mounts = [m for m in mounts if str(m.get("mountPath", "")).endswith("cgroup")]
    if not cgroup_mounts:
        problems.append(f"{label}: 没有把宿主 cgroup 根挂进来——容器实例的边界数不出来")
    elif not all(m.get("readOnly") is True for m in cgroup_mounts):
        problems.append(f"{label}: cgroup 挂载不是只读——「看一眼」变成了能改宿主")


chart = read("deploy/helm/cogneva/templates/process-census.yaml")
if chart:
    # chart 是模板，helm 值没展开；hostPID 这类字面量直接读文本即可。
    if not re.search(r"^\s+hostPID: true$", chart, re.M):
        problems.append("chart process-census.yaml: 没有 hostPID: true")
    if not re.search(r"^\s+automountServiceAccountToken: false$", chart, re.M):
        problems.append("chart process-census.yaml: 没有 automountServiceAccountToken: false")
    if "readOnly: true" not in chart:
        problems.append("chart process-census.yaml: cgroup 挂载没有 readOnly")

# profile 的集合在这里是**读出来的**，不是写出来的：每一棵交付树都是 deploy/rendered/
# 下的一个目录。写死一份名单，新增的那一个 profile 就会安静地从这条判据里消失——它装上
# 有没有采集器，判词一个字节都不会变。
RENDERED = os.path.join(ROOT, "deploy/rendered")
PROFILES = sorted(
    name for name in os.listdir(RENDERED) if os.path.isdir(os.path.join(RENDERED, name))
) if os.path.isdir(RENDERED) else []
if not PROFILES:
    problems.append(
        "deploy/rendered/ 下一个 profile 目录都没有：判不了「每个 profile 都有采集器」，"
        "这不等于全过"
    )
for profile in PROFILES:
    rel = f"deploy/rendered/{profile}/40-daemonset-cogneva-process-census.yaml"
    path = os.path.join(ROOT, rel)
    if not os.path.isfile(path):
        problems.append(f"{rel}: 渲染产物里没有这份 DaemonSet——这个 profile 装上就是没有采集器")
        continue
    check_daemonset(rel, daemonset_docs(path))

static = os.path.join(ROOT, "deploy/k3s/process-census-daemonset.yaml")
if not os.path.isfile(static):
    problems.append(
        "deploy/k3s/process-census-daemonset.yaml: 静态清单里没有——"
        "集群内 GitOps 拉取端 apply 的那份没有它，集群里就一直不会出现采集器"
    )
else:
    check_daemonset("deploy/k3s/process-census-daemonset.yaml", daemonset_docs(static))

if problems:
    sys.exit(
        "进程读数没有覆盖到所有容器：\n  "
        + "\n  ".join(problems)
        + "\n只有跑本平台镜像的容器会自报，第三方镜像的容器就永远是空格——"
        "而空格读作「里面没有进程」。"
    )

print("ok：process-census 子命令、chart、%d 个 profile 的渲染产物与静态清单都在，hostPID 与只读 cgroup 挂载齐"
      % len(PROFILES))
PYEOF

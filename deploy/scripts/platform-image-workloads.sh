#!/usr/bin/env bash
# 哪些工作负载跑着本项目的镜像——由交付树回答，不在这里列名字。
#
# 为什么要有这一份：换镜像面不止几个 Deployment。宿主账本 / 进程普查 / 声明仓是
# DaemonSet，注册仓的卷走查是边车。谁要滚动镜像，就得把这几类一起滚，而「名单
# 写在哪里」的答案是树——清单里 pin 了本镜像的容器，就是这一趟要换的容器。
# 每个调用方各抄一份名单，就是同一份词汇写两遍、没有任何东西比较它们：新增一个
# 工作负载时，抄漏的那一处不会红，它只是安静地继续跑旧二进制。
#
# 用法：
#   deploy/scripts/platform-image-workloads.sh [清单目录]
#   缺省目录是 deploy/k3s（apply 路径的交付树）。
#   环境变量 IMAGE_REPO 覆盖认定的镜像仓库名（缺省 localhost:30500/cogneva）。
#
# 输出：每行 `<kind>\t<名字>\t<容器>`。
# 退出码：0 = 读到了；2 = 判不了（目录不在、解析失败、或集合为空）——
# 「一个都没有」与「没读出来」必须分得开，否则空集合会退化成一次静默的空滚动。

set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/../.." && pwd)"
root="${1:-${repo_root}/deploy/k3s}"
image_repo="${IMAGE_REPO:-localhost:30500/cogneva}"

if [ ! -d "$root" ]; then
    printf 'platform-image-workloads: 清单目录不在：%s——判不了该滚谁\n' "$root" >&2
    exit 2
fi

rows="$(python3 - "$root" "$image_repo" <<'PY'
import os, sys, yaml

root, repo = sys.argv[1], sys.argv[2]
# 同一个镜像的两种写法规矩都算数：集群内 registry 引用与节点本地 tag。
REPOS = (repo, "localhost/" + repo.rsplit("/", 1)[-1])
KINDS = ("Deployment", "DaemonSet", "StatefulSet")

def repo_of(image):
    image = image.split("@")[0]
    last = image.rsplit("/", 1)[-1]
    return image.rsplit(":", 1)[0] if ":" in last else image

rows = []
for name in sorted(os.listdir(root)):
    if not name.endswith(".yaml"):
        continue
    with open(os.path.join(root, name)) as handle:
        for doc in yaml.safe_load_all(handle):
            if not isinstance(doc, dict) or doc.get("kind") not in KINDS:
                continue
            spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
            for container in spec.get("containers") or []:
                if repo_of(container.get("image", "")) in REPOS:
                    rows.append((doc["kind"],
                                 (doc.get("metadata") or {}).get("name"),
                                 container.get("name")))

for kind, name, container in rows:
    print("%s\t%s\t%s" % (kind, name, container))
PY
)" || {
    printf 'platform-image-workloads: %s 下的清单解析失败——判不了该滚谁\n' "$root" >&2
    exit 2
}

if [ -z "$rows" ]; then
    printf 'platform-image-workloads: %s 下没有任何工作负载引用 %s——判不了该滚谁\n' \
        "$root" "$image_repo" >&2
    exit 2
fi

printf '%s\n' "$rows"

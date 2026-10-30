#!/usr/bin/env bash
# sync-static-manifests.sh 的定向测试。
#
# 为什么要有这份测试：这条命令是「静态清单里那两块渲染值由机器写、不由人抄」的那
# 一半。它最坏的形状与别的门一样——脚本还在、输出还像样、退出码恒 0——于是每次改
# 配置都要人手抄一遍，而门从不告诉你抄漏了。所以它必须自己证明三件事：
#
#   1. 原样的树上是绿的（空跑也要有读数，否则「一直绿」与「从没跑过」同形）；
#   2. chart 改了、静态清单没跟上时，它红，并且点名是哪一份；
#   3. 写回之后变绿，而且写回去的正是 chart 那一份（解析后逐字段相等）。
#
# 渲染产物侧同理：指纹改了而静态清单还是旧的，定向红在那一份 Deployment 上。
#
# 每个用例都在真树的一份副本上跑（脚本按自身位置算仓库根，副本自成一体）。
#
# 用法：bash deploy/scripts/tests/sync-static-manifests.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }

for bin in python3; do
    command -v "${bin}" >/dev/null || { echo "缺少依赖：${bin}" >&2; exit 2; }
done
python3 -c 'import yaml' 2>/dev/null || { echo "缺少依赖：python3-yaml" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

case_dir() { # case_dir <名字> -> 打印一份可以随便改的树根
    local d="${work}/$1"
    mkdir -p "${d}"
    cp -a "${repo}/deploy" "${d}/deploy"
    printf '%s' "${d}"
}

script_in() { printf '%s' "$1/deploy/scripts/sync-static-manifests.sh"; }

run() { # run <树根> <参数…> -> 打印合并输出，回传退出码
    local root="$1"; shift
    bash "$(script_in "${root}")" "$@" 2>&1
}

expect_ok() { # expect_ok <名字> <输出>
    local name="$1" out="$2"
    [[ "${out}" == *"SYNC STATIC OK"* ]] || fail "${name}: 正常树上应当绿，实际输出：${out}"
}

expect_red_naming() { # expect_red_naming <名字> <输出> <应当被点名的路径>
    local name="$1" out="$2" file="$3"
    [[ "${out}" == *"FAIL"* ]] || fail "${name}: 漂移没被看见，实际输出：${out}"
    [[ "${out}" == *"${file}"* ]] || fail "${name}: 红是红了，但没点名 ${file}：${out}"
}

# ---- 1. 原样：绿 ----
root="$(case_dir base)"
out="$(run "${root}" --check)" || fail "正常树上 --check 应当退出 0：${out}"
expect_ok "原样" "${out}"

# ---- 2. 配置改了一个值：红，点名配置清单 ----
root="$(case_dir config-drift)"
python3 - "${root}" <<'PY'
import sys, pathlib
p = pathlib.Path(sys.argv[1]) / "deploy/helm/cogneva/files/cogneva.json"
s = p.read_text()
assert '"event_channel_capacity": 1024' in s, "夹具依赖的值不在 chart 配置里了"
p.write_text(s.replace('"event_channel_capacity": 1024', '"event_channel_capacity": 2048', 1))
PY
out="$(run "${root}" --check)" && fail "配置漂移时 --check 应当非零：${out}"
expect_red_naming "配置漂移" "${out}" "deploy/k3s/cogneva-json-configmap.yaml"

# ---- 3. 写回：绿，且正文就是 chart 那一份 ----
out="$(run "${root}")" || fail "写回失败：${out}"
out="$(run "${root}" --check)" || fail "写回之后仍然红：${out}"
expect_ok "写回后" "${out}"
python3 - "${root}" <<'PY' || exit 1
import sys, json, pathlib, yaml
root = pathlib.Path(sys.argv[1])
chart = json.loads((root / "deploy/helm/cogneva/files/cogneva.json").read_text())
doc = yaml.safe_load((root / "deploy/k3s/cogneva-json-configmap.yaml").read_text())
body = json.loads(list(doc["data"].values())[0])
assert body == chart, "写回去的正文与 chart 的解析结果不一致"
assert body["system"]["event_channel_capacity"] == 2048, "写回去的是旧值"
PY

# ---- 4. 指纹改了：红，点名那一份 Deployment ----
root="$(case_dir fingerprint-drift)"
target="$(python3 - "${root}" <<'PY'
import sys, pathlib, yaml
root = pathlib.Path(sys.argv[1])
for yml in sorted((root / "deploy/rendered/k3s-single").glob("*.yaml")):
    for doc in yaml.safe_load_all(yml.read_text()):
        if isinstance(doc, dict) and doc.get("kind") == "Deployment":
            ann = doc["spec"]["template"]["metadata"].get("annotations") or {}
            if "checksum/config" in ann:
                text = yml.read_text()
                old = ann["checksum/config"]
                yml.write_text(text.replace(old, "a" * 64, 1))
                print(doc["metadata"]["name"])
                raise SystemExit(0)
PY
)"
[ -n "${target}" ] || fail "渲染产物里一份带 checksum/config 的 Deployment 都没有，这条用例失去意义"
out="$(run "${root}" --check)" && fail "指纹漂移时 --check 应当非零：${out}"
expect_red_naming "指纹漂移" "${out}" "deploy/k3s/"

out="$(run "${root}")" || fail "指纹写回失败：${out}"
out="$(run "${root}" --check)" || fail "指纹写回之后仍然红：${out}"
expect_ok "指纹写回后" "${out}"

# ---- 5. 静态清单里根本没有这个键：不猜，直接说 ----
root="$(case_dir no-block)"
python3 - "${root}" <<'PY'
import sys, pathlib
p = pathlib.Path(sys.argv[1]) / "deploy/k3s/cogneva-json-configmap.yaml"
p.write_text(p.read_text().replace("  cogneva.json: |-", "  cogneva.json: ある", 1))
PY
out="$(run "${root}" --check)" && fail "少了块标量时应当红：${out}"
[[ "${out}" == *"cogneva.json"* ]] || fail "应当点名那个键：${out}"

echo "SYNC STATIC TEST OK：5/5"

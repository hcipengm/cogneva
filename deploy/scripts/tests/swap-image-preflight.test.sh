#!/usr/bin/env bash
# 换版脚本启动前校验的判据面：拒绝换版必须读 **registry 里的 :local**，
# 而不是节点本地 tag。
#
# 为什么需要这条判据：四个部署 pin 的是 localhost:30500/cogneva:local 且
# imagePullPolicy: Always，kubelet 每次启动都从 registry 拉；节点本地
# localhost/cogneva:local 只作暖缓存/离线回退，不在运行路径上。按节点 tag 判
# 的方向是**误报脱节**——运行面已经对齐也照样拒绝换版，而它印出的止血命令修的是
# 那个不参与运行的面（2026-09-26 线上实测就是这个状态：运行镜像与 registry :local
# 同为 5aa2da5a，节点 :local 是 2b53e2c5）。
#
# 判据跑真脚本的 `--check-only` 路径（不构建、不 apply），用桩替换三个外部读，
# 三格互为对照：
#   场景 A registry 与运行镜像一致、节点不同 → 放行（stderr 点名节点本地），退出 0
#   场景 B registry 与运行镜像不同、节点也旧 → 拒绝（点名两侧 digest 与指向 registry
#                                          的止血命令），退出 1
#   场景 C registry 旧、节点恰好是运行镜像 → 拒绝；这一格是旧判据会**静默放行**
#                                          的方向，换版照做而 apply 会把线上打回旧版
# A 必须放行、C 必须拒绝，两条合起来才说明判据读的是 registry：只读节点 tag 时 A 会
# 误报脱节、C 会漏报。
# 桩的取证形状逐字段抄自真工具实测输出（`kubectl get pods -o json`、
# `crictl inspecti`、registry manifest），并由开头那段 python3 断言其字段路径
# 与脚本的解析路径一致——判据依的是这里埋进去的 digest，读数里必须出现它们，
# 否则"通过"可能只是没读到任何东西。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/../../.." && pwd)"
script="${repo_root}/deploy/k3s/swap-image.sh"
[ -f "$script" ] || { echo "找不到换版脚本 ${script}"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
stub="${work}/bin"
mkdir -p "$stub"

# 三份 digest 各自用可区分的重复字符，读数里一眼能对上号
RUN_MANIFEST="sha256:$(printf 'a%.0s' {1..64})"   # 运行镜像的 manifest digest
RUN_CONFIG="$(printf 'b%.0s' {1..64})"            # 运行镜像的 config digest
STALE_CONFIG="$(printf 'c%.0s' {1..64})"          # 与运行镜像不同的那一份
RUN_VERSION_TAG="main-0123456789ab"

# 运行中 Pod 的取证（形状抄自 kubectl -n cogneva get pods -o json）
cat > "${work}/pods.json" <<EOF
{"items":[{"metadata":{"name":"cogneva-6b56846d59-ppcm7"},
"spec":{"containers":[{"name":"cogneva"}]},
"status":{"containerStatuses":[{"name":"cogneva","ready":true,
"imageID":"localhost:30500/cogneva@${RUN_MANIFEST}"}]}}]}
EOF

# crictl inspecti 的取证（形状抄自 k3s crictl inspecti 实测输出）
cat > "${work}/running-inspecti.json" <<EOF
{"info":{},"status":{"id":"sha256:${RUN_CONFIG}",
"repoTags":["localhost:30500/cogneva:${RUN_VERSION_TAG}","localhost:30500/cogneva:local"]}}
EOF
node_local() { # $1=config digest：节点本地 tag 指向的那一份
  cat > "${work}/node-local.json" <<EOF
{"info":{},"status":{"id":"sha256:$1",
"repoTags":["localhost/cogneva:0.5.7","localhost/cogneva:local"]}}
EOF
}
node_local "$STALE_CONFIG"

# registry manifest 的取证（形状抄自 /v2/<name>/manifests/<ref> 实测响应）。
# ref 既可能是 tag（:local）也可能是 manifest digest（Pod 的 imageID 折算走的就是
# 这一条），registry 两种都答，桩按 URL 末段分文件。
registry_manifest() { # $1=ref（tag 或 manifest digest） $2=config digest
  cat > "${work}/registry-$1.json" <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
"config":{"digest":"sha256:$2","mediaType":"application/vnd.oci.image.config.v1+json"},
"layers":[]}
EOF
}

cat > "${stub}/kubectl" <<EOF
#!/usr/bin/env bash
# 桩：只实现脚本用到的那一条读（get pods -o json）
case " \$* " in
  *" get pods "*) cat "${work}/pods.json" ;;
  *) echo "stub kubectl 未实现: \$*" >&2; exit 1 ;;
esac
EOF

cat > "${stub}/k3s" <<EOF
#!/usr/bin/env bash
# 桩：只实现 crictl inspecti <ref>；ref 指向 :local 就回节点本地那份取证
case " \$* " in
  *" crictl inspecti "*)
    ref="\${@: -1}"
    case "\$ref" in
      *":local") cat "${work}/node-local.json" ;;
      *) cat "${work}/running-inspecti.json" ;;
    esac ;;
  *) echo "stub k3s 未实现: \$*" >&2; exit 1 ;;
esac
EOF

cat > "${stub}/curl" <<EOF
#!/usr/bin/env bash
# 桩：只实现 registry manifest 读（URL 末段是 tag）
url="\${@: -1}"
case "\$url" in
  */v2/cogneva/manifests/*) cat "${work}/registry-\${url##*/}.json" ;;
  *) echo "stub curl 未实现: \$*" >&2; exit 22 ;;
esac
EOF
chmod +x "${stub}/kubectl" "${stub}/k3s" "${stub}/curl"

# 桩自证：取证里的字段路径就是脚本解析的字段路径
registry_manifest "$RUN_MANIFEST" "$RUN_CONFIG"   # 运行镜像自己的 manifest（按 digest 取）
registry_manifest local "$RUN_CONFIG"             # registry :local 暂与运行镜像一致
python3 - "${work}/pods.json" "${work}/running-inspecti.json" "${work}/node-local.json" \
         "${work}/registry-local.json" "${work}/registry-${RUN_MANIFEST}.json" <<'PY'
import json, sys
pods, running, node, local, by_digest = sys.argv[1:6]
cs = json.load(open(pods))["items"][0]["status"]["containerStatuses"][0]
assert cs["name"] == "cogneva" and cs["ready"], cs
assert "@sha256:" in cs["imageID"], cs
for path in (running, node):
    status = json.load(open(path))["status"]
    assert status["id"].startswith("sha256:"), status
    assert status["repoTags"], status
for path in (local, by_digest):
    assert json.load(open(path))["config"]["digest"].startswith("sha256:"), path
print("桩取证字段路径与脚本解析路径一致")
PY

run_check() { # $1=stdout 文件 $2=stderr 文件；返回脚本退出码
  local rc=0
  PATH="${stub}:${PATH}" bash "$script" --check-only >"$1" 2>"$2" || rc=$?
  printf '%s' "$rc"
}

fails=0
check() { # $1=描述 $2=条件（0 为通过）
  if [ "$2" -ne 0 ]; then
    echo "FAIL: $1"
    fails=$((fails + 1))
  else
    echo "ok: $1"
  fi
}
contains() { grep -qF -- "$2" "$1"; }

echo "--- 场景 A：registry 与运行镜像一致，节点本地 tag 不同（必须放行）"
registry_manifest local "$RUN_CONFIG"
rc="$(run_check "${work}/a.out" "${work}/a.err")"
check "退出码 0（实际 ${rc}）" "$([ "$rc" -eq 0 ] && echo 0 || echo 1)"
check "stdout 报出与运行镜像一致的 registry 读数（含 ${RUN_CONFIG:0:12}）" \
  "$(contains "${work}/a.out" "registry :local 与运行镜像一致（${RUN_CONFIG:0:12}）" && echo 0 || echo 1)"
check "stderr 点名节点本地 tag 不一致" \
  "$(contains "${work}/a.err" "节点本地" && echo 0 || echo 1)"
check "stderr 不说脱节" "$(contains "${work}/a.err" "脱节" && echo 1 || echo 0)"
check "stderr 不给节点侧止血命令" \
  "$(contains "${work}/a.err" "k3s ctr -n k8s.io images" && echo 1 || echo 0)"

echo "--- 场景 B：registry 与运行镜像不同（必须拒绝）"
registry_manifest local "$STALE_CONFIG"
rc="$(run_check "${work}/b.out" "${work}/b.err")"
check "退出码非 0（实际 ${rc}）" "$([ "$rc" -ne 0 ] && echo 0 || echo 1)"
check "stderr 说脱节" "$(contains "${work}/b.err" "脱节" && echo 0 || echo 1)"
check "stderr 报运行镜像 digest（${RUN_CONFIG:0:12}）" \
  "$(contains "${work}/b.err" "${RUN_CONFIG:0:12}" && echo 0 || echo 1)"
check "stderr 报 registry :local digest（${STALE_CONFIG:0:12}）" \
  "$(contains "${work}/b.err" "${STALE_CONFIG:0:12}" && echo 0 || echo 1)"
check "stderr 报运行版本 tag（${RUN_VERSION_TAG}）" \
  "$(contains "${work}/b.err" "${RUN_VERSION_TAG}" && echo 0 || echo 1)"
check "止血命令指向 registry 那一个面" \
  "$(contains "${work}/b.err" \
     "buildah push --tls-verify=false localhost/cogneva:${RUN_VERSION_TAG} localhost:30500/cogneva:local" \
     && echo 0 || echo 1)"
check "stdout 不报校验通过" \
  "$(contains "${work}/b.out" "校验通过" && echo 1 || echo 0)"

echo "--- 场景 C：registry 旧、节点本地 tag 恰好是运行镜像（必须拒绝）"
# 这一格是旧判据会**静默放行**的方向：判据读节点 tag 时两侧相同，于是换版照做，
# 而 apply/滚动会把线上打回 registry 里的旧镜像——同一个缺陷的两种征兆里更危险的那个。
registry_manifest local "$STALE_CONFIG"
node_local "$RUN_CONFIG"
rc="$(run_check "${work}/c.out" "${work}/c.err")"
check "退出码非 0（实际 ${rc}）" "$([ "$rc" -ne 0 ] && echo 0 || echo 1)"
check "stderr 说脱节" "$(contains "${work}/c.err" "脱节" && echo 0 || echo 1)"
check "stderr 报 registry :local digest（${STALE_CONFIG:0:12}）" \
  "$(contains "${work}/c.err" "${STALE_CONFIG:0:12}" && echo 0 || echo 1)"
check "stdout 不报校验通过" \
  "$(contains "${work}/c.out" "校验通过" && echo 1 || echo 0)"

if [ "$fails" -ne 0 ]; then
  # 失败要把两侧读数一并带出来：只有"哪条判据红了"读不出脚本当时说了什么
  for f in a.out a.err b.out b.err; do
    echo "--- ${f} ---"
    cat "${work}/${f}" 2>/dev/null || echo "(未产生)"
  done
  echo "${fails} 条判据未通过"
  exit 1
fi
echo "换版启动前校验判据面：全部通过"

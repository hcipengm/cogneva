#!/usr/bin/env bash
# Deterministic gate for host document access.
#
# The capability mounts a host directory into the sandbox executor, so two
# failures matter more than the happy path:
#   1. a scope that is configured but never mounted (or mounted but never
#      exported to the executor) — the operator believes document access is on
#      while every request answers "not configured";
#   2. a mount that appears with no scope behind it — an executor that can
#      reach a host directory nobody declared.
# Neither is visible from the values file alone, so the gate renders the chart
# in both states and reads the manifest it produces.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
chart="${repo}/deploy/helm/cogneva"
fail() { echo "FAIL: $*"; exit 1; }

render() {
  helm template cogneva "${chart}" --set secrets.create=false "$@"
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# --- 1) default: the capability is off, and provably so --------------------
default_out="$(render)"
if grep -q "HOST_DOCS_SCOPES" <<<"${default_out}"; then
  fail "默认 values 下执行器拿到了 HOST_DOCS_SCOPES；能力应当是关的"
fi
if grep -q "host-docs-" <<<"${default_out}"; then
  fail "默认 values 下出现了宿主目录挂载；默认必须不挂任何宿主路径"
fi

# --- 2) configured scope: env, mount and volume agree ----------------------
enabled_out="$(render --set hostDocuments.scopes.alice=/srv/alice/Documents)"
mount_expected="/opt/cogneva/host-docs/alice"
grep -q "value: \"alice=${mount_expected}\"" <<<"${enabled_out}" \
  || fail "HOST_DOCS_SCOPES 没有把 scope 名映射到挂载点"
grep -q "mountPath: ${mount_expected}" <<<"${enabled_out}" \
  || fail "声明了 scope 却没有对应的 volumeMount"
grep -q "path: \"/srv/alice/Documents\"" <<<"${enabled_out}" \
  || fail "宿主路径没有出现在 volumes 里"
# DirectoryOrCreate would let a typo create an empty directory on the host.
# Read the type from the host-docs volume itself: the chart uses
# DirectoryOrCreate elsewhere (buildah storage) and a whole-file grep would
# report that one instead.
host_volume_type() {
  awk '/name: host-docs-/{ hit = NR } hit && NR <= hit + 5 && /type:/ { print $2; exit }' "$1"
}
render --set hostDocuments.scopes.alice=/srv/alice/Documents > "${work}/render.yaml"
got_type="$(host_volume_type "${work}/render.yaml")"
[ "${got_type}" = "Directory" ] \
  || fail "host-docs 卷的类型是 ${got_type:-缺失}；必须是 Directory（DirectoryOrCreate 会在宿主上静默建空目录）"
# Negative control: the extractor has to answer "DirectoryOrCreate" when that
# is what the file says, otherwise the assertion above proves nothing.
sed 's/type: Directory$/type: DirectoryOrCreate/' "${work}/render.yaml" > "${work}/mutated.yaml"
if [ "$(host_volume_type "${work}/mutated.yaml")" != "DirectoryOrCreate" ]; then
  fail "自检失败：抽取器读不出被改成 DirectoryOrCreate 的那份清单"
fi

# --- 3) a scope entry with no path is a render error, not a silent skip ----
set +e
out="$(render --set hostDocuments.scopes.alice= 2>&1)"
rc=$?
set -e
[ "${rc}" -ne 0 ] || fail "scopes 里带空路径仍然渲染成功；应当 fail 而不是跳过这个 scope"
grep -q "hostDocuments.scopes.alice has no host path" <<<"${out}" \
  || fail "拒绝了，但不是因为空路径（原因不符）：${out}"

# --- 4) the executor reads every key the template sets --------------------
executor_rs="${repo}/crates/cog-extension/src/hostdocs.rs"
[ -f "${executor_rs}" ] || fail "找不到 ${executor_rs}"
for env_name in HOST_DOCS_SCOPES HOST_DOCS_JOURNAL_DIR HOST_DOCS_MAX_WRITE_BYTES; do
  grep -q "\"${env_name}\"" "${executor_rs}" \
    || fail "${env_name} 在模板里下发，但执行器不读它（死旋钮）"
done
# The journal has to outlive the process that wrote it, otherwise a rollback
# after a restart restores nothing: the default path must be on the executor
# volume, which the pod mounts at /opt/cogneva/sandbox.
grep -q 'const DEFAULT_JOURNAL_DIR: &str = "/opt/cogneva/sandbox' "${executor_rs}" \
  || fail "回滚日志的默认目录不在执行器卷上；进程重启后日志会丢"

echo "PASS: 文档范围按身份挂载、默认关闭、模板下发的每个键都有执行器侧读取方"

#!/usr/bin/env bash
# 恢复端怎么读「包里没有密钥清单」这件事。
#
# 写端已经把两片 Secret 清单做成了条件面：只有包本身是密文时才导进包，明文包里
# 没有它们，manifest 的 lossy_surfaces 记一条 name=credentials。恢复脚本早先的写法
# 是「没有清单就 exit 1」——于是这个刻意做出来的形状在恢复端读起来是「坏包」，而
# 换机恢复正是最不该含糊的时刻。
#
# 三种形状都必须有确定答案，判据只看包内文件与 manifest 自己那一笔，不看包名：
#   有清单            → 逐个 apply，两片都要（带外那片漏 apply 集群照样起得来，
#                       直到推代码或访问上游时才发现身份是空的）；
#   没清单 + 认领了   → 不 apply，但要说清哪一半由安装器重生成、哪一半要重进向导；
#   没清单 + 没认领   → 非零退出：清单缺席而账上没有这一笔，就是包不完整，
#                       放它过去等于悄悄换掉实例身份还报成功。
#
# 这一段从脚本里按标记抽出来跑，不复制一份：复制的那份会在脚本改了之后继续绿。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
script="${repo}/deploy/scripts/restore-from-package.sh"
fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/bin"

# kubectl 只回答 apply：记下被 apply 的文件路径，别的调用一律报错而不是静默通过。
cat >"${work}/bin/kubectl" <<'STUB'
#!/usr/bin/env bash
case "$*" in
    "apply -f "*) echo "$3" >>"${APPLIED_LOG}" ;;
    *) echo "stub kubectl: unexpected call: $*" >&2; exit 1 ;;
esac
STUB
chmod +x "${work}/bin/kubectl"
export PATH="${work}/bin:${PATH}"
export APPLIED_LOG="${work}/applied.log"

sed -n '/^# secret-restore: start$/,/^# secret-restore: end$/p' "${script}" >"${work}/block.sh"
[ -s "${work}/block.sh" ] || fail "no secret-restore block found in ${script}"

# 跑一次：把包目录摆好，source 抽取出来的那一段，返回脚本自己的退出码。
run_block() {
    local dir="$1"
    : >"${APPLIED_LOG}"
    (
        set -euo pipefail
        # shellcheck disable=SC1090
        source "${work}/block.sh"
        restore_secret_manifests "${dir}" || exit 1
    )
}

manifest_with_loss() {
    cat >"$1/manifest.json" <<'JSON'
{
  "schema_version": 1,
  "instance_fingerprint": "abc123",
  "secrets_keys": [],
  "lossy_surfaces": [
    {
      "name": "credentials",
      "lost": "两片 Secret 对象清单",
      "acceptable_because": "包不是密文时不把凭证抄进包"
    }
  ]
}
JSON
}

manifest_without_loss() {
    cat >"$1/manifest.json" <<'JSON'
{
  "schema_version": 1,
  "instance_fingerprint": "abc123",
  "secrets_keys": ["pg-password"],
  "lossy_surfaces": []
}
JSON
}

# 1) 有清单：两片都 apply。少了 secrets-oob 这一片，换机后平台 token 与 git 私钥
#    是空的，而恢复回执照样说成功。
pkg="${work}/sealed"
mkdir -p "${pkg}/secrets"
manifest_without_loss "${pkg}"
: >"${pkg}/secrets/cogneva-secrets.yaml"
: >"${pkg}/secrets/cogneva-secrets-oob.yaml"
run_block "${pkg}" || fail "有清单的包没能恢复：$(cat "${APPLIED_LOG}")"
applied="$(sort "${APPLIED_LOG}")"
grep -q "cogneva-secrets.yaml$" <<<"${applied}" || fail "内部片没有被 apply：${applied}"
grep -q "cogneva-secrets-oob.yaml$" <<<"${applied}" || fail "带外片没有被 apply：${applied}"

# 2) 没清单 + manifest 认领：不 apply、退出码 0，且说清哪一半要靠谁补回来。
pkg="${work}/plaintext"
mkdir -p "${pkg}"
manifest_with_loss "${pkg}"
out="$(run_block "${pkg}" 2>&1)" || fail "认领了凭证损失的包被判成坏包：${out}"
[ -s "${APPLIED_LOG}" ] && fail "没有清单却调用了 apply：$(cat "${APPLIED_LOG}")"
grep -q "向导" <<<"${out}" || fail "没有说清带外那片要重进向导：${out}"

# 3) 没清单 + manifest 没认领：非零退出。这是包不完整，不是有损面。
pkg="${work}/broken"
mkdir -p "${pkg}"
manifest_without_loss "${pkg}"
if run_block "${pkg}" >/dev/null 2>&1; then
    fail "清单缺席且 manifest 没认领，恢复端却放行了：这份包丢了实例身份也照样报成功"
fi
[ -s "${APPLIED_LOG}" ] && fail "坏包路径上调用了 apply：$(cat "${APPLIED_LOG}")"

echo "OK：有清单两片都 apply、认领的损失不 apply 但说清补法、没认领的缺席退出"

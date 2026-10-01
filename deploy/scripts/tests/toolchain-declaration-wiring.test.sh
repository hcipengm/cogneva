#!/usr/bin/env bash
# Deterministic gate for "one value, one holder" on the Rust toolchain this
# project compiles itself with.
#
# Several builds consume this source, and the compiler they use decides what
# "green" means: a gate that passed under one compiler says nothing about a
# binary produced by another. The release workflow used to keep its own copy of
# the pinned version next to a comment asking that it be kept in sync with the
# Dockerfile. A comment is not a gate, and what it leaves open is silent from
# both ends -- a binary compiled by a compiler other than the one CI validated
# reports the same version and passes the same tests, and a resolver that reads
# nothing falls through to the action's default toolchain and still builds.
# Neither artifact says which compiler produced it.
#
# The judgement is structural. The version has one reading
# (deploy/scripts/declared-toolchain.sh, which reads the Dockerfile), the
# consumers go through it, and the carriers that legitimately differ are
# enumerated here rather than left to whoever opens the files next:
#   - the two CN mirror overrides, which must be `stable` because TUNA does not
#     mirror version channels;
#   - the runtime image's toolchain, which is a different toolchain on purpose:
#     the self-evolution worker builds changes at runtime and tracks the
#     channel, not the pin.
# A carrier that turns up without being declared here fails the gate.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
cd "$repo"
fail() { echo "FAIL: $*" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

reader="deploy/scripts/declared-toolchain.sh"
[ -f "${reader}" ] || fail "找不到 ${reader}"

# --- 1) the declaration has one reading, and it fails closed ------------------
declared="$(bash "${reader}")"
case "${declared}" in
  [0-9]*.[0-9]*) ;;
  *) fail "${reader} 读出的工具链不像版本号：${declared}" ;;
esac
# A reader that agrees with the file because it read nothing would satisfy every
# assertion below, so both broken shapes are exercised against a mutated copy:
# the declaration removed entirely (an empty toolchain reaches
# `rustup default ""`) and the declaration appearing twice (the answer then
# depends on file order).
scratch="${work}/scratch"
mkdir -p "${scratch}/deploy/scripts"
cp "${reader}" "${scratch}/deploy/scripts/"
for mutation in removed duplicated; do
  case "${mutation}" in
    removed)    grep -v '^ARG RUST_TOOLCHAIN="' Dockerfile > "${scratch}/Dockerfile" ;;
    duplicated) { cat Dockerfile; grep '^ARG RUST_TOOLCHAIN="' Dockerfile; } > "${scratch}/Dockerfile" ;;
  esac
  if out="$(bash "${scratch}/deploy/scripts/declared-toolchain.sh" 2>/dev/null)"; then
    fail "自检失败：声明${mutation}时读取器仍然成功（输出 ${out}）"
  fi
done

# --- 2) no consumer restates the version --------------------------------------
# The release workflow is the one that used to. It has to take the value from
# the reader, and the step that resolves it has to feed the install step -- a
# resolution nothing consumes would leave the action on its own default with
# nothing to say so.
grep -q -F 'declared-toolchain.sh' .github/workflows/release.yml \
  || fail ".github/workflows/release.yml 没有走 declared-toolchain.sh 取声明版本"
grep -qE 'toolchain:[[:space:]]*\$\{\{ steps\.[a-z0-9_-]+\.outputs\.version \}\}' \
     .github/workflows/release.yml \
  || fail "release.yml 的 Install Rust toolchain 没接上解析出来的版本"
# A literal anywhere in a workflow is the form the drift hides in, so none is
# allowed -- the reader exists to make it unnecessary.
if literals="$(grep -rnE '^[[:space:]]+toolchain:[[:space:]]*"[0-9]' .github/workflows/)"; then
  fail "还有 workflow 直接写死了工具链版本：${literals}"
fi

# --- 3) every carrier outside the declaration is enumerated -------------------
# Two CN overrides pass `stable`; the Dockerfile's second, runtime install is a
# different toolchain on purpose. Anything else naming the variable is an
# undeclared carrier: it is a new answer to "which compiler", and the count of
# answers is what this gate is for.
for consumer in deploy/scripts/build-release-image.sh crates/bootstrap/src/main.rs; do
  [ -f "${consumer}" ] || fail "找不到 ${consumer}"
  grep -q -F 'RUST_TOOLCHAIN=stable' "${consumer}" \
    || fail "${consumer} 不再走 CN 的 stable 覆盖；它现在是第几个答案要先说清楚"
done
grep -q -F -- '--default-toolchain stable' Dockerfile \
  || fail "运行期那把自进化工具链不再是 stable 通道了；它与钉住那把有意不同，变了要在这里说"
while IFS= read -r f; do
  case "${f}" in
    Dockerfile) continue ;;
    deploy/scripts/declared-toolchain.sh) continue ;;
    deploy/scripts/tests/toolchain-declaration-wiring.test.sh) continue ;;
    deploy/scripts/build-release-image.sh|crates/bootstrap/src/main.rs) continue ;;
  esac
  if grep -q -- 'RUST_TOOLCHAIN' "${f}" 2>/dev/null; then
    fail "${f} 也带上了 RUST_TOOLCHAIN；它是新出现的载体，得先在这个门禁里声明"
  fi
done < <(git ls-files)

ci_sites="$(grep -c 'dtolnay/rust-toolchain@' .github/workflows/ci.yml || true)"
echo "PASS: 工具链声明一份读法（Dockerfile→${reader}），release 工作流走它且不写死版本，" \
     "载体清点为 declared=${declared} cn_overrides=stable×2 runtime=stable（自进化 worker，有意不同）" \
     "ci_sites=${ci_sites}（stable 通道）"

#!/usr/bin/env bash
# Deterministic gate for the build label.
#
# The label exists because the declared version alone names many code states:
# measured 2026-09-26, `0.5.8` covered 93 consecutive commits. A producer that
# stamps a revision but not the label fails in the quietest possible way — the
# build succeeds, the image comes up, and it reports the bare declared version,
# which is the ambiguity the label was added to remove. Nothing in any single
# file shows that, so the gate reads every stamping producer, the one consumer
# that computes the stamp, and the two implementations of the describe call.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
cd "$repo"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# --- 1) the stamp is emitted at build time and read where it is reported -----
build_rs="crates/cogneva/build.rs"
grep -q 'rustc-env=COGNEVA_VERSION_ID=' "${build_rs}" \
  || fail "${build_rs} 不再产出 COGNEVA_VERSION_ID；没有它就没有构建标签"
# Without this, a checkout that gains a tag keeps the label it was built with.
grep -q 'rerun-if-env-changed=COGNEVA_VERSION_ID' "${build_rs}" \
  || fail "${build_rs} 不认 COGNEVA_VERSION_ID 的变化，注入的标签不会重新编译进去"
grep -q 'rerun-if-changed=.git/refs/tags' "${build_rs}" \
  || fail "${build_rs} 不盯 tag 变化；切了新 release 也不会重算标签"

# `--version` must print the label. The extractor is checked against a mutated
# copy, otherwise a broken extractor would pass this section by reading nothing.
version_arm() {
  awk '/Command::Version =>/{f=1} f{print} f && /^        }$/{exit}' "$1"
}
arm="$(version_arm crates/cogneva/src/main.rs)"
[ -n "${arm}" ] || fail "抽取器没取到 --version 的输出路径；后面两条断言都会空转"
grep -q -F 'COGNEVA_VERSION_ID' <<<"${arm}" \
  || fail "--version 没有输出构建标签；它会退回成两个代码状态共用一个声明版本"
if grep -q -F 'CARGO_PKG_VERSION' <<<"${arm}"; then
  fail "--version 仍在输出裸的声明版本"
fi
sed 's/COGNEVA_VERSION_ID/CARGO_PKG_VERSION/' crates/cogneva/src/main.rs > "${work}/main.rs"
if grep -q -F 'COGNEVA_VERSION_ID' <<<"$(version_arm "${work}/main.rs")"; then
  fail "自检失败：抽取器在被改回裸声明版本的那份里仍看到了标签"
fi

# --- 2) every producer that stamps a revision stamps the label too -----------
check_pair() {
  local file="$1" revision="$2" label="$3"
  [ -f "${file}" ] || fail "找不到 ${file}"
  grep -q -F -- "${revision}" "${file}" || fail "${file} 不再注入 revision（${revision}）"
  grep -q -F -- "${label}" "${file}" \
    || fail "${file} 注入了 revision 却没有注入构建标签（缺 ${label}）"
}
check_pair deploy/scripts/build-release-image.sh \
  'GIT_REVISION=${GIT_REVISION}' 'VERSION_ID=${VERSION_ID}'
check_pair deploy/k3s/swap-image.sh \
  'COGNEVA_GIT_REVISION=' 'COGNEVA_VERSION_ID='
check_pair crates/bootstrap/src/main.rs \
  'GIT_REVISION={revision}' 'VERSION_ID={version_id}'
check_pair crates/cog-reflection/src/mainline_deployer.rs \
  '.env("COGNEVA_GIT_REVISION"' '.env("COGNEVA_VERSION_ID"'
# 容器内没有 .git，标签与 revision 一样只能自外部进入编译期环境。
grep -q '^ARG VERSION_ID' Dockerfile || fail "Dockerfile 没有声明 ARG VERSION_ID"
grep -q 'COGNEVA_VERSION_ID=${VERSION_ID}' Dockerfile \
  || fail "Dockerfile 声明了 ARG VERSION_ID 却没有传进编译期环境"

# --- 3) the three implementations of the describe call agree, and stay legal --
# Flags are not a style choice here: without `--match` the cluster's own
# `promote/*` tags become candidates for the nearest release, and the label
# names a release the code does not descend from. Three hand-kept copies of a
# flag list is how that happens, so the gate compares them.
#
# The same flags are only legal together when the working tree is described:
# git refuses `--dirty` next to a commit-ish, so a copy that passes the revision
# would fail at runtime and silently report an unknown distance in place of a
# name. Both halves are asserted — the flags are there, and nothing follows the
# match pattern except the punctuation that closes the call.
#
# Checks read a comment-stripped, newline-folded view of each file. Comments
# matter because every one of these files explains its flags in prose, which
# keeps a whole-file search green long after the call lost the flag it claims to
# have; newlines matter because rustfmt decides how the argument list wraps.
code_view() { grep -vE '^[[:space:]]*(//|///|#)' "$1" | tr '\n' ' '; }
flags_ok() {
  local text="$1" missing=""
  for flag in --tags --long --dirty --match; do
    grep -q -F -- "${flag}" <<<"${text}" || missing="${missing} ${flag}"
  done
  grep -q -F -- 'v[0-9]*' <<<"${text}" || missing="${missing} v[0-9]*"
  if [ -n "${missing}" ]; then
    echo "missing:${missing}"
    return 1
  fi
}
# Everything after the match pattern, up to the paren or bracket that closes the
# call, must be punctuation and quoting only; a revision would survive it.
rev_ok() {
  local after="${1#*"v[0-9]*"}"
  after="${after%%)*}"
  after="$(tr -d "[:space:]'\";,]" <<<"${after}" | sed 's|2>[^ )]*||')"
  [ -z "${after}" ]
}
for file in deploy/scripts/version-id.sh \
            crates/cog-reflection/src/mainline_deployer.rs \
            crates/bootstrap/src/main.rs; do
  view="$(code_view "${file}")"
  grep -q -F 'describe' <<<"${view}" || fail "抽取器没取到 ${file} 的 describe 调用"
  flags_ok "${view}" \
    || fail "${file} 的 describe 参数不全（$(flags_ok "${view}")）；缺哪个都会静默报出错误的名字"
  rev_ok "${view}" \
    || fail "${file} 给 describe 传了 commit-ish；与 --dirty 同用时 git 直接报错，标签会静默退化成 unknown"
done
# Controls: the discriminators have to see the defect, or the assertions above
# can never fail.
view="$(code_view crates/cog-reflection/src/mainline_deployer.rs)"
if flags_ok "$(sed 's/--match//' <<<"${view}")" >/dev/null; then
  fail "自检失败：摘掉 --match 后仍判参数齐全，上面那条断言会永远通过"
fi
if rev_ok "$(sed 's|v\[0-9\]\*"|v[0-9]*", rev|' <<<"${view}")"; then
  fail "自检失败：看不见插回来的 rev 参数，上面那条断言会永远通过"
fi
# The refusal itself, not the belief about it.
ctl="${work}/refuse"
mkdir -p "${ctl}"
git -C "${ctl}" init -q
git -C "${ctl}" -c user.email=t@t -c user.name=t commit -q --allow-empty -m one
git -C "${ctl}" tag v9.9.9
if ! git -C "${ctl}" describe --tags --long --dirty --match 'v[0-9]*' >/dev/null; then
  fail "自检失败：不带 commit-ish 的 describe 都跑不通，后面那条对照没有意义"
fi
if git -C "${ctl}" describe --tags --long --dirty --match 'v[0-9]*' HEAD >/dev/null 2>&1; then
  fail "自检失败：这个 git 接受 --dirty 与 commit-ish 同时出现，上面那条规则的前提变了"
fi

# --- 4) the label is derived, and says so when it cannot be -----------------
version="$(bash deploy/scripts/declared-version.sh)"
out="$(bash deploy/scripts/version-id.sh)"
if git describe --tags --match 'v[0-9]*' --abbrev=0 >/dev/null 2>&1; then
  case "${out}" in
    "v${version}-"*) ;;
    *) fail "本检出上有可达 release tag，算出的标签却是 ${out}" ;;
  esac
  # 有 tag 就必须真的读出距离：读不到只能是因为没有 tag，不能两者都占。
  case "${out}" in
    *-unknown) fail "本检出上有可达 release tag，标签却把距离报成未知（${out}）" ;;
  esac
else
  case "${out}" in
    "v${version}-unknown") ;;
    *) fail "本检出上没有可达 release tag，标签应该是 v${version}-unknown，实际是 ${out}" ;;
  esac
fi
# A checkout with no tags must report an unknown distance, never a zero one: a
# zero would claim the commit is the release. 这段在 CI 的浅检出上也要成立，
# 所以用一棵临时仓库而不是依赖本检出有没有 tag。
mkdir -p "${work}/bare/deploy/scripts"
cp deploy/scripts/version-id.sh deploy/scripts/declared-version.sh "${work}/bare/deploy/scripts/"
cp Cargo.toml "${work}/bare/Cargo.toml"
git -C "${work}/bare" init -q
fallback="$(bash "${work}/bare/deploy/scripts/version-id.sh")"
case "${fallback}" in
  "v${version}-unknown") ;;
  *) fail "无 tag 的检出上算出的标签是 ${fallback}；读不到距离必须说 v${version}-unknown" ;;
esac

# --- 5) the declared version has one reading ---------------------------------
# 镜像 tag、chart appVersion、清单 version label、release 标签全部派生自它，
# 每多一份读法就多一个答案，而漂移在制品里只表现为一个字符串。
for f in deploy/scripts/version-id.sh deploy/scripts/build-release-image.sh \
         deploy/k3s/swap-image.sh .github/workflows/ci.yml; do
  grep -q -F 'declared-version.sh' "${f}" \
    || fail "${f} 没有走 declared-version.sh 取声明版本"
done
if grep -rn "grep -m1 '\^version'" \
     deploy/scripts deploy/k3s .github/workflows bootstrap.sh 2>/dev/null \
     | grep -v 'declared-version.sh' | grep -q .; then
  fail "还有地方在自行解析 Cargo.toml 的 version；声明版本会有第二个答案"
fi
echo "PASS: 构建标签派生自历史、四个生产者的溯源与它同源、describe 参数三处一致、读不到距离时明说、声明版本只有一份读法"

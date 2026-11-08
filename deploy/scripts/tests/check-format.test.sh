#!/usr/bin/env bash
# 这条门的判据只有一句：rustfmt 说这份源码排过没有。用例要在**两个方向的字节**上都判一次，
# 否则「排过的绿了」可以只是一条永远退 0 的脚本，而 CI 的 Format 作业照样红。
#
# 第三种结局也要判：没有 rustfmt 的机器上它退 2（问不了），不是退 0。「问不了」与「问过
# 了、是绿的」在退出码上必须分得开，否则一台没装 rustfmt 的机器会把每一笔都说成绿的。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gate="${repo}/deploy/scripts/check-format.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -f "${gate}" ] || fail "门脚本不存在：${gate}"
command -v cargo >/dev/null || fail "这个环境没有 cargo，用例判不了（CI 有）"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 一份最小工作区：一条门只该依赖它的判据，不该依赖本仓的结构。
mkdir -p "${work}/repo/deploy/scripts" "${work}/repo/src"
cp "${gate}" "${work}/repo/deploy/scripts/check-format.sh"
printf '[package]\nname = "fixture"\nversion = "0.0.0"\nedition = "2021"\n' >"${work}/repo/Cargo.toml"
printf 'pub fn f() -> u8 {\n    1\n}\n' >"${work}/repo/src/lib.rs"

out="$(bash "${work}/repo/deploy/scripts/check-format.sh" 2>&1)" ||
    fail "排过的源码应当绿，实际退非零：${out}"

# 一处 rustfmt 一定会改的写法：这一行的空格排法不是 rustfmt 的排法。
printf 'pub fn g( ) -> u8   {   2 }\n' >>"${work}/repo/src/lib.rs"
set +e
out="$(bash "${work}/repo/deploy/scripts/check-format.sh" 2>&1)"
rc=$?
set -e
[ "${rc}" -eq 1 ] || fail "没排过的源码应当退 1，实际退 ${rc}：${out}"
printf '%s\n' "${out}" | grep -q 'src/lib.rs' ||
    fail "红的判词里没点名那个文件，推的人不知道该改哪一份：${out}"
printf '%s\n' "${out}" | grep -q 'Format' ||
    fail "红的判词没说这意味着 CI 的哪一格会红：${out}"

# 没有 rustfmt：退 2（问不了），不是 0（绿）。
mkdir -p "${work}/bin"
# 一只「cargo 在、rustfmt 不在」的假 cargo：`cargo fmt --version` 退非零。
cat >"${work}/bin/cargo" <<'SH'
#!/usr/bin/env bash
[ "$1" = "fmt" ] && exit 101
exit 0
SH
chmod +x "${work}/bin/cargo"
set +e
out="$(PATH="${work}/bin:${PATH}" bash "${work}/repo/deploy/scripts/check-format.sh" 2>&1)"
rc=$?
set -e
[ "${rc}" -eq 2 ] || fail "没有 rustfmt 应当退 2（问不了），实际退 ${rc}：${out}"

echo "ok - check-format：排过的绿、没排过的红且点名文件、没有 rustfmt 退 2 不冒充绿"

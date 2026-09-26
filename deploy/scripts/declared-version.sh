#!/usr/bin/env bash
# 声明版本（Cargo.toml 的 workspace.package.version）的唯一一份读法。
#
# 镜像 tag、chart appVersion、清单 version label、release 标签都派生自这一个值。
# 每个消费面各抄一份 grep 迟早会有一处读到别的值，而那种漂移在制品里只表现为
# 一个字符串——所以读法收在这里，谁要就调这里。
#
# 只认 [workspace.package] 段里的 version：文件里别处出现 version = 时不该被当成
# 声明版本，读不到就显式失败而不是吐一个空值（空值会一路变成空 tag）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"

version="$(awk '
  /^\[workspace\.package\]/ { in_table = 1; next }
  /^\[/ { in_table = 0 }
  in_table && /^version[[:space:]]*=/ {
    if (match($0, /"[^"]*"/)) {
      print substr($0, RSTART + 1, RLENGTH - 2)
      exit
    }
  }
' "${repo}/Cargo.toml")"

if [ -z "${version}" ]; then
    echo "读不到 ${repo}/Cargo.toml 里 [workspace.package] 的 version" >&2
    exit 1
fi
printf '%s\n' "${version}"

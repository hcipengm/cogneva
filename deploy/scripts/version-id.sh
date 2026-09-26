#!/usr/bin/env bash
# 一个检出的构建标签：它继承自哪个 release、距那个 release 多少提交、以及自己的
# rev。空格无法表达的是"同一个声明版本下的两个代码状态"——0.5.8 曾经覆盖 93 个
# 连续提交，而唯一的 release tag 停在第一个上，于是"0.5.8"同时指 93 个代码状态。
# 这个标签由历史派生，所以任何生产者在同一检出上跑都得到同一个字符串，不需要
# 事先约定任何东西。
#
# 语法归 git（describe 的格式即契约），本脚本只负责一件事：把那次调用连同它的
# 参数收在一处。参数是有讲究的——
#   --long   即使正好在 release 提交上也带上 "-0-"，形状不随位置变化，调用方
#            不必猜"没有距离"是零还是读不到；
#   --match  只认 v<数字> 开头的 tag。集群里还有 promote/*、gen-* 等本地 tag，
#            漏了这个参数会把它们当成最近的 release，静默地报出一个错的名字。
#
# 读不到就明说读不到：没有 .git 或没有可达 tag 时输出 v<声明版本>-unknown，
# 而不是假装距离是零。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"

if id="$(git -C "${repo}" describe --tags --long --dirty --match 'v[0-9]*' 2>/dev/null)"; then
  printf '%s\n' "${id}"
  exit 0
fi

version="$(bash "${repo}/deploy/scripts/declared-version.sh")"
printf 'v%s-unknown\n' "${version}"

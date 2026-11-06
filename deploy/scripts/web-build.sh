#!/usr/bin/env bash
# 判一条 WebUI 变更：按 lockfile 装依赖，再类型检查 + 打包。
#
# 这个脚本是**前端这一族准入条件的唯一写法**，有两个读者：
#   - CI 的 Web Build 作业跑它（`npm ci` 与 `npm run build` 原先写成两个 step，
#     与沙盒里那份是同一件事的两份抄写，抄的那一份会漂）；
#   - 沙盒执行器按部署声明（`self_evolution.web_admission`）跑它，跑在被改过的
#     那棵树上，退出码非零即拒收这条变更。
#
# 名字不带 `check-` 不是疏忽：工作流里任何 `scripts/check-*` 的行都会被变更流水线
# 当成**整棵树**的门，在每条变更上跑一遍。这一条判的不是整棵树，它判的是前端，
# 而且是分钟级的——挂给每条 Rust 变更就是把一次前端构建的价格加到全树上。
# 所以它的入口是准入声明，不是那族门禁。
#
# 退出码：0 = 这条变更编得过；非零 = 拒收，输出就是理由；2 = 判定本身跑不起来
# （没有 node/npm），与「编不过」是两回事，别当 0 读。
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root/web"

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  echo "web-build: 这个容器里没有 node/npm，前端这一族无从判定（退出码 2，不是 0）" >&2
  exit 2
fi

# CN 镜像：与镜像构建那段同一个变量名，一个部署只需要在一个地方说这件事。
if [ -n "${NPM_REGISTRY:-}" ]; then
  export npm_config_registry="$NPM_REGISTRY"
fi
# 三条都只是关掉与判据无关的噪声：审计与募捐不是这条变更的判据，更新提示只会
# 在输出里占行。
export npm_config_audit=false
export npm_config_fund=false
export npm_config_update_notifier=false

# `ci` 而不是 `install`：按 lockfile 装，装出来的依赖树与镜像里那一份同形；改了
# package.json 而没更新 lockfile 时它会红，这正是要它红的形态。
# `--prefer-offline`：每个任务一棵临时工作树，依赖缓存留在 `$HOME/.npm`（沙盒里是
# 同一个 HOME），第二次起不必把同一批包再下载一遍。
npm ci --prefer-offline --no-audit --no-fund

# package.json 的 build 就是 `tsc && vite build`：类型错在 tsc 那一半红，打包失败
# 在 vite 那一半红。
npm run build
